"""``gbx_zarr_write``: write a Zarr store transactionally, one Delta MERGE per write.

A write has two phases.

1. **Files**, on the cluster (``_write_tasks``). An explode stage cuts each input block
   into the inner chunks it touches (a block that covers a whole chunk is encoded there).
   For a write into an existing store the pieces are joined, in Spark, with the manifest
   rows of the same shards at the base version, and ``repartition(variable, part)`` brings
   each shard's pieces and base rows to one task. That task assembles each chunk. A chunk
   the input covers only in part is read from those base rows and patched
   (read-modify-write); the shard row it was read from becomes a read guard. The task
   appends the encoded chunks to its pack file and returns one small row per chunk, with
   the base ``write_id`` of its key.
2. **Commit**, on the driver. The rows become manifest rows: each replaced key carries
   the ``write_id`` it had at the base (the compare-and-swap expectation), a new key the
   expectation "must not exist", a read guard the expectation "unchanged". The array
   documents the write changes and the dimension-coordinate chunks, rewritten on the
   driver, are metadata. One MERGE commits all of it (``_manifest.commit``), so a write is
   all or nothing.

**How the input is read.** A DataFrame of GeoBrix tiles becomes blocks first
(``_write_tiles``): the driver plans every block from the tiles' headers, so ``create`` and
``append`` need no position pass, and a task decodes each tile once. An ``xarray.Dataset``
is cut into blocks on the driver. A blocks
DataFrame is read once for ``update`` and ``overwrite``: positions, labels and data come
from the same evaluation. ``create`` and ``append`` need the input's extent (and an
append its new labels) before they can lay the chunks out. They make a narrow first pass
over the block positions, which never reads the ``data`` column (an append also reads its
dimension's coordinate blocks), and they check that the write pass saw the same blocks
(a digest of their positions); an input that changed in between is refused.

**Concurrency.** Before each MERGE the writer compares its keys with the latest version
(``changed_since``). A data chunk, or a shard a chunk was read from, that another writer
changed since the base raises ``ZarrWriteConflict`` and nothing of this write becomes
visible. So does a coordinate cell another writer set to a different value. Anything else
that landed meanwhile is rebased:
- shapes become the maximum extent;
- an append grows every array on its dimension, including ones created meanwhile;
- a new array is sized to the latest extents;
- coordinate chunks are read-modified-written again over the latest version;
- the metadata expectations are refreshed.
Then the MERGE is retried, at most ``_ATTEMPTS`` (20) times with jitter. Data files are
never rewritten. Appenders to one array all change the array documents and the time chunk,
so Delta lands one of them per round; each failed attempt means another writer landed, so
N simultaneous appenders need at most N attempts (measured on Databricks: eight need up to
eight, and a bound of five gave up on three of them).

**Empty chunks.** A chunk whose every cell is the fill value is not stored, in every mode,
as zarr does by default (``write_empty_chunks=False``). The chunk's row is deleted; under a
live shard row it is replaced by a tombstone. With no row at all it is simply dropped:
dropping is a no-op on an absent key, and the result is the same as this write followed by
any concurrent write of that key.

Everything here is Spark Connect safe: no ``sparkContext``, ``.rdd``, ``_jvm``, ``_jsc``
or ``spark.conf.set``; ``spark.sql.shuffle.partitions`` is read through a guarded
``spark.conf.get``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import uuid
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._arrow import to_spark_df
from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr import _write_tiles
from databricks.labs.gbx.zarr._layout import chunk_layout, coordinate_chunks
from databricks.labs.gbx.zarr._meta import (
    ArrayMeta,
    cf_encode,
    decode_coord,
    doc_bytes,
    is_time_coord,
    merge_array,
    parse_v2,
    parse_v3,
    with_shape,
)
from databricks.labs.gbx.zarr._pack import PackWriter, crc32c_hex, encode_inner_chunk
from databricks.labs.gbx.zarr._plan import unit_key
from databricks.labs.gbx.zarr._write_tasks import (
    PIECES_DDL,
    ROWS_ARROW,
    ROWS_DDL,
    TaskConfig,
    assemble_function,
    block_hash,
    block_is_stored,
    chunk_grid,
    explode_function,
    extent,
    part_of,
)
from databricks.labs.gbx.zarr.errors import (
    ZarrAccessUnavailable,
    ZarrError,
    ZarrGridMismatch,
    ZarrOwnershipError,
    ZarrReadOnly,
    ZarrUnsupported,
    ZarrWriteConflict,
)

__all__ = ["gbx_zarr_write"]

_LOG = logging.getLogger(__name__)

_MODES = ("create", "append", "update", "overwrite")
_ACCESS = ("auto", "fuse", "direct")
_BLOCK_COLUMNS = ("variable", "dims", "start", "stop", "dtype", "shape", "data")
_MIB = 1 << 20
_BATCH_ROWS = 256
# Rounds a commit may lose to concurrent writers before it gives up. Writers of one array
# land one per round, so N simultaneous appenders need up to N attempts; 20 covers 16 with
# room for maintenance commits. The wait between attempts doubles up to _MAX_WAIT_SECONDS.
_ATTEMPTS = 20
_MAX_WAIT_SECONDS = 16
_HASH_MASK = (1 << 64) - 1
_CF_ATTRS = ("scale_factor", "add_offset", "_FillValue", "missing_value")
_TIME_ATTRS = ("units", "calendar")
_DEFAULT_PARTITIONS = 64  # when spark.sql.shuffle.partitions is "auto" or unreadable
_CHANGED_INPUT_FIX = (
    "the input changed between planning and writing; materialize it as a table first "
    "(write it to Delta, then pass spark.table(...))"
)

# Test hooks, never set in production. _FAULT = (array, chunk key): the task that encodes
# that chunk raises (it travels to the task in the closure, read when the write is
# planned). _BEFORE_COMMIT: called with the write_id on the driver after the files are
# written and before the commit, to let another write land in between.
_FAULT = None
_BEFORE_COMMIT = None


# --- small helpers ---------------------------------------------------------------------


def _jsonable(value):
    """``value`` with numpy scalars and arrays as plain Python, non-finite floats as the
    strings Zarr metadata uses (``"NaN"``, ``"Infinity"``, ``"-Infinity"``)."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_jsonable(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return (
            "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")
        )
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _fill_from_text(text):
    """A blocks ``fill_value`` (JSON text) as a Python value."""
    if text is None:
        return None
    try:
        value = json.loads(text)
    except ValueError:
        return None
    if value in ("NaN", "Infinity", "-Infinity"):
        return float(value.replace("Infinity", "inf"))
    return value


def _default_fill(dtype: np.dtype):
    if dtype.kind in "fc":
        return float("nan")
    if dtype.kind == "b":
        return False
    return 0


def _budget() -> int:
    """Decoded bytes a task may hold (``pyrx.core.budget``, the canonical budget)."""
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    return max(_MIB, decoded_budget_bytes("auto"))


def _sizes() -> dict:
    """Batch, xarray group and pack roll-over sizes, from the budget, capped where they
    were before (32 MiB, 64 MiB and 512 MiB)."""
    budget = _budget()
    return {
        "batch_bytes": min(32 * _MIB, max(_MIB, budget // 4)),
        "group_bytes": min(64 * _MIB, budget),
        "roll_bytes": min(512 * _MIB, budget * 8),
        "budget": budget,
    }


def _shuffle_partitions(spark) -> int | None:
    """``spark.sql.shuffle.partitions`` as a number, or ``None`` for ``auto`` (Serverless)
    or a key the session will not show. Nothing is ever set."""
    try:
        value = spark.conf.get("spark.sql.shuffle.partitions", None)
    except Exception:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _current_user(spark):
    try:
        return spark.sql("SELECT current_user()").first()[0]
    except Exception:
        return None


def _is_xarray(data) -> bool:
    return (type(data).__module__ or "").startswith("xarray") and hasattr(
        data, "data_vars"
    )


def _is_frame(data) -> bool:
    return hasattr(data, "mapInArrow") and hasattr(data, "columns")


def _cells_equal(a, b) -> np.ndarray:
    """Cell-wise equality, NaN equal to NaN (and NaT to NaT)."""
    a, b = np.asarray(a), np.asarray(b)
    same = a == b
    if a.dtype.kind in "fcM" and b.dtype.kind in "fcM":
        same = same | (np.isnan(a) & np.isnan(b))
    return same


# --- array documents -------------------------------------------------------------------

_CODEC_FIX = (
    "pass codecs as a list of Zarr v3 codec documents (dicts such as "
    "{'name': 'zstd', 'configuration': {'level': 3}}) or zarr codec objects, starting "
    "with a serializer such as {'name': 'bytes'}"
)


def _codec_chain(codecs) -> list[dict] | None:
    """``codecs`` as Zarr v3 codec documents: dicts as given, zarr codec objects through
    their ``to_dict``. Anything else is refused here, before any work."""
    if codecs is None:
        return None
    if isinstance(codecs, (dict, str)) or not hasattr(codecs, "__iter__"):
        raise ZarrUnsupported(
            f"codecs {codecs!r} is not a list of codecs", fix=_CODEC_FIX
        )
    chain = []
    for c in codecs:
        if isinstance(c, dict):
            chain.append(json.loads(json.dumps(c)))
        elif hasattr(c, "to_dict"):
            chain.append(json.loads(json.dumps(c.to_dict())))
        else:
            raise ZarrUnsupported(f"{c!r} is not a codec", fix=_CODEC_FIX)
    if not chain:
        raise ZarrUnsupported("codecs is empty", fix=_CODEC_FIX)
    return chain


def _v3_doc(name, dims, shape, dtype, chunks, shards, fill, attrs, codecs) -> dict:
    """The ``zarr.json`` of a new array, made by zarr itself so it is valid."""
    import zarr
    from zarr.codecs import BytesCodec, Crc32cCodec, ZstdCodec
    from zarr.storage import MemoryStore

    store = MemoryStore()
    zarr.create_array(
        store,
        shape=tuple(shape),
        chunks=tuple(chunks) if chunks is not None else None,  # () is a scalar's
        shards=tuple(shards) if shards else None,
        dtype=np.dtype(dtype),
        fill_value=fill,
        attributes=_jsonable(attrs),
        dimension_names=tuple(dims),
        zarr_format=3,
        serializer=BytesCodec(endian="little"),
        compressors=[ZstdCodec(level=3), Crc32cCodec()],
    )
    doc = json.loads(store._store_dict["zarr.json"].to_bytes())
    if codecs is not None:  # the caller's chain for each inner chunk
        if shards:
            doc["codecs"][0]["configuration"]["codecs"] = codecs
        else:
            doc["codecs"] = codecs
    return doc


def _check_doc(meta: ArrayMeta, codecs) -> None:
    """Open the array with zarr and encode one chunk, so a chain zarr cannot use (an
    unknown codec, no serializer) fails here and not inside a Spark task."""
    try:
        encode_inner_chunk(meta, np.zeros(meta.chunks, dtype=np.dtype(meta.dtype)))
    except Exception as e:  # zarr's own errors vary by codec
        raise ZarrUnsupported(
            f"the codecs {codecs} cannot encode array {meta.name!r} "
            f"({type(e).__name__}: {e})",
            fix=_CODEC_FIX,
        ) from None


def _v2_docs(name, dims, shape, dtype, chunks, fill, attrs, separator) -> dict:
    """``.zarray`` and ``.zattrs`` of a new array in a Zarr v2 store."""
    zarray = {
        "zarr_format": 2,
        "shape": list(shape),
        "chunks": list(chunks),
        "dtype": (
            np.dtype(dtype).newbyteorder("<").str
            if np.dtype(dtype).itemsize > 1
            else np.dtype(dtype).str
        ),
        "compressor": {"id": "zstd", "level": 3},
        "fill_value": _jsonable(fill),
        "order": "C",
        "filters": None,
        "dimension_separator": separator,
    }
    zattrs = {**_jsonable(attrs), "_ARRAY_DIMENSIONS": list(dims)}
    return {".zarray": zarray, ".zattrs": zattrs}


@dataclass
class _NewArray:
    """What a write knows about an array it creates."""

    name: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: np.dtype  # the stored dtype
    attrs: dict
    fill: object
    coordinate: bool


def _new_meta(spec: _NewArray, *, fmt, chunks, shards, codecs, separator) -> ArrayMeta:
    """The ``ArrayMeta`` of a new array: coordinates in one unsharded chunk, data arrays
    laid out by ``chunks`` and ``shards`` (``_layout``); an explicit codec chain is checked
    by encoding one chunk with it."""
    dtype = np.dtype(spec.dtype).newbyteorder("=")
    if dtype.kind not in "biufc":
        raise ZarrUnsupported(
            f"variable {spec.name!r} holds {dtype} values; GeoBrix writes numbers, "
            "booleans and CF times",
            fix="convert it to a numeric dtype, or leave it out of variables",
        )
    if not spec.shape:
        inner, outer = (), None
    elif spec.coordinate:
        inner, outer = coordinate_chunks(spec.shape[0], dtype), None
    else:
        inner, outer = chunk_layout(spec.shape, spec.dims, dtype, chunks)
        if shards is not None:
            outer = _explicit_shards(spec, inner, outer, shards)
        if fmt == 2:
            outer = None
    if fmt == 2:
        docs = _v2_docs(
            spec.name,
            spec.dims,
            spec.shape,
            dtype,
            inner,
            spec.fill,
            spec.attrs,
            separator,
        )
        return parse_v2(spec.name, docs[".zarray"], docs[".zattrs"])
    doc = _v3_doc(
        spec.name,
        spec.dims,
        spec.shape,
        dtype,
        inner,
        outer,
        spec.fill,
        spec.attrs,
        codecs,
    )
    try:
        meta = parse_v3(spec.name, doc)
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        raise ZarrUnsupported(
            f"the codecs {codecs} do not make a readable array ({e})", fix=_CODEC_FIX
        ) from None
    if codecs is not None:
        _check_doc(meta, codecs)
    return meta


def _explicit_shards(
    spec: _NewArray, inner, computed, shards
) -> tuple[int, ...] | None:
    if shards is False:
        return None
    if not isinstance(shards, dict):
        raise ValueError(
            f"shards must be None (computed), False (unsharded) or a dict of dimension "
            f"to shard size, got {shards!r}. Fix: pass shards={{'time': 48}} or None."
        )
    out = list(computed)
    for i, d in enumerate(spec.dims):
        if d in shards:
            size = shards[d]
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < inner[i]
                or size % inner[i]
            ):
                raise ValueError(
                    f"the shard size {size!r} of {d!r} is not a whole multiple of its "
                    f"chunk size {inner[i]}. Fix: pass a multiple of the chunk size."
                )
            out[i] = size
    return tuple(out)


def _doc_keys(meta: ArrayMeta) -> list[str]:
    return ["zarr.json"] if meta.zarr_format == 3 else [".zarray", ".zattrs"]


# --- the input -------------------------------------------------------------------------


@dataclass
class _Var:
    """One variable of the input, in input positions."""

    name: str
    dims: tuple[str, ...]
    dtype: np.dtype  # of the incoming values
    attrs: dict
    fill: object
    lo: tuple[int, ...]
    hi: tuple[int, ...]
    starts: np.ndarray  # (blocks, ndim) start of each block, in input positions
    stops: np.ndarray  # (blocks, ndim) its stop
    values: np.ndarray | None = None  # xarray variables and coordinates
    stored: bool = False  # the values are stored values (coordinate blocks)
    labels: dict = field(default_factory=dict)  # xarray: dim -> the dataset's labels
    encoding: dict | None = None  # xarray: the variable's CF encoding (None for blocks)

    @property
    def coordinate(self) -> bool:
        return len(self.dims) == 1 and self.dims[0] == self.name


@dataclass
class _Input:
    kind: str  # "xarray" | "blocks"
    vars: dict[str, _Var]  # xarray: every variable; blocks: from the position pass
    attrs: dict  # xarray: the dataset attributes
    frame: object = None  # blocks: the blocks to write
    digest: tuple | None = None  # blocks: (count, hash) of the planned blocks
    known: dict = field(default_factory=dict)  # coordinates the driver holds already


def _wanted(names, variables) -> list[str]:
    if variables is None:
        return list(names)
    chosen = [variables] if isinstance(variables, str) else list(variables)
    unknown = [v for v in chosen if v not in names]
    if unknown:
        raise ValueError(
            f"variables {unknown} are not in the input, which has {sorted(names)}. "
            "Fix: name variables of the input."
        )
    return list(dict.fromkeys(chosen))


def _names(variables) -> list[str] | None:
    if variables is None:
        return None
    names = [variables] if isinstance(variables, str) else list(variables)
    if not names or not all(isinstance(n, str) for n in names):
        raise ValueError(
            "variables must be a name or a non-empty list of names. Fix: pass "
            "variables=['t2m'], or leave it None to write every variable."
        )
    return list(dict.fromkeys(names))


_ENCODING_KEYS = (
    "dtype",
    "_FillValue",
    "missing_value",
    "scale_factor",
    "add_offset",
    "units",
    "calendar",
)


def _cf_encoding(encoding) -> dict:
    """The CF part of an xarray variable's encoding (its stored type, fill, packing, time
    units); the storage keys (chunks, compressors) are GeoBrix's to choose."""
    return {k: encoding[k] for k in _ENCODING_KEYS if k in (encoding or {})}


def _xarray_input(ds, variables) -> _Input:
    names = [str(n) for n in ds.data_vars] + [
        str(n) for n in ds.coords if str(n) not in ds.dims
    ]
    chosen = _wanted(names, variables)
    out: dict[str, _Var] = {}
    dims_needed = set()

    def var_of(name, variable) -> _Var:
        dims = tuple(str(d) for d in variable.dims)
        values = np.asarray(variable.values)
        return _Var(
            name,
            dims,
            values.dtype,
            dict(variable.attrs),
            None,
            (0,) * len(dims),
            tuple(values.shape),
            np.zeros((1, len(dims)), dtype=np.int64),
            np.array([values.shape], dtype=np.int64).reshape(1, len(dims)),
            values,
            encoding=_cf_encoding(variable.encoding),
        )

    for name in chosen:
        out[name] = var_of(name, ds[name].variable)
        dims_needed.update(out[name].dims)
    labels = {}
    for d in sorted(dims_needed):
        if d in ds.coords:
            out[d] = var_of(d, ds.coords[d].variable)
            labels[d] = out[d].values
    for v in out.values():
        v.labels = {d: labels[d] for d in v.dims if d in labels}
    return _Input("xarray", out, dict(ds.attrs))


def _coordinate_condition():
    from pyspark.sql import functions as F

    return (F.size("dims") == 1) & (F.element_at("dims", 1) == F.col("variable"))


def _blocks_frame(df, variables, dims=None):
    """The blocks to write: the blocks DataFrame, narrowed to ``variables`` when given.

    Naming variables never drops their labels: the coordinate blocks (one dimension, named
    like it) stay, all of them, or those of ``dims`` when the dimensions the named
    variables use are known.
    """
    from pyspark.sql import functions as F

    missing = [c for c in _BLOCK_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"the DataFrame lacks the blocks column(s) {missing}. Fix: pass the "
            "output of gbx_zarr_read(output='blocks'), or a DataFrame in BLOCKS_SCHEMA."
        )
    names = _names(variables)
    if names is None:
        return df
    coords = _coordinate_condition()
    if dims is not None:
        coords = coords & F.col("variable").isin(sorted(dims))
    return df.where(F.col("variable").isin(names) | coords)


def _digest_of(vars_: dict) -> tuple[int, int]:
    """``(count, hash)`` of a set of blocks: what the write pass must see again."""
    count, digest = 0, 0
    for name, var in vars_.items():
        for start, stop in zip(var.starts.tolist(), var.stops.tolist()):
            digest = (digest + block_hash(name, start, stop)) & _HASH_MASK
            count += 1
    return count, digest


def _planned_blocks(data, variables, seen=None, known=None) -> _Input:
    """A create's or append's blocks, planned from one position pass. With ``variables``
    the named variables are kept with the coordinates of the dimensions they use; any other
    coordinate block is left out of the frame (and so of the digest) before writing.

    ``seen`` is the positions of the blocks when the caller already holds them (tiles:
    the driver planned every block from the tiles' headers), instead of a pass over the
    frame; ``known`` the coordinate values it holds, which an append then does not read.
    """
    from pyspark.sql import functions as F

    frame = _blocks_frame(data, variables)
    vars_ = _position_pass(frame) if seen is None else _summarize_positions(seen)
    names = _names(variables)
    if names is not None:
        missing = [n for n in names if n not in vars_]
        if missing:
            raise ValueError(
                f"variables {missing} are not in the input, which has {sorted(vars_)}. "
                "Fix: name variables of the input."
            )
        used = {d for n in names for d in vars_[n].dims}
        keep = [
            n for n, v in vars_.items() if n in names or (v.coordinate and n in used)
        ]
        if len(keep) < len(vars_):
            frame = frame.where(F.col("variable").isin(keep))
            vars_ = {n: vars_[n] for n in keep}
    return _Input(
        "blocks", vars_, {}, frame=frame, digest=_digest_of(vars_), known=known or {}
    )


def _position_pass(frame) -> dict[str, _Var]:
    """The variables of a blocks frame from one pass over its block positions. Only
    position, type and attribute columns are selected, never ``data``: a staged input is
    not read for its bytes here."""
    from pyspark.sql import functions as F

    cols = [F.col(c) for c in ("variable", "dims", "start", "stop", "dtype", "shape")]
    for opt in ("attrs", "fill_value"):
        cols.append(
            F.col(opt)
            if opt in frame.columns
            else F.lit(None).cast("string").alias(opt)
        )
    return _summarize_positions(frame.select(*cols).toArrow())


def _summarize_positions(seen: pa.Table) -> dict[str, _Var]:
    """The variables of a table of block positions (one row per block)."""
    if not seen.num_rows:
        raise ValueError(
            "the blocks DataFrame is empty. Fix: pass the blocks to write."
        )
    out = {}
    for name in pc.unique(seen.column("variable")).to_pylist():
        if name is None:
            raise ValueError(
                "a block has no variable name. Fix: name the variable of every block."
            )
        sub = seen.filter(pc.equal(seen.column("variable"), name))
        out[name] = _summarize_blocks(name, sub)
    return out


def _positions(sub: pa.Table, column: str, ndim: int, name: str) -> np.ndarray:
    """A list column of one variable's blocks as an ``(blocks, ndim)`` int64 array."""
    col = sub.column(column)
    sizes = pc.fill_null(pc.list_value_length(col), -1)
    if not pc.all(pc.equal(sizes, ndim)).as_py() or col.null_count:
        raise ValueError(
            f"a block of {name!r} has a {column} that is not one integer per dimension "
            f"of its {ndim}. Fix: give each block start, stop and shape per dimension."
        )
    if ndim == 0:
        return np.zeros((sub.num_rows, 0), dtype=np.int64)
    flat = pc.list_flatten(col)
    if flat.null_count:
        raise ValueError(
            f"a block of {name!r} has a missing {column} value. Fix: fill them in."
        )
    return np.asarray(flat, dtype=np.int64).reshape(-1, ndim)


def _summarize_blocks(name, sub: pa.Table) -> _Var:
    """One variable's blocks, column by column: their positions as int64 arrays (no Python
    object per block), checked to be boxes of one set of dimensions and one dtype."""
    joined = pc.binary_join(sub.column("dims"), "\x1f")
    if len(pc.unique(joined)) != 1 or len(pc.unique(sub.column("dtype"))) != 1:
        raise ZarrGridMismatch(
            f"the blocks of {name!r} disagree on their dimensions or dtype",
            fix="give every block of a variable the same dims and dtype",
        )
    dims = tuple(sub.column("dims")[0].as_py() or ())
    dtype = np.dtype(sub.column("dtype")[0].as_py())
    ndim = len(dims)
    starts = _positions(sub, "start", ndim, name)
    stops = _positions(sub, "stop", ndim, name)
    shapes = _positions(sub, "shape", ndim, name)
    if (starts < 0).any() or not np.array_equal(stops - starts, shapes):
        raise ValueError(
            f"a block of {name!r} has a start, stop and shape that do not describe a box "
            "(stop - start must equal shape, from 0 up). Fix: correct its positions."
        )
    lo = tuple(int(x) for x in starts.min(axis=0)) if ndim else ()
    hi = tuple(int(x) for x in stops.max(axis=0)) if ndim else ()
    first = sub.slice(0, 1).select(["attrs", "fill_value"]).to_pylist()[0]
    return _Var(
        name,
        dims,
        dtype,
        _attrs_of(first.get("attrs")),
        _fill_from_text(first.get("fill_value")),
        lo,
        hi,
        starts,
        stops,
    )


def _attrs_of(text) -> dict:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coordinate_values(frame, name) -> tuple[np.ndarray, int, dict, np.dtype]:
    """``(values, start, attrs, dtype)`` of the coordinate blocks of ``name``: the one
    pass over ``data`` an append makes before writing, narrowed to that coordinate."""
    from pyspark.sql import functions as F

    rows = (
        frame.where((F.col("variable") == name) & _coordinate_condition())
        .select(
            "start",
            "stop",
            "dtype",
            "data",
            *(["attrs"] if "attrs" in frame.columns else []),
        )
        .toArrow()
        .to_pylist()
    )
    return _assemble_coordinate(name, rows)


def _assemble_coordinate(name, rows) -> tuple[np.ndarray, int, dict, np.dtype]:
    if not rows:
        return None, 0, {}, None
    dtype = np.dtype(rows[0]["dtype"])
    lo = min(
        r["start"][0] if isinstance(r["start"], list) else r["start"] for r in rows
    )
    hi = max(r["stop"][0] if isinstance(r["stop"], list) else r["stop"] for r in rows)
    values = np.empty(hi - lo, dtype=dtype.newbyteorder("="))
    seen = np.zeros(hi - lo, dtype=bool)
    for r in rows:
        a = (r["start"][0] if isinstance(r["start"], list) else r["start"]) - lo
        block = np.frombuffer(r["data"], dtype=np.dtype(r["dtype"]))
        if seen[a : a + len(block)].any():
            raise ZarrError(
                f"the blocks of the coordinate {name!r} overlap",
                fix="write each label of a coordinate once",
            )
        values[a : a + len(block)] = block
        seen[a : a + len(block)] = True
    if not seen.all():
        raise ValueError(
            f"the blocks of the coordinate {name!r} leave gaps between {lo} and {hi}. "
            "Fix: write the coordinate's labels whole."
        )
    return values, lo, _attrs_of(rows[0].get("attrs")), dtype


def _probe(frame, name) -> dict:
    """dims, dtype, attributes and fill of a variable the store lacks, from its first
    block (``limit(1)``): what creating it takes, without reading the input whole."""
    from pyspark.sql import functions as F

    cols = ["dims", "dtype"] + [
        c for c in ("attrs", "fill_value") if c in frame.columns
    ]
    row = frame.where(F.col("variable") == name).select(*cols).limit(1).toArrow()
    if not row.num_rows:
        raise ValueError(
            f"variables names {name!r}, which has no blocks in the input. Fix: name "
            "variables of the input."
        )
    return row.to_pylist()[0]


# --- the plan --------------------------------------------------------------------------


@dataclass
class _Docs:
    """The documents of a store at one version."""

    metas: dict[str, bytes]
    arrays: dict[str, ArrayMeta]
    unsupported: dict[str, str]
    ids: dict[tuple, str]  # (kind, array, key) -> write_id of the live row


def _docs_at(spark, snap) -> _Docs:
    from databricks.labs.gbx.zarr._read import parse_documents

    metas, ids = {}, {}
    for row in M.read_rows(spark, snap, kinds=["group", "array"]).to_pylist():
        ids[(row["kind"], row["array"], row["key"])] = row["write_id"]
        if row["meta"] is not None:
            metas[M._store_key(row["array"], row["key"])] = row["meta"].encode()
    arrays, unsupported = parse_documents(metas)
    return _Docs(metas, arrays, unsupported, ids)


def _extents_of(arrays: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in arrays.values():
        for d, n in zip(m.dims, m.shape):
            out[d] = max(out.get(d, 0), n)
    return out


@dataclass
class _Patch:
    """Labels of a dimension coordinate this write puts at ``start`` (stored values)."""

    name: str
    start: int
    values: np.ndarray
    base_values: np.ndarray | None = None  # what those cells held at the base


@dataclass
class _Plan:
    mode: str
    table: str
    write_id: str
    root: str  # where this write's files go
    base_root: str | None  # a registered store's own files
    base: object  # Snapshot, or None for a new store
    docs: _Docs | None  # the documents at the base
    arrays: dict[str, ArrayMeta]  # every array as it is after this write
    task_arrays: dict[str, ArrayMeta]  # the data arrays a block may name
    changed: list[str]  # arrays whose documents this write writes
    created: set[str]  # arrays this write creates
    groups: list[str]  # group paths a new store needs ("" is the root)
    group_attrs: dict
    coords: set[str]  # dimension-coordinate arrays of the store
    patches: list[_Patch]
    offsets: dict[str, int]  # dim -> added to an input position
    extents: dict[str, int]  # dim -> length after the write
    fmt: int = 3
    separator: str = "/"
    frame: object = None  # blocks: the blocks to write
    dataset_vars: dict = field(default_factory=dict)  # xarray: the data variables
    boxes: dict = field(
        default_factory=dict
    )  # variable -> (starts, stops), known plans
    digest: tuple | None = None  # (count, hash) the write pass must see
    append_dim: str | None = None
    append_labels: np.ndarray | None = None  # blocks append: the labels planned on
    sizing: set = field(default_factory=set)  # (kind, array, key) new arrays sized from
    single_pass: bool = False
    region: dict = field(default_factory=dict)
    store_labels: dict = field(default_factory=dict)  # decoded coordinates at the base
    planned: dict | None = None  # name -> ArrayMeta as planned, before any rebase


def _as_numbers(values):
    """``(numbers, missing, exact)``: times as int64 nanoseconds and integers as int64
    (exact), anything else as float64."""
    a = np.asarray(values)
    if a.dtype.kind == "M":
        ns = a.astype("datetime64[ns]")
        return ns.astype("int64"), np.isnat(ns), True
    if a.dtype.kind in "iub":
        return a.astype("int64"), np.zeros(a.shape, dtype=bool), True
    f = a.astype("float64")
    return f, np.isnan(f), False


def _axis_step(numbers, positions, exact):
    """The constant step of labels ``numbers`` at index ``positions``, or ``None``."""
    d, di = np.diff(numbers), np.diff(positions)
    if not d.size:
        return None
    if exact:
        if (d % di).any():
            return None
        steps = d // di
        ok = (steps == steps[0]).all() and steps[0] > 0
        return int(steps[0]) if ok else None
    steps = d / di
    ok = np.allclose(steps, steps[0], rtol=1e-9, atol=0) and steps[0] > 0
    return float(steps[0]) if ok else None


def _on_step(x, step, exact) -> bool:
    diffs = np.diff(x)
    return bool((diffs == step).all() if exact else np.allclose(diffs, step, rtol=1e-9))


def _step_index(first_valid_pos, first_valid, value, step, exact):
    """The index ``value`` falls on along a regular axis, or ``None`` off the step."""
    off = value - first_valid
    if exact:
        return None if off % step else int(first_valid_pos + off // step)
    pos = first_valid_pos + off / step
    return int(round(pos)) if abs(pos - round(pos)) <= 1e-6 else None


def _derive_append_start(dim, base_vals, new_vals) -> int:
    """Where along ``dim`` the appended labels go: after the base, on its regular step.

    On a regular axis (equal steps between its labels; missing labels are skipped) the new
    labels are placed by value, so an append may skip ahead and leave steps unwritten for a
    later append or update to fill (appends may land out of order). Otherwise, or with fewer
    than two labels to tell the step from, they start at the current length. Times and
    integers are compared exactly.
    """
    n = len(base_vals)
    if not len(new_vals):
        raise ValueError(
            f"the input has no {dim!r} labels to append. Fix: include the new steps."
        )
    b, bmissing, bexact = _as_numbers(base_vals)
    x, xmissing, xexact = _as_numbers(new_vals)
    exact = bexact and xexact
    if xmissing.any():
        raise ValueError(
            f"the {dim!r} labels to append hold missing values. Fix: append labelled steps."
        )
    if len(x) > 1 and not (np.diff(x) > 0).all():
        raise ValueError(
            f"the {dim!r} labels to append do not increase. Fix: sort the input by {dim!r}."
        )
    valid = np.flatnonzero(~bmissing)
    if valid.size and x[0] <= b[valid[-1]]:
        raise ValueError(
            f"the first {dim!r} label to append ({new_vals[0]}) is not after the store's "
            f'last ({base_vals[valid[-1]]}). Fix: use mode="update" to rewrite existing '
            "steps or fill a gap an earlier append left."
        )
    step = _axis_step(b[valid], valid, exact) if valid.size >= 2 else None
    if step is None and valid.size == 1 and len(x) >= 2:
        step = _axis_step(x, np.arange(len(x)), exact)
    if step is None or not valid.size:
        return n
    if len(x) > 1 and not _on_step(x, step, exact):
        raise ZarrGridMismatch(
            f"the {dim!r} labels to append are not spaced like the store's",
            fix=f"append {dim!r} labels on the store's step",
        )
    at = _step_index(valid[0], b[valid[0]], x[0], step, exact)
    if at is None or at < n:
        raise ZarrGridMismatch(
            f"the first {dim!r} label to append, {new_vals[0]}, does not fall on a step "
            "of the store's axis after its end",
            fix=f"append {dim!r} labels on the store's step",
        )
    return at


def _locate(dim, labels, coord_values) -> int:
    """The index where ``labels`` start along ``dim`` in the store's coordinate.

    They must be a contiguous run of the store's labels, in its order; or, on a regular
    axis, steps the store has no labels for yet (a gap an out-of-order append left), which
    the write then labels.
    """
    labels, coord = np.asarray(labels), np.asarray(coord_values)
    n = len(labels)
    if not n:
        return 0
    if coord.dtype.kind == "M" or labels.dtype.kind == "M":
        a, b = coord.astype("datetime64[ns]"), labels.astype("datetime64[ns]")
        hits = np.flatnonzero(a == b[0])
        match = lambda i: np.array_equal(a[i : i + n], b)  # noqa: E731
    elif coord.dtype.kind in "fc" or labels.dtype.kind in "fc":
        ok = coord[~np.isnan(coord.astype("float64"))] if coord.size else coord
        step = (
            float(np.abs(np.diff(ok.astype("float64"))).min()) if ok.size > 1 else 1.0
        )
        tol = 1e-6 * max(step, 1e-12)
        hits = np.flatnonzero(np.abs(coord - labels[0]) <= tol)
        match = lambda i: len(coord[i : i + n]) == n and np.allclose(  # noqa: E731
            coord[i : i + n], labels, rtol=0, atol=tol
        )
    else:
        hits = np.flatnonzero(coord == labels[0])
        match = lambda i: np.array_equal(coord[i : i + n], labels)  # noqa: E731
    for i in hits.tolist():
        if match(i):
            return int(i)
    at = _gap_index(labels, coord)
    if at is not None:
        return at
    beyond = False
    c, cmissing, _ = _as_numbers(coord)
    x, _, _ = _as_numbers(labels)
    valid = np.flatnonzero(~cmissing)
    if valid.size and x[0] > c[valid[-1]]:
        beyond = True
    raise ZarrGridMismatch(
        f"the input's {dim!r} labels ({labels[0]} ... {labels[-1]}, {n} of them) are not "
        f"a contiguous run of the store's {dim!r} coordinate, nor of its unlabelled steps",
        fix=(
            f'use mode="append" to add {dim!r} steps after the store\'s last'
            if beyond
            else f"write {dim!r} labels the store has, in its order (or steps on its "
            "regular axis that it has no labels for yet)"
        ),
    )


def _gap_index(labels, coord) -> int | None:
    """Where ``labels`` fall on a regular axis whose cells there are unlabelled (or equal
    to them), or ``None``."""
    c, cmissing, cexact = _as_numbers(coord)
    x, xmissing, xexact = _as_numbers(labels)
    exact = cexact and xexact
    valid = np.flatnonzero(~cmissing)
    if valid.size < 2 or xmissing.any():
        return None
    step = _axis_step(c[valid], valid, exact)
    if step is None or (len(x) > 1 and not _on_step(x, step, exact)):
        return None
    at = _step_index(valid[0], c[valid[0]], x[0], step, exact)
    if at is None or at < 0 or at + len(x) > len(c):
        return None
    span_missing = cmissing[at : at + len(x)]
    span = c[at : at + len(x)]
    same = (span == x) if exact else np.isclose(span, x, rtol=1e-12, atol=0)
    return at if (span_missing | same).all() else None


def _decoded_coords(spark, docs: _Docs, base, root, base_root, names) -> dict:
    """The decoded labels of the store's coordinate arrays ``names`` at ``base``."""
    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box
    from databricks.labs.gbx.zarr._store import ManifestStore

    names = [n for n in names if n in docs.arrays]
    if not names:
        return {}
    rows = M.read_rows(spark, base, kinds=["shard", "chunk"], arrays=names).to_pylist()
    store = ManifestStore(
        docs.metas,
        rows,
        FuseRangeReader(root, base_root=base_root),
        {n: docs.arrays[n] for n in names},
    )
    out = {}
    try:
        for n in names:
            raw = read_box(open_manifest_array(store, n), (slice(None),))
            out[n] = decode_coord(raw, docs.arrays[n])
    finally:
        store.close()
    return out


def _new_spec(var: _Var, shape, coordinate: bool) -> _NewArray:
    """The stored type and attributes of a variable the write creates.

    An xarray variable, and any coordinate with its labels, is CF-encoded by xarray (on one
    cell, or the whole of a coordinate or a time variable, whose time units xarray infers
    from the values), which gives its stored dtype and its attributes (units, scale factor,
    fill value). Blocks keep their dtype and attributes; their times are encoded the same
    way.
    """
    attrs = dict(var.attrs)
    dtype = var.dtype
    fill = var.fill
    if var.values is not None and (
        coordinate or dtype.kind in "mM" or var.encoding is not None
    ):
        from xarray import Variable
        from xarray.conventions import encode_cf_variable

        sample = var.values
        if not coordinate and dtype.kind not in "mM" and sample.ndim:
            sample = sample[(slice(0, 1),) * sample.ndim]
        try:
            enc = encode_cf_variable(
                Variable(
                    var.dims, sample, var.attrs, encoding=dict(var.encoding or {})
                ),
                name=var.name,
            )
        except (ValueError, TypeError) as e:
            raise ZarrUnsupported(
                f"variable {var.name!r} cannot be CF-encoded ({e})",
                fix="convert it to numbers or CF times, or leave it out of variables",
            ) from None
        dtype = np.dtype(enc.dtype)
        attrs = dict(enc.attrs)
    elif dtype.kind == "M":
        dtype = np.dtype("int64")
        attrs.setdefault("units", "nanoseconds since 1970-01-01")
        attrs.setdefault("calendar", "proleptic_gregorian")
    if dtype.kind == "m":
        raise ZarrUnsupported(
            f"variable {var.name!r} holds time differences",
            fix="convert it to numbers with units, or leave it out of variables",
        )
    if dtype.kind not in "biufc":
        raise ZarrUnsupported(
            f"variable {var.name!r} holds {dtype} values; GeoBrix writes numbers, "
            "booleans and CF times",
            fix="convert it to a numeric dtype, or leave it out of variables",
        )
    if "_FillValue" in attrs:
        fill = attrs["_FillValue"]
    if fill is None or (dtype.kind not in "fc" and isinstance(fill, float)):
        fill = _default_fill(dtype)
    if coordinate and "units" in attrs and " since " in str(attrs["units"]):
        if dtype.kind == "i" and "_FillValue" not in attrs:
            # a step no append has written reads as NaT, not as the reference date
            attrs["_FillValue"] = int(np.iinfo(dtype).min)
            fill = attrs["_FillValue"]
    return _NewArray(var.name, var.dims, tuple(shape), dtype, attrs, fill, coordinate)


def _new_array_meta(
    plan: _Plan, var: _Var, shape, *, chunks="balanced", shards=None, codecs=None
):
    spec = _new_spec(var, shape, var.coordinate)
    return _new_meta(
        spec,
        fmt=plan.fmt,
        chunks=chunks,
        shards=shards,
        codecs=codecs,
        separator=plan.separator,
    )


def _plan_create(table, write_id, root, inp: _Input, chunks, shards, codecs) -> _Plan:
    offsets: dict[str, int] = {}
    for v in inp.vars.values():
        for d, a in zip(v.dims, v.lo):
            offsets[d] = min(offsets.get(d, a), a)
    extents: dict[str, int] = {}
    for v in inp.vars.values():
        for d, a, b in zip(v.dims, v.lo, v.hi):
            size = b - offsets[d]
            if inp.kind == "xarray" and extents.get(d, size) != size:
                raise ZarrGridMismatch(
                    f"dimension {d!r} has different lengths in the input",
                    fix="write variables whose shared dimensions have one length",
                )
            extents[d] = max(extents.get(d, 0), size)
    offsets = {d: -o for d, o in offsets.items()}
    plan = _Plan(
        mode="create",
        table=table,
        write_id=write_id,
        root=root,
        base_root=None,
        base=None,
        docs=None,
        arrays={},
        task_arrays={},
        changed=[],
        created=set(),
        groups=[],
        group_attrs=_jsonable(inp.attrs),
        coords={n for n, v in inp.vars.items() if v.coordinate},
        patches=[],
        offsets=offsets,
        extents=extents,
        digest=inp.digest,
    )
    chunks_codecs = _codec_chain(codecs)
    for v in inp.vars.values():
        if v.coordinate and inp.kind == "blocks":
            continue  # made at commit, from the labels the write pass carries
        shape = tuple(extents[d] for d in v.dims)
        plan.arrays[v.name] = _new_array_meta(
            plan, v, shape, chunks=chunks, shards=shards, codecs=chunks_codecs
        )
    plan.changed = sorted(plan.arrays)
    plan.created = set(plan.arrays)
    plan.task_arrays = {n: m for n, m in plan.arrays.items() if n not in plan.coords}
    groups = {""}
    for name in inp.vars:
        parts = name.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            groups.add("/".join(parts[:i]))
    plan.groups = sorted(groups)
    if inp.kind == "blocks":
        unlabelled = sorted(
            {d for v in inp.vars.values() for d in v.dims} - set(plan.coords)
        )
        if unlabelled:
            _LOG.warning(
                "%s: the blocks carry no coordinate for %s, so the new store labels "
                "those dimensions by position only (read with variables naming the "
                "coordinates to carry them)",
                table,
                ", ".join(unlabelled),
            )
    _finish_plan(plan, inp)
    return plan


def _write_roots(table, props, owner) -> tuple[str, str | None]:
    """``(root, base_root)``: where this write's files go, and a registered store's own
    files. Refuses a read-only registration, and a root whose owner marker is not ours.
    """
    from databricks.labs.gbx.zarr._register import check_owner_marker

    if props.file_mode != "fuse":
        raise ZarrUnsupported(
            f"{table} keeps its files in file_mode {props.file_mode!r}, which GeoBrix does "
            "not write yet",
            fix="write to a store with file_mode 'fuse'",
        )
    if props.kind == "registered":
        if not props.writable or not props.overlay_root:
            raise ZarrReadOnly(
                f"{table} is a read-only registration of {props.root}",
                fix="register the store with writable=True and an overlay_root",
            )
        check_owner_marker(props.overlay_root, owner, "overlay")
        return props.overlay_root, props.root
    check_owner_marker(props.root, owner, "managed")
    return props.root, None


def _check_against_store(table, vars_, docs: _Docs, extents) -> None:
    """Every written variable is one the store decodes, on its dimensions in its order;
    a new one lies on dimensions the store has."""
    for v in vars_:
        if v.name in docs.unsupported:
            raise ZarrUnsupported(
                f"array {v.name!r} of {table} cannot be decoded, so it cannot be "
                f"written: {docs.unsupported[v.name]}",
                fix="write the other variables, or rewrite this array with "
                "mode='overwrite' after converting it",
            )
        meta = docs.arrays.get(v.name)
        if meta is not None and tuple(meta.dims) != tuple(v.dims):
            raise ZarrGridMismatch(
                f"the input's {v.name!r} has dimensions {v.dims}, the store's {meta.dims}",
                fix="write the variable on the store's dimensions, in their order",
            )
        unknown = [d for d in v.dims if d not in extents]
        if meta is None and unknown:
            raise ZarrGridMismatch(
                f"the new variable {v.name!r} has dimension(s) {unknown} the store does "
                "not have",
                fix="create a new store for data on other dimensions, or write it on the "
                "store's dimensions",
            )


def _default_append_dim(vars_, arrays, coords) -> str:
    """The first dimension, among those of the written variables, labelled by CF times."""
    for v in vars_:
        for d in v.dims:
            if d in coords and d in arrays and is_time_coord(arrays[d]):
                return d
    raise ValueError(
        "append needs append_dim: no dimension of the written variables is labelled by "
        "CF times. Fix: pass append_dim='<dimension>'."
    )


def _extents_after(vars_, offsets, extents, mode, append_dim) -> dict:
    """The length of every dimension after the write; only an append grows one."""
    out = dict(extents)
    for v in vars_:
        for d, b in zip(v.dims, v.hi):
            reach = b + offsets.get(d, 0)
            if reach <= extents.get(d, 0):
                continue
            if mode != "append" or d != append_dim:
                raise ZarrGridMismatch(
                    f"the input reaches index {reach} along {d!r}, beyond the store's "
                    f"{extents.get(d, 0)}",
                    fix=(
                        'use mode="append" to extend the store along it'
                        if mode != "append"
                        else f"stay within the store's {d!r} extent"
                    ),
                )
            out[d] = max(out[d], reach)
    return out


def _sizing_docs(plan: _Plan, docs: _Docs, name: str) -> None:
    """Guard the documents a new array was sized from: per dimension, its coordinate's
    document, or else that of the first array on the dimension. A concurrent change to
    them (an append) then rebases this write, which re-sizes the array."""
    meta = plan.arrays[name]
    for d in meta.dims:
        source = d if d in docs.arrays else None
        if source is None:
            source = next(
                (n for n in sorted(docs.arrays) if d in docs.arrays[n].dims), None
            )
        if source is not None:
            plan.sizing.add(("array", source, _doc_keys(docs.arrays[source])[0]))


def _prepare_tiles(spark, data, tiles, mode, variables, append_dim, store):
    """The plan and blocks of a DataFrame of tiles (``_write_tiles``); ``tiles`` is
    ``(tile column, dim_cols)``, ``store`` the existing store or ``None`` for a create.
    """
    column, dim_cols = tiles
    return _write_tiles.prepare(
        data,
        column,
        mode=mode,
        dim_cols=dim_cols,
        names=_names(variables),
        append_dim=append_dim,
        store=store,
        schema=_blocks_struct(),
    )


def _tile_input(data, dim_cols):
    """``(tile column, dim_cols)`` when ``data`` is a DataFrame of tiles, else ``None``;
    ``dim_cols`` with any other input is refused."""
    column = _write_tiles.tile_column(data) if _is_frame(data) else None
    if column is not None:
        return column, _write_tiles.check_dim_cols(dim_cols, data, column)
    if dim_cols is not None:
        raise ValueError(
            "dim_cols places GeoBrix tiles, and the data holds none. Fix: drop dim_cols, "
            "or pass a DataFrame with a tile column."
        )
    return None


def _create_input(spark, data, tiles, variables) -> _Input:
    """The planned input of a create."""
    if _is_xarray(data):
        return _xarray_input(data, variables)
    if tiles is not None:
        prep = _prepare_tiles(spark, data, tiles, "create", variables, None, None)
        return _planned_blocks(_tile_frame(spark, prep), None, seen=prep.seen)
    return _planned_blocks(data, variables)


def _tile_frame(spark, prep):
    """The blocks of a prepared tile write: the tiles' blocks, and the coordinate blocks the
    driver made for them (the labels of a new store, an append's new steps)."""
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    if not prep.coords:
        return prep.frame
    table = pa.Table.from_pylist(prep.coords, schema=_BLOCKS_ARROW)
    return prep.frame.unionByName(to_spark_df(spark, table, _blocks_struct()))


def _plan_existing(
    spark, table, write_id, mode, props, data, variables, append_dim, owner, tiles=None
) -> _Plan:
    root, base_root = _write_roots(table, props, owner)
    base = M.snapshot(spark, table)
    docs = _docs_at(spark, base)
    arrays = dict(docs.arrays)
    fmt = 3 if any(k.endswith("zarr.json") for k in docs.metas) else 2
    separator = next(
        (m.key_encoding.separator for m in arrays.values() if m.zarr_format == 2), "."
    )
    coords = {n for n, m in arrays.items() if len(m.dims) == 1 and m.dims[0] == n}
    extents = _extents_of(arrays)
    plan = _Plan(
        mode=mode,
        table=table,
        write_id=write_id,
        root=root,
        base_root=base_root,
        base=base,
        docs=docs,
        arrays=arrays,
        task_arrays={},
        changed=[],
        created=set(),
        groups=[],
        group_attrs={},
        coords=coords,
        patches=[],
        offsets={},
        extents=extents,
        fmt=fmt,
        separator=separator,
    )
    labels = None
    if _is_xarray(data):
        inp = _xarray_input(data, variables)
    elif tiles is not None:
        labels = _decoded_coords(spark, docs, base, root, base_root, sorted(coords))
        prep = _prepare_tiles(
            spark,
            data,
            tiles,
            mode,
            variables,
            append_dim,
            _write_tiles.StoreView(arrays, labels, docs),
        )
        frame = _tile_frame(spark, prep)
        if mode in ("update", "overwrite"):
            return _plan_single_pass(spark, plan, frame, prep.names)
        inp = _planned_blocks(frame, None, seen=prep.seen, known=prep.known)
        plan.digest = inp.digest
        append_dim = prep.append_dim
    elif mode in ("update", "overwrite"):
        return _plan_single_pass(spark, plan, data, variables)
    else:
        inp = _planned_blocks(data, variables)
        plan.digest = inp.digest
    _check_against_store(table, inp.vars.values(), docs, extents)
    dims_in = {d for v in inp.vars.values() for d in v.dims}
    store_labels = (
        {c: labels[c] for c in sorted(coords) if c in dims_in and c in labels}
        if labels is not None
        else _decoded_coords(
            spark,
            docs,
            base,
            root,
            base_root,
            sorted(c for c in coords if c in dims_in),
        )
    )
    plan.store_labels = store_labels
    if mode == "append":
        append_dim = append_dim or _default_append_dim(
            inp.vars.values(), arrays, coords
        )
        if append_dim not in extents:
            raise ValueError(
                f"append_dim {append_dim!r} is not a dimension of {table}, which has "
                f"{sorted(extents)}. Fix: name one of its dimensions."
            )
        labels, lo = _append_labels(inp, append_dim, arrays)
        if append_dim not in store_labels:
            raise ZarrError(
                f"{table} has no coordinate array for {append_dim!r}, so the new labels "
                f"cannot be placed after its {extents[append_dim]} steps",
                fix=f"write the {append_dim!r} labels of the existing steps first "
                f'(mode="update" with the coordinate), then append',
            )
        at = _derive_append_start(append_dim, store_labels[append_dim], labels)
        plan.offsets[append_dim] = at - lo
        plan.append_dim = append_dim
        plan.append_labels = labels
    for d in sorted(dims_in - set(plan.offsets)):
        ref = next((v for v in inp.vars.values() if d in v.labels), None)
        if inp.kind == "xarray" and ref is not None and d in store_labels:
            plan.offsets[d] = _locate(d, ref.labels[d], store_labels[d])
        else:
            plan.offsets[d] = 0
    plan.extents = _extents_after(
        inp.vars.values(), plan.offsets, extents, mode, plan.append_dim
    )
    _grow_arrays(plan)
    for v in inp.vars.values():
        if v.name in arrays or (v.coordinate and inp.kind == "blocks"):
            continue
        shape = tuple(plan.extents[d] for d in v.dims)
        arrays[v.name] = _new_array_meta(plan, v, shape)
        plan.changed.append(v.name)
        plan.created.add(v.name)
        _sizing_docs(plan, docs, v.name)
        _add_parent_groups(plan, docs, v.name)
        if v.coordinate:
            coords.add(v.name)
    plan.changed = sorted(set(plan.changed))
    plan.task_arrays = {
        v.name: arrays[v.name]
        for v in inp.vars.values()
        if not v.coordinate and v.name in arrays
    }
    _finish_plan(plan, inp, store_labels=store_labels)
    return plan


def _add_parent_groups(plan: _Plan, docs: _Docs, name: str) -> None:
    """A new array ``a/b/x`` in an existing store needs the group documents of ``a`` and
    ``a/b`` where the store has none (a document it has is left as it is)."""
    key = "zarr.json" if plan.fmt == 3 else ".zgroup"
    parts = name.split("/")[:-1]
    for i in range(1, len(parts) + 1):
        path = "/".join(parts[:i])
        if ("group", path, key) not in docs.ids and path not in plan.groups:
            plan.groups.append(path)


def _grow_arrays(plan: _Plan) -> None:
    """Every array whose dimensions the write lengthens takes the new lengths."""
    for name, meta in list(plan.arrays.items()):
        shape = tuple(plan.extents.get(d, n) for d, n in zip(meta.dims, meta.shape))
        if shape != meta.shape:
            plan.arrays[name] = with_shape(meta, shape)
            plan.changed.append(name)


def _append_labels(inp: _Input, append_dim, arrays) -> tuple[np.ndarray, int]:
    """The decoded labels an append adds along ``append_dim`` and their input start."""
    cvar = inp.vars.get(append_dim)
    if inp.kind == "blocks" and cvar is not None:
        values, lo, attrs, _ = (
            inp.known[append_dim]
            if append_dim in inp.known
            else _coordinate_values(inp.frame, append_dim)
        )
        cvar.values, cvar.attrs = values, attrs
        cvar.stored = any(k in attrs for k in _CF_ATTRS + _TIME_ATTRS)
    if cvar is None or cvar.values is None:
        raise ValueError(
            f"an append needs the {append_dim!r} labels of the new steps. Fix: include "
            f"the {append_dim!r} coordinate in the input (an xarray coordinate, or "
            "blocks of that variable)."
        )
    labels = (
        decode_coord(cvar.values, arrays[append_dim])
        if cvar.stored and append_dim in arrays
        else cvar.values
    )
    return labels, cvar.lo[0]


def _plan_single_pass(spark, plan: _Plan, data, variables) -> _Plan:
    """An update or overwrite from blocks: no pass before the write. The tasks may meet
    any decodable data array of the store (or those ``variables`` names, with the
    coordinate blocks of their dimensions); a variable the store lacks is created only when
    ``variables`` names it, from its first block."""
    frame = _blocks_frame(data, None)
    docs, names = plan.docs, _names(variables)
    data_arrays = {n: m for n, m in plan.arrays.items() if n not in plan.coords}
    if names is not None:
        for n in names:
            if n in docs.unsupported:
                raise ZarrUnsupported(
                    f"array {n!r} of {plan.table} cannot be decoded, so it cannot be "
                    f"written: {docs.unsupported[n]}",
                    fix="write the other variables",
                )
            if n in data_arrays or n in plan.coords:
                continue
            probe = _probe(frame, n)
            dims = tuple(probe["dims"] or ())
            var = _Var(
                n,
                dims,
                np.dtype(probe["dtype"]),
                _attrs_of(probe.get("attrs")),
                _fill_from_text(probe.get("fill_value")),
                (0,) * len(dims),
                (0,) * len(dims),
                np.zeros((0, len(dims)), dtype=np.int64),
                np.zeros((0, len(dims)), dtype=np.int64),
            )
            if var.coordinate:
                continue  # coordinates are made at commit, from their blocks
            _check_against_store(plan.table, [var], docs, plan.extents)
            shape = tuple(plan.extents[d] for d in dims)
            plan.arrays[n] = data_arrays[n] = _new_array_meta(plan, var, shape)
            plan.changed.append(n)
            plan.created.add(n)
            _sizing_docs(plan, docs, n)
            _add_parent_groups(plan, docs, n)
        data_arrays = {n: m for n, m in data_arrays.items() if n in names}
        dims = {d for n in names if n in plan.arrays for d in plan.arrays[n].dims}
        frame = _blocks_frame(
            data, names, dims | {n for n in names if n in plan.coords}
        )
    plan.task_arrays = data_arrays
    plan.frame = frame
    plan.single_pass = True
    return plan


def _same_labels(name, values, stored, meta: ArrayMeta, labels, start) -> None:
    """Refuse coordinate blocks beside an append that disagree with the store's labels."""
    mine = decode_coord(values, meta) if stored else np.asarray(values)
    theirs = np.asarray(labels)[start : start + len(mine)]
    if len(theirs) != len(mine):
        same = False
    elif theirs.dtype.kind == "M" or mine.dtype.kind == "M":
        same = np.array_equal(
            theirs.astype("datetime64[ns]"), mine.astype("datetime64[ns]")
        )
    elif theirs.dtype.kind in "fc" or mine.dtype.kind in "fc":
        same = np.allclose(theirs, mine, rtol=1e-12, atol=0, equal_nan=True)
    else:
        same = np.array_equal(theirs, mine)
    if not same:
        raise ZarrGridMismatch(
            f"the input's {name!r} labels differ from the store's at the same positions",
            fix=f"append on the store's {name!r} grid, or leave {name!r} out",
        )


def _finish_plan(plan: _Plan, inp: _Input, *, store_labels=None):
    """Target boxes (for sizing the shuffle), and an xarray input's coordinate patches."""
    for v in inp.vars.values():
        shift = np.array([plan.offsets.get(d, 0) for d in v.dims], dtype=np.int64)
        starts, stops = v.starts + shift, v.stops + shift
        if v.coordinate:
            if inp.kind == "xarray":
                _xarray_patch(plan, v, store_labels)
            continue
        meta = plan.arrays[v.name]
        shape = np.array(meta.shape, dtype=np.int64)
        outside = (starts < 0).any(axis=1) | (stops > shape).any(axis=1)
        if outside.any():
            i = int(np.flatnonzero(outside)[0])
            raise ZarrGridMismatch(
                f"a block of {v.name!r} spans {tuple(starts[i].tolist())}.."
                f"{tuple(stops[i].tolist())}, outside the array's shape {meta.shape}",
                fix="write blocks within the store's extent",
            )
        plan.boxes[v.name] = (starts, stops)
    if inp.kind == "blocks":
        plan.frame = inp.frame
    else:
        plan.dataset_vars = {n: inp.vars[n] for n in plan.task_arrays}


def _xarray_patch(plan: _Plan, v: _Var, store_labels) -> None:
    """An xarray coordinate is written where it is new: a new store, an append's labels,
    or steps a gap left unlabelled; where the labels placed the input they are the
    store's already."""
    d = v.dims[0]
    start = v.lo[0] + plan.offsets.get(d, 0)
    meta = plan.arrays[v.name]
    if (
        plan.mode != "create"
        and d != plan.append_dim
        and store_labels
        and d in store_labels
    ):
        theirs = np.asarray(store_labels[d])[start : start + len(v.values)]
        if (
            len(theirs) == len(v.values)
            and _cells_equal(
                (
                    np.asarray(v.values).astype(theirs.dtype, copy=False)
                    if theirs.dtype.kind == "M"
                    else v.values
                ),
                theirs,
            ).all()
        ):
            return
    plan.patches.append(_Patch(v.name, start, cf_encode(v.values, meta, stored=False)))


# --- phase 1: the files ----------------------------------------------------------------


def _coverage(meta: ArrayMeta, starts: np.ndarray, stops: np.ndarray):
    """``(touched, partial)``: the chunks the blocks touch, and those they cover only in
    part. A block inside one chunk (the usual case: blocks read from a store, or cut from a
    dataset) is counted without a Python loop; only a block across chunks is walked."""
    covered: dict[tuple, int] = {}
    n, ndim = starts.shape
    if ndim == 0:
        covered = {(): n} if n else {}
    elif n:
        c = np.array(meta.chunks, dtype=np.int64)
        first, last = starts // c, (stops - 1) // c
        nonempty = (stops > starts).all(axis=1)
        single = nonempty & (first == last).all(axis=1)
        if single.any():
            cells = np.prod(stops[single] - starts[single], axis=1)
            keys, inverse = np.unique(first[single], axis=0, return_inverse=True)
            sums = np.zeros(len(keys), dtype=np.int64)
            np.add.at(sums, inverse.reshape(-1), cells)
            covered = {tuple(k): int(v) for k, v in zip(keys.tolist(), sums.tolist())}
        for i in np.flatnonzero(nonempty & ~single).tolist():
            start, stop = starts[i].tolist(), stops[i].tolist()
            for chunk in chunk_grid(meta, start, stop):
                origin = [k * cc for k, cc in zip(chunk, meta.chunks)]
                ext = extent(meta, chunk)
                cells = 1
                for a, b, o, e in zip(start, stop, origin, ext):
                    cells *= max(0, min(b, o + e) - max(a, o))
                covered[chunk] = covered.get(chunk, 0) + cells
    partial = {k for k, v in covered.items() if v < math.prod(extent(meta, k))}
    return set(covered), partial


def _planned(plan: _Plan):
    """``(parts, nbytes)`` of a write whose blocks are known ahead: the ``(variable,
    part)`` groups it touches and their decoded bytes; ``(None, None)`` for one pass."""
    if not plan.boxes:
        return None, None
    parts, nbytes = set(), 0
    for name, (starts, stops) in plan.boxes.items():
        meta = plan.task_arrays[name]
        touched, _ = _coverage(meta, starts, stops)
        parts.update((name, part_of(meta, c)[0]) for c in touched)
        nbytes += len(touched) * math.prod(meta.chunks) * meta.itemsize
    return parts, nbytes


def _partitions(spark, parts, nbytes, budget: int) -> int | None:
    """The shuffle's partition count: enough for the session's parallelism and for the
    planned bytes (an assemble task's input within the budget), no more than the groups.
    ``None`` when the input was not read ahead (one pass): Spark then sizes the shuffle
    (AQE coalesces it by bytes)."""
    if parts is None:
        return None
    par = _shuffle_partitions(spark) or _DEFAULT_PARTITIONS
    return max(1, min(len(parts), max(par, math.ceil(nbytes / budget))))


def _xarray_frame(spark, plan: _Plan, group_bytes: int):
    """An xarray input's data variables as chunk-aligned blocks, in target positions,
    created in Spark about ``group_bytes`` at a time."""
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    frames, rows, held = [], [], 0
    schema = _blocks_struct()

    def flush():
        table = pa.Table.from_batches(
            [pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)]
        )
        frames.append(to_spark_df(spark, table, schema))

    for name, var in plan.dataset_vars.items():
        meta = plan.arrays[name]
        values = np.ascontiguousarray(var.values)
        start = tuple(plan.offsets.get(d, 0) for d in var.dims)
        stop = tuple(a + n for a, n in zip(start, values.shape))
        for chunk in chunk_grid(meta, start, stop):
            origin = tuple(k * c for k, c in zip(chunk, meta.chunks))
            lo = tuple(max(a, o) for a, o in zip(start, origin))
            hi = tuple(min(b, o + c) for b, o, c in zip(stop, origin, meta.chunks))
            block = np.ascontiguousarray(
                values[tuple(slice(a - s, b - s) for a, b, s in zip(lo, hi, start))]
            )
            if not block.dtype.isnative:
                block = block.astype(block.dtype.newbyteorder("="))
            if rows and held + block.nbytes > group_bytes:
                flush()
                rows, held = [], 0
            rows.append(
                {
                    "variable": name,
                    "dims": list(var.dims),
                    "start": list(lo),
                    "stop": list(hi),
                    "coord_start": None,
                    "coord_stop": None,
                    "dtype": block.dtype.str,
                    "shape": list(block.shape),
                    "data": block.tobytes(),
                    "fill_value": None,
                    "attrs": None,
                }
            )
            held += block.nbytes
    if rows:
        flush()
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def _blocks_struct():
    from pyspark.sql import types as T

    s, b = T.StringType(), T.LongType()
    return T.StructType(
        [
            T.StructField("variable", s),
            T.StructField("dims", T.ArrayType(s)),
            T.StructField("start", T.ArrayType(b)),
            T.StructField("stop", T.ArrayType(b)),
            T.StructField("coord_start", T.ArrayType(s)),
            T.StructField("coord_stop", T.ArrayType(s)),
            T.StructField("dtype", s),
            T.StructField("shape", T.ArrayType(b)),
            T.StructField("data", T.BinaryType()),
            T.StructField("fill_value", s),
            T.StructField("attrs", s),
        ]
    )


def _base_frame(spark, plan: _Plan, names: list[str], parts=None):
    """The live shard and chunk rows of ``names`` at the base version, as pieces of the
    group (``part``) they serve; only those of ``parts`` (``(variable, part)`` pairs) when
    the write knows which groups it touches. Read by the cluster; never collected."""
    from pyspark.sql import functions as F

    rows = spark.sql(
        f"SELECT kind, array, key, shard_key, file, offset, length, origin, identity, "
        f"write_id FROM {M._q(plan.table)} VERSION AS OF {int(plan.base.version)} "
        "WHERE tag IS NULL AND kind IN ('shard', 'chunk')"
    ).where(F.col("array").isin(names))
    part = (
        F.when(F.col("kind") == "shard", F.col("key"))
        .when(F.col("shard_key").isNotNull(), F.col("shard_key"))
        .otherwise(F.col("key"))
    )
    record = F.to_json(
        F.struct(
            "kind",
            "array",
            "key",
            "shard_key",
            "file",
            "offset",
            "length",
            "origin",
            "identity",
            "write_id",
        )
    )
    out = rows.select(
        F.col("array").alias("variable"),
        F.col("key"),
        part.alias("part"),
        F.lit(None).cast("string").alias("shard_key"),
        F.lit(0).cast("int").alias("ord"),
        F.lit(None).cast("array<bigint>").alias("chunk"),
        F.lit("base").alias("kind"),
        F.lit(None).cast("array<bigint>").alias("lo"),
        F.lit(None).cast("array<bigint>").alias("shape"),
        F.lit(None).cast("binary").alias("data"),
        F.lit(None).cast("boolean").alias("empty"),
        record.alias("row"),
    )
    if parts is not None:
        wanted = spark.createDataFrame(
            sorted(p for p in parts if p[0] in names), "variable STRING, part STRING"
        )
        out = out.join(F.broadcast(wanted), ["variable", "part"], "left_semi")
    return out


def _write_files(spark, plan: _Plan) -> pa.Table:
    """Phase 1: the explode and assemble stages. Returns their rows (``ROWS_ARROW``):
    chunks, guards, deletes, coordinate blocks and digests."""
    sizes = _sizes()
    frame = plan.frame
    if frame is None and plan.dataset_vars:
        frame = _xarray_frame(spark, plan, sizes["group_bytes"])
    if frame is None:
        return ROWS_ARROW.empty_table()
    base_names = sorted(
        n for n in plan.task_arrays if plan.docs and n in plan.docs.arrays
    )
    unknown_fix = (
        "name it in variables=[...] so the write creates it, or leave its blocks out"
        if plan.single_pass
        else _CHANGED_INPUT_FIX
    )
    cfg = TaskConfig(
        write_id=plan.write_id,
        mode=plan.mode,
        root=plan.root,
        base_root=plan.base_root,
        arrays=dict(plan.task_arrays),
        offsets=dict(plan.offsets) if plan.frame is not None else {},
        base_docs={
            k: v
            for k, v in (plan.docs.metas.items() if plan.docs else ())
            if k.rpartition("/")[0] in base_names
        },
        base_arrays={n: plan.docs.arrays[n] for n in base_names},
        unknown_fix=unknown_fix,
        unsupported=dict(plan.docs.unsupported) if plan.docs else {},
        batch_rows=_BATCH_ROWS,
        batch_bytes=sizes["batch_bytes"],
        roll_bytes=sizes["roll_bytes"],
        fault=_FAULT,
    )
    pieces = frame.mapInArrow(explode_function(cfg), PIECES_DDL)
    parts, nbytes = _planned(plan)
    if plan.mode != "create" and base_names:
        # an overwrite deletes every row its input does not replace: it needs them all
        scope = None if plan.mode == "overwrite" else parts
        if scope is None or scope:
            pieces = pieces.unionByName(_base_frame(spark, plan, base_names, scope))
    n = _partitions(spark, parts, nbytes, sizes["budget"])
    _LOG.info(
        "gbx_zarr_write %s (%s): %s shuffle partitions",
        plan.table,
        plan.mode,
        n if n is not None else "adaptive",
    )
    grouped = (
        pieces.repartition("variable", "part")
        if n is None
        else pieces.repartition(n, "variable", "part")
    ).sortWithinPartitions("variable", "part", "ord", "key")
    return grouped.mapInArrow(assemble_function(cfg), ROWS_DDL).toArrow()


# --- after the files: side rows, coordinates ---------------------------------------------


def _side_info(rows: pa.Table) -> dict:
    """What the explode stage reported beside the chunks: the digest of the blocks it saw,
    the region they cover, the variables they hold, and the coordinate blocks."""
    digests = rows.filter(pc.equal(rows.column("rtype"), "digest")).column("info")
    count, digest, region, variables = 0, 0, {}, set()
    for text in digests.to_pylist():
        info = json.loads(text)
        count += info["count"]
        digest = (digest + int(info["hash"])) & _HASH_MASK
        variables.update(info["variables"])
        for d, (a, b) in info["region"].items():
            lo_hi = region.setdefault(d, [a, b])
            lo_hi[0], lo_hi[1] = min(lo_hi[0], a), max(lo_hi[1], b)
    coord = rows.filter(pc.equal(rows.column("rtype"), "coord"))
    coords: dict[str, list] = {}
    for info, data in zip(
        coord.column("info").to_pylist(), coord.column("data").to_pylist()
    ):
        rec = json.loads(info)
        coords.setdefault(rec["variable"], []).append({**rec, "data": data})
    return {
        "count": count,
        "hash": digest,
        "region": region,
        "variables": variables,
        "coords": coords,
    }


def _apply_coordinates(plan: _Plan, coords: dict) -> None:
    """The coordinate blocks of a blocks input, as patches; a coordinate the store lacks is
    created first (its units come from its labels)."""
    for name, recs in sorted(coords.items()):
        values, lo, attrs, dtype = _assemble_coordinate(name, recs)
        start = lo + plan.offsets.get(name, 0)
        meta = plan.arrays.get(name)
        if meta is not None and (start < 0 or start + len(values) > meta.shape[0]):
            raise ZarrGridMismatch(
                f"the {name!r} coordinate blocks span {start}..{start + len(values)}, "
                f"outside the array's {meta.shape[0]} labels",
                fix='write coordinate blocks within the store\'s extent, or use mode="append" '
                "to extend it",
            )
        stored = (
            block_is_stored(json.dumps(attrs), meta)
            if meta is not None
            else any(k in attrs for k in _CF_ATTRS + _TIME_ATTRS)
        )
        if meta is None:
            length = max(plan.extents.get(name, 0), start + len(values))
            var = _Var(
                name,
                (name,),
                dtype,
                attrs,
                None,
                (lo,),
                (lo + len(values),),
                np.zeros((0, 1), dtype=np.int64),
                np.zeros((0, 1), dtype=np.int64),
                values,
                stored,
            )
            meta = _new_array_meta(plan, var, (length,))
            plan.arrays[name] = meta
            plan.changed = sorted({*plan.changed, name})
            plan.created.add(name)
            plan.coords.add(name)
            if plan.docs is not None:
                _sizing_docs(plan, plan.docs, name)
        elif plan.mode == "append" and name == plan.append_dim:
            got = decode_coord(values, meta) if stored else values
            if not _cells_equal(
                (
                    np.asarray(got).astype("datetime64[ns]")
                    if np.asarray(got).dtype.kind == "M"
                    else got
                ),
                (
                    np.asarray(plan.append_labels).astype("datetime64[ns]")
                    if np.asarray(plan.append_labels).dtype.kind == "M"
                    else plan.append_labels
                ),
            ).all() or len(got) != len(plan.append_labels):
                raise ZarrError(
                    f"the {name!r} labels the write saw differ from those it planned on",
                    fix=_CHANGED_INPUT_FIX,
                )
        elif plan.mode == "append" and name in plan.coords and plan.docs is not None:
            labels = plan.store_labels.get(name)
            if labels is not None:
                _same_labels(name, values, stored, meta, labels, start)
                continue
        plan.patches.append(_Patch(name, start, cf_encode(values, meta, stored=stored)))
        lo_hi = plan.region.setdefault(name, [start, start + len(values)])
        lo_hi[0] = min(lo_hi[0], start)
        lo_hi[1] = max(lo_hi[1], start + len(values))


# --- phase 2: the commit ---------------------------------------------------------------

_SCHEMA = M._COMMIT_SCHEMA


def _empty_rows() -> pa.Table:
    return _SCHEMA.empty_table()


def _rows_table(recs: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(
        [{c: r.get(c) for c in _SCHEMA.names} for r in recs], schema=_SCHEMA
    )


def _typed_rows(columns: dict, n: int) -> pa.Table:
    arrays = []
    for f in _SCHEMA:
        col = columns.get(f.name)
        if col is None:
            arrays.append(pa.nulls(n, f.type))
        else:
            col = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
            arrays.append(col.cast(f.type))
    return pa.Table.from_arrays(arrays, schema=_SCHEMA)


def _const(value: str, n: int) -> pa.Array:
    return pa.array([value] * n, pa.string())


def _chunk_rows(chunks: pa.Table, w: str, tomb, delete) -> pa.Table:
    n = chunks.num_rows
    if not n:
        return _empty_rows()
    nulls = pa.nulls(n, pa.string())
    gone = pc.or_(tomb, delete)
    return _typed_rows(
        {
            "kind": _const("chunk", n),
            "array": chunks.column("array"),
            "key": chunks.column("key"),
            "shard_key": chunks.column("shard_key"),
            "file": pc.if_else(gone, nulls, chunks.column("file")),
            "offset": pc.if_else(
                gone, pa.nulls(n, pa.int64()), chunks.column("offset")
            ),
            "length": pc.if_else(
                tomb,
                pa.array(np.zeros(n, dtype=np.int64)),
                pc.if_else(delete, pa.nulls(n, pa.int64()), chunks.column("length")),
            ),
            "checksum": pc.if_else(gone, nulls, chunks.column("checksum")),
            "origin": _const("gbx", n),
            "write_id": _const(w, n),
            "op": pc.if_else(delete, _const("delete", n), _const("upsert", n)),
            "expected_write_id": chunks.column("expected_write_id"),
            "identity": chunks.column("identity"),
        },
        n,
    )


def _commit_data_rows(plan: _Plan, rows: pa.Table, written: set) -> pa.Table:
    """The data rows to commit, column by column (a commit can carry millions):

    - each chunk with the base write_id of its key; a chunk of fill is dropped, or deletes
      its row, or is a tombstone (under a live shard row, or over a base object it hides);
    - one check row per shard a chunk was read from;
    - in an overwrite, the deletes of every base row of the written variables that no
      chunk replaced.
    """
    w = plan.write_id
    rtype = rows.column("rtype")
    parts = []
    chunks = rows.filter(pc.equal(rtype, "chunk")).combine_chunks()
    if chunks.num_rows:
        empty = pc.fill_null(chunks.column("empty"), False)
        live = pc.is_valid(chunks.column("expected_write_id"))
        held = pc.fill_null(chunks.column("shard_live"), False)
        # an unsharded chunk row that shadows a base object (a registered store's own file)
        # must not simply go: with no row left, the next sync would list that object as new
        # and bring its old values back. It becomes a tombstone that remembers the object.
        shadows = pc.is_valid(chunks.column("identity"))
        tomb = pc.and_(empty, pc.or_(held, shadows))
        delete = pc.and_(pc.and_(empty, pc.invert(tomb)), live)
        keep = pc.invert(pc.and_(pc.and_(empty, pc.invert(tomb)), pc.invert(live)))
        parts.append(
            _chunk_rows(chunks.filter(keep), w, tomb.filter(keep), delete.filter(keep))
        )
    guards = rows.filter(pc.equal(rtype, "guard"))
    if guards.num_rows:
        once = guards.group_by(["array", "key"]).aggregate(
            [("expected_write_id", "min")]
        )
        n = once.num_rows
        parts.append(
            _typed_rows(
                {
                    "kind": _const("shard", n),
                    "array": once.column("array"),
                    "key": once.column("key"),
                    "write_id": _const(w, n),
                    "op": _const("check", n),
                    "expected_write_id": once.column("expected_write_id_min"),
                },
                n,
            )
        )
    deletes = rows.filter(pc.equal(rtype, "delete"))
    if deletes.num_rows:
        deletes = deletes.filter(
            pc.is_in(
                deletes.column("array"),
                value_set=pa.array(sorted(written), pa.string()),
            )
        )
        n = deletes.num_rows
        if n:
            parts.append(
                _typed_rows(
                    {
                        "kind": deletes.column("kind"),
                        "array": deletes.column("array"),
                        "key": deletes.column("key"),
                        "write_id": _const(w, n),
                        "op": _const("delete", n),
                        "expected_write_id": deletes.column("expected_write_id"),
                    },
                    n,
                )
            )
    parts = [p for p in parts if p.num_rows]
    return pa.concat_tables(parts) if parts else _empty_rows()


def _doc_commit_rows(plan: _Plan, ids: dict) -> pa.Table:
    """The group and array documents this write writes, with their expectations, and a
    check row on each document a new array was sized from."""
    w = plan.write_id
    recs = []
    for path in plan.groups:
        if plan.fmt == 3:
            key = "zarr.json"
            doc = {"zarr_format": 3, "node_type": "group", "attributes": {}}
            if path == "":
                doc["attributes"] = plan.group_attrs
        else:
            key, doc = ".zgroup", {"zarr_format": 2}
        recs.append(
            {
                "kind": "group",
                "array": path or None,
                "key": key,
                "origin": "gbx",
                "write_id": w,
                "op": "upsert",
                "expected_write_id": ids.get(("group", path or None, key)),
                "meta": json.dumps(doc, sort_keys=True),
            }
        )
    written = set()
    for name in plan.changed:
        for key, raw in doc_bytes(plan.arrays[name]).items():
            written.add(("array", name, key))
            recs.append(
                {
                    "kind": "array",
                    "array": name,
                    "key": key,
                    "origin": "gbx",
                    "write_id": w,
                    "op": "upsert",
                    "expected_write_id": ids.get(("array", name, key)),
                    "meta": raw.decode(),
                }
            )
    for kind, array, key in sorted(plan.sizing - written):
        recs.append(
            {
                "kind": kind,
                "array": array,
                "key": key,
                "write_id": w,
                "op": "check",
                "expected_write_id": ids.get((kind, array, key)),
            }
        )
    return _rows_table(recs) if recs else _empty_rows()


def _coordinate_rows(spark, plan: _Plan, snap, docs: _Docs | None, *, rebase=False):
    """The coordinate chunks this write rewrites, read-modified-written on the driver over
    ``snap`` (the base, or the latest version in a rebase), in a pack of their own.

    Every touched chunk is read at ``snap`` (they are small), so labels another writer
    added there are kept whatever this write's own view of the shape. At the base the
    cells the write patches are remembered; in a rebase a cell another writer changed
    since, to a value other than this write's, is a conflict.
    """
    if not plan.patches:
        return _empty_rows()
    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box
    from databricks.labs.gbx.zarr._store import ManifestStore

    w = plan.write_id
    recs = []
    writers: dict[str, PackWriter] = {}
    try:
        for patch in plan.patches:
            recs += _patch_rows(
                spark,
                plan,
                snap,
                docs,
                patch,
                writers,
                rebase,
                (FuseRangeReader, open_manifest_array, read_box, ManifestStore),
            )
        for writer in writers.values():
            writer.close()
    except BaseException:
        for writer in writers.values():
            writer.abort()
        raise
    for r in recs:
        r["write_id"] = w
    return _rows_table(recs)


def _patch_rows(spark, plan, snap, docs, patch, writers, rebase, tools):
    FuseRangeReader, open_manifest_array, read_box, ManifestStore = tools
    meta = plan.arrays[patch.name]
    c = meta.chunks[0]
    end = patch.start + len(patch.values)
    chunks = list(range(patch.start // c, (end - 1) // c + 1))
    keys = [meta.key_encoding.chunk_key((k,)) for k in chunks]
    native = np.dtype(meta.dtype).newbyteorder("=")
    fill = 0 if meta.fill_value is None else meta.fill_value
    current = {k: np.full(meta.chunks, fill, dtype=native) for k in chunks}
    live: dict[str, str] = {}
    shadowed: dict[str, str | None] = (
        {}
    )  # key -> identity of the base object it shadows
    base_meta = None if docs is None else docs.arrays.get(patch.name)
    if snap is not None and base_meta is not None:
        kw = {"keys": keys}
        if base_meta.shard is not None:
            kw = {"shard_keys": sorted({unit_key(base_meta, (k,)) for k in chunks})}
        rows = M.read_rows(
            spark, snap, kinds=["shard", "chunk"], arrays=[patch.name], **kw
        ).to_pylist()
        live = {r["key"]: r["write_id"] for r in rows if r["kind"] == "chunk"}
        shadowed = {r["key"]: r["identity"] for r in rows if r["kind"] == "chunk"}
        store = ManifestStore(
            docs.metas,
            rows,
            FuseRangeReader(plan.root, base_root=plan.base_root),
            {patch.name: base_meta},
        )
        try:
            arr = open_manifest_array(store, patch.name)
            for k in chunks:
                top = min((k + 1) * c, base_meta.shape[0])
                if top > k * c:
                    current[k][: top - k * c] = read_box(arr, (slice(k * c, top),))
        finally:
            store.close()
    cells = np.concatenate([current[k] for k in chunks])[
        patch.start - chunks[0] * c : end - chunks[0] * c
    ]
    if not rebase:
        patch.base_values = cells.copy()
    else:
        theirs = ~_cells_equal(cells, patch.base_values)
        mine = ~_cells_equal(cells, patch.values)
        if (theirs & mine).any():
            key = keys[0]
            raise _conflict(
                [
                    {
                        "kind": "chunk",
                        "array": patch.name,
                        "key": key,
                        "expected_write_id": None,
                        "write_id": live.get(key),
                    }
                ],
                message=f"another write set the {patch.name!r} labels this write sets, "
                "to different values",
            )
    out = []
    for k, key in zip(chunks, keys):
        values = current[k]
        lo, hi = max(patch.start, k * c), min(end, (k + 1) * c)
        values[lo - k * c : hi - k * c] = patch.values[
            lo - patch.start : hi - patch.start
        ]
        data = encode_inner_chunk(meta, values)
        writer = writers.get(patch.name)
        if writer is None:
            writer = writers[patch.name] = PackWriter(
                os.path.join(plan.root, "data", patch.name, plan.write_id),
                reader_root=plan.root,
            )
        offset, length = writer.add(data)
        out.append(
            {
                "kind": "chunk",
                "array": patch.name,
                "key": key,
                "shard_key": unit_key(meta, (k,)) if meta.shard is not None else None,
                "file": writer.path,
                "offset": offset,
                "length": length,
                "checksum": crc32c_hex(data),
                "origin": "gbx",
                "op": "upsert",
                "expected_write_id": live.get(key),
                "identity": shadowed.get(key),
            }
        )
    return out


def _is_metadata(plan: _Plan, kind, array) -> bool:
    return kind in ("array", "group") or (kind == "chunk" and array in plan.coords)


def _conflict(rows: list[dict], message=None) -> ZarrWriteConflict:
    keys = tuple(M._store_key(r["array"], r["key"]) for r in rows)
    return ZarrWriteConflict(
        message or "another write changed the same chunks",
        keys=keys,
        winner=rows[0].get("write_id") if rows else None,
        conflicts=tuple(rows),
        fix="re-read the region and retry, or write disjoint chunks",
    )


def _rebase(spark, plan: _Plan, latest) -> tuple[pa.Table, pa.Table]:
    """Doc and coordinate rows again, over ``latest``.

    - An array this write changes is rebuilt over the document that is live now
      (``merge_array``): the larger shape, and whatever another writer did to its
      attributes, are kept; a different layout, or an attribute both set differently, is a
      conflict. An array it creates takes the latest extents of its dimensions.
    - An append grows every array on its dimension, including ones created meanwhile.
    - Coordinate chunks are read-modified-written over the latest ones.
    - Every metadata expectation is the latest write_id.
    """
    docs = _docs_at(spark, latest)
    latest_ext = _extents_of(docs.arrays)
    if plan.planned is None:  # the first rebase: the arrays still are as planned
        plan.planned = dict(plan.arrays)
    base_arrays = plan.docs.arrays if plan.docs is not None else {}

    def merged(name: str, shape) -> ArrayMeta:
        theirs, mine = docs.arrays.get(name), plan.planned.get(name)
        if theirs is None:
            return with_shape(mine, shape)
        if mine is None:  # created by another write, not planned by this one
            return with_shape(theirs, shape)
        return merge_array(base_arrays.get(name), mine, theirs, shape)

    for name in list(plan.changed):
        cur, theirs = plan.arrays[name], docs.arrays.get(name)
        same_rank = theirs is not None and len(theirs.shape) == len(cur.shape)
        shape = tuple(
            max(
                n,
                theirs.shape[i] if same_rank else 0,  # another rank: a layout conflict
                latest_ext.get(d, 0) if name in plan.created else 0,
            )
            for i, (d, n) in enumerate(zip(cur.dims, cur.shape))
        )
        plan.arrays[name] = merged(name, shape)
    for patch in plan.patches:
        mine, theirs = plan.arrays[patch.name], docs.arrays.get(patch.name)
        if theirs is not None and theirs.shape[0] > mine.shape[0]:
            plan.arrays[patch.name] = with_shape(mine, theirs.shape)
    if plan.mode == "append" and plan.append_dim:
        d = plan.append_dim
        target = max(plan.extents.get(d, 0), latest_ext.get(d, 0))
        for name, theirs in docs.arrays.items():
            if d not in theirs.dims:
                continue
            cur = plan.arrays.get(name, theirs)
            shape = tuple(
                max(a, b, target if dim == d else 0)
                for dim, a, b in zip(theirs.dims, cur.shape, theirs.shape)
            )
            if name not in plan.arrays or shape != cur.shape or name in plan.changed:
                plan.arrays[name] = merged(name, shape)
            if shape != theirs.shape and name not in plan.changed:
                plan.changed.append(name)
    return (
        _doc_commit_rows(plan, docs.ids),
        _coordinate_rows(spark, plan, latest, docs, rebase=True),
    )


def _pause(seconds: float) -> None:
    """The jittered wait between commit attempts (tests replace it)."""
    time.sleep(seconds)


def _wait(attempt: int) -> float:
    """Seconds to wait after losing attempt number ``attempt`` (from 0): jittered, doubling
    each time, up to ``_MAX_WAIT_SECONDS``."""
    return random.uniform(0.2, 1.0) * min(2**attempt, _MAX_WAIT_SECONDS)


def _commit(spark, plan: _Plan, data_rows: pa.Table, stats: dict) -> int:
    """Phase 2: the CAS commit, rebasing metadata at most ``_ATTEMPTS`` times."""
    ids = plan.docs.ids if plan.docs is not None else {}
    docs_rows = _doc_commit_rows(plan, ids)
    coord_rows = _coordinate_rows(spark, plan, plan.base, plan.docs)
    meta = {
        "mode": plan.mode,
        "write_id": plan.write_id,
        "variables": sorted(stats.pop("variables", set())),
        "region": {d: list(v) for d, v in sorted(plan.region.items())},
        "user": _current_user(spark),
        **stats,
    }
    sharded = {n: m.shard is not None for n, m in plan.arrays.items()}
    if plan.mode == "create":
        rows = pa.concat_tables([docs_rows, coord_rows, data_rows])
        return M.commit(
            spark, plan.table, rows, write_id=plan.write_id, meta=meta, sharded=sharded
        )
    base = plan.base
    for attempt in range(_ATTEMPTS):
        rows = pa.concat_tables([docs_rows, coord_rows, data_rows])
        latest = M.snapshot(spark, plan.table)
        if latest.version != plan.base.version:
            stale = M.changed_since(spark, plan.table, base, rows).to_pylist()
            data_stale = [
                r for r in stale if not _is_metadata(plan, r["kind"], r["array"])
            ]
            if data_stale:
                raise _conflict(data_stale)
            docs_rows, coord_rows = _rebase(spark, plan, latest)
            sharded = {n: m.shard is not None for n, m in plan.arrays.items()}
            rows = pa.concat_tables([docs_rows, coord_rows, data_rows])
        try:
            return M.commit(
                spark,
                plan.table,
                rows,
                write_id=plan.write_id,
                meta=meta,
                sharded=sharded,
            )
        except M.ConcurrentCommit:
            _pause(_wait(attempt))
            base = latest
        except ZarrWriteConflict as e:
            found = list(e.conflicts)
            data = [r for r in found if not _is_metadata(plan, r["kind"], r["array"])]
            if data:
                raise _conflict(data) from e
            if not found:
                raise
            _pause(_wait(attempt))
            base = latest
    raise ZarrWriteConflict(
        f"gave up after {_ATTEMPTS} attempts",
        fix="retry later or reduce concurrent writers to this array",
    )


def _write(spark, plan: _Plan) -> int:
    """Files, then the commit."""
    rows = _write_files(spark, plan)
    if _BEFORE_COMMIT is not None:
        _BEFORE_COMMIT(plan.write_id)
    side = _side_info(rows)
    if plan.digest is not None and (side["count"], side["hash"]) != tuple(plan.digest):
        raise ZarrError(
            f"the write pass saw {side['count']} block(s), not the {plan.digest[0]} its "
            "plan was made from, or saw them at other positions",
            fix=_CHANGED_INPUT_FIX,
        )
    plan.region = {d: list(v) for d, v in side["region"].items()}
    for patch in plan.patches:  # an xarray input's coordinates
        lo_hi = plan.region.setdefault(patch.name, [patch.start, patch.start])
        lo_hi[0] = min(lo_hi[0], patch.start)
        lo_hi[1] = max(lo_hi[1], patch.start + len(patch.values))
    _apply_coordinates(plan, side["coords"])
    written = set(side["variables"]) | set(plan.dataset_vars)
    data_rows = _commit_data_rows(plan, rows, written)
    chunk_rows = rows.filter(pc.equal(rows.column("rtype"), "chunk"))
    stats = {
        "chunks": chunk_rows.num_rows,
        "rmw": int(
            pc.sum(
                pc.fill_null(chunk_rows.column("rmw"), False).cast(pa.int64())
            ).as_py()
            or 0
        ),
        "variables": written | {p.name for p in plan.patches},
    }
    return _commit(spark, plan, data_rows, stats)


# --- the public function ---------------------------------------------------------------


def _session(data, spark):
    if spark is not None:
        return spark
    if _is_frame(data):
        return data.sparkSession
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is None:
        raise ValueError(
            "an xarray.Dataset is written through a Spark session. Fix: pass spark=spark."
        )
    return active


def _check_new_root(root: str, owner: str) -> None:
    """``create`` makes a new data root (spec 8.1): an existing root is refused unless it
    is empty or holds this table's own marker (a create of this table that stopped
    before its commit)."""
    from databricks.labs.gbx.core._retry import _retry_transient
    from databricks.labs.gbx.zarr._register import OWNER_MARKER, _check_owner

    if not os.path.exists(root):
        return
    if not os.path.isdir(root):
        raise ZarrOwnershipError(
            f"{root} exists and is not a directory",
            fix="choose a new root for the store",
        )
    marker = os.path.join(root, OWNER_MARKER)
    if os.path.exists(marker):
        _check_owner(marker, owner, "managed")  # another table's: refused
        return
    entries = _retry_transient(lambda: sorted(os.listdir(root)))
    if entries:
        shown = ", ".join(entries[:3]) + (" ..." if len(entries) > 3 else "")
        raise ZarrOwnershipError(
            f"{root} already exists and is not empty ({shown}); create makes a new store "
            "and never writes into an existing directory",
            fix="choose a new or empty root, or register an existing Zarr store there "
            "with gbx_zarr_register",
        )


def _adoptable(spark, table: str, root: str, owner: str) -> bool:
    """Whether an existing ``table`` is this create's own, left empty by a crash: a
    managed manifest of this root, with no commit, whose root marker names it."""
    from databricks.labs.gbx.zarr._register import check_owner_marker

    try:
        props = M.read_props(spark, table)
    except ZarrError:
        return False
    if props.kind != "managed" or os.path.normpath(props.root) != os.path.normpath(
        root
    ):
        return False
    committed = spark.sql(
        f"SELECT 1 FROM {M._q(table)} WHERE kind = 'commit' LIMIT 1"
    ).first()
    if committed is not None:
        return False
    try:
        check_owner_marker(root, owner, "managed")
    except ZarrError:
        return False
    return True


def gbx_zarr_write(
    data,
    target: str,
    *,
    mode: str = "create",
    root: str | None = None,
    variables=None,
    append_dim=None,
    dim_cols=None,
    chunks="balanced",
    shards=None,
    codecs=None,
    file_mode: str = "auto",
    retain: str = "7 days",
    write_id: str | None = None,
    access: str = "auto",
    optimize: bool = False,
    spark=None,
) -> int:
    """Write ``data`` to the Zarr store whose manifest table is ``target``; returns the
    store version (the Delta version) the write committed.

    ``data`` is a blocks DataFrame (``BLOCKS_SCHEMA``, as ``gbx_zarr_read`` returns it), a
    DataFrame with one GeoBrix tile column (below), or an ``xarray.Dataset``
    (driver-sized). Blocks with ``decode=True`` values are CF-encoded
    with the array's attributes (times to its units, packed values to their integer type,
    NaN to the fill value); blocks that still carry the array's packing attributes
    (``decode=False``) are written as the stored values they are.

    ``mode``:

    - ``"create"``: a new managed store, table ``target`` and data root ``root``. The root
      must not exist, or be empty; it gets an owner marker naming the table. ``chunks``
      (``"maps"``, ``"series"``, ``"balanced"`` or a dict of dimension to size), ``shards``
      (``None`` computes them, a dict sets some, ``False`` means none) and ``codecs`` (the
      inner chunk's codec chain, as Zarr v3 codec documents or zarr codec objects; default
      ``bytes`` + ``zstd`` level 3 + ``crc32c``) lay out each array; coordinates are one
      unsharded chunk. ``file_mode`` and ``retain`` are the store's.
    - ``"append"``: extend along ``append_dim`` (default: the first dimension labelled by
      CF times). The input must carry that dimension's labels, all after the store's last;
      on a regular axis they land at their step (so appends may arrive out of order), else
      at the current length.
    - ``"update"``: write into the existing extent. Inner chunks the input covers whole are
      encoded without reading; partly covered ones are read at the base version, patched
      and re-encoded. Blocks are placed by their ``start``/``stop``; an ``xarray.Dataset``
      by its coordinate labels, which may also be steps an out-of-order append left
      unlabelled.
    - ``"overwrite"``: replace the named variables: chunks the input does not cover read as
      the fill value. Older versions stay readable until vacuumed.

    A variable the store lacks is created on the store's dimensions with the default
    layout and codecs; from blocks in an update or overwrite, name it in ``variables``.
    A registered store takes writes only when registered with ``writable=True``; they go
    to its overlay root, in the base arrays' own key encoding, and the base is never
    touched.

    **Tiles.** A DataFrame with a tile column (found by its schema; other columns may
    stay) writes each tile's pixels into the array of its variable, in any mode. A tile cut
    from Zarr by ``gbx_zarr_read(output="tiles")`` still carries its provenance
    (``zarr_array``, ``sliceDims``, ``window``): it goes to the array it came from, whose
    grid (geotransform and CRS, ``crs_equal``) must be the plane it was cut from, else
    ``ZarrGridMismatch``; its other positions are its ``sliceDims``. Most pixel-producing
    ``rst_*`` functions return a fresh tile without provenance; so does a stack of
    GeoTIFFs. Such a tile needs ``dim_cols`` (``{dimension: column}``, a number or time
    column) for every non-spatial dimension, and ``variables="<name>"`` for its variable
    (a new store defaults to ``"data"``). ``dim_cols`` also overrides ``sliceDims``. Tiles
    are placed by their decoded geotransform against the array's grid (so a clip or a
    change of NoData keeps its place); a null tile is skipped.

    - ``create`` derives the grid from the tiles: their CRS, cell size and the extent of
      all of them. A tile with another CRS, another cell size or an origin more than 1e-6
      of a cell off the cell edges raises ``ZarrGridMismatch``. Each ``dim_cols`` column
      gives the sorted labels of its dimension. The latitude/longitude (or ``y``/``x``)
      labels come from the grid; a CRS other than EPSG:4326 is carried by a
      ``spatial_ref`` grid-mapping variable, so ``gbx_zarr_read(output="tiles")`` reads
      the CRS back.
    - ``append`` takes the new labels of the ``append_dim`` (default: the ``dim_cols``
      dimension labelled by CF times) from its column; the other ``dim_cols`` values must
      be labels the store has.
    - ``update`` and ``overwrite`` look the ``dim_cols`` values up in the store's
      coordinates.

    Values keep the tile's type in a new store. In an existing array they are cast to the
    array's type (floats to integers round half to even) and the tile's NoData becomes the
    array's ``_FillValue``. Tiles with a pending clip or warp are decoded to know their
    grid. A tile that does not fit is refused on the driver, before any file is written.

    ``write_id`` (32 lowercase hex characters; default a new ``uuid4().hex``) makes a write
    idempotent: writing again with a ``write_id`` that committed returns its version and
    writes nothing.

    One MERGE commits a write, so it is visible whole or not at all. A concurrent write
    that changed the same chunks since this one read them (or a shard a chunk was read
    from, or a coordinate label to another value) raises ``ZarrWriteConflict``, and nothing
    of this write is visible. One that changed only metadata is rebased and the commit
    retried, at most twenty times (every lost round means another writer landed, so up to
    twenty simultaneous appenders to one array all land). Examples are another append's
    shape, a shared coordinate chunk, or an array created meanwhile.

    A blocks DataFrame is read once for ``update`` and ``overwrite``. ``create`` and
    ``append`` first read its block positions (never the ``data`` column; an append also
    reads its dimension's coordinate blocks), then write; an input that changes in between
    is refused, so give them a deterministic input (a table is best). Errors raised while
    Spark runs the write arrive as ``pyspark.errors.PythonException`` with the GeoBrix
    error name and its ``Fix:`` line in the message; a failed write commits nothing.
    ``access`` is ``"auto"`` or ``"fuse"``; ``optimize`` arrives with
    ``gbx_zarr_optimize``.
    """
    from databricks.labs.gbx.zarr._filemode import resolve as resolve_file_mode
    from databricks.labs.gbx.zarr._register import (
        _claim_marker,
        _discard,
        _local_root,
        _release_marker,
        _table_exists,
        qualified_table_name,
    )

    if mode not in _MODES:
        raise ValueError(f"mode must be one of {_MODES}, got {mode!r}. Fix: pass one.")
    if access not in _ACCESS:
        raise ValueError(
            f"access must be one of {_ACCESS}, got {access!r}. Fix: pass 'auto' or 'fuse'."
        )
    if access == "direct":
        raise ZarrAccessUnavailable(
            "direct storage access is not available yet: files are written through the "
            "Volume mount",
            fix='use access="auto" or "fuse"',
        )
    if optimize:
        raise ZarrUnsupported(
            "optimize=True folds chunk rows into shards, which arrives with "
            "gbx_zarr_optimize",
            fix="write with optimize=False",
        )
    if mode == "create":
        if not root:
            raise ValueError(
                "mode='create' needs root, the directory the store's files go to. Fix: "
                "pass root='/Volumes/<catalog>/<schema>/<volume>/<dir>'."
            )
        M.retain_seconds(retain)
        _codec_chain(codecs)
    else:
        given = [
            n
            for n, v, default in (
                ("root", root, None),
                ("chunks", chunks, "balanced"),
                ("shards", shards, None),
                ("codecs", codecs, None),
                ("file_mode", file_mode, "auto"),
                ("retain", retain, "7 days"),
            )
            if v != default
        ]
        if given:
            raise ValueError(
                f"{', '.join(given)} apply to mode='create' only. Fix: drop them, or "
                "create a new store."
            )
    M._q(target)
    write_id = write_id or uuid.uuid4().hex
    M._check_write_id(write_id)
    if not (_is_xarray(data) or _is_frame(data)):
        raise ValueError(
            f"data must be an xarray.Dataset or a blocks DataFrame, got "
            f"{type(data).__name__}. Fix: pass gbx_zarr_read(output='blocks') output or "
            "an xarray.Dataset."
        )
    spark = _session(data, spark)
    tiles = _tile_input(data, dim_cols)

    exists = _table_exists(spark, target)
    if exists:
        landed = M.find_commit_version(spark, target, write_id)
        if landed is not None:
            return landed
    owner = qualified_table_name(spark, target)
    if mode != "create":
        if not exists:
            raise ZarrError(
                f"{target} does not exist",
                fix="create the store with mode='create' (or register an existing one)",
            )
        props = M.read_props(spark, target)
        plan = _plan_existing(
            spark,
            target,
            write_id,
            mode,
            props,
            data,
            variables,
            append_dim,
            owner,
            tiles,
        )
        return _write(spark, plan)

    data_root = _local_root(root)
    adopt = exists and _adoptable(spark, target, data_root, owner)
    if exists and not adopt:
        raise ZarrOwnershipError(
            f"{target} already exists",
            fix="choose a new table name, or write to it with mode='append', 'update' "
            "or 'overwrite'",
        )
    _check_new_root(data_root, owner)
    fm = resolve_file_mode(spark, file_mode, has_filespace=False)
    inp = _create_input(spark, data, tiles, variables)
    plan = _plan_create(target, write_id, data_root, inp, chunks, shards, codecs)
    claimed = made = False
    try:
        claimed = _claim_marker(data_root, owner, "managed")
        if not adopt:
            M.create_manifest(
                spark,
                target,
                M.StoreProps(
                    kind="managed",
                    root=data_root,
                    overlay_root=None,
                    writable=True,
                    file_mode=fm,
                    retain=retain,
                ),
            )
            made = True
        return _write(spark, plan)
    except BaseException:
        # Only a table this call made, and then dropped, gives its marker back: a table
        # that may hold a commit, or one another call made, relies on it.
        if made and _discard(spark, target, write_id) and claimed:
            _release_marker(data_root, owner, "managed")
        raise
