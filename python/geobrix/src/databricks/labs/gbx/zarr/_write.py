"""``gbx_zarr_write``: write a Zarr store transactionally, one Delta MERGE per write.

A write has two phases.

1. **Files**, on the cluster. The input becomes blocks (an ``xarray.Dataset`` is cut into
   chunk-aligned blocks on the driver). A ``mapInArrow`` cuts each block into the inner
   chunks it touches, and a block that covers a whole chunk is encoded right there.
   ``repartition(N, "variable", "part")`` then brings every piece of one shard (of one
   chunk, for an unsharded array) to one task. The task assembles each chunk; when the input
   covers a chunk only in part it reads the base chunk through a ``ManifestStore`` at the base
   version, patches it and re-encodes it (read-modify-write). It appends the encoded chunks
   to its pack file (``_pack.PackWriter``) and returns one small manifest row per chunk.
2. **Commit**, on the driver. The rows are collected and matched with the base snapshot:
   a replaced key carries the ``write_id`` it had there (the compare-and-swap
   expectation), a new key the expectation "must not exist". The array documents the write
   changes (a shape, a new variable) and the dimension-coordinate chunks, rewritten on the
   driver, are metadata. One MERGE commits all of it (``_manifest.commit``), so a write is
   all or nothing.

Before each MERGE the writer compares its keys with the latest version
(``changed_since``). A data chunk that another writer changed since the base raises
``ZarrWriteConflict`` and nothing of this write becomes visible. A change to metadata alone
is rebased: shapes become the maximum extent, coordinate chunks are read-modified-written
again over the latest version, the expectations are refreshed and the MERGE is retried, at
most five times with jitter. Data files are never rewritten.

A chunk whose every cell is the fill value is not stored, as zarr does by default
(``write_empty_chunks=False``): a new store gets no row for it, and an existing one deletes
the chunk's row, or writes a tombstone when a live shard row still holds the chunk. A chunk
with neither is written like any other, so the commit still checks that no other write
created that key in the meantime (dropping it would lose that check).

Everything here is Spark Connect safe: no ``sparkContext``, ``.rdd``, ``_jvm``, ``_jsc``
or ``spark.conf.set``; the session's parallelism is read through a guarded
``spark.conf.get``.
"""

from __future__ import annotations

import itertools
import json
import math
import os
import random
import time
import uuid
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr._keys import shard_of
from databricks.labs.gbx.zarr._layout import chunk_layout, coordinate_chunks
from databricks.labs.gbx.zarr._meta import (
    ArrayMeta,
    cf_encode,
    decode_coord,
    is_time_coord,
    parse_v2,
    parse_v3,
    with_shape,
)
from databricks.labs.gbx.zarr._pack import (
    PackWriter,
    crc32c_hex,
    encode_inner_chunk,
    is_fill,
    pad_to_chunk,
)
from databricks.labs.gbx.zarr._plan import unit_key
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

_MODES = ("create", "append", "update", "overwrite")
_ACCESS = ("auto", "fuse", "direct")
_BLOCK_COLUMNS = ("variable", "dims", "start", "stop", "dtype", "shape", "data")
_GROUP_BYTES = (
    64 << 20
)  # an xarray input reaches Spark in createDataFrame calls of this
_PACK_ROLL_BYTES = 512 << 20  # a task starts a new pack once its pack reaches this
_BATCH_BYTES = 32 << 20  # pieces and rows: Arrow batches of about this many bytes
_BATCH_ROWS = 256
_PARTS_PER_CORE = 4
_ATTEMPTS = 5
_CF_ATTRS = ("scale_factor", "add_offset", "_FillValue", "missing_value")
_TIME_ATTRS = ("units", "calendar")

# Test hooks, never set in production. _FAULT = (array, chunk key): the task that encodes
# that chunk raises (it travels to the task in the closure, read when the write is
# planned). _BEFORE_COMMIT: called with the write_id on the driver after the files are
# written and before the commit, to let another write land in between.
_FAULT = None
_BEFORE_COMMIT = None

_PIECES_DDL = (
    "variable STRING, key STRING, part STRING, shard_key STRING, chunk ARRAY<BIGINT>, "
    "kind STRING, lo ARRAY<BIGINT>, shape ARRAY<BIGINT>, data BINARY, empty BOOLEAN"
)
_PIECES_ARROW = pa.schema(
    [
        ("variable", pa.string()),
        ("key", pa.string()),
        ("part", pa.string()),
        ("shard_key", pa.string()),
        ("chunk", pa.list_(pa.int64())),
        ("kind", pa.string()),
        ("lo", pa.list_(pa.int64())),
        ("shape", pa.list_(pa.int64())),
        ("data", pa.binary()),
        ("empty", pa.bool_()),
    ]
)
_ROWS_DDL = (
    "array STRING, key STRING, shard_key STRING, file STRING, offset BIGINT, "
    "length BIGINT, checksum STRING, empty BOOLEAN"
)
_ROWS_ARROW = pa.schema(
    [
        ("array", pa.string()),
        ("key", pa.string()),
        ("shard_key", pa.string()),
        ("file", pa.string()),
        ("offset", pa.int64()),
        ("length", pa.int64()),
        ("checksum", pa.string()),
        ("empty", pa.bool_()),
    ]
)


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


def _parallelism(spark) -> int:
    """The session's default parallelism, read without ``sparkContext`` (Connect has none):
    ``spark.default.parallelism``, else ``spark.sql.shuffle.partitions``, else 8. Never sets
    anything."""
    for key in ("spark.default.parallelism", "spark.sql.shuffle.partitions"):
        try:
            value = spark.conf.get(key, None)
        except Exception:  # Serverless refuses some keys
            continue
        try:
            n = int(value)
        except (TypeError, ValueError):
            continue
        if n > 0:
            return n
    return 8


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


def _store_key(array, key):
    return "/".join(p for p in (array, key) if p)


def _block_is_stored(attrs_text, meta: ArrayMeta) -> bool:
    """Whether a block holds stored values: ``gbx_zarr_read(decode=True)`` drops the CF
    attributes its decoding consumed, ``decode=False`` keeps them. A block that still
    carries one of the array's packing or fill attributes (or a time coordinate's units)
    is therefore stored values; anything else is decoded values, to be encoded."""
    if not attrs_text:
        return False
    try:
        attrs = json.loads(attrs_text)
    except ValueError:
        return False
    if not isinstance(attrs, dict):
        return False
    keys = [k for k in _CF_ATTRS if k in meta.attrs]
    if is_time_coord(meta):
        keys += [k for k in _TIME_ATTRS if k in meta.attrs]
    return any(k in attrs for k in keys)


# --- array documents -------------------------------------------------------------------


def _codec_objects(codecs):
    """``(serializer, compressors)`` for zarr: GeoBrix's defaults, or the given chain."""
    from zarr.codecs import BytesCodec, Crc32cCodec, ZstdCodec

    if codecs is None:
        return BytesCodec(endian="little"), [ZstdCodec(level=3), Crc32cCodec()]
    return None, None


def _v3_doc(name, dims, shape, dtype, chunks, shards, fill, attrs, codecs) -> dict:
    """The ``zarr.json`` of a new array, made by zarr itself so it is valid."""
    import zarr
    from zarr.storage import MemoryStore

    serializer, compressors = _codec_objects(codecs)
    store = MemoryStore()
    kw = {}
    if serializer is not None:
        kw.update(serializer=serializer, compressors=compressors)
    zarr.create_array(
        store,
        shape=tuple(shape),
        chunks=tuple(chunks) if chunks else None,
        shards=tuple(shards) if shards else None,
        dtype=np.dtype(dtype),
        fill_value=fill,
        attributes=_jsonable(attrs),
        dimension_names=tuple(dims),
        zarr_format=3,
        **kw,
    )
    doc = json.loads(store._store_dict["zarr.json"].to_bytes())
    if codecs is not None:  # the caller's chain for each inner chunk
        chain = [dict(c) for c in codecs]
        if shards:
            doc["codecs"][0]["configuration"]["codecs"] = chain
        else:
            doc["codecs"] = chain
    return doc


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
    laid out by ``chunks`` and ``shards`` (``_layout``)."""
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
    return parse_v3(spec.name, doc)


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


def _doc_rows(meta: ArrayMeta) -> dict[str, str]:
    """The manifest documents of an array: ``{key: text}``."""
    if meta.zarr_format == 3:
        return {"zarr.json": json.dumps(meta.doc, sort_keys=True)}
    return {
        ".zarray": json.dumps(meta.doc[".zarray"], sort_keys=True),
        ".zattrs": json.dumps(meta.doc[".zattrs"], sort_keys=True),
    }


def _same_layout(a: ArrayMeta, b: ArrayMeta) -> bool:
    """Everything but the shape: what a rebase may not change under a write."""

    def strip(meta):
        doc = json.loads(json.dumps(meta.doc))
        if meta.zarr_format == 3:
            doc.pop("shape", None)
            doc.pop("attributes", None)
        else:
            doc[".zarray"].pop("shape", None)
            doc.pop(".zattrs", None)
        return doc

    return (a.zarr_format, a.dims, a.dtype, a.chunks, a.shards) == (
        b.zarr_format,
        b.dims,
        b.dtype,
        b.chunks,
        b.shards,
    ) and strip(a) == strip(b)


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
    values: np.ndarray | None = None  # xarray variables, and every coordinate
    stored: bool = False  # the values are stored values (a coordinate's blocks)
    labels: dict = field(default_factory=dict)  # xarray: dim -> the dataset's labels
    encoding: dict | None = None  # xarray: the variable's CF encoding (None for blocks)

    @property
    def coordinate(self) -> bool:
        return len(self.dims) == 1 and self.dims[0] == self.name


@dataclass
class _Input:
    kind: str  # "xarray" | "blocks"
    vars: dict[str, _Var]
    attrs: dict  # xarray: the dataset attributes
    frame: object = None  # blocks: the data blocks (no coordinates)
    dataset: object = None


def _input_of(data, variables, spark) -> _Input:
    if _is_xarray(data):
        return _xarray_input(data, variables)
    if _is_frame(data):
        if "tile" in data.columns:
            raise ZarrUnsupported(
                "writing GeoBrix tiles arrives with the tile reader",
                fix="write blocks (gbx_zarr_read output='blocks') or an xarray.Dataset",
            )
        missing = [c for c in _BLOCK_COLUMNS if c not in data.columns]
        if missing:
            raise ValueError(
                f"the DataFrame lacks the blocks column(s) {missing}. Fix: pass the "
                "output of gbx_zarr_read(output='blocks'), or a DataFrame in BLOCKS_SCHEMA."
            )
        return _blocks_input(data, variables)
    raise ValueError(
        f"data must be an xarray.Dataset or a blocks DataFrame, got {type(data).__name__}. "
        "Fix: pass gbx_zarr_read(output='blocks') output or an xarray.Dataset."
    )


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
    for name in chosen:
        var = ds[name].variable
        dims = tuple(str(d) for d in var.dims)
        dims_needed.update(dims)
        values = np.asarray(var.values)
        out[name] = _Var(
            name,
            dims,
            values.dtype,
            dict(var.attrs),
            None,
            (0,) * len(dims),
            tuple(values.shape),
            np.zeros((1, len(dims)), dtype=np.int64),
            np.array([values.shape], dtype=np.int64).reshape(1, len(dims)),
            values,
            encoding=_cf_encoding(var.encoding),
        )
    labels = {}
    for d in sorted(dims_needed):
        if d in ds.coords:
            coord = ds.coords[d].variable
            values = np.asarray(coord.values)
            labels[d] = values
            out[d] = _Var(
                d,
                (d,),
                values.dtype,
                dict(coord.attrs),
                None,
                (0,),
                (len(values),),
                np.zeros((1, 1), dtype=np.int64),
                np.array([[len(values)]], dtype=np.int64),
                values,
                encoding=_cf_encoding(coord.encoding),
            )
    for v in out.values():
        v.labels = {d: labels[d] for d in v.dims if d in labels}
    return _Input("xarray", out, dict(ds.attrs), dataset=ds)


def _coordinate_condition():
    from pyspark.sql import functions as F

    return (F.size("dims") == 1) & (F.element_at("dims", 1) == F.col("variable"))


def _blocks_input(df, variables) -> _Input:
    """The variables of a blocks DataFrame, from one pass over its block positions.

    Only the coordinate blocks (one-dimensional, named like their dimension) bring their
    data to the driver; every other block brings its position, type and attributes. The
    data blocks stay in Spark for the write (so a lazily computed input is evaluated twice:
    once here, once when it is written).
    """
    from pyspark.sql import functions as F

    cond = _coordinate_condition()
    cols = [F.col(c) for c in ("variable", "dims", "start", "stop", "dtype", "shape")]
    for opt in ("attrs", "fill_value"):
        cols.append(
            F.col(opt) if opt in df.columns else F.lit(None).cast("string").alias(opt)
        )
    cols.append(F.when(cond, F.col("data")).alias("data"))
    seen = df.select(*cols).toArrow()
    if not seen.num_rows:
        raise ValueError(
            "the blocks DataFrame is empty. Fix: pass the blocks to write."
        )
    by_name = {}
    for name in pc.unique(seen.column("variable")).to_pylist():
        if name is None:
            raise ValueError(
                "a block has no variable name. Fix: name the variable of every block."
            )
        by_name[name] = seen.filter(pc.equal(seen.column("variable"), name))
    coord_names = {
        n for n, sub in by_name.items() if (sub.column("dims")[0].as_py() or []) == [n]
    }
    data_names = [n for n in by_name if n not in coord_names]
    chosen = _wanted(sorted(data_names) + sorted(coord_names), variables)
    keep_dims = {d for n in chosen for d in by_name[n].column("dims")[0].as_py() or ()}
    chosen += [c for c in sorted(coord_names) if c in keep_dims and c not in chosen]
    out = {}
    for name in chosen:
        out[name] = _summarize_blocks(name, by_name[name], name in coord_names)
    written = [n for n in chosen if n not in coord_names]
    frame = df.where(~cond & F.col("variable").isin(written)) if written else None
    return _Input("blocks", out, {}, frame=frame)


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


def _summarize_blocks(name, sub: pa.Table, coordinate) -> _Var:
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
    attrs = {}
    if first.get("attrs"):
        try:
            parsed = json.loads(first["attrs"])
            attrs = parsed if isinstance(parsed, dict) else {}
        except ValueError:
            attrs = {}
    var = _Var(
        name,
        dims,
        dtype,
        attrs,
        _fill_from_text(first.get("fill_value")),
        lo,
        hi,
        starts,
        stops,
    )
    if coordinate:
        values = np.empty(hi[0] - lo[0], dtype=dtype.newbyteorder("="))
        seen = np.zeros(hi[0] - lo[0], dtype=bool)
        data = sub.column("data")
        for i in range(sub.num_rows):
            a, b = int(starts[i, 0]) - lo[0], int(stops[i, 0]) - lo[0]
            values[a:b] = np.frombuffer(data[i].as_buffer(), dtype=dtype)
            seen[a:b] = True
        if not seen.all():
            raise ValueError(
                f"the blocks of the coordinate {name!r} leave gaps between {lo[0]} and "
                f"{hi[0]}. Fix: write the coordinate's labels whole."
            )
        var.values = values
        var.stored = any(k in attrs for k in _CF_ATTRS + _TIME_ATTRS)
    return var


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
            metas[_store_key(row["array"], row["key"])] = row["meta"].encode()
    arrays, unsupported = parse_documents(metas)
    return _Docs(metas, arrays, unsupported, ids)


@dataclass
class _Patch:
    """Labels of a dimension coordinate this write puts at ``start`` (stored values)."""

    name: str
    start: int
    values: np.ndarray


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
    changed: list[str]  # arrays whose documents this write writes
    groups: list[str]  # group paths a new store needs ("" is the root)
    group_attrs: dict
    coords: set[str]  # dimension-coordinate arrays of the store
    patches: list[_Patch]
    data: list[str]  # data variables written through Spark
    offsets: dict[str, int]  # dim -> added to an input position
    boxes: dict[str, tuple]  # data variable -> (starts, stops) in target positions
    region: dict[str, list[int]]
    frame: object = None  # the blocks to write (data variables only)
    fmt: int = 3
    dataset_vars: dict = field(default_factory=dict)  # xarray: the data variables


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


def _derive_append_start(dim, base_vals, new_vals) -> int:
    """Where along ``dim`` the appended labels go: after the base, on its regular step.

    On a regular axis (equal steps between its labels; missing labels are skipped) the new
    labels are placed by value, so an append may skip ahead and leave steps unwritten for a
    later append to fill (appends may land out of order). Otherwise, or with fewer than two
    labels to tell the step from, they start at the current length. Times and integers are
    compared exactly.
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
            "steps."
        )
    step = _axis_step(b[valid], valid, exact) if valid.size >= 2 else None
    if step is None and valid.size == 1 and len(x) >= 2:
        step = _axis_step(x, np.arange(len(x)), exact)
    if step is None or not valid.size:
        return n
    if len(x) > 1:
        diffs = np.diff(x)
        on = (
            (diffs == step).all()
            if exact
            else np.allclose(diffs, step, rtol=1e-9, atol=0)
        )
        if not on:
            raise ZarrGridMismatch(
                f"the {dim!r} labels to append are not spaced like the store's",
                fix=f"append {dim!r} labels on the store's step",
            )
    off = x[0] - b[valid[0]]
    if exact:
        if off % step:
            at = None
        else:
            at = int(valid[0] + off // step)
    else:
        pos = valid[0] + off / step
        at = int(round(pos)) if abs(pos - round(pos)) <= 1e-6 else None
    if at is None or at < n:
        raise ZarrGridMismatch(
            f"the first {dim!r} label to append, {new_vals[0]}, does not fall on a step "
            "of the store's axis after its end",
            fix=f"append {dim!r} labels on the store's step",
        )
    return at


def _locate(dim, labels, coord_values, size_hint) -> int:
    """The index where ``labels`` start along ``dim`` in the store's coordinate."""
    labels = np.asarray(labels)
    coord = np.asarray(coord_values)
    n = len(labels)
    if coord.dtype.kind == "M" or labels.dtype.kind == "M":
        a = coord.astype("datetime64[ns]")
        b = labels.astype("datetime64[ns]")
        hits = np.flatnonzero(a == b[0]) if n else np.array([0])
        match = lambda i: np.array_equal(a[i : i + n], b)  # noqa: E731
    elif coord.dtype.kind in "fc" or labels.dtype.kind in "fc":
        step = (
            float(np.abs(np.diff(coord.astype("float64"))).min())
            if len(coord) > 1
            else 1.0
        )
        tol = 1e-6 * max(step, 1e-12)
        hits = np.flatnonzero(np.abs(coord - labels[0]) <= tol) if n else np.array([0])
        match = lambda i: len(coord[i : i + n]) == n and np.allclose(  # noqa: E731
            coord[i : i + n], labels, rtol=0, atol=tol
        )
    else:
        hits = np.flatnonzero(coord == labels[0]) if n else np.array([0])
        match = lambda i: np.array_equal(coord[i : i + n], labels)  # noqa: E731
    for i in hits.tolist():
        if match(i):
            return int(i)
    raise ZarrGridMismatch(
        f"the input's {dim!r} labels ({labels[0] if n else ''} ... {labels[-1] if n else ''}, "
        f"{n} of them) are not a contiguous run of the store's {dim!r} coordinate",
        fix=f'write labels {dim!r} that the store has, in its order, or use mode="append" '
        "to add new ones",
    )


def _decoded_coords(spark, plan_docs: _Docs, base, root, base_root, names) -> dict:
    """The decoded labels of the store's coordinate arrays ``names`` at ``base``."""
    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box
    from databricks.labs.gbx.zarr._store import ManifestStore

    names = [n for n in names if n in plan_docs.arrays]
    if not names:
        return {}
    rows = M.read_rows(spark, base, kinds=["shard", "chunk"], arrays=names).to_pylist()
    store = ManifestStore(
        plan_docs.metas,
        rows,
        FuseRangeReader(root, base_root=base_root),
        {n: plan_docs.arrays[n] for n in names},
    )
    out = {}
    try:
        for n in names:
            raw = read_box(open_manifest_array(store, n), (slice(None),))
            out[n] = decode_coord(raw, plan_docs.arrays[n])
    finally:
        store.close()
    return out


def _new_spec(var: _Var, shape, coordinate: bool) -> _NewArray:
    """The stored type and attributes of a variable the write creates.

    An xarray variable is CF-encoded by xarray (on one cell, or the whole of a coordinate or
    a time variable, whose time units xarray infers from the values), which gives its stored
    dtype and its attributes (units, scale factor, fill value). Blocks keep their dtype and
    attributes; their times are encoded the same way.
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


def _plan_create(
    spark, table, write_id, root, inp: _Input, chunks, shards, codecs
) -> _Plan:
    extents: dict[str, int] = {}
    offsets: dict[str, int] = {}
    for v in inp.vars.values():
        for d, a in zip(v.dims, v.lo):
            offsets[d] = min(offsets.get(d, a), a)
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
    arrays = {}
    for v in inp.vars.values():
        shape = tuple(extents[d] for d in v.dims)
        spec = _new_spec(v, shape, v.coordinate)
        arrays[v.name] = _new_meta(
            spec, fmt=3, chunks=chunks, shards=shards, codecs=codecs, separator="/"
        )
    coords = {n for n, v in inp.vars.items() if v.coordinate}
    groups = {""}
    for name in arrays:
        parts = name.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            groups.add("/".join(parts[:i]))
    plan = _Plan(
        mode="create",
        table=table,
        write_id=write_id,
        root=root,
        base_root=None,
        base=None,
        docs=None,
        arrays=arrays,
        changed=sorted(arrays),
        groups=sorted(groups),
        group_attrs=_jsonable(inp.attrs),
        coords=coords,
        patches=[],
        data=[n for n in inp.vars if n not in coords],
        offsets=offsets,
        boxes={},
        region={},
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


def _check_against_store(table, inp: _Input, docs: _Docs, extents) -> None:
    """Every written variable is one the store decodes, on its dimensions in its order;
    a new one lies on dimensions the store has."""
    for v in inp.vars.values():
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


def _append_offset(table, inp: _Input, arrays, coords, extents, labels, append_dim):
    """``(append_dim, offset)``: the dimension an append extends and what moves its input
    positions to the new steps (``_derive_append_start``)."""
    if append_dim is None:
        append_dim = _default_append_dim(inp, arrays, coords)
    if append_dim not in extents:
        raise ValueError(
            f"append_dim {append_dim!r} is not a dimension of {table}, which has "
            f"{sorted(extents)}. Fix: name one of its dimensions."
        )
    cvar = inp.vars.get(append_dim)
    if cvar is None or cvar.values is None:
        raise ValueError(
            f"an append needs the {append_dim!r} labels of the new steps. Fix: include "
            f"the {append_dim!r} coordinate in the input (an xarray coordinate, or "
            "blocks of that variable)."
        )
    new_labels = (
        decode_coord(cvar.values, arrays[append_dim])
        if cvar.stored and append_dim in arrays
        else cvar.values
    )
    base_labels = labels.get(append_dim, np.arange(extents[append_dim]))
    at = _derive_append_start(append_dim, base_labels, new_labels)
    return append_dim, at - cvar.lo[0]


def _extents_after(inp: _Input, offsets, extents, mode, append_dim) -> dict:
    """The length of every dimension after the write; only an append grows one."""
    out = dict(extents)
    for v in inp.vars.values():
        for d, b in zip(v.dims, v.hi):
            reach = b + offsets[d]
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


def _plan_existing(
    spark, table, write_id, mode, props, inp: _Input, append_dim, owner
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
    extents: dict[str, int] = {}
    for m in arrays.values():
        for d, n in zip(m.dims, m.shape):
            extents[d] = max(extents.get(d, 0), n)
    _check_against_store(table, inp, docs, extents)
    # where the input lands: an offset per dimension
    offsets: dict[str, int] = {}
    dims_in = {d for v in inp.vars.values() for d in v.dims}
    store_labels = _decoded_coords(
        spark, docs, base, root, base_root, sorted(c for c in coords if c in dims_in)
    )
    if mode == "append":
        append_dim, offsets[append_dim] = _append_offset(
            table, inp, arrays, coords, extents, store_labels, append_dim
        )
    for d in sorted(dims_in - set(offsets)):
        ref = next((v for v in inp.vars.values() if d in v.labels), None)
        if inp.kind == "xarray" and ref is not None and d in store_labels:
            offsets[d] = _locate(d, ref.labels[d], store_labels[d], extents.get(d))
        else:
            offsets[d] = 0
    # the store after the write
    new_extent = _extents_after(inp, offsets, extents, mode, append_dim)
    changed = []
    for name, meta in list(arrays.items()):
        shape = tuple(new_extent.get(d, n) for d, n in zip(meta.dims, meta.shape))
        if shape != meta.shape:
            arrays[name] = with_shape(meta, shape)
            changed.append(name)
    for v in inp.vars.values():
        if v.name in arrays:
            continue
        spec = _new_spec(v, tuple(new_extent[d] for d in v.dims), v.coordinate)
        arrays[v.name] = _new_meta(
            spec,
            fmt=fmt,
            chunks="balanced",
            shards=None,
            codecs=None,
            separator=separator,
        )
        changed.append(v.name)
        if v.coordinate:
            coords.add(v.name)
    plan = _Plan(
        mode=mode,
        table=table,
        write_id=write_id,
        root=root,
        base_root=base_root,
        base=base,
        docs=docs,
        arrays=arrays,
        changed=sorted(set(changed)),
        groups=[],
        group_attrs={},
        coords=coords,
        patches=[],
        data=[n for n in inp.vars if n not in coords],
        offsets=offsets,
        boxes={},
        region={},
        fmt=fmt,
    )
    _finish_plan(plan, inp, store_labels=store_labels, append_dim=append_dim)
    return plan


def _default_append_dim(inp: _Input, arrays, coords) -> str:
    """The first dimension, among those of the written variables, labelled by CF times."""
    for v in inp.vars.values():
        for d in v.dims:
            if d in coords and d in arrays and is_time_coord(arrays[d]):
                return d
    raise ValueError(
        "append needs append_dim: no dimension of the written variables is labelled by "
        "CF times. Fix: pass append_dim='<dimension>'."
    )


def _same_labels(v: _Var, meta: ArrayMeta, labels, start) -> None:
    """Refuse coordinate blocks beside an append that disagree with the store's labels."""
    mine = decode_coord(v.values, meta) if v.stored else np.asarray(v.values)
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
            f"the input's {v.name!r} labels differ from the store's at the same positions",
            fix=f"append on the store's {v.name!r} grid, or leave {v.name!r} out",
        )


def _finish_plan(plan: _Plan, inp: _Input, *, store_labels=None, append_dim=None):
    """Target boxes, coordinate patches, the region and the blocks frame of a plan."""
    region: dict[str, list[int]] = {}

    def grow(d, a, b):
        lo_hi = region.setdefault(d, [a, b])
        lo_hi[0], lo_hi[1] = min(lo_hi[0], a), max(lo_hi[1], b)

    for v in inp.vars.values():
        meta = plan.arrays[v.name]
        shift = np.array([plan.offsets.get(d, 0) for d in v.dims], dtype=np.int64)
        starts, stops = v.starts + shift, v.stops + shift
        if v.coordinate:
            d = v.dims[0]
            start = v.lo[0] + plan.offsets.get(d, 0)
            known = store_labels is not None and v.name in store_labels
            if plan.mode != "create" and known and d != append_dim:
                if inp.kind == "xarray":
                    continue  # the labels placed the input; they are the store's already
                if plan.mode == "append":
                    _same_labels(v, meta, store_labels[v.name], start)
                    continue  # an unchanged coordinate beside the append
            stored = cf_encode(v.values, meta, stored=v.stored)
            plan.patches.append(_Patch(v.name, start, stored))
            grow(d, start, start + len(stored))
            continue
        shape = np.array(meta.shape, dtype=np.int64)
        outside = (starts < 0).any(axis=1) | (stops > shape).any(axis=1)
        if outside.any():
            i = int(np.flatnonzero(outside)[0])
            raise ZarrGridMismatch(
                f"a block of {v.name!r} spans {tuple(starts[i].tolist())}.."
                f"{tuple(stops[i].tolist())}, outside the array's shape {meta.shape}",
                fix="write blocks within the store's extent",
            )
        for i, d in enumerate(v.dims):
            grow(d, int(starts[:, i].min()), int(stops[:, i].max()))
        plan.boxes[v.name] = (starts, stops)
    plan.region = region
    plan.data = [n for n in plan.data if n in plan.boxes]
    if inp.kind == "blocks":
        plan.frame = inp.frame
    else:
        plan.frame = None  # built on demand, once the layout is known (_xarray_frame)
        plan.dataset_vars = {n: inp.vars[n] for n in plan.data}


# --- phase 1: the files ----------------------------------------------------------------


@dataclass
class _TaskConfig:
    """What the tasks need; travels in the ``mapInArrow`` closures."""

    write_id: str
    root: str
    base_root: str | None
    arrays: dict  # name -> ArrayMeta after the write
    offsets: dict  # dim -> added to an input position
    rmw_docs: dict  # documents at the base of the arrays with partial chunks
    rmw_arrays: dict  # name -> ArrayMeta at the base
    rmw_rows: list  # base rows of the partial chunks
    fault: tuple | None = None


def _chunk_grid(meta: ArrayMeta, start, stop):
    """The chunk coordinates a box ``[start, stop)`` touches."""
    ranges = [
        range(a // c, (b - 1) // c + 1) if b > a else range(0)
        for a, b, c in zip(start, stop, meta.chunks)
    ]
    return itertools.product(*ranges)


def _extent(meta: ArrayMeta, chunk) -> tuple[int, ...]:
    """Cells of ``chunk`` inside the array (an edge chunk has fewer)."""
    return tuple(
        max(0, min(c, n - k * c)) for k, c, n in zip(chunk, meta.chunks, meta.shape)
    )


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
            for chunk in _chunk_grid(meta, start, stop):
                origin = [k * cc for k, cc in zip(chunk, meta.chunks)]
                ext = _extent(meta, chunk)
                cells = 1
                for a, b, o, e in zip(start, stop, origin, ext):
                    cells *= max(0, min(b, o + e) - max(a, o))
                covered[chunk] = covered.get(chunk, 0) + cells
    partial = {k for k, v in covered.items() if v < math.prod(_extent(meta, k))}
    return set(covered), partial


def _part_of(meta: ArrayMeta, chunk) -> tuple[str, str | None]:
    """``(part, shard_key)``: what a chunk is grouped by in Spark, and its shard."""
    if meta.shard is not None:
        key = meta.key_encoding.chunk_key(shard_of(chunk, meta.shard.cps))
        return key, key
    return meta.key_encoding.chunk_key(chunk), None


def _encode(cfg: _TaskConfig, meta: ArrayMeta, key: str, chunk_values) -> bytes:
    if cfg.fault is not None and tuple(cfg.fault) == (meta.name, key):
        raise ZarrError(
            f"test fault while encoding {meta.name}/{key}",
            fix="clear databricks.labs.gbx.zarr._write._FAULT",
        )
    return encode_inner_chunk(meta, chunk_values)


def _batched(rows: list[dict], schema: pa.Schema):
    return pa.RecordBatch.from_pylist(rows, schema=schema)


def _explode_function(cfg: _TaskConfig):
    """Blocks to pieces: one piece per (block, inner chunk it touches)."""

    def run(batches):
        out, held = [], 0
        for batch in batches:
            meta_cols = (
                pa.Table.from_batches([batch])
                .select(
                    ["variable", "dims", "start", "stop", "dtype", "shape"]
                    + (["attrs"] if "attrs" in batch.schema.names else [])
                )
                .to_pylist()
            )
            data = batch.column("data")
            for i, row in enumerate(meta_cols):
                for piece in _pieces(cfg, row, data[i].as_buffer()):
                    out.append(piece)
                    held += len(piece["data"] or b"")
                    if len(out) >= _BATCH_ROWS or held >= _BATCH_BYTES:
                        yield _batched(out, _PIECES_ARROW)
                        out, held = [], 0
        if out:
            yield _batched(out, _PIECES_ARROW)

    return run


def _pieces(cfg: _TaskConfig, row: dict, buf):
    name = row["variable"]
    meta = cfg.arrays.get(name)
    if meta is None:
        raise ZarrError(
            f"the blocks hold a variable {name!r} the write did not plan",
            fix="write again; the input changed between its two passes",
        )
    dtype = np.dtype(row["dtype"])
    shape = tuple(row["shape"])
    if buf is None or len(buf) != math.prod(shape) * dtype.itemsize:
        raise ZarrError(
            f"a block of {name!r} holds {0 if buf is None else len(buf)} bytes, not the "
            f"{math.prod(shape) * dtype.itemsize} its shape {shape} and dtype {dtype} need",
            fix="give each block data of exactly shape x itemsize bytes",
        )
    values = np.frombuffer(buf, dtype=dtype).reshape(shape)
    stored = cf_encode(values, meta, stored=_block_is_stored(row.get("attrs"), meta))
    start = tuple(a + cfg.offsets.get(d, 0) for d, a in zip(meta.dims, row["start"]))
    stop = tuple(a + n for a, n in zip(start, shape))
    for chunk in _chunk_grid(meta, start, stop):
        origin = tuple(k * c for k, c in zip(chunk, meta.chunks))
        ext = _extent(meta, chunk)
        lo = tuple(max(a, o) for a, o in zip(start, origin))
        hi = tuple(min(b, o + e) for b, o, e in zip(stop, origin, ext))
        part = stored[tuple(slice(a - s, b - s) for a, b, s in zip(lo, hi, start))]
        key = meta.key_encoding.chunk_key(chunk)
        group, shard_key = _part_of(meta, chunk)
        rel = [a - o for a, o in zip(lo, origin)]
        whole = (
            all(a == o for a, o in zip(lo, origin))
            and tuple(b - a for a, b in zip(lo, hi)) == ext
        )
        if whole:
            padded = pad_to_chunk(meta, part)
            data = _encode(cfg, meta, key, padded)
            kind, empty = "enc", is_fill(padded, meta.fill_value)
        else:
            data = np.ascontiguousarray(part).tobytes()
            kind, empty = "raw", None
        yield {
            "variable": name,
            "key": key,
            "part": group,
            "shard_key": shard_key,
            "chunk": list(chunk),
            "kind": kind,
            "lo": rel,
            "shape": list(part.shape),
            "data": data,
            "empty": empty,
        }


class _BaseChunks:
    """Reads base chunks for read-modify-write, through a store built on first use."""

    def __init__(self, cfg: _TaskConfig):
        self.cfg, self.store, self.opened = cfg, None, {}

    def read(self, meta: ArrayMeta, chunk) -> np.ndarray:
        """The stored values of ``chunk`` at the base, padded with fill to a whole chunk."""
        from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box

        base = self.cfg.rmw_arrays.get(meta.name)
        native = np.dtype(meta.dtype).newbyteorder("=")
        fill = 0 if meta.fill_value is None else meta.fill_value
        out = np.full(meta.chunks, fill, dtype=native)
        if base is None:
            return out
        origin = [k * c for k, c in zip(chunk, meta.chunks)]
        box = [(o, min(o + c, n)) for o, c, n in zip(origin, meta.chunks, base.shape)]
        if any(b <= a for a, b in box):
            return out
        if self.store is None:
            from databricks.labs.gbx.zarr._access import FuseRangeReader
            from databricks.labs.gbx.zarr._store import ManifestStore

            self.store = ManifestStore(
                self.cfg.rmw_docs,
                self.cfg.rmw_rows,
                FuseRangeReader(self.cfg.root, base_root=self.cfg.base_root),
                self.cfg.rmw_arrays,
            )
        arr = self.opened.get(meta.name)
        if arr is None:
            arr = self.opened[meta.name] = open_manifest_array(self.store, meta.name)
        values = read_box(arr, tuple(slice(a, b) for a, b in box))
        out[tuple(slice(0, b - a) for a, b in box)] = values
        return out

    def close(self):
        if self.store is not None:
            self.store.close()


def _assemble_function(cfg: _TaskConfig):
    """Pieces to encoded chunks in pack files; returns one manifest row per chunk."""

    def run(batches):
        writers: dict[str, PackWriter] = {}
        done: list[PackWriter] = []
        rows: list[dict] = []
        base = _BaseChunks(cfg)
        try:
            current, group = None, []
            for batch in batches:
                for piece in _piece_rows(batch):
                    ident = (piece["variable"], piece["key"])
                    if ident != current and group:
                        rows.append(_assemble(cfg, group, base, writers, done))
                        group = []
                    current = ident
                    group.append(piece)
            if group:
                rows.append(_assemble(cfg, group, base, writers, done))
            for w in list(writers.values()):
                w.close()
            for w in done:
                w.close()
        except BaseException:
            for w in [*writers.values(), *done]:
                w.abort()
            raise
        finally:
            base.close()
        for lo in range(0, len(rows), _BATCH_ROWS * 16):
            yield _batched(rows[lo : lo + _BATCH_ROWS * 16], _ROWS_ARROW)

    return run


def _piece_rows(batch):
    cols = (
        pa.Table.from_batches([batch])
        .select(
            ["variable", "key", "shard_key", "chunk", "kind", "lo", "shape", "empty"]
        )
        .to_pylist()
    )
    data = batch.column("data")
    for i, row in enumerate(cols):
        row["data"] = data[i].as_buffer()
        yield row


def _assemble(cfg, group, base: _BaseChunks, writers, done) -> dict:
    first = group[0]
    name, key = first["variable"], first["key"]
    meta = cfg.arrays[name]
    chunk = tuple(first["chunk"])
    encoded = [p for p in group if p["kind"] == "enc"]
    if encoded:
        if len(group) > 1:
            raise ZarrError(
                f"the input writes chunk {name}/{key} more than once (its blocks overlap)",
                fix="write each cell once: make the blocks of a variable disjoint",
            )
        data, empty = bytes(first["data"]), bool(first["empty"])
    else:
        ext = _extent(meta, chunk)
        seen = np.zeros(ext, dtype=bool)
        for p in group:
            box = tuple(slice(a, a + n) for a, n in zip(p["lo"], p["shape"]))
            if seen[box].any():
                raise ZarrError(
                    f"the input writes cells of chunk {name}/{key} more than once (its "
                    "blocks overlap)",
                    fix="write each cell once: make the blocks of a variable disjoint",
                )
            seen[box] = True
        native = np.dtype(meta.dtype).newbyteorder("=")
        if seen.all():
            fill = 0 if meta.fill_value is None else meta.fill_value
            values = np.full(meta.chunks, fill, dtype=native)
        else:
            values = base.read(meta, chunk)
        for p in group:
            box = tuple(slice(a, a + n) for a, n in zip(p["lo"], p["shape"]))
            values[box] = np.frombuffer(p["data"], dtype=native).reshape(p["shape"])
        empty = is_fill(values, meta.fill_value)
        data = _encode(cfg, meta, key, values)
    writer = writers.get(name)
    if writer is None:
        writer = writers[name] = PackWriter(
            os.path.join(cfg.root, "data", name, cfg.write_id), reader_root=cfg.root
        )
    offset, length = writer.add(data)
    row = {
        "array": name,
        "key": key,
        "shard_key": first["shard_key"],
        "file": writer.path,
        "offset": offset,
        "length": length,
        "checksum": crc32c_hex(data),
        "empty": empty,
    }
    if writer.size >= _PACK_ROLL_BYTES:
        writer.close()
        done.append(writer)
        del writers[name]
    return row


def _xarray_frame(spark, plan: _Plan):
    """An xarray input's data variables as chunk-aligned blocks, in target positions,
    created in Spark ``_GROUP_BYTES`` at a time."""
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    frames, rows, held = [], [], 0
    schema = _blocks_struct()

    def flush():
        table = pa.Table.from_batches(
            [pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)]
        )
        frames.append(spark.createDataFrame(table, schema=schema))

    for name in plan.data:
        var = plan.dataset_vars[name]
        meta = plan.arrays[name]
        values = np.ascontiguousarray(var.values)
        start = tuple(plan.offsets.get(d, 0) for d in var.dims)
        stop = tuple(a + n for a, n in zip(start, values.shape))
        for chunk in _chunk_grid(meta, start, stop):
            origin = tuple(k * c for k, c in zip(chunk, meta.chunks))
            lo = tuple(max(a, o) for a, o in zip(start, origin))
            hi = tuple(min(b, o + c) for b, o, c in zip(stop, origin, meta.chunks))
            block = np.ascontiguousarray(
                values[tuple(slice(a - s, b - s) for a, b, s in zip(lo, hi, start))]
            )
            if not block.dtype.isnative:
                block = block.astype(block.dtype.newbyteorder("="))
            if rows and held + block.nbytes > _GROUP_BYTES:
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


def _base_rows(spark, plan: _Plan, name: str, touched) -> pa.Table | None:
    """The live shard and chunk rows of ``name`` at the base for the chunks ``touched``
    (every row of the array for an overwrite); ``None`` for a new array."""
    if plan.base is None or plan.docs is None or name not in plan.docs.arrays:
        return None
    meta = plan.docs.arrays[name]
    kw = {}
    if plan.mode != "overwrite":
        keys = sorted({unit_key(meta, c) for c in touched})
        kw["shard_keys" if meta.shard is not None else "keys"] = keys
    return M.read_rows(spark, plan.base, kinds=["shard", "chunk"], arrays=[name], **kw)


def _write_files(spark, plan: _Plan):
    """Phase 1. Returns ``(rows, base_rows, stats)``: one manifest row per written chunk
    (Arrow), the base rows of the touched chunks, and counts for the commit."""
    stats = {"chunks": 0, "rmw": 0}
    base_rows: dict[str, pa.Table] = {}
    rmw_rows, rmw_arrays, parts = [], {}, set()
    for name in plan.data:
        meta = plan.arrays[name]
        touched, partial = _coverage(meta, *plan.boxes[name])
        parts.update((name, _part_of(meta, c)[0]) for c in touched)
        stats["rmw"] += len(partial)
        rows = _base_rows(spark, plan, name, touched)
        if rows is not None:
            base_rows[name] = rows
        if partial and rows is not None and rows.num_rows:
            base_meta = plan.docs.arrays[name]
            want_chunks = {base_meta.key_encoding.chunk_key(c) for c in partial}
            want_shards = (
                {unit_key(base_meta, c) for c in partial}
                if base_meta.shard is not None
                else set()
            )
            kind, key = rows.column("kind"), rows.column("key")
            pick = pc.or_(
                pc.and_(
                    pc.equal(kind, "chunk"),
                    pc.is_in(key, value_set=pa.array(sorted(want_chunks), pa.string())),
                ),
                pc.and_(
                    pc.equal(kind, "shard"),
                    pc.is_in(key, value_set=pa.array(sorted(want_shards), pa.string())),
                ),
            )
            rmw_rows += rows.filter(pick).to_pylist()
            rmw_arrays[name] = base_meta
    if not plan.data:
        return _ROWS_ARROW.empty_table(), base_rows, stats
    frame = plan.frame if plan.frame is not None else _xarray_frame(spark, plan)
    if frame is None:
        return _ROWS_ARROW.empty_table(), base_rows, stats
    rmw_docs = {}
    if rmw_arrays:
        rmw_docs = {
            k: v
            for k, v in plan.docs.metas.items()
            if k.rpartition("/")[0] in rmw_arrays
        }
    cfg = _TaskConfig(
        write_id=plan.write_id,
        root=plan.root,
        base_root=plan.base_root,
        arrays={n: plan.arrays[n] for n in plan.data},
        offsets=dict(plan.offsets) if plan.frame is not None else {},
        rmw_docs=rmw_docs,
        rmw_arrays=rmw_arrays,
        rmw_rows=rmw_rows,
        fault=_FAULT,
    )
    n = max(1, min(len(parts), _PARTS_PER_CORE * _parallelism(spark)))
    pieces = frame.mapInArrow(_explode_function(cfg), _PIECES_DDL)
    grouped = pieces.repartition(n, "variable", "part").sortWithinPartitions(
        "variable", "key"
    )
    rows = grouped.mapInArrow(_assemble_function(cfg), _ROWS_DDL).toArrow()
    stats["chunks"] = rows.num_rows
    return rows, base_rows, stats


# --- phase 2: the commit ---------------------------------------------------------------

_SCHEMA = M._COMMIT_SCHEMA


def _empty_rows() -> pa.Table:
    return _SCHEMA.empty_table()


def _rows_table(recs: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(
        [{c: r.get(c) for c in _SCHEMA.names} for r in recs], schema=_SCHEMA
    )


def _commit_data_rows(plan: _Plan, rows: pa.Table, base_rows: dict) -> pa.Table:
    """The chunk rows to commit, with their expectations (columnar: millions of rows)."""
    w = plan.write_id
    parts = []
    names = sorted({*pc.unique(rows.column("array")).to_pylist(), *base_rows})
    for name in names:
        mine = rows.filter(pc.equal(rows.column("array"), name))
        base = base_rows.get(name)
        live_chunks = (
            base.filter(pc.equal(base.column("kind"), "chunk")).select(
                ["array", "key", "write_id"]
            )
            if base is not None
            else pa.table(
                {
                    "array": pa.array([], pa.string()),
                    "key": pa.array([], pa.string()),
                    "write_id": pa.array([], pa.string()),
                }
            )
        )
        live_shards = (
            base.filter(pc.equal(base.column("kind"), "shard")).select(["array", "key"])
            if base is not None
            else pa.table(
                {"array": pa.array([], pa.string()), "key": pa.array([], pa.string())}
            )
        )
        joined = mine.join(
            live_chunks.rename_columns(["array", "key", "expected_write_id"]),
            keys=["array", "key"],
            join_type="left outer",
        )
        if plan.mode == "overwrite" or not live_shards.num_rows:
            shard_live = pa.array(np.zeros(joined.num_rows, dtype=bool))
        else:
            held = live_shards.rename_columns(["array", "shard_key"]).append_column(
                "shard_live", pa.array(np.ones(live_shards.num_rows, dtype=bool))
            )
            joined = joined.join(
                held, keys=["array", "shard_key"], join_type="left outer"
            )
            shard_live = pc.fill_null(joined.column("shard_live"), False)
            shard_live = shard_live.combine_chunks()
        joined = joined.combine_chunks()
        empty = pc.fill_null(joined.column("empty"), False).combine_chunks()
        expected = joined.column("expected_write_id")
        has_row = pc.is_valid(expected)
        if plan.mode == "create":
            joined = joined.filter(pc.invert(empty))
            parts.append(_chunk_rows(joined, w, None, None))
        else:
            tomb = pc.and_(empty, shard_live)
            delete = pc.and_(pc.and_(empty, pc.invert(tomb)), has_row)
            drop = pc.and_(
                pc.and_(pc.and_(empty, pc.invert(tomb)), pc.invert(has_row)),
                pa.scalar(plan.mode == "overwrite"),
            )
            keep = pc.invert(drop)
            joined = joined.filter(keep)
            tomb, delete = tomb.filter(keep), delete.filter(keep)
            parts.append(_chunk_rows(joined, w, tomb, delete))
        if plan.mode == "overwrite" and base is not None and base.num_rows:
            written = mine.select(["array", "key"]).append_column(
                "kind", pa.array(["chunk"] * mine.num_rows, pa.string())
            )
            gone = base.select(["kind", "array", "key", "write_id"]).join(
                written, keys=["kind", "array", "key"], join_type="left anti"
            )
            if gone.num_rows:
                parts.append(
                    _typed_rows(
                        {
                            "kind": gone.column("kind"),
                            "array": gone.column("array"),
                            "key": gone.column("key"),
                            "write_id": pa.array([w] * gone.num_rows, pa.string()),
                            "op": pa.array(["delete"] * gone.num_rows, pa.string()),
                            "expected_write_id": gone.column("write_id"),
                        },
                        gone.num_rows,
                    )
                )
    parts = [p for p in parts if p.num_rows]
    return pa.concat_tables(parts) if parts else _empty_rows()


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


def _chunk_rows(joined: pa.Table, w: str, tomb, delete) -> pa.Table:
    n = joined.num_rows
    if not n:
        return _empty_rows()
    nulls = pa.nulls(n, pa.string())
    file, offset, length = (
        joined.column("file"),
        joined.column("offset"),
        joined.column("length"),
    )
    checksum = joined.column("checksum")
    op = pa.array(["upsert"] * n, pa.string())
    if tomb is not None:
        gone = pc.or_(tomb, delete)
        file = pc.if_else(gone, nulls, file)
        offset = pc.if_else(gone, pa.nulls(n, pa.int64()), offset)
        checksum = pc.if_else(gone, nulls, checksum)
        length = pc.if_else(
            tomb,
            pa.array(np.zeros(n, dtype=np.int64)),
            pc.if_else(delete, pa.nulls(n, pa.int64()), length),
        )
        op = pc.if_else(delete, pa.array(["delete"] * n, pa.string()), op)
    return _typed_rows(
        {
            "kind": pa.array(["chunk"] * n, pa.string()),
            "array": joined.column("array"),
            "key": joined.column("key"),
            "shard_key": joined.column("shard_key"),
            "file": file,
            "offset": offset,
            "length": length,
            "checksum": checksum,
            "origin": pa.array(["gbx"] * n, pa.string()),
            "write_id": pa.array([w] * n, pa.string()),
            "op": op,
            "expected_write_id": joined.column("expected_write_id"),
        },
        n,
    )


def _doc_commit_rows(plan: _Plan, ids: dict) -> pa.Table:
    """The group and array documents this write writes, with their expectations."""
    w = plan.write_id
    recs = []
    for path in plan.groups:
        doc = {"zarr_format": 3, "node_type": "group", "attributes": {}}
        if path == "":
            doc["attributes"] = plan.group_attrs
        key = ("group", path or None, "zarr.json")
        recs.append(
            {
                "kind": "group",
                "array": path or None,
                "key": "zarr.json",
                "origin": "gbx",
                "write_id": w,
                "op": "upsert",
                "expected_write_id": ids.get(key),
                "meta": json.dumps(doc, sort_keys=True),
            }
        )
    for name in plan.changed:
        for key, text in _doc_rows(plan.arrays[name]).items():
            recs.append(
                {
                    "kind": "array",
                    "array": name,
                    "key": key,
                    "origin": "gbx",
                    "write_id": w,
                    "op": "upsert",
                    "expected_write_id": ids.get(("array", name, key)),
                    "meta": text,
                }
            )
    return _rows_table(recs) if recs else _empty_rows()


def _coordinate_rows(spark, plan: _Plan, snap, docs: _Docs | None) -> pa.Table:
    """The coordinate chunks this write rewrites, read-modified-written on the driver over
    ``snap`` (the base, or the latest version in a rebase), in a pack of their own."""
    if not plan.patches:
        return _empty_rows()
    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box
    from databricks.labs.gbx.zarr._store import ManifestStore

    w = plan.write_id
    recs = []
    writers: dict[str, PackWriter] = {}
    for patch in plan.patches:
        meta = plan.arrays[patch.name]
        c = meta.chunks[0]
        end = patch.start + len(patch.values)
        chunks = list(range(patch.start // c, (end - 1) // c + 1))
        keys = [meta.key_encoding.chunk_key((k,)) for k in chunks]
        live = {}
        base_meta = None if docs is None else docs.arrays.get(patch.name)
        store = None
        if snap is not None and base_meta is not None:
            kw = {"keys": keys}
            if base_meta.shard is not None:
                kw = {"shard_keys": sorted({unit_key(base_meta, (k,)) for k in chunks})}
            rows = M.read_rows(
                spark, snap, kinds=["shard", "chunk"], arrays=[patch.name], **kw
            ).to_pylist()
            live = {r["key"]: r["write_id"] for r in rows if r["kind"] == "chunk"}
            store = ManifestStore(
                docs.metas,
                rows,
                FuseRangeReader(plan.root, base_root=plan.base_root),
                {patch.name: base_meta},
            )
        try:
            for k, key in zip(chunks, keys):
                origin = k * c
                ext = max(0, min(c, meta.shape[0] - origin))
                lo, hi = max(patch.start, origin), min(end, origin + ext)
                native = np.dtype(meta.dtype).newbyteorder("=")
                fill = 0 if meta.fill_value is None else meta.fill_value
                values = np.full(meta.chunks, fill, dtype=native)
                whole = lo == origin and hi == origin + ext
                if store is not None and not whole:
                    top = min(origin + c, base_meta.shape[0])
                    if top > origin:
                        arr = open_manifest_array(store, patch.name)
                        values[: top - origin] = read_box(arr, (slice(origin, top),))
                values[lo - origin : hi - origin] = patch.values[
                    lo - patch.start : hi - patch.start
                ]
                data = encode_inner_chunk(meta, values)
                writer = writers.get(patch.name)
                if writer is None:
                    writer = writers[patch.name] = PackWriter(
                        os.path.join(plan.root, "data", patch.name, w),
                        reader_root=plan.root,
                    )
                offset, length = writer.add(data)
                recs.append(
                    {
                        "kind": "chunk",
                        "array": patch.name,
                        "key": key,
                        "shard_key": (
                            unit_key(meta, (k,)) if meta.shard is not None else None
                        ),
                        "file": writer.path,
                        "offset": offset,
                        "length": length,
                        "checksum": crc32c_hex(data),
                        "origin": "gbx",
                        "write_id": w,
                        "op": "upsert",
                        "expected_write_id": live.get(key),
                    }
                )
        finally:
            if store is not None:
                store.close()
    for writer in writers.values():
        writer.close()
    return _rows_table(recs)


def _is_metadata(plan: _Plan, kind, array) -> bool:
    return kind in ("array", "group") or (kind == "chunk" and array in plan.coords)


def _conflict(rows: list[dict]) -> ZarrWriteConflict:
    keys = tuple(_store_key(r["array"], r["key"]) for r in rows)
    return ZarrWriteConflict(
        "another write changed the same chunks",
        keys=keys,
        winner=rows[0].get("write_id") if rows else None,
        conflicts=tuple(rows),
        fix="re-read the region and retry, or write disjoint chunks",
    )


def _rebase(spark, plan: _Plan, latest) -> tuple[pa.Table, pa.Table]:
    """Doc and coordinate rows again, over ``latest``: shapes at the maximum extent,
    coordinate chunks read-modified-written over the latest ones, expectations refreshed.
    """
    docs = _docs_at(spark, latest)
    for name in plan.changed:
        mine, theirs = plan.arrays[name], docs.arrays.get(name)
        if theirs is None:
            continue
        if not _same_layout(mine, theirs):
            raise ZarrWriteConflict(
                f"another write changed the layout of array {name!r} (its type, chunks or "
                "codecs) since this write read it",
                keys=(name,),
                fix="re-read the store and write again",
            )
        shape = tuple(max(a, b) for a, b in zip(mine.shape, theirs.shape))
        if shape != mine.shape:
            plan.arrays[name] = with_shape(mine, shape)
    return _doc_commit_rows(plan, docs.ids), _coordinate_rows(spark, plan, latest, docs)


def _commit(spark, plan: _Plan, data_rows: pa.Table, stats: dict) -> int:
    """Phase 2: the CAS commit, rebasing metadata at most ``_ATTEMPTS`` times."""
    ids = plan.docs.ids if plan.docs is not None else {}
    docs_rows = _doc_commit_rows(plan, ids)
    coord_rows = _coordinate_rows(spark, plan, plan.base, plan.docs)
    meta = {
        "mode": plan.mode,
        "write_id": plan.write_id,
        "variables": sorted({*plan.data, *(p.name for p in plan.patches)}),
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
        stale = []
        if latest.version != base.version:
            stale = M.changed_since(spark, plan.table, base, rows).to_pylist()
        data_stale = [r for r in stale if not _is_metadata(plan, r["kind"], r["array"])]
        if data_stale:
            raise _conflict(data_stale)
        if stale:
            docs_rows, coord_rows = _rebase(spark, plan, latest)
            continue_rows = pa.concat_tables([docs_rows, coord_rows, data_rows])
        else:
            continue_rows = rows
        try:
            return M.commit(
                spark,
                plan.table,
                continue_rows,
                write_id=plan.write_id,
                meta=meta,
                sharded=sharded,
            )
        except M.ConcurrentCommit:
            time.sleep(random.uniform(0.2, 1.0) * (2**attempt))
            base = latest
        except ZarrWriteConflict as e:
            found = list(e.conflicts)
            data = [r for r in found if not _is_metadata(plan, r["kind"], r["array"])]
            if data:
                raise _conflict(data) from e
            if not found:
                raise
            time.sleep(random.uniform(0.2, 1.0) * (2**attempt))
            base = latest
    raise ZarrWriteConflict(
        "gave up after 5 attempts",
        fix="retry later or reduce concurrent writers to this array",
    )


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

    ``data`` is a blocks DataFrame (``BLOCKS_SCHEMA``, as ``gbx_zarr_read`` returns it) or
    an ``xarray.Dataset`` (driver-sized). Blocks with ``decode=True`` values are CF-encoded
    with the array's attributes (times to its units, packed values to their integer type,
    NaN to the fill value); blocks that still carry the array's packing attributes
    (``decode=False``) are written as the stored values they are.

    ``mode``:

    - ``"create"``: a new managed store, table ``target`` and data root ``root`` (which
      gets an owner marker naming the table). ``chunks`` (``"maps"``, ``"series"``,
      ``"balanced"`` or a dict of dimension to size), ``shards`` (``None`` computes them, a
      dict sets some, ``False`` means none) and ``codecs`` (the inner chunk's codec chain;
      default ``bytes`` + ``zstd`` level 3 + ``crc32c``) lay out each array; coordinates
      are one unsharded chunk. ``file_mode`` and ``retain`` are the store's.
    - ``"append"``: extend along ``append_dim`` (default: the first dimension labelled by
      CF times). The input must carry that dimension's labels, all after the store's last;
      on a regular axis they land at their step (so appends may arrive out of order), else
      at the current length.
    - ``"update"``: write into the existing extent. Inner chunks the input covers whole are
      encoded without reading; partly covered ones are read at the base version, patched
      and re-encoded. Blocks are placed by their ``start``/``stop``; an ``xarray.Dataset``
      by its coordinate labels.
    - ``"overwrite"``: replace the named variables: chunks the input does not cover read as
      the fill value. Older versions stay readable until vacuumed.

    A variable the store lacks is created on the store's dimensions with the default
    layout and codecs. A registered store takes writes only when registered with
    ``writable=True``; they go to its overlay root, in the base arrays' own key encoding,
    and the base is never touched.

    ``write_id`` (32 lowercase hex characters; default a new ``uuid4().hex``) makes a write
    idempotent: writing again with a ``write_id`` that committed returns its version and
    writes nothing.

    One MERGE commits a write, so it is visible whole or not at all. A concurrent write that
    changed the same chunks since this one read them raises ``ZarrWriteConflict`` (nothing
    of this write is visible); one that changed only metadata (another append's shape or a
    shared coordinate chunk) is rebased and the commit retried, at most five times.

    A blocks DataFrame is evaluated twice: once for its block positions (no data reaches
    the driver except coordinate labels) and once to write. Errors raised while Spark runs
    the write arrive as ``pyspark.errors.PythonException`` with the GeoBrix error name and
    its ``Fix:`` line in the message; a failed write commits nothing. ``access`` is
    ``"auto"`` or ``"fuse"``; ``dim_cols`` and ``optimize`` arrive with the tile writer and
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
    if dim_cols is not None:
        raise ZarrUnsupported(
            "dim_cols places GeoBrix tiles, which arrive with the tile writer",
            fix="write blocks or an xarray.Dataset, without dim_cols",
        )
    if mode == "create":
        if not root:
            raise ValueError(
                "mode='create' needs root, the directory the store's files go to. Fix: "
                "pass root='/Volumes/<catalog>/<schema>/<volume>/<dir>'."
            )
        M.retain_seconds(retain)
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
    spark = _session(data, spark)

    exists = _table_exists(spark, target)
    if exists:
        landed = M.find_commit_version(spark, target, write_id)
        if landed is not None:
            return landed
    owner = qualified_table_name(spark, target)
    if mode == "create":
        if exists:
            raise ZarrOwnershipError(
                f"{target} already exists",
                fix="choose a new table name, or write to it with mode='append', "
                "'update' or 'overwrite'",
            )
        fm = resolve_file_mode(spark, file_mode, has_filespace=False)
        data_root = _local_root(root)
        inp = _input_of(data, variables, spark)
        plan = _plan_create(
            spark, target, write_id, data_root, inp, chunks, shards, codecs
        )
        claimed = made = False
        try:
            claimed = _claim_marker(data_root, owner, "managed")
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
            rows, base_rows, stats = _write_files(spark, plan)
            if _BEFORE_COMMIT is not None:
                _BEFORE_COMMIT(write_id)
            data_rows = _commit_data_rows(plan, rows, base_rows)
            return _commit(spark, plan, data_rows, stats)
        except BaseException:
            if made:
                _discard(spark, target, write_id)
            if claimed:
                _release_marker(data_root, owner, "managed")
            raise
    if not exists:
        raise ZarrError(
            f"{target} does not exist",
            fix="create the store with mode='create' (or register an existing one)",
        )
    props = M.read_props(spark, target)
    inp = _input_of(data, variables, spark)
    plan = _plan_existing(spark, target, write_id, mode, props, inp, append_dim, owner)
    rows, base_rows, stats = _write_files(spark, plan)
    if _BEFORE_COMMIT is not None:
        _BEFORE_COMMIT(write_id)
    data_rows = _commit_data_rows(plan, rows, base_rows)
    return _commit(spark, plan, data_rows, stats)
