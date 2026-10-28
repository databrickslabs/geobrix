"""``gbx_zarr_read``: a Zarr store, or a cut of one, as Spark blocks or a table.

The read is planned on the driver and run in tasks:

1. Resolve the source. A table is a managed or registered store: one Delta version of its
   manifest is pinned and everything below comes from that version. A path is a plain
   store and reads whatever bytes are there.
2. Read the array documents and the coordinate arrays (small, read whole), turn ``sel``
   (labels) and ``isel`` (positions) into index ranges, and find the inner chunks they
   touch.
3. Fetch only the manifest rows of the touched shards, and pack the chunks into work units
   that fit the memory budget (``_plan``).
4. The units are the rows of a DataFrame (one per unit, one per partition) that
   ``mapInArrow`` runs: each task rebuilds its store from its own unit's rows alone, reads
   each chunk's selected cells, decodes them, and emits Arrow batches. No task needs a
   session or the manifest table, and what is sent to every task does not grow with the
   number of units (the labels it carries are those of the selection).

``output="blocks"`` returns one row per (variable, touched chunk, selected sub-box): the
decoded cells as C-order bytes with their position in the array (``BLOCKS_SCHEMA``).
``output="table"`` returns one row per selected cell with a column per dimension and per
variable. Both pin the snapshot, so a read is consistent whatever commits meanwhile.

``output="tiles"`` returns GeoBrix tiles (``_tiles.py``): the driver plans their windows and
the manifest rows behind them, and tasks make the tile rows from that plan (``spark.range``
of tile numbers into ``mapInArrow``), so no tile passes through the driver.
"""

from __future__ import annotations

import dataclasses
import itertools
import json
import logging
import math
import re
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from databricks.labs.gbx.zarr._access import (
    FUSE,
    AccessConfig,
    bounded_closure,
    reader_for,
    resolve_access,
)
from databricks.labs.gbx.zarr._meta import (
    ArrayMeta,
    _check_mask_flag,
    cf_decode,
    cf_unpack,
    decode_coord,
    is_time_coord,
    isolate_unreadable_groups,
    parse_v2,
    parse_v3,
)
from databricks.labs.gbx.zarr._plan import (
    Unit,
    boxes_of,
    keys_of_boxes,
    plan_units,
    row_unit_key,
    rows_for_boxes,
    unit_key,
)
from databricks.labs.gbx.zarr._select import AxisSel, resolve, touched_chunks
from databricks.labs.gbx.zarr.errors import (
    ZarrAccessUnavailable,
    ZarrError,
    ZarrGridMismatch,
    ZarrUnsupported,
    ZarrVersionUnavailable,
)

__all__ = [
    "BLOCKS_SCHEMA",
    "gbx_zarr_read",
    "block_to_numpy",
    "block_to_xarray",
    "check_access",
    "check_version_args",
    "classify_source",
    "parse_documents",
    "store_documents",
]

_LOG = logging.getLogger(__name__)

BLOCKS_SCHEMA = (
    "variable STRING, dims ARRAY<STRING>, start ARRAY<BIGINT>, stop ARRAY<BIGINT>, "
    "coord_start ARRAY<STRING>, coord_stop ARRAY<STRING>, dtype STRING, "
    "shape ARRAY<BIGINT>, data BINARY, fill_value STRING, attrs STRING"
)
_BLOCKS_ARROW = pa.schema(
    [
        ("variable", pa.string()),
        ("dims", pa.list_(pa.string())),
        ("start", pa.list_(pa.int64())),
        ("stop", pa.list_(pa.int64())),
        ("coord_start", pa.list_(pa.string())),
        ("coord_stop", pa.list_(pa.string())),
        ("dtype", pa.string()),
        ("shape", pa.list_(pa.int64())),
        ("data", pa.binary()),
        ("fill_value", pa.string()),
        ("attrs", pa.string()),
    ]
)

_OUTPUTS = ("blocks", "tiles", "table")
_ACCESS = ("auto", "fuse", "direct")
_SOURCE_TYPES = ("auto", "path", "table")
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:/")
_BATCH_ROWS = 64  # blocks per Arrow batch
_TABLE_BATCH_ROWS = 65536  # cells per Arrow batch of a table read
_BATCH_SHARE = 4  # a batch holds at most this fraction of the decoded-memory budget
_READ_THREADS = 8  # boxes a task reads at once
_CONSOLIDATED = "consolidated_metadata"
# attributes cf_decode consumes: a decoded block no longer carries them
_PACKING_ATTRS = ("scale_factor", "add_offset", "_FillValue", "missing_value")
_TIME_ATTRS = ("units", "calendar")
_ROWS = ("shard", "chunk")


def classify_source(source: str, source_type: str = "auto") -> str:
    """``"path"`` or ``"table"``.

    ``auto`` takes a string that starts with ``/`` or has a URI scheme (``dbfs:/``,
    ``file:/``, ``s3://``) for a path and anything else for a table name.
    """
    if source_type not in _SOURCE_TYPES:
        raise ValueError(
            f"source_type must be one of {_SOURCE_TYPES}, got {source_type!r}. "
            "Fix: pass 'auto', 'path' or 'table'."
        )
    if not isinstance(source, str) or not source.strip():
        raise ValueError(
            "source must be a table name or a store path. "
            "Fix: pass the manifest table of a store, or the path of a Zarr store."
        )
    if source_type != "auto":
        return source_type
    text = source.strip()
    return "path" if text.startswith("/") or _SCHEME.match(text) else "table"


def check_access(access) -> None:
    """Refuse an ``access`` that is not ``"auto"``, ``"fuse"`` or ``"direct"``. Whether
    direct access is available is decided later, by ``source_access`` (``resolve_access``).
    """
    if access not in _ACCESS:
        raise ValueError(
            f"access must be one of {_ACCESS}, got {access!r}. "
            "Fix: pass 'auto', 'fuse' or 'direct'."
        )


def plain_direct_error(root: str) -> ZarrAccessUnavailable:
    return ZarrAccessUnavailable(
        f"direct storage access applies to the files of a managed or registered store; "
        f"the plain store {root} is read through its path (FUSE only)",
        fix="register the store with gbx_zarr_register to use direct access, or use "
        'access="auto" or "fuse"',
    )


def source_access(spark, src, access: str, *, operation: str = "READ_VOLUME"):
    """The ``AccessConfig`` the tasks read ``src`` with (``resolve_access`` on the driver).

    The files of a managed or registered store are read through ``resolve_access``: direct
    when the credentials can be had and ``access`` is not ``"fuse"``. A plain store is read
    by zarr itself from its path, so ``"direct"`` has nothing to apply to and raises
    ``ZarrAccessUnavailable``; ``"auto"`` reads the path, and no credentials are asked for.
    """
    if src.kind == "plain":
        if access == "direct":
            raise plain_direct_error(src.root)
        return FUSE
    return resolve_access(
        spark,
        src.root,
        access=access,
        operation=operation,
        also=[src.base_root] if src.base_root else [],
    )


def check_version_args(version_as_of, timestamp_as_of) -> None:
    if version_as_of is not None and timestamp_as_of is not None:
        raise ValueError(
            "pass version_as_of or timestamp_as_of, not both. "
            "Fix: name the version one way."
        )


def _budget_bytes() -> int:
    """Decoded bytes a task may hold: the same table ``raster_gbx`` sizes its tiles by."""
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    return decoded_budget_bytes("auto")


# --- documents -----------------------------------------------------------------------


def store_documents(metas: dict[str, bytes]) -> dict[str, bytes]:
    """``metas`` as a store should serve them: with no consolidated metadata.

    Consolidated metadata (``.zmetadata``, or the ``consolidated_metadata`` block of a group's
    ``zarr.json``) is a copy of the per-node documents taken at some past moment. The
    per-node documents are the truth and a registered store can change under it, so a stale
    copy must never be served; a reader that opens a group then reads the per-node documents.
    """
    out = {}
    for key, raw in metas.items():
        name = key.rpartition("/")[2]
        if name == ".zmetadata":
            continue
        if name == "zarr.json":
            try:
                doc = json.loads(raw)
            except ValueError:
                out[key] = raw
                continue
            if isinstance(doc, dict) and _CONSOLIDATED in doc:
                doc = {k: v for k, v in doc.items() if k != _CONSOLIDATED}
                raw = json.dumps(doc).encode()
        out[key] = raw
    return out


def parse_documents(
    metas: dict[str, bytes],
) -> tuple[dict[str, ArrayMeta], dict[str, str]]:
    """``(arrays, unsupported)`` from the metadata documents of a store.

    ``arrays`` maps the path of each array GeoBrix can decode to its ``ArrayMeta``.
    ``unsupported`` maps the path of each one it cannot (a string coordinate, say, or a
    malformed document) to the reason. A parse failure of one array never fails the others:
    the array is named only when it is decoded. A group whose document is unreadable is
    not an array: the arrays under it are in ``unsupported`` for that cause instead
    (``isolate_unreadable_groups``).
    """
    arrays: dict[str, ArrayMeta] = {}
    unsupported: dict[str, str] = {}
    for key in sorted(metas):
        prefix, _, name = key.rpartition("/")
        if not prefix:
            continue  # the root group's documents
        try:
            if name == "zarr.json":
                doc = json.loads(metas[key])
                if doc.get("node_type") == "array":
                    arrays[prefix] = parse_v3(prefix, doc)
            elif name == ".zarray":
                zattrs = json.loads(metas.get(f"{prefix}/.zattrs", b"{}"))
                arrays[prefix] = parse_v2(prefix, json.loads(metas[key]), zattrs)
        except ZarrUnsupported as e:
            unsupported[prefix] = e.message
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as e:
            unsupported[prefix] = f"its metadata is malformed ({type(e).__name__}: {e})"
    isolate_unreadable_groups(metas, arrays, unsupported)
    return arrays, unsupported


# --- the source ----------------------------------------------------------------------


@dataclass
class _Source:
    """What the driver knows about a store: documents, arrays, and how to read its rows."""

    kind: str  # "table" | "plain"
    root: str  # where gbx-origin files live (a plain store: the store)
    base_root: str | None  # where a registered store's own files live
    metas: dict[str, bytes]
    arrays: dict[str, ArrayMeta]
    unsupported: dict[str, str]
    spark: object = None
    table: str | None = None
    snap: object = None

    def rows(self, meta: ArrayMeta, keys: list[str] | None) -> dict[str, list[dict]]:
        """The shard and chunk rows of ``meta``'s array, grouped by ``unit_key``; only the
        groups in ``keys`` (all of them for ``None``). Empty for a plain store."""
        if self.kind == "plain" or keys == []:
            return {}
        from databricks.labs.gbx.zarr._manifest import read_rows

        # always a pruned query: read_rows turns a long key list into a semi-join on the
        # cluster, so the driver never fetches rows it did not ask for
        kw = {}
        if keys is not None:
            kw["shard_keys" if meta.shard is not None else "keys"] = keys
        table = read_rows(
            self.spark, self.snap, kinds=list(_ROWS), arrays=[meta.name], **kw
        )
        wanted = None if keys is None else set(keys)
        grouped: dict[str, list[dict]] = {}
        for row in table.to_pylist():
            key = row_unit_key(meta, row)
            if wanted is None or key in wanted:
                grouped.setdefault(key, []).append(row)
        return grouped

    def reader(self):
        from databricks.labs.gbx.zarr._access import FuseRangeReader

        return FuseRangeReader(self.root, base_root=self.base_root)


def _open_source(spark, source, kind, version_as_of, timestamp_as_of) -> _Source:
    if kind == "table":
        return _open_table(spark, source, version_as_of, timestamp_as_of)
    if version_as_of is not None or timestamp_as_of is not None:
        raise ZarrVersionUnavailable(
            f"{source} is a plain Zarr store, which has no versions: it reads the bytes "
            "that are there now",
            fix="register the store with gbx_zarr_register to read it as of a version, "
            "or drop version_as_of and timestamp_as_of",
        )
    from databricks.labs.gbx.zarr._register import _local_root, read_base_metadata

    root = _local_root(source)
    metas, arrays, unsupported = read_base_metadata(root, tolerant=True)
    return _Source("plain", root, None, metas, arrays, unsupported)


def _open_table(spark, table, version_as_of, timestamp_as_of) -> _Source:
    from databricks.labs.gbx.zarr._filemode import FileLayout
    from databricks.labs.gbx.zarr._manifest import read_props, read_rows, snapshot

    props = read_props(spark, table)
    snap = snapshot(
        spark,
        table,
        version_as_of=version_as_of,
        timestamp_as_of=timestamp_as_of,
        files=FileLayout.of(props),
    )
    metas = {}
    for row in read_rows(spark, snap, kinds=["group", "array"]).to_pylist():
        if row["meta"] is not None:
            key = "/".join(p for p in (row["array"], row["key"]) if p)
            metas[key] = row["meta"].encode()
    arrays, unsupported = parse_documents(metas)
    if props.kind == "registered":
        # a registered store's own objects are under its base; what GeoBrix wrote is under
        # the overlay (a read-only registration has none, and so no gbx-origin rows)
        root, base = props.overlay_root or props.root, props.root
    else:
        root, base = props.root, None
    return _Source(
        "table",
        root,
        base,
        store_documents(metas),
        arrays,
        unsupported,
        spark,
        table,
        snap,
    )


# --- variables, coordinates and selections -------------------------------------------


def _raw_grid(metas: dict[str, bytes], path: str):
    """``(shape, dims)`` read straight from the documents of an array GeoBrix cannot
    decode (a string coordinate, say), or ``None`` when they cannot be read either."""
    try:
        if f"{path}/zarr.json" in metas:
            doc = json.loads(metas[f"{path}/zarr.json"])
            shape, names = doc["shape"], doc.get("dimension_names")
        else:
            shape = json.loads(metas[f"{path}/.zarray"])["shape"]
            attrs = json.loads(metas.get(f"{path}/.zattrs", b"{}"))
            names = attrs.get("_ARRAY_DIMENSIONS")
        shape = tuple(shape)
        dims = tuple(names) if names else tuple(f"dim_{i}" for i in range(len(shape)))
    except (KeyError, TypeError, ValueError, AttributeError):
        return None
    return shape, dims


def _coordinate_of(dim, array, src: _Source) -> str | None:
    """The path of the array that holds the labels of ``dim`` for ``array``, if any.

    A dimension coordinate is a one-dimensional array named like the dimension, in the
    array's own group or the root. An array GeoBrix cannot decode counts when its documents
    say it is one: the dimension does have labels, they just cannot be read. One that is
    not (a 2-D array that happens to share the name) labels nothing, and neither does one
    whose documents cannot be read at all, which is given the benefit of the doubt.
    """
    parent = array.rpartition("/")[0]
    for path in ([f"{parent}/{dim}"] if parent else []) + [dim]:
        if path in src.unsupported:
            grid = _raw_grid(src.metas, path)
            if grid is None or (len(grid[0]) == 1 and grid[1] == (dim,)):
                return path
            continue
        meta = src.arrays.get(path)
        if meta is not None and len(meta.shape) == 1 and meta.dims == (dim,):
            return path
    return None


def _auxiliary(arrays: dict[str, ArrayMeta]) -> set[str]:
    """Arrays other arrays name as coordinates or bounds (CF ``coordinates``/``bounds``)."""
    out = set()
    for name, meta in arrays.items():
        parent = name.rpartition("/")[0]
        for attr in ("coordinates", "bounds"):
            text = meta.attrs.get(attr)
            for ref in text.split() if isinstance(text, str) else ():
                for path in ([f"{parent}/{ref}"] if parent else []) + [ref]:
                    if path in arrays:
                        out.add(path)
                        break
    return out


def _choose_variables(variables, src: _Source) -> list[str]:
    arrays, unsupported = src.arrays, src.unsupported
    if variables is None:
        coordinates = {
            c
            for name, meta in arrays.items()
            for d in meta.dims
            if (c := _coordinate_of(d, name, src))
        }
        skip = coordinates | _auxiliary(arrays)
        chosen = [n for n in sorted(arrays) if n not in skip and arrays[n].shape != ()]
        left_out = sorted(unsupported)
        if left_out:
            _LOG.warning(
                "%s: %d array(s) cannot be decoded and are left out (or have no labels): "
                "%s",
                src.table or src.root,
                len(left_out),
                ", ".join(f"{a} ({unsupported[a]})" for a in left_out),
            )
        if not chosen:
            raise ValueError(
                "the store has no gridded variable to read. "
                "Fix: name the arrays to read with variables=[...]."
            )
        return chosen
    names = [variables] if isinstance(variables, str) else list(variables)
    if not names or not all(isinstance(n, str) for n in names):
        raise ValueError(
            "variables must be a variable name or a non-empty list of names. "
            "Fix: pass variables=['t2m'] or leave it None for every variable."
        )
    names = list(dict.fromkeys(names))
    for n in names:
        if n in unsupported:
            raise ZarrUnsupported(
                f"array {n!r} cannot be decoded: {unsupported[n]}",
                fix="read the other variables, or convert this array to a numeric dtype",
            )
    unknown = [n for n in names if n not in arrays]
    if unknown:
        raise ValueError(
            f"unknown variable(s) {unknown}; the store has {sorted(arrays)}. "
            "Fix: name arrays of the store."
        )
    return names


def _native(values) -> np.ndarray:
    """``values`` in native byte order and C order."""
    values = np.asarray(values)
    if not values.dtype.isnative:
        values = values.astype(values.dtype.newbyteorder("="))
    return np.asarray(values, order="C")


def _decode_values(values: np.ndarray, meta: ArrayMeta, decode: bool, flag):
    """``values`` as the caller sees them: CF-decoded (times become ``datetime64[ns]``), or
    as stored for ``decode=False``; always in native byte order."""
    if decode:
        if is_time_coord(meta):
            values = decode_coord(values, meta, use_zarr_fill_value_as_mask=flag)
        else:
            values = cf_decode(values, meta, use_zarr_fill_value_as_mask=flag)
    return _native(values)


def _table_values(values, meta: ArrayMeta, decode: bool, flag):
    """``(values, mask)`` for a table column: missing cells are a mask, not NaN.

    Integers and booleans stay exact (a masked integer is still an integer), packed arrays
    decode to floating point, times become ``datetime64[ns]`` with NaT for a missing time
    (which is its own mask), and ``decode=False`` is the stored values with no mask.
    """
    values = _native(values)
    if not decode:
        return values, None
    if is_time_coord(meta):
        return (
            _native(decode_coord(values, meta, use_zarr_fill_value_as_mask=flag)),
            None,
        )
    values, mask = cf_unpack(values, meta, use_zarr_fill_value_as_mask=flag)
    return _native(values), mask


def _read_coordinates(src: _Source, paths) -> dict[str, np.ndarray]:
    """The stored values of each coordinate array, read whole (not decoded)."""
    from databricks.labs.gbx.zarr._engine import (
        open_manifest_array,
        open_plain_array,
        read_box,
    )

    paths = sorted(paths)
    if not paths:
        return {}
    out = {}
    if src.kind == "plain":
        for path in paths:
            arr = open_plain_array(src.root, path, engine="auto")
            out[path] = read_box(arr, (slice(None),))
    else:
        from databricks.labs.gbx.zarr._store import ManifestStore

        rows = []
        for path in paths:
            for group in src.rows(src.arrays[path], None).values():
                rows += group
        store = ManifestStore(
            src.metas,
            rows,
            src.reader(),
            {p: src.arrays[p] for p in paths},
            unsupported=src.unsupported,
        )
        try:
            for path in paths:
                out[path] = read_box(open_manifest_array(store, path), (slice(None),))
        finally:
            store.close()
    return out


def _check_selection(sel, isel, metas: list[ArrayMeta]) -> tuple[dict, dict]:
    sel, isel = dict(sel or {}), dict(isel or {})
    both = [d for d in sel if d in isel]
    if both:
        raise ValueError(
            f"dimension(s) {both} are in both sel and isel. "
            "Fix: select each dimension one way."
        )
    known = {d for m in metas for d in m.dims}
    unknown = [d for d in (*sel, *isel) if d not in known]
    if unknown:
        raise ValueError(
            f"unknown dimension(s) {unknown}; the variables have {sorted(known)}. "
            "Fix: select only existing dimensions."
        )
    return sel, isel


def _mismatch(variables, arrays) -> ZarrGridMismatch:
    shown = "; ".join(f"{v}{arrays[v].dims}={arrays[v].shape}" for v in variables)
    return ZarrGridMismatch(
        f"output='table' needs variables on the same grid, but they differ: {shown}",
        fix="select variables with the same dimensions, or read them separately",
    )


# --- the plan ------------------------------------------------------------------------


@dataclass(frozen=True)
class _Span:
    """The labels of one coordinate over the index range a read touches.

    A task needs the labels of its own cells only, so the closure carries the span the
    selection covers (``values[i - offset]`` is the label at index ``i``), not the whole
    coordinate array: a day of an hourly series ships a day of labels, not forty years.
    ``mask`` marks missing labels (a table gives them NULL).
    """

    offset: int
    values: np.ndarray
    mask: np.ndarray | None = None

    def at(self, pos):
        return self.values[pos - self.offset]

    def missing(self, pos):
        return None if self.mask is None else self.mask[pos - self.offset]


@dataclass
class _TaskConfig:
    """Everything a task needs besides its unit; travels in the ``mapInArrow`` closure.

    Nothing here grows with the number of units: the units themselves are rows of the
    DataFrame the tasks read, one per unit, and a task reads only its own.
    """

    kind: str  # "table" | "plain"
    output: str  # "blocks" | "table"
    root: str
    base_root: str | None
    docs: dict[str, bytes]  # the documents of the arrays read
    arrays: dict[str, ArrayMeta]  # the arrays read
    axes: dict[str, tuple[AxisSel, ...]]
    coords: dict[str, _Span]  # blocks: decoded labels by coordinate array
    labels: dict[str, _Span]  # table: typed labels, with their masks
    dim_coords: dict[
        str, tuple[str | None, ...]
    ]  # per variable, per dim: coordinate path
    variables: tuple[str, ...]
    decode: bool
    flag: bool | None
    batch_bytes: int  # a batch is flushed once its cells reach this many bytes
    columns: tuple[tuple[str, str], ...] = ()  # table: (name, kind) per column
    access: AccessConfig | None = (
        None  # how the files are reached; holds the credentials
    )


# table column kinds by (numpy kind, itemsize): the Spark type each one is, and Arrow's
_KINDS = {
    ("i", 1): "byte",
    ("i", 2): "short",
    ("i", 4): "int",
    ("i", 8): "long",
    ("u", 1): "short",
    ("u", 2): "int",
    ("u", 4): "long",
    ("u", 8): "decimal",
    ("b", 1): "boolean",
    ("f", 2): "float",
    ("f", 4): "float",
    ("f", 8): "double",
    ("M", 8): "timestamp",
}
_TABLE_ARROW = {
    "byte": pa.int8(),
    "short": pa.int16(),
    "int": pa.int32(),
    "long": pa.int64(),
    "decimal": pa.decimal128(20, 0),  # the whole range of a uint64
    "boolean": pa.bool_(),
    "float": pa.float32(),
    "double": pa.float64(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}
_ARROW_NUMPY = {
    "byte": "int8",
    "short": "int16",
    "int": "int32",
    "long": "int64",
    "float": "float32",
    "double": "float64",
}
_ARROW_WIDTH = {
    "byte": 1,
    "short": 2,
    "int": 4,
    "long": 8,
    "decimal": 16,
    "boolean": 1,
    "float": 4,
    "double": 8,
    "timestamp": 8,
}


def _spark_type(kind: str):
    from pyspark.sql import types as T

    return {
        "byte": T.ByteType(),
        "short": T.ShortType(),
        "int": T.IntegerType(),
        "long": T.LongType(),
        "decimal": T.DecimalType(20, 0),
        "boolean": T.BooleanType(),
        "float": T.FloatType(),
        "double": T.DoubleType(),
        "timestamp": T.TimestampType(),
    }[kind]


def _table_struct(cfg: _TaskConfig):
    """The Spark schema of a ``output="table"`` read."""
    from pyspark.sql import types as T

    return T.StructType(
        [T.StructField(n, _spark_type(k), True) for n, k in cfg.columns]
    )


def _column_kind(name: str, dtype: np.dtype) -> str:
    """The table column kind for values of ``dtype``; complex and the like are refused."""
    kind = _KINDS.get((dtype.kind, dtype.itemsize))
    if kind is None:
        what = "complex numbers" if dtype.kind == "c" else f"{dtype} values"
        raise ZarrUnsupported(
            f"{name!r} holds {what}, which a table has no column for",
            fix="read it as blocks, or leave it out of variables",
        )
    return kind


def _table_columns(variables, arrays, dim_coords, typed, decode, flag):
    """``(name, kind)`` per table column: the dimensions, then the variables.

    A column keeps the type of its values (``_KINDS``): integers stay integers, booleans
    booleans, float32 and float16 FLOAT, float64 DOUBLE, times TIMESTAMP. A dimension with
    no coordinate array is labelled by position (BIGINT).
    """
    ref = arrays[variables[0]]
    columns = []
    for dim, cpath in zip(ref.dims, dim_coords[variables[0]]):
        if cpath is None:
            columns.append((dim, "long"))
        else:
            columns.append((dim, _column_kind(dim, typed[cpath][0].dtype)))
    names = {c for c, _ in columns}
    for v in variables:
        if v in names:
            raise ValueError(
                f"variable {v!r} has the name of a dimension, so a table cannot hold both. "
                "Fix: leave the coordinate out of variables, or read it as blocks."
            )
        meta = arrays[v]
        probe = np.zeros((1,) * len(meta.shape), dtype=np.dtype(meta.dtype))
        columns.append(
            (v, _column_kind(v, _table_values(probe, meta, decode, flag)[0].dtype))
        )
    return tuple(columns)


def _label_spans(values_by_path, variables, dim_coords, axes):
    """A ``_Span`` per coordinate: the labels over the indices the selection touches."""
    extent: dict[str, list[int]] = {}
    for v in variables:
        for ax, cpath in zip(axes[v], dim_coords[v]):
            if cpath not in values_by_path:
                continue
            lo_hi = extent.setdefault(cpath, [None, None])
            for start, stop in ax.ranges:
                lo_hi[0] = start if lo_hi[0] is None else min(lo_hi[0], start)
                lo_hi[1] = stop if lo_hi[1] is None else max(lo_hi[1], stop)
    spans = {}
    for cpath, (lo, hi) in extent.items():
        values, mask = values_by_path[cpath]
        lo, hi = (0, 0) if lo is None else (lo, hi)
        spans[cpath] = _Span(
            lo,
            np.array(values[lo:hi]),
            None if mask is None else np.array(mask[lo:hi]),
        )
    return spans


def _resolve_axes(src: _Source, variables, sel, isel, decode, flag):
    """``(dim_coords, stored, coords, axes)`` of a read of ``variables``: the coordinate
    array of each dimension, the stored and decoded values of those that can be read, and
    the selection along each dimension of each variable (``_select.resolve``)."""
    arrays = src.arrays
    sel, isel = _check_selection(sel, isel, [arrays[v] for v in variables])

    dim_coords: dict[str, tuple[str | None, ...]] = {}
    for v in variables:
        dim_coords[v] = tuple(_coordinate_of(d, v, src) for d in arrays[v].dims)
    need = {c for cs in dim_coords.values() for c in cs if c} - set(src.unsupported)
    stored = _read_coordinates(src, need)
    coords = {p: _decode_values(v, arrays[p], decode, flag) for p, v in stored.items()}
    for v in variables:
        for dim, size, cpath in zip(arrays[v].dims, arrays[v].shape, dim_coords[v]):
            if cpath in coords and coords[cpath].shape != (size,):
                raise ZarrError(
                    f"coordinate {cpath!r} has {coords[cpath].shape[0]} labels but "
                    f"{v!r} has {size} cells along {dim!r}",
                    fix="correct the store's metadata"
                    + (", or run gbx_zarr_sync" if src.kind == "table" else ""),
                )

    axes: dict[str, list[AxisSel]] = {}
    for v in variables:
        meta = arrays[v]
        mine_sel = {d: x for d, x in sel.items() if d in meta.dims}
        mine_isel = {d: x for d, x in isel.items() if d in meta.dims}
        for d, cpath in zip(meta.dims, dim_coords[v]):
            if d in mine_sel and cpath in src.unsupported:
                raise ZarrUnsupported(
                    f"array {cpath!r} cannot be decoded, so sel has no labels for "
                    f"{d!r}: {src.unsupported[cpath]}",
                    fix=f"select {d!r} by position with isel",
                )
        axes[v] = resolve(
            meta.dims,
            meta.shape,
            {d: coords[c] for d, c in zip(meta.dims, dim_coords[v]) if c in coords},
            mine_sel,
            mine_isel,
            coord_attrs={
                d: arrays[c].attrs
                for d, c in zip(meta.dims, dim_coords[v])
                if c in arrays
            },
        )
    return dim_coords, stored, coords, axes


def _plan(
    spark, src: _Source, variables, sel, isel, output, decode, flag, *, units=True
):
    """``(units, config)`` for reading ``variables``, or ``([], config)`` for nothing.

    ``units=False`` returns the config alone (``[]`` for the units): a task that is handed
    its unit still needs the config, and has no use for the units it was not handed.
    """
    arrays = src.arrays
    dim_coords, stored, coords, axes = _resolve_axes(
        src, variables, sel, isel, decode, flag
    )

    columns, typed = (), {}
    if output == "table":
        ref = arrays[variables[0]]
        if any(
            (arrays[v].dims, arrays[v].shape) != (ref.dims, ref.shape)
            for v in variables
        ):
            raise _mismatch(variables, arrays)
        for dim, cpath in zip(ref.dims, dim_coords[variables[0]]):
            if cpath in src.unsupported:
                raise ZarrUnsupported(
                    f"array {cpath!r} cannot be decoded, so the table has no labels for "
                    f"{dim!r}: {src.unsupported[cpath]}",
                    fix="read it as blocks, or select the variables by position",
                )
        typed = {
            p: _table_values(v, arrays[p], decode, flag) for p, v in stored.items()
        }
        columns = _table_columns(variables, arrays, dim_coords, typed, decode, flag)

    budget = _budget_bytes()
    planned: list[Unit] = []
    if units and output == "blocks":
        for v in variables:
            meta = arrays[v]
            touched = touched_chunks(axes[v], meta.chunks)
            by_key = src.rows(meta, sorted({unit_key(meta, c) for c in touched}))
            planned += plan_units(meta, axes[v], by_key, budget)
    elif units:
        planned = _plan_table(src, variables, axes[variables[0]], budget)

    read = {v: arrays[v] for v in variables}
    cfg = _TaskConfig(
        kind=src.kind,
        output=output,
        root=src.root,
        base_root=src.base_root,
        docs={k: raw for k, raw in src.metas.items() if k.rpartition("/")[0] in read},
        arrays=read,
        axes={v: tuple(a) for v, a in axes.items()},
        coords=(
            _label_spans(
                {p: (c, None) for p, c in coords.items()}, variables, dim_coords, axes
            )
            if output == "blocks"
            else {}
        ),
        labels=_label_spans(typed, variables, dim_coords, axes),
        dim_coords=dim_coords,
        variables=tuple(variables),
        decode=decode,
        flag=flag,
        batch_bytes=max(1, budget // _BATCH_SHARE),
        columns=columns,
    )
    return planned, cfg


def _plan_table(src: _Source, variables, axes, budget) -> list[Unit]:
    """Units that cover the same cells of every variable.

    The first variable's chunks cut the grid. Each unit also carries the rows of the other
    variables' chunks its cells lie in (their chunking may differ from the first's).
    """
    arrays = src.arrays
    ref = arrays[variables[0]]
    touched = touched_chunks(axes, ref.chunks)
    ref_rows = src.rows(ref, sorted({unit_key(ref, c) for c in touched}))
    units = plan_units(ref, axes, ref_rows, max(1, budget // len(variables)))
    if len(variables) == 1 or src.kind == "plain":
        return units
    boxes = [[b for c in u.chunks for b in boxes_of(ref, axes, c)] for u in units]
    rows_of = {}
    for v in variables[1:]:
        keys = {k for group in boxes for k in keys_of_boxes(arrays[v], group)}
        rows_of[v] = src.rows(arrays[v], sorted(keys))
    return [
        Unit(
            u.array,
            u.chunks,
            (
                *u.rows,
                *(
                    r
                    for v in variables[1:]
                    for r in rows_for_boxes(arrays[v], group, rows_of[v])
                ),
            ),
        )
        for u, group in zip(units, boxes)
    ]


# --- tasks ---------------------------------------------------------------------------


class _UnitReader:
    """Opens what a unit reads: a store built from its rows, or the plain arrays."""

    def __init__(self, cfg: _TaskConfig, unit: Unit, engine: str = "zarr"):
        self.cfg, self.store, self._open, self._engine = cfg, None, {}, engine
        if cfg.kind == "table":
            from databricks.labs.gbx.zarr._store import ManifestStore

            reader = reader_for(cfg.access, cfg.root, cfg.base_root)
            self.store = ManifestStore(cfg.docs, list(unit.rows), reader, cfg.arrays)

    def array(self, name: str):
        from databricks.labs.gbx.zarr._engine import (
            open_manifest_array,
            open_plain_array,
        )

        arr = self._open.get(name)
        if arr is None:
            if self.store is not None:
                arr = open_manifest_array(self.store, name)
            else:  # tensorstore regresses inside tasks (spike), so tasks pass "zarr"
                arr = open_plain_array(self.cfg.root, name, engine=self._engine)
            self._open[name] = arr
        return arr

    def stored(self, meta: ArrayMeta, box) -> np.ndarray:
        from databricks.labs.gbx.zarr._engine import read_box

        return read_box(self.array(meta.name), tuple(slice(a, b) for a, b in box))

    def read(self, meta: ArrayMeta, box) -> np.ndarray:
        return _decode_values(
            self.stored(meta, box), meta, self.cfg.decode, self.cfg.flag
        )

    def read_table(self, meta: ArrayMeta, box):
        return _table_values(
            self.stored(meta, box), meta, self.cfg.decode, self.cfg.flag
        )

    def close(self) -> None:
        if self.store is not None:
            self.store.close()


def _run_unit(
    cfg: _TaskConfig, unit: Unit, engine: str = "zarr"
) -> Iterator[pa.RecordBatch]:
    """The batches of one unit. ``engine`` is how a plain store's arrays are opened."""
    reader = _UnitReader(cfg, unit, engine)
    try:
        for name in (unit.array,) if cfg.output == "blocks" else cfg.variables:
            reader.array(name)  # open before the threads start
        if cfg.output == "blocks":
            yield from _block_batches(cfg, unit, reader)
        else:
            yield from _table_batches(cfg, unit, reader)
    finally:
        reader.close()


def _cells(block: np.ndarray) -> memoryview:
    """The cells of a C-order block as bytes Arrow can read in place (no copy of its own)."""
    return memoryview(block.reshape(-1).view(np.uint8))


def _label(span: _Span | None, index: int) -> str | None:
    return None if span is None else str(span.at(index))


def _fill_text(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, np.datetime64):
        return str(v)
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(v, complex):
        return str(v)
    return json.dumps(v)


def _block_fill(cfg: _TaskConfig, meta: ArrayMeta) -> str | None:
    """What a cell nothing was written to holds in this array's blocks."""
    fill = meta.fill_value
    if fill is None:
        return None
    if cfg.decode:
        probe = np.full((1,) * len(meta.shape), fill, dtype=np.dtype(meta.dtype))
        fill = _decode_values(probe, meta, True, cfg.flag).reshape(-1)[0]
    return _fill_text(fill)


def _block_attrs(cfg: _TaskConfig, meta: ArrayMeta) -> str:
    attrs = dict(meta.attrs)
    if cfg.decode:  # the decoding has consumed these
        drop = set(_PACKING_ATTRS) | (
            set(_TIME_ATTRS) if is_time_coord(meta) else set()
        )
        attrs = {k: v for k, v in attrs.items() if k not in drop}
    return json.dumps(attrs, sort_keys=True, default=str)


def _unit_boxes(cfg: _TaskConfig, unit: Unit) -> list:
    meta = cfg.arrays[unit.array]
    axes = list(cfg.axes[unit.array])
    return [box for c in unit.chunks for box in boxes_of(meta, axes, c)]


def _block_batches(cfg, unit, reader) -> Iterator[pa.RecordBatch]:
    """Batches of blocks: at most ``_BATCH_ROWS`` rows and ``cfg.batch_bytes`` of cells.

    Each block goes into Arrow as soon as it is decoded and is freed there, so a task holds
    one batch (and the few blocks being decoded), not a unit's worth of blocks beside a copy.
    """
    meta = cfg.arrays[unit.array]
    boxes = _unit_boxes(cfg, unit)
    fill, attrs = _block_fill(cfg, meta), _block_attrs(cfg, meta)
    spans = [cfg.coords.get(c) if c else None for c in cfg.dim_coords[unit.array]]
    rows: list[dict] = []
    data: list[pa.Array] = []
    held = 0

    def flush() -> pa.RecordBatch:
        columns = [
            (
                pa.concat_arrays(data)
                if f.name == "data"
                else pa.array([r[f.name] for r in rows], type=f.type)
            )
            for f in _BLOCKS_ARROW
        ]
        return pa.RecordBatch.from_arrays(columns, schema=_BLOCKS_ARROW)

    with ThreadPoolExecutor(max_workers=max(1, min(_READ_THREADS, len(boxes)))) as pool:
        for lo in range(0, len(boxes), _READ_THREADS):
            wave = boxes[lo : lo + _READ_THREADS]
            for box, block in zip(wave, pool.map(lambda b: reader.read(meta, b), wave)):
                rows.append(
                    {
                        "variable": meta.name,
                        "dims": list(meta.dims),
                        "start": [a for a, _ in box],
                        "stop": [z for _, z in box],
                        "coord_start": [_label(s, a) for s, (a, _) in zip(spans, box)],
                        "coord_stop": [
                            _label(s, z - 1) for s, (_, z) in zip(spans, box)
                        ],
                        "dtype": block.dtype.str,
                        "shape": list(block.shape),
                        "fill_value": fill,
                        "attrs": attrs,
                    }
                )
                data.append(pa.array([_cells(block)], type=pa.binary()))
                held += block.nbytes
                del block  # the Arrow array holds its own copy
                if len(rows) >= _BATCH_ROWS or held >= cfg.batch_bytes:
                    yield flush()
                    rows, data, held = [], [], 0
        if rows:
            yield flush()


def _arrow_column(values, kind: str, mask=None) -> pa.Array:
    """``values`` as an Arrow column of table kind ``kind``; cells under ``mask`` are NULL."""
    values = np.asarray(values)
    if kind == "timestamp":
        us = values.astype("datetime64[us]")
        gone = np.isnat(us)
        return pa.array(
            us.astype("int64"),
            type=_TABLE_ARROW["timestamp"],
            mask=gone if mask is None else gone | mask,
        )
    if kind == "decimal":  # uint64: DECIMAL(20,0) holds all of it
        ints = pa.array(
            values.astype("uint64", copy=False), type=pa.uint64(), mask=mask
        )
        return ints.cast(_TABLE_ARROW["decimal"])
    if kind == "boolean":
        return pa.array(values.astype(bool, copy=False), type=pa.bool_(), mask=mask)
    return pa.array(
        values.astype(_ARROW_NUMPY[kind], copy=False),
        type=_TABLE_ARROW[kind],
        mask=mask,
    )


def _table_batches(cfg, unit, reader) -> Iterator[pa.RecordBatch]:
    ref = cfg.arrays[unit.array]
    schema = pa.schema([(name, _TABLE_ARROW[kind]) for name, kind in cfg.columns])
    ndim = len(ref.dims)
    spans = [cfg.labels.get(c) if c else None for c in cfg.dim_coords[unit.array]]
    row_bytes = max(1, sum(_ARROW_WIDTH[kind] for _, kind in cfg.columns))
    step = max(1, min(_TABLE_BATCH_ROWS, cfg.batch_bytes // row_bytes))
    boxes = _unit_boxes(cfg, unit)
    with ThreadPoolExecutor(
        max_workers=max(1, min(_READ_THREADS, len(cfg.variables)))
    ) as pool:
        for box in boxes:
            shape = tuple(z - a for a, z in box)
            n = math.prod(shape)
            read = list(
                pool.map(lambda v: reader.read_table(cfg.arrays[v], box), cfg.variables)
            )
            values = [v.reshape(-1) for v, _ in read]
            masks = [None if m is None else m.reshape(-1) for _, m in read]
            del read
            for lo in range(0, n, step):
                hi = min(n, lo + step)
                at = np.unravel_index(np.arange(lo, hi), shape) if ndim else ()
                cols = []
                for d in range(ndim):
                    kind = cfg.columns[d][1]
                    pos = box[d][0] + at[d]
                    if spans[d] is None:
                        cols.append(_arrow_column(pos, kind))
                    else:
                        cols.append(
                            _arrow_column(spans[d].at(pos), kind, spans[d].missing(pos))
                        )
                for (name, kind), v, m in zip(cfg.columns[ndim:], values, masks):
                    cols.append(
                        _arrow_column(v[lo:hi], kind, None if m is None else m[lo:hi])
                    )
                yield pa.RecordBatch.from_arrays(cols, schema=schema)
            del values, masks


def _work_frame(spark, units: list[Unit]):
    """One row ``(id, unit)`` per unit, in exactly one partition each.

    The units are data, not part of the task closure, and each task reads only its own. They
    are joined to a ``spark.range(n, numPartitions=n)`` scan: a Range scan puts one id in
    each of the n partitions and the broadcast join keeps those partitions, so no shuffle
    can leave one empty or double another up, and nothing for Serverless's AQE to coalesce.
    (Hashing the ids into n partitions with ``repartition(n, id)`` puts several units in one
    partition and leaves others empty.) The units are built from a plain list of tuples: an
    Arrow table conversion drops the rows that follow an empty chunk.

    The join broadcasts the whole ``(id, unit)`` table, every unit's manifest rows
    included, to every executor: once per executor, not once per task. The cost grows with
    the executors times all the rows a read touches, and a read large enough to pass Spark's
    broadcast size limit fails loudly rather than reading wrong data.
    """
    from pyspark.sql import functions as F

    n = len(units)
    work = spark.createDataFrame(
        [(i, u.to_json()) for i, u in enumerate(units)], "id BIGINT, unit STRING"
    )
    return spark.range(n, numPartitions=n).join(F.broadcast(work), "id")


def _task_function(cfg: _TaskConfig):
    """The function ``mapInArrow`` runs: each input row is one unit, as JSON."""

    def run(batches):
        for batch in batches:
            for text in batch.column("unit").to_pylist():
                yield from _run_unit(cfg, Unit.from_json(text))

    return run


# --- the public function -------------------------------------------------------------


def gbx_zarr_read(
    spark,
    source: str,
    *,
    source_type: str = "auto",
    variables=None,
    sel=None,
    isel=None,
    output: str = "blocks",
    version_as_of=None,
    timestamp_as_of=None,
    access: str = "auto",
    decode: bool = True,
    virtual_tiles: bool = True,
    band_dim=None,
    tile_size=None,
    use_zarr_fill_value_as_mask: bool | None = None,
):
    """Read a Zarr store, or a selection of it, into a DataFrame.

    ``source`` is the manifest table of a managed or registered store, or the path of a
    plain store; ``source_type="auto"`` takes a string that starts with ``/`` or has a URI
    scheme for a path. A table read is pinned to one version: the latest, or
    ``version_as_of`` (a version number or ``"tag:<name>"``) or ``timestamp_as_of``. A plain
    store has no versions.

    ``variables`` is a name or a list (default: every gridded variable; coordinate arrays and
    scalars are left out). ``sel`` selects by label and ``isel`` by position: a dict of
    dimension to a scalar, a slice, or a list. Label slices are inclusive and follow the
    axis order, a longitude slice may cross the seam, and times may be ISO strings; see
    ``_select.resolve``. A scalar keeps its dimension, with length one. Variables that lack
    a selected dimension are read whole along the others.

    ``output="blocks"`` returns ``BLOCKS_SCHEMA``: one row per variable and touched chunk,
    holding that chunk's selected cells as C-order ``data`` with their ``start``/``stop`` in
    the array, the first and last label of each dimension, ``dtype``, ``shape``, the
    ``fill_value`` a never-written cell holds, and the array's attributes as JSON.
    ``block_to_numpy`` and ``block_to_xarray`` turn a row into an array. With ``decode=True``
    the CF conventions are applied (scale and offset, missing values to NaN, times to
    ``datetime64[ns]``) and the attributes they consumed are dropped; ``decode=False``
    returns the stored values. ``output="table"`` returns one row per selected cell: a
    column per dimension (its labels, or the position as ``BIGINT`` when the dimension has
    no coordinate array) and a column per variable. A column keeps the type of its values:
    int8 ``TINYINT``, int16 and uint8 ``SMALLINT``, int32 and uint16 ``INT``, int64 and
    uint32 ``BIGINT``, uint64 ``DECIMAL(20,0)``, bool ``BOOLEAN``, float16 and float32
    ``FLOAT``, float64 ``DOUBLE``, times ``TIMESTAMP`` (UTC, to the microsecond). A packed
    array (``scale_factor``, ``add_offset``) decodes to floating point, so its column is
    ``FLOAT`` or ``DOUBLE``. Missing data is ``NULL``: a cell equal to the ``_FillValue``
    or ``missing_value`` (or to the array ``fill_value`` where
    ``use_zarr_fill_value_as_mask`` applies), found on the stored values before any
    scaling or cast; a NaN that is data beside a numeric fill stays NaN. (An integer array
    read as booleans through a ``dtype`` attribute of ``"bool"`` is masked on its integers,
    so a cell equal to the fill is ``NULL`` here where blocks, following xarray, cast the
    masked cell's NaN to True.) Complex arrays raise
    ``ZarrUnsupported``: read them as blocks. With ``decode=False`` the table holds the
    stored values and nothing is ``NULL``. The variables must share their dimensions and
    shape. Row order is not defined.

    ``use_zarr_fill_value_as_mask`` is xarray's option of that name: ``None`` masks on the
    array ``fill_value`` for Zarr v2 only, ``False`` keeps an integer v2 array's zeros.

    ``output="tiles"`` returns GeoBrix tiles, a ``(source, tile)`` DataFrame like the other
    raster readers, so every ``rst_*`` function applies to Zarr data without converting the
    cube to GeoTIFF. Each selected 2-D (latitude, longitude) plane of each variable is cut
    into windows, one inner chunk each (``tile_size=(width, height)`` in cells overrides
    this; a window never crosses the longitude seam), and every selected position along the
    other dimensions makes its own tile (``sliceDims``, ``sliceCoord_<dim>`` in the tile
    metadata, as the NetCDF reader writes them). ``band_dim`` stacks one dimension as the
    bands of one tile instead. With ``virtual_tiles=True`` (the default) a tile holds no
    pixels: it names its window and the storage objects behind it, and reads them when a
    function opens it, so a table of tiles can be saved and read back by tasks that have
    no session; the store must stay on a Volume and, for a table, the pinned version
    readable. ``virtual_tiles=False`` reads each tile now and holds its GeoTIFF bytes.
    Tiles are north-up (rows flipped when latitude ascends); the CRS is the array's
    ``grid_mapping``, or EPSG:4326 for a plain latitude/longitude grid; NoData is NaN for
    floating-point cells. The coordinates must be a regular grid over the selected cells
    (a constant step; what lies outside the selection does not matter): an irregular or
    curvilinear one raises ``ZarrUnsupported`` (read it as blocks or a table). By default
    a variable with no such grid is left out, with a warning; one named in ``variables``
    raises.

    ``access`` is how the tasks reach the store's files: ``"fuse"`` reads them through the
    Volume mount; ``"direct"`` reads byte ranges straight from the Volume's cloud storage with
    temporary credentials Unity Catalog vends to the driver for this call (many small ranges
    are several times faster than through the mount), and raises ``ZarrAccessUnavailable``
    where they cannot be had (Serverless, no SDK, no permission, a path that is not on a
    Volume); ``"auto"`` (the default) is direct where the credentials can be had and the
    mount everywhere else, without an error. A plain-path store is read through its path (FUSE
    only), so ``"direct"`` raises for it: register the store to use direct access. Virtual
    tiles are opened later, over the mount, so ``"direct"`` needs ``virtual_tiles=False``.

    Direct access sends short-lived (at most one hour), Volume-scoped credentials to the
    executors. Under memory pressure Spark may write task data, those credentials included,
    to the local disk of the executors and of the driver: turn on local-disk encryption on
    clusters that use direct access. ``access="auto"`` is direct access on dedicated
    compute, where Unity Catalog can issue the credentials, so this applies to the default
    too; ``access="fuse"`` sends no credentials.

    An array GeoBrix cannot decode (a string coordinate, or one whose metadata is corrupt)
    does not fail the read, on a table or a plain path: it is left out, its dimension gets
    no labels, and it is named only when you ask for it, select by its labels, or need its
    labels for a table.

    Errors raised while planning on the driver (a bad argument, an unknown variable or
    dimension, an array that cannot be decoded, a version that does not exist) keep their
    ``Zarr*`` classes. The DataFrame is lazy, so an error raised while Spark runs the read
    (a file that has gone missing, a chunk that cannot be decoded) arrives from the action
    that triggers it, ``collect`` or ``write`` for example, as a
    ``pyspark.errors.PythonException``. Its message keeps the GeoBrix error name and the
    ``Fix:`` line, for example ``ZarrStoreDrift: ... Fix: run gbx_zarr_sync ...``.
    """
    if output not in _OUTPUTS:
        raise ValueError(
            f"output must be one of {_OUTPUTS}, got {output!r}. "
            "Fix: pass 'blocks', 'tiles' or 'table'."
        )
    check_access(access)
    check_version_args(version_as_of, timestamp_as_of)
    if output != "tiles":
        for name, value in (("band_dim", band_dim), ("tile_size", tile_size)):
            if value is not None:
                raise ValueError(
                    f"{name} applies to output='tiles', and this read is output={output!r}. "
                    f"Fix: drop {name}."
                )
    _check_mask_flag(use_zarr_fill_value_as_mask)
    kind = classify_source(source, source_type)
    if kind == "path" and access == "direct":
        raise plain_direct_error(source)
    if output == "tiles":
        from databricks.labs.gbx.zarr._tiles import check_tile_args

        tile_size = check_tile_args(band_dim, tile_size)
        if not isinstance(virtual_tiles, bool):
            raise ValueError(
                f"virtual_tiles must be True or False, got {virtual_tiles!r}. "
                "Fix: pass virtual_tiles=True for tiles that read their chunks when "
                "opened, or False for tiles that hold their bytes."
            )
        if virtual_tiles and access == "direct":
            raise ZarrAccessUnavailable(
                "virtual tiles read their chunks when a function opens them, in a later "
                "job and without this call's credentials, so they cannot use direct "
                "storage access",
                fix="use virtual_tiles=False to read the tiles now with direct access, or "
                'access="auto" or "fuse"',
            )

    src = _open_source(spark, source, kind, version_as_of, timestamp_as_of)
    names = _choose_variables(variables, src)
    if output == "tiles":
        plans = _plan_tile_set(
            spark,
            src,
            names,
            variables is not None,
            sel,
            isel,
            decode,
            use_zarr_fill_value_as_mask,
            band_dim,
            tile_size,
        )
        # a virtual tile reads nothing now, and is opened later over the mount
        reach = FUSE if virtual_tiles else source_access(spark, src, access)
        return _tiles_frame(spark, plans, virtual_tiles, reach)
    units, cfg = _plan(
        spark, src, names, sel, isel, output, decode, use_zarr_fill_value_as_mask
    )
    reach = source_access(spark, src, access)
    schema = BLOCKS_SCHEMA if output == "blocks" else _table_struct(cfg)
    if not units:
        return spark.createDataFrame([], schema)
    task, _ = bounded_closure(
        spark, reach, lambda a: _task_function(dataclasses.replace(cfg, access=a))
    )
    return _work_frame(spark, units).mapInArrow(task, schema)


# --- tiles ---------------------------------------------------------------------------

_TILES_PER_TASK = 2048  # virtual tiles one task makes
_MATERIALIZED_PER_TASK = 64  # tiles a task reads and encodes when virtual_tiles=False
_TILE_BATCH = 512  # virtual tiles per Arrow batch
_MATERIALIZED_BATCH = 8  # materialized tiles per Arrow batch (each holds its bytes)
_WINDOW_ARROW = pa.struct(
    [
        ("col_off", pa.int32()),
        ("row_off", pa.int32()),
        ("width", pa.int32()),
        ("height", pa.int32()),
    ]
)
_TILE_ARROW = pa.struct(
    [
        ("cellid", pa.int64()),
        ("raster", pa.binary()),
        ("path", pa.string()),
        ("path_mode", pa.string()),
        ("window", _WINDOW_ARROW),
        ("clip_polygon", pa.binary()),
        ("clip_crs", pa.string()),
        ("crs", pa.string()),
        ("metadata", pa.map_(pa.string(), pa.string())),
    ]
)
_TILES_ARROW = pa.schema([("source", pa.string()), ("tile", _TILE_ARROW)])


def _raw_attrs(metas: dict[str, bytes], path: str) -> dict | None:
    """The attributes of the node at ``path`` read from its documents, or ``None``."""
    try:
        if f"{path}/zarr.json" in metas:
            return dict(json.loads(metas[f"{path}/zarr.json"]).get("attributes") or {})
        if f"{path}/.zattrs" in metas:
            return dict(json.loads(metas[f"{path}/.zattrs"]))
    except (ValueError, TypeError, AttributeError):
        pass
    return None


def _grid_mapping_attrs(src: _Source, meta: ArrayMeta) -> dict | None:
    """The attributes of the CF grid-mapping variable ``meta`` names, if the store has it."""
    name = meta.attrs.get("grid_mapping")
    if not isinstance(name, str) or not name.split():
        return None
    name = name.split()[0].rstrip(":")  # "crs", or "crs: x y"
    parent = meta.name.rpartition("/")[0]
    for path in ([f"{parent}/{name}"] if parent else []) + [name]:
        if path in src.arrays:
            return dict(src.arrays[path].attrs)
        found = _raw_attrs(src.metas, path)
        if found is not None:
            return found
    return None


def _plan_tile_set(
    spark,
    src,
    names,
    explicit,
    sel,
    isel,
    decode,
    flag,
    band_dim,
    tile_size,
    cap_bytes=None,
):
    """The ``TilePlan`` of each of ``names`` that has a regular 2-D grid.

    A variable named in ``variables`` (``explicit``) that cannot be tiled raises; one picked
    by default is left out with a warning, as a variable GeoBrix cannot decode is.
    ``cap_bytes`` is the largest tile a task may hold, for a caller that has no session to
    measure it with; by default it is measured from ``spark``.
    """
    from databricks.labs.gbx.ds.file_gbx import _connect_aware_lru_sizing
    from databricks.labs.gbx.zarr._tiles import plan_tiles

    arrays = src.arrays
    dim_coords, _, coords, axes = _resolve_axes(src, names, sel, isel, decode, flag)
    if band_dim is not None and not any(band_dim in arrays[v].dims for v in names):
        raise ValueError(
            f"band_dim {band_dim!r} is not a dimension of {names}. "
            "Fix: name a dimension of the variables."
        )
    # the largest tile a task may hold (64 MiB on Spark Connect, 256 MiB on classic): a task
    # cannot measure it, so the driver does, and it travels with the tiles
    cap = _connect_aware_lru_sizing(spark)[0] if cap_bytes is None else cap_bytes
    plans, left_out = [], {}
    for v in names:
        meta = arrays[v]
        kwargs = dict(
            root=src.root,
            base_root=src.base_root,
            table=src.table or "",
            version="" if src.snap is None else src.snap.version,
            tile_size=tile_size,
            band_dim=band_dim,
            coord_attrs={
                d: arrays[c].attrs
                for d, c in zip(meta.dims, dim_coords[v])
                if c in arrays
            },
            grid_mapping=_grid_mapping_attrs(src, meta),
            decode=decode,
            flag=flag,
            cap_bytes=cap,
        )
        by_dim = {d: coords[c] for d, c in zip(meta.dims, dim_coords[v]) if c in coords}
        try:  # decide before fetching any manifest rows
            plan_tiles(meta, by_dim, axes[v], {}, **kwargs)
        except ZarrUnsupported as e:
            if explicit:
                raise
            left_out[v] = e.message
            continue
        touched = touched_chunks(axes[v], meta.chunks)
        rows = src.rows(meta, sorted({unit_key(meta, c) for c in touched}))
        plans.append(plan_tiles(meta, by_dim, axes[v], rows, **kwargs))
    if left_out:
        _LOG.warning(
            "%s: %d variable(s) have no regular 2-D grid and are left out of the tiles: %s",
            src.table or src.root,
            len(left_out),
            "; ".join(f"{a} ({why})" for a, why in left_out.items()),
        )
    if not plans:
        raise ZarrUnsupported(
            "no variable of the store has a regular latitude/longitude grid to tile",
            fix='read it with output="blocks" or output="table"',
        )
    return plans


def _tile_batches(
    plan, ids: list[int], virtual: bool, access=None
) -> Iterator[pa.RecordBatch]:
    from databricks.labs.gbx.zarr._tiles import source_of, tile_access, tile_rows

    if not virtual:
        from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes
        from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    step = _TILE_BATCH if virtual else _MATERIALIZED_BATCH
    for lo in range(0, len(ids), step):
        rows = tile_rows(plan, ids[lo : lo + step])
        if not virtual:
            with tile_access(access):
                rows = [
                    materialize_to_bytes(VirtualTile.from_row(r)).to_row() for r in rows
                ]
        yield pa.RecordBatch.from_pylist(
            [{"source": source_of(r), "tile": r} for r in rows], schema=_TILES_ARROW
        )


def _tile_task(plans, virtual: bool, access=None):
    """The function ``mapInArrow`` runs: tile numbers in, tile rows out. The plans (and the
    ``access`` a materialized tile is read with) are its closure, so the tiles are made
    where they are used and never pass through the driver. A virtual tile carries no
    credentials: it is opened later, over the mount.
    """
    offsets = list(itertools.accumulate((p.n_tiles for p in plans), initial=0))

    def run(batches):
        for batch in batches:
            ids = batch.column("id").to_pylist()
            for plan, lo, hi in zip(plans, offsets, offsets[1:]):
                mine = [i - lo for i in ids if lo <= i < hi]
                if mine:
                    yield from _tile_batches(plan, mine, virtual, access)

    return run


def _tiles_frame(spark, plans, virtual: bool, access=None):
    """The ``(source, tile)`` DataFrame of the plans' tiles, made in tasks."""
    from databricks.labs.gbx.ds.raster import reader_schema_v2

    schema = reader_schema_v2()
    total = sum(p.n_tiles for p in plans)
    if not total:
        return spark.createDataFrame([], schema)
    per_task = _TILES_PER_TASK if virtual else _MATERIALIZED_PER_TASK
    tiles = spark.range(total, numPartitions=-(-total // per_task))
    task, _ = bounded_closure(spark, access, lambda a: _tile_task(plans, virtual, a))
    return tiles.mapInArrow(task, schema)


# --- block helpers -------------------------------------------------------------------


def _field(row, name):
    return row[name]


def block_to_numpy(row) -> np.ndarray:
    """The cells of a block row (a ``Row`` or a dict from ``output="blocks"``) as an array.

    The array has the block's ``shape`` and ``dtype``, in C order, and is a writable copy.
    """
    data = _field(row, "data")
    dtype = np.dtype(_field(row, "dtype"))
    shape = tuple(int(n) for n in _field(row, "shape"))
    return np.frombuffer(data, dtype=dtype).reshape(shape).copy()


def block_to_xarray(row):
    """A block row as an ``xarray.DataArray``: named for the variable, with the block's
    dimensions and attributes. A block carries the first and last label of each
    dimension, not every label, so no coordinates are attached."""
    import xarray as xr

    return xr.DataArray(
        block_to_numpy(row),
        dims=tuple(_field(row, "dims")),
        name=_field(row, "variable"),
        attrs=json.loads(_field(row, "attrs") or "{}"),
    )
