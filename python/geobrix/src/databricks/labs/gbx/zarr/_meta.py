"""Zarr array metadata for v2 and v3 stores, with CF decoding.

``ArrayMeta`` is the one parsed view of an array's metadata document that every store
operation works from: shape, dtype, the inner chunk shape (the read/write unit), the
shard shape when the array is sharded, dimension names, attributes and the chunk key
encoding. ``ArrayMeta.doc`` keeps the raw document (``zarr.json`` for v3; ``.zarray`` and
``.zattrs`` for v2), and that is what ``doc_bytes`` writes back.

CF decoding (scale/offset, missing values, time units) is delegated to xarray.
"""

from __future__ import annotations

import base64
import copy
import dataclasses
import json
import math
import struct
from dataclasses import dataclass

import numpy as np

from databricks.labs.gbx.zarr._keys import KeyEncoding, chunks_per_shard, encoding_for
from databricks.labs.gbx.zarr.errors import ZarrUnsupported

_V3_DTYPES = {
    "bool": "|b1",
    "int8": "|i1",
    "uint8": "|u1",
    "int16": "i2",
    "uint16": "u2",
    "int32": "i4",
    "uint32": "u4",
    "int64": "i8",
    "uint64": "u8",
    "float16": "f2",
    "float32": "f4",
    "float64": "f8",
    "complex64": "c8",
    "complex128": "c16",
}
_DEFAULT_INDEX_CODECS = (
    {"name": "bytes", "configuration": {"endian": "little"}},
    {"name": "crc32c"},
)
_FLOAT_NAMES = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
_STANDARD_CALENDARS = ("standard", "gregorian", "proleptic_gregorian")


@dataclass(frozen=True)
class ShardSpec:
    cps: tuple[int, ...]  # chunks per shard
    inner_codecs: tuple[dict, ...]
    index_codecs: tuple[dict, ...]
    index_location: str  # "end" | "start"


@dataclass(frozen=True)
class ArrayMeta:
    name: str
    zarr_format: int
    shape: tuple[int, ...]
    dtype: str  # numpy dtype string, e.g. "<f4"
    chunks: tuple[int, ...]  # INNER chunk shape (the read/write unit)
    shards: tuple[int, ...] | None  # shard shape, or None when unsharded
    shard: ShardSpec | None
    dims: tuple[str, ...]
    fill_value: object
    attrs: dict
    key_encoding: KeyEncoding
    doc: dict  # raw zarr.json (v3) or {".zarray": ..., ".zattrs": ...} (v2)

    @property
    def grid_shape(self) -> tuple[int, ...]:
        """Number of inner chunks along each dimension: ``ceil(shape / chunks)``."""
        return tuple(-(-s // c) for s, c in zip(self.shape, self.chunks))

    @property
    def itemsize(self) -> int:
        return np.dtype(self.dtype).itemsize


def _float_fill(raw, itemsize: int) -> float:
    """A JSON float fill value: a number, ``"NaN"``/``"Infinity"``/``"-Infinity"``, or the
    big-endian IEEE bit pattern as ``"0x..."`` (Zarr v3)."""
    if isinstance(raw, str):
        if raw in _FLOAT_NAMES:
            return _FLOAT_NAMES[raw]
        if raw.startswith("0x"):
            bits = int(raw, 16).to_bytes(itemsize, "big")
            return float(np.frombuffer(bits, dtype=f">f{itemsize}")[0])
    return float(raw)


def _fill_value(raw, dtype: str):
    """The JSON ``fill_value`` as a Python scalar; ``raw`` (and ``doc``) stay untouched."""
    if raw is None:
        return None
    dt = np.dtype(dtype)
    if dt.kind == "f":
        return _float_fill(raw, dt.itemsize)
    if dt.kind == "c":
        half = dt.itemsize // 2
        return complex(_float_fill(raw[0], half), _float_fill(raw[1], half))
    if dt.kind == "b":
        return bool(raw)
    if dt.kind in "iu":
        return int(raw)
    return raw


def _endian(codecs) -> str:
    for c in codecs:
        if c.get("name") == "bytes":
            return (
                "<"
                if (c.get("configuration") or {}).get("endian", "little") == "little"
                else ">"
            )
    return "<"


def parse_v3(name: str, doc: dict) -> ArrayMeta:
    dt = doc.get("data_type")
    if not isinstance(dt, str) or dt not in _V3_DTYPES:
        raise ZarrUnsupported(
            f"data type {dt!r} of array {name!r} is not supported",
            fix="read it as raw blocks with decode=False, or convert the array to a numeric dtype",
        )
    grid = doc.get("chunk_grid") or {}
    if grid.get("name") != "regular":
        raise ZarrUnsupported(
            f"chunk grid {grid.get('name')!r} of array {name!r} is not supported",
            fix="rewrite the array with a regular chunk grid",
        )
    codecs = tuple(doc.get("codecs") or ())
    outer = tuple(grid["configuration"]["chunk_shape"])
    shard = None
    shards = None
    chunks = outer
    inner_codecs = codecs
    if codecs and codecs[0].get("name") == "sharding_indexed":
        cfg = codecs[0]["configuration"]
        chunks = tuple(cfg["chunk_shape"])
        shards = outer
        inner_codecs = tuple(cfg.get("codecs") or ())
        shard = ShardSpec(
            cps=chunks_per_shard(shards, chunks),
            inner_codecs=inner_codecs,
            index_codecs=tuple(cfg.get("index_codecs") or _DEFAULT_INDEX_CODECS),
            index_location=cfg.get("index_location", "end"),
        )
    base = _V3_DTYPES[dt]
    dtype = base if base[0] == "|" else _endian(inner_codecs) + base
    shape = tuple(doc["shape"])
    return ArrayMeta(
        name=name,
        zarr_format=3,
        shape=shape,
        dtype=dtype,
        chunks=chunks,
        shards=shards,
        shard=shard,
        dims=tuple(
            doc.get("dimension_names") or (f"dim_{i}" for i in range(len(shape)))
        ),
        fill_value=_fill_value(doc.get("fill_value"), dtype),
        attrs=dict(doc.get("attributes") or {}),
        key_encoding=encoding_for(3, doc),
        doc=doc,
    )


def parse_v2(name: str, zarray: dict, zattrs: dict) -> ArrayMeta:
    order = zarray.get("order", "C")
    if order != "C":
        raise ZarrUnsupported(
            f"array {name!r} is stored in {order!r} order, only 'C' order is supported",
            fix="rewrite the array in C order",
        )
    np_dtype = np.dtype(zarray["dtype"])
    if np_dtype.kind in "OUSV":
        raise ZarrUnsupported(
            f"data type {zarray['dtype']!r} of array {name!r} is not supported",
            fix="read it as raw blocks with decode=False, or convert the array to a numeric dtype",
        )
    dtype = np_dtype.str
    shape = tuple(zarray["shape"])
    dims = tuple(
        zattrs.get("_ARRAY_DIMENSIONS") or (f"dim_{i}" for i in range(len(shape)))
    )
    return ArrayMeta(
        name=name,
        zarr_format=2,
        shape=shape,
        dtype=dtype,
        chunks=tuple(zarray["chunks"]),
        shards=None,
        shard=None,
        dims=dims,
        fill_value=_fill_value(zarray.get("fill_value"), dtype),
        # the dimension names live in ``dims``; they are not a user attribute
        attrs={k: v for k, v in zattrs.items() if k != "_ARRAY_DIMENSIONS"},
        key_encoding=encoding_for(2, zarray),
        doc={".zarray": zarray, ".zattrs": zattrs},
    )


def with_shape(meta: ArrayMeta, shape: tuple[int, ...]) -> ArrayMeta:
    """``meta`` with a new array shape, in both the model and its metadata document."""
    shape = tuple(int(s) for s in shape)
    doc = copy.deepcopy(meta.doc)
    if meta.zarr_format == 3:
        doc["shape"] = list(shape)
    else:
        doc[".zarray"]["shape"] = list(shape)
    return dataclasses.replace(meta, shape=shape, doc=doc)


def doc_bytes(meta: ArrayMeta) -> dict[str, bytes]:
    """The metadata documents to write, keyed by file name under the array's prefix."""
    if meta.zarr_format == 3:
        return {"zarr.json": json.dumps(meta.doc).encode()}
    return {
        ".zarray": json.dumps(meta.doc[".zarray"]).encode(),
        ".zattrs": json.dumps(meta.doc[".zattrs"]).encode(),
    }


def inner_meta_doc(meta: ArrayMeta) -> dict:
    """A v3 document for an UNSHARDED array whose chunks are ``meta``'s inner chunks.

    Each inner chunk of a shard is encoded as a standalone chunk of this array: its shape
    is the inner chunk shape and its codecs are the shard's inner codecs.
    """
    if meta.zarr_format != 3:
        raise ValueError(f"array {meta.name!r} is Zarr v{meta.zarr_format}; need v3")
    doc = copy.deepcopy(meta.doc)
    doc["chunk_grid"]["configuration"]["chunk_shape"] = list(meta.chunks)
    if meta.shard is not None:
        doc["codecs"] = copy.deepcopy(list(meta.shard.inner_codecs))
    doc["shape"] = list(meta.chunks)
    return doc


def _cf_fill_attr(raw, dtype: str):
    """xarray stores a Zarr v3 float ``_FillValue`` attribute as base64 of a little-endian
    float64 (JSON cannot hold NaN); anything else is already a plain value."""
    if isinstance(raw, str) and np.dtype(dtype).kind == "f":
        if raw in _FLOAT_NAMES:
            return _FLOAT_NAMES[raw]
        return struct.unpack("<d", base64.standard_b64decode(raw))[0]
    return raw


def _cf_attrs(meta: ArrayMeta) -> dict:
    """The attributes CF decoding sees, following xarray's own Zarr defaults.

    In Zarr v2 the array ``fill_value`` is the missing-data marker (there is no
    ``_FillValue`` attribute), so it becomes ``_FillValue``. In Zarr v3 ``fill_value`` is only
    what unwritten chunks read as (``0`` for every integer array), so only an explicit
    ``_FillValue`` attribute masks.
    """
    attrs = dict(meta.attrs)
    if "_FillValue" in attrs:
        attrs["_FillValue"] = _cf_fill_attr(attrs["_FillValue"], meta.dtype)
    elif (
        meta.zarr_format == 2
        and meta.fill_value is not None
        and np.dtype(meta.dtype).kind != "b"  # masking a boolean yields an object array
    ):
        attrs["_FillValue"] = meta.fill_value
    return attrs


def cf_decode(values: np.ndarray, meta: ArrayMeta) -> np.ndarray:
    """Apply the CF conventions in ``meta.attrs`` (missing values, scale/offset) to ``values``.

    Times are not decoded here; see ``decode_coord``. ``values`` may be any block of the array.
    """
    from xarray import Variable
    from xarray.conventions import decode_cf_variable

    values = np.asarray(values)
    dims = (
        meta.dims
        if len(meta.dims) == values.ndim
        else tuple(f"dim_{i}" for i in range(values.ndim))
    )
    var = Variable(dims, values, _cf_attrs(meta))
    return decode_cf_variable(
        meta.name, var, decode_times=False, decode_timedelta=False
    ).values


def decode_coord(values: np.ndarray, meta: ArrayMeta) -> np.ndarray:
    """Decode a coordinate: CF time units become ``datetime64[ns]``, anything else is returned as is."""
    units = meta.attrs.get("units")
    if not isinstance(units, str) or " since " not in units:
        return values
    calendar = str(meta.attrs.get("calendar") or "standard").lower()
    if calendar not in _STANDARD_CALENDARS:
        raise ZarrUnsupported(
            f"calendar {calendar!r} of coordinate {meta.name!r} is not supported",
            fix="pass decode=False to read the raw time numbers",
        )
    from xarray.coding.times import decode_cf_datetime

    decoded = decode_cf_datetime(values, units, calendar, use_cftime=False)
    return np.asarray(decoded).astype("datetime64[ns]")
