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
import warnings
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
# Names a node (group or array) keeps its own document under, in Zarr v3 and v2.
NODE_DOCS = ("zarr.json", ".zarray", ".zgroup")


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


def _extent(name: str, value, what: str, *, minimum: int) -> tuple[int, ...]:
    """``value`` as a tuple of integers, each at least ``minimum``; anything else is malformed."""
    ok = isinstance(value, (list, tuple)) and all(
        isinstance(n, int) and not isinstance(n, bool) and n >= minimum for n in value
    )
    if not ok:
        raise ZarrUnsupported(
            f"the {what} {value!r} of array {name!r} is not a list of integers "
            f"({'positive' if minimum else 'non-negative'})",
            fix="repair or rewrite the array's metadata",
        )
    return tuple(value)


def _same_rank(name: str, shape, chunks) -> None:
    if len(shape) != len(chunks):
        raise ZarrUnsupported(
            f"array {name!r} has shape {shape} but chunks {chunks}, which differ in length",
            fix="repair or rewrite the array's metadata",
        )


def is_sharded_doc(doc) -> bool:
    """Whether a Zarr v3 array document stores its chunks in shards (its outer codec is
    ``sharding_indexed``). Decided from the document alone, so it holds for an array
    GeoBrix cannot decode as well."""
    codecs = doc.get("codecs") if isinstance(doc, dict) else None
    return (
        isinstance(codecs, (list, tuple))
        and bool(codecs)
        and isinstance(codecs[0], dict)
        and codecs[0].get("name") == "sharding_indexed"
    )


def parse_v3(name: str, doc: dict) -> ArrayMeta:
    # the model owns its document; the caller's dict stays theirs
    doc = copy.deepcopy(doc)
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
    names = [c.get("name") for c in codecs]
    if "sharding_indexed" in names and len(codecs) > 1:
        # Zarr allows array->array codecs before, and bytes->bytes codecs after, an
        # array->bytes codec; ShardSpec and inner_meta_doc have no place for them.
        raise ZarrUnsupported(
            f"array {name!r} has codecs {names} around sharding_indexed, which is not supported",
            fix="rewrite the array so sharding_indexed is its only outer codec; "
            "put compression and other codecs inside it",
        )
    outer = _extent(
        name, grid["configuration"]["chunk_shape"], "chunk shape", minimum=1
    )
    shard = None
    shards = None
    chunks = outer
    inner_codecs = codecs
    if is_sharded_doc(doc):
        cfg = codecs[0]["configuration"]
        chunks = _extent(name, cfg["chunk_shape"], "inner chunk shape", minimum=1)
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
    shape = _extent(name, doc["shape"], "shape", minimum=0)
    _same_rank(name, shape, outer)
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
        attrs=copy.deepcopy(doc.get("attributes") or {}),
        key_encoding=encoding_for(3, doc),
        doc=doc,
    )


def parse_v2(name: str, zarray: dict, zattrs: dict) -> ArrayMeta:
    zarray, zattrs = copy.deepcopy(zarray), copy.deepcopy(zattrs)  # as in parse_v3
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
    shape = _extent(name, zarray["shape"], "shape", minimum=0)
    chunks = _extent(name, zarray["chunks"], "chunk shape", minimum=1)
    _same_rank(name, shape, chunks)
    dims = tuple(
        zattrs.get("_ARRAY_DIMENSIONS") or (f"dim_{i}" for i in range(len(shape)))
    )
    return ArrayMeta(
        name=name,
        zarr_format=2,
        shape=shape,
        dtype=dtype,
        chunks=chunks,
        shards=None,
        shard=None,
        dims=dims,
        fill_value=_fill_value(zarray.get("fill_value"), dtype),
        # the dimension names live in ``dims``; they are not a user attribute
        attrs={
            k: copy.deepcopy(v) for k, v in zattrs.items() if k != "_ARRAY_DIMENSIONS"
        },
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


def isolate_unreadable_groups(
    metas: dict, arrays: dict, unsupported: dict[str, str]
) -> None:
    """Keep a group whose document cannot be read out of ``unsupported``, in place.

    A node document that cannot be parsed (not JSON, a node type that is neither group nor
    array) is recorded in ``unsupported`` as an array nobody can decode, because the
    document alone does not say what the node is. But a node with other nodes under it is
    a group, and it is not an array: it is dropped from ``unsupported``, and every array
    under it moves there with the reason ``group document unreadable``, so asking for one
    is a typed refusal that names the cause and not an "unknown variable". Arrays that were
    unsupported for a reason of their own keep it.

    ``metas`` maps store keys to documents (``t2m/zarr.json``), ``arrays`` the path of each
    array GeoBrix decodes to its ``ArrayMeta``, ``unsupported`` the path of each it cannot
    to the reason.
    """
    parents: set[str] = set()  # every path that has a node under it
    for key in metas:
        node, _, doc = key.rpartition("/")
        if doc not in NODE_DOCS:
            continue
        while "/" in node:
            node = node.rpartition("/")[0]
            if node in parents:
                break  # it, and so all its parents, are in
            parents.add(node)
    groups = {p: why for p, why in unsupported.items() if p in parents}
    if not groups:
        return

    def outermost(path: str):
        found, node = None, path
        while "/" in node:
            node = node.rpartition("/")[0]
            if node in groups:
                found = node
        return found

    for path in [*arrays, *(p for p in unsupported if p not in groups)]:
        group = outermost(path)
        if group is not None:
            why = groups[group].rstrip(".")
            arrays.pop(path, None)
            unsupported.setdefault(
                path,
                f"group document unreadable: {why}, so the arrays under {group!r} "
                "are not read.",
            )
    for group in groups:
        del unsupported[group]


def _cf_fill_attr(raw, meta: ArrayMeta):
    """A Zarr v3 float ``_FillValue`` attribute as a number.

    xarray stores it as base64 of a little-endian float64, because JSON cannot hold NaN;
    ``"NaN"``, ``"Infinity"`` and ``"-Infinity"`` are accepted too. Any other kind of
    value is already a plain number or boolean.
    """
    if not isinstance(raw, str) or np.dtype(meta.dtype).kind != "f":
        return raw
    if raw in _FLOAT_NAMES:
        return _FLOAT_NAMES[raw]
    try:
        return struct.unpack("<d", base64.b64decode(raw, validate=True))[0]
    except (ValueError, struct.error):  # binascii.Error is a ValueError
        raise ZarrUnsupported(
            f"the _FillValue attribute {raw!r} of array {meta.name!r} is not a "
            "base64-encoded float64",
            fix="store _FillValue as a number, or read the array with decode=False",
        ) from None


def _check_mask_flag(flag) -> None:
    if flag is not None and not isinstance(flag, (bool, np.bool_)):
        raise ValueError(
            f"use_zarr_fill_value_as_mask must be True, False or None, got {flag!r}"
        )


def _cf_attrs(meta: ArrayMeta, use_zarr_fill_value_as_mask=None) -> dict:
    """The attributes CF decoding sees: xarray's ``open_zarr`` rules for each Zarr version.

    ``use_zarr_fill_value_as_mask`` is xarray's switch of the same name. When it is true the
    array ``fill_value`` is the missing-data marker: it becomes ``_FillValue`` and overrides
    the attribute. ``None`` (the default) means true for Zarr v2, which has no ``_FillValue``
    attribute convention, and false for v3. A boolean array is never masked (that would
    produce an object array), and a ``fill_value`` of ``null`` masks nothing.

    When it is false ``fill_value`` is only what unwritten chunks read as (``0`` for every
    integer array in v3), so an integer v2 array whose ``fill_value`` is a real value, such as
    ``0``, keeps it. An explicit ``_FillValue`` attribute still masks; a v3 float one is
    decoded from xarray's base64 form first, and a v2 one stays exactly as stored.

    An integer array read as booleans (a ``dtype`` attribute of ``"bool"``) is never masked
    on the array ``fill_value`` either, only on an explicit ``_FillValue`` or
    ``missing_value`` attribute: its ``fill_value`` (zarr's default ``0``) is the stored
    ``False``, and masking it would turn every ``False`` into a missing cell (``NULL`` in a
    table, ``True`` in xarray's blocks). xarray does mask it; GeoBrix does not.
    """
    _check_mask_flag(use_zarr_fill_value_as_mask)
    attrs = dict(meta.attrs)
    mask = (
        meta.zarr_format == 2
        if use_zarr_fill_value_as_mask is None
        else bool(use_zarr_fill_value_as_mask)
    )
    boolean = np.dtype(meta.dtype).kind == "b" or attrs.get("dtype") == "bool"
    if mask and meta.fill_value is not None and not boolean:
        attrs["_FillValue"] = meta.fill_value
    elif meta.zarr_format == 3 and "_FillValue" in attrs:
        attrs["_FillValue"] = _cf_fill_attr(attrs["_FillValue"], meta)
    return attrs


def _native_values(values) -> np.ndarray:
    """``values`` as an array in native byte order: the integer coders below reinterpret
    bytes, which only means the same thing as the numbers in the machine's own order."""
    values = np.asarray(values)
    if not values.dtype.isnative:
        values = values.astype(values.dtype.newbyteorder("="))
    return values


def _unsigned_target(values: np.ndarray, attrs: dict):
    """The integer type xarray's ``_Unsigned`` coder turns ``values`` into, or ``None``.

    ``_Unsigned = "true"`` makes a signed integer the unsigned one of the same width, and
    ``"false"`` an unsigned integer the signed one. Anything else is left alone.
    """
    unsigned, kind = attrs.get("_Unsigned"), values.dtype.kind
    if kind == "i" and unsigned == "true":
        return np.dtype(f"u{values.dtype.itemsize}")
    if kind == "u" and unsigned == "false":
        return np.dtype(f"i{values.dtype.itemsize}")
    return None


def _require_scalar_fill(meta: ArrayMeta, fill) -> None:
    """``_Unsigned`` converts the ``_FillValue`` to the reinterpreted type, which needs one
    value: CF defines ``_FillValue`` as a scalar, and xarray fails on a list with a bare
    ``TypeError``. Several missing values belong in ``missing_value``."""
    if fill is not None and np.ndim(fill) != 0:
        raise ZarrUnsupported(
            f"the _FillValue {fill!r} of array {meta.name!r} is not a single value, but "
            "_Unsigned needs one: CF requires a scalar fill",
            fix="store _FillValue as one value (several missing values belong in "
            "missing_value), or read with decode=False",
        )


def cf_decode(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> np.ndarray:
    """Apply the CF conventions in ``meta.attrs`` (missing values, scale/offset) to ``values``.

    Times are not decoded here; see ``decode_coord``. ``values`` may be any block of the array.
    ``use_zarr_fill_value_as_mask`` is xarray's option of that name: ``None`` masks on the
    array ``fill_value`` for Zarr v2 only (see ``_cf_attrs``).
    """
    from xarray import Variable
    from xarray.conventions import decode_cf_variable

    values = _native_values(values)
    dims = (
        meta.dims
        if len(meta.dims) == values.ndim
        else tuple(f"dim_{i}" for i in range(values.ndim))
    )
    attrs = _cf_attrs(meta, use_zarr_fill_value_as_mask)
    unsigned = _unsigned_target(values, attrs) is not None
    if unsigned:
        _require_scalar_fill(meta, attrs.get("_FillValue"))
    var = Variable(dims, values, attrs)
    try:
        return decode_cf_variable(
            meta.name, var, decode_times=False, decode_timedelta=False
        ).values
    except OverflowError as e:
        if not unsigned:  # not about the fill: it is not ours to word
            raise
        # an _Unsigned array whose fill is not in its storage type
        raise _fill_error(meta, attrs.get("_FillValue"), e) from None


def _fill_error(meta: ArrayMeta, fill, cause) -> ZarrUnsupported:
    return ZarrUnsupported(
        f"the _FillValue {fill!r} of array {meta.name!r} does not fit its stored "
        f"{np.dtype(meta.dtype)} type once _Unsigned is applied ({cause})",
        fix="store _FillValue in the array's stored integer type, or read with decode=False",
    )


def _reinterpret_integers(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> tuple[np.ndarray, dict]:
    """``(values, attrs)`` after xarray's ``_Unsigned`` coder, in native byte order.

    The values are the signed or unsigned integers of the same width (the bits are kept)
    and ``_FillValue`` follows, converted as xarray converts it (the stored signed -1 of an
    unsigned byte is 255), so integer comparisons stay exact. ``_Unsigned`` is consumed.
    Any other value of it, or a non-integer array, is left alone. A boolean ``dtype``
    attribute is still in ``attrs``: see ``cf_reinterpret``.
    """
    values = _native_values(values)
    attrs = _cf_attrs(meta, use_zarr_fill_value_as_mask)
    target = _unsigned_target(values, attrs)
    attrs.pop("_Unsigned", None)
    if target is not None:
        fill = attrs.get("_FillValue")
        _require_scalar_fill(meta, fill)
        if fill is not None:
            try:
                if values.dtype.kind == "i":
                    # the fill is the signed value on disk: the same bits, unsigned
                    attrs["_FillValue"] = (
                        np.array(fill, dtype=values.dtype).view(target).item()
                    )
                else:
                    attrs["_FillValue"] = target.type(fill)
            except (OverflowError, ValueError, TypeError) as e:
                raise _fill_error(meta, fill, e) from None
        values = values.astype(target)
    return values, attrs


def cf_reinterpret(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> tuple[np.ndarray, dict]:
    """``(values, attrs)``: the stored values as xarray's integer and boolean coders see them.

    xarray applies these before it masks or scales, and every reader of raw values must too:

    - ``_Unsigned = "true"`` on a signed integer makes the values the unsigned integers of
      the same width, and ``"false"`` on an unsigned one the signed integers (the bits are
      kept). ``_FillValue`` follows, converted as xarray converts it (the stored signed -1 of
      an unsigned byte is 255), so integer comparisons stay exact. Any other value of
      ``_Unsigned``, or a non-integer array, is left alone.
    - ``dtype = "bool"`` on an integer array makes the values booleans.

    ``attrs`` are the CF attributes (``_cf_attrs``) with ``_Unsigned`` and a boolean
    ``dtype`` consumed and the fill converted. Raises ``ZarrUnsupported`` for a
    ``_FillValue`` that does not fit the stored type or is not a single value.
    """
    values, attrs = _reinterpret_integers(
        values, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
    )
    if attrs.get("dtype") == "bool":
        del attrs["dtype"]
        values = values.astype(bool)
    return values, attrs


def _mask_of(values: np.ndarray, attrs: dict) -> np.ndarray | None:
    """Cells of ``values`` that equal a fill value in ``attrs``; ``None`` for no mask."""
    if values.dtype.kind == "b":
        return None
    fills = []
    for key in ("_FillValue", "missing_value"):
        raw = attrs.get(key)
        if raw is not None:
            fills.extend(np.asarray(raw).reshape(-1).tolist())
    mask = None
    for fill in fills:
        if isinstance(fill, bool) or not isinstance(fill, (int, float)):
            # xarray compares the data with the fill, and a string is never equal to a
            # number: it masks nothing, so neither does this
            continue
        if isinstance(fill, float) and math.isnan(fill):
            hit = np.isnan(values) if values.dtype.kind in "fc" else None
        else:
            hit = values == fill
        if hit is not None:
            mask = hit if mask is None else mask | hit
    return mask


def missing_mask(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> np.ndarray | None:
    """Which cells of ``values`` (as stored) are missing data, or ``None`` when nothing masks.

    A cell is missing when it equals the ``_FillValue`` or one of the ``missing_value``s
    (and, where ``use_zarr_fill_value_as_mask`` applies, the array ``fill_value``; see
    ``_cf_attrs``). The comparison is on the stored values after the ``_Unsigned``
    reinterpretation (``cf_reinterpret``) and before any scaling or boolean cast, so it is
    exact for integers of any size, and an integer array read as booleans (``dtype`` of
    ``"bool"``) is masked on its integers. A NaN fill value matches NaN cells; a NaN that
    is data beside a numeric fill is not missing. A stored boolean array is never masked,
    and a fill that is not a number matches nothing, as in xarray.
    """
    values, attrs = _reinterpret_integers(
        values, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
    )
    return _mask_of(values, attrs)


def cf_unpack(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> tuple[np.ndarray, np.ndarray | None]:
    """``(values, mask)`` for a reader that wants missing data as a mask, not as NaN.

    ``mask`` is ``missing_mask`` (``None`` when nothing is masked): it is found before the
    boolean cast, so a boolean column has NULLs where blocks, following xarray, cast the
    masked cell's NaN to True. ``values`` are the stored values after ``cf_reinterpret``
    (so ``_Unsigned`` and a boolean ``dtype`` are applied, as in ``cf_decode``) and keep
    their integer dtype, and so stay exact, unless the array is packed (``scale_factor``
    or ``add_offset``), when they are decoded to floating point as ``cf_decode`` does. The
    cells under ``mask`` hold no meaningful value.
    """
    stored = _native_values(values)
    values, attrs = _reinterpret_integers(
        stored, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
    )
    mask = _mask_of(values, attrs)
    if "scale_factor" in attrs or "add_offset" in attrs:
        values = cf_decode(
            stored, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
        )
    elif attrs.get("dtype") == "bool":
        values = values.astype(bool)
    return values, mask


_ENCODE_FIX = (
    "write values that fit the array's type and attributes, or write its stored values "
    "(blocks read with decode=False)"
)


def _encode_refusal(meta: ArrayMeta, why: str) -> ZarrUnsupported:
    return ZarrUnsupported(
        f"values cannot be written to array {meta.name!r}: {why}", fix=_ENCODE_FIX
    )


def _check_integer_range(values: np.ndarray, attrs: dict, meta: ArrayMeta, target):
    """Refuse floating values that, once packed and rounded, leave the integer ``target``
    (numpy would wrap them around silently)."""
    finite = values[np.isfinite(values)] if values.dtype.kind in "fc" else values
    if not finite.size:
        return
    packed = finite.astype("float64")
    if "add_offset" in attrs:
        packed = packed - float(attrs["add_offset"])
    if "scale_factor" in attrs:
        packed = packed / float(attrs["scale_factor"])
    lo, hi = float(np.min(np.around(packed))), float(np.max(np.around(packed)))
    info = np.iinfo(target)
    if lo < info.min or hi > info.max:
        raise _encode_refusal(
            meta,
            f"the values pack to {lo:g}..{hi:g}, outside the stored {target} range "
            f"{info.min}..{info.max}",
        )


def _same_time_units(a: str, b: str) -> bool:
    """Whether two CF time units are the same unit since the same instant (xarray
    rewrites ``"hours since 2023-01-01 00:00:00"`` as ``"hours since 2023-01-01"``)."""
    import pandas as pd

    ua, _, ra = str(a).partition(" since ")
    ub, _, rb = str(b).partition(" since ")
    if ua.strip().lower() != ub.strip().lower():
        return False
    try:
        return pd.Timestamp(ra.strip()) == pd.Timestamp(rb.strip())
    except (ValueError, TypeError, OverflowError):
        return ra.strip() == rb.strip()


def cf_encode(values, meta: ArrayMeta, *, stored: bool = False) -> np.ndarray:
    """``values`` as ``meta`` stores them, in native byte order: the inverse of the reading.

    ``stored=False`` takes values as ``gbx_zarr_read`` returns them with ``decode=True``
    and encodes them with the array's CF attributes, through xarray's own encoder (the one
    ``to_zarr`` uses): times become numbers in its ``units`` and ``calendar``, packed values
    are ``(v - add_offset) / scale_factor`` rounded to the stored integer type, and a NaN
    (or NaT) becomes the ``_FillValue`` (for Zarr v2, the array ``fill_value``, which is what
    reading masks). ``stored=True`` takes values that are already stored values and only
    casts them, which must be exact (``safe`` casting).

    Raises ``ZarrUnsupported`` for values the array cannot hold: a NaN in an integer array
    with no fill, values that leave the stored integer range once packed, complex values in
    a real array, an ``_Unsigned`` array written with decoded values.
    """
    target = np.dtype(meta.dtype).newbyteorder("=")
    values = _native_values(values)
    if stored:
        if values.dtype != target and not np.can_cast(values.dtype, target, "safe"):
            raise _encode_refusal(
                meta,
                f"stored values of type {values.dtype} do not cast exactly to {target}",
            )
        return values.astype(target, copy=False)
    attrs = _cf_attrs(meta)
    if attrs.get("_Unsigned") in ("true", "false") and target.kind in "iu":
        raise _encode_refusal(
            meta,
            "it reinterprets its integers through _Unsigned, so only its stored values "
            "can be written",
        )
    if values.dtype.kind == "c" and target.kind != "c":
        raise _encode_refusal(meta, "complex values into a real array")
    encoding: dict = {"dtype": target}
    for key in ("_FillValue", "missing_value", "scale_factor", "add_offset"):
        if attrs.get(key) is not None:
            encoding[key] = attrs[key]
    if values.dtype.kind == "M":
        if not is_time_coord(meta):
            raise _encode_refusal(meta, "times into an array with no CF time units")
        encoding["units"] = meta.attrs["units"]
        encoding["calendar"] = meta.attrs.get("calendar") or "standard"
    elif target.kind in "iub" and values.dtype.kind in "fc":
        masks = "_FillValue" in encoding or "missing_value" in encoding
        if not masks and np.isnan(values).any():
            raise _encode_refusal(
                meta,
                "it holds NaN, and an integer array without a _FillValue has no "
                "cell for a missing value",
            )
        if target.kind in "iu":
            _check_integer_range(values, encoding, meta, target)
    from xarray import Variable
    from xarray.conventions import encode_cf_variable

    var = Variable(
        tuple(f"dim_{i}" for i in range(values.ndim)), values, encoding=encoding
    )
    try:
        with warnings.catch_warnings():
            # xarray warns, then silently picks finer units, when the times do not fit
            # the requested ones; the units it used are checked below instead
            warnings.simplefilter("ignore")
            encoded = encode_cf_variable(var, name=meta.name)
    except (ValueError, TypeError, OverflowError) as e:
        raise _encode_refusal(meta, str(e)) from None
    if "units" in encoding and not _same_time_units(
        encoded.attrs.get("units", encoding["units"]), encoding["units"]
    ):
        raise _encode_refusal(
            meta,
            f"the times are not whole multiples of its units {encoding['units']!r} "
            f"(they would need {encoded.attrs['units']!r}), and its stored type "
            f"{target} has no fractions",
        )
    return np.asarray(encoded.values).astype(target, copy=False)


def is_time_coord(meta: ArrayMeta) -> bool:
    """Whether the array's ``units`` attribute is a CF time unit (``"hours since 1900-01-01"``)."""
    units = meta.attrs.get("units")
    return isinstance(units, str) and " since " in units


def decode_coord(
    values: np.ndarray, meta: ArrayMeta, *, use_zarr_fill_value_as_mask=None
) -> np.ndarray:
    """Decode a coordinate: CF time units become ``datetime64[ns]``, anything else is returned as is.

    A missing value (a ``_FillValue`` or ``missing_value``, a NaN, or the array ``fill_value``
    where ``use_zarr_fill_value_as_mask`` applies) decodes to ``NaT``, as in xarray. Integer
    times decode exactly.
    Anything GeoBrix cannot decode raises ``ZarrUnsupported``: a calendar other than the standard
    ones, a unit that is not a CF time unit, a reference date that is not a date, and times
    outside what ``datetime64[ns]`` holds (the years 1678 to 2262).
    """
    if not is_time_coord(meta):
        return values
    units = meta.attrs["units"]
    calendar = str(meta.attrs.get("calendar") or "standard").lower()
    if calendar not in _STANDARD_CALENDARS:
        raise ZarrUnsupported(
            f"calendar {calendar!r} of coordinate {meta.name!r} is not supported",
            fix="pass decode=False to read the raw time numbers",
        )
    from pandas.errors import OutOfBoundsDatetime, OutOfBoundsTimedelta
    from xarray.coding.times import decode_cf_datetime

    fix = "pass decode=False to read the raw time numbers"
    # The stored integers are reinterpreted first (_Unsigned), as xarray does. Missing times
    # are then found on them and put back as NaT afterwards, so an integer coordinate
    # (nanoseconds since 1970, say) never passes through float64; the fill itself is
    # replaced by 0 for the decoding, where it could lie outside the range.
    stored = np.asarray(values)
    values, attrs = cf_reinterpret(
        stored, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
    )
    mask = None
    if "scale_factor" in attrs or "add_offset" in attrs:
        values = cf_decode(
            stored, meta, use_zarr_fill_value_as_mask=use_zarr_fill_value_as_mask
        )
    else:
        mask = _mask_of(values, attrs)
        if mask is not None and mask.any():
            values = np.where(mask, np.zeros((), dtype=values.dtype), values)
    try:
        decoded = decode_cf_datetime(values, units, calendar, use_cftime=False)
    except KeyError as e:  # xarray looks the unit up by name
        raise ZarrUnsupported(
            f"the time units {units!r} of coordinate {meta.name!r} are not understood: "
            f"{e.args[0] if e.args else ''!r} is not a CF time unit (days, hours, minutes, "
            "seconds, milliseconds, microseconds or nanoseconds)",
            fix=fix + ", or correct the units attribute",
        ) from None
    except (OutOfBoundsDatetime, OutOfBoundsTimedelta, OverflowError):
        problem = _reference_problem(units)
        if problem:
            raise ZarrUnsupported(
                f"the time units {units!r} of coordinate {meta.name!r} have a reference "
                f"date, {problem!r}, that is not a date",
                fix=fix + ", or correct the units attribute",
            ) from None
        # datetime64[ns] spans only the years 1678 to 2262 (xarray would fall back to cftime)
        raise ZarrUnsupported(
            f"times of coordinate {meta.name!r} ({units}) fall outside the range of "
            "datetime64[ns], the years 1678 to 2262",
            fix=fix,
        ) from None
    decoded = np.array(decoded).astype("datetime64[ns]")
    if mask is not None:
        decoded[mask] = np.datetime64("NaT")
    return decoded


def _reference_problem(units: str) -> str | None:
    """The reference date of ``units`` when it cannot be parsed as a date, else ``None``.

    xarray reports a malformed reference date and a time outside ``datetime64[ns]`` with the
    same (empty) pandas error; a reference date that parses but lies outside the range is the
    second kind.
    """
    import pandas as pd
    from pandas.errors import OutOfBoundsDatetime

    reference = units.partition(" since ")[2].strip()
    try:
        pd.Timestamp(reference)
    except OutOfBoundsDatetime:
        return None
    except (ValueError, TypeError, OverflowError):
        return reference
    return None
