"""The engine's Spark UDFs: level L3 of a manifest row with a `gdal` block.

- c0 (``c0(sql_name)``): an Arrow-optimized row-wise UDF over the whole tile (or array of tiles) and the other
  arguments, returning a tile. It takes virtual tiles (materialized through ``open_tile``) and materialized ones.
  With ``file_ref=True`` it takes a FILE reference as its second argument (one per tile for an array input).
- forced_c0 (``forced_c0(sql_name)``): the Python API's force-output form of c0: the same engine run, its tile
  shaped by the force-output keywords (``virtualize_dir``, ``virtualize_prefix``, ``materialize``).
- A row whose output is features (``gdal.out.kind``) returns an array of structs (``return_type``), NULL for a NULL,
  empty, error or unreadable tile and for a call GDAL fails (the reason is logged once per function and reason in the
  process); it has no c1 (``c1`` refuses it), so its batch form is c0.
- A row whose output is JSON (``gdal.out.kind``) returns GDAL's text as a STRING, NULL in the same cases; it has no c1 either.
- c1 (``c1(sql_name)``): a batch UDF over the raster bytes and the other arguments, returning
  ``struct<raster, error, command>`` (no MAP field, so neither pandas nor Arrow has to carry one); the SQL wrapper
  the manager installs (``sqlwrap.engine_dispatch``) builds the tile, and sends virtual tiles to c0.

Both run ``engine.run`` on each row: the same argv, the same GDAL, the same error map. pyrx Serverless rules hold: no ``spark.conf``, ``_jvm`` or ``.rdd`` here.
"""

from __future__ import annotations

import contextlib
import dataclasses
import errno
import logging
import threading
from collections import OrderedDict
from typing import Optional, Tuple

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx import _gdal_policy, manifest
from databricks.labs.gbx.engine import _tile as tile_checks
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

RESULT = StructType(
    [
        StructField("raster", BinaryType()),
        StructField("error", StringType()),
        StructField("command", StringType()),
    ]
)
RESULT_SQL = RESULT.simpleString()


_FIELD_TYPES = {
    "DOUBLE": DoubleType(),
    "INT": IntegerType(),
    "BIGINT": LongType(),
    "STRING": StringType(),
}
_LOGGED_MAX = 1024
_logged: "OrderedDict" = (
    OrderedDict()
)  # (function, reason) -> None, least recently logged first
_logged_lock = threading.Lock()


def return_type(fn) -> DataType:
    """The Spark type a row's engine UDF returns: a tile, or for a features output an array of structs (the geometry
    as WKB, then the declared fields), or for a JSON output a string."""
    if fn.gdal.out_kind == "raster":
        return V2_TILE_SCHEMA
    if fn.gdal.out_kind == "json":
        return StringType()
    if fn.gdal.out_kind == "features":
        return ArrayType(
            StructType(
                [StructField("geom_wkb", BinaryType())]
                + [StructField(n, _FIELD_TYPES[t]) for n, t, _ in fn.gdal.out_fields]
            )
        )
    raise ValueError(
        f"{fn.sql_name}: the {fn.gdal.out_kind} output is not built in this release"
    )


def _log_once(sql_name: str, reason: str) -> None:
    """A failed call of a non-raster row is NULL; its reason is logged once per function and reason in the process. The
    reasons kept are the last ``_LOGGED_MAX``: a reason that has been evicted is logged again if it recurs.
    """
    key = (sql_name, reason)
    with _logged_lock:
        if key in _logged:
            _logged.move_to_end(key)
            return
        _logged[key] = None
        while len(_logged) > _LOGGED_MAX:
            _logged.popitem(last=False)
    logging.getLogger("databricks.labs.gbx.engine").warning(
        "%s: NULL, %s", sql_name, reason
    )


def _value(fn, res: Optional[run.Result]):
    """A features or JSON result as SQL sees it: a list of structs or GDAL's text, or None for a NULL result or a failed
    call."""
    if res is None:
        return None
    if res.error is not None:
        _log_once(fn.sql_name, res.error)
        return None
    if fn.gdal.out_kind == "json":
        return res.text
    names = ["geom_wkb"] + [n for n, _, _ in fn.gdal.out_fields]
    return [dict(zip(names, row)) for row in res.features]


def metadata(res: run.Result) -> dict:
    """The metadata of an engine tile, the same on every level: the output's format and the command for a
    success; GDAL's reason under `last_error` and `error_message` for a failure."""
    if res.error is not None:
        return {"last_error": res.error, "error_message": res.error}
    return {"driver": "GTiff", "compression": "ZSTD", "last_command": res.command}


def _tile(res: Optional[run.Result], cellid) -> Optional[dict]:
    if res is None:
        return None
    row = {f.name: None for f in V2_TILE_SCHEMA.fields}
    row.update(cellid=cellid, raster=res.raster, metadata=metadata(res))
    return row


def _setup(grid_dirs: Tuple[str, ...]) -> None:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env(list(grid_dirs))


_chain_text = (
    _gdal_policy.chain_text
)  # the shared mapping (engine and light tier read one chain)


class _Settled(Exception):
    """A failure of a virtual-tile read that is not a missing file: it leaves the transient retry as it is."""

    def __init__(self, exc: BaseException):
        super().__init__(str(exc))
        self.exc = exc


def _is_missing(exc: BaseException) -> bool:
    """Whether a read failed because the tile's file is not there: a FileNotFoundError or ENOENT anywhere in the
    chain, or GDAL's own "No such file or directory". A refusal of the driver policy (GDAL reports a refused
    driver's connection string as missing) is not a missing file."""
    if run.policy_reason(_chain_text(exc)) is not None:
        return False
    cur, depth = exc, 0
    while cur is not None and depth < 8:
        if isinstance(cur, FileNotFoundError) or (
            isinstance(cur, OSError) and cur.errno == errno.ENOENT
        ):
            return True
        cur, depth = cur.__cause__ or cur.__context__, depth + 1
    return _gdal_policy._MISSING in _chain_text(exc)


def _virtual_raster(
    fn, tile, read_cap: Optional[int], file_ref=None, decimate=None, sizes_out=None
) -> bytes:
    """A virtual tile's window as GeoTIFF bytes, read under the engine's input rules: its file (and a VRT's
    sources) must open with a driver the virtual-tile policy allows, and its decoded window must fit this compute's
    read cap, judged from the header before any pixel is read. A corrupt or refused file is a DataError (an error
    tile). A file that is not there is not data: it is retried as transient (``_retry_transient``, the limits of
    every Volume read; a UC Volume can lag a write) and, still missing, raises ``MissingSourceError`` with a Fix.
    ``file_ref`` is the tile's FILE reference: a read through it is not staged through the path as well.
    ``decimate`` (``_decimation``) reads a plain window at its target size, which an overview serves where the file has
    one; the window's own size is then appended to ``sizes_out``.
    """
    from databricks.labs.gbx.core._retry import _retry_transient
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.zarr import _tiles as zarr_tiles

    vt = ot._to_virtual_tile(tile)
    try:
        with contextlib.ExitStack() as stack:
            is_zarr = zarr_tiles.is_zarr_tile(
                vt
            )  # a Zarr tile is decoded by GeoBrix, not opened by a GDAL driver
            if not is_zarr:
                # the path GDAL opens, resolved once as every light read resolves it (the remote-path gate, the
                # dbfs:/ and file:/ spellings, /Volumes staging, which retries a Volume file on its own); the
                # policy and the reads below judge that path
                vt = dataclasses.replace(
                    vt, path=ot._resolve_local_or_windowed(vt, file_ref, stack)
                )

            def attempt() -> bytes:
                try:
                    if not is_zarr:
                        run.virtual_source(fn.sql_name, vt.path)
                    return _read_virtual(
                        fn, vt, read_cap, file_ref, decimate, sizes_out
                    )
                except (MemoryError, RecursionError):
                    raise
                except (
                    Exception
                ) as exc:  # noqa: BLE001 - sorted: a missing file is retried, the rest settles
                    if _is_missing(exc):
                        raise FileNotFoundError(str(exc)) from exc
                    raise _Settled(exc) from None

            failure = None
            try:
                return _retry_transient(attempt)
            except _Settled as settled:
                failure = settled.exc
            raise failure  # the failure itself, for the classification below
    except (MemoryError, RecursionError):
        raise
    except _gdal_policy.RemotePathError as exc:
        raise run.DataError(f"{fn.sql_name}: {exc}") from None
    except (
        Exception
    ) as exc:  # noqa: BLE001 - an unreadable virtual tile is data, as on every light function
        if _is_missing(exc):
            raise run.MissingSourceError(
                f"{fn.sql_name}: the virtual tile's file {vt.path} is missing or unreadable, still after the "
                f"retries for a file a Volume has not caught up with ({exc}). "
                "Fix: check the path exists and is readable from the workers"
            ) from None
        # a source the policy stopped while GDAL read the tile: GDAL's message is in the chain (rasterio's read
        # failure, or the DataError the read raised), so the reason is judged on the whole chain
        text = _chain_text(exc)
        what = run.policy_reason(text)
        if what is not None:
            raise run.DataError(
                f"{fn.sql_name}: the virtual tile reads {what}, which GeoBrix does not open for a virtual tile "
                f"(GDAL: {text})"
            ) from None
        if isinstance(
            exc, run.DataError
        ):  # a refusal the engine already named (the file, the read cap)
            raise
        raise run.DataError(
            f"{fn.sql_name}: the virtual tile could not be read ({exc})"
        ) from None


# resampling words only a warp applies: a decimated read refuses them
_WARP_ONLY_WORDS = frozenset({"max", "min", "med", "q1", "q3", "sum"})


def _decimation(fn, values):
    """``(factor, rasterio resampling)`` when a virtual tile of this row is read at its target size: the row reads a
    factor (``in_read: "factor"``) below 1 and a resampling word that a read applies (not max, min, med, q1 or q3).
    None otherwise; a wrong factor or word is left to the argument check, which raises.
    """
    if values is None or fn.gdal.in_read != "factor":
        return None
    size_arg = next((a for a in fn.gdal.args if a.type == "size_from_factor"), None)
    word_arg = next(
        (a for a in fn.gdal.args if a.arg == "resampling" and a.source), None
    )
    if size_arg is None or word_arg is None:
        return None
    try:
        factor = float(values.get(size_arg.source))
        word = values.get(word_arg.source)
        from databricks.labs.gbx.pyrx.core._util import resampling_enum

        resampling = resampling_enum(str(word))
    except (TypeError, ValueError):
        return None
    if resampling.name in _WARP_ONLY_WORDS:
        return None  # the reproject applies it; a decimated read cannot (rasterio refuses it on a read)
    return (factor, resampling) if 0.0 < factor < 1.0 and word is not None else None


def _decimated_window(fn, src, window, decimate, read_cap):
    """``(GeoTIFF bytes, (width, height))`` of ``window`` of the open file ``src`` read at its target size (an overview
    serves the read where the file has one), with the window's own size. The read cap is judged on the decimated
    size. ``open_tile.NOT_PLAIN`` for a file whose own mask band the plain read would drop.
    """
    from affine import Affine

    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.engine.argv import _ITEMSIZE
    from databricks.labs.gbx.pyrx.core import compression as comp
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.core._nodata import has_own_mask

    if has_own_mask(src):
        return ot.NOT_PLAIN
    factor, resampling = decimate
    w, h = int(window.width), int(window.height)
    tw, th = max(1, round(w * factor)), max(1, round(h * factor))
    dtype = str(src.dtypes[0])
    size = tw * th * src.count * _ITEMSIZE.get(_GDAL_TYPE.get(dtype, dtype), 8)
    if materialize_decision(size, "ingest", cap_bytes=read_cap) == "error":
        raise run.DataError(
            f"{fn.sql_name}: the virtual tile is {size // 2**20} MiB decoded at its target size, over this "
            "compute's read cap. Fix: retile it first (gbx_rst_retile) so each tile is smaller"
        )
    data = src.read(window=window, out_shape=(src.count, th, tw), resampling=resampling)
    profile = comp.source_profile(src.profile)
    profile.update(
        driver="GTiff",
        width=tw,
        height=th,
        count=src.count,
        transform=src.window_transform(window) * Affine.scale(w / tw, h / th),
    )
    return comp.write_gtiff(profile, data, alpha_like=src.colorinterp), (w, h)


def _read_virtual(
    fn, vt, read_cap: Optional[int], file_ref=None, decimate=None, sizes_out=None
) -> bytes:
    """The window of a virtual tile whose path is resolved, as GeoTIFF bytes, under this compute's read cap. With
    ``decimate`` a plain window is read at its target size instead (``_decimated_window``).
    """
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.engine.argv import _ITEMSIZE
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    with run.engine_settings():
        if decimate is not None and sizes_out is not None:
            # vt.path is the local path the FILE reference resolved to (`_virtual_raster`), so a tile that arrives with a
            # FILE reference is still a plain window of a file here
            got = ot.with_plain_window(
                vt,
                None,
                lambda src, window: _decimated_window(
                    fn, src, window, decimate, read_cap
                ),
            )
            if got is not ot.NOT_PLAIN:
                sizes_out.append(got[1])
                return got[0]
        with ot.open_header(vt, file_ref=file_ref) as h:
            dtype = str(h.dtypes[0])
            size = (
                h.width
                * h.height
                * h.count
                * _ITEMSIZE.get(_GDAL_TYPE.get(dtype, dtype), 8)
            )
        if materialize_decision(size, "ingest", cap_bytes=read_cap) == "error":
            raise run.DataError(
                f"{fn.sql_name}: the virtual tile is {size // 2**20} MiB decoded, over this compute's read "
                "cap. Fix: retile it first (gbx_rst_retile) so each tile is smaller"
            )
        with ot._open(vt, file_ref=file_ref) as ds:
            return prx._dataset_to_gtiff_bytes(ds)


# numpy dtype names -> GDAL type names, for the item size of a header's band type
_GDAL_TYPE = {
    "uint8": "Byte",
    "int8": "Int8",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "uint64": "UInt64",
    "int64": "Int64",
    "float32": "Float32",
    "float64": "Float64",
    "complex64": "CFloat32",
    "complex128": "CFloat64",
}


def _inputs(
    fn,
    first,
    read_cap: Optional[int] = None,
    file_refs=None,
    sizes_out=None,
    values=None,
):
    """(rasters, passthrough, cellid): the rasters to run on, or (None, what to return as it is, None) for a
    NULL, empty or error input (an error tile comes out unchanged; NULL and an empty tile give NULL). A virtual
    tile is read by ``_virtual_raster`` (DataError when it is refused or unreadable). ``file_refs`` is one FILE
    reference, or for an array input a list with one per tile. For a row that reads a virtual tile at its target
    size (``in_read: "factor"``, with the call's ``values``), ``sizes_out`` is filled with one ``(width, height)`` or
    None per raster: the window's own size where it was read decimated.
    """
    tiles = [first] if fn.gdal.inputs[0][1] == "raster" else list(first or ())
    if first is None or not tiles:
        return None, None, None
    for t in tiles:
        err = tile_checks.error_tile_of(t)
        if err is not None:
            return None, err, None
        if t is None or tile_checks.is_empty(t):
            return None, None, None
    rasters = []
    decimate = _decimation(fn, values)
    for i, t in enumerate(tiles):
        d = t.asDict() if hasattr(t, "asDict") else dict(t)
        raw = d.get("raster")
        ref = file_refs[i] if isinstance(file_refs, (list, tuple)) else file_refs
        if raw is not None:
            rasters.append(bytes(raw))
            if sizes_out is not None:
                sizes_out.append(None)
            continue
        box: list = []
        rasters.append(_virtual_raster(fn, t, read_cap, ref, decimate, box))
        if sizes_out is not None:
            sizes_out.append(box[0] if box else None)
    return rasters, None, tile_checks.cellid(tiles[0])


def _sql_default(text: Optional[str]):
    """A manifest param's SQL DEFAULT text as the Python value SQL would pass."""
    if text is None or text.upper() == "NULL":
        return None
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    if text.lower() in ("true", "false"):
        return text.lower() == "true"
    return float(text) if any(c in text for c in ".eE") else int(text)


def _function(sql_name: str, fn_ref: Optional[str]):
    """The manifest row `sql_name`, or the Function object `fn_ref` ('module:attr') names (a spec that is not a
    SQL function, such as the bench's floor). A UDF captures only these names: a worker looks the row up itself.
    """
    return (
        manifest.resolve_ref(fn_ref) if fn_ref else manifest.load().functions[sql_name]
    )


def _section(section_ref: Optional[str]):
    """The allowlist the engine checks against: the manifest's `gdal` section, or the one `section_ref` names (the
    bench's, which adds its floor's algorithm)."""
    return manifest.resolve_ref(section_ref) if section_ref else None


def c0(
    sql_name: str,
    grid_dirs: Tuple[str, ...] = (),
    fn_ref: Optional[str] = None,
    read_cap: Optional[int] = None,
    section_ref: Optional[str] = None,
    file_ref: bool = False,
):
    """The row-wise engine UDF of `sql_name`: (tile or tiles, the other arguments...) -> tile. Registered on its
    own (no SQL wrapper), it is called with the arguments the caller wrote: the rest take the manifest's
    defaults. `read_cap` is the decoded size a virtual tile may have here (the driver's connect-aware cap,
    ``ds.file_gbx.report_detected_cap``); a worker cannot measure it itself. With `file_ref` the UDF takes
    ``(first, file_ref, <every other param>)``: the FILE reference of the tile (an array of them, one per tile,
    for a `rasters` input), so a FILE-backed virtual tile is read through it."""
    fn = _function(sql_name, fn_ref)
    names = [p.name for p in fn.params[1:]]
    defaults = [_sql_default(p.default) for p in fn.params[1:]]
    raster_out = fn.gdal.out_kind == "raster"

    @F.udf(return_type(fn), useArrow=True)
    def _c0(first, *args):
        _setup(grid_dirs)
        refs = None
        if file_ref:
            refs, args = args[0], args[1:]
        values = dict(zip(names, list(args) + defaults[len(args) :]))
        sizes: list = []
        try:
            rasters, passthrough, cellid = _inputs(
                fn, first, read_cap, refs, sizes, values
            )
        except run.DataError as exc:
            if not raster_out:
                _log_once(fn.sql_name, str(exc))
                return None
            return _tile(run.Result(None, str(exc), None), _cellid_of(fn, first))
        if rasters is None:
            return passthrough if raster_out else None
        res = run.run(fn, rasters, values, _section(section_ref), src_sizes=sizes)
        return _tile(res, cellid) if raster_out else _value(fn, res)

    return _c0


def forced_c0(
    sql_name: str,
    grid_dirs: Tuple[str, ...] = (),
    read_cap: Optional[int] = None,
):
    """The Python API's force-output form of an engine row: the engine's result, shaped by the light tier's
    force-output keywords, so every engine row keeps them. The UDF takes ``(first, <every other param>,
    virtualize_dir, virtualize_prefix, materialize)`` and returns a tile."""
    fn = _function(sql_name, None)
    names = [p.name for p in fn.params[1:]]
    defaults = [_sql_default(p.default) for p in fn.params[1:]]

    @F.udf(V2_TILE_SCHEMA, useArrow=True)
    def _forced(first, *rest):
        _setup(grid_dirs)
        args, (vdir, vprefix, mat) = rest[:-3], rest[-3:]
        values = dict(zip(names, list(args) + defaults[len(args) :]))
        sizes: list = []
        try:
            rasters, passthrough, cellid = _inputs(
                fn, first, read_cap, None, sizes, values
            )
        except run.DataError as exc:
            return _tile(run.Result(None, str(exc), None), _cellid_of(fn, first))
        if rasters is None:
            return passthrough
        res = run.run(fn, rasters, values, src_sizes=sizes)
        if res is not None and res.raster is not None:
            from databricks.labs.gbx.pyrx import functions as prx

            return prx._shaped_result_row(res.raster, cellid, vdir, vprefix, mat)
        return _tile(res, cellid)

    return _forced


def _cellid_of(fn, first) -> int:
    """The cell id of the (first) input tile, for an error tile."""
    t = (
        first
        if fn.gdal.inputs[0][1] == "raster"
        else (list(first)[0] if first else None)
    )
    return tile_checks.cellid(t) if t is not None else 0


def _batch(fn, names, rasters, args, section=None):
    """The engine over a batch. A NULL input is a NULL result: Spark runs a UDF it lifted out of the wrapper's
    CASE on every row, with NULL for the rows another branch serves."""
    out = {"raster": [], "error": [], "command": []}
    for i, r in enumerate(rasters):
        res = None
        if r is not None:
            tiles = [r] if fn.gdal.inputs[0][1] == "raster" else list(r)
            values = {n: a[i] for n, a in zip(names, args)}
            res = run.run(fn, [bytes(t) for t in tiles], values, section)
        out["raster"].append(None if res is None else res.raster)
        out["error"].append(None if res is None else res.error)
        out["command"].append(None if res is None else res.command)
    return out


def c1(
    sql_name: str,
    grid_dirs: Tuple[str, ...] = (),
    fn_ref: Optional[str] = None,
    section_ref: Optional[str] = None,
):
    """The batch engine UDF of `sql_name`: (raster bytes or an array of them, the other arguments...) ->
    struct<raster, error, command>; all three NULL for a NULL result. PySpark 4.1+ `arrow_udf` where present
    (no pandas hop), else `pandas_udf`. The caller sends only materialized rasters (the wrapper's CASE).
    """
    fn = _function(sql_name, fn_ref)
    if fn.gdal.out_kind != "raster":
        raise ValueError(
            f"{sql_name}: a {fn.gdal.out_kind} output has no batch form; use c0"
        )
    names = [p.name for p in fn.params[1:]]
    arrow_udf = getattr(F, "arrow_udf", None)
    if arrow_udf is not None:
        import pyarrow as pa

        def _c1_arrow(rasters, *args):
            _setup(grid_dirs)
            cols = _batch(
                fn,
                names,
                rasters.to_pylist(),
                [a.to_pylist() for a in args],
                _section(section_ref),
            )
            return pa.StructArray.from_arrays(
                [
                    pa.array(cols["raster"], pa.binary()),
                    pa.array(cols["error"], pa.string()),
                    pa.array(cols["command"], pa.string()),
                ],
                names=["raster", "error", "command"],
            )

        return arrow_udf(RESULT)(_c1_arrow)

    def _c1(rasters: pd.Series, *args: pd.Series) -> pd.DataFrame:
        _setup(grid_dirs)
        vals = [
            [None if pd.isna(v) else v for v in a] if a.dtype.kind == "f" else list(a)
            for a in args
        ]
        return pd.DataFrame(
            _batch(fn, names, list(rasters), vals, _section(section_ref))
        )

    return F.pandas_udf(_c1, RESULT)
