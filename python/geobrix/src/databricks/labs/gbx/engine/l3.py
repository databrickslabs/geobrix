"""The engine's Spark UDFs: level L3 of a manifest row with a `gdal` block.

- c0 (``c0(sql_name)``): an Arrow-optimized row-wise UDF over the whole tile (or array of tiles) and the other
  arguments, returning a tile. It takes virtual tiles (materialized through ``open_tile``) and materialized ones.
- c1 (``c1(sql_name)``): a batch UDF over the raster bytes and the other arguments, returning
  ``struct<raster, error, command>`` (no MAP field, so neither pandas nor Arrow has to carry one); the SQL wrapper
  the manager installs (``sqlwrap.engine_dispatch``) builds the tile, and sends virtual tiles to c0.

Both run ``engine.run`` on each row: the same argv, the same GDAL, the same error map as the JVM's
``GdalSpecExpr``. pyrx Serverless rules hold: no ``spark.conf``, ``_jvm`` or ``.rdd`` here.
"""

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from databricks.labs.gbx import manifest
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


def _virtual_raster(fn, tile, read_cap: Optional[int]) -> bytes:
    """A virtual tile's window as GeoTIFF bytes, read under the engine's input rules: its file (and a VRT's
    sources) must open with a driver the virtual-tile policy allows, and its decoded window must fit this compute's
    read cap, judged from the header before any pixel is read. Any failure is a DataError (an error tile).
    """
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.engine.argv import _ITEMSIZE
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.zarr import _tiles as zarr_tiles

    vt = ot._to_virtual_tile(tile)
    try:
        if not zarr_tiles.is_zarr_tile(
            vt
        ):  # a Zarr tile is decoded by GeoBrix, not opened by a GDAL driver
            run.virtual_source(fn.sql_name, vt.path)
        with run.engine_settings():
            with ot.open_header(vt) as h:
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
            with ot._open(vt) as ds:
                return prx._dataset_to_gtiff_bytes(ds)
    except run.DataError:
        raise
    except (MemoryError, RecursionError):
        raise
    except (
        Exception
    ) as exc:  # noqa: BLE001 - an unreadable virtual tile is data, as on every light function
        raise run.DataError(
            f"{fn.sql_name}: the virtual tile could not be read ({exc})"
        ) from None


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


def _inputs(fn, first, read_cap: Optional[int] = None):
    """(rasters, passthrough, cellid): the rasters to run on, or (None, what to return as it is, None) for a
    NULL, empty or error input (an error tile comes out unchanged; NULL and an empty tile give NULL). A virtual
    tile is read by ``_virtual_raster`` (DataError when it is refused or unreadable).
    """
    from databricks.labs.gbx.pyrx import functions as prx

    tiles = [first] if fn.gdal.inputs[0][1] == "raster" else list(first or ())
    if first is None or not tiles:
        return None, None, None
    for t in tiles:
        err = prx._error_tile_of(t)
        if err is not None:
            return None, err, None
        if t is None or prx._tile_is_empty(t):
            return None, None, None
    rasters = []
    for t in tiles:
        d = t.asDict() if hasattr(t, "asDict") else dict(t)
        raw = d.get("raster")
        rasters.append(
            bytes(raw) if raw is not None else _virtual_raster(fn, t, read_cap)
        )
    return rasters, None, prx._tile_cellid(tiles[0])


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
):
    """The row-wise engine UDF of `sql_name`: (tile or tiles, the other arguments...) -> tile. Registered on its
    own (no SQL wrapper), it is called with the arguments the caller wrote: the rest take the manifest's
    defaults. `read_cap` is the decoded size a virtual tile may have here (the driver's connect-aware cap,
    ``ds.file_gbx.report_detected_cap``); a worker cannot measure it itself."""
    fn = _function(sql_name, fn_ref)
    names = [p.name for p in fn.params[1:]]
    defaults = [_sql_default(p.default) for p in fn.params[1:]]

    @F.udf(V2_TILE_SCHEMA, useArrow=True)
    def _c0(first, *args):
        _setup(grid_dirs)
        values = dict(zip(names, list(args) + defaults[len(args) :]))
        try:
            rasters, passthrough, cellid = _inputs(fn, first, read_cap)
        except run.DataError as exc:
            return _tile(run.Result(None, str(exc), None), _cellid_of(fn, first))
        if rasters is None:
            return passthrough
        return _tile(run.run(fn, rasters, values, _section(section_ref)), cellid)

    return _c0


def _cellid_of(fn, first) -> int:
    """The cell id of the (first) input tile, for an error tile."""
    from databricks.labs.gbx.pyrx import functions as prx

    t = (
        first
        if fn.gdal.inputs[0][1] == "raster"
        else (list(first)[0] if first else None)
    )
    return prx._tile_cellid(t) if t is not None else 0


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
