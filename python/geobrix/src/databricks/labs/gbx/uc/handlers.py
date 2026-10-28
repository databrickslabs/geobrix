"""UC Python handlers. Import-light: no pyspark, and only the dependencies the manifest declares.

A UC function is for warehousing, closely aligned to the product built-ins. Data it cannot serve
as the product would is refused with a Fix: that names the session function (or the Python API);
a parameter or form error raises. Corrupt data is NULL: a UC function has no session error tile.
Resource exhaustion (MemoryError, RecursionError) and a missing dependency (ImportError) are never data
and propagate (as the JVM's NonFatal).
"""

from __future__ import annotations

import math


class _Refusal(ValueError):
    """Input the UC surface does not serve; the message ends with a Fix: line."""


def _refuse(fn: str, what: str) -> _Refusal:
    return _Refusal(
        f"{fn}: {what}. Fix: use the session function {fn} "
        "(after databricks.labs.gbx.register(spark)) or the Python API."
    )


def _field(row, name):
    if row is None:
        return None
    try:
        return row[name]
    except (KeyError, TypeError, IndexError, ValueError):
        return getattr(row, name, None)


def bng_eastnorthasbng(easting, northing, resolution):
    from databricks.labs.gbx.pygx import _bng

    if easting is None or northing is None or resolution is None:
        return None
    e, n = float(easting), float(northing)
    if math.isnan(e) or math.isnan(n):
        return None
    # UC has one STRING type: an INT resolution arrives as its digits ('3'), which BNG parses.
    res = resolution.strip() if isinstance(resolution, str) else resolution
    return _bng.point_as_cell(
        e, n, res
    )  # an unknown resolution raises (a parameter error)


def _warp(raster: bytes, crs, fn: str) -> bytes:
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import authority_srid_of
    from databricks.labs.gbx.pyrx.core import warp

    with MemoryFile(raster) as mf, mf.open() as ds:
        if authority_srid_of(ds.crs) is None:
            raise _refuse(fn, "the raster's CRS is missing or has no authority code")
        return warp.reproject_to_crs(ds, crs.code)


def rst_transformcrs(tile, target_crs):
    from databricks.labs.gbx.core.crs import parse_uc_crs

    fn = "gbx_rst_transformcrs"
    if tile is None or target_crs is None:
        return None
    crs = parse_uc_crs(target_crs, fn=fn)  # .srid (int) for outputs, .code for rasterio
    raster = _field(tile, "raster")
    if raster is None:
        if _field(tile, "path") is not None:
            raise _refuse(
                fn, "virtual tiles (a path, no raster bytes) are session-only"
            )
        return None  # nothing to transform
    try:
        out = _warp(bytes(raster), crs, fn)
    except (MemoryError, RecursionError, ImportError, _Refusal):
        raise  # resource exhaustion, a missing dependency and a refusal are never "bad data"
    except Exception:  # noqa: BLE001 - unreadable data is NULL on UC (no error tile)
        return None
    # The tile's crs field carries the integer SRID; the raster itself embeds the authority.
    return {
        "cellid": int(_field(tile, "cellid") or 0),
        "raster": out,
        "path": None,
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": str(crs.srid),
        "metadata": {},
    }


def _geom_bytes(geom):
    """GEOMETRY as the UC Python side receives it -> WKB/EWKB bytes (or WKT text).

    The branches are a stand-in until a run on a UC runtime shows what a handler receives.
    """
    if isinstance(geom, (bytes, bytearray, memoryview)):
        return bytes(geom)
    for attr in ("ewkb", "wkb"):
        v = getattr(geom, attr, None)
        if isinstance(v, (bytes, bytearray)):
            return bytes(v)
    if isinstance(geom, str):
        return geom
    raise TypeError(
        f"gbx: unsupported GEOMETRY value {type(geom).__module__}.{type(geom).__name__}"
    )


def st_makevalid(geom, level):
    from databricks.labs.gbx.pyvx._validity import st_makevalid as _makevalid

    if geom is None:
        return None
    return _makevalid(
        _geom_bytes(geom), level or "linework"
    )  # EWKB in, EWKB out: the SRID is kept
