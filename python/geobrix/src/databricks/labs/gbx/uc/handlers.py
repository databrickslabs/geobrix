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
    if not (math.isfinite(e) and math.isfinite(n)):
        return None  # NaN and the infinities are NULL (AP-21)
    # UC has one STRING type: an INT resolution arrives as its digits ('3'), which BNG parses.
    res = resolution.strip() if isinstance(resolution, str) else resolution
    return _bng.point_as_cell(
        e, n, res
    )  # an unknown resolution raises (a parameter error)


def _res(raw):
    """A UC resolution (STRING: '3', '1km', '-3') as a BNG index; one that BNG does not define raises."""
    from databricks.labs.gbx.pygx import _bng

    return _bng.get_resolution(raw.strip() if isinstance(raw, str) else raw)


def bng_eastnorthasbng_batch(batches):
    """PARAMETER STYLE PANDAS handler: an iterator of (easting, northing, resolution) Series.

    The OD-18 probe form of bng_eastnorthasbng: the same Rust kernel as the session's `d` rung, fed a
    batch at a time. A wheel without the platform build answers row by row, as `bng_eastnorthasbng`.
    """
    import numpy as np
    import pandas as pd

    from databricks.labs.gbx import _rs

    for e, n, r in batches:
        if _rs.unavailable_reason():
            yield pd.Series([bng_eastnorthasbng(a, b, c) for a, b, c in zip(e, n, r)])
            continue
        out = np.full(len(e), None, dtype=object)
        ev = e.to_numpy(dtype="float64", na_value=np.nan)
        nv = n.to_numpy(dtype="float64", na_value=np.nan)
        # a non-finite coordinate is NULL before its resolution is read
        ok = np.isfinite(ev) & np.isfinite(nv)
        for raw in pd.unique(r[ok].dropna()):
            mask = ok & (r == raw).to_numpy()
            out[mask] = _rs.bng_cells(ev[mask], nv[mask], _res(raw)).to_numpy(
                zero_copy_only=False
            )
        yield pd.Series(out)


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


def bng_tessellate(geom, resolution, keep_core_geom):
    """UC: GEOMETRY in (SRID 27700 only: BNG cells are 27700); the chips out as EWKB with SRID 27700.

    gbx_bng_tessellateexplode is a SQL table function over this one (`inline` of the array).
    """
    import shapely

    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_tessellate"
    if geom is None or resolution is None:
        return None
    res = _res(resolution)  # an unknown resolution raises (a parameter error)
    try:
        raw = _geom_bytes(geom)
        g = shapely.from_wkt(raw) if isinstance(raw, str) else shapely.from_wkb(raw)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - unreadable geometry is NULL on UC
        return None
    srid = shapely.get_srid(g)
    if srid != 27700:
        raise _refuse(
            fn,
            f"BNG needs geometry in SRID 27700; got SRID {srid} (st_transform(geom, 27700) first)",
        )
    try:
        chips = _bng.tessellate_str(
            shapely.to_wkb(g), res, keep_core_geom=bool(keep_core_geom)
        )
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - a geometry that cannot be tessellated is NULL on UC
        return None
    return [
        {
            "cellid": cell,
            "core": bool(core),
            "chip": (
                None
                if chip is None
                else shapely.to_wkb(
                    shapely.set_srid(shapely.from_wkb(chip), 27700), include_srid=True
                )
            ),
        }
        for cell, core, chip in chips
    ]


def st_makevalid(geom, level):
    from databricks.labs.gbx.pyvx._validity import st_makevalid as _makevalid

    if geom is None:
        return None
    return _makevalid(
        _geom_bytes(geom), level or "linework"
    )  # EWKB in, EWKB out: the SRID is kept
