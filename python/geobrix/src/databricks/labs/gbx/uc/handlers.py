"""UC Python handlers. Import-light: no pyspark, and only the dependencies the manifest declares.

A UC function is for warehousing, closely aligned to the product built-ins. Data it cannot serve
as the product would is refused with a Fix: that names the session function (or the Python API);
a parameter or form error raises. Corrupt data is NULL: a UC function has no session error tile.
Resource exhaustion (MemoryError, RecursionError) and a missing dependency (ImportError) are never data
and propagate (as the JVM's NonFatal).
"""

from __future__ import annotations

import math

from databricks.labs.gbx._gdal_policy import open_bytes


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
        return None  # NaN and the infinities are NULL, as on every level
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

    The probe form of bng_eastnorthasbng: the same Rust kernel as the session's `d` rung, fed a
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

    with MemoryFile(raster) as mf, open_bytes(mf) as ds:
        if authority_srid_of(ds.crs) is None:
            raise _refuse(fn, "the raster's CRS is missing or has no authority code")
        return warp.reproject_to_crs(ds, crs.code)


def rst_transformcrs(tile, target_crs):
    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.uc.tier_b import tile_row  # tier_b imports this module

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
    # tile_row is the canonical UC tile builder: driver, size and band count in the metadata, the integer SRID in crs.
    return tile_row(fn, tile, out)


def _geom_bytes(geom):
    """GEOMETRY as a UC Python function receives it -> EWKB bytes, the SRID kept.

    Measured on Serverless env 6: a GEOMETRY(ANY) argument arrives as ``pyspark.sql.types.Geometry``, whose WKB
    (``getBytes()``) carries no SRID and whose ``getSrid()`` is the SRID.
    """
    get_wkb, get_srid = getattr(geom, "getBytes", None), getattr(geom, "getSrid", None)
    if callable(get_wkb) and callable(get_srid):
        import shapely

        g = shapely.from_wkb(bytes(get_wkb()))
        return shapely.to_wkb(shapely.set_srid(g, int(get_srid())), include_srid=True)
    raise TypeError(
        f"gbx: unsupported GEOMETRY value {type(geom).__module__}.{type(geom).__name__}: a BINARY or STRING is not a "
        "GEOMETRY. Fix: build the GEOMETRY first, for example st_geomfromwkb(wkb, srid)."
    )


def _geometry_type():
    """The GEOMETRY value class of a UC Python function (the oracle swaps in a stand-in where there is no pyspark)."""
    from pyspark.sql.types import Geometry

    return Geometry


def _geom_out(ewkb):
    """EWKB -> the GEOMETRY value a UC Python function returns: a ``Geometry`` of the WKB and its SRID (measured:
    returning bytes for a GEOMETRY(ANY) result fails in the worker)."""
    if ewkb is None:
        return None
    import shapely

    g = shapely.from_wkb(ewkb)
    srid = shapely.get_srid(g)
    return _geometry_type().fromWKB(shapely.to_wkb(shapely.set_srid(g, 0)), srid)


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
    return _geom_out(
        _makevalid(_geom_bytes(geom), level or "linework")
    )  # the SRID is kept


# --- the grid and vector slice ----------------------------------------------------------------------------------
# One shape for every handler: a NULL argument is NULL; a PARAMETER error (a resolution, a mode, a form the function
# does not take) raises with a Fix: line; DATA the function cannot serve (a cell id that is no cell, a point that is not
# finite) is NULL, because a UC function has no error tile. A GEOMETRY argument is checked against the SRID the grid is
# defined in, and a geometry in another SRID is refused. The light kernels are the session functions' own.

BNG_SRID, WGS84_SRID = 27700, 4326


def _parameter(fn: str, exc: Exception) -> ValueError:
    """A parameter error, with the message of the kernel and a Fix: line that names the session function."""
    return ValueError(
        f"{exc}. Fix: pass a value {fn} accepts; the session function {fn} takes the same parameters "
        "(after databricks.labs.gbx.register(spark))."
    )


def _grid_geometry(fn: str, geom, srid: int, grid: str, hint: str = ""):
    """A GEOMETRY(ANY) argument as a shapely geometry, or None when it cannot be read; a geometry in an SRID other than
    the grid's is refused (a UC GEOMETRY always carries an integer SRID). ``hint`` replaces the parenthesis after the
    refusal (the default names ``st_transform``)."""
    import shapely

    raw = _geom_bytes(geom)
    try:
        g = shapely.from_wkb(raw)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - unreadable geometry is NULL on UC
        return None
    got = shapely.get_srid(g)
    if got != srid:
        raise _refuse(
            fn,
            f"{grid} needs geometry in SRID {srid}; got SRID {got} "
            f"({hint or f'st_transform(geom, {srid}) first'})",
        )
    return g


def _bng_res(fn: str, raw):
    try:
        return _res(raw)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception as exc:  # noqa: BLE001 - the kernel names the value
        raise _parameter(fn, exc) from exc


def _whole(fn: str, raw) -> int:
    from databricks.labs.gbx.pygx._params import _whole_res

    try:
        return _whole_res(fn, raw)
    except ValueError as exc:
        raise _parameter(fn, exc) from exc


def _mode(fn: str, mode):
    from databricks.labs.gbx.pygx._params import _dilate_check_mode

    try:
        _dilate_check_mode(mode)
    except ValueError as exc:
        raise _parameter(fn, exc) from exc


def _coverage(fn: str, coverage) -> str:
    """The coverage basis (NULL or ``''`` is the default, as for ``mode``); an unknown one is a PARAMETER error, before
    the kernel runs."""
    from databricks.labs.gbx.pygx import _dilate

    if not coverage:
        return _dilate.DEFAULT_COVERAGE
    if coverage not in _dilate.COVERAGE:
        raise _parameter(
            fn,
            ValueError(
                f"unknown coverage {coverage!r}; expected one of {_dilate.COVERAGE}"
            ),
        )
    return coverage


def _ring_k(fn: str, k) -> int:
    """The ring distance of a geometry dilation: a whole number of at least 0; anything else is a PARAMETER error, before
    the kernel runs (the kernel would otherwise raise inside a handler that turns every failure into NULL).
    """
    try:
        whole = int(k)
    except (TypeError, ValueError, OverflowError):
        whole = None
    if whole is None or whole != k or whole < 0:
        raise _parameter(
            fn, ValueError(f"k must be a whole number of at least 0; got {k!r}")
        )
    return whole


def _range(fn: str, name: str, value: int, lo: int, hi: int) -> int:
    """An integer parameter inside the grid's own range; one outside it is a PARAMETER error."""
    if not lo <= value <= hi:
        raise _parameter(fn, ValueError(f"{name} must be in [{lo}, {hi}]; got {value}"))
    return value


H3_MAX_RES = 15


def _finite(g) -> bool:
    import numpy as np
    import shapely

    return bool(np.isfinite(shapely.get_coordinates(g)).all())


def _bng_cell(cellid):
    """A BNG cell id (STRING) as the kernel's integer id; a string that is no cell is None (data)."""
    from databricks.labs.gbx.pygx import _bng

    return None if cellid is None else _bng.parse_safe(cellid)


def _ewkb(g, srid: int) -> bytes:
    """EWKB, always little-endian: the bytes the UC Scala handlers write, so a Volume with or without the JAR gives
    the same BINARY."""
    import shapely

    return shapely.to_wkb(shapely.set_srid(g, srid), include_srid=True, byte_order=1)


def _cell_polygon(cid):
    """The BNG cell polygon with the JVM's ring (south-west corner first, counter-clockwise), the ring the Scala
    handler writes; the kernel builds it, so the light and UC surfaces share one ring.
    """
    from databricks.labs.gbx.pygx import _bng

    return _bng.cell_id_to_geometry(cid)


def bng_pointascell(geom, resolution):
    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_pointascell"
    if geom is None or resolution is None:
        return None
    res = _bng_res(fn, resolution)
    g = _grid_geometry(fn, geom, BNG_SRID, "BNG")
    if g is None or g.is_empty:
        return None
    c = g.centroid  # the cell of the geometry's centroid
    if not (math.isfinite(c.x) and math.isfinite(c.y)):
        return None
    return _bng.point_as_cell(c.x, c.y, res)


def bng_polyfill(geom, resolution):
    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_polyfill"
    if geom is None or resolution is None:
        return None
    res = _bng_res(fn, resolution)
    g = _grid_geometry(fn, geom, BNG_SRID, "BNG")
    if g is None:
        return None
    try:
        return _bng.polyfill_str(g, res)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - a geometry that cannot be filled is NULL on UC
        return None


def bng_geomkring(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_geomkring"
    if geom is None or resolution is None or k is None:
        return None
    res = _bng_res(fn, resolution)
    _mode(fn, mode)
    coverage = _coverage(fn, coverage)
    k = _ring_k(fn, k)
    g = _grid_geometry(fn, geom, BNG_SRID, "BNG")
    if g is None or not _finite(g):
        return None  # a non-finite coordinate has no cell
    try:
        return sorted(
            _bng.geometry_k_ring_str(g, res, k, mode or "boundary-out", coverage)
        )
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the parameters are checked above: a geometry that cannot be dilated is NULL
        return None


def bng_kring(cellid, k):
    from databricks.labs.gbx.pygx import _bng

    if cellid is None or k is None:
        return None
    cid = _bng_cell(cellid)
    if cid is None:
        return None  # a string that is no cell
    return [_bng.format(c) for c in _bng.k_ring(cid, int(k))]


def bng_cellarea(cellid):
    from databricks.labs.gbx.pygx import _bng

    cid = _bng_cell(cellid)
    return None if cid is None else _bng.area(cid)


def bng_aswkb(cellid):
    """The cell polygon as EWKB with SRID 27700 (a UC BINARY result carries the integer SRID)."""
    cid = _bng_cell(cellid)
    return None if cid is None else _ewkb(_cell_polygon(cid), BNG_SRID)


def bng_centroid(cellid):
    """The cell centre as EWKB with SRID 27700."""
    cid = _bng_cell(cellid)
    return None if cid is None else _ewkb(_cell_polygon(cid).centroid, BNG_SRID)


def quadbin_pointascell(longitude, latitude, resolution):
    from databricks.labs.gbx.pygx import _quadbin

    fn = "gbx_quadbin_pointascell"
    if longitude is None or latitude is None or resolution is None:
        return None
    lon, lat = float(longitude), float(latitude)
    z = _range(fn, "zoom", _whole(fn, resolution), 0, _quadbin._MAX_RES)
    if not (math.isfinite(lon) and math.isfinite(lat)):
        return None  # a coordinate that is not finite has no cell
    return _quadbin.point_as_cell(lon, lat, z)


def quadbin_polyfill(geom, resolution):
    from databricks.labs.gbx.pygx import _quadbin

    fn = "gbx_quadbin_polyfill"
    if geom is None or resolution is None:
        return None
    z = _range(fn, "zoom", _whole(fn, resolution), 0, _quadbin._MAX_POLYFILL_RES)
    g = _grid_geometry(fn, geom, WGS84_SRID, "quadbin")
    if g is None:
        return None
    try:
        return _quadbin.polyfill(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom is checked above: a geometry that cannot be filled (too many cells) is NULL
        return None


def h3_geomkring(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _h3

    fn = "gbx_h3_geomkring"
    if geom is None or resolution is None or k is None:
        return None
    _mode(fn, mode)
    coverage = _coverage(fn, coverage)
    res = _range(fn, "resolution", _whole(fn, resolution), 0, H3_MAX_RES)
    k = _ring_k(fn, k)
    g = _grid_geometry(fn, geom, WGS84_SRID, "H3")
    if g is None or not _finite(g):
        return None
    try:
        return sorted(
            _h3.geom_expand("ring", g, res, k, mode or "boundary-out", coverage)
        )
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the parameters are checked above: a geometry that cannot be dilated is NULL
        return None


# --- array results are bounded, never truncated ---------------------------------------------------------------------
# A Unity Catalog function returns at most UC_DECODED_CAP (64 MiB) per result. A cell array past it is refused with a Fix
# before the kernel builds it where its size is known up front (a ring of k), and after it where it is not (a geometry's
# cells). One helper judges every grid array, so the rule has one place.

ARRAY_ITEM_BYTES = (
    40  # one element of a cell array: the id and the row's overhead, an upper estimate
)


def _array_cap() -> int:
    from databricks.labs.gbx.pyrx.core.budget import UC_DECODED_CAP

    return UC_DECODED_CAP


def _check_bytes(fn: str, size: int, what: str) -> None:
    """Refuse a result of ``size`` bytes over the cap, with a Fix that names the session function."""
    cap = _array_cap()
    if size > cap:
        from databricks.labs.gbx.pyrx.core.budget import size_text

        raise _Refusal(
            f"{fn}: {what} is {size_text(size)}, over the {size_text(cap)} a Unity Catalog function returns. "
            f"Fix: lower k, or split the input into smaller groups, or use the session function {fn} "
            "(after databricks.labs.gbx.register(spark)) or the Python API."
        )


def _check_array(fn: str, count: int, what: str = "the array of cells") -> None:
    """Refuse an array of ``count`` cells over the cap (see :func:`_check_bytes`)."""
    _check_bytes(fn, int(count) * ARRAY_ITEM_BYTES, what)


def _le(wkb: bytes) -> bytes:
    """A WKB or EWKB as little-endian bytes (the byte order the UC Scala handlers write); little-endian input is as is."""
    if wkb[:1] == b"\x01":
        return wkb
    import shapely

    return shapely.to_wkb(shapely.from_wkb(wkb), include_srid=True, byte_order=1)


def _ring_cells(k: int, side: int) -> int:
    """The most cells a square ring of distance k holds on a grid ``side`` cells wide: the square, cut to the grid."""
    return min(2 * k + 1, side) ** 2


def _grid_distance(fn: str, a, b, kernel):
    """``kernel(a, b)`` of two parsed cells; NULL when either is not a cell (the parsers answer None for that)."""
    return None if a is None or b is None else kernel(a, b)


def _dilation(fn: str, mode, coverage, k):
    """The checked dilation arguments of a geometry-aware ring: ``(mode, coverage, k)``; a wrong one is a PARAMETER error,
    raised before the geometry is read."""
    _mode(fn, mode)
    return mode or "boundary-out", _coverage(fn, coverage), _ring_k(fn, k)


def _cells_of(fn: str, g, run):
    """``run(g)`` as a cell array: NULL for a NULL, non-finite or unreadable geometry (data), else bounded by the cap."""
    if g is None or not _finite(g):
        return None  # a non-finite coordinate has no cell
    try:
        cells = run(g)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the parameters are checked: a geometry that cannot be dilated is NULL
        return None
    _check_array(fn, len(cells))
    return cells


# --- quadbin --------------------------------------------------------------------------------------------------------


def _qb_cell(cell):
    """A quadbin cell id as an int, or None when it is no cell (data)."""
    if cell is None:
        return None
    import quadbin

    try:
        c = int(cell)
        return c if quadbin.is_valid_cell(c) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _qb_zoom(cell: int) -> int:
    import quadbin

    return quadbin.get_resolution(cell)


def quadbin_aswkb(cell):
    """The cell polygon as EWKB with SRID 4326."""
    from databricks.labs.gbx.pygx import _quadbin

    c = _qb_cell(cell)
    return None if c is None else _le(_quadbin.as_wkb(c))


def quadbin_centroid(cell):
    """The cell centre as EWKB with SRID 4326."""
    from databricks.labs.gbx.pygx import _quadbin

    c = _qb_cell(cell)
    return None if c is None else _le(_quadbin.centroid(c))


def quadbin_distance(cellid1, cellid2):
    from databricks.labs.gbx.pygx import _quadbin

    fn = "gbx_quadbin_distance"
    if cellid1 is None or cellid2 is None:
        return None
    a, b = _qb_cell(cellid1), _qb_cell(cellid2)
    if a is None or b is None:
        return None
    za, zb = _qb_zoom(a), _qb_zoom(b)
    if za != zb:
        raise ValueError(
            f"{fn}: the cells are at different zooms ({za} and {zb}). "
            "Fix: compare cells of one zoom; gbx_quadbin_resolution(cell) gives a cell's zoom."
        )
    return _quadbin.distance(a, b)


def _qb_ring(fn: str, cell, k, kernel):
    """kring and kloop of a cell: the arguments checked, the ring's size bounded before the kernel builds it."""
    if cell is None or k is None:
        return None
    kk = _ring_k(fn, k)
    c = _qb_cell(cell)
    if c is None:
        return None  # an id that is no cell
    _check_array(
        fn, _ring_cells(kk, 2 ** _qb_zoom(c))
    )  # a loop is built as the ring minus the one inside it
    return kernel(c, kk)


def quadbin_kring(cell, k):
    from databricks.labs.gbx.pygx import _quadbin

    return _qb_ring("gbx_quadbin_kring", cell, k, _quadbin.k_ring)


def quadbin_kloop(cell, k):
    from databricks.labs.gbx.pygx import _quadbin

    return _qb_ring("gbx_quadbin_kloop", cell, k, _quadbin.k_loop)


def _qb_geom_ring(fn: str, geom, resolution, k, mode, coverage, kernel):
    if geom is None or resolution is None or k is None:
        return None
    mode, coverage, kk = _dilation(fn, mode, coverage, k)
    from databricks.labs.gbx.pygx import _quadbin

    z = _range(fn, "zoom", _whole(fn, resolution), 0, _quadbin._MAX_RES)
    g = _grid_geometry(fn, geom, WGS84_SRID, "quadbin")
    return _cells_of(fn, g, lambda g: kernel(g, z, kk, mode, coverage))


def quadbin_geomkring(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _quadbin

    return _qb_geom_ring(
        "gbx_quadbin_geomkring",
        geom,
        resolution,
        k,
        mode,
        coverage,
        _quadbin.geometry_k_ring,
    )


def quadbin_geomkloop(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _quadbin

    return _qb_geom_ring(
        "gbx_quadbin_geomkloop",
        geom,
        resolution,
        k,
        mode,
        coverage,
        _quadbin.geometry_k_loop,
    )


def quadbin_tessellate(geom, resolution):
    """The chips of a geometry: ``(cell, geom)`` with the chip as EWKB with SRID 4326."""
    from databricks.labs.gbx.pygx import _quadbin

    fn = "gbx_quadbin_tessellate"
    if geom is None or resolution is None:
        return None
    z = _range(fn, "zoom", _whole(fn, resolution), 0, _quadbin._MAX_POLYFILL_RES)
    g = _grid_geometry(fn, geom, WGS84_SRID, "quadbin")
    if g is None:
        return None
    try:
        chips = _quadbin.tessellate(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom is checked above: a geometry that cannot be cut (too many cells) is NULL
        return None
    out = [{"cell": int(c), "geom": _le(chip)} for c, chip in chips]
    _check_bytes(
        fn,
        sum(len(r["geom"]) + ARRAY_ITEM_BYTES for r in out),
        "the array of chips",
    )
    return out


def quadbin_cellunion(cells):
    """The union of the cells' polygons as EWKB with SRID 4326; NULL for no cell, or for an id that is no cell."""
    from databricks.labs.gbx.pygx import _quadbin

    fn = "gbx_quadbin_cellunion"
    if cells is None:
        return None
    ids = []
    for cell in cells:
        if cell is None:
            continue  # a NULL beside cells is no cell
        c = _qb_cell(cell)
        if c is None:
            return None
        ids.append(c)
    if not ids:
        return None
    _check_array(fn, len(ids))
    return _le(_quadbin.cell_union(ids))


def st_explainvalidity(geom):
    from databricks.labs.gbx.pyvx._validity import explain_validity

    if geom is None:
        return None
    try:
        return explain_validity(_geom_bytes(geom))
    except (MemoryError, RecursionError, ImportError, TypeError):
        raise
    except Exception:  # noqa: BLE001 - unreadable geometry is NULL on UC
        return None


def st_wrapx(geom, wrap_x_origin, wrap_direction):
    from databricks.labs.gbx.pyvx._antimeridian import _udf_st_wrapx

    if geom is None or wrap_x_origin is None or wrap_direction is None:
        return None
    return _geom_out(
        _udf_st_wrapx(_geom_bytes(geom), wrap_x_origin, wrap_direction)
    )  # the SRID is kept


def st_legacyaswkb(geom):
    """The legacy Mosaic struct as a GEOMETRY carrying the struct's own `srid` (0 when it has none)."""
    from databricks.labs.gbx.pyvx import _legacy

    fn = "gbx_st_legacyaswkb"
    if geom is None:
        return None
    try:
        g = _legacy.legacy_to_geom(geom)
    except (MemoryError, RecursionError, ImportError):
        raise
    except ValueError as exc:  # a collection, or a type id the form does not define
        raise _parameter(fn, exc) from exc
    return _geom_out(_ewkb(g, int(_legacy._field(geom, "srid", 1) or 0)))
