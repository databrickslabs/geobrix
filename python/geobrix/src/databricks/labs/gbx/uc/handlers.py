"""UC Python handlers. Import-light: no pyspark, and only the dependencies the manifest declares.

A UC function is for warehousing, closely aligned to the product built-ins. Data it cannot serve
as the product would is refused with a Fix: that names the session function (or the Python API);
a parameter or form error raises. Corrupt data is NULL: a UC function has no session error tile.
Resource exhaustion (MemoryError, RecursionError) and a missing dependency (ImportError) are never data
and propagate (as the JVM's NonFatal).
"""

from __future__ import annotations

import math
import re
from typing import Optional


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
    from decimal import Decimal

    from databricks.labs.gbx.pygx import _bng

    if isinstance(raw, str):
        raw = raw.strip()
        if re.fullmatch(
            r"-?\d+\.0*", raw
        ):  # '3.0': a whole number, like the session's 3.0
            raw = raw.split(".")[0]
    elif isinstance(raw, (Decimal, float)) and float(raw).is_integer():
        raw = int(raw)  # a SQL literal 3.0 is DECIMAL; the session takes a whole double
    return _bng.get_resolution(raw)


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
    returning bytes for a GEOMETRY(ANY) result fails in the worker).

    The WKB is ISO (type 1001 for a point with Z, the form the product's ``st_asbinary`` writes): the engine's WKB
    parser refuses shapely's default EWKB Z flag (``WKB_PARSE_ERROR``, type 2147483649). The SRID travels beside the
    bytes, not in them. A geometry without Z has the same bytes in both.
    """
    if ewkb is None:
        return None
    import shapely

    g = shapely.from_wkb(ewkb)
    srid = shapely.get_srid(g)
    return _geometry_type().fromWKB(
        shapely.to_wkb(shapely.set_srid(g, 0), flavor="iso"), srid
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
    out = [
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
    _check_bytes(
        fn,
        sum(len(r["chip"] or b"") + ARRAY_ITEM_BYTES for r in out),
        "the array of chips",
        _FILL_LOWER,
    )
    return out


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
    from decimal import Decimal

    if isinstance(
        raw, Decimal
    ):  # a SQL literal 3.0 reaches a UC function as DECIMAL, not DOUBLE
        raw = float(raw)
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
    _precheck(fn, lambda: _bng_cells_bound(g, res, 0))
    try:
        cells = _bng.polyfill_str(g, res)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - a geometry that cannot be filled is NULL on UC
        return None
    _check_array(fn, len(cells), "the cells of the geometry", lower=_FILL_LOWER)
    return cells


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
    _precheck(fn, lambda: _bng_cells_bound(g, res, k, interior=False))
    try:
        cells = sorted(
            _bng.geometry_k_ring_str(g, res, k, mode or "boundary-out", coverage)
        )
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the parameters are checked above: a geometry that cannot be dilated is NULL
        return None
    _check_array(fn, len(cells))
    return cells


def bng_kring(cellid, k):
    from databricks.labs.gbx.pygx import _bng

    if cellid is None or k is None:
        return None
    cid = _bng_cell(cellid)
    if cid is None:
        return None  # a string that is no cell
    kk = int(k)
    _check_array(
        "gbx_bng_kring", (2 * max(kk, 0) + 1) ** 2
    )  # a ring of distance k is (2k+1)^2 cells
    return [_bng.format(c) for c in _bng.k_ring(cid, kk)]


def bng_cellarea(cellid):
    from databricks.labs.gbx.pygx import _bng

    cid = _bng_cell(cellid)
    if cid is None:
        return None
    try:
        return _bng.area(cid)
    except (
        KeyError
    ):  # a cell finer than 1 m: the core has no size for it, on every level
        raise ValueError(
            f"gbx_bng_cellarea: the cell {cellid} is finer than 1 m and has no area. "
            "Fix: pass a cell id of 1 m or coarser."
        ) from None


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


def _check_quadbin_fill(fn: str, count: int) -> None:
    """Refuse a quadbin fill of ``count`` tiles that the kernel will not build (a million) or the cap will not return."""
    from databricks.labs.gbx.pygx import _quadbin

    if count > _quadbin.MAX_POLYFILL_CELLS:
        raise _Refusal(
            f"{fn}: the geometry covers {count} tiles at this zoom, over the {_quadbin.MAX_POLYFILL_CELLS} a fill holds. "
            "Fix: use a lower zoom, or split the geometry into smaller parts."
        )
    _check_array(fn, count, "the cells of the geometry", lower=_FILL_LOWER)


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
        count = _quadbin.polyfill_count(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom is checked above: a geometry that cannot be read is NULL
        return None
    _check_quadbin_fill(fn, count)
    try:
        return _quadbin.polyfill(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom and the size are checked above: a geometry that cannot be filled is NULL
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
    _precheck(fn, lambda: _h3_cells_bound(g, res, k))
    try:
        cells = sorted(
            _h3.geom_expand("ring", g, res, k, mode or "boundary-out", coverage)
        )
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the parameters are checked above: a geometry that cannot be dilated is NULL
        return None
    _check_array(fn, len(cells))
    return cells


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


def _check_bytes(
    fn: str,
    size: int,
    what: str,
    lower: str = "lower k, or split the input into smaller groups",
    cap: Optional[int] = None,
) -> None:
    """Refuse a result of ``size`` bytes over the cap, with a Fix that names the session function. ``lower`` is the
    first remedy (what to change to get a smaller result). ``cap`` replaces the product cap (a bench probe copy, which
    measures above it, is the one caller that passes it)."""
    cap = _array_cap() if cap is None else cap
    if size > cap:
        from databricks.labs.gbx.pyrx.core.budget import size_text

        raise _Refusal(
            f"{fn}: {what} is {size_text(size)}, over the {size_text(cap)} a Unity Catalog function returns. "
            f"Fix: {lower}, or use the session function {fn} "
            "(after databricks.labs.gbx.register(spark)) or the Python API."
        )


def _check_array(fn: str, count: int, what: str = "the array of cells", **kw) -> None:
    """Refuse an array of ``count`` cells over the cap (see :func:`_check_bytes`)."""
    _check_bytes(fn, int(count) * ARRAY_ITEM_BYTES, what, **kw)


_FILL_LOWER = "use a coarser resolution, or split the geometry into smaller parts"

# --- the size of a cell array, estimated BEFORE the kernel builds it ----------------------------------------------------
# An upper estimate from the geometry's parts alone (their boxes, areas and lengths) and the cell size: it never falls
# under what the kernel makes, so a result is never allocated over the cap. The price is that it is an upper bound, not
# the count: it can refuse a request whose real result would have fitted (measured 1.5 to 2.9 times over in some
# shapes), and the Fix names the session function for that case. A request it lets through is still judged on the real
# cells afterwards (the backstop). A part is bounded twice, by the cells its box covers and by the cells its area grown
# by the reach of the cells could hold; the smaller counts.

_EARTH_RADIUS_KM = 6371.0088
_KM_PER_DEGREE = 111.195
# The least H3 hexagon is 0.61 of the average area at every resolution (the twelve pentagons, 0.5, are added as slack): cells
# are counted at 0.6 of it so an estimate never falls under the cells that fit in an area.
_H3_MIN_AREA_FRACTION = 0.6
_H3_PENTAGONS = 12
# A cell's reach, in edge lengths: the cells that meet a shape lie inside it grown by two edges (a cell is at most two
# edges across), and each ring beyond the first adds the distance between neighbouring centres, 1.73 edges.
_H3_REACH_EDGES = 2.2
_H3_RING_EDGES = 1.8


def _precheck(fn: str, estimate, what="the cells of the geometry", lower=None) -> None:
    """Refuse, before the kernel runs, an array whose ``estimate()`` (a count of cells) is over the cap. An estimate that
    cannot be made (an odd geometry) is no estimate: the check after the kernel still bounds the result.
    """
    try:
        count = estimate()
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - no estimate; the cells are judged after the kernel
        return
    _check_array(fn, count, what, lower=lower or _FILL_LOWER)


def _parts_of(g):
    """The non-empty parts of a geometry and their boxes ``(west, south, east, north)`` as an ``(n, 4)`` array."""
    import shapely

    parts = shapely.get_parts(g)
    parts = parts[~shapely.is_empty(parts)]
    return parts, shapely.bounds(parts)


def _square_cells_bound(
    g, x0, y0, cell_w, cell_h, cells_x, cells_y, pad, interior=True, clip=True
) -> int:
    """The most cells of a grid of squares (origin ``x0, y0``, ``cells_x`` by ``cells_y`` cells of ``cell_w`` by
    ``cell_h``) that cover the geometry and the ``pad`` cells around each part, cut to the grid unless ``clip`` is False
    (for a kernel that does not drop the cells past the grid). With ``interior`` False the cells are those within the
    reach of the parts' outlines only (what a dilation of a boundary makes).
    """
    import math

    import numpy as np
    import shapely

    parts, box = _parts_of(g)
    if not len(parts):
        return 0
    lo_x = np.floor((box[:, 0] - x0) / cell_w) - pad
    hi_x = np.floor((box[:, 2] - x0) / cell_w) + pad
    lo_y = np.floor((box[:, 1] - y0) / cell_h) - pad
    hi_y = np.floor((box[:, 3] - y0) / cell_h) + pad
    if clip:
        lo_x, hi_x = np.maximum(lo_x, 0), np.minimum(hi_x, cells_x - 1)
        lo_y, hi_y = np.maximum(lo_y, 0), np.minimum(hi_y, cells_y - 1)
    nx = np.maximum(hi_x - lo_x + 1, 0)
    ny = np.maximum(hi_y - lo_y + 1, 0)
    reach = (pad + 1) * math.hypot(
        cell_w, cell_h
    )  # a cell that meets a part lies inside its buffer by this
    inside = shapely.area(parts) if interior else 0.0
    grown = (inside + 2 * shapely.length(parts) * reach + math.pi * reach**2) / (
        cell_w * cell_h
    )
    return int(math.ceil(float(np.minimum(nx * ny, grown).sum())))


def _h3_box_cells(box, resolution: int, rings: int = 0):
    """Per lon/lat box (an ``(n, 4)`` array of west, south, east, north), the most H3 cells that meet it or lie within
    ``rings`` rings of a cell that does, and the reach in km that bounds them: those cells are disjoint and inside the box
    grown by their reach, so their count is at most the grown area over the least cell area.
    """
    import h3
    import numpy as np

    reach_km = (
        _H3_REACH_EDGES + _H3_RING_EDGES * rings
    ) * h3.average_hexagon_edge_length(resolution, "km")
    grow = reach_km / _KM_PER_DEGREE
    south = np.maximum(box[:, 1] - grow, -90.0)
    north = np.minimum(box[:, 3] + grow, 90.0)
    reach_cos = np.maximum(
        np.cos(np.radians(np.maximum(np.abs(south), np.abs(north)))), 0.05
    )
    dlon = np.radians(np.minimum(box[:, 2] - box[:, 0] + 2 * grow / reach_cos, 360.0))
    area = (
        _EARTH_RADIUS_KM**2
        * dlon
        * (np.sin(np.radians(north)) - np.sin(np.radians(south)))
    )
    least = h3.average_hexagon_area(resolution, "km^2") * _H3_MIN_AREA_FRACTION
    return area / least, reach_km, least


def _h3_boxes_cells(box, resolution: int, rings: int = 0) -> float:
    """The most H3 cells that meet the boxes (see :func:`_h3_box_cells`), plus the twelve pentagons."""
    cells, _, _ = _h3_box_cells(box, resolution, rings)
    return float(cells.sum()) + _H3_PENTAGONS


def _h3_cells_bound(g, resolution: int, k: int) -> int:
    """The most H3 cells a geometry's dilation by ``k`` rings holds. A dilation makes the cells near the outlines of the
    parts (the modes work from the boundary and the holes), so each part is bounded by its box and by the band of its
    outline: its length (in degrees, which only overstates it) times the reach, both sides.
    """
    import math

    import numpy as np
    import shapely

    parts, box = _parts_of(g)
    if not len(parts):
        return 0
    boxed, reach_km, least = _h3_box_cells(box, resolution, k)
    outline_km = shapely.length(parts) * _KM_PER_DEGREE
    band = (2 * outline_km * reach_km + math.pi * reach_km**2) / least
    return int(math.ceil(float(np.minimum(boxed, band).sum()) + _H3_PENTAGONS))


def _bng_cells_bound(g, resolution: int, pad: int, interior: bool = True) -> int:
    """The most BNG cells that cover a geometry and ``pad`` rings around it, cut to the grid (0..700000 by 0..1300000 m).
    ``interior`` False: only the cells near its outlines (a dilation)."""
    from databricks.labs.gbx.pygx import _bng

    edge = _bng.get_edge_size(resolution)
    return _square_cells_bound(
        g, 0.0, 0.0, edge, edge, 700000 // edge + 1, 1300000 // edge + 1, pad, interior
    )


def _custom_cells_bound(
    conf, g, resolution: int, pad: int, interior: bool = True, clip: bool = True
) -> int:
    """The most cells of a custom grid that cover a geometry and ``pad`` rings around it, cut to the grid unless ``clip``
    is False. ``interior`` False: only the cells near its outlines (a dilation). The custom ring kernel does not drop
    the cells past the grid (its ring runs to ``total_cells`` inclusive, one past the last cell), so a ring is bounded
    with ``clip`` False; a fill drops them and is bounded with ``clip`` True."""
    from databricks.labs.gbx.pygx import _custom

    return _square_cells_bound(
        g,
        conf.bound_x_min,
        conf.bound_y_min,
        _custom.cell_width(conf, resolution),
        _custom.cell_height(conf, resolution),
        _custom.total_cells_x(conf, resolution),
        _custom.total_cells_y(conf, resolution),
        pad,
        interior,
        clip,
    )


def _le(wkb: bytes) -> bytes:
    """A WKB or EWKB as little-endian bytes (the byte order the UC Scala handlers write); little-endian input is as is."""
    if wkb[:1] == b"\x01":
        return wkb
    import shapely

    return shapely.to_wkb(shapely.from_wkb(wkb), include_srid=True, byte_order=1)


def _ring_cells(k: int, side: int) -> int:
    """The most cells a square ring of distance k holds on a grid ``side`` cells wide: the square, cut to the grid."""
    return min(2 * k + 1, side) ** 2


def _dilation(fn: str, mode, coverage, k):
    """The checked dilation arguments of a geometry-aware ring: ``(mode, coverage, k)``; a wrong one is a PARAMETER error,
    raised before the geometry is read."""
    _mode(fn, mode)
    return mode or "boundary-out", _coverage(fn, coverage), _ring_k(fn, k)


def _cells_of(fn: str, g, run, bound=None):
    """``run(g)`` as a cell array: NULL for a NULL, non-finite or unreadable geometry (data), else bounded by the cap:
    before ``run`` by ``bound(g)`` (an upper estimate of the cells, when the grid has one), after it by their count.
    """
    if g is None or not _finite(g):
        return None  # a non-finite coordinate has no cell
    if bound is not None:
        _precheck(fn, lambda: bound(g))
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
        count = _quadbin.polyfill_count(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom is checked above: a geometry that cannot be read is NULL
        return None
    _check_quadbin_fill(fn, count)
    try:
        chips = _quadbin.tessellate(g, z)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the zoom and the size are checked above: a geometry that cannot be cut is NULL
        return None
    out = [{"cell": int(c), "geom": _le(chip)} for c, chip in chips]
    _check_bytes(
        fn,
        sum(len(r["geom"]) + ARRAY_ITEM_BYTES for r in out),
        "the array of chips",
        _FILL_LOWER,
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


# --- custom grid ----------------------------------------------------------------------------------------------------
# The grid is a struct from gbx_custom_grid. Its srid is the SRID of the cells it makes (-1: the grid has no CRS), and a
# geometry given with it must carry that SRID (0 for a grid with no CRS): the cells are metres or degrees of that CRS.


def _custom_conf(fn: str, grid):
    """The grid struct as the kernel's configuration; one that gbx_custom_grid would refuse is a PARAMETER error."""
    from databricks.labs.gbx.pygx import _custom

    try:
        conf = _custom.conf_from_row(grid)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError(
            f"{fn}: the grid is not a custom grid ({exc}). Fix: build it with gbx_custom_grid."
        ) from exc
    problem = next(
        (
            text
            for bad, text in (
                (
                    conf.bound_x_max <= conf.bound_x_min,
                    "bound_x_max must be greater than bound_x_min",
                ),
                (
                    conf.bound_y_max <= conf.bound_y_min,
                    "bound_y_max must be greater than bound_y_min",
                ),
                (conf.cell_splits < 2, "cell_splits must be >= 2"),
                (
                    conf.root_cell_size_x <= 0 or conf.root_cell_size_y <= 0,
                    "root_cell_size_x and root_cell_size_y must be > 0",
                ),
            )
            if bad
        ),
        "",
    )
    if problem:
        raise ValueError(
            f"{fn}: the grid is not a custom grid ({problem}). Fix: build it with gbx_custom_grid."
        )
    return conf


def _custom_srid(conf) -> int:
    """The SRID of the grid's cells and geometries: its srid, or 0 for a grid with no CRS (srid -1)."""
    return conf.srid if conf.srid > 0 else 0


def _custom_geometry(fn: str, geom, conf):
    srid = _custom_srid(conf)
    hint = (
        f"st_transform(geom, {srid}) first"
        if srid
        else "the grid has no CRS, so give a geometry with SRID 0, for example st_setsrid(geom, 0)"
    )
    return _grid_geometry(fn, geom, srid, "the custom grid", hint)


def _custom_res(fn: str, conf, resolution) -> int:
    return _range(fn, "resolution", _whole(fn, resolution), 0, conf.max_resolution)


def _custom_cell(conf, cell):
    """A custom cell id as an int, or None when it is no cell of this grid (data): a resolution the grid does not have,
    or a position past its last cell."""
    from databricks.labs.gbx.pygx import _custom

    if cell is None:
        return None
    try:
        c = int(cell)
    except (TypeError, ValueError, OverflowError):
        return None
    if c < 0:
        return None
    res = _custom.get_cell_resolution(c)
    if res > conf.max_resolution:
        return None
    cells = _custom.total_cells_x(conf, res) * _custom.total_cells_y(conf, res)
    return c if _custom.get_cell_position(c) < cells else None


def custom_pointascell(point, grid, resolution):
    import shapely

    from databricks.labs.gbx.pygx import _custom

    fn = "gbx_custom_pointascell"
    if point is None or grid is None or resolution is None:
        return None
    conf = _custom_conf(fn, grid)
    res = _custom_res(fn, conf, resolution)
    g = _custom_geometry(fn, point, conf)
    if g is None or g.is_empty:
        return None
    x, y = (
        float(v) for v in shapely.get_coordinates(g)[0]
    )  # the first coordinate, not the centroid
    if not (math.isfinite(x) and math.isfinite(y)):
        return None
    return _custom.point_to_cell_id_or_none(conf, x, y, res)


def custom_cellaswkb(cell, grid):
    """The cell polygon as EWKB with the grid's SRID (plain WKB for a grid with no CRS)."""
    from databricks.labs.gbx.pygx import _custom

    if cell is None or grid is None:
        return None
    conf = _custom_conf("gbx_custom_cellaswkb", grid)
    c = _custom_cell(conf, cell)
    if c is None:
        return None
    return _ewkb(_custom.cell_id_to_polygon(conf, c), _custom_srid(conf))


def custom_cellaswkt(cell, grid):
    from databricks.labs.gbx.pygx import _custom

    if cell is None or grid is None:
        return None
    conf = _custom_conf("gbx_custom_cellaswkt", grid)
    c = _custom_cell(conf, cell)
    return None if c is None else _custom.cell_aswkt(conf, c)


def custom_centroid(cell, grid):
    """The cell centre as EWKB with the grid's SRID (plain WKB for a grid with no CRS)."""
    from databricks.labs.gbx.pygx import _custom

    if cell is None or grid is None:
        return None
    conf = _custom_conf("gbx_custom_centroid", grid)
    c = _custom_cell(conf, cell)
    if c is None:
        return None
    return _ewkb(_custom.cell_id_to_centroid(conf, c), _custom_srid(conf))


def custom_distance(cellid1, grid, cellid2):
    from databricks.labs.gbx.pygx import _custom

    if cellid1 is None or grid is None or cellid2 is None:
        return None
    conf = _custom_conf("gbx_custom_distance", grid)
    a, b = _custom_cell(conf, cellid1), _custom_cell(conf, cellid2)
    return None if a is None or b is None else int(_custom.distance(conf, a, b))


def _custom_ring(fn: str, cell, grid, k, kernel):
    """kring and kloop of a cell: the arguments checked, the ring's size (cut to the grid, as the kernel cuts it) bounded
    before the kernel builds it."""
    from databricks.labs.gbx.pygx import _custom

    if cell is None or grid is None or k is None:
        return None
    conf = _custom_conf(fn, grid)
    kk = _ring_k(fn, k)
    c = _custom_cell(conf, cell)
    if c is None:
        return None
    res = _custom.get_cell_resolution(c)
    pos = _custom.get_cell_position(c)
    px, py = (
        _custom.get_cell_position_x(conf, pos, res),
        _custom.get_cell_position_y(conf, pos, res),
    )
    nx = min(px + kk, _custom.total_cells_x(conf, res)) - max(px - kk, 0) + 1
    ny = min(py + kk, _custom.total_cells_y(conf, res)) - max(py - kk, 0) + 1
    _check_array(fn, nx * ny)  # a loop is built as the ring minus the one inside it
    return kernel(conf, c, kk)


def custom_kring(cell, grid, k):
    from databricks.labs.gbx.pygx import _custom

    return _custom_ring("gbx_custom_kring", cell, grid, k, _custom.k_ring)


def custom_kloop(cell, grid, k):
    from databricks.labs.gbx.pygx import _custom

    return _custom_ring("gbx_custom_kloop", cell, grid, k, _custom.k_loop)


def custom_polyfill(geom, grid, resolution):
    from databricks.labs.gbx.pygx import _custom

    fn = "gbx_custom_polyfill"
    if geom is None or grid is None or resolution is None:
        return None
    conf = _custom_conf(fn, grid)
    res = _custom_res(fn, conf, resolution)
    g = _custom_geometry(fn, geom, conf)
    if g is None:
        return None
    # the fill tests the cells of the box and one more on each side, so it is counted with a pad of one
    _precheck(fn, lambda: _custom_cells_bound(conf, g, res, 1))
    try:
        cells = _custom.polyfill(conf, g, res)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        Exception
    ):  # noqa: BLE001 - the resolution is checked above: a geometry that cannot be filled is NULL
        return None
    _check_array(fn, len(cells), "the cells of the geometry", lower=_FILL_LOWER)
    return cells


def _custom_geom_ring(fn: str, geom, grid, resolution, k, mode, coverage, kernel):
    if geom is None or grid is None or resolution is None or k is None:
        return None
    conf = _custom_conf(fn, grid)
    mode, coverage, kk = _dilation(fn, mode, coverage, k)
    res = _custom_res(fn, conf, resolution)
    g = _custom_geometry(fn, geom, conf)
    return _cells_of(
        fn,
        g,
        lambda g: kernel(conf, g, res, kk, mode, coverage),
        bound=lambda g: _custom_cells_bound(
            conf, g, res, kk, interior=False, clip=False
        ),
    )


def custom_geomkring(
    geom, grid, resolution, k, mode="boundary-out", coverage="coveras"
):
    from databricks.labs.gbx.pygx import _custom

    return _custom_geom_ring(
        "gbx_custom_geomkring",
        geom,
        grid,
        resolution,
        k,
        mode,
        coverage,
        _custom.geometry_k_ring,
    )


def custom_geomkloop(
    geom, grid, resolution, k, mode="boundary-out", coverage="coveras"
):
    from databricks.labs.gbx.pygx import _custom

    return _custom_geom_ring(
        "gbx_custom_geomkloop",
        geom,
        grid,
        resolution,
        k,
        mode,
        coverage,
        _custom.geometry_k_loop,
    )


# --- BNG ------------------------------------------------------------------------------------------------------------


def bng_aswkt(cellid):
    """The cell polygon as WKT (EPSG:27700 metres)."""
    from databricks.labs.gbx.pygx import _bng

    cid = _bng_cell(cellid)
    return None if cid is None else _bng.cell_aswkt(cid)


def bng_distance(cellid1, cellid2):
    from databricks.labs.gbx.pygx import _bng

    if cellid1 is None or cellid2 is None:
        return None
    a, b = _bng_cell(cellid1), _bng_cell(cellid2)
    return None if a is None or b is None else int(_bng.distance(a, b))


def bng_euclideandistance(cellid1, cellid2):
    from databricks.labs.gbx.pygx import _bng

    if cellid1 is None or cellid2 is None:
        return None
    a, b = _bng_cell(cellid1), _bng_cell(cellid2)
    return None if a is None or b is None else int(_bng.euclidean_distance(a, b))


def bng_kloop(cellid, k):
    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_kloop"
    if cellid is None or k is None:
        return None
    kk = _ring_k(fn, k)
    cid = _bng_cell(cellid)
    if cid is None:
        return None  # a string that is no cell
    _check_array(fn, 8 * kk)  # a loop of distance k is 8k cells
    return [_bng.format(c) for c in _bng.k_loop(cid, kk)]


def bng_geomkloop(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _bng

    fn = "gbx_bng_geomkloop"
    if geom is None or resolution is None or k is None:
        return None
    res = _bng_res(fn, resolution)
    mode, coverage, kk = _dilation(fn, mode, coverage, k)
    g = _grid_geometry(fn, geom, BNG_SRID, "BNG")
    return _cells_of(
        fn,
        g,
        lambda g: sorted(_bng.geometry_k_loop_str(g, res, kk, mode, coverage)),
        bound=lambda g: _bng_cells_bound(g, res, kk, interior=False),
    )


def _chip(fn: str, chip):
    """A chip struct ``{cellid, core, chip}`` as ``(cellid, core, shapely geometry)``: the geometry is EWKB with SRID
    27700 (what gbx_bng_tessellate writes) or plain WKB, and NULL bytes are an empty geometry (an empty intersection).
    """
    import shapely

    from databricks.labs.gbx.pygx import _bng

    if isinstance(chip, (tuple, list)):  # a plain tuple is read by position
        cellid, core, raw = chip[0], chip[1], chip[2]
    else:
        cellid, core, raw = (_field(chip, n) for n in ("cellid", "core", "chip"))
    if raw is None:
        return cellid, bool(core), _bng._empty_polygon()
    g = shapely.from_wkb(bytes(raw))
    srid = shapely.get_srid(g)
    if srid not in (0, BNG_SRID):
        raise _refuse(
            fn,
            f"BNG chips are in SRID {BNG_SRID}; got a chip in SRID {srid} (st_transform the chip geometry first)",
        )
    return cellid, bool(core), g


def _chip_pair(fn: str, left, right, kernel):
    if left is None or right is None:
        return None
    a, b = _chip(fn, left), _chip(fn, right)
    try:
        cellid, core, g = kernel(a, b)
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - a chip that cannot be combined is NULL
        return None
    return {
        "cellid": cellid,
        "core": bool(core),
        "chip": None if g is None or g.is_empty else _ewkb(g, BNG_SRID),
    }


def bng_cellunion(left_chip, right_chip):
    from databricks.labs.gbx.pygx import _bng

    return _chip_pair("gbx_bng_cellunion", left_chip, right_chip, _bng.cell_union)


def bng_cellintersection(left_chip, right_chip):
    from databricks.labs.gbx.pygx import _bng

    return _chip_pair(
        "gbx_bng_cellintersection", left_chip, right_chip, _bng.cell_intersection
    )


# --- H3 -------------------------------------------------------------------------------------------------------------


def h3_geomkloop(geom, resolution, k, mode="boundary-out", coverage="coveras"):
    from databricks.labs.gbx.pygx import _h3

    fn = "gbx_h3_geomkloop"
    if geom is None or resolution is None or k is None:
        return None
    mode, coverage, kk = _dilation(fn, mode, coverage, k)
    res = _range(fn, "resolution", _whole(fn, resolution), 0, H3_MAX_RES)
    g = _grid_geometry(fn, geom, WGS84_SRID, "H3")
    return _cells_of(
        fn,
        g,
        lambda g: sorted(_h3.geom_expand("loop", g, res, kk, mode, coverage)),
        bound=lambda g: _h3_cells_bound(g, res, kk),
    )


def h3_cell_bbox(cellid, srid=4326, mode="centroids", kring_pad=0, out_crs=None):
    """The box of an H3 cell (or of it and its ``kring_pad`` rings) as ``(xmin, ymin, xmax, ymax)`` in the output CRS:
    ``out_crs`` when it is set, else ``srid``. A CRS is an SRID, ``EPSG:<n>`` or ``ESRI:<n>``; any other form is refused.
    """
    import h3

    from databricks.labs.gbx.core.crs import get_transformer, parse_uc_crs

    fn = "gbx_h3_cell_bbox"
    if cellid is None:
        return None
    mode = mode or "centroids"
    if mode not in ("centroids", "spatial_envelope"):
        raise _parameter(
            fn,
            ValueError(
                f"unknown mode {mode!r}; expected 'centroids' or 'spatial_envelope'"
            ),
        )
    pad = 0 if kring_pad is None else _ring_k(fn, kring_pad)
    spec = out_crs if out_crs is not None and str(out_crs).strip() else srid
    crs = parse_uc_crs(4326 if spec is None else spec, fn=fn)
    try:
        cell = h3.int_to_str(int(cellid))
        if not h3.is_valid_cell(cell):
            return None  # an id that is no H3 cell
    except (TypeError, ValueError, OverflowError):
        return None
    _check_array(fn, 3 * pad * (pad + 1) + 1, "the cells around the cell")
    cells = list(h3.grid_disk(cell, pad)) if pad else [cell]
    lons, lats = [], []
    for c in cells:
        if mode == "centroids":
            lat, lon = h3.cell_to_latlng(c)
            lons.append(lon)
            lats.append(lat)
        else:
            for lat, lon in h3.cell_to_boundary(c):
                lons.append(lon)
                lats.append(lat)
    if crs.code == f"EPSG:{WGS84_SRID}":
        xs, ys = lons, lats  # the cells are already in the output CRS
    else:
        tr = get_transformer(f"EPSG:{WGS84_SRID}", crs.code)
        if (
            len(lons) == 1
        ):  # pyproj reads a one-element array as a scalar, and numpy deprecates that
            x, y = tr.transform(lons[0], lats[0])
            xs, ys = [x], [y]
        else:
            xs, ys = tr.transform(lons, lats)
    return {
        "xmin": float(min(xs)),
        "ymin": float(min(ys)),
        "xmax": float(max(xs)),
        "ymax": float(max(ys)),
    }


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


# --- the shapely scalars: GEOMETRY(ANY) in and out, the SRID of the first argument kept ----------------------------
# One shape for the seven (gbx_st_node, _reduceprecision, _removerepeatedpoints, _shiftlongitude,
# _simplifypreservetopology, _snap, _split), over the session's own kernels (``pyvx._cleaning``, ``pyvx._antimeridian``).
# A NULL geometry is NULL. A wrong distance or grid size raises with a Fix. A form the operation does not take (a point
# has nothing to cut) raises with a Fix. A geometry GEOS cannot process (an invalid one the operation needs valid) is
# NULL on UC, where a session raises. Two geometries must share an SRID, as the product's own ST functions require.


def _two_srids(fn: str, first: bytes, second: bytes) -> None:
    import shapely

    a, b = (shapely.get_srid(shapely.from_wkb(x)) for x in (first, second))
    if a != b:
        raise _Refusal(
            f"{fn}: the two geometries are in different SRIDs ({a} and {b}). "
            "Fix: st_transform one of them first, so both are in the same SRID"
        )


def _shapely_scalar(fn: str, kernel, geoms, *args, check=None):
    """GEOMETRY(ANY) result of ``kernel(*geoms as EWKB, *args)``; None when any geometry is NULL."""
    from shapely.errors import GeometryTypeError, ShapelyError

    if any(g is None for g in geoms):
        return None
    ewkb = [_geom_bytes(g) for g in geoms]
    if len(ewkb) > 1:
        _two_srids(fn, ewkb[0], ewkb[1])
    if check is not None:
        check(ewkb)
    try:
        out = kernel(*ewkb, *args)
    except (MemoryError, RecursionError, ImportError):
        raise
    except GeometryTypeError as exc:  # a form the operation does not take
        raise _Refusal(
            f"{fn}: {exc}. Fix: pass a geometry type {fn} takes, or cast it first"
        ) from exc
    except ValueError as exc:  # a distance or a grid size that is no usable number
        raise _parameter(fn, exc) from exc
    except ShapelyError:  # noqa: BLE001 - a geometry GEOS cannot process is NULL on UC
        return None
    return _geom_out(out)  # the SRID is kept


def st_node(geom):
    from databricks.labs.gbx.pyvx import _cleaning

    return _shapely_scalar("gbx_st_node", _cleaning.st_node, (geom,))


def st_reduceprecision(geom, grid_size):
    from databricks.labs.gbx.pyvx import _cleaning

    if grid_size is None:
        return None
    return _shapely_scalar(
        "gbx_st_reduceprecision", _cleaning.st_reduceprecision, (geom,), grid_size
    )


def st_removerepeatedpoints(geom, tolerance):
    from databricks.labs.gbx.pyvx import _cleaning

    # a NULL tolerance is the default (exact repeats only), as in a session
    return _shapely_scalar(
        "gbx_st_removerepeatedpoints",
        _cleaning.st_removerepeatedpoints,
        (geom,),
        0.0 if tolerance is None else tolerance,
    )


def st_shiftlongitude(geom):
    from databricks.labs.gbx.pyvx import _antimeridian

    return _shapely_scalar(
        "gbx_st_shiftlongitude", _antimeridian.st_shiftlongitude, (geom,)
    )


def st_simplifypreservetopology(geom, tolerance):
    from databricks.labs.gbx.pyvx import _cleaning

    if tolerance is None:
        return None
    return _shapely_scalar(
        "gbx_st_simplifypreservetopology",
        _cleaning.st_simplifypreservetopology,
        (geom,),
        tolerance,
    )


def st_snap(geom, reference, tolerance):
    from databricks.labs.gbx.pyvx import _cleaning

    if tolerance is None:
        return None
    return _shapely_scalar(
        "gbx_st_snap", _cleaning.st_snap, (geom, reference), tolerance
    )


def _split_takes_lines_and_polygons(ewkb) -> None:
    """A point has nothing to cut. GEOS says so in words that differ by shapely version (a type error, or an
    IllegalArgumentException), so the handler states it once, the same on every version.
    """
    import shapely

    kind = shapely.from_wkb(ewkb[0]).geom_type
    if kind in ("Point", "MultiPoint"):
        raise _Refusal(
            f"gbx_st_split: Splitting {kind} geometry is not supported. "
            "Fix: pass a line or a polygon to split, or use st_buffer to turn the points into polygons first"
        )


def st_split(input_geom, blade_geom):
    from databricks.labs.gbx.pyvx import _antimeridian

    return _shapely_scalar(
        "gbx_st_split",
        _antimeridian.st_split,
        (input_geom, blade_geom),
        check=_split_takes_lines_and_polygons,
    )


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
