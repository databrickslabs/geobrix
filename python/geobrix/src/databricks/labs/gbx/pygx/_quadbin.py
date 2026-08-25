"""Pure-Python quadbin GridX core for the pygx light tier.

Cell math via the `quadbin` package; logic the package lacks (distance, bbox
polyfill) is ported to match the heavy `gridx/grid/Quadbin.scala` exactly.
Geometry outputs are EWKB (SRID 4326), matching heavy's JTS.toEWKB.

Out-of-range coordinates are CLAMPED (not rejected): latitude is clamped to
±85.05112878° and longitude to ±180°. This is intentional web-mercator behavior
inherited from the quadbin tile scheme, not a degrade. It is distinct from BNG
and Custom grids, which return NULL (None) when a point falls outside their
declared extent.
"""

import math

import numpy as np
import quadbin
from shapely import set_srid, to_wkb, union_all
from shapely.geometry import Point, box

from ._geom import parse_geom

_MAX_RES = 26
_MAX_POLYFILL_RES = 20

# Web-mercator latitude clamp (matches Quadbin.scala LAT_MIN/LAT_MAX).
_LAT_MIN = -85.05112878
_LAT_MAX = 85.05112878

# Quadbin bit-packing constants (mirror the `quadbin.main` HEADER/FOOTER/B/S used
# by tile_to_cell). Kept as uint64 so the vectorized Morton spread below is
# bit-identical to the scalar quadbin.tile_to_cell path.
_HEADER_U64 = np.uint64(0x4000000000000000)
_FOOTER_U64 = np.uint64(0xFFFFFFFFFFFFF)
_B_U64 = [
    np.uint64(0x5555555555555555),
    np.uint64(0x3333333333333333),
    np.uint64(0x0F0F0F0F0F0F0F0F),
    np.uint64(0x00FF00FF00FF00FF),
    np.uint64(0x0000FFFF0000FFFF),
    np.uint64(0x00000000FFFFFFFF),
]
_S_U64 = [np.uint64(1), np.uint64(2), np.uint64(4), np.uint64(8), np.uint64(16)]


def point_as_cell(lon: float, lat: float, resolution: int) -> int:
    z = int(resolution)
    if z < 0 or z > _MAX_RES:
        raise ValueError(f"quadbin resolution must be in [0,{_MAX_RES}]; got {z}")
    # Match heavy Quadbin.scala pointToCell exactly: derive the (x, y) tile via the
    # heavy lonLatToTile clamp (floor + clamp to [0, n-1]), then pack via the
    # canonical CARTO tile_to_cell. NOTE: we do NOT delegate to quadbin.point_to_cell
    # — it wraps lon at the antimeridian (lon=180 -> xTile 0), whereas heavy floors
    # to n then clamps to n-1 (keeps the easternmost tile). Routing through
    # _lonlat_to_tile makes the antimeridian/pole edges bit-identical to heavy.
    x, y = _lonlat_to_tile(float(lon), float(lat), z)
    return quadbin.tile_to_cell((x, y, z))


def resolution(cell: int) -> int:
    return quadbin.get_resolution(int(cell))


def k_ring(cell: int, k: int) -> list:
    if int(k) < 0:
        raise ValueError(f"k must be >= 0; got {k}")
    return list(quadbin.k_ring(int(cell), int(k)))


def distance(cell_a: int, cell_b: int) -> int:
    if resolution(cell_a) != resolution(cell_b):
        raise ValueError("quadbin_distance: cells must be at same resolution")
    ax, ay = quadbin.cell_to_tile(int(cell_a))[:2]
    bx, by = quadbin.cell_to_tile(int(cell_b))[:2]
    return int(max(abs(ax - bx), abs(ay - by)))


def _ewkb(geom) -> bytes:
    return to_wkb(set_srid(geom, 4326), include_srid=True)


def as_wkb(cell: int) -> bytes:
    w, s, e, n = quadbin.cell_to_bounding_box(int(cell))
    return _ewkb(box(w, s, e, n))


def centroid(cell: int) -> bytes:
    # Match heavy Quadbin.scala cellCenter exactly: the ARITHMETIC MEAN of the
    # cell bbox corners ((lonMin+lonMax)/2, (latMin+latMax)/2). NOTE: we do NOT use
    # quadbin.cell_to_point — it returns the TRUE inverse-mercator cell center,
    # whose latitude differs from the corner-mean (heavy averages the two
    # mercator-projected corner latitudes). The corner-mean keeps light bit-faithful
    # to heavy's centroid.
    w, s, e, n = quadbin.cell_to_bounding_box(int(cell))
    return _ewkb(Point((w + e) / 2.0, (s + n) / 2.0))


def cell_union(cells):
    if not cells:
        return None
    polys = [box(*quadbin.cell_to_bounding_box(int(c))) for c in cells if c is not None]
    if not polys:
        return None
    return _ewkb(union_all(polys))


def _lonlat_to_tile(lon: float, lat: float, z: int):
    """Port of Quadbin.scala lonLatToTile: (lon, lat) -> clamped (xTile, yTile)."""
    lat = max(_LAT_MIN, min(_LAT_MAX, lat))
    lon = max(-180.0, min(180.0, lon))
    n = 1 if z == 0 else (1 << z)
    lat_rad = lat * math.pi / 180.0
    x = math.floor((lon + 180.0) / 360.0 * n)
    y = math.floor(
        (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi)
        / 2.0
        * n
    )
    x = max(0, min(n - 1, x))
    y = max(0, min(n - 1, y))
    return x, y


# --- vectorized cell-id kernels (numpy) ---------------------------------------
# These are bit-identical to the scalar functions above but operate on numpy
# arrays, so the pandas_udf registrations can amortize the JVM<->Python boundary.
# They reuse the exact same web-mercator clamp + Morton bit-packing as the scalar
# path (verified bit-for-bit against point_as_cell / resolution over a dense
# lon/lat grid incl. the +-180 antimeridian and +-85 pole edges).


def _lonlat_to_tile_vec(lons: np.ndarray, lats: np.ndarray, z: int):
    """Vectorized port of _lonlat_to_tile -> (xTile, yTile) uint64 arrays."""
    lat = np.clip(lats.astype(np.float64), _LAT_MIN, _LAT_MAX)
    lon = np.clip(lons.astype(np.float64), -180.0, 180.0)
    n = 1 if z == 0 else (1 << z)
    lat_rad = lat * math.pi / 180.0
    x = np.floor((lon + 180.0) / 360.0 * n)
    y = np.floor(
        (1.0 - np.log(np.tan(lat_rad) + 1.0 / np.cos(lat_rad)) / math.pi) / 2.0 * n
    )
    x = np.clip(x, 0, n - 1).astype(np.uint64)
    y = np.clip(y, 0, n - 1).astype(np.uint64)
    return x, y


def _tile_to_cell_vec(x: np.ndarray, y: np.ndarray, z: int) -> np.ndarray:
    """Vectorized port of quadbin.tile_to_cell. x,y are uint64 tile coords.

    Returns a uint64 array of packed quadbin cells (Morton-interleaved x/y).
    """
    zz = np.uint64(z)
    sh = np.uint64(32 - z)
    x = x << sh
    y = y << sh
    for i in (4, 3, 2, 1, 0):
        x = (x | (x << _S_U64[i])) & _B_U64[i]
        y = (y | (y << _S_U64[i])) & _B_U64[i]
    return (
        _HEADER_U64
        | (np.uint64(1) << np.uint64(59))
        | (zz << np.uint64(52))
        | ((x | (y << np.uint64(1))) >> np.uint64(12))
        | (_FOOTER_U64 >> np.uint64(z * 2))
    )


def point_as_cell_vec(
    lons: np.ndarray, lats: np.ndarray, resolution: int
) -> np.ndarray:
    """Vectorized point_as_cell: int64 array of quadbin cells for a fixed `z`.

    Bit-identical to point_as_cell(lon, lat, z) for every element. `resolution`
    is a single scalar (the SQL signature passes a literal/column broadcast to a
    constant per batch is handled by the caller).
    """
    z = int(resolution)
    if z < 0 or z > _MAX_RES:
        raise ValueError(f"quadbin resolution must be in [0,{_MAX_RES}]; got {z}")
    x, y = _lonlat_to_tile_vec(lons, lats, z)
    return _tile_to_cell_vec(x, y, z).view(np.int64)


def resolution_vec(cells: np.ndarray) -> np.ndarray:
    """Vectorized resolution: int array of ((cell >> 52) & 0x1F)."""
    c = cells.astype(np.uint64)
    return ((c >> np.uint64(52)) & np.uint64(0x1F)).astype(np.int64)


def polyfill(geom, resolution: int) -> list:
    """Quadbin cells covering the geometry's envelope (bbox) at `resolution`.

    Mirrors heavy Quadbin_Polyfill / Quadbin.polyfillBbox: derive the SW/NE
    tile range from the bbox corners at `resolution` and enumerate every tile.
    """
    z = int(resolution)
    if z < 0 or z > _MAX_POLYFILL_RES:
        raise ValueError(
            f"quadbin_polyfill: resolution must be in [0, {_MAX_POLYFILL_RES}]; got {z}"
        )
    parsed = parse_geom(geom)
    if parsed is None or parsed.is_empty:
        return []
    w, s, e, n = parsed.bounds  # (minx, miny, maxx, maxy)
    x0, y0 = _lonlat_to_tile(w, n, z)  # upper-left
    x1, y1 = _lonlat_to_tile(e, s, z)  # lower-right
    x_lo, x_hi = min(x0, x1), max(x0, x1)
    y_lo, y_hi = min(y0, y1), max(y0, y1)
    count = (x_hi - x_lo + 1) * (y_hi - y_lo + 1)
    if count > 1_000_000:  # mirrors Quadbin.polyfillBbox maxCells
        raise ValueError(
            f"polyfill would produce {count} cells (max=1000000); use a lower zoom"
        )
    return [
        quadbin.tile_to_cell((x, y, z))
        for x in range(x_lo, x_hi + 1)
        for y in range(y_lo, y_hi + 1)
    ]


def tessellate(geom, resolution: int) -> list:
    """Tessellate a geometry into quadbin chips: list of (cell, EWKB(intersection)).

    Polyfill the bbox, then per cell intersect its polygon with the input geom,
    emitting (cell, EWKB) and dropping empty intersections. Mirrors heavy
    Quadbin_Tessellate.execute.
    """
    cells = polyfill(geom, resolution)
    if not cells:
        return []
    parsed = parse_geom(geom)
    chips = []
    for cell in cells:
        cell_box = box(*quadbin.cell_to_bounding_box(int(cell)))
        try:
            inter = cell_box.intersection(parsed)
        except Exception:
            continue
        if inter is None or inter.is_empty:
            continue
        chips.append((int(cell), _ewkb(inter)))
    return chips
