"""Pure-Python British National Grid (BNG) core for the pygx light tier.

A faithful port of the heavy ``com.databricks.labs.gbx.gridx.grid.BNG`` Scala
object (gridx/grid/BNG.scala). No PyPI BNG library exists; this module reproduces
the codec, cell geometry, neighborhood walks, polyfill, and tessellation EXACTLY
so light and heavy share bit-identical cell ids and cell sets.

Coordinates are EPSG:27700 eastings/northings. Cell ids are STRING in the public
surface (``format``/``parse`` round-trip a Long digit-id internally). Geometry is
emitted as plain WKB (NO SRID) and WKT, matching heavy ``JTS.toWKB``/``toWKT``
(BNG does NOT stamp an SRID, unlike quadbin which is EWKB SRID 4326).

Mosaic-lineage bug status (validated against this port):
  * mosaic#434 (fixed upstream in mosaic#580): 100km NE/NW/SE/SW cells are res 1,
    not quadrant resolutions. Carried here by the digit-length + trailing-quadrant
    logic in ``get_resolution_from_digits``/``format`` (a 100km id is 6 digits,
    trailing quadrant-digit 0).
  * mosaic#423: grid-aligned polygons must not emit POINT/LINESTRING chips. Carried
    here by the input-geometry-type filter on border chips in ``tessellate``.
"""

import math
from typing import Any

import numpy as np
from shapely import equals_exact as _equals_exact
from shapely import to_wkb as _to_wkb
from shapely.geometry import Point as _Point
from shapely.geometry import box as _box
from shapely.geometry import mapping as _mapping

from ._geom import parse_geom

CRS_ID = 27700
NAME = "BNG"

QUADRANTS = ["", "SW", "NW", "NE", "SE"]

RESOLUTION_MAP = {
    "500km": -1,
    "100km": 1,
    "50km": -2,
    "10km": 2,
    "5km": -3,
    "1km": 3,
    "500m": -4,
    "100m": 4,
    "50m": -5,
    "10m": 5,
    "5m": -6,
    "1m": 6,
}
SIZE_MAP = {
    "500km": 500000,
    "100km": 100000,
    "50km": 50000,
    "10km": 10000,
    "5km": 5000,
    "1km": 1000,
    "500m": 500,
    "100m": 100,
    "50m": 50,
    "10m": 10,
    "5m": 5,
    "1m": 1,
}
RESOLUTIONS = {1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6}

LETTER_MAP = [
    ["SV", "SW", "SX", "SY", "SZ", "TV", "TW", "TX"],
    ["SQ", "SR", "SS", "ST", "SU", "TQ", "TR", "TS"],
    ["SL", "SM", "SN", "SO", "SP", "TL", "TM", "TN"],
    ["SF", "SG", "SH", "SJ", "SK", "TF", "TG", "TH"],
    ["SA", "SB", "SC", "SD", "SE", "TA", "TB", "TC"],
    ["NV", "NW", "NX", "NY", "NZ", "OV", "OW", "OX"],
    ["NQ", "NR", "NS", "NT", "NU", "OQ", "OR", "OS"],
    ["NL", "NM", "NN", "NO", "NP", "OL", "OM", "ON"],
    ["NF", "NG", "NH", "NJ", "NK", "OF", "OG", "OH"],
    ["NA", "NB", "NC", "ND", "NE", "OA", "OB", "OC"],
    ["HV", "HW", "HX", "HY", "HZ", "JV", "JW", "JX"],
    ["HQ", "HR", "HS", "HT", "HU", "JQ", "JR", "JS"],
    ["HL", "HM", "HN", "HO", "HP", "JL", "JM", "JN"],
    ["HF", "HG", "HH", "HJ", "HK", "JF", "JG", "JH"],
]


def get_resolution(res: Any) -> int:
    """Resolution Int. Overloads BNG.getResolution(Any) and BNG.getResolution(Seq[Int]).

    - A list/tuple of digits dispatches to ``get_resolution_from_digits`` (the
      Scala ``getResolution(Seq[Int])`` overload: resolution implied by digit
      length + trailing quadrant marker).
    - An Int index passes through if in RESOLUTIONS; a resolutionMap string key
      maps to its index. NEVER accepts metres-as-Int (e.g. 1000).
    """
    if isinstance(res, (list, tuple)):
        return get_resolution_from_digits(res)
    if isinstance(res, bool):
        raise ValueError(f"BNG resolution not supported; found {res!r}")
    if isinstance(res, int):
        if res in RESOLUTIONS:
            return res
        raise ValueError(
            f"BNG resolution index must be one of {sorted(RESOLUTIONS)} "
            f"(1=100km..6=1m, negatives=quadrants); got {res}. "
            "Metres-as-Int (e.g. 1000) is NOT a resolution."
        )
    if isinstance(res, str) and res in RESOLUTION_MAP:
        return RESOLUTION_MAP[res]
    raise ValueError(f"BNG resolution not supported; found {res!r}")


def get_edge_size(resolution: int) -> int:
    """Edge size (metres) for an Int resolution (BNG.getEdgeSize(Int))."""
    res_str = get_resolution_str(resolution)
    return SIZE_MAP[res_str]


def get_resolution_str(resolution: int) -> str:
    for k, v in RESOLUTION_MAP.items():
        if v == resolution:
            return k
    return ""


def cell_digits(cell_id: int) -> list:
    """Cell id Long -> list of decimal digits (BNG.cellDigits).

    A negative id can arise from an out-of-GB coordinate (the encoder runs before
    ``is_valid``); take the digits of its magnitude so ``is_valid`` can reject it
    via its len/bounds checks instead of crashing on the ``-`` sign.
    """
    return [int(c) for c in str(abs(int(cell_id)))]


def _safe_digit_index(digit_slice, max_idx: int) -> int:
    s = "".join(str(d) for d in digit_slice)
    n = 0 if s == "" else int(s)
    return max(0, min(max_idx, n))


def get_resolution_from_digits(digits) -> int:
    """BNG.getResolution(Seq[Int]) — resolution implied by digit length + quadrant."""
    if len(digits) < 6:
        return -1  # 500km
    quadrant = digits[-1]
    k = (len(digits) - 6) // 2
    return -(k + 2) if quadrant > 0 else k + 1


def get_x(digits, edge_size: int) -> int:
    n = len(digits)
    k = (n - 6) // 2
    x_digits = digits[1:3] + digits[5 : 5 + k]
    quadrant = digits[-1]
    edge_adj = 2 * edge_size if quadrant > 0 else edge_size
    x_offset = edge_size if quadrant in (3, 4) else 0
    return int("".join(str(d) for d in x_digits)) * edge_adj + x_offset


def get_y(digits, edge_size: int) -> int:
    n = len(digits)
    k = (n - 6) // 2
    y_digits = digits[3:5] + digits[5 + k : 5 + 2 * k]
    quadrant = digits[-1]
    edge_adj = 2 * edge_size if quadrant > 0 else edge_size
    y_offset = edge_size if quadrant in (2, 3) else 0
    return int("".join(str(d) for d in y_digits)) * edge_adj + y_offset


def _get_quadrant_core(resolution, eastings, northings, divisor):
    """Numpy-polymorphic quadrant (0 for res >= -1; 1/2/4/3 for res < -1).

    ``eastings``/``northings`` are int64 (scalar or array). Mirrors the scalar
    ``get_quadrant`` if-chain order via nested ``np.where``.
    """
    if resolution >= -1:
        return eastings * 0  # int64 zeros, scalar or array
    e_q = eastings / divisor
    n_q = northings / divisor
    e_dec = e_q - np.floor(e_q)
    n_dec = n_q - np.floor(n_q)
    q = np.where(
        (e_dec < 0.5) & (n_dec < 0.5),
        1,
        np.where(e_dec < 0.5, 2, np.where(n_dec < 0.5, 4, 3)),
    )
    return q.astype(np.int64)


def get_quadrant(
    resolution: int, eastings: float, northings: float, divisor: float
) -> int:
    """Scalar wrapper over :func:`_get_quadrant_core` (unchanged public behavior)."""
    return int(_get_quadrant_core(resolution, eastings, northings, divisor))


def _encode_core(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution):
    """Pack the BNG digit-id in int64 (scalar or array). Bit-exact with the
    original pure-integer ``encode`` (the res==-1 ``/100`` is always exactly
    divisible, so integer ``//100`` matches ``int(float/100)``)."""
    id_placeholder = 10 ** (5 + 2 * n_positions - 2)
    e_letter_shift = 10 ** (3 + 2 * n_positions - 2)
    n_letter_shift = 10 ** (1 + 2 * n_positions - 2)
    e_shift = 10**n_positions
    n_shift = 10
    if resolution == -1:
        val = (id_placeholder + e_letter * e_letter_shift) // 100 + quadrant
    else:
        val = (
            id_placeholder
            + e_letter * e_letter_shift
            + n_letter * n_letter_shift
            + e_bin * e_shift
            + n_bin * n_shift
            + quadrant
        )
    return val


def encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution) -> int:
    """Scalar wrapper over :func:`_encode_core` (unchanged public behavior)."""
    return int(
        _encode_core(
            e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution
        )
    )


def _point_to_cell_id_core(e, n, resolution: int):
    """Numpy-polymorphic BNG encoder core (int64 result; scalar or array).

    Mirrors the scalar ``point_to_cell_id`` EXACTLY, incl. truncation semantics:
    letter indices use ``int(e_int/100000)`` (truncate toward zero -> ``np.trunc``);
    bins use ``math.floor(.../divisor)`` (floor -> ``np.floor``). These differ for
    negative (out-of-GB) coords, which the encoder sees (encode runs before is_valid).
    """
    e_int = np.trunc(e).astype(np.int64)
    n_int = np.trunc(n).astype(np.int64)
    # int(e_int/100000): float division then truncate toward zero (NOT floor-div).
    e_letter = np.trunc(e_int / 100000).astype(np.int64)
    n_letter = np.trunc(n_int / 100000).astype(np.int64)
    if resolution < 0:
        divisor = 10 ** (6 - abs(resolution) + 1)
    else:
        divisor = 10 ** (6 - resolution)
    quadrant = _get_quadrant_core(resolution, e_int, n_int, divisor)
    n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
    # math.floor((e_int % 100000) / divisor): float division then floor.
    e_bin = np.floor((e_int % 100000) / divisor).astype(np.int64)
    n_bin = np.floor((n_int % 100000) / divisor).astype(np.int64)
    return _encode_core(
        e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution
    )


def point_to_cell_id(eastings: float, northings: float, resolution: int) -> int:
    """Scalar wrapper over :func:`_point_to_cell_id_core` (unchanged behavior)."""
    if math.isnan(eastings) or math.isnan(northings):
        raise ValueError("NaN coordinates are not supported.")
    return int(_point_to_cell_id_core(float(eastings), float(northings), resolution))


def point_to_cell_id_vec(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized BNG encoder: array of int64 cell ids from EPSG:27700 (e, n).

    Shares :func:`_point_to_cell_id_core` with the scalar ``point_to_cell_id``, so
    the two forms cannot drift. Fed clean (post-mask) coords, so no NaN guard.
    """
    e = np.asarray(e, dtype="float64")
    n = np.asarray(n, dtype="float64")
    return _point_to_cell_id_core(e, n, resolution).astype(np.int64)


def format(cell_id: int) -> str:
    digits = cell_digits(cell_id)
    if len(digits) < 6:
        x_idx = _safe_digit_index(digits[3:5], len(LETTER_MAP) - 1)
        y_idx = _safe_digit_index(digits[1:3], len(LETTER_MAP[0]) - 1)
        return LETTER_MAP[x_idx][y_idx][0]
    q_idx = max(0, min(len(QUADRANTS) - 1, digits[-1]))
    x_idx = _safe_digit_index(digits[3:5], len(LETTER_MAP) - 1)
    y_idx = _safe_digit_index(digits[1:3], len(LETTER_MAP[0]) - 1)
    prefix = LETTER_MAP[x_idx][y_idx]
    coords = digits[5:-1]
    k = len(coords) // 2
    if not coords:
        x_str = y_str = ""
    else:
        x_part = coords[:k]
        y_part = coords[k : 2 * k]
        # padTo(k, 0): right-pad to length k with zeros.
        x_part = x_part + [0] * (k - len(x_part))
        y_part = y_part + [0] * (k - len(y_part))
        x_str = "".join(str(d) for d in x_part)
        y_str = "".join(str(d) for d in y_part)
    return f"{prefix}{x_str}{y_str}{QUADRANTS[q_idx]}"


def parse_safe(cell_id: str):
    """Bad-DATA-tolerant parse: a malformed BNG cell string (unrecognised prefix or
    non-digit body) returns None rather than raising StopIteration/ValueError, so one
    bad cell id in a column degrades to NULL instead of killing the stage."""
    try:
        return parse(cell_id)
    except (
        Exception
    ):  # noqa: BLE001 — StopIteration (bad prefix) or ValueError (bad body)
        return None


def parse(cell_id: str) -> int:
    prefix = cell_id[:2] if len(cell_id) >= 2 else f"{cell_id}V"
    letter_row = next(row for row in LETTER_MAP if prefix in row)
    e_letter = letter_row.index(prefix)
    n_letter = LETTER_MAP.index(letter_row)
    if len(cell_id) == 1:
        return encode(e_letter, 0, 0, 0, 0, 1, -1)
    suffix = cell_id[-2:]
    quadrant = (
        QUADRANTS.index(suffix) if (suffix in QUADRANTS and len(cell_id) > 2) else 0
    )
    bin_digits = cell_id[2:-2] if quadrant > 0 else cell_id[2:]
    if not bin_digits:
        return encode(e_letter, n_letter, 0, 0, quadrant, 1, -2)
    half = len(bin_digits) // 2
    # Scala: dropRight(len/2) keeps the LEFT half, drop(len/2) keeps the RIGHT half.
    e_bin = int(bin_digits[: len(bin_digits) - half] or "0")
    n_bin = int(bin_digits[half:] or "0")
    n_positions = half + 1
    resolution = (n_positions + 1) if quadrant == 0 else -n_positions
    return encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution)


def area(cell_id: int) -> float:
    """Cell area in square KILOMETRES (BNG.area): (edgeSize/1000)^2."""
    resolution = get_resolution_from_digits(cell_digits(cell_id))
    edge = float(get_edge_size(resolution))
    return (edge / 1000.0) ** 2


def distance(cell_id: int, cell_id2: int) -> int:
    """Manhattan grid distance in edge-size units (BNG.distance)."""
    d1, d2 = cell_digits(cell_id), cell_digits(cell_id2)
    edge = get_edge_size(
        min(get_resolution_from_digits(d1), get_resolution_from_digits(d2))
    )
    x1, x2 = get_x(d1, edge), get_x(d2, edge)
    y1, y2 = get_y(d1, edge), get_y(d2, edge)
    return abs(x1 - x2) // edge + abs(y1 - y2) // edge


def euclidean_distance(cell_id: int, cell_id2: int) -> int:
    """Chebyshev (max of dx, dy) grid distance in edge-size units.

    Mirrors BNG.euclideanDistance: along a diagonal the distance is 1 where
    Manhattan would be 2.
    """
    d1, d2 = cell_digits(cell_id), cell_digits(cell_id2)
    edge = get_edge_size(
        min(get_resolution_from_digits(d1), get_resolution_from_digits(d2))
    )
    x1, x2 = get_x(d1, edge), get_x(d2, edge)
    y1, y2 = get_y(d1, edge), get_y(d2, edge)
    return max(abs(x1 - x2), abs(y1 - y2)) // edge


def point_as_cell(eastings: float, northings: float, resolution) -> str:
    """EPSG:27700 (eastings, northings) -> STRING cellid (BNG_EastNorthAsBNG core)."""
    res = get_resolution(resolution)
    return format(point_to_cell_id(float(eastings), float(northings), res))


# east_north_as_bng is an alias of point_as_cell at the coordinate level; the SQL
# split (pointascell takes a POINT geom, eastnorthasbng takes scalar e/n) happens
# in functions.py. Both call this.
east_north_as_bng = point_as_cell


# ---------------------------------------------------------------------------
# cell -> geometry (BNG.cellIdToGeometry / aswkb / aswkt / centroid)
#
# Heavy emits PLAIN WKB (no SRID) via JTS.toWKB and plain WKT via JTS.toWKT.
# BNG does NOT stamp an SRID (unlike quadbin, which is EWKB SRID 4326), so the
# cell polygon is built with shapely ``box`` and serialized WITHOUT include_srid.
# ---------------------------------------------------------------------------


def cell_id_to_geometry(cell_id: int):
    """Closed BNG cell polygon (shapely), NO SRID (BNG.cellIdToGeometry).

    The ring is (x,y),(x+e,y),(x+e,y+e),(x,y+e),(x,y) from getX/getY/getEdgeSize
    in EPSG:27700 eastings/northings.
    """
    digits = cell_digits(cell_id)
    resolution = get_resolution_from_digits(digits)
    edge = get_edge_size(resolution)
    x = get_x(digits, edge)
    y = get_y(digits, edge)
    return _box(x, y, x + edge, y + edge)


def cell_aswkb(cell_id: int) -> bytes:
    """Cell polygon as plain WKB (no SRID; heavy uses toWKB, not toEWKB)."""
    return _to_wkb(cell_id_to_geometry(cell_id))  # include_srid defaults False


def cell_aswkt(cell_id: int) -> str:
    """Cell polygon as WKT text (BNG.asWKT)."""
    return cell_id_to_geometry(cell_id).wkt


def cell_centroid(cell_id: int) -> bytes:
    """Cell centre as plain WKB POINT (no SRID)."""
    return _to_wkb(cell_id_to_geometry(cell_id).centroid)


def k_loop(cell_id: int, k: int) -> list:
    """Hollow square ring of Long cell ids at radius k (BNG.kLoop).

    Walks the four corners plus the interior edge runs of the square at
    distance ``k * edgeSize`` around the cell centre, mapping each point back
    to a cell id. Mirrors the Scala ``until ... by edgeSize`` (exclusive upper
    bound) edge runs via Python ``range`` (also exclusive).
    """
    digits = cell_digits(cell_id)
    resolution = get_resolution_from_digits(digits)
    edge = get_edge_size(resolution)
    x = get_x(digits, edge)
    y = get_y(digits, edge)
    xmin, xmax = x - k * edge, x + k * edge
    ymin, ymax = y - k * edge, y + k * edge
    pts = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
    pts += [(xmin, yy) for yy in range(ymin + edge, ymax, edge)]  # left
    pts += [(xmax, yy) for yy in range(ymin + edge, ymax, edge)]  # right
    pts += [(xx, ymax) for xx in range(xmin + edge, xmax, edge)]  # up
    pts += [(xx, ymin) for xx in range(xmin + edge, xmax, edge)]  # down
    return [point_to_cell_id(px, py, resolution) for (px, py) in pts]


def k_ring(cell_id: int, n: int) -> list:
    """Center + all k-loops 1..n of Long cell ids (BNG.kRing)."""
    if n == 1:
        return [cell_id] + k_loop(cell_id, 1)
    out = [cell_id]
    for k in range(1, n + 1):
        out += k_loop(cell_id, k)
    return out


def k_ring_str(cell_id_str: str, n: int) -> list:
    """String-id wrapper over :func:`k_ring` (parse -> walk -> format)."""
    return [format(c) for c in k_ring(parse(cell_id_str), int(n))]


def k_loop_str(cell_id_str: str, k: int) -> list:
    """String-id wrapper over :func:`k_loop` (parse -> walk -> format)."""
    return [format(c) for c in k_loop(parse(cell_id_str), int(k))]


# ---------------------------------------------------------------------------
# coverage (BNG.polyfill)
#
# Centroid-membership BFS flood-fill. Mirrors BNG.polyfill (gridx/grid/BNG.scala
# line 207): seed the queue with the cell ids of EVERY geometry coordinate plus
# the centroid's coordinate(s), then BFS over kLoop(.,1) neighbours, INCLUDING a
# cell iff its own centroid is ``contains``-ed by the input geometry. Neighbours
# are enqueued only from cells that are themselves contained, exactly as heavy --
# a contained cell seeds the expansion of its 8 neighbours; an excluded cell is a
# dead end. The membership test is centroid-in-geometry (NOT intersects), which
# matches H3/quadbin and is what gives exact cell-set parity with the heavy tier.
# ---------------------------------------------------------------------------


def _seed_coords(geometry) -> list:
    """All vertex coords across any geometry type (mirrors getCoordinates).

    Walks the GeoJSON coordinate tree so Point/Line/Polygon/Multi*/Geometry
    collections all yield their flat list of (x, y) vertices, matching the
    Scala ``geometry.getCoordinates`` used to seed the BFS.
    """
    pts: list = []

    def _walk(obj):
        if isinstance(obj, (list, tuple)):
            if obj and isinstance(obj[0], (int, float)):
                pts.append((obj[0], obj[1]))
            else:
                for o in obj:
                    _walk(o)

    if geometry.geom_type == "GeometryCollection":
        for part in geometry.geoms:
            _walk(_mapping(part).get("coordinates", []))
    else:
        _walk(_mapping(geometry).get("coordinates", []))
    return pts


def polyfill(geometry, resolution: int) -> list:
    """Long cell ids whose centroid is contained by the geometry (BNG.polyfill).

    Centroid-membership BFS flood-fill seeded from every vertex coordinate plus
    the geometry centroid. Returns Long cell ids (the ``*_str`` wrapper formats).
    """
    if geometry is None or geometry.is_empty:
        return []

    centroid = geometry.centroid
    seeds = _seed_coords(geometry) + [(centroid.x, centroid.y)]
    queue = [point_to_cell_id(px, py, resolution) for (px, py) in seeds]

    visited: set = set()
    out: list = []
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        # Skip out-of-GB / malformed seed cells (e.g. a raster reprojected to
        # 27700 whose coords fall outside the BNG envelope): they have no valid
        # geometry to test or walk, so the fill simply yields nothing for them.
        if not is_valid(current):
            continue
        center = cell_id_to_geometry(current).centroid
        if geometry.contains(_Point(center.x, center.y)):
            out.append(current)
            for nb in k_loop(current, 1):
                if nb not in visited:
                    queue.append(nb)
    return out


def polyfill_str(geom, resolution) -> list:
    """String-id wrapper over :func:`polyfill` (parse geom -> walk -> format).

    ``geom`` is any WKB/EWKB/WKT/EWKT input (via ``parse_geom``); ``resolution``
    is a BNG Int index or resolutionMap string key.
    """
    res = get_resolution(resolution)
    parsed = parse_geom(geom)
    return [format(c) for c in polyfill(parsed, res)]


# ---------------------------------------------------------------------------
# tessellation (BNG.tessellate)
#
# Splits the input geometry into per-cell chips, each a (Long cellid, core,
# chip) tuple. Mirrors BNG.tessellate (gridx/grid/BNG.scala line 754):
#   * radius = edgeSize * sqrt(2) / 2  (half the cell diagonal -> no blind spots)
#   * carved = geometry.buffer(-radius); polyfill(carved) -> CORE cells (fully
#     inside, chip == None unless keep_core_geom).
#   * border = polyfill(borderGeometry) minus core, where borderGeometry is a
#     1%-grown, simplified buffer of either the geometry (if carved is empty) or
#     its boundary; each border cell's chip is cell_geom.intersection(geometry).
#     A border chip is PROMOTED to core only when it matches the whole cell via
#     vertex-order-sensitive equals_exact(cell_geom, 0.1) -- mirroring heavy JTS
#     equalsExact, which (like shapely) rejects a renormalized grid-aligned ring,
#     so grid-aligned full cells STAY border.
#   * mosaic#423 fix: the final result keeps only chips whose geometry type
#     matches the INPUT geometry type (drops degenerate POINT/LINESTRING chips
#     produced by grid-aligned edges/vertices touching a Polygon input).
# ---------------------------------------------------------------------------


# Tolerance (metres) for promoting a near-full border chip to a core cell --
# matches heavy BNG.tessellate's equalsExact(cellGeom, 0.1) (BNG.scala ~L803).
_FULL_CELL_TOL = 0.1


def get_buffer_radius(resolution: int) -> float:
    """Optimal polyfill buffer radius (BNG.getBufferRadius): edgeSize*sqrt(2)/2.

    Half the cell diagonal -- buffering by this avoids polyfill blind spots near
    the geometry boundary.
    """
    return get_edge_size(resolution) * math.sqrt(2) / 2.0


def tessellate(geometry, resolution: int, keep_core_geom: bool = False) -> list:
    """Split ``geometry`` into per-cell chips (BNG.tessellate).

    Returns a list of ``(Long cellid, core: bool, chip: shapely|None)``. ``core``
    cells are fully inside the geometry (chip ``None`` unless ``keep_core_geom``);
    border cells carry the clipped intersection. The ``*_str`` wrapper formats the
    id and emits plain WKB (no SRID).
    """
    if geometry is None or geometry.is_empty:
        return []

    in_type = geometry.geom_type
    radius = get_buffer_radius(resolution)
    carved = geometry.buffer(-radius)

    # 1% grow ensures the union of carved + border has no holes inside the
    # original area; simplify by 1% of the radius (matches JTS.simplify).
    if carved.is_empty:
        border_geom = geometry.buffer(radius * 1.01).simplify(0.01 * radius)
    else:
        border_geom = geometry.boundary.buffer(radius * 1.01).simplify(0.01 * radius)

    core_set = set(polyfill(carved, resolution))
    border = [c for c in polyfill(border_geom, resolution) if c not in core_set]

    chips = []
    for cell in core_set:
        core_geom = cell_id_to_geometry(cell) if keep_core_geom else None
        chips.append((cell, True, core_geom))
    for cell in border:
        cell_geom = cell_id_to_geometry(cell)
        intersect = cell_geom.intersection(geometry)
        if intersect.is_empty:
            continue  # heavy: (cell, false, intersect).filterNot(_._3.isEmpty)
        if intersect.geom_type == "GeometryCollection":
            adjusted = intersect.difference(cell_geom.boundary)
        else:
            adjusted = intersect
        # Heavy promotes a border cell to core iff the clipped chip is the whole
        # cell per JTS ``adjusted.equalsExact(cellGeom, 0.1)`` (BNG.scala ~L803).
        # JTS ``equalsExact`` is vertex-ORDER sensitive: when the polygon-vs-cell
        # intersection renormalizes the clipped ring's start vertex / winding
        # (which it does for grid-aligned cells whose edges sit ON the cell
        # boundary), equalsExact returns False and heavy KEEPS the cell as border.
        # shapely's ``equals_exact`` shares that vertex-order sensitivity, so it is
        # the faithful mirror. An earlier order-INSENSITIVE near-containment check
        # over-promoted those grid-aligned full cells to core, shrinking the border
        # set and so under-expanding the geometry k-ring/k-loop (heavy expands the
        # kept-border cells) -- a light-vs-heavy parity break on grid-aligned
        # polygons. Match heavy exactly. (BNG coords are integer metres.)
        is_core = _equals_exact(adjusted, cell_geom, _FULL_CELL_TOL)
        if is_core:
            chips.append((cell, True, cell_geom if keep_core_geom else None))
        else:
            chips.append((cell, False, adjusted))

    # mosaic#423: drop degenerate (non-areal) chips by keeping only chips whose
    # geometry type matches the input geometry type. Heavy (line 813):
    #   .filter(t => t._3 == null || t._3.getGeometryType == inGeomType)
    # A POINT/LINESTRING intersection of a grid-aligned vertex/edge against a
    # Polygon input is dropped, leaving only areal (Polygon/MultiPolygon) chips.
    return [
        (cell, core, chip)
        for (cell, core, chip) in chips
        if chip is None or chip.geom_type == in_type
    ]


def tessellate_str(geom, resolution, keep_core_geom: bool = False) -> list:
    """String-id wrapper over :func:`tessellate`.

    ``geom`` is any WKB/EWKB/WKT/EWKT input (via ``parse_geom``); ``resolution``
    is a BNG Int index or resolutionMap string key. Returns
    ``(cellid: str, core: bool, chip: WKB bytes | None)`` -- chip WKB has NO SRID
    (heavy ``JTS.toWKB``).
    """
    res = get_resolution(resolution)
    parsed = parse_geom(geom)
    return [
        (format(c), core, (_to_wkb(chip) if chip is not None else None))
        for (c, core, chip) in tessellate(parsed, res, keep_core_geom)
    ]


# ---------------------------------------------------------------------------
# geometry-centric neighborhood (BNG.geometryKRing / geometryKLoop)
#
# Cover the geometry with chips (getChips), then expand the BORDER chips by a
# cell k_ring/k_loop. Core cells (fully inside) are kept as-is for k-ring; for
# k-loop they form part of the inner n-ring that is subtracted to hollow out the
# result. All emitted ids are filtered by ``is_valid`` (in-bounds + indexable),
# mirroring the heavy ``.filter(BNG.isValid)`` on both walks (BNG.scala L635/645).
# ---------------------------------------------------------------------------


def is_valid(cell_id: int) -> bool:
    """Whether a Long cell id is in-bounds + indexable (BNG.isValid, BNG.scala L305).

    EPSG:27700 origin coords must satisfy 0<=x<=700000, 0<=y<=1300000, and the
    letter-grid indices must fall within the LETTER_MAP dimensions.
    """
    digits = cell_digits(cell_id)
    if len(digits) < 4:
        # Malformed id (e.g. extreme-negative-easting res=-1 encode output):
        # not a valid BNG cell.
        return False
    x_letter = int("".join(str(d) for d in digits[3:5]))
    y_letter = int("".join(str(d) for d in digits[1:3]))
    resolution = get_resolution_from_digits(digits)
    edge = get_edge_size(resolution)
    x = get_x(digits, edge)
    y = get_y(digits, edge)
    return (
        0 <= x <= 700000
        and 0 <= y <= 1300000
        and x_letter < len(LETTER_MAP)
        and y_letter < len(LETTER_MAP[0])
    )


def is_valid_vec(cell_ids: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized :func:`is_valid` for a single-resolution batch of Long ids.

    Equivalent to ``[is_valid(int(c)) for c in cell_ids]`` when every id was
    produced at ``resolution``. Extracts digit slices by integer arithmetic
    (fixed digit layout per resolution), reproducing ``get_x``/``get_y`` and the
    bounds/index checks on arrays.

    The digit layout (most-significant first) at a given resolution:

    - ``digits[0]``        : leading ``1`` placeholder
    - ``digits[1:3]``      : easting 100km letter index (``y_letter`` in :func:`is_valid`)
    - ``digits[3:5]``      : northing 100km letter index (``x_letter`` in :func:`is_valid`)
    - ``digits[5:5+k]``    : easting sub-cell bins
    - ``digits[5+k:5+2k]`` : northing sub-cell bins
    - ``digits[-1]``       : quadrant (0 for positive resolutions)

    where ``k = (ndigits - 6) // 2`` and ``ndigits = 4 + 2 * n_positions``.
    """
    cell_ids = np.asarray(cell_ids, dtype=np.int64)
    if cell_ids.size == 0:
        return np.zeros(0, dtype=bool)

    # res -1 (500km) is a DEGENERATE 4-digit id (encode divides by 100) and its
    # get_x/get_y math has k=-1 -- the generic array formula below does NOT apply.
    # 500km cells never arise at raster-tile scale, so keep parity via the scalar
    # path for this one resolution (still correct; not the hot case).
    if resolution == -1:
        return np.array(
            [is_valid(int(c)) for c in cell_ids], dtype=bool
        )  # vectorscan: ok (res -1 degenerate, non-hot)

    # For all other resolutions the digit count is fixed by the encode layout:
    #   ndigits = 4 + 2*n_positions  (leading placeholder '1' + letter + coord fields).
    n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
    ndigits = 4 + 2 * n_positions

    def digit(pos):
        """Extract decimal digit at ``pos`` (0=MSB) from every cell id."""
        p = ndigits - 1 - pos  # power of 10 for that position
        return (cell_ids // (10**p)) % 10

    k = (ndigits - 6) // 2
    edge = get_edge_size(resolution)

    # Easting 100km letter index: digits[1:3] (called y_letter in is_valid).
    east_100km = digit(1) * 10 + digit(2)
    # Northing 100km letter index: digits[3:5] (called x_letter in is_valid).
    north_100km = digit(3) * 10 + digit(4)

    quadrant = digit(ndigits - 1)
    edge_adj = np.where(quadrant > 0, 2 * edge, edge)

    # Easting value (reproduces get_x): east_100km base + digits[5:5+k].
    east_base = east_100km
    for i in range(k):
        east_base = east_base * 10 + digit(5 + i)
    x_offset = np.where((quadrant == 3) | (quadrant == 4), edge, 0)
    easting = east_base * edge_adj + x_offset

    # Northing value (reproduces get_y): north_100km base + digits[5+k:5+2k].
    north_base = north_100km
    for i in range(k):
        north_base = north_base * 10 + digit(5 + k + i)
    y_offset = np.where((quadrant == 2) | (quadrant == 3), edge, 0)
    northing = north_base * edge_adj + y_offset

    # Reproduces is_valid bounds check:
    #   0 <= x (=easting=get_x) <= 700000
    #   0 <= y (=northing=get_y) <= 1300000
    #   x_letter (north_100km) < len(LETTER_MAP) = 14
    #   y_letter (east_100km)  < len(LETTER_MAP[0]) = 8
    return (
        (easting >= 0)
        & (easting <= 700000)
        & (northing >= 0)
        & (northing <= 1300000)
        & (north_100km < len(LETTER_MAP))
        & (east_100km < len(LETTER_MAP[0]))
    )


def get_chips(geometry, resolution: int, keep_core_geom: bool = False) -> list:
    """Per-cell chips covering ``geometry`` (BNG.getChips, BNG.scala L648).

    Dispatched on geometry type:
      * Point          -> single border chip at the point's cell (core False).
      * MultiPoint     -> one border chip per point.
      * Line/MultiLine -> ``_line_fill`` (BFS walk along each line).
      * else (Polygon/MultiPolygon/...) -> ``tessellate``.

    Returns ``(Long cellid, core: bool, chip: shapely|None)`` tuples. For Point
    inputs the chip is the point only when ``keep_core_geom`` (matches heavy
    ``pointChip``); line chips always carry the clipped line segment.
    """
    gt = geometry.geom_type
    if gt == "Point":
        return _point_chip(geometry, resolution, keep_core_geom)
    if gt == "MultiPoint":
        return _multi_point_chips(geometry, resolution, keep_core_geom)
    if gt in ("LineString", "MultiLineString"):
        return _line_fill(geometry, resolution)
    return list(tessellate(geometry, resolution, keep_core_geom))


def _point_chip(geometry, resolution: int, keep_core_geom: bool) -> list:
    """Single non-core chip at the point's cell (BNG.pointChip, BNG.scala L662)."""
    chip = geometry if keep_core_geom else None
    return [(point_to_cell_id(geometry.x, geometry.y, resolution), False, chip)]


def _multi_point_chips(geometry, resolution: int, keep_core_geom: bool) -> list:
    """One ``_point_chip`` per sub-point (BNG.multiPointChips, BNG.scala L673)."""
    out: list = []
    for p in geometry.geoms:
        out += _point_chip(p, resolution, keep_core_geom)
    return out


def _line_fill(geometry, resolution: int) -> list:
    """Decompose each line into per-cell chips (BNG.lineFill, BNG.scala L682)."""
    lines = [geometry] if geometry.geom_type == "LineString" else list(geometry.geoms)
    out: list = []
    for line in lines:
        out += _line_decompose(line, resolution)
    return out


def _line_decompose(line, resolution: int) -> list:
    """BFS walk along a LineString yielding ``(cellid, False, segment)`` chips.

    Faithful port of ``BNG.lineDecompose`` (BNG.scala L698): start at the line's
    start-point cell; for each queued cell, intersect the line with the cell
    polygon and -- if non-empty -- emit the segment chip and enqueue the cell's
    unseen ``k_ring(.,1)`` neighbours. The ``traversed.size == 1`` special case
    re-seeds the walk when the very first cell's intersection is empty (the start
    point lies exactly on a cell boundary).
    """
    start = line.coords[0]
    start_cell = point_to_cell_id(start[0], start[1], resolution)
    queue = [start_cell]
    traversed: set = set()
    chips: list = []
    while queue:
        traversed |= set(queue)
        next_queue: list = []
        for current in queue:
            cell_geom = cell_id_to_geometry(current)
            seg = line.intersection(cell_geom)
            if not seg.is_empty:
                chips.append((current, False, seg))
                for nb in k_ring(current, 1):
                    if nb not in traversed and nb not in next_queue:
                        next_queue.append(nb)
            elif len(traversed) == 1:
                for nb in k_ring(current, 1):
                    if nb not in traversed and nb not in next_queue:
                        next_queue.append(nb)
        queue = next_queue
    return chips


def geometry_k_ring(geometry, resolution: int, k: int) -> set:
    """k-ring of cell ids covering ``geometry`` (BNG.geometryKRing, BNG.scala L639).

    Core (interior) cells are kept; border cells are each expanded by
    ``k_ring(.,k)``. The union is filtered by ``is_valid``.
    """
    chips = get_chips(geometry, resolution, keep_core_geom=False)
    core_ids = {c for (c, core, _) in chips if core}
    border = [c for (c, core, _) in chips if not core]
    border_kring = {x for c in border for x in k_ring(c, k)}
    return {c for c in (core_ids | border_kring) if is_valid(c)}


def geometry_k_loop(geometry, resolution: int, k: int) -> set:
    """Hollow k-loop of cell ids around ``geometry`` (BNG.geometryKLoop, L619).

    Subtracts the inner n-ring (n = k-1: core ids + border ``k_ring(.,n)``) from
    the border ``k_loop(.,k)``, then filters by ``is_valid``.
    """
    n = k - 1
    chips = get_chips(geometry, resolution, keep_core_geom=False)
    core_ids = {c for (c, core, _) in chips if core}
    border = [c for (c, core, _) in chips if not core]
    border_nring = {x for c in border for x in k_ring(c, n)}
    n_ring = core_ids | border_nring
    border_kloop = {x for c in border for x in k_loop(c, k)}
    return {c for c in (border_kloop - n_ring) if is_valid(c)}


def geometry_k_ring_str(geom, resolution, k) -> list:
    """String-id wrapper over :func:`geometry_k_ring` (parse geom -> walk -> format)."""
    res = get_resolution(resolution)
    return [format(c) for c in geometry_k_ring(parse_geom(geom), res, int(k))]


def geometry_k_loop_str(geom, resolution, k) -> list:
    """String-id wrapper over :func:`geometry_k_loop` (parse geom -> walk -> format)."""
    res = get_resolution(resolution)
    return [format(c) for c in geometry_k_loop(parse_geom(geom), res, int(k))]


# ---------------------------------------------------------------------------
# chip-pair ops (BNG_CellUnion / BNG_CellIntersection expressions)
#
# Operate on chip structs ``(cellid, core, chip)`` -- the tessellate/getChips
# output -- NOT on cell-id arrays like quadbin. Faithful port of the heavy
# eval order in BNG_CellUnion.evalLong/evalString and
# BNG_CellIntersection.evalLong/evalString (BNG_CellUnion.scala L37-63 /
# BNG_CellIntersection.scala L37-63), which is IDENTICAL for union and
# intersection:
#   1. ``if (chip1.core) return chip1``  -- LEFT core early-exit, FIRST
#   2. ``if (chip2.core) return chip2``  -- RIGHT core early-exit, SECOND
#   3. else execute*: different cell ids -> (left cellid, left core, empty)
#                     else -> left.union(right) / left.intersection(right)
# The core-flag check happens BEFORE the cellid-equality check, so a CORE chip
# with a mismatched cellid returns that core chip (NOT an empty polygon). The
# geometric-op result reuses the LEFT chip's core flag (chip1._2). Chip geometry
# is plain (no SRID); the *_str wrappers emit plain WKB.
# ---------------------------------------------------------------------------


def _empty_polygon():
    """Empty Polygon, mirroring heavy ``JTS.emptyPolygon`` (no SRID)."""
    from shapely.geometry import Polygon as _Polygon

    return _Polygon()


def cell_union(left, right):
    """Union two chips ``(cellid, core, shapely)`` (BNG_CellUnion eval order).

    Core-flag early-exit FIRST (left core -> left; right core -> right), then the
    cellid check (different ids -> empty polygon), then the geometric union. A
    core chip with a mismatched cellid returns the core chip, matching heavy.
    """
    lc, lcore, lg = left
    rc, rcore, rg = right
    if lcore:
        return left
    if rcore:
        return right
    if lc != rc:
        return (lc, lcore, _empty_polygon())
    return (lc, lcore, lg.union(rg))


def cell_intersection(left, right):
    """Intersect two chips ``(cellid, core, shapely)`` (BNG_CellIntersection).

    IDENTICAL eval order to :func:`cell_union` (heavy union/intersection share it):
    core-flag early-exit FIRST (left core -> left; right core -> right), then the
    cellid check (different ids -> empty polygon), then the geometric intersection.
    """
    lc, lcore, lg = left
    rc, rcore, rg = right
    if lcore:
        return left
    if rcore:
        return right
    if lc != rc:
        return (lc, lcore, _empty_polygon())
    return (lc, lcore, lg.intersection(rg))
