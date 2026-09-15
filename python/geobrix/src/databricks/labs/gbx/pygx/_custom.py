"""Pure-Python custom-grid core for the pygx light tier.

A faithful, BIT-EXACT port of the heavy
``com.databricks.labs.gbx.gridx.grid.CustomGridSystem`` + ``GridConf`` Scala
objects (gridx/grid/CustomGridSystem.scala, GridConf.scala). No PyPI library
exists; this module reproduces the cell-ID bit-packing, coordinate<->cell
mapping, polyfill (centroid-containment), and k-ring (Chebyshev clamp) EXACTLY
so light and heavy share bit-identical cell ids and cell sets.

A custom grid is a user-defined regular rectangular grid: extent, root cell
size, and a recursive ``cell_splits`` factor (each resolution level subdivides
into ``cell_splits x cell_splits`` sub-cells). Cell ids are BIGINT (the top 8
bits hold the resolution, the low 56 hold the row-major cell position).

Geometry is emitted as plain WKB (NO SRID) / WKT, matching heavy ``JTS.toWKB``
(line 159, the 2D no-SRID variant). The grid ``srid`` is metadata only and is
NOT stamped into output geometry.

Resolved decision 3 (spec 2026-06-14): heavy ``pointToCellID`` had a
``require(!x.isNaN && !x.isNaN, ...)`` typo that left a NaN Y unguarded;
``point_to_cell_id`` here (and the heavy fix) guards BOTH x and y.
"""

import math
from dataclasses import dataclass
from typing import Any, List

import shapely  # noqa: F401  (geometry surfaces in later tasks; load-time dep guard)
from shapely import to_wkb as _to_wkb
from shapely.geometry import Point as _Point
from shapely.geometry import box as _box

from . import _dilate
from ._geom import parse_geom

ID_BITS = 56  # GridConf.idBits — low 56 bits hold the cell position
RES_BITS = 8  # GridConf.resBits — top 8 bits hold the resolution
_POSITION_MASK = 0x00FFFFFFFFFFFFFF


def _as_int(v: Any) -> int:
    if isinstance(v, bool):  # bool is an int subclass; reject explicitly
        raise ValueError(f"gbx_custom: expected INT/LONG, got bool {v!r}")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    raise ValueError(f"gbx_custom: expected INT/LONG, got {v!r}")


@dataclass(frozen=True)
class CustomGridConf:
    bound_x_min: int
    bound_x_max: int
    bound_y_min: int
    bound_y_max: int
    cell_splits: int
    root_cell_size_x: int
    root_cell_size_y: int
    srid: int = -1  # -1 == no CRS

    @property
    def sub_cells_count(self) -> int:
        return self.cell_splits * self.cell_splits

    @property
    def bits_per_resolution(self) -> int:
        # GridConf.scala:25 — ceil(log10(subCellsCount) / log10(2))
        return math.ceil(math.log10(self.sub_cells_count) / math.log10(2))

    @property
    def max_resolution(self) -> int:
        # GridConf.scala:28 — min(20, floor(56 / bitsPerResolution))
        return min(20, math.floor(ID_BITS / self.bits_per_resolution))

    @property
    def root_cell_count_x(self) -> int:
        # GridConf.scala:30 — ceil(spanX / rootCellSizeX)
        span = self.bound_x_max - self.bound_x_min
        return math.ceil(span / self.root_cell_size_x)

    @property
    def root_cell_count_y(self) -> int:
        # GridConf.scala:31 — ceil(spanY / rootCellSizeY)
        span = self.bound_y_max - self.bound_y_min
        return math.ceil(span / self.root_cell_size_y)


def conf_from_row(row: Any) -> CustomGridConf:
    """Reconstruct a CustomGridConf from a grid-spec struct (Row/dict).

    Mirrors Custom_GridSpec.systemFromRow; Int/Long tolerant (PySpark sends Long
    for INT literals).
    """
    if row is None:
        raise ValueError("gbx_custom: grid spec must not be null")
    g = row.asDict() if hasattr(row, "asDict") else dict(row)
    return CustomGridConf(
        bound_x_min=_as_int(g["bound_x_min"]),
        bound_x_max=_as_int(g["bound_x_max"]),
        bound_y_min=_as_int(g["bound_y_min"]),
        bound_y_max=_as_int(g["bound_y_max"]),
        cell_splits=_as_int(g["cell_splits"]),
        root_cell_size_x=_as_int(g["root_cell_size_x"]),
        root_cell_size_y=_as_int(g["root_cell_size_y"]),
        srid=_as_int(g["srid"]),
    )


# --- cell-ID codec + grid math (CustomGridSystem) -----------------------------


def total_cells_x(conf: CustomGridConf, resolution: int) -> int:
    # CustomGridSystem.scala:274 — rootCellCountX * pow(cellSplits, res).toLong
    return conf.root_cell_count_x * int(math.pow(conf.cell_splits, resolution))


def total_cells_y(conf: CustomGridConf, resolution: int) -> int:
    # CustomGridSystem.scala:278 — rootCellCountY * pow(cellSplits, res).toLong
    return conf.root_cell_count_y * int(math.pow(conf.cell_splits, resolution))


def cell_width(conf: CustomGridConf, resolution: int) -> float:
    # CustomGridSystem.scala:196 — rootCellSizeX / pow(cellSplits, res)
    return conf.root_cell_size_x / math.pow(conf.cell_splits, resolution)


def cell_height(conf: CustomGridConf, resolution: int) -> float:
    # CustomGridSystem.scala:200 — rootCellSizeY / pow(cellSplits, res)
    return conf.root_cell_size_y / math.pow(conf.cell_splits, resolution)


def get_cell_id(cell_position: int, resolution: int) -> int:
    # CustomGridSystem.scala:310 — cellPosition | (resolution.toLong << idBits)
    return cell_position | (resolution << ID_BITS)


def get_cell_resolution(cell_id: int) -> int:
    # CustomGridSystem.scala:180 — (cellId >> idBits).toInt
    return cell_id >> ID_BITS


def get_cell_position(cell_id: int) -> int:
    # CustomGridSystem.scala:184 — cellId & 0x00ffffffffffffffL
    return cell_id & _POSITION_MASK


def get_cell_position_x(conf: CustomGridConf, id_number: int, resolution: int) -> int:
    # CustomGridSystem.scala:188 — idNumber % totalCellsX(res)
    return id_number % total_cells_x(conf, resolution)


def get_cell_position_y(conf: CustomGridConf, id_number: int, resolution: int) -> int:
    # CustomGridSystem.scala:192 — floor(idNumber / totalCellsX(res)).toLong
    return int(math.floor(id_number / total_cells_x(conf, resolution)))


def get_cell_position_from_positions(
    conf: CustomGridConf, cell_pos_x: int, cell_pos_y: int, resolution: int
) -> int:
    # CustomGridSystem.scala:317 — cellPosY * totalCellsX(res) + cellPosX
    return cell_pos_y * total_cells_x(conf, resolution) + cell_pos_x


def _trunc_long(v: float) -> int:
    # Scala Double->Long truncates toward zero (NOT math.floor).
    return int(v)


def get_cell_position_from_coordinates(
    conf: CustomGridConf, x: float, y: float, resolution: int
):
    # CustomGridSystem.scala:268-272
    cell_pos_x = _trunc_long((x - conf.bound_x_min) / cell_width(conf, resolution))
    cell_pos_y = _trunc_long((y - conf.bound_y_min) / cell_height(conf, resolution))
    return (
        cell_pos_x,
        cell_pos_y,
        get_cell_position_from_positions(conf, cell_pos_x, cell_pos_y, resolution),
    )


def get_cell_center_x(
    conf: CustomGridConf, cell_position_x: int, resolution: int
) -> float:
    # CustomGridSystem.scala:296-301
    w = cell_width(conf, resolution)
    return cell_position_x * w + (w / 2) + conf.bound_x_min


def get_cell_center_y(
    conf: CustomGridConf, cell_position_y: int, resolution: int
) -> float:
    # CustomGridSystem.scala:303-308
    h = cell_height(conf, resolution)
    return cell_position_y * h + (h / 2) + conf.bound_y_min


def point_to_cell_id(conf: CustomGridConf, x: float, y: float, resolution: int) -> int:
    """Cell ID containing (x, y) at `resolution` (CustomGridSystem.pointToCellID).

    Port of CustomGridSystem.scala:249-266; the four guards fire in the same
    order as the heavy ``require``s: NaN, max-resolution, x-bounds, y-bounds.
    Heavy uses the geometry's FIRST coordinate (getCoordinate), not the centroid.

    Resolved decision 3: guard BOTH x and y for NaN. The heavy Scala
    (CustomGridSystem.scala:250) has a ``require(!x.isNaN && !x.isNaN, ...)``
    typo — the second clause repeats ``x``, leaving a NaN Y unguarded. This
    port (and the heavy fix, CG-T8) guards both; cellPosX/Y truncate toward
    zero (Scala ``Double.toLong``), via ``_trunc_long`` (not ``math.floor``).
    """
    if math.isnan(x) or math.isnan(y):
        raise ValueError("gbx_custom: NaN coordinates are not supported.")
    if resolution > conf.max_resolution:
        raise ValueError(
            f"gbx_custom: resolution ({resolution}) exceeds maximum "
            f"resolution of {conf.max_resolution}."
        )
    if not (conf.bound_x_min <= x < conf.bound_x_max):
        raise ValueError(
            f"gbx_custom: X coordinate ({x}) out of bounds "
            f"{conf.bound_x_min}-{conf.bound_x_max}"
        )
    if not (conf.bound_y_min <= y < conf.bound_y_max):
        raise ValueError(
            f"gbx_custom: Y coordinate ({y}) out of bounds "
            f"{conf.bound_y_min}-{conf.bound_y_max}"
        )
    _, _, cell_pos = get_cell_position_from_coordinates(conf, x, y, resolution)
    return get_cell_id(cell_pos, resolution)


def point_to_cell_id_or_none(conf: CustomGridConf, x: float, y: float, resolution: int):
    """Resolution PARAMETER still raises ValueError; NaN / out-of-bounds coordinate DATA
    returns None so one bad row degrades to NULL."""
    if resolution > conf.max_resolution:  # PARAMETER -> raise
        raise ValueError(
            f"gbx_custom: resolution ({resolution}) exceeds maximum "
            f"resolution of {conf.max_resolution}."
        )
    try:
        return point_to_cell_id(conf, x, y, resolution)  # DATA (NaN/bounds) -> None
    except ValueError:
        return None


# --- cell -> geometry (CustomGridSystem.cellIdToGeometry / cellIdToCenter) ----


def cell_id_to_polygon(conf: CustomGridConf, cell_id: int):
    """Closed custom-grid cell polygon (shapely), NO SRID.

    Port of CustomGridSystem.scala:213-235 — the closed ring
    ``(x,y),(x+w,y),(x+w,y+h),(x,y+h),(x,y)``. shapely ``box`` produces the same
    axis-aligned rectangle.
    """
    resolution = get_cell_resolution(cell_id)
    cell_number = get_cell_position(cell_id)
    cell_x = get_cell_position_x(conf, cell_number, resolution)
    cell_y = get_cell_position_y(conf, cell_number, resolution)
    w = cell_width(conf, resolution)
    h = cell_height(conf, resolution)
    x = cell_x * w + conf.bound_x_min
    y = cell_y * h + conf.bound_y_min
    return _box(x, y, x + w, y + h)


def cell_id_to_centroid(conf: CustomGridConf, cell_id: int):
    # CustomGridSystem.scala:332-338 — polygon centroid.
    return cell_id_to_polygon(conf, cell_id).centroid


def cell_aswkb(conf: CustomGridConf, cell_id: int) -> bytes:
    # Heavy JTS.toWKB — plain 2D WKB, NO SRID (include_srid defaults False).
    return _to_wkb(cell_id_to_polygon(conf, cell_id))


def cell_aswkt(conf: CustomGridConf, cell_id: int) -> str:
    return cell_id_to_polygon(conf, cell_id).wkt


def cell_centroid(conf: CustomGridConf, cell_id: int) -> bytes:
    return _to_wkb(cell_id_to_centroid(conf, cell_id))


# --- polyfill (CustomGridSystem.polyfill) -------------------------------------


def polyfill(conf: CustomGridConf, geometry, resolution: int) -> List[int]:
    """Cell IDs whose CENTER is contained by the geometry (CustomGridSystem.polyfill).

    Port of CustomGridSystem.scala:145-178. Mirrors heavy EXACTLY incl. the
    intentional ``first..last + 1`` bbox over-scan (Resolved decision 3: the
    over-scan is by design, not a bug — the centroid-containment filter discards
    the extra ring of cells). The cell envelope corners (minX,minY)/(maxX,maxY)
    map to the first/last cell positions; the over-scan iterates one extra cell
    past ``last`` in each axis (Scala ``a to b`` is INCLUSIVE → Python
    ``range(first, last + 2)``). Each candidate cell's CENTER is tested with
    ``geometry.contains`` (not bbox overlap), so a cell whose bbox overlaps the
    geometry but whose center lies outside is excluded.

    Heavy uses JTS ``geometry.isEmpty``; here we also treat a null geometry as
    empty (the UDF may receive a null column value). Both return ``[]``.
    """
    if geometry is None or geometry.is_empty:
        return []
    min_x, min_y, max_x, max_y = geometry.bounds
    first_x, first_y, _ = get_cell_position_from_coordinates(
        conf, min_x, min_y, resolution
    )
    last_x, last_y, _ = get_cell_position_from_coordinates(
        conf, max_x, max_y, resolution
    )
    out: List[int] = []
    # Scala `first to last + 1` is INCLUSIVE -> Python range(first, (last + 1) + 1)
    for x in range(first_x, last_x + 2):
        for y in range(first_y, last_y + 2):
            cx = get_cell_center_x(conf, x, resolution)
            cy = get_cell_center_y(conf, y, resolution)
            if geometry.contains(_Point(cx, cy)):
                # The over-scan (and ceil-rounded grid extent) can produce an
                # edge cell whose CENTER lies just past bound_x/y_max — not a
                # real grid cell. Use the _or_none form so such a center is
                # skipped rather than raising a data-context ValueError that
                # would surface as a task failure (a geometry straddling the
                # upper grid boundary). Bad resolution still raises (param).
                cid = point_to_cell_id_or_none(conf, cx, cy, resolution)
                if cid is not None:
                    out.append(cid)
    return out


def covering_candidate_cells(
    conf: CustomGridConf, geometry, resolution: int
) -> List[int]:
    """Candidate cell IDs for COVERING tessellation of a raster bbox / pixel rect.

    Port of ``CustomGridSystem.coveringCandidateCells`` (CustomGridSystem.scala:197-200).
    Custom ``polyfill`` is CENTROID-containment, so a geometry SMALLER than a cell
    — the normal raster→grid regime (a sub-cell pixel, or a small raster bbox) —
    contains NO cell centre and ``polyfill`` alone returns ZERO cells, silently
    dropping that pixel's mass.  Buffering the geometry by one cell dimension
    (``max(cell_width, cell_height)``) before the centroid-containment scan
    guarantees the containing cell — whose centre is within half a cell of any
    interior point — is enumerated.  The caller's positive-area / intersection-area
    keep-test discards the extra ring the buffer adds, so over-scanning is safe;
    UNDER-scanning was the bug (light custom covering dropped sub-cell pixels,
    unlike heavy, which buffers here).

    The buffer ring legitimately reaches past the grid extent for a geometry near
    the grid edge, so the buffered geometry is CLIPPED to the grid extent before
    ``polyfill`` — no candidate centre then falls outside the grid bounds (which
    would make ``polyfill``'s ``point_to_cell_id`` raise, exactly as heavy
    ``pointToCellID`` does on an out-of-bounds centre).  This is the exact analog
    of the BNG covering path (buffer, then drop out-of-GB cells): the clip removes
    only candidates the covering keep-test would drop anyway, so the covered cell
    set is unchanged for the interior rasters both tiers actually process, while a
    raster reaching the grid edge degrades gracefully instead of raising.
    """
    if geometry is None or geometry.is_empty:
        return []
    buf = max(cell_width(conf, resolution), cell_height(conf, resolution))
    grid_extent = _box(
        conf.bound_x_min, conf.bound_y_min, conf.bound_x_max, conf.bound_y_max
    )
    clipped = geometry.buffer(buf).intersection(grid_extent)
    if clipped.is_empty:
        return []
    return polyfill(conf, clipped, resolution)


# --- k_ring (CustomGridSystem.kRing) ------------------------------------------


def k_ring(conf: CustomGridConf, cell_id: int, k: int) -> List[int]:
    """Chebyshev (square) k-ring of cell IDs around ``cell_id``.

    Port of CustomGridSystem.scala:38-60. The neighborhood is the Chebyshev
    (square / Moore) block of cells within ``k`` steps of the center cell in
    both axes, INCLUDING the center cell itself (heavy iterates the full
    ``[posX-k, posX+k] x [posY-k, posY+k]`` square with no center exclusion).

    The block is CLAMPED to the grid bounds rather than wrapped: the lower
    bound is ``max(pos-k, 0)`` so positions never go negative, and the upper
    bound is ``min(pos+k, totalCellsX/Y(res))`` so off-edge cells are dropped.
    A center cell at a grid corner therefore yields fewer cells than an interior
    cell. The upper clamp uses ``totalCells`` ITSELF (not ``totalCells - 1``) and
    iterates INCLUSIVELY (Scala ``a to b``) — ported verbatim so the cell set is
    bit-identical to heavy; the parity test locks heavy's set.
    """
    if k < 0:
        raise ValueError("gbx_custom: k must be at least 0")
    res = get_cell_resolution(cell_id)
    cell_position = get_cell_position(cell_id)
    pos_x = get_cell_position_x(conf, cell_position, res)
    pos_y = get_cell_position_y(conf, cell_position, res)

    from_x = max(pos_x - k, 0)
    to_x = min(pos_x + k, total_cells_x(conf, res))
    from_y = max(pos_y - k, 0)
    to_y = min(pos_y + k, total_cells_y(conf, res))

    out: List[int] = []
    for x in range(from_x, to_x + 1):  # Scala `a to b` is INCLUSIVE
        for y in range(from_y, to_y + 1):
            pos = get_cell_position_from_positions(conf, x, y, res)
            out.append(get_cell_id(pos, res))
    return out


def k_loop(conf: CustomGridConf, cell_id: int, k: int) -> List[int]:
    """Hollow ring of custom-grid cells at EXACTLY Chebyshev distance k.

    k=0 returns [cell_id] (center only).  k<0 raises ValueError.  For k>=1,
    computed as sorted(k_ring(k) - k_ring(k-1)), mirroring heavy
    CustomGridSystem.kLoop semantics exactly (set-difference of clamped rings).
    """
    if k < 0:
        raise ValueError(f"gbx_custom: k_loop k must be >= 0; got {k}")
    if k == 0:
        return [int(cell_id)]
    return sorted(set(k_ring(conf, cell_id, k)) - set(k_ring(conf, cell_id, k - 1)))


def distance(conf: CustomGridConf, cell_a: int, cell_b: int) -> int:
    """Chebyshev grid-ring distance between two custom-grid cells.

    Distance = max(|dx|, |dy|) in cell-position units, consistent with
    CustomGridSystem.distance (heavy).  This is the minimum k such that
    cell_b appears in k_ring(cell_a, k).

    Both cells are decoded to their (x, y) grid positions at their
    respective resolutions; positions are in separate resolution spaces
    when resolutions differ (caller's responsibility to pass same-resolution
    cells for meaningful results, matching heavy behavior).
    """
    res_a = get_cell_resolution(cell_a)
    pos_a = get_cell_position(cell_a)
    ax = get_cell_position_x(conf, pos_a, res_a)
    ay = get_cell_position_y(conf, pos_a, res_a)

    res_b = get_cell_resolution(cell_b)
    pos_b = get_cell_position(cell_b)
    bx = get_cell_position_x(conf, pos_b, res_b)
    by = get_cell_position_y(conf, pos_b, res_b)

    return max(abs(ax - bx), abs(ay - by))


# --- geometry-aware kring/kloop (shared dilation engine) ----------------------


def _cell_geom(conf: CustomGridConf, cell_id: int):
    """Shapely polygon for a custom-grid cell (reuses cell_id_to_polygon)."""
    return cell_id_to_polygon(conf, cell_id)


def classify(conf: CustomGridConf, geom, resolution: int):
    """Classify polyfill candidates vs P (geom), S (solid), H (holes).

    geom must already be a Shapely geometry; call parse_geom first if raw
    bytes/str.  Closures capture `conf` so callers need not thread it into
    the engine internals.

    For non-polygon geometries (points, lines), the custom grid's centroid-
    containment polyfill returns nothing.  The ``point_to_cell_fn`` hook is
    passed so that the engine can fall back to coordinate sampling and per-grid
    point-to-cell lookup, enabling dimension-aware coverage classification for
    all geometry types.
    """
    res = int(resolution)
    return _dilate.classify(
        geom,
        res,
        polyfill_fn=lambda g, r: polyfill(conf, g, r),
        cell_geom_fn=lambda c: _cell_geom(conf, c),
        point_to_cell_fn=lambda x, y: point_to_cell_id_or_none(conf, x, y, res),
    )


def geometry_k_ring(
    conf: CustomGridConf,
    geom,
    resolution: int,
    k: int,
    mode: str = _dilate.DEFAULT_MODE,
    coverage: str = _dilate.DEFAULT_COVERAGE,
) -> List[int]:
    """Geometry-aware k-ring for a custom grid.

    Mirrors ``_quadbin.geometry_k_ring`` with the leading ``conf`` argument
    that all custom-grid functions require.  ``coverage`` ∈
    {"coveras","polyfill","core"} selects the belongs-to basis.

    geom: WKB bytes, WKT string, or Shapely geometry.
    Returns a sorted list of int (BIGINT) cell ids.
    """
    parsed = parse_geom(geom)
    if parsed is None or parsed.is_empty:
        return []
    cls = classify(conf, parsed, int(resolution))
    return sorted(
        _dilate.geom_expand(
            "ring", int(k), mode, cls, lambda c: k_loop(conf, c, 1), coverage
        )
    )


def geometry_k_loop(
    conf: CustomGridConf,
    geom,
    resolution: int,
    k: int,
    mode: str = _dilate.DEFAULT_MODE,
    coverage: str = _dilate.DEFAULT_COVERAGE,
) -> List[int]:
    """Geometry-aware k-loop (hollow ring) for a custom grid.

    ``coverage`` ∈ {"coveras","polyfill","core"} selects the belongs-to basis.
    Returns a sorted list of int (BIGINT) cell ids.
    """
    parsed = parse_geom(geom)
    if parsed is None or parsed.is_empty:
        return []
    cls = classify(conf, parsed, int(resolution))
    return sorted(
        _dilate.geom_expand(
            "loop", int(k), mode, cls, lambda c: k_loop(conf, c, 1), coverage
        )
    )
