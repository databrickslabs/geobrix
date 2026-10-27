"""Spark-free unit tests for BNG tessellation (Task 6).

Tests the core/border split and the mosaic#423 input-dimension chip filter
ported from ``BNG.tessellate`` (gridx/grid/BNG.scala). Chips are plain
WKB (NO SRID), matching heavy ``JTS.toWKB``.
"""

import pytest

shapely = pytest.importorskip("shapely")

from shapely import from_wkb, from_wkt, get_srid, set_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402

# Synthetic, valid polygons with no holes crossing Easting 400000. Their eastern
# clips contain two polygons + a boundary line, or a polygon + a boundary point.
_BOUNDARY_CLIPS = [
    (
        "POLYGON ((399998 500001, 400002 500001, 400002 500002, 400000 500002, "
        "400000 500003, 400002 500003, 400002 500004, 399998 500004, 399998 500001))",
        "MultiPolygon",
    ),
    (
        "POLYGON ((399998 500001, 400002 500001, 400002 500002, 399999 500002, "
        "400000 500003, 399999 500004, 399998 500004, 399998 500001))",
        "Polygon",
    ),
]


@pytest.mark.parametrize("wkt, eastern_type", _BOUNDARY_CLIPS)
@pytest.mark.parametrize("keep_core_geom", [False, True])
def test_tessellate_mixed_boundary_clip_preserves_cells_and_area(
    wkt, eastern_type, keep_core_geom
):
    polygon = from_wkt(wkt)
    assert polygon.is_valid and not polygon.interiors
    east = _bng.point_to_cell_id(400001, 500001, 2)
    west = _bng.point_to_cell_id(399999, 500001, 2)
    clipped = _bng.cell_id_to_geometry(east).intersection(polygon)
    assert clipped.geom_type == "GeometryCollection"

    chips = _bng.tessellate(polygon, 2, keep_core_geom)
    assert {cell for cell, _, _ in chips} == {east, west}
    assert all(chip.geom_type in ("Polygon", "MultiPolygon") for _, _, chip in chips)
    assert sum(chip.area for _, _, chip in chips) == pytest.approx(polygon.area)
    assert (
        next(chip for cell, _, chip in chips if cell == east).geom_type == eastern_type
    )

    # Public wrapper must retain the same cells and geometry after WKB encoding.
    encoded = _bng.tessellate_str(to_wkb(polygon), 2, keep_core_geom)
    assert {cell for cell, _, _ in encoded} == {_bng.format(east), _bng.format(west)}
    assert sum(from_wkb(chip).area for _, _, chip in encoded) == pytest.approx(
        polygon.area
    )


def test_tessellate_multipolygon_retains_polygon_chips():
    polygon = from_wkt(_BOUNDARY_CLIPS[1][0])
    multi = shapely.geometry.MultiPolygon([polygon])
    chips = _bng.tessellate(multi, 2)
    assert len(chips) == 2
    assert all(chip.geom_type == "Polygon" for _, _, chip in chips)
    assert sum(chip.area for _, _, chip in chips) == pytest.approx(polygon.area)


def test_clean_chip_geometry_handles_nested_collections_and_preserves_srid():
    collection = set_srid(
        from_wkt(
            "GEOMETRYCOLLECTION (POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1)), "
            "GEOMETRYCOLLECTION (LINESTRING (0 1, 0 2), POINT (0 3), POINT EMPTY))"
        ),
        27700,
    )
    result = _bng._clean_chip_geometry(collection, box(0, 0, 4, 4).boundary)
    assert result.geom_type == "Polygon"
    assert result.equals(box(1, 1, 2, 2))
    assert get_srid(result) == 27700


def test_clean_chip_geometry_preserves_non_boundary_points_and_lines():
    collection = from_wkt("GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 1, 2 2))")
    result = _bng._clean_chip_geometry(collection, box(0, 0, 4, 4).boundary)
    assert result.normalize().equals_exact(collection.normalize(), 0)


def test_clean_chip_geometry_discards_empty_results_and_leaves_ordinary_clips_alone():
    boundary = box(0, 0, 4, 4).boundary
    for wkt in [
        "GEOMETRYCOLLECTION EMPTY",
        "GEOMETRYCOLLECTION (POINT (0 1), LINESTRING (0 2, 0 3))",
    ]:
        assert _bng._clean_chip_geometry(from_wkt(wkt), boundary).is_empty
    polygon = box(0, 0, 2, 2)
    assert _bng._clean_chip_geometry(polygon, boundary) is polygon


def test_tessellate_box_has_core_and_border():
    # Box whose top/right edges cut cells in half (4.5km span on the 1km grid)
    # -> interior cells (whose carved-polyfill centroid is inside the shrunk
    # geometry) become core, the half/quarter edge cells stay clipped border
    # chips. Grid-aligned border cells are NOT promoted (heavy JTS equalsExact is
    # vertex-order sensitive), so a mix of core and border requires interior
    # cells away from the boundary -- which this 4.5km box provides.
    geom = to_wkb(box(530000.0, 180000.0, 534500.0, 184500.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    assert len(chips) > 0
    cores = [c for c in chips if c[1]]
    borders = [c for c in chips if not c[1]]
    assert cores and borders
    # core chip geom is None (keep_core_geom False default for the array form);
    # border chips carry a WKB polygon.
    for cell, core, chip in cores:
        assert chip is None
    for cell, core, chip in borders:
        assert chip is not None
        g = from_wkb(chip)
        assert g.geom_type in ("Polygon", "MultiPolygon")


def test_tessellate_chip_wkb_has_no_srid():
    # Heavy uses JTS.toWKB (no SRID), unlike quadbin's EWKB SRID 4326.
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    for cell, core, chip in chips:
        if chip is not None:
            assert get_srid(from_wkb(chip)) == 0


def test_tessellate_grid_aligned_no_degenerate_chips():
    # mosaic#423: a polygon aligned exactly to the 1km grid must NOT emit
    # POINT/LINESTRING chips at shared edges. Box edges land on 1km lines.
    geom = to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    assert len(chips) > 0
    for cell, core, chip in chips:
        if chip is not None:
            g = from_wkb(chip)
            assert g.geom_type in (
                "Polygon",
                "MultiPolygon",
            ), f"degenerate chip {g.geom_type} for {cell} (mosaic#423)"


def test_tessellate_grid_aligned_full_cell_stays_border():
    # A grid-aligned box that fully covers one 1km cell. Because the box width
    # equals the cell edge, carved = buffer(-radius) is EMPTY, so the cell goes
    # through the BORDER path. Heavy's promotion test is JTS
    # ``adjusted.equalsExact(cellGeom, 0.1)`` (BNG.borderChips), which is
    # vertex-ORDER sensitive: the polygon-vs-cell intersection renormalizes the
    # clipped ring's start vertex, so equalsExact returns False and heavy KEEPS
    # the cell as core=False with a full-cell chip. (Verified against the heavy
    # JAR: getChips of this box -> (TQ3080, core=False, full-cell chip).) Light
    # mirrors heavy with shapely ``equals_exact`` -- it must NOT promote this
    # grid-aligned cell, or geomkring/geomkloop diverge (the bench regression).
    res = _bng.get_resolution("1km")
    cell_box = box(530000.0, 180000.0, 531000.0, 181000.0)
    assert cell_box.buffer(-_bng.get_buffer_radius(res)).is_empty  # border path
    chips = _bng.tessellate_str(to_wkb(cell_box), res)
    # No core promotion: heavy keeps the grid-aligned full cell as a border chip.
    cores = [c for c in chips if c[1]]
    assert (
        not cores
    ), "grid-aligned full cell must stay border (matches heavy JTS equalsExact)"
    # The single surviving chip is the whole cell as a Polygon (areal, not None).
    assert len(chips) == 1, f"expected one border chip, got {chips}"
    cell, core, chip = chips[0]
    assert core is False and chip is not None
    g = from_wkb(chip)
    assert g.geom_type == "Polygon"
    assert g.normalize().equals_exact(
        box(530000.0, 180000.0, 531000.0, 181000.0).normalize(), 1e-6
    )


def test_tessellate_empty_geom_is_empty():
    assert _bng.tessellate_str(None, 3) == []


def test_tessellate_returns_string_cellids():
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    for cell, core, chip in chips:
        assert isinstance(cell, str)
        assert isinstance(core, bool)
        assert cell.startswith("TQ")


def test_tessellate_core_set_subset_of_polyfill():
    # Every cell tessellate emits must also be a polyfill cell of the geometry
    # (tessellate = polyfill(carved) core + polyfill(border) clipped chips).
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    res = _bng.get_resolution("1km")
    fill = set(_bng.polyfill_str(geom, res))
    cores = {c for (c, core, chip) in _bng.tessellate_str(geom, res) if core}
    assert cores <= fill


def test_bng_tessellate_udf_keep_core_geom_toggles_interior_chip():
    from databricks.labs.gbx.pygx.functions import _bng_tessellate

    # 4.5km box on the 1km grid -> has interior (core) cells and border cells.
    geom = to_wkb(box(530000.0, 180000.0, 534500.0, 184500.0))
    res = _bng.get_resolution("1km")

    keep = _bng_tessellate(geom, res, keep_core_geom=True)
    interior_keep = [c for c in keep if c["core"]]
    assert interior_keep, "expected at least one interior (core) cell"
    assert any(
        c["chip"] is not None for c in interior_keep
    ), "keep_core_geom=True must populate interior-cell chip WKB"

    drop = _bng_tessellate(geom, res, keep_core_geom=False)
    interior_drop = [c for c in drop if c["core"]]
    assert all(
        c["chip"] is None for c in interior_drop
    ), "keep_core_geom=False must leave interior-cell chip null"

    # default is False in every API (heavy SQL 2-arg, heavy wrappers, tessellateexplode)
    default = _bng_tessellate(geom, res)
    assert all(
        c["chip"] is None for c in default if c["core"]
    ), "light bng_tessellate default must be keep_core_geom=False (heavy parity)"
    # a NULL keep_core_geom behaves as the default
    assert _bng_tessellate(geom, res, None) == default
