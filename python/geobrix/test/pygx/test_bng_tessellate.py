"""Spark-free unit tests for BNG tessellation (Task 6).

Tests the core/border split and the mosaic#423 input-geometry-type chip filter
ported from ``BNG.tessellate`` (gridx/grid/BNG.scala line 754). Chips are plain
WKB (NO SRID), matching heavy ``JTS.toWKB``.
"""

import pytest

shapely = pytest.importorskip("shapely")

from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


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
    # ``adjusted.equalsExact(cellGeom, 0.1)`` (BNG.scala ~L803), which is
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

    # default matches heavy (True): interior chips populated
    default = _bng_tessellate(geom, res)
    assert any(
        c["chip"] is not None for c in default if c["core"]
    ), "light bng_tessellate default must be keep_core_geom=True (heavy parity)"
