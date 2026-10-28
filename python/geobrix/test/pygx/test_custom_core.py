import pytest

shapely = pytest.importorskip("shapely")  # _custom imports shapely at module load
from shapely import from_wkb, from_wkt, get_srid  # noqa: E402

from databricks.labs.gbx.pygx import _custom  # noqa: E402  (after importorskip guard)


def _conf(splits=2, rootx=1000, rooty=1000, srid=-1):
    # A 0..1,000,000 grid (mirrors the doc SQL example grid).
    return _custom.CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=splits,
        root_cell_size_x=rootx,
        root_cell_size_y=rooty,
        srid=srid,
    )


def test_gridconf_derived_quantities_match_scala():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # subCellsCount = 4 -> ceil(log10(4)/log10(2)) = ceil(2.0) = 2
    assert c.bits_per_resolution == 2
    # min(20, floor(56/2)) = 20  (the 20 cap binds here)
    assert c.max_resolution == 20
    # ceil(1_000_000 / 1000) = 1000
    assert c.root_cell_count_x == 1000
    assert c.root_cell_count_y == 1000


def test_max_resolution_is_cell_splits_dependent():
    # cell_splits=4 -> subCells=16 -> ceil(log10(16)/log10(2)) = ceil(4.0) = 4
    # -> min(20, floor(56/4)) = 14
    c = _conf(splits=4)
    assert c.bits_per_resolution == 4
    assert c.max_resolution == 14
    # cell_splits=8 -> subCells=64 -> bitsPerRes = ceil(log10(64)/log10(2)) = 6
    # -> min(20, floor(56/6)=9) = 9
    c8 = _conf(splits=8)
    assert c8.bits_per_resolution == 6
    assert c8.max_resolution == 9


def test_cell_id_pack_unpack_roundtrip():
    c = _conf()
    for res in (0, 1, 5, 10):
        for px, py in [(0, 0), (3, 7), (123, 456)]:
            pos = _custom.get_cell_position_from_positions(c, px, py, res)
            cid = _custom.get_cell_id(pos, res)
            assert _custom.get_cell_resolution(cid) == res
            decoded = _custom.get_cell_position(cid)
            assert _custom.get_cell_position_x(c, decoded, res) == px
            assert _custom.get_cell_position_y(c, decoded, res) == py


def test_total_cells_and_cell_width():
    c = _conf(splits=2, rootx=1000)
    assert _custom.total_cells_x(c, 0) == 1000  # rootCellCountX * 2^0
    assert _custom.total_cells_x(c, 1) == 2000  # * 2^1
    assert _custom.cell_width(c, 0) == 1000.0
    assert _custom.cell_width(c, 1) == 500.0  # 1000 / 2^1 (FLOAT division)


def test_conf_from_row_int_long_tolerant():
    # Simulate the struct arriving as a dict (PySpark Row.asDict) with Long bounds.
    row = {
        "bound_x_min": 0,
        "bound_x_max": 1_000_000,
        "bound_y_min": 0,
        "bound_y_max": 1_000_000,
        "cell_splits": 2,
        "root_cell_size_x": 1000,
        "root_cell_size_y": 1000,
        "srid": 27700,
    }
    c = _custom.conf_from_row(row)
    assert c.srid == 27700 and c.cell_splits == 2 and c.bound_x_max == 1_000_000


def test_point_to_cell_id_known_fixture():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # res 0: 1000m root cells. Point (530000, 180000) -> posX=530, posY=180.
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    assert _custom.get_cell_resolution(cid) == 0
    pos = _custom.get_cell_position(cid)
    assert _custom.get_cell_position_x(c, pos, 0) == 530
    assert _custom.get_cell_position_y(c, pos, 0) == 180


def test_point_to_cell_id_rejects_nan_x_and_y():
    c = _conf()
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, float("nan"), 180000.0, 0)
    # A NaN Y must ALSO raise (heavy typo left Y unguarded).
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, 530000.0, float("nan"), 0)


def test_point_to_cell_id_rejects_out_of_bounds_and_over_max_res():
    c = _conf(splits=2)  # max_resolution == 20
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, -1.0, 180000.0, 0)  # x < bound_x_min
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(
            c, 1_000_000.0, 180000.0, 0
        )  # x == bound_x_max (exclusive)
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, 530000.0, 180000.0, 21)  # res > max_resolution


def test_cell_aswkb_is_polygon_no_srid():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)  # res 0 -> 1000m cell
    g = from_wkb(_custom.cell_aswkb(c, cid))
    assert g.geom_type == "Polygon"
    assert get_srid(g) == 0  # custom WKB carries NO SRID
    assert g.bounds == (530000.0, 180000.0, 531000.0, 181000.0)


def test_cell_aswkt_is_polygon_text():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    g = from_wkt(_custom.cell_aswkt(c, cid))
    assert g.geom_type == "Polygon"


def test_cell_centroid_is_point_no_srid():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    g = from_wkb(_custom.cell_centroid(c, cid))
    assert g.geom_type == "Point" and get_srid(g) == 0
    assert (g.x, g.y) == (530500.0, 180500.0)  # cell center


from shapely.geometry import box as _box2  # noqa: E402


def test_polyfill_small_box_res0():
    c = _conf(splits=2, rootx=1000, rooty=1000)  # 1000m root cells
    # A 3000m x 3000m box aligned to the grid -> 9 cell centers fall inside.
    geom = _box2(530000.0, 180000.0, 533000.0, 183000.0)
    cells = _custom.polyfill(c, geom, 0)
    assert len(cells) == 9
    assert all(_custom.get_cell_resolution(cid) == 0 for cid in cells)


def test_polyfill_empty_geom_is_empty():
    c = _conf()
    assert _custom.polyfill(c, None, 0) == []
    from shapely.geometry import Polygon

    assert _custom.polyfill(c, Polygon(), 0) == []


def test_polyfill_centroid_containment_only():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # A box smaller than one cell, off-center, contains NO cell center -> empty.
    geom = _box2(530100.0, 180100.0, 530400.0, 180400.0)
    assert _custom.polyfill(c, geom, 0) == []


# --- k_ring (CustomGridSystem.kRing, Chebyshev square clamped to totalCells) --


def test_kring_interior_k1_is_nine_cells():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)  # interior cell
    ring = _custom.k_ring(c, cid, 1)
    # Chebyshev (square) neighborhood includes the center cell.
    assert cid in ring
    assert len(set(ring)) == 9  # 3x3 block, far from edges


def test_kring_k0_is_self_only():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    assert _custom.k_ring(c, cid, 0) == [cid]


def test_kring_clamps_at_origin_corner():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 100.0, 100.0, 0)  # posX=posY=0 (origin corner)
    ring = _custom.k_ring(c, cid, 1)
    # fromX/fromY clamp to 0; no cell extends to a negative position.
    assert cid in ring
    assert all(_custom.get_cell_position(rc) >= 0 for rc in ring)
    # Corner: the [-1,1]^2 square is clamped to x in [0,1], y in [0,1] -> 2x2 = 4
    # cells (fewer than the interior 9). The upper bound uses totalCells itself,
    # but posX/posY=0 with k=1 only reaches x,y in {0,1}, well within bounds.
    assert len(set(ring)) == 4


def test_kring_k2_interior_is_twenty_five_cells():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)  # interior cell
    ring = _custom.k_ring(c, cid, 2)
    assert cid in ring
    assert len(set(ring)) == 25  # 5x5 block, far from edges


def test_kring_rejects_negative_k():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    with pytest.raises(ValueError):
        _custom.k_ring(c, cid, -1)
