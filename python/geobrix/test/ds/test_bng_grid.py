from databricks.labs.gbx.ds._bng_grid import BngCell, bng_cells_for_bounds
from databricks.labs.gbx.pygx import _bng


def test_bng_cells_cover_bbox_including_edges():
    # 1 km resolution (index 3), a bbox spanning ~2x2 cells near London (EPSG:27700)
    res = _bng.get_resolution("1km")
    bounds = (530000.0, 180000.0, 531500.0, 181500.0)  # 1.5 km box → 2x2 = 4 cells
    cells = bng_cells_for_bounds(bounds, res)
    assert len(cells) == 4
    # every returned cell's extent actually overlaps the bbox
    for c in cells:
        assert c.east > bounds[0] and c.west < bounds[2]
        assert c.north > bounds[1] and c.south < bounds[3]
    # cellids are unique + valid BNG strings that round-trip through pygx._bng
    ids = {c.cellid for c in cells}
    assert len(ids) == 4
    for c in cells:
        assert _bng.format(_bng.parse(c.cellid)) == c.cellid


def test_bng_source_smaller_than_one_cell_yields_its_covering_cell():
    res = _bng.get_resolution("1km")
    bounds = (530100.0, 180100.0, 530200.0, 180200.0)  # 100 m box inside one 1 km cell
    cells = bng_cells_for_bounds(bounds, res)
    assert len(cells) == 1  # NOT empty (centroid membership would risk 0)


def test_bng_cellid_bit_identical_to_point_as_cell():
    res = _bng.get_resolution("1km")
    bounds = (530000.0, 180000.0, 530500.0, 180500.0)  # one cell
    (cell,) = bng_cells_for_bounds(bounds, res)
    # the cell's own centroid classified by pygx._bng must equal the mosaic cellid
    cx = (cell.west + cell.east) / 2
    cy = (cell.south + cell.north) / 2
    assert cell.cellid == _bng.point_as_cell(cx, cy, res)


def test_bng_in_gb_london_fixture_no_valid_cells_dropped():
    """London 20 km × 20 km synthetic source: is_valid filter is a no-op for in-GB sources.

    Bounds from _write_gb_source: UL=(E=530000, N=180000), 200×200 px @ 100 m/px →
    extent west=530000, south=160000, east=550000, north=180000.  The loop is
    edge-inclusive so 21 steps × 21 steps = 441 cells (all valid in-GB cells).
    The is_valid filter must not drop any of them.
    """
    res = _bng.get_resolution("1km")
    bounds = (530000.0, 160000.0, 550000.0, 180000.0)  # 20 km × 20 km near London
    cells = bng_cells_for_bounds(bounds, res)
    assert len(cells) == 441, (
        f"London 20×20 km fixture at 1 km resolution must yield 441 cells "
        f"(21×21 edge-inclusive); got {len(cells)}"
    )
    # All emitted cells must pass is_valid — filter is a no-op for in-GB sources
    for c in cells:
        cid_int = _bng.parse(c.cellid)
        assert _bng.is_valid(cid_int), f"emitted cell {c.cellid!r} failed is_valid"


def test_bng_out_of_gb_bbox_emits_only_valid_cells():
    """A bbox outside the GB envelope emits only is_valid cells (no invalid ids leak through).

    Note: point_to_cell_id snaps extreme coords to boundary cells (e.g. negative
    eastings snap to the x=0 boundary which IS valid).  The invariant is that every
    emitted cell passes is_valid — not that an out-of-GB bbox always produces an
    empty list.
    """
    res = _bng.get_resolution("1km")
    # Negative eastings — these snap to the BNG x=0 boundary (still valid)
    bounds = (-200000.0, 160000.0, -50000.0, 180000.0)
    cells = bng_cells_for_bounds(bounds, res)
    for c in cells:
        cid_int = _bng.parse(c.cellid)
        assert _bng.is_valid(cid_int), (
            f"emitted cell {c.cellid!r} from out-of-GB bbox failed is_valid"
        )


def test_bng_partial_out_of_gb_bbox_emits_only_valid_cells():
    """A bbox straddling the western edge of the GB envelope emits only is_valid cells."""
    res = _bng.get_resolution("100km")  # coarse resolution so we cross the boundary clearly
    # This bbox straddles x=0 (the western edge of the BNG envelope).
    # Any cell whose encoding produces invalid letter indices is filtered.
    bounds = (-150000.0, 0.0, 200000.0, 200000.0)
    cells = bng_cells_for_bounds(bounds, res)
    for c in cells:
        cid_int = _bng.parse(c.cellid)
        assert _bng.is_valid(cid_int), (
            f"emitted cell {c.cellid!r} from partial-out-of-GB bbox failed is_valid"
        )
