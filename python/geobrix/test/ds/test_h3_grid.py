"""Unit tests for ds/_h3_grid.py — pure h3 cell-grid computation."""

import h3
import pytest


def test_h3_cells_cover_bbox():
    from databricks.labs.gbx.ds._h3_grid import h3_cells_for_bounds

    # A ~0.1° box near London at resolution 5 (cell edge ~89 km; 1–4 cells expected).
    cells = h3_cells_for_bounds((-0.15, 51.45, -0.05, 51.55), 5)
    assert len(cells) >= 1

    # Every cell has a distinct cellid string and a valid 4326 bbox.
    ids = {c.cellid for c in cells}
    assert len(ids) == len(cells)
    for c in cells:
        assert isinstance(c.cellid, str)
        assert c.west < c.east
        assert c.south < c.north
        # bbox must be plausible lat/lon ranges
        assert -180.0 <= c.west <= 180.0
        assert -90.0 <= c.south <= 90.0


def test_h3_cells_cellid_round_trips_h3_lib():
    from databricks.labs.gbx.ds._h3_grid import h3_cells_for_bounds

    cells = h3_cells_for_bounds((-74.01, 40.70, -73.99, 40.72), 7)
    assert len(cells) >= 1
    for c in cells:
        # Confirm get_resolution does not raise and returns 7.
        assert h3.get_resolution(c.cellid) == 7
        # Round-trip: str_to_int → int_to_str must reproduce the same cellid.
        assert h3.int_to_str(h3.str_to_int(c.cellid)) == c.cellid


def test_h3_cells_bbox_covers_source():
    from databricks.labs.gbx.ds._h3_grid import h3_cells_for_bounds

    minlon, minlat, maxlon, maxlat = (-0.12, 51.48, -0.08, 51.52)
    cells = h3_cells_for_bounds((minlon, minlat, maxlon, maxlat), 6)
    assert cells, "expected at least one cell"
    # Union of cell bboxes in 4326 must contain the source bbox.
    assert min(c.west for c in cells) <= minlon
    assert min(c.south for c in cells) <= minlat
    assert max(c.east for c in cells) >= maxlon
    assert max(c.north for c in cells) >= maxlat


def test_h3_cells_resolution_out_of_range():
    from databricks.labs.gbx.ds._h3_grid import h3_cells_for_bounds

    with pytest.raises(ValueError, match="resolution"):
        h3_cells_for_bounds((-74.0, 40.7, -73.9, 40.8), 16)  # h3 max = 15
