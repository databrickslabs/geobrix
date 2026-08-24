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
