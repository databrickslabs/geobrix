"""Tests for quadbin cell-grid computation (ds/_quadbin_grid.py).

Pure math tests: no Spark, no GDAL dataset, no I/O.
"""


def test_quadbin_cells_cover_bbox():
    from databricks.labs.gbx.ds._quadbin_grid import quadbin_cells_for_bounds

    # a ~0.1° box near NYC at resolution 12
    cells = quadbin_cells_for_bounds((-74.02, 40.70, -73.92, 40.80), 12)
    assert len(cells) >= 1
    # every cell has a distinct quadbin id and a valid 3857 extent (west<east, south<north)
    ids = {c.cellid for c in cells}
    assert len(ids) == len(cells)
    for c in cells:
        assert c.west < c.east and c.south < c.north
    # the union of cell extents (in 3857) covers the bbox's 3857 projection
    from rasterio.warp import transform_bounds

    bx = transform_bounds("EPSG:4326", "EPSG:3857", -74.02, 40.70, -73.92, 40.80)
    assert min(c.west for c in cells) <= bx[0] and max(c.east for c in cells) >= bx[2]


def test_quadbin_cell_ids_roundtrip_quadbin_lib():
    import quadbin

    from databricks.labs.gbx.ds._quadbin_grid import quadbin_cells_for_bounds

    cells = quadbin_cells_for_bounds((-74.0, 40.7, -73.99, 40.71), 14)
    # each cellid is a real quadbin cell (quadbin.cell_to_tile does not raise)
    for c in cells:
        assert quadbin.cell_to_tile(c.cellid) is not None
