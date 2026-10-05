import pytest
from databricks.labs.gbx.pyrx import tile_range  # adjust import to chosen export


def test_tile_range_centered():
    # origin 0, tile 1000 m; coord at 5000 (tile 5 start), radius 1500 → window [3500, 6500]
    assert tile_range(5000.0, 0.0, 1500.0, 1000.0) == (3, 6)


def test_tile_range_single_tile_small_radius():
    # coord mid-tile-5 (5500), radius 100 → window [5400,5600] within tile 5
    assert tile_range(5500.0, 0.0, 100.0, 1000.0) == (5, 5)


def test_tile_range_negative_origin():
    # mercator-style origin; coord-radius lands in tile -1
    assert tile_range(50.0, 0.0, 100.0, 1000.0) == (-1, 0)
