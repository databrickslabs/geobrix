import math

import pytest

from transformations._shard import shard_bounds, tile_shard


def test_tile_shard_descendant_shifts_to_ancestor():
    # z=13, shard_zoom=6 -> delta 7; 1600>>7 == 12, 3000>>7 == 23.
    assert tile_shard(13, 1600, 3000, 6) == "6/12/23"


def test_tile_shard_at_shard_zoom_is_identity():
    assert tile_shard(6, 33, 24, 6) == "6/33/24"


def test_tile_shard_shallow_delta():
    # z=8, shard_zoom=6 -> delta 2; 100>>2 == 25, 200>>2 == 50.
    assert tile_shard(8, 100, 200, 6) == "6/25/50"


def test_tile_shard_coarser_than_shard_is_own_key():
    assert tile_shard(4, 3, 5, 6) == "4/3/5"


def test_shard_bounds_top_left_shard():
    minlon, minlat, maxlon, maxlat = shard_bounds("6/0/0")
    assert minlon == pytest.approx(-180.0)
    assert maxlon == pytest.approx(-174.375)          # 1/64*360 - 180
    assert maxlat == pytest.approx(85.0511, abs=1e-3)  # Web-Mercator north edge
    assert minlat == pytest.approx(84.5414, abs=1e-3)  # one row south at z6
    assert minlat < maxlat and minlon < maxlon


def test_shard_bounds_prime_meridian_equator():
    # At z6 there are 64 columns; x=32 starts at lon 0. y=32 straddles the equator.
    minlon, minlat, maxlon, maxlat = shard_bounds("6/32/32")
    assert minlon == pytest.approx(0.0)
    assert maxlon == pytest.approx(5.625)   # 33/64*360 - 180
    assert maxlat == pytest.approx(0.0, abs=1e-6)   # equator is the tile's north edge
    assert minlat < 0.0


def test_shard_bounds_full_lon_span():
    # A whole z6 row spans the full -180..180 longitude range.
    west0, _, _, _ = shard_bounds("6/0/0")
    _, _, east63, _ = shard_bounds("6/63/0")
    assert west0 == pytest.approx(-180.0)
    assert east63 == pytest.approx(180.0)
