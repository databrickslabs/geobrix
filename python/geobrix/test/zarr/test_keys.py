import pytest

from databricks.labs.gbx.zarr._keys import (
    KeyEncoding,
    chunks_per_shard,
    encoding_for,
    inner_coords,
    inner_index,
    shard_of,
)
from databricks.labs.gbx.zarr.errors import ZarrUnsupported


def test_v3_default_keys_round_trip():
    enc = encoding_for(
        3,
        {
            "chunk_key_encoding": {
                "name": "default",
                "configuration": {"separator": "/"},
            }
        },
    )
    assert enc == KeyEncoding(3, "/", "c")
    assert enc.chunk_key((0, 12, 3)) == "c/0/12/3"
    assert enc.parse("c/0/12/3", 3) == (0, 12, 3)
    assert enc.chunk_key(()) == "c" and enc.parse("c", 0) == ()


def test_v2_dot_and_slash_keys():
    dot = encoding_for(2, {})
    assert dot.chunk_key((1, 2)) == "1.2" and dot.parse("1.2", 2) == (1, 2)
    slash = encoding_for(2, {"dimension_separator": "/"})
    assert slash.chunk_key((1, 2)) == "1/2"
    assert dot.parse("0", 0) == () and dot.parse("0", 1) == (0,)


def test_v3_v2_encoding_name():
    enc = encoding_for(3, {"chunk_key_encoding": {"name": "v2"}})
    assert enc.chunk_key((4, 5)) == "4.5"


def test_shard_math_c_order():
    cps = chunks_per_shard((24, 74, 144), (12, 37, 72))
    assert cps == (2, 2, 2)
    assert shard_of((3, 1, 2), cps) == (1, 0, 1)
    assert inner_index((3, 1, 2), cps) == 1 * 4 + 1 * 2 + 0
    assert inner_coords((1, 0, 1), 6, cps) == (3, 1, 2)


def test_bad_key_raises():
    enc = encoding_for(3, {})
    with pytest.raises(ValueError):
        enc.parse("x/1/2", 2)


def test_inner_coords_rejects_an_index_outside_the_shard():
    cps = (2, 2, 2)
    assert inner_coords((0, 0, 0), 7, cps) == (1, 1, 1)
    for bad in (-1, 8):
        with pytest.raises(ValueError):
            inner_coords((0, 0, 0), bad, cps)


def test_chunks_per_shard_rejects_a_dimension_count_mismatch():
    with pytest.raises(ZarrUnsupported) as e:
        chunks_per_shard((24, 74), (12, 37, 72))
    assert "Fix:" in str(e.value)
