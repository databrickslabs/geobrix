"""``chunk_layout``: the chunk and shard presets of the arrays ``gbx_zarr_write`` creates."""

import math

import pytest

from databricks.labs.gbx.zarr._layout import (
    SHARD_CAP_BYTES,
    TARGET_CHUNK_BYTES,
    chunk_layout,
    coordinate_chunks,
)

ERA5 = (744, 721, 1440)  # a month of hourly 0.25 degree fields
DIMS = ("time", "latitude", "longitude")


@pytest.mark.parametrize(
    "preset, chunks, shards",
    [
        # s = floor(sqrt(4 MiB / 4 B)) = 1024, clipped to 721 latitudes; shards: the globe
        # (1440 -> 2048 longitudes) times as many steps as fit in 128 MiB (22)
        ("maps", (1, 721, 1024), (22, 721, 2048)),
        # 32 x 32 tiles of all 744 steps (4 MiB / 4 KiB = 1024, clipped); the globe's
        # shard (736 x 1440) halved three times in whole chunks to fit 128 MiB, then two
        # leading chunks fit (past the 744 steps there are: room for appends)
        ("series", (744, 32, 32), (1488, 96, 192)),
        # 24 steps, s = floor(sqrt(4 MiB / (24 x 4 B))) = 209; the whole globe in 4 x 7
        # chunks fits one shard of 24 steps, two would not
        ("balanced", (24, 209, 209), (24, 836, 1463)),
    ],
)
def test_layout_presets(preset, chunks, shards):
    got_chunks, got_shards = chunk_layout(ERA5, DIMS, "float32", preset)
    assert (got_chunks, got_shards) == (chunks, shards)
    assert math.prod(got_chunks) * 4 <= TARGET_CHUNK_BYTES
    assert math.prod(got_shards) * 4 <= SHARD_CAP_BYTES
    assert all(s % c == 0 for s, c in zip(got_shards, got_chunks))


def test_balanced_is_the_default():
    assert chunk_layout(ERA5, DIMS, "float32") == chunk_layout(
        ERA5, DIMS, "float32", "balanced"
    )


def test_a_dict_sets_some_dimensions_and_the_rest_are_balanced():
    chunks, shards = chunk_layout(ERA5, DIMS, "float32", {"time": 48})
    assert chunks == (48, 209, 209)  # the spatial sizes are balanced's (24 steps)
    # 48 x 836 x 1463 x 4 B is over 128 MiB: the spatial shard halves (2 x 4 chunks),
    # then two leading chunks fit
    assert shards == (96, 418, 836)
    assert math.prod(shards) * 4 <= SHARD_CAP_BYTES


def test_small_arrays_are_clipped_and_their_leading_shard_grows_past_the_extent():
    """Ruling Z40 (M7): the leading shard grows by whole chunks while it stays within
    128 MiB, past today's length, so a store created small and appended to along time
    keeps filling the same shards."""
    chunks, shards = chunk_layout((48, 73, 144), DIMS, "float32")
    assert chunks == (24, 73, 144)
    assert shards == (24 * 133, 73, 144)  # 133 slabs of 24 x 73 x 144 x 4 B fit 128 MiB
    assert math.prod(shards) * 4 <= SHARD_CAP_BYTES
    one_step = chunk_layout((1, 721, 1440), DIMS, "float32")
    assert one_step[0][0] == 1 and one_step[1][0] > 1  # an hourly store's first hour


def test_other_ranks():
    assert chunk_layout((), (), "float64") == ((), ())
    assert chunk_layout((10,), ("x",), "float64") == ((10,), (10,))
    big = chunk_layout((10**8,), ("x",), "float64")[0]
    assert big == (TARGET_CHUNK_BYTES // 8,)
    chunks, shards = chunk_layout((4, 10, 50, 60), ("t", "lev", "y", "x"), "float32")
    assert chunks == (4, 1, 50, 60)  # only the first leading dimension takes the preset
    assert all(s % c == 0 for s, c in zip(shards, chunks))


def test_a_coordinate_is_one_chunk_with_room_to_grow():
    assert coordinate_chunks(721, "float64") == (TARGET_CHUNK_BYTES // 8,)
    assert coordinate_chunks(10**7, "int64") == (10**7,)


@pytest.mark.parametrize(
    "args, match",
    [
        ((ERA5, DIMS, "float32", "tiles"), "maps"),
        ((ERA5, DIMS, "float32", {"depth": 4}), "depth"),
        ((ERA5, DIMS, "float32", {"time": 0}), "positive"),
        ((ERA5, DIMS[:2], "float32"), "dims"),
    ],
)
def test_bad_layouts_are_refused_with_a_fix(args, match):
    with pytest.raises(ValueError, match=match) as e:
        chunk_layout(*args)
    assert "Fix:" in str(e.value)


def test_rr1_m5_a_shard_holds_at_most_4096_inner_chunks():
    """Ruling Z41 (RR1-M5): the leading growth stops at 4,096 inner chunks a shard, so a
    shard's index stays near 64 KiB (the (12, 5, 6) case grew to 93,204 and 1.5 MB)."""
    import numpy as np

    from databricks.labs.gbx.zarr._layout import SHARD_MAX_CHUNKS
    from databricks.labs.gbx.zarr._shard import index_nbytes

    chunks, shards = chunk_layout(
        (48, 10, 12), DIMS, "float32", {"time": 12, "latitude": 5, "longitude": 6}
    )
    per_shard = int(np.prod([s // c for s, c in zip(shards, chunks)]))
    assert shards == (12288, 10, 12) and per_shard == SHARD_MAX_CHUNKS == 4096
    codecs = (
        {"name": "bytes", "configuration": {"endian": "little"}},
        {"name": "crc32c"},
    )
    assert index_nbytes(per_shard, codecs) == 65540  # about 64 KiB, not 1.5 MB
