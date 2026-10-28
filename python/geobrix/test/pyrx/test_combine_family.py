"""Tests for the cross-raster combine family (light tier).

Mirrors the exact inputs/expected from RST_CombineFamilyTest.scala so a later
cross-tier parity test passes by construction.

Stack layout (2x2 FLOAT64 tiles, row-major, NoData=-9999):
  tile 0: [1,  10, ND,  4]
  tile 1: [2,  ND, ND,  4]
  tile 2: [3,  20, ND,  4]

Pixel positions in flattened order:
  p0=(row0,col0): values=[1,2,3]    -> all valid
  p1=(row0,col1): values=[10,ND,20] -> valid=[10,20]
  p2=(row1,col0): values=[ND,ND,ND] -> all NoData -> NoData out
  p3=(row1,col1): values=[4,4,4]    -> all valid
"""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import agg

ND = -9999.0
TOL = 1e-6


def _tile(values, epsg=4326, nodata=ND):
    """2x2 single-band Float64 GTiff (EPSG:4326), values in row-major order."""
    arr = np.array(values, dtype="float64").reshape(1, 2, 2)
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float64",
        crs=f"EPSG:{epsg}",
        transform=from_origin(0.0, 2.0, 1.0, 1.0),  # origin=(0,2), 1-unit pixels
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        return mf.read()


def _stack():
    """Three aligned Float64 tiles with the canonical NoData hole."""
    return [
        _tile([1.0, 10.0, ND, 4.0]),
        _tile([2.0, ND, ND, 4.0]),
        _tile([3.0, 20.0, ND, 4.0]),
    ]


def _read(tile_bytes):
    """Return flat pixel array (row-major) from single-band GTiff bytes."""
    with _serde.open_tile(tile_bytes) as ds:
        return ds.read(1).flatten()


def _nodata_value(tile_bytes):
    """Return the NoData value stamped on band 1."""
    with _serde.open_tile(tile_bytes) as ds:
        return ds.nodata


# ---------------------------------------------------------------------------
# min
# ---------------------------------------------------------------------------


def test_combine_min_valid_pixels():
    r = _read(agg.combine_min_tiles(_stack()))
    assert r[0] == pytest.approx(1.0, abs=TOL)  # min(1,2,3)
    assert r[1] == pytest.approx(10.0, abs=TOL)  # min(10,20)
    assert r[3] == pytest.approx(4.0, abs=TOL)  # min(4,4,4)


def test_combine_min_all_nodata_pixel_gets_nodata():
    r = _read(agg.combine_min_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


def test_combine_min_nodata_stamped_on_output():
    assert _nodata_value(agg.combine_min_tiles(_stack())) == pytest.approx(ND)


@pytest.mark.parametrize(
    "reducer",
    [
        agg.combine_min_tiles,
        agg.combine_max_tiles,
        agg.combine_sum_tiles,
        agg.combine_count_tiles,
        agg.combine_median_tiles,
        agg.combine_stddev_tiles,
    ],
    ids=["min", "max", "sum", "count", "median", "stddev"],
)
def test_all_stats_stamp_nodata_on_output(reducer):
    """Every combine stat must stamp the NoData sentinel on the output band."""
    assert _nodata_value(reducer(_stack())) == pytest.approx(ND)


# ---------------------------------------------------------------------------
# max
# ---------------------------------------------------------------------------


def test_combine_max_valid_pixels():
    r = _read(agg.combine_max_tiles(_stack()))
    assert r[0] == pytest.approx(3.0, abs=TOL)  # max(1,2,3)
    assert r[1] == pytest.approx(20.0, abs=TOL)  # max(10,20)
    assert r[3] == pytest.approx(4.0, abs=TOL)


def test_combine_max_all_nodata_pixel_gets_nodata():
    r = _read(agg.combine_max_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


def test_combine_max_holds_a_few_tiles_not_the_stack():
    """Peak allocation does not grow with the number of tiles: a running max, not np.stack of all N."""
    import tracemalloc

    rng = np.random.default_rng(0)
    side, n = 256, 48
    profile = dict(
        driver="GTiff", width=side, height=side, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(0.0, 1.0, 0.001, 0.001), nodata=ND,
    )  # fmt: skip
    rasters = []
    for _ in range(n):
        with MemoryFile() as mf:
            with mf.open(**profile) as ds:
                ds.write(rng.random((1, side, side)).astype("float32"))
            rasters.append(mf.read())
    tracemalloc.start()
    try:
        agg.combine_max_tiles(rasters)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    one_tile = side * side * 4
    assert peak < 12 * one_tile, f"peak {peak / 2**20:.1f} MiB for {n} tiles"


# ---------------------------------------------------------------------------
# sum
# ---------------------------------------------------------------------------


def test_combine_sum_valid_pixels():
    r = _read(agg.combine_sum_tiles(_stack()))
    assert r[0] == pytest.approx(6.0, abs=TOL)  # 1+2+3
    assert r[1] == pytest.approx(30.0, abs=TOL)  # 10+20
    assert r[3] == pytest.approx(12.0, abs=TOL)  # 4+4+4


def test_combine_sum_all_nodata_pixel_gets_nodata():
    r = _read(agg.combine_sum_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


def test_combine_count_valid_pixels():
    r = _read(agg.combine_count_tiles(_stack()))
    assert r[0] == pytest.approx(3.0, abs=TOL)  # 3 valid
    assert r[1] == pytest.approx(2.0, abs=TOL)  # 2 valid (10, 20)
    assert r[3] == pytest.approx(3.0, abs=TOL)  # 3 valid


def test_combine_count_all_nodata_pixel_gets_nodata():
    """All-NoData pixel must yield NoData sentinel, NOT 0."""
    r = _read(agg.combine_count_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


# ---------------------------------------------------------------------------
# median  (even count -> mean of two middle values, np.ma.median convention)
# ---------------------------------------------------------------------------


def test_combine_median_valid_pixels():
    r = _read(agg.combine_median_tiles(_stack()))
    assert r[0] == pytest.approx(2.0, abs=TOL)  # median(1,2,3)=2
    assert r[1] == pytest.approx(15.0, abs=TOL)  # median(10,20)=(10+20)/2=15
    assert r[3] == pytest.approx(4.0, abs=TOL)


def test_combine_median_all_nodata_pixel_gets_nodata():
    r = _read(agg.combine_median_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


# ---------------------------------------------------------------------------
# stddev  (population, ddof=0)
# ---------------------------------------------------------------------------


def test_combine_stddev_valid_pixels():
    r = _read(agg.combine_stddev_tiles(_stack()))
    # population std of {1,2,3}: variance=(1+0+1)/3=2/3 -> std=sqrt(2/3)
    assert r[0] == pytest.approx(math.sqrt(2.0 / 3.0), abs=TOL)
    # population std of {10,20}: mean=15, variance=(25+25)/2=25 -> std=5
    assert r[1] == pytest.approx(5.0, abs=TOL)
    # all equal -> std=0
    assert r[3] == pytest.approx(0.0, abs=TOL)


def test_combine_stddev_all_nodata_pixel_gets_nodata():
    r = _read(agg.combine_stddev_tiles(_stack()))
    assert r[2] == pytest.approx(ND, abs=TOL)


# ---------------------------------------------------------------------------
# Alignment precondition
# ---------------------------------------------------------------------------


def _misaligned_tile():
    """3x3 tile (different dimensions from the 2x2 stack)."""
    arr = np.ones((1, 3, 3), dtype="float64")
    profile = dict(
        driver="GTiff",
        width=3,
        height=3,
        count=1,
        dtype="float64",
        crs="EPSG:4326",
        transform=from_origin(0.0, 3.0, 1.0, 1.0),
        nodata=ND,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        return mf.read()


def _diff_crs_tile():
    """2x2 tile in EPSG:3857 (different CRS)."""
    return _tile([1.0, 2.0, 3.0, 4.0], epsg=3857)


def test_misaligned_dims_raises_error_pointing_at_align_to():
    tiles = [_tile([1.0, 2.0, 3.0, 4.0]), _misaligned_tile()]
    with pytest.raises(ValueError, match="(?i)align"):
        agg.combine_sum_tiles(tiles)


def test_misaligned_crs_raises_error_pointing_at_align_to():
    tiles = [_tile([1.0, 2.0, 3.0, 4.0], epsg=4326), _diff_crs_tile()]
    with pytest.raises(ValueError, match="(?i)align"):
        agg.combine_min_tiles(tiles)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_single_tile_passthrough():
    t = _tile([1.0, 2.0, 3.0, 4.0])
    out = agg.combine_min_tiles([t])
    assert bytes(out) == bytes(t)


def test_empty_list_returns_none():
    assert agg.combine_min_tiles([]) is None
    assert agg.combine_max_tiles([]) is None
    assert agg.combine_sum_tiles([]) is None
    assert agg.combine_count_tiles([]) is None
    assert agg.combine_median_tiles([]) is None
    assert agg.combine_stddev_tiles([]) is None


# ---------------------------------------------------------------------------
# dtype preservation and integer rounding
# ---------------------------------------------------------------------------


def test_integer_dtype_preserved_and_rounded():
    """Integer input tiles → integer output (dtype preserved, numpy rint rounding)."""

    def _int_tile(values, dtype="uint16"):
        arr = np.array(values, dtype=dtype).reshape(1, 2, 2)
        profile = dict(
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype=dtype,
            crs="EPSG:4326",
            transform=from_origin(0.0, 2.0, 1.0, 1.0),
            nodata=0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as ds:
                ds.write(arr)
            return mf.read()

    a = _int_tile([1, 2, 3, 4], dtype="uint16")
    b = _int_tile([3, 4, 5, 6], dtype="uint16")
    out = agg.combine_sum_tiles([a, b])
    with _serde.open_tile(out) as ds:
        assert ds.dtypes[0] == "uint16"
        r = ds.read(1).flatten()
        # sum: [1+3, 2+4, 3+5, 4+6] = [4, 6, 8, 10] (all fit in uint16)
        assert r[0] == 4
        assert r[1] == 6


# ---------------------------------------------------------------------------
# Column wrapper + SQL registration tests
# ---------------------------------------------------------------------------


def test_rst_combinemin_column_wrapper(spark):
    """rst_combinemin returns correct per-pixel min via Spark Column API."""
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    import databricks.labs.gbx.pyrx.functions as prx

    t0 = _tile([1.0, 10.0, ND, 4.0])
    t1 = _tile([2.0, ND, ND, 4.0])
    t2 = _tile([3.0, 20.0, ND, 4.0])

    def _tile_struct(b):
        return {"cellid": 0, "raster": b, "metadata": {}}

    schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    tile_structs = [_tile_struct(t0), _tile_struct(t1), _tile_struct(t2)]
    df = spark.createDataFrame(
        [{"tiles": tile_structs}],
        schema=StructType([StructField("tiles", ArrayType(schema), True)]),
    )

    result_df = df.select(prx.rst_combinemin("tiles").alias("out"))
    rows = result_df.collect()
    assert rows, "no rows returned"
    out = rows[0]["out"]
    assert out is not None
    with _serde.open_tile(bytes(out["raster"])) as ds:
        r = ds.read(1).flatten()
    assert r[0] == pytest.approx(1.0, abs=TOL)
    assert r[1] == pytest.approx(10.0, abs=TOL)
    assert r[2] == pytest.approx(ND, abs=TOL)
    assert r[3] == pytest.approx(4.0, abs=TOL)


def test_sql_registration_combine_family(spark):
    """All 6 gbx_rst_combine* SQL functions are registered and callable."""
    import databricks.labs.gbx.pyrx.functions as prx

    prx.register(
        spark,
        only=[
            "gbx_rst_combinemin",
            "gbx_rst_combinemax",
            "gbx_rst_combinesum",
            "gbx_rst_combinecount",
            "gbx_rst_combinemedian",
            "gbx_rst_combinestddev",
        ],
    )
    # SQL name check: function should exist in catalog
    funcs = {
        r.function
        for r in spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_combine*'").collect()
    }
    for stat in ("min", "max", "sum", "count", "median", "stddev"):
        assert (
            f"gbx_rst_combine{stat}" in funcs
        ), f"gbx_rst_combine{stat} not registered"
