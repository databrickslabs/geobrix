"""Pure-function tests for core/agg.py reducers (Spark-free) + Spark grouped-agg tests."""

import numpy as np
import pytest
import shapely.wkb
from pyspark.sql.types import (
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import agg
from databricks.labs.gbx.pyrx.functions import (
    _combineavg_agg_sql_udf,
    _combineavg_agg_udf,
    _combineavg_bytes,
    _derivedband_agg_udf,
    _frombands_agg_udf,
    _frombands_bytes,
    _merge_agg_udf,
    _merge_bytes,
)


def _ras(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    """GTiff bytes from a 2-D or 3-D numpy array with a known georef."""
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[None, :, :]
    bands, h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=bands,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


# --- merge_tiles ------------------------------------------------------------
def test_merge_tiles_union_extent():
    # Two adjacent 2x2 tiles side by side -> 2x4 mosaic spanning the union.
    left = _ras(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0, px=1.0)
    right = _ras(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        assert ds.width == 4
        assert ds.height == 2
        b = ds.bounds
        assert b.left == pytest.approx(0.0)
        assert b.right == pytest.approx(4.0)


def test_merge_tiles_single_passthrough():
    one = _ras(np.array([[1, 2], [3, 4]]))
    assert agg.merge_tiles([one]) == one


def test_merge_tiles_overlap_last_wins():
    # Two 4x4 tiles overlapping in x=[2,4]: union mosaic is 4x6, overlap = cols
    # 2 and 3. Heavyweight MergeRasters builds a GDAL VRT (gdalbuildvrt), where
    # overlapping pixels take the LAST listed source. The order passed here is
    # [left, right], so the overlap must take the RIGHT tile's value (20), not
    # the left's (10) -- pre-fix rasterio defaults to first-wins and returns 10.
    left = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    right = _ras(np.full((4, 4), 20.0), ulx=2.0, uly=4.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        assert arr.shape == (4, 6)
        # Non-overlap left cols (0,1) -> 10 ; overlap cols (2,3) -> 20 (last wins)
        assert np.all(arr[:, 0:2] == 10.0)
        assert np.all(arr[:, 2:4] == 20.0)
        # Non-overlap right cols (4,5) -> 20
        assert np.all(arr[:, 4:6] == 20.0)


def test_merge_tiles_overlap_winner_order_invariant():
    # A Spark groupBy().agg() gives no row-arrival-order guarantee, so a last-wins
    # mosaic must not depend on the order tiles are passed. merge_tiles sorts by the
    # raw GTiff bytes, so one tile reliably wins the overlap whether it is listed
    # first or last.
    left = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    right = _ras(np.full((4, 4), 20.0), ulx=2.0, uly=4.0, px=1.0)
    out_lr = agg.merge_tiles([left, right])
    out_rl = agg.merge_tiles([right, left])
    # Bitwise-identical output regardless of input order.
    assert out_lr == out_rl
    with _serde.open_tile(out_lr) as ds:
        arr = ds.read(1)
        # Overlap cols (2,3) resolve to a single canonical winner regardless of order.
        overlap = arr[:, 2:4]
        winner = overlap.flat[0]
        assert winner in (10.0, 20.0)
        assert np.all(overlap == winner)


def test_merge_tiles_same_origin_overlap_winner_order_invariant():
    # The residual nondeterminism hole the content-byte sort closes: two tiles with
    # the SAME geotransform origin but different content fully overlap. A geotransform
    # -origin key cannot separate them (they tie on origin), so the old key fell back
    # to a per-open /vsimem/<uuid> description -- random, so the winner varied run to
    # run and the two tiers disagreed. Sorting on raw GTiff bytes is a total order with
    # no tie, so the winner is fixed and identical regardless of input order.
    a = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    b = _ras(np.full((4, 4), 20.0), ulx=0.0, uly=4.0, px=1.0)
    out_ab = agg.merge_tiles([a, b])
    out_ba = agg.merge_tiles([b, a])
    # Bitwise-identical regardless of order -- this is the case the origin key failed.
    assert out_ab == out_ba
    with _serde.open_tile(out_ab) as ds:
        arr = ds.read(1)
    # Fully overlapping tiles -> one constant wins everywhere (10.0 or 20.0).
    winner = arr.flat[0]
    assert winner in (10.0, 20.0)
    assert np.all(arr == winner)


# --- combineavg_tiles -------------------------------------------------------
def test_combineavg_tiles_mean():
    a = _ras(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])


def test_combineavg_tiles_ignores_nodata():
    # Where one input is NoData, the mean is taken over the valid input only.
    a = _ras(np.array([[2.0, -9999.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 10.0], [-9999.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    # (2+4)/2=3 ; only-b=10 ; only-a=6 ; (8+12)/2=10
    assert np.allclose(got, [[3.0, 10.0], [6.0, 10.0]])


def test_combineavg_tiles_all_nodata_pixel_gets_fallback():
    a = _ras(np.array([[-9999.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[-9999.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    assert got[0, 0] == pytest.approx(-9999.0)


def test_combineavg_tiles_shape_mismatch_raises():
    a = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))
    b = _ras(np.array([[1.0, 2.0, 3.0]]))
    with pytest.raises(ValueError, match="aligned tiles"):
        agg.combineavg_tiles([a, b])


def test_combineavg_tiles_streaming_many_tiles_with_nodata():
    # Exercises the streaming sum+count accumulation over N>2 tiles (the memory rewrite):
    # the per-pixel mean must use ONLY the valid (non-NoData) inputs at each pixel, no matter
    # how the tiles are folded one-at-a-time.
    tiles = [
        _ras(np.array([[10.0, -9999.0], [1.0, 5.0]])),
        _ras(np.array([[20.0, 4.0], [2.0, -9999.0]])),
        _ras(np.array([[30.0, 8.0], [-9999.0, -9999.0]])),
        _ras(np.array([[40.0, -9999.0], [4.0, 5.0]])),
        _ras(np.array([[50.0, 12.0], [-9999.0, 5.0]])),
    ]
    out = agg.combineavg_tiles(tiles)
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    # pixel(0,0): mean(10,20,30,40,50)=30 ; (0,1): mean(4,8,12)=8 ;
    # (1,0): mean(1,2,4)=7/3 ; (1,1): mean(5,5,5)=5
    assert np.allclose(got, [[30.0, 8.0], [7.0 / 3.0, 5.0]])


def test_combineavg_tiles_streaming_no_nodata_declared():
    # When no input declares NoData, every value counts (the valid=None fast path).
    tiles = [_ras(np.array([[v, v]]), nodata=None) for v in (1.0, 2.0, 3.0, 6.0)]
    out = agg.combineavg_tiles(tiles)
    with _serde.open_tile(out) as ds:
        assert np.allclose(ds.read(1), [[3.0, 3.0]])  # mean(1,2,3,6)=3


def test_open_all_closes_and_raises_on_corrupt_tile():
    # A corrupt tile mid-group must raise cleanly (not hang/crash) -- exercises the _open_all
    # partial-open failure path that closes the already-opened buffers before re-raising.
    good = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))
    with pytest.raises(Exception):  # noqa: B017 — rasterio raises its own IO error type
        agg.merge_tiles([good, b"not a valid geotiff", good])


# --- frombands_tiles --------------------------------------------------------
def test_frombands_tiles_ascending_order():
    # Provide out of order: index 2 then index 0 then index 1.
    b0 = _ras(np.full((2, 2), 10.0))
    b1 = _ras(np.full((2, 2), 20.0))
    b2 = _ras(np.full((2, 2), 30.0))
    out = agg.frombands_tiles([(2, b2), (0, b0), (1, b1)])
    with _serde.open_tile(out) as ds:
        assert ds.count == 3
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)
        assert np.allclose(ds.read(3), 30.0)


# --- rasterize_features -----------------------------------------------------
def test_rasterize_features_burns_values():
    # Extent 0..4 x 0..4, 4x4 px (1 unit/px). Two boxes, second overlaps first.
    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))  # left half -> value 1
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))  # overlaps col 1 -> value 2 (last wins)
    out = agg.rasterize_features([(g1, 1.0), (g2, 2.0)], 0, 0, 4, 4, 4, 4, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # Column 0 only g1 -> 1 ; columns 1..3 -> g2 last-wins -> 2.
    assert np.all(arr[:, 0] == 1.0)
    assert np.all(arr[:, 1] == 2.0)


def test_rasterize_features_overlap_winner_order_invariant():
    # A Spark groupBy().agg() gives no feature-arrival-order guarantee, so a
    # last-wins burn must not depend on feature order. rasterize_features burns in
    # a canonical (geom_wkb, value) order, so the overlap pixel is identical
    # whichever order the features are supplied.
    g1 = shapely.wkb.dumps(box(0, 0, 3, 4))  # left band, value 1
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))  # overlaps cols 1..2, value 2
    out_ab = agg.rasterize_features([(g1, 1.0), (g2, 2.0)], 0, 0, 4, 4, 4, 4, 32633)
    out_ba = agg.rasterize_features([(g2, 2.0), (g1, 1.0)], 0, 0, 4, 4, 4, 4, 32633)
    assert out_ab == out_ba  # bitwise-identical regardless of order
    with _serde.open_tile(out_ab) as ds:
        arr = ds.read(1)
    # Overlap cols 1..2 resolve to a single canonical winner (1.0 or 2.0).
    overlap = arr[:, 1:3]
    winner = overlap.flat[0]
    assert winner in (1.0, 2.0)
    assert np.all(overlap == winner)


def test_rasterize_features_empty_returns_none():
    assert agg.rasterize_features([], 0, 0, 4, 4, 4, 4, 32633) is None


# --- derivedband_tiles ------------------------------------------------------
PYFUNC_SUM = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def test_derivedband_tiles_sum_across_group():
    a = _ras(np.full((2, 2), 3.0))
    b = _ras(np.full((2, 2), 4.0))
    c = _ras(np.full((2, 2), 5.0))
    out = agg.derivedband_tiles([a, b, c], PYFUNC_SUM, "addbands")
    with _serde.open_tile(out) as ds:
        assert ds.count == 1
        assert np.allclose(ds.read(1), 12.0)


# --- corrupt-member skip in light-tier helpers --------------------------------
# These tests exercise the skip-and-count behaviour added to _merge_bytes,
# _combineavg_bytes, and _frombands_bytes (Task 5).  Each helper now wraps the
# per-member open/materialize in try/except so a corrupt-but-non-empty member is
# dropped instead of raising.


def _tile_struct(raster_bytes, cellid=0):
    """Minimal materialized v1 tile input dict (cellid, raster, metadata)."""
    return {"cellid": cellid, "raster": raster_bytes, "metadata": {}}


def _corrupt_tile():
    """Non-empty tile whose raster bytes are invalid GTiff (causes open failure)."""
    return _tile_struct(b"not a valid geotiff")


def _valid_tile(data=None, cellid=0):
    """Valid single-band 2x2 tile struct."""
    if data is None:
        data = np.array([[1.0, 2.0], [3.0, 4.0]])
    raster_bytes = _ras(data)
    return _tile_struct(raster_bytes, cellid=cellid)


class TestMergeBytesSkipsCorrupt:
    # _merge_bytes now returns (bytes, dropped: int) or None.
    # dropped > 0 when at least one corrupt member was skipped.

    def test_corrupt_member_does_not_raise(self):
        # Before the fix _merge_bytes raised when it hit the corrupt tile.
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _merge_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_tuple_bytes_and_dropped(self):
        good = _valid_tile(np.array([[7.0, 8.0], [9.0, 10.0]]))
        corrupt = _corrupt_tile()
        result = _merge_bytes([good, corrupt])
        new_bytes, dropped = result
        assert new_bytes is not None
        assert dropped == 1
        with _serde.open_tile(new_bytes) as ds:
            assert ds.count >= 1

    def test_all_corrupt_returns_none(self):
        result = _merge_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        # No corrupt members → dropped == 0.
        a = _valid_tile(np.array([[1.0, 2.0], [3.0, 4.0]]))
        b = _valid_tile(np.array([[5.0, 6.0], [7.0, 8.0]]))
        new_bytes, dropped = _merge_bytes([a, b])
        assert new_bytes is not None
        assert dropped == 0


class TestCombineavgBytesSkipsCorrupt:
    # _combineavg_bytes now returns (bytes, cellid, dropped: int) or None.

    def test_corrupt_member_does_not_raise(self):
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _combineavg_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_triple_with_dropped(self):
        good = _valid_tile(np.array([[2.0, 4.0], [6.0, 8.0]]))
        corrupt = _corrupt_tile()
        result = _combineavg_bytes([good, corrupt])
        new_bytes, cellid, dropped = result
        assert new_bytes is not None
        assert dropped == 1
        with _serde.open_tile(new_bytes) as ds:
            assert ds.count >= 1

    def test_all_corrupt_returns_none(self):
        result = _combineavg_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        a = _valid_tile(np.array([[2.0, 4.0], [6.0, 8.0]]))
        b = _valid_tile(np.array([[4.0, 8.0], [10.0, 12.0]]))
        new_bytes, _cellid, dropped = _combineavg_bytes([a, b])
        assert new_bytes is not None
        assert dropped == 0


class TestFrombandsBytesSkipsCorrupt:
    # _frombands_bytes now returns (bytes, cellid, dropped: int) or None.

    def test_corrupt_member_does_not_raise(self):
        good = _valid_tile()
        corrupt = _corrupt_tile()
        result = _frombands_bytes([good, corrupt])  # must not raise
        assert result is not None

    def test_result_is_triple_with_dropped(self):
        good = _valid_tile(np.array([[5.0, 6.0], [7.0, 8.0]]))
        corrupt = _corrupt_tile()
        result = _frombands_bytes([good, corrupt])
        new_bytes, _cellid, dropped = result
        assert new_bytes is not None
        assert dropped == 1

    def test_all_corrupt_returns_none(self):
        result = _frombands_bytes([_corrupt_tile(), _corrupt_tile()])
        assert result is None

    def test_clean_group_zero_dropped(self):
        b0 = _valid_tile(np.full((2, 2), 1.0))
        b1 = _valid_tile(np.full((2, 2), 2.0))
        new_bytes, _cellid, dropped = _frombands_bytes([b0, b1])
        assert new_bytes is not None
        assert dropped == 0


# ---------------------------------------------------------------------------
# Spark grouped-agg corrupt-member skip tests (Task A1 / Finding C)
#
# These exercise the ACTUAL pandas_udfs via a Spark groupBy().agg() with one
# valid + one corrupt tile struct in the same group, asserting:
#   (a) no raise on .collect()
#   (b) non-null aggregate over the good member
#
# The drop-count has no metadata carrier at the pandas_udf layer (bare-bytes
# return type), so we assert (a)+(b) only — not a last_error value.
# ---------------------------------------------------------------------------

_PYFUNC_IDENTITY = """
def identity(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = in_ar[0]
"""


def _valid_raster():
    """Valid GTiff bytes for Spark-based tests."""
    return _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))


def _corrupt_raster():
    """Corrupt (non-GTiff) bytes for Spark-based tests."""
    return b"not a valid geotiff"


# A 3-field (v1) tile input schema, defined locally to inject raw/corrupt bytes.
# These tests deliberately feed a v1 tile to the aggregators: the light-tier
# `_open` front-door accepts v1 tiles on INPUT indefinitely (the aggregators emit
# v2), so a v1 input row is a realistic corrupt-skip scenario. Defined here rather
# than importing a production constant so the test owns its fixture shape.
_V1_TILE_INPUT_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


def _spark_tile_df_raw(spark, raster_bytes_seq):
    """Create a one-group DataFrame of tile structs by injecting raw raster bytes.

    Uses a schematized createDataFrame (v1 tile input rows) instead of
    rst_fromcontent so that corrupt bytes can be injected without rst_fromcontent
    raising on open_tile during fromcontent. This matches how the heavy Scala tests
    inject corrupt bytes: they use rst_fromcontent on the heavy tier where build_tile
    is a GDAL open (which tolerates the inject path differently), but the light tier's
    rst_fromcontent raises on corrupt bytes before the agg_udf is ever called. The
    aggregators accept a v1 tile on input (front-door contract) and emit v2.
    """
    from pyspark.sql import functions as sf

    rows = [{"cellid": 0, "raster": rb, "metadata": {}} for rb in raster_bytes_seq]
    df = spark.createDataFrame(rows, schema=_V1_TILE_INPUT_SCHEMA)
    return df.select(sf.lit(1).alias("g"), sf.struct("*").alias("tile"))


class TestGroupedAggUdfSkipsCorrupt:
    """Spark grouped-agg pandas_udfs skip corrupt members instead of raising."""

    def test_merge_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(_merge_agg_udf(sf.col("tile")).alias("agg_bytes"))
        # Must not raise and must produce a non-null aggregate.
        rows = result.collect()
        assert rows, "no rows returned"
        assert rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_combineavg_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _combineavg_agg_udf(sf.col("tile")).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        # combineavg prepends 8-byte cellid envelope; total > 8 bytes for a real tile.
        agg_b = rows[0]["agg_bytes"]
        assert agg_b is not None and len(agg_b) > 8, "aggregate bytes must be non-null"

    def test_derivedband_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _derivedband_agg_udf(
                sf.col("tile"),
                sf.lit(_PYFUNC_IDENTITY),
                sf.lit("identity"),
            ).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        assert rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_frombands_agg_udf_no_raise(self, spark):
        from pyspark.sql import functions as sf
        from pyspark.sql.types import IntegerType

        # Build a two-row group: valid tile → band 1, corrupt bytes → band 2.
        # Reuses the local v1 tile input schema (aggregators accept v1 on input).
        schema_with_band = StructType(
            list(_V1_TILE_INPUT_SCHEMA.fields)
            + [StructField("band_idx", IntegerType(), True)]
        )
        rows_data = [
            {"cellid": 0, "raster": _valid_raster(), "metadata": {}, "band_idx": 1},
            {"cellid": 0, "raster": _corrupt_raster(), "metadata": {}, "band_idx": 2},
        ]
        df_raw = spark.createDataFrame(rows_data, schema=schema_with_band)
        df = df_raw.select(
            sf.lit(1).alias("g"),
            sf.struct("cellid", "raster", "metadata").alias("tile"),
            sf.col("band_idx"),
        )
        result = df.groupBy("g").agg(
            _frombands_agg_udf(sf.col("tile"), sf.col("band_idx")).alias("agg_bytes")
        )
        out_rows = result.collect()
        assert out_rows, "no rows returned"
        assert out_rows[0]["agg_bytes"] is not None, "aggregate bytes must be non-null"

    def test_combineavg_agg_sql_udf_no_raise(self, spark):
        """_combineavg_agg_sql_udf (backs gbx_rst_combineavg_agg) skips corrupt members.

        SQL registration variant: returns raw GTiff bytes (no cellid envelope),
        so we assert bytes are non-null and openable (not just non-empty).
        """
        from pyspark.sql import functions as sf

        df = _spark_tile_df_raw(spark, [_valid_raster(), _corrupt_raster()])
        result = df.groupBy("g").agg(
            _combineavg_agg_sql_udf(sf.col("tile")).alias("agg_bytes")
        )
        rows = result.collect()
        assert rows, "no rows returned"
        agg_b = rows[0]["agg_bytes"]
        assert agg_b is not None, "aggregate bytes must be non-null"
        # Confirm the output is a valid (openable) GTiff, not a corrupt passthrough.
        from databricks.labs.gbx.pyrx import _serde

        with _serde.open_tile(bytes(agg_b)):
            pass  # raises if bytes are not a valid raster
