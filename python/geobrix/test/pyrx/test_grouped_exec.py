"""Tests for grouped_tile_map — partition-scoped mapInPandas executor.

Local Spark (local[2]) always returns file_supported()=False, so all tests
exercise the fallback opener path over materialized tiles.  The FILE-stream
fast path is validated on-cluster in Task 9.

T9b fix: the worker no longer calls file_supported() to decide the FILE path;
it keys off _file_ref column presence (has_fr_col) instead, avoiding the
getActiveSession()==None pitfall on Spark-Connect worker threads.
"""

import numpy as np
from pyspark.sql.types import LongType, StructField, StructType

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

_FLOAT32_ITEMSIZE = 4  # bytes per sample in the test GeoTIFFs (float32)


def _tile_df(spark, tile_bytes):
    """Create a 3-row DataFrame with materialized tiles (raster inline, path None)."""
    rows = [
        (VirtualTile.from_v1(cellid=i, raster=tile_bytes).to_row(),) for i in range(3)
    ]
    return spark.createDataFrame(
        rows, StructType([StructField("tile", V2_TILE_SCHEMA)])
    )


def test_grouped_map_matches_per_row_memsize(spark, gtiff_bytes):
    """grouped_tile_map result equals per-row memsize computation.

    gtiff_bytes = 4x3 float32 count=1; expected sz = 4*3*1*4 = 48 bytes.
    The fallback path (file_supported()=False locally) opens each materialized
    tile per-row via _open and applies core_fn on the open DatasetReader.
    """

    def core_fn(ds):
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        core_fn,
        return_field=StructField("sz", LongType()),
    )
    vals = sorted(r["sz"] for r in out.collect())
    assert vals == [4 * 3 * 1 * 4] * 3, f"unexpected sizes: {vals}"


def test_grouped_map_output_schema_extends_input(spark, gtiff_bytes):
    """Output schema is input schema + return_field (no extra cast)."""
    df = _tile_df(spark, gtiff_bytes)
    out = grouped_tile_map(
        df,
        lambda ds: ds.count,
        return_field=StructField("band_count", LongType()),
    )
    assert out.schema.fieldNames() == ["tile", "band_count"]


def test_grouped_map_custom_tile_col(spark, gtiff_bytes):
    """tile_col kwarg selects the correct struct column by name."""
    rows = [
        (i, VirtualTile.from_v1(cellid=i, raster=gtiff_bytes).to_row())
        for i in range(2)
    ]
    df = spark.createDataFrame(
        rows,
        StructType(
            [
                StructField("id", LongType()),
                StructField("raster_tile", V2_TILE_SCHEMA),
            ]
        ),
    )
    out = grouped_tile_map(
        df,
        lambda ds: ds.count,
        return_field=StructField("nb", LongType()),
        tile_col="raster_tile",
    )
    nbs = [r["nb"] for r in out.collect()]
    assert all(nb == 1 for nb in nbs)


# ---------------------------------------------------------------------------
# T9b: _make_opener no longer calls worker-side file_supported()
# ---------------------------------------------------------------------------


def test_make_opener_returns_four_items_not_five():
    """_make_opener must return (fr_holder, opener, closer, weigher) — 4 items.

    Under the OLD implementation _make_opener called file_supported() on the
    worker and returned a 5-tuple (file_ok, fr_holder, opener, closer, weigher).
    The T9b fix removes the worker-side file_supported() call and drops file_ok
    from the return: the FILE capability signal is now the _file_ref column's
    presence (has_fr_col), which the driver set via an explicit df.sparkSession.

    This structural assertion FAILS under the old 5-tuple contract and PASSES
    under the new 4-tuple contract, confirming the getActiveSession() dependency
    has been removed from the worker path.
    """
    from databricks.labs.gbx.pyrx.grouped_exec import _make_opener

    result = _make_opener()
    assert len(result) == 4, (
        f"_make_opener must return 4 items (fr_holder, opener, closer, weigher); "
        f"got {len(result)} — worker-side file_supported() may still be present "
        f"(T9b regression)"
    )
    fr_holder, opener, closer, weigher = result
    assert isinstance(fr_holder, list) and len(fr_holder) == 1
    assert callable(opener)
    assert callable(closer)
    assert callable(weigher)


# ---------------------------------------------------------------------------
# I2: grouped executor robustness
# ---------------------------------------------------------------------------


def test_grouped_map_windowless_virtual_tile(spark, gtiff_bytes, tmp_path):
    """A windowless virtual tile (window=None) returns the full-source footprint.

    file_supported()=False locally so tiles take the fallback path.  The fallback
    must route windowless virtual tiles through open_header (not _open, which
    raises ValueError for window=None).  This matches per-row rst_memsize behavior.
    gtiff_bytes is 4x3 float32 count=1 → 4*3*1*4=48 bytes footprint.
    """
    tif = tmp_path / "whole.tif"
    tif.write_bytes(gtiff_bytes)

    vt = VirtualTile(cellid=7, path=str(tif), raster=None, window=None)
    df = spark.createDataFrame(
        [(vt.to_row(),)],
        StructType([StructField("tile", V2_TILE_SCHEMA)]),
    )

    def core_fn(ds):
        return int(ds.count * ds.width * ds.height * np.dtype(ds.dtypes[0]).itemsize)

    out = grouped_tile_map(df, core_fn, return_field=StructField("sz", LongType()))
    vals = [r["sz"] for r in out.collect()]
    assert vals == [
        4 * 3 * 1 * _FLOAT32_ITEMSIZE
    ], f"unexpected result for windowless virtual tile: {vals}"


def test_grouped_map_unreadable_tile_degrades_gracefully(spark, gtiff_bytes, tmp_path):
    """An unreadable tile (bogus path) degrades to None; partition does NOT crash.

    The good tile in the same partition still computes its result correctly.
    gtiff_bytes is 4x3 float32 count=1 → 48 bytes footprint.
    """
    good = tmp_path / "good.tif"
    good.write_bytes(gtiff_bytes)

    rows = [
        (
            VirtualTile(
                cellid=0,
                path="/nonexistent/bogus.tif",
                raster=None,
                window=(0, 0, 4, 3),
            ).to_row(),
        ),
        (
            VirtualTile(
                cellid=1, path=str(good), raster=None, window=(0, 0, 4, 3)
            ).to_row(),
        ),
    ]
    df = spark.createDataFrame(rows, StructType([StructField("tile", V2_TILE_SCHEMA)]))

    def core_fn(ds):
        return int(ds.count * ds.width * ds.height * np.dtype(ds.dtypes[0]).itemsize)

    out = grouped_tile_map(df, core_fn, return_field=StructField("sz", LongType()))
    result = {r["tile"]["cellid"]: r["sz"] for r in out.collect()}

    assert result[0] is None, f"expected None for unreadable tile, got {result[0]}"
    assert (
        result[1] == 4 * 3 * 1 * _FLOAT32_ITEMSIZE
    ), f"readable tile wrong: {result[1]}"
