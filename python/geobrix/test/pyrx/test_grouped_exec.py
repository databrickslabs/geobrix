"""Tests for grouped_tile_map — partition-scoped mapInPandas executor.

Local Spark (local[2]) always returns file_supported()=False, so all tests
exercise the fallback opener path over materialized tiles.  The FILE-stream
fast path is validated on-cluster in Task 9.
"""

import numpy as np
from pyspark.sql.types import LongType, StructField, StructType

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map


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
