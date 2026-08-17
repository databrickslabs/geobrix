"""Tests for rst_memsize_grouped — partition-scoped memsize via grouped executor.

Verifies that rst_memsize_grouped(df) matches per-row rst_memsize for the same
tiles, and that the computed value equals count * width * height * itemsize.
"""

import numpy as np
from pyspark.sql.types import StructField as SF
from pyspark.sql.types import StructType
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.pyrx.functions import rst_memsize_grouped


def _make_bytes(width=8, height=8, count=1, epsg=4326):
    """In-memory GTiff bytes (float32) of the requested dimensions."""
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def test_grouped_memsize_formula(spark):
    """rst_memsize_grouped computes count * width * height * itemsize via rasterio.

    Uses materialized tiles for local convenience (no FILE type needed).  The
    grouped executor opens each tile via _open and applies _core on the DatasetReader,
    returning the decoded-window footprint (not the serialized buffer length that
    per-row rst_memsize returns for materialized tiles).  For virtual tiles the two
    paths agree; for materialized tiles only the formula value is asserted here.
    """
    b = _make_bytes(width=8, height=8, count=1, epsg=4326)
    df = spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=i, raster=b).to_row(),) for i in range(4)],
        StructType([SF("tile", V2_TILE_SCHEMA)]),
    )
    grouped = {
        r["tile"]["cellid"]: r["memsize"] for r in rst_memsize_grouped(df).collect()
    }
    assert set(grouped.keys()) == {0, 1, 2, 3}
    assert set(grouped.values()) == {8 * 8 * 1 * 4}


def test_grouped_memsize_all_cellids_present(spark):
    """All input tiles appear in the output with correct cellids."""
    b = _make_bytes(width=4, height=4, count=2, epsg=4326)
    df = spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=i, raster=b).to_row(),) for i in range(3)],
        StructType([SF("tile", V2_TILE_SCHEMA)]),
    )
    result = {
        r["tile"]["cellid"]: r["memsize"] for r in rst_memsize_grouped(df).collect()
    }
    # 4 * 4 * 2 * 4 = 128 bytes decoded footprint
    assert set(result.keys()) == {0, 1, 2}
    assert set(result.values()) == {4 * 4 * 2 * 4}


def test_grouped_memsize_custom_out_col(spark):
    """out_col parameter renames the output column."""
    b = _make_bytes(width=2, height=2, count=1, epsg=4326)
    df = spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=0, raster=b).to_row(),)],
        StructType([SF("tile", V2_TILE_SCHEMA)]),
    )
    result = rst_memsize_grouped(df, out_col="sz").collect()
    assert result[0]["sz"] == 2 * 2 * 1 * 4
