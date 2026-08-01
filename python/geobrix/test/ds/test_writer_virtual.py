"""Writers dual-accept v1 OR v2 tile input and auto-materialize virtual tiles.

Integration (local Spark + rasterio, light tier — rasterio only, no osgeo.gdal):
  (a) v1-shaped DataFrame still writes            (regression)
  (b) v2 materialized DataFrame writes
  (c) virtual DataFrame (raster=None) auto-materializes on write
  (d) assert_write_schema accepts v1 AND v2, rejects a wrong 2-field schema
"""

import os

import numpy as np
import pytest
import rasterio
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.raster import (
    RasterGbxDataSource,
    reader_schema,
    reader_schema_v2,
)
from databricks.labs.gbx.ds.writer import assert_write_schema

_EXPECTED = np.arange(12, dtype="float32").reshape(3, 4)


def _write_sample(path):
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(_EXPECTED, 1)


def _read_single(out_dir):
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1, f"expected one tif, got {written}"
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        return ds.read(1)


# ---------------------------------------------------------------------------
# (a) v1 round-trip still writes (regression)
# ---------------------------------------------------------------------------
def test_v1_dataframe_still_writes(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_v1"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    # Build a v1 (source, tile<cellid, raster, metadata>) DataFrame explicitly.
    from databricks.labs.gbx.pyrx import _serde

    raster_bytes = src.read_bytes()
    row = ("s0", (-1, raster_bytes, {"driver": "GTiff"}))
    df = spark.createDataFrame([row], schema=reader_schema())
    assert [f.name for f in df.schema["tile"].dataType.fields] == [
        f.name for f in _serde.TILE_SCHEMA.fields
    ]

    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    arr = _read_single(out_dir)
    np.testing.assert_allclose(arr, _EXPECTED, rtol=1e-6)


# ---------------------------------------------------------------------------
# (b) v2 materialized DataFrame writes
# ---------------------------------------------------------------------------
def test_v2_materialized_dataframe_writes(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_v2"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    from databricks.labs.gbx.ds.raster import _v2_tile_row

    raster_bytes = src.read_bytes()
    tile = _v2_tile_row(
        cellid=-1,
        raster=raster_bytes,
        path=str(src),
        window=(0, 0, 4, 3),
        metadata={"driver": "GTiff"},
    )
    df = spark.createDataFrame([("s0", tile)], schema=reader_schema_v2())
    assert df.collect()[0]["tile"]["raster"] is not None  # materialized

    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    arr = _read_single(out_dir)
    np.testing.assert_allclose(arr, _EXPECTED, rtol=1e-6)


# ---------------------------------------------------------------------------
# (c) virtual DataFrame auto-materializes on write
# ---------------------------------------------------------------------------
def test_virtual_dataframe_auto_materializes(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_virtual"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    df = spark.read.format("raster_gbx").option("virtualTiles", "true").load(str(src))
    # Confirm the rows really are bytes-free before the writer sees them.
    assert df.collect()[0]["tile"]["raster"] is None

    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    arr = _read_single(out_dir)
    # Auto-materialized window equals the source pixels.
    with rasterio.open(str(src)) as ds:
        exp = ds.read(1)
    np.testing.assert_array_equal(arr, exp)


# ---------------------------------------------------------------------------
# (d) assert_write_schema accepts v1 AND v2, rejects a wrong 2-field schema
# ---------------------------------------------------------------------------
def test_assert_write_schema_accepts_v1_and_v2_rejects_wrong():
    assert_write_schema(reader_schema())  # v1 ok
    assert_write_schema(reader_schema_v2())  # v2 ok

    # Right top-level names (source, tile) but tile is a bogus 2-field struct.
    wrong = StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField(
                "tile",
                StructType(
                    [
                        StructField("cellid", LongType(), nullable=False),
                        StructField("raster", BinaryType(), nullable=True),
                    ]
                ),
                nullable=False,
            ),
        ]
    )
    with pytest.raises(ValueError):
        assert_write_schema(wrong)
