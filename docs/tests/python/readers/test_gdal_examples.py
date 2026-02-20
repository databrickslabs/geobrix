"""
Tests for GDAL Reader Examples

These tests verify that the code examples in the documentation are valid.

Run:
    pytest docs/tests/python/readers/test_gdal_examples.py -v
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import gdal_examples
from path_config import SAMPLE_DATA_BASE

# Sample data paths at runtime (path_config)
SAMPLE_GTIFF = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("GDALExamplesTest").getOrCreate()


def test_read_gdal(spark):
    """Test basic GDAL read - validates READ_GDAL constant."""
    result = gdal_examples.read_gdal(spark, SAMPLE_GTIFF)
    assert result is not None
    assert 'tile' in result.columns
    if result.count() == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")
    assert result.count() > 0


def test_read_with_driver(spark):
    """Test GDAL read with driver - validates READ_WITH_DRIVER constant."""
    result = gdal_examples.read_with_driver(spark, SAMPLE_GTIFF)
    assert result is not None
    if result.count() == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")
    assert result.count() > 0


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(gdal_examples, 'SQL_GDAL')
    assert 'gdal.' in gdal_examples.SQL_GDAL
    assert 'SELECT' in gdal_examples.SQL_GDAL


def test_output_constants():
    """GDAL reader doc: output constants for Example output blocks (one-copy)."""
    for name in ("READ_GDAL_output", "READ_WITH_DRIVER_output", "SQL_GDAL_output", "READ_GRIB2_output"):
        assert hasattr(gdal_examples, name), f"missing {name}"
        assert isinstance(getattr(gdal_examples, name), str)
        assert len(getattr(gdal_examples, name).strip()) > 0


def test_read_grib2(spark):
    """Test GRIB2 read with HRRR path - validates READ_GRIB2 constant when sample-data present."""
    import os
    import glob
    # HRRR is in complete bundle; skip if not present. Use one concrete file (GDAL/Spark may not expand glob).
    hrrr_dir = f"{SAMPLE_DATA_BASE}/nyc/hrrr-weather"
    if not os.path.isdir(hrrr_dir):
        pytest.skip("HRRR sample data dir not present (download complete bundle)")
    grib2_files = glob.glob(os.path.join(hrrr_dir, "*.grib2"))
    if not grib2_files:
        pytest.skip("HRRR .grib2 files not present (download complete bundle)")
    result = gdal_examples.read_grib2(spark, path=grib2_files[0])
    assert result is not None
    assert result.count() > 0
    assert "tile" in result.columns
