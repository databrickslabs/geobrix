"""
Tests for GDAL Writer Examples

Verifies the code examples in docs/docs/writers/gdal.mdx run end-to-end
against sample-data rasters, and that written files are valid GDAL datasets.

Run:
    pytest docs/tests/python/writers/test_gdal_examples.py -v
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import gdal_examples
from path_config import SAMPLE_DATA_BASE

SAMPLE_GTIFF = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


# NOTE: no local `spark` fixture here. A bare SparkSession.getOrCreate() would create
# a JAR-less session first, and because Spark reuses one active session per JVM, conftest's
# `spark_with_geobrix` (which attaches the geobrix JAR + registers) would then get that
# existing JAR-less session back (builder .config is ignored once a session is active),
# so `format("gdal")` fails with DATA_SOURCE_NOT_FOUND. Inherit the conftest `spark`
# fixture instead so the JAR-attached, registered session is created.


@pytest.fixture
def out_dir(tmp_path):
    """Per-test scratch directory; removed after each test."""
    d = tmp_path / "gdal_write_out"
    d.mkdir()
    yield str(d)
    shutil.rmtree(d, ignore_errors=True)


def _skip_if_no_sample(path):
    if not os.path.exists(path):
        pytest.skip(f"Raster sample not present at {path}; use full bundle or generate minimal bundle")


def _assert_valid_tif(out_dir):
    """Every non-crc file under out_dir should open with GDAL."""
    from osgeo import gdal
    files = [f for f in os.listdir(out_dir) if not f.endswith(".crc")]
    assert len(files) > 0, f"no output files produced in {out_dir}"
    for fname in files:
        ds = gdal.Open(os.path.join(out_dir, fname))
        assert ds is not None, f"GDAL could not open {fname}"
        # Minimal sanity: non-zero raster dimensions
        assert ds.RasterXSize > 0 and ds.RasterYSize > 0


def test_write_gdal(spark, out_dir):
    _skip_if_no_sample(SAMPLE_GTIFF)
    gdal_examples.write_gdal(spark, SAMPLE_GTIFF, out_dir)
    _assert_valid_tif(out_dir)


def test_write_with_namecol(spark, out_dir):
    """nameCol controls the output filename (prefix)."""
    _skip_if_no_sample(SAMPLE_GTIFF)
    gdal_examples.write_with_namecol(spark, SAMPLE_GTIFF, out_dir)
    _assert_valid_tif(out_dir)
    files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert any(f.startswith("tile_") for f in files), (
        f"expected nameCol prefix 'tile_' in {files}"
    )


def test_constants_defined():
    """Doc-display constants are non-empty strings."""
    for name in ("WRITE_GDAL", "WRITE_GDAL_output", "WRITE_WITH_NAMECOL", "MATERIALIZE_PIPELINE"):
        assert hasattr(gdal_examples, name), f"missing {name}"
        value = getattr(gdal_examples, name)
        assert isinstance(value, str) and len(value.strip()) > 0
