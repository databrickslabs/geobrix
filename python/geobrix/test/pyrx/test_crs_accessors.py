"""Task 2 tests: rst_crs accessor + summary crs key.

Tests:
- accessors.crs() returns canonical CRS string (EPSG:x or ESRI:54008).
- accessors.srid() is UNCHANGED (still int / None).
- accessors.summary() JSON includes 'coordinateSystem.crs' string.
- rst_crs Spark Column UDF returns StringType result.

Uses REAL rasters:
- EPSG:4326 tile from conftest make_geotiff_bytes.
- ESRI:54008 MODIS TIF from target/test-classes/modis/ — the key non-EPSG case.
"""

import json
import os

import pytest

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors

from .conftest import make_geotiff_bytes

# Path to a real ESRI:54008 raster (built into target/ by Maven).
_MODIS_TIF = os.path.join(
    os.path.dirname(__file__),
    "../../../../target/test-classes/modis",
    "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF",
)


def _ds_from_bytes(b):
    return _serde.open_tile(b)


def _ds_from_file(path):
    """Open a rasterio DatasetReader from a file path (not a tile struct)."""
    import rasterio

    return rasterio.open(path)


# ---------------------------------------------------------------------------
# accessors.crs — unit tests (no Spark needed)
# ---------------------------------------------------------------------------


def test_crs_epsg_raster():
    """EPSG:4326 raster -> crs() == 'EPSG:4326'; srid() == 4326 (unchanged)."""
    b = make_geotiff_bytes(epsg=4326)
    with _ds_from_bytes(b) as ds:
        assert accessors.srid(ds) == 4326  # unchanged
        assert accessors.crs(ds) == "EPSG:4326"


def test_crs_epsg_raster_other():
    """EPSG:32633 raster -> crs() == 'EPSG:32633'."""
    b = make_geotiff_bytes(epsg=32633)
    with _ds_from_bytes(b) as ds:
        assert accessors.crs(ds) == "EPSG:32633"
        assert accessors.srid(ds) == 32633


@pytest.mark.skipif(not os.path.exists(_MODIS_TIF), reason="MODIS TIF not built yet")
def test_crs_esri_raster():
    """ESRI:54008 MODIS raster -> srid() == None (unchanged); crs() == 'ESRI:54008'."""
    with _ds_from_file(_MODIS_TIF) as ds:
        assert accessors.srid(ds) is None, "srid must remain None for non-EPSG CRS"
        crs_str = accessors.crs(ds)
        assert crs_str is not None, "crs() must not return None for ESRI:54008"
        assert "ESRI" in crs_str, f"Expected 'ESRI' in crs string, got: {crs_str!r}"
        assert "54008" in crs_str, f"Expected '54008' in crs string, got: {crs_str!r}"
        assert crs_str == "ESRI:54008", f"Expected exact 'ESRI:54008', got: {crs_str!r}"


# ---------------------------------------------------------------------------
# accessors.summary — crs key in coordinateSystem
# ---------------------------------------------------------------------------


def test_summary_epsg_includes_crs_key():
    """summary JSON coordinateSystem has both 'epsg' and 'crs' for EPSG raster."""
    b = make_geotiff_bytes(epsg=4326)
    with _ds_from_bytes(b) as ds:
        info = json.loads(accessors.summary(ds))
    cs = info["coordinateSystem"]
    assert cs["epsg"] == 4326
    assert cs["crs"] == "EPSG:4326"


@pytest.mark.skipif(not os.path.exists(_MODIS_TIF), reason="MODIS TIF not built yet")
def test_summary_esri_includes_crs_key():
    """summary JSON coordinateSystem.crs is non-null for ESRI:54008; epsg is null."""
    with _ds_from_file(_MODIS_TIF) as ds:
        info = json.loads(accessors.summary(ds))
    cs = info["coordinateSystem"]
    assert cs["epsg"] is None, "epsg must remain null for ESRI:54008"
    crs_str = cs["crs"]
    assert crs_str is not None, "coordinateSystem.crs must be non-null for ESRI:54008"
    assert "ESRI" in crs_str and "54008" in crs_str, f"Got: {crs_str!r}"


# ---------------------------------------------------------------------------
# rst_crs Spark Column UDF
# ---------------------------------------------------------------------------


def test_rst_crs_spark_epsg(spark):
    """rst_crs() on an EPSG:4326 tile returns 'EPSG:4326' as a Spark string."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    b = make_geotiff_bytes(epsg=4326)
    df = spark.createDataFrame([(b,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(prx.rst_crs("tile").alias("c")).first()
    assert row["c"] == "EPSG:4326"


def test_rst_crs_spark_esri(spark):
    """rst_crs() on an ESRI:54008 tile returns 'ESRI:54008'; rst_srid stays null."""
    from pyspark.sql import functions as f
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx import functions as prx

    if not os.path.exists(_MODIS_TIF):
        pytest.skip("MODIS TIF not built yet")

    # Read the MODIS TIF bytes and re-encode as standalone GTiff in memory.
    with open(_MODIS_TIF, "rb") as fh:
        src_bytes = fh.read()
    with MemoryFile(src_bytes) as mf, mf.open() as src:
        profile = src.profile.copy()
        profile.update(driver="GTiff")
        with MemoryFile() as mf2:
            with mf2.open(**profile) as dst:
                dst.write(src.read())
            tile_bytes = mf2.read()

    df = spark.createDataFrame([(tile_bytes,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(
        prx.rst_crs("tile").alias("c"), prx.rst_srid("tile").alias("s")
    ).first()
    crs_str = row["c"]
    assert crs_str is not None
    assert "ESRI" in crs_str and "54008" in crs_str, f"Got: {crs_str!r}"
    assert row["s"] is None, "rst_srid must remain null for ESRI:54008"
