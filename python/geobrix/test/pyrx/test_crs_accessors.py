"""Tests for the rst_crs accessor.

Tests:
- accessors.crs() returns canonical CRS string (EPSG:x or ESRI:54008).
- accessors.srid() returns ESRI-aware int code (ESRI:54008 → 54008, not None).
- rst_crs Spark Column UDF returns StringType result.

Uses REAL rasters:
- EPSG:4326 tile from conftest make_geotiff_bytes.
- ESRI:54008 MODIS TIF from target/test-classes/modis/ — the key non-EPSG case.
"""

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
    """ESRI:54008 MODIS raster -> srid() == 54008; crs() == 'ESRI:54008'."""
    with _ds_from_file(_MODIS_TIF) as ds:
        assert (
            accessors.srid(ds) == 54008
        ), "srid must return 54008 for ESRI:54008 (not None)"
        crs_str = accessors.crs(ds)
        assert crs_str is not None, "crs() must not return None for ESRI:54008"
        assert "ESRI" in crs_str, f"Expected 'ESRI' in crs string, got: {crs_str!r}"
        assert "54008" in crs_str, f"Expected '54008' in crs string, got: {crs_str!r}"
        assert crs_str == "ESRI:54008", f"Expected exact 'ESRI:54008', got: {crs_str!r}"


# ---------------------------------------------------------------------------
# rst_crs Spark Column UDF
# ---------------------------------------------------------------------------
