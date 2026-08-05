"""Task 10 (RasterX CRS-100, Group G): VizX CRS consistency.

_resolve_plot_crs routes the basemap CRS through the shared canonical authority
and honours a `crs` override for a CRS-less raster, never erroring on absent CRS.
Unit-tests the CRS-resolution decision directly (no network / basemap fetch).
"""

from rasterio.crs import CRS

from databricks.labs.gbx.vizx._cog import _resolve_plot_crs


def test_resolve_plot_crs_uses_ds_crs_canonical():
    # A raster that carries a CRS -> canonical authority string (ignores override).
    assert _resolve_plot_crs(CRS.from_epsg(4326), None) == "EPSG:4326"
    assert _resolve_plot_crs(CRS.from_epsg(32633), "EPSG:3857") == "EPSG:32633"


def test_resolve_plot_crs_esri_ds_crs_canonical():
    # An ESRI-authority raster CRS canonicalises to ESRI:54008.
    esri = CRS.from_user_input("ESRI:54008")
    assert _resolve_plot_crs(esri, None) == "ESRI:54008"


def test_resolve_plot_crs_override_for_crsless():
    # CRS-less raster + override -> the override (canonicalised).
    assert _resolve_plot_crs(None, "EPSG:32633") == "EPSG:32633"
    assert _resolve_plot_crs(None, 4326) == "EPSG:4326"
    assert _resolve_plot_crs(None, "ESRI:54008") == "ESRI:54008"


def test_resolve_plot_crs_absent_no_override_is_none():
    # CRS-less raster, no override -> None (basemap-less plot; never errors).
    assert _resolve_plot_crs(None, None) is None
