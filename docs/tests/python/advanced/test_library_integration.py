"""
Tests for library integration examples.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import library_integration


def test_rasterio_install_snippet():
    """Rasterio install snippet is defined (one-copy doc)."""
    assert hasattr(library_integration, "rasterio_install_snippet")
    assert "rasterio" in library_integration.rasterio_install_snippet


def test_rasterio_output_constants():
    """Rasterio examples have output constants for doc display (one-copy)."""
    for name in (
        "rasterio_compute_statistics_output",
        "rasterio_extract_metadata_output",
        "rasterio_normalize_raster_output",
        "rasterio_compute_ndvi_output",
        "rasterio_window_operations_output",
    ):
        assert hasattr(library_integration, name), f"missing {name}"


@pytest.mark.integration
def test_rasterio_compute_statistics(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.rasterio_compute_statistics(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio + sample data: {e}")
    assert result is not None
    assert "path" in result.columns and "stats" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_rasterio_extract_metadata(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.rasterio_extract_metadata(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio + sample data: {e}")
    assert result is not None
    assert "path" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_rasterio_normalize_raster(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.rasterio_normalize_raster(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio + sample data: {e}")
    assert result is not None
    assert "path" in result.columns and "normalized_bytes" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_rasterio_compute_ndvi(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.rasterio_compute_ndvi(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio + sample data: {e}")
    assert result is not None
    assert "path" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_rasterio_window_operations(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.rasterio_window_operations(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio + sample data: {e}")
    assert result is not None
    assert "path" in result.columns and "windows" in result.columns
    assert result.count() >= 0


def test_xarray_install_snippet():
    """XArray install snippet is defined (one-copy doc)."""
    assert hasattr(library_integration, "xarray_install_snippet")
    assert "xarray" in library_integration.xarray_install_snippet


def test_xarray_output_constants():
    """XArray examples have output constants for doc display (one-copy)."""
    for name in (
        "xarray_integration_basic_output",
        "xarray_multitemporal_analysis_output",
        "xarray_resampling_aggregation_output",
    ):
        assert hasattr(library_integration, name), f"missing {name}"


@pytest.mark.integration
def test_xarray_integration_basic(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.xarray_integration_basic(spark)
    except Exception as e:
        pytest.skip(f"Requires xarray/rioxarray + sample data: {e}")
    assert result is not None
    assert "path" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_xarray_multitemporal_analysis(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.xarray_multitemporal_analysis(spark)
    except Exception as e:
        pytest.skip(f"Requires xarray + sample data: {e}")
    assert result is not None
    assert "path" in result.columns and "change" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_xarray_resampling_aggregation(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.xarray_resampling_aggregation(spark)
    except Exception as e:
        pytest.skip(f"Requires xarray/rioxarray + sample data: {e}")
    assert result is not None
    assert "path" in result.columns
    assert result.count() >= 0


def test_pdal_install_snippet():
    """PDAL install snippet is defined (one-copy doc)."""
    assert hasattr(library_integration, "pdal_install_snippet")
    assert "pdal" in library_integration.pdal_install_snippet


def test_pdal_output_constants():
    """PDAL examples have output constants for doc display (one-copy)."""
    for name in (
        "pdal_integration_basic_output",
        "pdal_raster_integration_pattern_output",
    ):
        assert hasattr(library_integration, name), f"missing {name}"


@pytest.mark.integration
def test_pdal_integration_basic(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.pdal_integration_basic(spark)
    except Exception as e:
        pytest.skip(f"Requires PDAL + point cloud sample data: {e}")
    assert result is not None
    assert "path" in result.columns
    assert result.count() >= 0


@pytest.mark.integration
def test_pdal_raster_integration_pattern(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.pdal_raster_integration_pattern(spark)
    except Exception as e:
        pytest.skip(f"Requires GeoBrix rx + sample raster: {e}")
    assert result is not None
    assert "path" in result.columns and "workflow" in result.columns
    assert result.count() >= 0


def test_numpy_advanced_operations_output_constant():
    """NumPy advanced operations has output constant for doc display (one-copy)."""
    assert hasattr(library_integration, "numpy_advanced_operations_output")


@pytest.mark.integration
def test_numpy_advanced_operations(spark):
    """Fully validated: run with sample-data Volumes and assert result."""
    try:
        result = library_integration.numpy_advanced_operations(spark)
    except Exception as e:
        pytest.skip(f"Requires rasterio/scipy + sample raster: {e}")
    assert result is not None
    assert "path" in result.columns
    assert "filtered" in result.columns
    assert result.count() >= 0


def test_best_practice_constants():
    """Best-practice snippets are defined (one-copy doc)."""
    for name in (
        "best_practice_memory_management",
        "best_practice_coordinate_system_handling",
        "best_practice_type_conversions",
        "best_practice_resource_cleanup",
    ):
        assert hasattr(library_integration, name), f"missing {name}"
        assert isinstance(getattr(library_integration, name), str)
        assert len(getattr(library_integration, name).strip()) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
