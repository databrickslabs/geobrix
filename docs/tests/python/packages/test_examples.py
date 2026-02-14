"""
GeoBrix Packages Examples Tests

Tests for all packages documentation code examples.
"""

import pytest
import sys
from pathlib import Path
import importlib.util

# Import the module under test - explicitly load from current directory to avoid conflicts
examples_path = Path(__file__).parent / "examples.py"
spec = importlib.util.spec_from_file_location("examples", examples_path)
examples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(examples)


# ============================================================================
# Packages Overview Tests
# ============================================================================

def test_register_all_packages(spark):
    """Test registering all packages (structure check)."""
    assert callable(examples.register_all_packages)


def test_register_only_rasterx(spark):
    """Test registering only RasterX (structure check)."""
    assert callable(examples.register_only_rasterx)


# ============================================================================
# RasterX Tests
# ============================================================================

def test_rasterx_basic_usage(spark):
    """Test basic RasterX usage: run example and assert on result when sample data available."""
    assert callable(examples.rasterx_basic_usage)
    assert hasattr(examples, "rasterx_basic_usage_output")
    assert "width" in examples.rasterx_basic_usage_output and "height" in examples.rasterx_basic_usage_output
    try:
        result = examples.rasterx_basic_usage(spark)
        assert result is not None
        cols = result.columns
        assert "width" in cols and "height" in cols
        assert "bands" in cols and "srid" in cols
        assert result.count() >= 1
    except Exception as e:
        if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
            pytest.skip("Sample raster path not available")
        if "No module named 'databricks.labs.gbx" in str(e):
            pytest.skip("GeoBrix not installed")
        raise


def test_rasterx_clip_raster(spark):
    """Test raster clipping (structure check)."""
    assert callable(examples.rasterx_clip_raster)


def test_rasterx_cataloging_workflow(spark):
    """Test raster cataloging workflow (structure check)."""
    assert callable(examples.rasterx_cataloging_workflow)


def test_rasterx_processing_pipeline(spark):
    """Test raster processing pipeline (structure check)."""
    assert callable(examples.rasterx_processing_pipeline)


def test_rasterx_multiband_analysis(spark):
    """Test multi-band analysis (structure check)."""
    assert callable(examples.rasterx_multiband_analysis)


def test_rasterx_tiling_performance(spark):
    """Test raster tiling (structure check)."""
    assert callable(examples.rasterx_tiling_performance)


def test_rasterx_delta_integration(spark):
    """Test Delta integration (structure check)."""
    assert callable(examples.rasterx_delta_integration)


def test_sql_rasterx_usage_constant():
    """SQL_RASTERX_USAGE and output constant exist and contain expected keys."""
    assert hasattr(examples, "SQL_RASTERX_USAGE")
    assert hasattr(examples, "SQL_RASTERX_USAGE_output")
    assert "gbx_rst_width" in examples.SQL_RASTERX_USAGE
    assert "width" in examples.SQL_RASTERX_USAGE_output


def test_sql_rasterx_usage_executable(spark):
    """Run SQL RasterX example: register, create view from GDAL, then run metadata SELECT."""
    try:
        from databricks.labs.gbx.rasterx import functions as rx
        rx.register(spark)
        path = getattr(examples, "SAMPLE_RASTER_PATH", None) or ""
        df = spark.read.format("gdal").load(path)
        df.createOrReplaceTempView("rasters")
        out = spark.sql(
            "SELECT source, gbx_rst_width(tile) as width, gbx_rst_height(tile) as height, "
            "gbx_rst_numbands(tile) as num_bands, gbx_rst_srid(tile) as srid FROM rasters"
        )
        assert out.count() >= 1
        assert "width" in out.columns and "height" in out.columns
    except Exception as e:
        if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
            pytest.skip("Sample raster path not available for SQL test")
        if "No module named 'databricks.labs.gbx" in str(e):
            pytest.skip("GeoBrix not installed")
        raise


# ============================================================================
# VectorX Tests
# ============================================================================

def test_vectorx_basic_migration(spark):
    """Test basic VectorX migration (structure check)."""
    assert callable(examples.vectorx_basic_migration)


def test_vectorx_sql_migration(spark):
    """Test SQL migration (structure check)."""
    assert callable(examples.vectorx_sql_migration)


def test_vectorx_transition_validation(spark):
    """Test transition validation (structure check)."""
    assert callable(examples.vectorx_transition_validation)


def test_vectorx_enable_spatial_analysis(spark):
    """Test enabling spatial analysis (structure check)."""
    assert callable(examples.vectorx_enable_spatial_analysis)


def test_vectorx_migration_backup(spark):
    """Test migration backup (structure check)."""
    assert callable(examples.vectorx_migration_backup)


def test_vectorx_migration_convert(spark):
    """Test migration convert (structure check)."""
    assert callable(examples.vectorx_migration_convert)


def test_vectorx_migration_validate(spark):
    """Test migration validation (structure check)."""
    assert callable(examples.vectorx_migration_validate)


def test_vectorx_complete_migration_example(spark):
    """Test complete migration example (structure check)."""
    assert callable(examples.vectorx_complete_migration_example)


# ============================================================================
# GridX Tests
# ============================================================================

def test_gridx_basic_usage(spark):
    """Test basic GridX usage: run example and assert when GeoBrix available."""
    assert callable(examples.gridx_basic_usage)
    assert hasattr(examples, "gridx_basic_usage_output")
    assert "bng_cell" in examples.gridx_basic_usage_output and "cell_area_km2" in examples.gridx_basic_usage_output
    try:
        result = examples.gridx_basic_usage(spark)
        assert result is not None
        assert "bng_cell" in result.columns and "cell_area_km2" in result.columns
        assert result.count() >= 1
        row = result.first()
        assert row["bng_cell"] is not None and str(row["bng_cell"]).startswith("TQ")
        assert row["cell_area_km2"] == 1.0
    except Exception as e:
        if "No module named 'databricks.labs.gbx" in str(e):
            pytest.skip("GeoBrix not installed")
        raise


def test_sql_gridx_usage_constant():
    """SQL_GRIDX_BNG_USAGE and output constant exist."""
    assert hasattr(examples, "SQL_GRIDX_BNG_USAGE")
    assert hasattr(examples, "SQL_GRIDX_BNG_USAGE_output")
    assert "gbx_bng_cellarea" in examples.SQL_GRIDX_BNG_USAGE
    assert "gbx_bng_pointascell" in examples.SQL_GRIDX_BNG_USAGE


def test_sql_gridx_usage_executable(spark):
    """Run GridX SQL example: register, then run pointascell with BNG coords and cellarea."""
    try:
        from databricks.labs.gbx.gridx.bng import functions as bx
        bx.register(spark)
        # BNG coordinates (eastings, northings); resolution 1000 m = 1 km
        cells = spark.sql(
            "SELECT gbx_bng_pointascell('POINT(530000 180000)', '1km') as bng_cell_1km, "
            "gbx_bng_cellarea(gbx_bng_pointascell('POINT(530000 180000)', '1km')) as area_km2"
        )
        assert cells.count() == 1
        row = cells.first()
        assert row["bng_cell_1km"] is not None and str(row["bng_cell_1km"]).startswith("TQ")
        assert row["area_km2"] == 1.0
    except Exception as e:
        if "No module named 'databricks.labs.gbx" in str(e):
            pytest.skip("GeoBrix not installed")
        if "UNRESOLVED_ROUTINE" in str(e):
            pytest.skip("GeoBrix/Spark not available for GridX SQL test")
        raise


def test_gridx_spatial_aggregation(spark):
    """Test spatial aggregation (structure check)."""
    assert callable(examples.gridx_spatial_aggregation)


def test_gridx_grid_based_joins(spark):
    """Test grid-based joins (structure check)."""
    assert callable(examples.gridx_grid_based_joins)


def test_gridx_multi_resolution_analysis(spark):
    """Test multi-resolution analysis (structure check)."""
    assert callable(examples.gridx_multi_resolution_analysis)


def test_gridx_kring_analysis(spark):
    """Test k-ring analysis (structure check)."""
    assert callable(examples.gridx_kring_analysis)


def test_gridx_partition_strategy(spark):
    """Test partition strategy (structure check)."""
    assert callable(examples.gridx_partition_strategy)


def test_gridx_zorder_optimization(spark):
    """Test Z-order optimization (structure check)."""
    assert callable(examples.gridx_zorder_optimization)


def test_gridx_rasterx_integration(spark):
    """Test RasterX integration (structure check)."""
    assert callable(examples.gridx_rasterx_integration)


def test_gridx_vectorx_integration(spark):
    """Test VectorX integration (structure check)."""
    assert callable(examples.gridx_vectorx_integration)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
