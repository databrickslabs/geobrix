"""
Pytest configuration for documentation tests.

This module sets up fixtures and configuration for all documentation tests.
Most importantly, it creates a Spark session with GeoBrix properly registered.
"""

import pytest
import os
from pathlib import Path


# Determine paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
GEOBRIX_JAR = PROJECT_ROOT / "target" / "geobrix-0.2.0-jar-with-dependencies.jar"


@pytest.fixture(scope="session")
def spark_with_geobrix():
    """
    Create a Spark session with GeoBrix JAR registered.
    
    This session is shared across all tests in the session to avoid
    creating multiple Spark contexts (which causes issues).
    
    Returns:
        SparkSession with GeoBrix readers and functions available
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise RuntimeError("PySpark is required for doc tests; run in Docker with full env.") from e

    if not GEOBRIX_JAR.exists():
        raise FileNotFoundError(
            f"GeoBrix JAR not found at {GEOBRIX_JAR}. Run 'mvn package' first (or use Docker with built project)."
        )
    
    # Unset JAVA_TOOL_OPTIONS to prevent debug port conflicts
    # (Docker container may have JDWP enabled which conflicts with Spark)
    if 'JAVA_TOOL_OPTIONS' in os.environ:
        del os.environ['JAVA_TOOL_OPTIONS']
        print("ℹ️  Unset JAVA_TOOL_OPTIONS to prevent Spark conflicts")
    
    # Create Spark session with GeoBrix JAR
    spark = SparkSession.builder \
        .appName("GeoBrix Documentation Tests") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.executor.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=/usr/local/lib") \
        .getOrCreate()
    
    print(f"✅ Spark session created with GeoBrix JAR: {GEOBRIX_JAR}")
    print(f"   JAR exists: {GEOBRIX_JAR.exists()}")
    print(f"   JAR size: {GEOBRIX_JAR.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Try to register GeoBrix functions
    try:
        from databricks.labs.gbx.rasterx import functions as rx
        rx.register(spark)
        print(f"✅ GeoBrix rasterx functions registered")
    except ImportError:
        print("⚠️  GeoBrix rasterx not available (Python bindings may not be installed)")
    except Exception as e:
        print(f"⚠️  Could not register rasterx functions: {e}")
    
    try:
        from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
        vx.register(spark)
        print(f"✅ GeoBrix vectorx functions registered")
    except ImportError:
        print("⚠️  GeoBrix vectorx not available")
    except Exception as e:
        print(f"⚠️  Could not register vectorx functions: {e}")
    
    yield spark
    
    # Cleanup
    spark.stop()


@pytest.fixture(scope="module")
def spark(spark_with_geobrix):
    """
    Provide Spark session for tests.
    
    Module-scoped so it can be used by module-scoped fixtures.
    Tests can use this directly.
    """
    return spark_with_geobrix


# Configure pytest markers
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "structure: marks tests as structure tests (compile/import checks)"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (require full environment)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to add markers automatically.
    
    This helps organize tests even if developers forget to add markers.
    """
    for item in items:
        # Auto-mark integration tests
        if "integration" in item.nodeid.lower() or "full_workflow" in item.name.lower():
            item.add_marker(pytest.mark.integration)
        
        # Auto-mark slow tests
        if "slow" in item.name.lower() or "batch" in item.name.lower():
            item.add_marker(pytest.mark.slow)


# Sample data paths (centralized)
SAMPLE_DATA_BASE = "/Volumes/main/default/geobrix_samples/geobrix-examples"


@pytest.fixture
def sample_nyc_taxi_zones():
    """NYC Taxi Zones GeoJSON path."""
    return f"{SAMPLE_DATA_BASE}/nyc/taxi-zones/nyc_taxi_zones.geojson"


@pytest.fixture
def sample_nyc_boroughs():
    """NYC Boroughs GeoJSON path."""
    return f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"


@pytest.fixture
def sample_london_boroughs():
    """London Boroughs GeoJSON path."""
    return f"{SAMPLE_DATA_BASE}/london/boroughs/london_boroughs.geojson"


@pytest.fixture
def sample_nyc_parks_shp():
    """NYC Parks zipped shapefile path (.shp.zip format)."""
    return f"{SAMPLE_DATA_BASE}/nyc/parks/nyc_parks.shp.zip"


@pytest.fixture
def sample_nyc_subway_shp():
    """NYC Subway Stations zipped shapefile path (.shp.zip format)."""
    return f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"


@pytest.fixture
def sample_nyc_filegdb():
    """NYC Sample FileGDB zipped path."""
    return f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"


@pytest.fixture
def sample_nyc_raster():
    """NYC Sentinel-2 raster path."""
    return f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


@pytest.fixture
def sample_data_base():
    """Base path for all sample data."""
    return SAMPLE_DATA_BASE


@pytest.fixture
def sample_geojsonseq_taxi_zones():
    """
    NYC Taxi Zones in GeoJSONSeq format (newline-delimited).
    
    This file is in the test resources and uses GeoJSONSeq format,
    which is better for large files and parallel processing.
    
    Returns absolute path for Spark to read.
    """
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent.parent
    geojsonseq_file = project_root / "src" / "test" / "resources" / "text" / "NYC_Taxi_Zones.geojson"
    return str(geojsonseq_file)
