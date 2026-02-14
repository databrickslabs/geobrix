"""
Pytest configuration for function-info tests.

Creates a Spark session with GeoBrix JAR and registers RasterX, GridX (BNG), and VectorX
so DESCRIBE FUNCTION / DESCRIBE FUNCTION EXTENDED can be exercised.
"""

import os
from pathlib import Path

import pytest

# Project root: docs/tests-function-info -> docs -> repo root
DOCS_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = DOCS_DIR.parent
GEOBRIX_JAR = PROJECT_ROOT / "target" / "geobrix-0.2.0-jar-with-dependencies.jar"


def _register_all(spark):
    """Register RasterX, GridX (BNG), and VectorX with the given Spark session."""
    try:
        from databricks.labs.gbx.rasterx import functions as rx
        rx.register(spark)
    except Exception as e:
        raise RuntimeError("Failed to register RasterX") from e
    try:
        from databricks.labs.gbx.gridx.bng import functions as bx
        bx.register(spark)
    except Exception as e:
        raise RuntimeError("Failed to register GridX BNG") from e
    try:
        from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
        vx.register(spark)
    except Exception as e:
        raise RuntimeError("Failed to register VectorX") from e


@pytest.fixture(scope="session")
def spark_with_geobrix():
    """Create Spark session with GeoBrix JAR and all packages registered."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark not available; run in Docker with full env")

    if not GEOBRIX_JAR.exists():
        pytest.skip(
            f"GeoBrix JAR not found at {GEOBRIX_JAR}. Run 'mvn package' first."
        )

    if "JAVA_TOOL_OPTIONS" in os.environ:
        del os.environ["JAVA_TOOL_OPTIONS"]

    spark = SparkSession.builder \
        .appName("GeoBrix Function-Info Tests") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.executor.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=/usr/local/lib") \
        .getOrCreate()

    _register_all(spark)
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark(spark_with_geobrix):
    """Session-scoped Spark with GeoBrix registered."""
    return spark_with_geobrix


@pytest.fixture(scope="session")
def registered_gbx_functions(spark):
    """List of all registered function names with gbx_ prefix (from Spark)."""
    df = spark.sql("SHOW FUNCTIONS")
    names = [row.function for row in df.collect() if row.function and row.function.startswith("gbx_")]
    return sorted(set(names))


def load_registered_functions_txt():
    """Load registered function names from docs/tests-function-info/registered_functions.txt."""
    path = Path(__file__).resolve().parent / "registered_functions.txt"
    names = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return sorted(set(names))
