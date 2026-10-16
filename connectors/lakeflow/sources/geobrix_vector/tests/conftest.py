import os
import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[5]
NYC = REPO_ROOT / "sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/nyc"
BOROUGHS = NYC / "boroughs/nyc_boroughs.geojson"
GPKG = NYC / "geopackage/nyc_complete.gpkg"


@pytest.fixture(scope="session")
def spark():
    # Python workers must run the same interpreter (and venv) as the driver.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    from databricks.labs.community_connector.sources.geobrix_vector import (
        GeoBrixVectorDataSource,
    )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("geobrix_vector_connector_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.dataSource.register(GeoBrixVectorDataSource)
    yield session
    session.stop()


@pytest.fixture
def root(tmp_path):
    """A Volume-like root: a multi-file GeoJSON dataset folder plus a GeoPackage."""
    for src in (BOROUGHS, GPKG):
        if not src.exists():
            pytest.skip(f"sample data missing: {src}")
    (tmp_path / "nyc_boroughs").mkdir()
    shutil.copy(BOROUGHS, tmp_path / "nyc_boroughs" / "part_a.geojson")
    shutil.copy(GPKG, tmp_path / "nyc_complete.gpkg")
    (tmp_path / "_staging").mkdir()  # hidden by convention: never a table
    return tmp_path


def set_mtime(path, mtime_ns):
    os.utime(path, ns=(mtime_ns, mtime_ns))
