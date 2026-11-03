"""Executable doc examples for the lightweight vector writers (Docker).

Each function is a self-contained round-trip: read source → write via *_gbx →
read back → assert feature-count parity. Used as the single source of truth for
the per-format writer pages in docs/docs/writers/.

No ``coalesce(1)`` is needed: the writer is two-phase (each partition emits one
Arrow fragment in parallel; the driver merges all fragments into a single output
file on commit), so a single-file output is produced regardless of partition count.
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

# ---------------------------------------------------------------------------
# Snippet strings shown in the MDX pages (CodeFromTest functionName=...)
# ---------------------------------------------------------------------------

WRITE_VECTOR_GBX = """# Generic lightweight vector writer (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
df = spark.read.format("geojson_gbx").load(src)
out = "/tmp/nyc_boroughs.geojson"  # a single named output file
(df.write.format("vector_gbx")
   .option("driverName", "GeoJSON")
   .mode("overwrite").save(out))
back = spark.read.format("vector_gbx").option("driverName", "GeoJSON").load(out)
assert back.count() == df.count()"""

WRITE_SHAPEFILE_GBX = """# Lightweight Shapefile writer (pyogrio; OGR driver preset to "ESRI Shapefile")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
df = spark.read.format("shapefile_gbx").load(src)
out = "/tmp/nyc_subway.shp"  # a single named output file (.shp + sidecars)
df.write.format("shapefile_gbx").mode("overwrite").save(out)
back = spark.read.format("shapefile_gbx").load(out)
assert back.count() == df.count()"""

WRITE_GEOJSON_GBX = """# Lightweight GeoJSON writer (pyogrio; OGR driver preset to "GeoJSON")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
df = spark.read.format("geojson_gbx").load(src)
out = "/tmp/nyc_boroughs.geojson"  # a single named output file
df.write.format("geojson_gbx").mode("overwrite").save(out)
back = spark.read.format("geojson_gbx").load(out)
assert back.count() == df.count()"""

WRITE_GPKG_GBX = """# Lightweight GeoPackage writer (pyogrio; OGR driver preset to "GPKG")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
df = spark.read.format("gpkg_gbx").load(src)
out = "/tmp/nyc_complete.gpkg"  # a single named output file
df.write.format("gpkg_gbx").mode("overwrite").save(out)
back = spark.read.format("gpkg_gbx").load(out)
assert back.count() == df.count()"""

WRITE_FILE_GDB_GBX = """# Lightweight GeoDatabase writer (pyogrio; OGR driver preset to "OpenFileGDB")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"
df = spark.read.format("file_gdb_gbx").load(src)
out = "/tmp/NYC_Sample.gdb"  # FileGDB output path must end in .gdb
df.write.format("file_gdb_gbx").mode("overwrite").save(out)
back = spark.read.format("file_gdb_gbx").load(out)
assert back.count() == df.count()"""


# ---------------------------------------------------------------------------
# Executable round-trip functions (called by the test file)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)


def write_vector_gbx(spark):
    """WRITE_VECTOR_GBX: generic vector_gbx round-trip (GeoJSON driver)."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
    df = spark.read.format("geojson_gbx").load(src)
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "nyc_boroughs.geojson")
        (
            df.write.format("vector_gbx")
            .option("driverName", "GeoJSON")
            .mode("overwrite")
            .save(out)
        )
        back = spark.read.format("vector_gbx").option("driverName", "GeoJSON").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
        return out


def write_shapefile_gbx(spark):
    """WRITE_SHAPEFILE_GBX: shapefile_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
    df = spark.read.format("shapefile_gbx").load(src)
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "nyc_subway.shp")
        df.write.format("shapefile_gbx").mode("overwrite").save(out)
        back = spark.read.format("shapefile_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
        return out


def write_geojson_gbx(spark):
    """WRITE_GEOJSON_GBX: geojson_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
    df = spark.read.format("geojson_gbx").load(src)
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "nyc_boroughs.geojson")
        df.write.format("geojson_gbx").mode("overwrite").save(out)
        back = spark.read.format("geojson_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
        return out


def write_gpkg_gbx(spark):
    """WRITE_GPKG_GBX: gpkg_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
    df = spark.read.format("gpkg_gbx").load(src)
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "nyc_complete.gpkg")
        df.write.format("gpkg_gbx").mode("overwrite").save(out)
        back = spark.read.format("gpkg_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
        return out


def write_file_gdb_gbx(spark):
    """WRITE_FILE_GDB_GBX: file_gdb_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"
    df = spark.read.format("file_gdb_gbx").load(src)
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "NYC_Sample.gdb")  # FileGDB output path must end in .gdb
        df.write.format("file_gdb_gbx").mode("overwrite").save(out)
        back = spark.read.format("file_gdb_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
        return out
