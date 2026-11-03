"""Executable doc example for the lightweight shapefile_gbx reader (Docker)."""

from path_config import SAMPLE_DATA_BASE

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"

READ_SHAPEFILE_GBX = """# Lightweight Shapefile reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("shapefile_gbx").load(SAMPLE)   # (attrs..., geom_0, geom_0_srid, geom_0_srid_proj)
df.show()"""


def read_shapefile_gbx(spark, path=None):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    df = spark.read.format("shapefile_gbx").load(path or SAMPLE)
    assert "geom_0" in df.columns and "geom_0_srid" in df.columns
    assert df.count() > 0
