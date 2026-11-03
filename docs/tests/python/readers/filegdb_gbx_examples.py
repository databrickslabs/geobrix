"""Executable doc example for the lightweight file_gdb_gbx reader (Docker)."""

from path_config import SAMPLE_DATA_BASE

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"

READ_FILEGDB_GBX = """# Lightweight File Geodatabase reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("file_gdb_gbx").load(SAMPLE)   # (attrs..., <geom>, <geom>_srid, <geom>_srid_proj)
df.show()"""


def read_filegdb_gbx(spark, path=None):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    df = spark.read.format("file_gdb_gbx").load(path or SAMPLE)
    # FileGDB geometry column name matches the source layer column (e.g. SHAPE)
    srid_cols = [c for c in df.columns if c.endswith("_srid") and not c.endswith("_srid_proj")]
    assert len(srid_cols) > 0, f"Expected a *_srid column, got: {df.columns}"
    assert df.count() > 0
