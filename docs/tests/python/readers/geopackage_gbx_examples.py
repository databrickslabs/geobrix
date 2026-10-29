"""Executable doc example for the lightweight gpkg_gbx reader (Docker)."""

from path_config import SAMPLE_DATA_BASE

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"

READ_GPKG_GBX = """# Lightweight GeoPackage reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("gpkg_gbx").load(SAMPLE)   # (attrs..., <geom>, <geom>_srid, <geom>_srid_proj)
df.show()"""


def read_gpkg_gbx(spark, path=None):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = spark.read.format("gpkg_gbx").load(path or SAMPLE)
    # gpkg geometry column name matches the source layer column (e.g. shape/SHAPE)
    srid_cols = [c for c in df.columns if c.endswith("_srid") and not c.endswith("_srid_proj")]
    assert len(srid_cols) > 0, f"Expected a *_srid column, got: {df.columns}"
    assert df.count() > 0


REGEXP_GPKG = """# Rows whose name matches a regular expression, read from a GeoPackage
import pyogrio

df = pyogrio.read_dataframe(
    SAMPLE,
    sql="SELECT boroname FROM boroughs WHERE boroname REGEXP '^M.*n$' ORDER BY boroname",
    sql_dialect="INDIRECT_SQLITE",  # a GeoPackage's own SQLite has no REGEXP
)
print(list(df["boroname"]))"""

REGEXP_GPKG_output = "['Manhattan']"


def regexp_gpkg(path=None):
    """REGEXP (GDAL's SQLite dialect, PCRE2) on a GeoPackage, through INDIRECT_SQLITE."""
    import pyogrio

    return pyogrio.read_dataframe(
        path or SAMPLE,
        sql="SELECT boroname FROM boroughs WHERE boroname REGEXP '^M.*n$' ORDER BY boroname",
        sql_dialect="INDIRECT_SQLITE",
    )
