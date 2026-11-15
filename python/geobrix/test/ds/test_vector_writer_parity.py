"""Light vector writer round-trip (Docker / integration).

read(<fmt>_gbx) -> write(<fmt>_gbx) -> read(<fmt>_gbx) is feature-count and
geometry stable against the real corpus. Writer is light-only (heavy has no
vector writer); this is the writer's correctness gate. Skips unless sample
data is mounted."""

import os

import pytest

pytestmark = pytest.mark.integration

SAMPLE = (
    os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data").rstrip(
        "/"
    )
    + "/geobrix-examples"
)

_CASES = [
    ("geojson_gbx", f"{SAMPLE}/nyc/boroughs/nyc_boroughs.geojson", "rt.geojson"),
    ("gpkg_gbx", f"{SAMPLE}/nyc/geopackage/nyc_complete.gpkg", "rt.gpkg"),
    ("shapefile_gbx", f"{SAMPLE}/nyc/subway/nyc_subway.shp.zip", "rt.shp"),
]


@pytest.fixture(scope="module")
def spark():
    import logging

    from pyspark.sql import SparkSession

    from databricks.labs.gbx import manager as gbx_manager

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-vector-writer-parity")
        .getOrCreate()
    )

    gbx_manager.register_sources(s)
    yield s


@pytest.mark.parametrize("fmt,src,target", _CASES)
def test_vector_writer_roundtrip(spark, tmp_path, fmt, src, target):
    if not os.path.exists(src):
        pytest.skip(f"sample not mounted: {src}")
    src_df = spark.read.format(fmt).load(src)
    n = src_df.count()
    out = str(tmp_path / target)
    src_df.coalesce(1).write.format(fmt).mode("overwrite").save(out)
    back = spark.read.format(fmt).load(out)
    assert back.count() == n
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert back.where(f"{gcol} is not null").count() == n
