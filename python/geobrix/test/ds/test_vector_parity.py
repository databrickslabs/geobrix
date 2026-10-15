"""Light vs heavy vector reader parity (Docker / integration).

Same source -> light *_gbx vs heavy *_ogr produce the same schema (columns +
types), row count, and decoded geometries. Skips unless the geobrix JAR is staged
+ sample data is mounted."""

import logging
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))
SAMPLE = (
    os.environ.get(
        "GBX_SAMPLE_DATA_ROOT",
        "/Volumes/main/default/test-data",
    ).rstrip("/")
    + "/geobrix-examples"
)

# (light, heavy, path, heavy options). geojson_ogr defaults to GeoJSONSeq (multi=true);
# a standard FeatureCollection is read with multi=false, as the reader docs prescribe.
_CASES = [
    (
        "geojson_gbx",
        "geojson_ogr",
        f"{SAMPLE}/nyc/boroughs/nyc_boroughs.geojson",
        {"multi": "false"},
    ),
    ("shapefile_gbx", "shapefile_ogr", f"{SAMPLE}/nyc/subway/nyc_subway.shp.zip", {}),
    ("gpkg_gbx", "gpkg_ogr", f"{SAMPLE}/nyc/geopackage/nyc_complete.gpkg", {}),
    ("file_gdb_gbx", "file_gdb_ogr", f"{SAMPLE}/nyc/filegdb/NYC_Sample.gdb.zip", {}),
]


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-vector-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.register import register

    register(s)
    yield s


@pytest.mark.parametrize("light_fmt,heavy_fmt,path,heavy_opts", _CASES)
def test_vector_reader_parity(spark_with_jar, light_fmt, heavy_fmt, path, heavy_opts):
    if not os.path.exists(path):
        pytest.skip(f"sample not mounted: {path}")
    spark = spark_with_jar
    light = spark.read.format(light_fmt).load(path)
    heavy = spark.read.format(heavy_fmt).options(**heavy_opts).load(path)
    assert [(f.name, f.dataType.simpleString()) for f in light.schema.fields] == [
        (f.name, f.dataType.simpleString()) for f in heavy.schema.fields
    ]
    assert light.count() == heavy.count()
    # The geometry column is geom_0 by default but takes the source's OGR geom-field
    # name when present (e.g. SHAPE for GPKG/FileGDB) — both tiers do this, so derive
    # it from the schema (the column X with a matching X_srid) rather than hardcoding.
    srid_cols = [f.name for f in light.schema.fields if f.name.endswith("_srid")]
    geom_col = srid_cols[0][: -len("_srid")]
    lg = {bytes(r[geom_col]) for r in light.select(geom_col).collect()}
    hg = {bytes(r[geom_col]) for r in heavy.select(geom_col).collect()}
    assert lg == hg


_CODED_COLS = ("asc", "uns", "neg", "scode", "plain", "rng")


@pytest.mark.parametrize(
    "light_fmt,heavy_fmt,fixture",
    [
        ("file_gdb_gbx", "file_gdb_ogr", "gdb_with_coded_domains"),
        ("gpkg_gbx", "gpkg_ogr", "gpkg_with_coded_domains"),
    ],
)
def test_vector_coded_domain_parity(
    spark_with_jar, request, tmp_path, light_fmt, heavy_fmt, fixture
):
    """Coded-value domain fields: the light default returns the same stored codes
    as the heavy reader, row for row (the heavy tier reads zipped FileGDBs)."""
    from databricks.labs.gbx.ds import vector as vec

    path = request.getfixturevalue(fixture)
    if path.endswith(".gdb"):
        zpath = str(tmp_path / "coded.gdb.zip")
        vec._zip_gdb_bundle(path, zpath)
        path = zpath
    spark = spark_with_jar
    light = spark.read.format(light_fmt).load(path)
    heavy = spark.read.format(heavy_fmt).load(path)
    lrows = [tuple(r) for r in light.select(*_CODED_COLS).collect()]
    hrows = [tuple(r) for r in heavy.select(*_CODED_COLS).collect()]
    assert lrows == hrows
    assert _coded_types(light) == _coded_types(heavy)


def _coded_types(df):
    return [(c, df.schema[c].dataType.simpleString()) for c in _CODED_COLS]
