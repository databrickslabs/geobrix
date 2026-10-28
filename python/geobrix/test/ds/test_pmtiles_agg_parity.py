"""Cross-tier decoded parity for gbx_pmtiles_agg (light vs heavy). JAR-gated."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Two MVT-ish payloads + one POLYGON-ish payload across two zooms.
_TILES = [
    ("g", b"tile-point-0\x00", 3, 2, 4),
    ("g", b"tile-point-1\x00", 3, 5, 6),
    ("g", b"tile-polygon-\x07\x08\x09", 2, 1, 1),
]


def _decode(path):
    from databricks.labs.gbx.pmtiles._directory import iter_tiles

    return dict(iter_tiles(Path(path).read_bytes()))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pmtiles-agg-parity")
        .config("spark.jars", str(_JARS[0]))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def _archive(spark, register_fn, tmp_path, name):
    register_fn(spark)
    from databricks.labs.gbx.pmtiles import functions as pt

    df = spark.createDataFrame(_TILES, ["g", "tile", "z", "x", "y"])
    blob = (
        df.groupBy("g")
        .agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
        .collect()[0]["arc"]
    )
    p = tmp_path / f"{name}.pmtiles"
    p.write_bytes(blob)
    return _decode(p)


def test_decoded_tile_parity(spark_with_jar, tmp_path):
    from databricks.labs.gbx.pmtiles import functions as heavy_pt
    from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

    light_tiles = _archive(spark_with_jar, register_pmtiles_agg, tmp_path, "light")
    heavy_tiles = _archive(spark_with_jar, heavy_pt.register, tmp_path, "heavy")
    assert light_tiles == heavy_tiles
