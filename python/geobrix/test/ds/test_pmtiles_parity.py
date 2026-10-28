"""Light vs heavy PMTiles parity (Docker / integration).

`pmtiles_gbx` (light) and the heavy Scala `pmtiles` writer take the same input
`(z,x,y,bytes)` and must produce archives that decode to the same z/x/y->bytes
set (decoded-tile parity, not byte-identical). Heavy needs the geobrix JAR +
GDAL, so this SKIPS locally and runs in Docker / on cluster.

Run: bash scripts/commands/gbx-test-python.sh \
    --path python/geobrix/test/ds/test_pmtiles_parity.py \
    --with-integration --log pmtiles-parity.log
"""

import logging
import os
from pathlib import Path

import pytest
from pmtiles import tile as _pmtile
from pmtiles.tile import Compression

pytestmark = pytest.mark.integration

PNG = b"\x89PNG\r\n\x1a\n"

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-pmtiles-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.register import register

    register(session)
    yield session


def _decode_archive(path):
    from databricks.labs.gbx.ds._pmtiles_read import iter_tiles

    return dict(iter_tiles(Path(path).read_bytes()))


def _decode_any(path):
    """Decode every .pmtiles under `path` (file or directory) into one dict."""
    merged = {}
    if os.path.isfile(path):
        merged.update(_decode_archive(path))
        return merged
    for root, _dirs, files in os.walk(path):
        for f in files:
            if f.endswith(".pmtiles"):
                merged.update(_decode_archive(os.path.join(root, f)))
    return merged


def test_light_vs_heavy_single_archive(spark_with_jar, tmp_path):
    spark = spark_with_jar
    tiles = [(2, 1, 1), (2, 2, 1), (3, 4, 3)]
    rows = [(z, x, y, bytearray(PNG + bytes([i]))) for i, (z, x, y) in enumerate(tiles)]
    df = spark.createDataFrame(rows, schema="z int, x int, y int, bytes binary")

    light_out = str(tmp_path / "light.pmtiles")
    df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "0").save(
        light_out
    )

    # The heavy writer names a file-like target itself: a path that is not an existing
    # directory is the archive (".pmtiles" completed), not a directory to fill. Name the
    # archive, as for the light writer, so the decoder reads the file the writer wrote.
    heavy_out = str(tmp_path / "heavy.pmtiles")
    df.write.format("pmtiles").mode("overwrite").save(heavy_out)

    light = _decode_any(light_out)
    heavy = _decode_any(heavy_out)
    assert light, "light produced no tiles"
    assert heavy, "heavy produced no tiles"
    assert set(light.keys()) == set(heavy.keys()), (
        f"tile-key mismatch: light-only={set(light) - set(heavy)}, "
        f"heavy-only={set(heavy) - set(light)}"
    )
    assert light == heavy, "decoded tile bytes differ between light and heavy"


def test_heavy_written_archive_reads_back_through_pmtiles_gbx(spark_with_jar, tmp_path):
    """An archive the heavy writer produced (uncompressed directories) reads back
    through the light `pmtiles_gbx` reader with every tile written, bytes intact."""
    spark = spark_with_jar
    tiles = [(2, 1, 1), (2, 2, 1), (3, 4, 3)]
    written = {t: PNG + bytes([i]) for i, t in enumerate(tiles)}
    df = spark.createDataFrame(
        [(z, x, y, bytearray(b)) for (z, x, y), b in written.items()],
        schema="z int, x int, y int, bytes binary",
    )
    heavy_out = str(tmp_path / "heavy.pmtiles")
    df.write.format("pmtiles").mode("overwrite").save(heavy_out)

    with open(heavy_out, "rb") as f:
        header = _pmtile.deserialize_header(f.read(127))
    assert header["internal_compression"] == Compression.NONE  # else vacuous

    back = (
        spark.read.format("pmtiles_gbx")
        .option("source", "archive")
        .option("path", heavy_out)
        .load()
        .collect()
    )
    assert len(back) == len(written)
    assert {(r["z"], r["x"], r["y"]): bytes(r["bytes"]) for r in back} == written
