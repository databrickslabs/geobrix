"""Executable doc examples for the lightweight pmtiles_gbx writer (run in Docker)."""

import json
import os
import tempfile

from pmtiles.reader import MmapSource, Reader

PNG = b"\x89PNG\r\n\x1a\n"

WRITE_PMTILES_SHARDED = """# Lightweight PMTiles writer - distributed spatial sharding (default).
# Input is a tile pyramid: (z, x, y, bytes). shardZoom=6 emits one
# tileset/{z}/{x}/{y}.pmtiles per populated parent + overview.pmtiles + a
# STAC catalog.json.
from databricks.labs.gbx.ds.register import register
register(spark)
df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "6").save(OUT_DIR)"""

WRITE_PMTILES_SINGLE = """# Single-archive PMTiles: shardZoom=0 packs every tile into one .pmtiles file.
from databricks.labs.gbx.ds.register import register
register(spark)
df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "0").save(OUT_FILE)"""

OPTIONS_NOTE = """# Knobs (sensible defaults):
#   shardZoom            6  -> sharded; 0 -> single archive
#   targetTilesPerShard  adaptive sharding (subdivide dense cells)
#   catalog              stac (default) | tilejson | none
#   tileType             auto-sniff (png/jpeg/webp/mvt); override if needed
#   tileCompression      none (default) | gzip | brotli | zstd
#   metadata             JSON string -> archive metadata"""


def _pyramid_df(spark, tiles):
    rows = [(z, x, y, bytearray(PNG + bytes([i]))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(rows, schema="z int, x int, y int, bytes binary")


def write_pmtiles_single(spark):
    """Verify WRITE_PMTILES_SINGLE: write one archive, read tiles back."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    df = _pyramid_df(spark, [(2, 1, 1), (2, 2, 1), (3, 4, 3)])
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "world.pmtiles")
        df.write.format("pmtiles_gbx").mode("overwrite").option(
            "shardZoom", "0"
        ).save(out)
        with open(out, "rb") as f:
            r = Reader(MmapSource(f))
            assert r.get(2, 1, 1) is not None
            assert r.get(3, 4, 3) is not None


def write_pmtiles_sharded(spark):
    """Verify WRITE_PMTILES_SHARDED: sharded output + overview + STAC catalog."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    df = _pyramid_df(spark, [(6, 32, 21), (8, 130, 85), (3, 4, 2)])
    with tempfile.TemporaryDirectory() as d:
        df.write.format("pmtiles_gbx").mode("overwrite").option(
            "shardZoom", "6"
        ).save(d)
        tileset = os.path.join(d, "tileset")
        assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))
        assert os.path.isfile(os.path.join(tileset, "overview.pmtiles"))
        cat = json.load(open(os.path.join(tileset, "catalog.json")))
        assert cat["type"] == "FeatureCollection" and cat["features"]
