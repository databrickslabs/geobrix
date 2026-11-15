import json
import os

from pmtiles.reader import MmapSource, Reader

PNG = b"\x89PNG\r\n\x1a\n"


def _png(tag: int) -> bytes:
    return PNG + bytes([tag])


def _rows(spark, tiles):
    data = [(z, x, y, bytearray(_png(i))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(data, schema="z int, x int, y int, bytes binary")


def _read_tile(path, z, x, y):
    with open(path, "rb") as f:
        return Reader(MmapSource(f)).get(z, x, y)


def test_single_archive(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    out = str(tmp_path / "world.pmtiles")
    tiles = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(out)
    assert os.path.isfile(out)
    assert _read_tile(out, 6, 32, 21) is not None
    assert _read_tile(out, 7, 64, 42) is not None


def test_sharded_with_overview_and_catalog(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    out = str(tmp_path / "tileset_out")
    tiles = [(6, 32, 21), (8, 130, 85), (3, 4, 2)]  # body, body(same parent), overview
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").save(out)

    tileset = os.path.join(out, "tileset")
    assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))
    assert os.path.isfile(os.path.join(tileset, "overview.pmtiles"))
    catalog = json.load(open(os.path.join(tileset, "catalog.json")))
    assert catalog["type"] == "FeatureCollection"
    # body tile reads back from its shard
    assert (
        _read_tile(os.path.join(tileset, "6", "32", "21.pmtiles"), 6, 32, 21)
        is not None
    )
    # overview tile reads back from overview archive
    assert _read_tile(os.path.join(tileset, "overview.pmtiles"), 3, 4, 2) is not None
    # scratch cleaned up
    assert not os.path.isdir(os.path.join(out, "_scratch"))


def test_adaptive_sharding_option(spark, tmp_path):
    """targetTilesPerShard (camelCase option) must reach the writer despite
    PySpark lowercasing option keys, and subdivide dense cells to deeper shards."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    out = str(tmp_path / "adaptive_out")
    # 4 z8 tiles under one z6 parent (6,32,21), in distinct z7 children.
    tiles = [(8, 128, 84), (8, 129, 84), (8, 130, 86), (8, 131, 86)]
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "6"
    ).option("targetTilesPerShard", "2").save(out)

    tileset = os.path.join(out, "tileset")
    # at least one shard deeper than the base shardZoom of 6 (dense -> subdivided)
    deep = []
    for root, _dirs, files in os.walk(tileset):
        for f in files:
            if f.endswith(".pmtiles"):
                # rel path is {z}/{x}/{y}.pmtiles
                rel = os.path.relpath(os.path.join(root, f), tileset)
                z = rel.split(os.sep)[0]
                if z.isdigit():
                    deep.append(int(z))
    assert any(
        z > 6 for z in deep
    ), f"expected a shard deeper than z6; got zooms {deep}"


def test_append_mode_rejected(spark, tmp_path):
    """Appending to an existing pmtiles output must raise an error.

    Write once to establish the output at the resolved path, then try a second
    write with mode('append') -- the writer detects the existing target and rejects.
    The stem path 'appendme' resolves to 'appendme.pmtiles' (single-archive).
    """
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    import pytest

    stem = str(tmp_path / "appendme")
    resolved = stem + ".pmtiles"  # single-archive; shardZoom=0 -> <stem>.pmtiles
    _rows(spark, [(6, 32, 21)]).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(stem)
    assert os.path.isfile(resolved)

    with pytest.raises(Exception):
        _rows(spark, [(6, 32, 21)]).write.format("pmtiles_gbx").mode("append").option(
            "shardZoom", "0"
        ).save(stem)
