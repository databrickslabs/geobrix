"""versionAsOf / timestampAsOf: the raster readers plan from a pinned version of a tilesTable.

The option parsing needs no Spark; the read of a pinned version needs a Delta table, so it
runs on a local Delta session (the recipe of test/zarr/conftest.py).
"""

import uuid

import pytest

from databricks.labs.gbx.ds.raster import RasterGbxReader

from ..zarr.conftest import delta_session

_COLS = ["path", "col_off", "row_off", "width", "height"]


def _tile(name):
    return (f"/data/{name}.tif", 0, 0, 4, 3)


@pytest.fixture(scope="module")
def spark_delta(tmp_path_factory):
    """Module scope: the Delta session ends with this file, later ds modules get a plain one."""
    session = delta_session(tmp_path_factory.mktemp("delta_wh"))
    yield session
    session.stop()


@pytest.fixture
def tiles_table(spark_delta):
    """A Delta tilesTable with three commits: v0 has a, v1 adds b, v2 adds c."""
    name = f"gbx_tiles_version_{uuid.uuid4().hex[:8]}"
    for tile in ("a", "b", "c"):
        mode = "overwrite" if tile == "a" else "append"
        spark_delta.createDataFrame([_tile(tile)], _COLS).write.format("delta").mode(
            mode
        ).saveAsTable(name)
    yield name
    spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


def _planned(options):
    parts = RasterGbxReader({"path": "/data", "virtualTiles": "true", **options})
    return [p.file_path for p in parts.partitions()]


@pytest.mark.delta
def test_tiles_table_version_as_of(spark_delta, tiles_table):
    stamp_v1 = (
        spark_delta.sql(f"DESCRIBE HISTORY {tiles_table}")
        .where("version = 1")
        .collect()[0]["timestamp"]
        .isoformat(sep=" ", timespec="milliseconds")
    )
    base = {"tilesTable": tiles_table}
    a, b, c = (f"/data/{t}.tif" for t in "abc")

    assert _planned(base) == [a, b, c]  # no option: the latest version
    assert _planned({**base, "versionAsOf": "0"}) == [a]
    assert _planned({**base, "versionAsOf": 0}) == [
        a
    ]  # a programmatic int 0 is a version
    assert _planned({**base, "versionAsOf": "1"}) == [a, b]
    assert _planned({**base, "versionasof": "1"}) == [a, b]  # Spark lowercases keys
    assert _planned({**base, "timestampAsOf": stamp_v1}) == [a, b]


def test_both_options_rejected():
    with pytest.raises(ValueError, match="mutually exclusive"):
        RasterGbxReader(
            {
                "path": "/data",
                "tilesTable": "t",
                "versionAsOf": "0",
                "timestampAsOf": "2024-01-01 00:00:00",
            }
        )


@pytest.mark.parametrize(
    "options",
    [
        {"versionAsOf": "0"},
        {"timestampAsOf": "2024-01-01 00:00:00"},
        {"versionAsOf": "0", "manifest": "/data/tiles.json"},
    ],
)
def test_option_requires_tiles_table(options):
    with pytest.raises(ValueError, match="requires 'tilesTable'"):
        RasterGbxReader({"path": "/data", **options})


@pytest.mark.parametrize("value", ["latest", "-1", "1.5"])
def test_version_as_of_must_be_a_non_negative_integer(value):
    with pytest.raises(ValueError, match="non-negative integer"):
        RasterGbxReader({"path": "/data", "tilesTable": "t", "versionAsOf": value})
