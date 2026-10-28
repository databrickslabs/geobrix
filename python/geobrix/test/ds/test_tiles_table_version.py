"""versionAsOf / timestampAsOf: the raster readers plan from a pinned version of a tilesTable.

The option parsing needs no Spark; the read of a pinned version needs a Delta table, so it
runs on a local Delta session (the recipe of test/zarr/conftest.py's ``spark_delta``).
"""

import uuid

import pytest

from databricks.labs.gbx.ds.raster import RasterGbxReader

_DELTA_EXT = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

_COLS = ["path", "col_off", "row_off", "width", "height"]


def _tile(name):
    return (f"/data/{name}.tif", 0, 0, 4, 3)


@pytest.fixture(scope="module")
def spark_delta(tmp_path_factory):
    """Local Spark with OSS Delta; the jars come from PYSPARK_SUBMIT_ARGS (test/conftest.py)."""
    import logging

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and _DELTA_EXT not in (
        active.conf.get("spark.sql.extensions", "") or ""
    ):
        active.stop()
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("gbx-ds-tiles-table-version")
            .config("spark.sql.extensions", _DELTA_EXT)
            .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("delta_wh")))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.delta.logStore.class", "io.delta.storage.LocalLogStore")
            .getOrCreate()
        )
    except Exception as e:  # the JVM could not load the Delta classes
        raise RuntimeError(
            "Could not start a Delta-enabled Spark session; the delta-spark jars are "
            "not on the JVM classpath. Fix: run the test through "
            "scripts/commands/gbx-test-pyrx.sh (it fetches the jars), or set "
            "PYSPARK_SUBMIT_ARGS to include them before pytest starts."
        ) from e
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
