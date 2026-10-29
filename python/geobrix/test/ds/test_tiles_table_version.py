"""A tilesTable (tile index) on a Delta table: a pinned version, and how it is read.

The option parsing needs no Spark; reading a table needs a Delta session (the recipe of
test/zarr/conftest.py). ``spark.read.format("raster_gbx").option("tilesTable", ...)`` cannot
read the table (its planner has no session); ``read_tiles_table`` does, and returns what the
reader returns for the same index.
"""

import uuid

import numpy as np
import pytest
import rasterio
from pyspark.sql.datasource import CaseInsensitiveDict
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource, RasterGbxReader

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


# --- reading the table: spark.read cannot, read_tiles_table can ---------------------


@pytest.mark.delta
def test_spark_read_of_a_tiles_table_names_the_helper(spark_delta, tiles_table):
    """The real path: the planner worker has no session to read the table with."""
    spark_delta.dataSource.register(RasterGbxDataSource)
    reader = (
        spark_delta.read.format("raster_gbx")
        .option("tilesTable", tiles_table)
        .option("virtualTiles", "true")
    )
    with pytest.raises(Exception, match="read_tiles_table") as raised:
        reader.load("/").collect()
    assert "Fix:" in str(raised.value)


def _tif(path, width, height):
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)
    return str(path)


@pytest.fixture
def windowed_index(spark_delta, tmp_path):
    """A Delta tile index over real files, with `window` structs: v0 = a (2 tiles), v1 adds b."""
    a, b = _tif(tmp_path / "a.tif", 8, 6), _tif(tmp_path / "b.tif", 4, 3)
    name = f"gbx_tiles_read_{uuid.uuid4().hex[:8]}"
    ddl = "path string, window struct<col_off:int,row_off:int,width:int,height:int>"
    commits = [[(a, (0, 0, 4, 6)), (a, (4, 0, 4, 6))], [(b, (0, 0, 4, 3))]]
    for i, rows in enumerate(commits):
        spark_delta.createDataFrame(rows, ddl).write.format("delta").mode(
            "append" if i else "overwrite"
        ).saveAsTable(name)
    yield name
    spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


def _direct(options):
    """What the reader returns for the index with a session in hand (the old direct use).

    Options are case-insensitive, as Spark hands them to a reader.
    """
    reader = RasterGbxReader(
        CaseInsensitiveDict({"path": "/", "virtualTiles": "true", **options})
    )
    return [
        (src, tile) for part in reader.partitions() for src, tile in reader.read(part)
    ]


@pytest.mark.delta
@pytest.mark.parametrize(
    "fmt, options, num_partitions, tiles",
    [
        ("raster_gbx", {}, None, 3),  # the latest version
        ("raster_gbx", {"versionAsOf": 0}, 2, 2),  # a pinned version (0 is one), spread
        ("raster_gbx", {"skipOrdering": "true"}, 2, 3),  # no order inside a task
        (
            "gtiff_gbx",
            {"virtualtiles": "false"},
            None,
            3,
        ),  # named reader, key in any case
    ],
)
def test_read_tiles_table_equals_the_reader(
    spark_delta, windowed_index, fmt, options, num_partitions, tiles
):
    from databricks.labs.gbx.ds.raster import read_tiles_table
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    fields = V2_TILE_SCHEMA.fieldNames()

    def keyed(pairs):  # (source, tile tuple) -> {(source, window): tile tuple}
        return {
            (src, tuple(tile[fields.index("window")].values())): tile
            for src, tile in pairs
        }

    expected = keyed(_direct({"tilesTable": windowed_index, **options}))
    df = read_tiles_table(
        spark_delta, windowed_index, options, format=fmt, num_partitions=num_partitions
    )

    assert [f.name for f in df.schema.fields] == ["source", "tile"]
    rows = df.collect()
    got = keyed(
        (r["source"], tuple(r["tile"].asDict(recursive=True)[n] for n in fields))
        for r in rows
    )
    assert len(rows) == tiles == len(got)
    assert got == expected


@pytest.mark.delta
def test_read_tiles_table_spreads_the_tiles_of_one_source(spark_delta, tmp_path):
    """A one-file index of many tiles is read by many tasks, as the reader does."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.ds.raster import read_tiles_table

    src = _tif(tmp_path / "one.tif", 64, 64)
    name = f"gbx_tiles_spread_{uuid.uuid4().hex[:8]}"
    ddl = "path string, col_off int, row_off int, width int, height int"
    rows = [(src, c, r, 16, 16) for c in range(0, 64, 16) for r in range(0, 64, 16)]
    spark_delta.createDataFrame(rows, ddl).write.format("delta").saveAsTable(name)
    try:
        df = read_tiles_table(spark_delta, name)
        by_task = df.groupBy(F.spark_partition_id().alias("task")).count().collect()
        tasks = df.rdd.getNumPartitions()
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")
    assert sum(r["count"] for r in by_task) == len(rows) == 16
    assert len(by_task) > 1
    # two tasks per index row by default, so fewer tiles share a task than with one per row
    assert tasks == 2 * len(rows)


@pytest.mark.parametrize("fmt", ["gdal", "ogr", "png_gbx", "", None])
def test_read_tiles_table_refuses_a_format_that_reads_no_tile_index(fmt):
    """Only the raster_gbx reader and the two that extend it take a tile index; the error names
    the three and says what to pass. It is raised before the table is touched: no session needed.
    """
    from databricks.labs.gbx.ds.raster import read_tiles_table

    with pytest.raises(
        ValueError, match=r"raster_gbx, gtiff_gbx, cog_gbx.*Fix: pass one of"
    ):
        read_tiles_table(None, "any_table", format=fmt)


@pytest.mark.delta
def test_read_tiles_table_refuses_an_index_with_no_path_column(spark_delta):
    """A table with no ``path`` column is not a tile index: the error lists the columns it has."""
    from databricks.labs.gbx.ds.raster import read_tiles_table

    name = f"gbx_tiles_nopath_{uuid.uuid4().hex[:8]}"
    spark_delta.createDataFrame([(1, 2)], ["file", "col_off"]).write.format(
        "delta"
    ).saveAsTable(name)
    try:
        with pytest.raises(
            ValueError, match=r"no 'path' column .*'file'.*Fix: read a table"
        ):
            read_tiles_table(spark_delta, name)
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")
