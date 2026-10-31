"""Zarr reader examples -- single source of truth.

Code shown on docs/docs/readers/zarr.mdx is imported from here, and every function runs for
real in test_zarr_read_examples.py on the sample ERA5-like cube (a small synthetic weather
cube: ``time`` 48 hourly steps x ``latitude`` 181 x ``longitude`` 360, variables ``t2m``,
``u10``, ``v10`` and ``msl``) or on a small store the test writes. The page shows each
function's body: the ``def`` line, the docstring and the closing ``return`` are not shown.

Names that stand for what is yours when you run an example:

- ``spark``: the Spark session (a notebook has one);
- ``cube``: the path of a Zarr store (``SETUP``);
- ``table``: the Unity Catalog name of a managed or registered store's manifest table.

The ``*_output`` constants are what the examples print. The tests compare them with a real
run, so they cannot drift.
"""

CUBE_SUBPATH = "global/era5-weather/era5_like_v3.zarr"

SETUP = """# A Zarr store is a path; a managed or registered store is the name of its manifest table.
cube = "/Volumes/main/geobrix_samples/geobrix-examples/global/era5-weather/era5_like_v3.zarr"
table = "main.weather.era5"
"""

# What the examples print. The tests compare these with a real run.
READ_BLOCKS_output = """
+--------+---------------------------+------------+-------------+------------+
|variable|dims                       |start       |stop         |shape       |
+--------+---------------------------+------------+-------------+------------+
|t2m     |[time, latitude, longitude]|[0, 0, 0]   |[3, 91, 180] |[3, 91, 180]|
|t2m     |[time, latitude, longitude]|[0, 0, 180] |[3, 91, 360] |[3, 91, 180]|
|t2m     |[time, latitude, longitude]|[0, 91, 0]  |[3, 181, 180]|[3, 90, 180]|
|t2m     |[time, latitude, longitude]|[0, 91, 180]|[3, 181, 360]|[3, 90, 180]|
+--------+---------------------------+------------+-------------+------------+

(3, 91, 180) float32
"""
SELECT_output = """
by label: 363 cells
by position: 363 cells
"""
READ_A_TABLE_output = """
root
 |-- time: timestamp (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- t2m: float (nullable = true)
 |-- msl: float (nullable = true)

+-------------------+--------+---------+-----+--------+
|               time|latitude|longitude|  t2m|     msl|
+-------------------+--------+---------+-----+--------+
|2023-01-01 00:00:00|    40.0|      0.0|299.5|100580.0|
|2023-01-01 00:00:00|    40.0|      1.0|299.5|100570.0|
|2023-01-01 00:00:00|    40.0|      2.0|299.5|100580.0|
+-------------------+--------+---------+-----+--------+
only showing top 3 rows
"""
READ_MISSING_DATA_output = """
root
 |-- day: timestamp (nullable = true)
 |-- station: long (nullable = true)
 |-- rain: short (nullable = true)

+-------------------+-------+----+
|                day|station|rain|
+-------------------+-------+----+
|2023-06-01 00:00:00|    101|   0|
|2023-06-01 00:00:00|    102|  12|
|2023-06-01 00:00:00|    103|NULL|
|2023-06-02 00:00:00|    101|   3|
|2023-06-02 00:00:00|    102|NULL|
|2023-06-02 00:00:00|    103|   8|
|2023-06-03 00:00:00|    101|   1|
|2023-06-03 00:00:00|    102|   2|
|2023-06-03 00:00:00|    103|   0|
+-------------------+-------+----+
"""
KEEP_ZEROS_output = """
None -> 2 NULL cells
False -> 0 NULL cells
"""
READ_A_VERSION_output = """
latest: 24 hours
version 1: 12 hours
tag first-day: 12 hours
"""
READ_TILES_output = """
+------+-------+-------+-----+------+---------+
|slice |col_off|row_off|width|height|crs      |
+------+-------+-------+-----+------+---------+
|time=0|0      |0      |180  |91    |EPSG:4326|
|time=0|180    |0      |180  |91    |EPSG:4326|
|time=0|0      |91     |180  |90    |EPSG:4326|
|time=0|180    |91     |180  |90    |EPSG:4326|
+------+-------+-------+-----+------+---------+
"""
MEASURE_TILES_output = """
+-----+------+-----+
|width|height| mean|
+-----+------+-----+
|  180|    90|294.9|
|  180|    90|300.0|
|  180|    91|295.0|
|  180|    91|300.1|
+-----+------+-----+
"""
MATERIALIZE_TILES_output = """
+------------+
|holds_pixels|
+------------+
|        true|
+------------+
"""
STACK_BANDS_output = """
+-----+
|bands|
+-----+
|    6|
+-----+
"""
DATASOURCE_output = """
root
 |-- time: timestamp (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- t2m: float (nullable = true)

1302 cells
"""
SAMPLE_output = """
+------+---------------+-------------------+-----+--------+
|name  |geom           |time               |t2m  |msl     |
+------+---------------+-------------------+-----+--------+
|london|POINT(0.0 51.0)|2023-01-01 06:00:00|301.5|100380.0|
|paris |POINT(2.0 48.0)|2023-01-01 06:00:00|302.0|100530.0|
+------+---------------+-------------------+-----+--------+
"""
SAMPLE_PROJECTED_output = """
302.0 302.0
"""
UNKNOWN_VARIABLE_output = """
ValueError: unknown variable(s) ['temperature']; the store has ['latitude', 'longitude', 'msl', 't2m', 'time', 'u10', 'v10']. Fix: name arrays of the store.
"""
EXECUTOR_ERROR_output = """
PythonException
run gbx_zarr_sync(spark, <table>) on the store's manifest table to record the change, then read again
"""


# --- blocks and selections ------------------------------------------------------------


def read_blocks(spark, cube):
    """Three hours of 2 m temperature as blocks: one row per variable and touched chunk."""
    from databricks.labs.gbx.zarr import block_to_numpy, gbx_zarr_read

    blocks = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T02:00")},
    )
    blocks.orderBy("start").select("variable", "dims", "start", "stop", "shape").show(
        truncate=False
    )
    cells = block_to_numpy(blocks.orderBy("start").first())  # a numpy array
    print(cells.shape, cells.dtype)
    return blocks


def select_by_label_or_position(spark, cube):
    """The same cells selected by label (`sel`) and by position (`isel`)."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    by_label = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={
            "time": slice("2023-01-01T00:00", "2023-01-01T02:00"),
            "latitude": slice(60, 50),  # latitude runs north to south
            "longitude": slice(0, 10),  # both ends are included
        },
        output="table",
    )
    by_position = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        isel={
            "time": slice(0, 3),  # positions: the end is not included
            "latitude": slice(30, 41),
            "longitude": slice(0, 11),
        },
        output="table",
    )
    print("by label:", by_label.count(), "cells")
    print("by position:", by_position.count(), "cells")
    return by_label, by_position


# --- tables -----------------------------------------------------------------------------


def read_a_table(spark, cube):
    """One row per cell, with a column per dimension and per variable."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    europe = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl"],
        sel={
            "time": slice("2023-01-01T00:00", "2023-01-01T01:00"),
            "latitude": slice(60, 40),
            "longitude": slice(0, 10),
        },
        output="table",
    )
    europe.printSchema()
    europe.orderBy("time", "latitude", "longitude").show(3)
    return europe


def read_missing_data(spark, gauges):
    """Rainfall stored as int16 with a _FillValue of -9999: missing cells are NULL."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    rain = gbx_zarr_read(spark, gauges, variables=["rain"], output="table")
    rain.printSchema()
    rain.orderBy("day", "station").show()
    return rain


def keep_zero_counts(spark, counts):
    """A Zarr v2 array whose fill_value is 0 masks its zeros; the flag keeps them."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    for flag in (None, False):
        cells = gbx_zarr_read(
            spark,
            counts,
            variables=["counts"],
            output="table",
            use_zarr_fill_value_as_mask=flag,
        )
        print(flag, "->", cells.where("counts IS NULL").count(), "NULL cells")


# --- versions ---------------------------------------------------------------------------


def read_a_version(spark, table):
    """A read of a managed or registered store is pinned to one version."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    def hours(**version):
        return gbx_zarr_read(
            spark,
            table,
            variables=["t2m"],
            sel={"latitude": 48.0, "longitude": 2.0},
            output="table",
            **version,
        ).count()

    print("latest:", hours(), "hours")
    print("version 1:", hours(version_as_of=1), "hours")
    print("tag first-day:", hours(version_as_of="tag:first-day"), "hours")


# --- tiles ------------------------------------------------------------------------------


def read_tiles(spark, cube):
    """Tiles of one hour of 2 m temperature: a table of GeoBrix tiles, with no pixels yet."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read

    tiles = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": "2023-01-01T00:00"},
        output="tiles",
    )
    tiles.select(
        F.col("tile.metadata")["sliceDims"].alias("slice"),
        "tile.window.col_off",
        "tile.window.row_off",
        "tile.window.width",
        "tile.window.height",
        rx.rst_crs("tile").alias("crs"),  # answered from the header: no chunk is read
    ).orderBy("row_off", "col_off").show(truncate=False)
    return tiles


def measure_tiles(spark, cube):
    """Every rst_* function opens a tile on demand: here the size and mean of the windows."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read

    tiles = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": "2023-01-01T00:00"},
        output="tiles",
    )
    stats = tiles.select(
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        F.round(rx.rst_avg("tile").getItem(0), 1).alias("mean"),
    )
    stats.orderBy("height", "mean").show()
    return stats


def materialize_tiles(spark, cube):
    """virtual_tiles=False reads each tile now and holds its pixels as GeoTIFF bytes."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.zarr import gbx_zarr_read

    tiles = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": "2023-01-01T00:00"},
        output="tiles",
        virtual_tiles=False,
    )
    tiles.select((F.length("tile.raster") > 0).alias("holds_pixels")).distinct().show()
    return tiles


def stack_time_as_bands(spark, cube):
    """band_dim stacks one dimension as the bands of each tile: six hours, six bands."""
    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read

    tiles = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T05:00")},
        band_dim="time",
        output="tiles",
    )
    tiles.select(rx.rst_numbands("tile").alias("bands")).distinct().show()


# --- the DataSource ----------------------------------------------------------------------


def read_with_the_datasource(spark, cube):
    """A plain store read like any other source: no table, no manifest, no session call."""
    from databricks.labs.gbx.ds.register import register

    register(spark)  # once per session; it registers every GeoBrix DataSource without a JAR
    europe = (
        spark.read.format("zarr_gbx")
        .option("variables", "t2m")
        .option("sel", '{"time": ["2023-01-01T00:00", "2023-01-01T01:00"]}')
        .option("isel", '{"latitude": [30, 51], "longitude": [0, 31]}')
        .option("output", "table")
        .load(cube)
    )
    europe.printSchema()
    print(europe.count(), "cells")
    return europe


# --- sampling ---------------------------------------------------------------------------


def sample_at_points(spark, table):
    """The 2 m temperature and pressure at two places at 06:00."""
    from databricks.labs.gbx.zarr import gbx_zarr_sample

    points = spark.createDataFrame(
        [("paris", "POINT(2.0 48.0)"), ("london", "POINT(0.0 51.0)")], ["name", "geom"]
    )
    sampled = gbx_zarr_sample(
        spark,
        points,
        table,
        variables=["t2m", "msl"],
        sel={"time": "2023-01-01T06:00"},
    )
    sampled.orderBy("name").show(truncate=False)
    return sampled


def sample_projected_points(spark, cube):
    """Points in another CRS are moved to the store's: Paris in Web Mercator is Paris."""
    from databricks.labs.gbx.zarr import gbx_zarr_sample

    sel = {"time": "2023-01-01T06:00"}
    degrees = spark.createDataFrame([("POINT(2.0 48.0)",)], ["geom"])
    metres = spark.createDataFrame([("POINT(222638.98 6106854.83)",)], ["geom"])
    same = gbx_zarr_sample(spark, degrees, cube, variables=["t2m"], sel=sel)
    moved = gbx_zarr_sample(
        spark, metres, cube, variables=["t2m"], sel=sel, crs="EPSG:3857"
    )
    print(same.first()["t2m"], moved.first()["t2m"])
    return same, moved


# --- errors -----------------------------------------------------------------------------


def ask_for_an_unknown_variable(spark, cube):
    """A mistake in the call is raised at once, on the driver, with a Fix."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    try:
        gbx_zarr_read(spark, cube, variables=["temperature"])
    except ValueError as err:
        print(type(err).__name__ + ":", err)


def read_a_changed_store(spark, table):
    """An error inside a task arrives from the action that runs the read, as a PythonException."""
    from pyspark.errors import PythonException

    from databricks.labs.gbx.zarr import gbx_zarr_read

    cell = {"time": "2023-01-01T00:00", "latitude": 0.0, "longitude": 0.0}
    cells = gbx_zarr_read(spark, table, variables=["t2m"], sel=cell, output="table")
    try:
        cells.collect()
    except PythonException as err:
        # the message keeps the GeoBrix error name and its Fix: line
        message = next(ln for ln in str(err).splitlines() if "ZarrStoreDrift: " in ln)
        print(type(err).__name__)
        print(message.split("Fix: ")[1])
