"""Zarr writer examples -- single source of truth.

Code shown on docs/docs/writers/zarr.mdx is imported from here, and every function runs for
real in test_zarr_write_examples.py on the sample ERA5-like cube (a small synthetic weather
cube: ``time`` 48 hourly steps x ``latitude`` 181 x ``longitude`` 360, variables ``t2m``,
``u10``, ``v10`` and ``msl``). The page shows each function's body: the ``def`` line, the
docstring and the closing ``return`` are not shown.

Names that stand for what is yours when you run an example:

- ``spark``: the Spark session (a notebook has one);
- ``cube``: the path of the sample Zarr store (``SETUP``);
- ``table``: the Unity Catalog name of a store's manifest table;
- ``root``: a directory on a Volume that GeoBrix writes a store's files to.

The ``*_output`` constants are what the examples print. The tests compare them with a real
run, so they cannot drift.
"""

CUBE_SUBPATH = "global/era5-weather/era5_like_v3.zarr"

SETUP = """# The sample cube, a name for the store's manifest table, and a Volume directory for its files.
cube = "/Volumes/main/geobrix_samples/geobrix-examples/global/era5-weather/era5_like_v3.zarr"
table = "main.weather.era5"
root = "/Volumes/main/weather/stores/era5"
"""

# What the examples print. The tests compare these with a real run.
DESCRIBE_THE_LAYOUT_output = """
shape:  [12, 181, 360]
shard:  [24, 182, 360]
chunk:  [12, 91, 180]
codecs: ['bytes', 'zstd', 'crc32c']
"""
ADD_A_VARIABLE_output = """
+-------+------+-------------------------------------+
|version|mode  |variables                            |
+-------+------+-------------------------------------+
|1      |create|[latitude, longitude, msl, t2m, time]|
|2      |update|[t2m_c]                              |
+-------+------+-------------------------------------+
"""
APPEND_AND_FOLD_output = """
+-------+--------+
|version|    mode|
+-------+--------+
|      1|  create|
|      2|  append|
|      3|optimize|
+-------+--------+
"""
DELETE_A_CHUNK_output = """
+-----+-----+-------+-------+
|kind |array|key    |deleted|
+-----+-----+-------+-------+
|chunk|t2m  |c/0/0/0|true   |
+-----+-----+-------+-------+
"""
CLEAR_A_STALE_LOCK_output = """
wait for the publish to finish. If it was killed and none is running, clear the lock with gbx_zarr_sync('weather_lock', break_lock=True), then publish again with gbx_zarr_export('weather_lock', method="publish"); a lock can be broken once it is at least 1 hour old
"""
RECOVER_A_PUBLISH_output = """
the objects this publish already wrote are in the store, so run gbx_zarr_sync('weather_conflict') to record them, then publish again
+------+-----+-------+
|change|array|    key|
+------+-----+-------+
|  both|  t2m|c/0/0/0|
+------+-----+-------+
"""
REFUSE_FRACTIONAL_TIMES_output = """
values cannot be written to array 'time': the times are not whole multiples of its units 'hours since 2023-01-01 00:00:00' (they would need 'minutes since 2023-01-01'), and its stored type int64 has no fractions Fix: write values that fit the array's type and attributes, or write its stored values (blocks read with decode=False)
"""


# --- create: layouts and inputs ---------------------------------------------------------


def create_with_a_layout(spark, cube, table, root):
    """Choose the chunks, the shards and the codecs of the arrays a new store holds."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    first = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    version = gbx_zarr_write(
        first,
        table,
        mode="create",
        root=root,
        chunks={"time": 12, "latitude": 91, "longitude": 180},  # inner chunks, in cells
        shards={"time": 24},  # a shard of two chunks along time; the rest is computed
        codecs=[  # the inner chunk's codec chain, as Zarr v3 codec documents
            {"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "zstd", "configuration": {"level": 9, "checksum": False}},
            {"name": "crc32c"},
        ],
    )
    return version


def describe_the_layout(spark, table):
    """The layout is in the array's document, which is a row of the manifest."""
    import json

    row = spark.sql(
        f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 't2m'"
    ).first()
    document = json.loads(row["meta"])
    sharding = document["codecs"][0]["configuration"]
    print("shape: ", document["shape"])
    print("shard: ", document["chunk_grid"]["configuration"]["chunk_shape"])
    print("chunk: ", sharding["chunk_shape"])
    print("codecs:", [codec["name"] for codec in sharding["codecs"]])


def create_from_xarray(spark, cube, table, root):
    """A store from an xarray Dataset: the Dataset is held on the driver, so keep it small."""
    import xarray as xr

    from databricks.labs.gbx.zarr import gbx_zarr_write

    cube_ds = xr.open_zarr(cube, consolidated=False)
    first = cube_ds[["t2m"]].isel(time=slice(0, 12)).load()
    version = gbx_zarr_write(first, table, mode="create", root=root, spark=spark)
    return version


def stage_the_blocks_first(spark, cube, table, staged, root):
    """A create or an append reads its input twice: stage a computed input in a table first."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    blocks = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    blocks.write.mode("overwrite").saveAsTable(staged)  # evaluated once, here
    version = gbx_zarr_write(spark.table(staged), table, mode="create", root=root)
    return version


# --- overwrite, and variables the store lacks --------------------------------------------


def overwrite_a_variable(spark, table):
    """Replace a variable with a bias-corrected one; the old version stays readable."""
    from databricks.labs.gbx.zarr import (
        BLOCKS_SCHEMA,
        block_to_numpy,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    rows = gbx_zarr_read(spark, table, variables=["t2m"]).collect()
    corrected = [
        {
            **row.asDict(),
            "data": (block_to_numpy(row) - 0.5).astype("float32").tobytes(),
        }
        for row in rows
    ]
    version = gbx_zarr_write(
        spark.createDataFrame(corrected, BLOCKS_SCHEMA),
        table,
        mode="overwrite",
        variables=["t2m"],  # the variables to replace
    )
    return version


def add_a_variable(spark, table):
    """A new variable in an update is named in `variables`; its first block gives its attributes."""
    import json

    from databricks.labs.gbx.zarr import (
        BLOCKS_SCHEMA,
        block_to_numpy,
        gbx_zarr_history,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    rows = gbx_zarr_read(spark, table, variables=["t2m"]).collect()
    celsius = [
        {
            **row.asDict(),
            "variable": "t2m_c",
            "data": (block_to_numpy(row) - 273.15).astype("float32").tobytes(),
            "attrs": json.dumps({**json.loads(row["attrs"]), "units": "degC"}),
        }
        for row in rows
    ]
    version = gbx_zarr_write(
        spark.createDataFrame(celsius, BLOCKS_SCHEMA),
        table,
        mode="update",
        variables=["t2m_c"],  # required: the store has no such array yet
    )
    history = gbx_zarr_history(spark, table)
    history.select("version", "mode", "variables").orderBy("version").show(
        truncate=False
    )
    return version


# --- tiles ------------------------------------------------------------------------------


def write_tiles_with_provenance(spark, table):
    """Correct four hours as tiles, and write them back to the cells they were cut from."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    tiles = gbx_zarr_read(
        spark, table, variables=["t2m"], isel={"time": slice(0, 4)}, output="tiles"
    )
    # rst_mapalgebra returns a new tile with no record of where it came from: copy the
    # metadata of the tile it was made from back onto it
    corrected = (
        tiles.select(
            rx.rst_mapalgebra(F.array("tile"), F.lit("A-1")).alias("tile"),
            F.col("tile.metadata").alias("provenance"),
        )
        .withColumn("tile", F.col("tile").withField("metadata", F.col("provenance")))
        .select("tile")
    )
    version = gbx_zarr_write(corrected, table, mode="update")
    return version


def write_tiles_with_dim_cols(spark, table):
    """The same correction, saying where each tile goes with `dim_cols` and `variables`."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    tiles = gbx_zarr_read(
        spark, table, variables=["t2m"], isel={"time": slice(0, 4)}, output="tiles"
    )
    corrected = tiles.select(
        rx.rst_mapalgebra(F.array("tile"), F.lit("A-1")).alias("tile"),
        F.to_timestamp(F.col("tile.metadata")["sliceCoord_time"]).alias("time"),
    )
    version = gbx_zarr_write(
        corrected,
        table,
        mode="update",
        dim_cols={"time": "time"},  # the dimension, and the column that labels it
        variables="t2m",  # the array the tiles go to
    )
    return version


def create_from_tiles(spark, cube, table, root):
    """A new store from tiles: its grid comes from the tiles, its time labels from `dim_cols`."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    tiles = gbx_zarr_read(
        spark, cube, variables=["t2m"], isel={"time": slice(0, 3)}, output="tiles"
    )
    warmer = tiles.select(
        rx.rst_mapalgebra(F.array("tile"), F.lit("A+1")).alias("tile"),
        F.to_timestamp(F.col("tile.metadata")["sliceCoord_time"]).alias("time"),
    )
    version = gbx_zarr_write(
        warmer,
        table,
        mode="create",
        root=root,
        dim_cols={"time": "time"},
        variables="t2m",  # a new store names its variable "data" unless you say
    )
    return version


# --- append and fold ---------------------------------------------------------------------


def append_and_fold(spark, cube, table):
    """Append, and fold the shards the append touched right after it commits."""
    from databricks.labs.gbx.zarr import gbx_zarr_history, gbx_zarr_read, gbx_zarr_write

    following = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T12:00", "2023-01-01T23:00")},
    )
    version = gbx_zarr_write(following, table, mode="append", optimize=True)
    # `version` is the append's own; the fold, if it had work to do, is the next version
    gbx_zarr_history(spark, table).select("version", "mode").orderBy("version").show()
    return version


# --- registered stores ------------------------------------------------------------------


def delete_a_chunk(spark, table):
    """A chunk written with nothing but the fill value is deleted, not stored."""
    import numpy as np

    from databricks.labs.gbx.zarr import BLOCKS_SCHEMA, gbx_zarr_read, gbx_zarr_write

    chunk = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        isel={"time": slice(0, 12), "latitude": slice(0, 91), "longitude": slice(0, 180)},
    ).first()  # exactly one inner chunk
    empty = {
        **chunk.asDict(),
        "data": np.full(tuple(chunk["shape"]), np.nan, dtype="float32").tobytes(),
    }
    gbx_zarr_write(spark.createDataFrame([empty], BLOCKS_SCHEMA), table, mode="update")
    spark.sql(
        f"SELECT kind, array, key, file IS NULL AS deleted FROM {table} "
        "WHERE origin = 'gbx' AND array = 't2m' AND kind IN ('chunk', 'shard') "
        "AND tag IS NULL"
    ).show(truncate=False)


def clear_a_stale_publish_lock(spark, table):
    """A publish that was killed leaves its lock; clear it once it is an hour old."""
    from databricks.labs.gbx.zarr import (
        ZarrWriteConflict,
        gbx_zarr_export,
        gbx_zarr_sync,
    )

    try:
        gbx_zarr_export(spark, table, method="publish")
    except ZarrWriteConflict as err:
        print(err.fix)
    # no publish is running, and the lock is more than an hour old: clear it, then publish
    gbx_zarr_sync(spark, table, break_lock=True)
    version = gbx_zarr_export(spark, table, method="publish")
    return version


def recover_a_publish(spark, table):
    """A write that lands during a publish fails its commit: sync, then publish again."""
    from databricks.labs.gbx.zarr import (
        ZarrWriteConflict,
        gbx_zarr_export,
        gbx_zarr_sync,
    )

    try:
        gbx_zarr_export(spark, table, method="publish")
    except ZarrWriteConflict as err:
        print(err.fix)
    gbx_zarr_sync(spark, table).show()  # lists the objects the failed publish wrote
    version = gbx_zarr_export(spark, table, method="publish")
    return version


# --- time units -------------------------------------------------------------------------


def refuse_fractional_times(spark, table):
    """Relabel two hours half an hour later: not whole hours of the axis, so refused whole."""
    import numpy as np

    from databricks.labs.gbx.zarr import (
        BLOCKS_SCHEMA,
        ZarrUnsupported,
        block_to_numpy,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    times = gbx_zarr_read(
        spark, table, variables=["time"], isel={"time": slice(0, 2)}
    ).collect()
    half_past = [
        {
            **row.asDict(),
            "data": (block_to_numpy(row) + np.timedelta64(30, "m")).tobytes(),
        }
        for row in times
    ]
    try:
        gbx_zarr_write(
            spark.createDataFrame(half_past, BLOCKS_SCHEMA),
            table,
            mode="update",
            variables=["time"],
        )
    except ZarrUnsupported as err:
        print(err)
