"""Zarr function reference examples -- single source of truth.

Code shown on docs/docs/api/zarr-functions.mdx is imported from here, one example for each
function, and every function runs for real in test_zarr_functions_examples.py on the sample
ERA5-like cube (a small synthetic weather cube: ``time`` 48 hourly steps x ``latitude`` 181 x
``longitude`` 360, variables ``t2m``, ``u10``, ``v10`` and ``msl``). The page shows each
function's body: the ``def`` line, the docstring and the closing ``return`` are not shown.

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
READ_A_STORE_output = """
+-------------------+--------+---------+------+
|               time|latitude|longitude|   t2m|
+-------------------+--------+---------+------+
|2023-01-01 06:00:00|    48.0|      2.0| 302.0|
|2023-01-01 06:00:00|    49.0|      2.0|301.75|
|2023-01-01 06:00:00|    50.0|      2.0|301.75|
+-------------------+--------+---------+------+
"""
CONVERT_BLOCKS_output = """
(3, 181, 295) float32
t2m ('time', 'latitude', 'longitude')
variable STRING
"""
SAMPLE_A_POINT_output = """
+---------------+-------------------+-----+
|geom           |time               |t2m  |
+---------------+-------------------+-----+
|POINT(2.0 48.0)|2023-01-01 06:00:00|302.0|
+---------------+-------------------+-----+
"""
SHOW_THE_HISTORY_output = """
+-------+------+-------------------------------------+
|version|mode  |variables                            |
+-------+------+-------------------------------------+
|1      |create|[latitude, longitude, msl, t2m, time]|
|2      |append|[msl, t2m, time]                     |
+-------+------+-------------------------------------+
"""
TAG_A_VERSION_output = """
24 hours at tag:day-one
"""
VACUUM_A_STORE_output = """
+------+--------+-----+
|action|  reason|count|
+------+--------+-----+
|  kept|    live|    7|
|  kept|retained|    3|
+------+--------+-----+
"""
SYNC_A_STORE_output = """
+-------+-----+-------+
| change|array|    key|
+-------+-----+-------+
|changed|  t2m|c/0/0/0|
+-------+-----+-------+
"""
CATCH_A_ZARR_ERROR_output = """
ZarrVersionUnavailable
pick a later version or tag the version you need with gbx_zarr_tag
"""


# --- write, read and sample --------------------------------------------------------------


def write_a_store(spark, cube, table, root):
    """gbx_zarr_write: create a managed store from the first 12 hours of two variables."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    first = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    version = gbx_zarr_write(first, table, mode="create", root=root)  # the first version
    return version


def read_a_store(spark, table):
    """gbx_zarr_read: a cut of the store as a table of cells."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    cells = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        sel={"time": "2023-01-01T06:00", "latitude": slice(50, 48), "longitude": 2.0},
        output="table",
    )
    cells.orderBy("latitude").show()
    return cells


def convert_blocks(spark, table):
    """BLOCKS_SCHEMA, block_to_numpy, block_to_xarray: a block row as an array."""
    from databricks.labs.gbx.zarr import (
        BLOCKS_SCHEMA,
        block_to_numpy,
        block_to_xarray,
        gbx_zarr_read,
    )

    blocks = gbx_zarr_read(spark, table, variables=["t2m"], isel={"time": slice(0, 3)})
    row = blocks.orderBy("start").first()  # the block at the top left of the grid
    cells = block_to_numpy(row)  # a numpy array of the block's shape and dtype
    named = block_to_xarray(row)  # a DataArray named for the variable, with its dimensions
    print(cells.shape, cells.dtype)
    print(named.name, named.dims)
    print(BLOCKS_SCHEMA.split(",")[0])  # the schema, as a DDL string


def sample_a_point(spark, table):
    """gbx_zarr_sample: the 2 m temperature at Paris at 06:00."""
    from databricks.labs.gbx.zarr import gbx_zarr_sample

    points = spark.createDataFrame([("POINT(2.0 48.0)",)], ["geom"])
    sampled = gbx_zarr_sample(
        spark, points, table, variables=["t2m"], sel={"time": "2023-01-01T06:00"}
    )
    sampled.show(truncate=False)
    return sampled


# --- history, tags, maintenance ----------------------------------------------------------


def show_the_history(spark, table):
    """gbx_zarr_history: one row per commit, oldest first."""
    from databricks.labs.gbx.zarr import gbx_zarr_history

    history = gbx_zarr_history(spark, table)
    history.select("version", "mode", "variables").orderBy("version").show(
        truncate=False
    )
    return history


def tag_a_version(spark, table):
    """gbx_zarr_tag: name the store as it is now, and read it back by name."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_tag

    gbx_zarr_tag(spark, table, "day-one")  # version_as_of= tags an older version
    hours = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        sel={"latitude": 48.0, "longitude": 2.0},
        version_as_of="tag:day-one",
        output="table",
    ).count()
    print(hours, "hours at tag:day-one")
    return hours


def fold_the_store(spark, table):
    """gbx_zarr_optimize: fold the chunk rows that appends and updates left into shards."""
    from databricks.labs.gbx.zarr import gbx_zarr_optimize

    version = gbx_zarr_optimize(spark, table)  # the version the fold committed
    return version


def restore_a_tag(spark, table):
    """gbx_zarr_restore: make the store equal a tagged version again, as a new commit."""
    from databricks.labs.gbx.zarr import gbx_zarr_restore

    version = gbx_zarr_restore(spark, table, version_as_of="tag:day-one")
    return version


def vacuum_a_store(spark, table):
    """gbx_zarr_vacuum: list the files no readable version needs, and delete nothing."""
    from databricks.labs.gbx.zarr import gbx_zarr_vacuum

    plan = gbx_zarr_vacuum(spark, table, dry_run=True)
    plan.groupBy("action", "reason").count().orderBy("action", "reason").show()
    # gbx_zarr_vacuum(spark, table) deletes the files marked would_delete


def untag_a_version(spark, table):
    """gbx_zarr_untag: remove a tag; its files can be vacuumed once nothing else needs them."""
    from databricks.labs.gbx.zarr import gbx_zarr_untag

    gbx_zarr_untag(spark, table, "day-one")


# --- export -----------------------------------------------------------------------------


def export_a_copy(spark, table, to):
    """gbx_zarr_export(method="copy"): a plain Zarr store, in the store's own format."""
    from databricks.labs.gbx.zarr import gbx_zarr_export

    path = gbx_zarr_export(spark, table, to)  # `to` must not exist, or be empty
    return path


def export_a_reference(spark, table, to):
    """gbx_zarr_export(method="reference"): a JSON file that points into the store's files."""
    from databricks.labs.gbx.zarr import (
        gbx_zarr_export,
        gbx_zarr_optimize,
        gbx_zarr_tag,
    )

    gbx_zarr_optimize(spark, table)  # a reference names one range per shard
    gbx_zarr_tag(spark, table, "for-reference")  # so a vacuum keeps the files it points at
    path = gbx_zarr_export(
        spark, table, to, method="reference", version_as_of="tag:for-reference"
    )
    # open it with xarray, on compute with fsspec 2024.12.0 or later:
    #   xarray.open_zarr("reference://", storage_options={"fo": to}, consolidated=False)
    return path


def publish_an_overlay(spark, table):
    """gbx_zarr_export(method="publish"): write a writable registration's overlay into its store."""
    from databricks.labs.gbx.zarr import gbx_zarr_export

    version = gbx_zarr_export(spark, table, method="publish")
    return version


# --- registered stores ------------------------------------------------------------------


def register_a_store(spark, base, table):
    """gbx_zarr_register: a table over an existing store; nothing is copied or written."""
    from databricks.labs.gbx.zarr import gbx_zarr_register

    version = gbx_zarr_register(spark, base, table)  # the first version of the table
    return version


def sync_a_store(spark, table):
    """gbx_zarr_sync: record what other tools changed in the store, as one new version."""
    from databricks.labs.gbx.zarr import gbx_zarr_sync

    changes = gbx_zarr_sync(spark, table)
    changes.show()
    return changes


# --- errors -----------------------------------------------------------------------------


def catch_a_zarr_error(spark, table):
    """Every error is a ZarrError with a `fix`; the subclass says what kind."""
    from databricks.labs.gbx.zarr import ZarrError, gbx_zarr_read

    try:
        gbx_zarr_read(spark, table, variables=["t2m"], version_as_of=99)
    except ZarrError as err:
        print(type(err).__name__)
        print(err.fix)
