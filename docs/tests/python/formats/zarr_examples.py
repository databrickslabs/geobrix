"""Formats / Zarr examples — single source of truth.

Code shown on docs/docs/formats/zarr.mdx is imported from here, and every function runs for
real in test_zarr_examples.py on the sample ERA5-like cube (a small synthetic weather cube:
``time`` 48 hourly steps x ``latitude`` 181 x ``longitude`` 360, variables ``t2m``, ``u10``,
``v10`` and ``msl``). The page shows each function's body: the ``def`` line, the docstring
and the closing ``return`` are not shown.

Four names stand for what is yours when you run an example:

- ``spark``: the Spark session (a notebook has one);
- ``cube``: the path of the sample Zarr store (``SETUP``);
- ``table``: the Unity Catalog name of a store's manifest table;
- ``root`` / ``overlay_root``: a directory on a Volume that GeoBrix writes a store's files to.

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
READ_A_SELECTION_output = """
+-------------------+--------+---------+-----+
|               time|latitude|longitude|  t2m|
+-------------------+--------+---------+-----+
|2023-01-01 00:00:00|    40.0|      0.0|299.5|
|2023-01-01 00:00:00|    40.0|      1.0|299.5|
|2023-01-01 00:00:00|    40.0|      2.0|299.5|
+-------------------+--------+---------+-----+
only showing top 3 rows
"""
SAMPLE_A_TIME_SERIES_output = """
+------+---------------+-------------------+------+
|name  |geom           |time               |t2m   |
+------+---------------+-------------------+------+
|london|POINT(0.0 51.0)|2023-01-01 00:00:00|297.5 |
|london|POINT(0.0 51.0)|2023-01-01 01:00:00|298.5 |
|london|POINT(0.0 51.0)|2023-01-01 02:00:00|299.5 |
|paris |POINT(2.0 48.0)|2023-01-01 00:00:00|298.25|
|paris |POINT(2.0 48.0)|2023-01-01 01:00:00|299.25|
|paris |POINT(2.0 48.0)|2023-01-01 02:00:00|300.25|
+------+---------------+-------------------+------+
"""
COMPARE_BRANCHES_output = """
read-only 303.0
writable 302.0
"""
DETECT_OUTSIDE_CHANGES_output = """
Fix: run gbx_zarr_sync(<table>) on the store's manifest table to record the change, then read again
+-------+-----+-------+
| change|array|    key|
+-------+-----+-------+
|changed|  t2m|c/0/0/0|
+-------+-----+-------+
"""
MANAGED_IS_NOT_AVAILABLE_output = """
file_mode="managed" is not available in 0.6.0: Delta VACUUM does not reclaim the bytes of a managed file (nor does a deletion-vector delete or REORG PURGE), so a managed store would only grow Fix: use file_mode="auto" (the default), "external" or "fuse"
"""
READ_HISTORY_output = """
+-------+------+-------------------------------------+
|version|mode  |variables                            |
+-------+------+-------------------------------------+
|1      |create|[latitude, longitude, msl, t2m, time]|
|2      |append|[msl, t2m, time]                     |
|3      |append|[msl, t2m, time]                     |
|4      |append|[msl, t2m, time]                     |
|5      |tag   |NULL                                 |
|6      |update|[t2m]                                |
|7      |update|[t2m]                                |
|8      |update|[t2m]                                |
+-------+------+-------------------------------------+

+--------------------------------------------------------------+
|region                                                        |
+--------------------------------------------------------------+
|{latitude -> [0, 181], longitude -> [0, 360], time -> [0, 12]}|
+--------------------------------------------------------------+
"""
TIME_TRAVEL_output = """
version 1: 12
as of the second commit's time: 24
tag two-days: 48
latest: 48
"""
SHOW_RETENTION_output = """
+----------------------------------+----------------+
|key                               |value           |
+----------------------------------+----------------+
|delta.deletedFileRetentionDuration|interval 7 days |
|delta.logRetentionDuration        |interval 30 days|
|geobrix.zarr.retain               |7 days          |
+----------------------------------+----------------+
"""
VACUUM_THE_STORE_output = """
+------------+-----------+-----+
|      action|     reason|count|
+------------+-----------+-----+
|        kept|       live|    5|
|        kept|     tagged|    8|
|would_delete| superseded|    7|
|would_delete|uncommitted|    1|
+------------+-----------+-----+
"""
COUNT_MANIFEST_ROWS_before_output = """
+-----+----+
| kind|rows|
+-----+----+
|chunk|   8|
+-----+----+
"""
COUNT_MANIFEST_ROWS_after_output = """
+-----+----+
| kind|rows|
+-----+----+
|shard|   1|
+-----+----+
"""


# --- the sample cube ------------------------------------------------------------------


def make_the_sample_cube(out_dir):
    """Write the sample cube (a sharded Zarr v3 store and a Zarr v2 store) into a directory."""
    from databricks.labs.gbx.sample.zarr_era5 import write_era5_cube

    written = write_era5_cube(out_dir)
    return written


# --- why Zarr -------------------------------------------------------------------------


def read_a_selection(spark, cube):
    """Three hours of 2 m temperature over western Europe, as a table."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    europe = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={
            "time": slice("2023-01-01T00:00", "2023-01-01T02:00"),
            "latitude": slice(
                60, 40
            ),  # labels, both ends included; latitude runs north to south
            "longitude": slice(0, 30),
        },
        output="table",
    )
    europe.orderBy("time", "latitude", "longitude").show(3)
    return europe


def sample_a_time_series(spark, table):
    """The 2 m temperature of two places over three hours."""
    from databricks.labs.gbx.zarr import gbx_zarr_sample

    points = spark.createDataFrame(
        [("paris", "POINT(2.0 48.0)"), ("london", "POINT(0.0 51.0)")], ["name", "geom"]
    )
    series = gbx_zarr_sample(
        spark,
        points,
        table,
        variables=["t2m"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T02:00")},
    )
    series.orderBy("name", "time").show(truncate=False)
    return series


# --- store kinds ----------------------------------------------------------------------


def create_a_store(spark, cube, table, root):
    """A managed store: the first 12 hours of two variables, with the coordinates that label them."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    first = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    # "create" makes a managed store: `table` is its manifest, `root` holds its files.
    version = gbx_zarr_write(first, table, mode="create", root=root)
    return version


def refuse_a_second_owner(spark, cube, other_table, root):
    """A root belongs to one table; a second table cannot claim it."""
    from databricks.labs.gbx.zarr import (
        ZarrOwnershipError,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    one_hour = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "time", "latitude", "longitude"],
        sel={"time": "2023-01-01T00:00"},
    )
    try:
        gbx_zarr_write(one_hour, other_table, mode="create", root=root)
    except ZarrOwnershipError as err:
        print(err)


def register_in_place(spark, cube, table):
    """Register an existing Zarr store as a table: nothing is copied or written."""
    from databricks.labs.gbx.zarr import gbx_zarr_register

    version = gbx_zarr_register(spark, cube, table)
    return version


def register_with_an_overlay(spark, base, table, overlay_root):
    """Register a store so that GeoBrix can write to it without ever touching it."""
    from databricks.labs.gbx.zarr import gbx_zarr_register

    version = gbx_zarr_register(
        spark, base, table, writable=True, overlay_root=overlay_root
    )
    return version


def compare_branches(spark, read_only_table, writable_table):
    """Two registrations of one store: each sees the store plus only its own writes."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    cell = {"time": "2023-01-01T00:00", "latitude": 0.0, "longitude": 0.0}
    for name, table in {
        "read-only": read_only_table,
        "writable": writable_table,
    }.items():
        row = gbx_zarr_read(
            spark, table, variables=["t2m"], sel=cell, output="table"
        ).first()
        print(name, row["t2m"])


def detect_outside_changes(spark, table):
    """A registered store changed by another tool is detected on read, and brought up to date."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_sync

    cell = {"time": "2023-01-01T00:00", "latitude": 0.0, "longitude": 0.0}
    try:
        gbx_zarr_read(
            spark, table, variables=["t2m"], sel=cell, output="table"
        ).collect()
    except Exception as err:  # the read task raised ZarrStoreDrift
        message = next(
            line for line in str(err).splitlines() if "ZarrStoreDrift: " in line
        )
        print("Fix:", message.split("Fix: ")[1])
    gbx_zarr_sync(spark, table).show()


def publish_the_overlay(spark, table):
    """Write a registration's overlay into the store itself, so every Zarr reader sees it."""
    from databricks.labs.gbx.zarr import gbx_zarr_export

    version = gbx_zarr_export(spark, table, method="publish")
    return version


# --- file modes -----------------------------------------------------------------------


def create_with_a_file_mode(spark, cube, table, root):
    """Choose how the manifest refers to the store's files."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    first = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    # "fuse": the manifest keeps plain path strings. It works on every compute.
    gbx_zarr_write(first, table, mode="create", root=root, file_mode="fuse")


def managed_is_not_available(spark, cube, table, root):
    """FILE MANAGED stores are not available in this release."""
    from databricks.labs.gbx.zarr import (
        ZarrUnsupported,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    first = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T11:00")},
    )
    try:
        gbx_zarr_write(first, table, mode="create", root=root, file_mode="managed")
    except ZarrUnsupported as err:
        print(err)


# --- transactions ---------------------------------------------------------------------


def append_hours(spark, cube, table):
    """Extend the store along time: the next 12 hours."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    following = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m", "msl", "time", "latitude", "longitude"],
        sel={"time": slice("2023-01-01T12:00", "2023-01-01T23:00")},
    )
    # The new `time` labels must come after the store's last one.
    version = gbx_zarr_write(following, table, mode="append")
    return version


def append_from_two_writers(spark, cube, table):
    """Two jobs append at the same moment; both land."""
    from concurrent.futures import ThreadPoolExecutor

    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_write

    def hours(start, stop):
        return gbx_zarr_read(
            spark,
            cube,
            variables=["t2m", "msl", "time", "latitude", "longitude"],
            sel={"time": slice(start, stop)},
        )

    morning = hours("2023-01-02T00:00", "2023-01-02T11:00")
    afternoon = hours("2023-01-02T12:00", "2023-01-02T23:00")
    with ThreadPoolExecutor(max_workers=2) as pool:
        versions = list(
            pool.map(
                lambda block: gbx_zarr_write(block, table, mode="append"),
                [morning, afternoon],
            )
        )
    return versions


def update_a_region(spark, table):
    """Correct a region in place: 1 K off the first 12 hours of 2 m temperature."""
    from databricks.labs.gbx.zarr import (
        BLOCKS_SCHEMA,
        block_to_numpy,
        gbx_zarr_read,
        gbx_zarr_write,
    )

    region = {"time": slice("2023-01-01T00:00", "2023-01-01T11:00")}
    rows = gbx_zarr_read(spark, table, variables=["t2m"], sel=region).collect()
    corrected = [
        {
            **row.asDict(),
            "data": (block_to_numpy(row) - 1.0).astype("float32").tobytes(),
        }
        for row in rows
    ]
    patch = spark.createDataFrame(corrected, BLOCKS_SCHEMA)
    version = gbx_zarr_write(patch, table, mode="update")
    return version


def handle_a_conflict(spark, table, patch):
    """A write that meets another write's chunks fails whole, and says whose they are."""
    from databricks.labs.gbx.zarr import ZarrWriteConflict, gbx_zarr_write

    try:
        gbx_zarr_write(patch, table, mode="update")
    except ZarrWriteConflict as err:
        # Another write changed these chunks after this one read them; none of this write
        # is visible. Read the region again, rebuild the patch and write again.
        print("lost to", err.winner, "on", err.keys)


def retry_with_a_write_id(spark, table, patch):
    """A write with its own id is safe to run again."""
    import uuid

    from databricks.labs.gbx.zarr import gbx_zarr_write

    write_id = uuid.uuid4().hex  # 32 hex characters, chosen before the first attempt
    version = gbx_zarr_write(patch, table, mode="update", write_id=write_id)
    # If the first attempt committed but never reported back, run it again. A write_id
    # that already committed returns its version and writes nothing.
    again = gbx_zarr_write(patch, table, mode="update", write_id=write_id)
    return version, again


# --- snapshots, time travel and tags --------------------------------------------------


def read_history(spark, table):
    """One row per commit."""
    from databricks.labs.gbx.zarr import gbx_zarr_history

    history = gbx_zarr_history(spark, table)
    history.select("version", "mode", "variables").show(truncate=False)
    # The region a write covered, per dimension, as [start, stop) in cells.
    history.where("version = 6").select("region").show(truncate=False)


def tag_a_version(spark, table):
    """Name the store as it is now."""
    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_tag

    gbx_zarr_tag(spark, table, "two-days")
    tagged = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        sel={"latitude": 48.0, "longitude": 2.0},
        version_as_of="tag:two-days",
        output="table",
    )
    return tagged.count()


def time_travel(spark, table):
    """The hours one cell holds at four points in the store's history."""
    from databricks.labs.gbx.zarr import gbx_zarr_history, gbx_zarr_read

    def hours_at(**version):
        return gbx_zarr_read(
            spark,
            table,
            variables=["t2m"],
            sel={"latitude": 48.0, "longitude": 2.0},
            output="table",
            **version,
        ).count()

    stamp = gbx_zarr_history(spark, table).where("version = 2").first()["timestamp"]
    print("version 1:", hours_at(version_as_of=1))
    print("as of the second commit's time:", hours_at(timestamp_as_of=stamp))
    print("tag two-days:", hours_at(version_as_of="tag:two-days"))
    print("latest:", hours_at())


def restore_a_version(spark, table):
    """Make the store equal to a tagged version again, as a new commit."""
    from databricks.labs.gbx.zarr import gbx_zarr_restore

    version = gbx_zarr_restore(spark, table, version_as_of="tag:two-days")
    return version


def export_a_version(spark, table, to):
    """Copy the store out as a plain Zarr store that any Zarr tool opens."""
    import xarray as xr

    from databricks.labs.gbx.zarr import gbx_zarr_export

    gbx_zarr_export(spark, table, to)
    cube = xr.open_zarr(to)
    return cube


# --- lifecycle and retention ----------------------------------------------------------


def show_retention(spark, table):
    """How long the store keeps old versions, and what the manifest table keeps to match."""
    (
        spark.sql(f"SHOW TBLPROPERTIES {table}")
        .where("key LIKE 'delta.%Retention%' OR key = 'geobrix.zarr.retain'")
        .orderBy("key")
        .show(truncate=False)
    )


def vacuum_the_store(spark, table):
    """Delete the files that no readable version needs."""
    from databricks.labs.gbx.zarr import gbx_zarr_vacuum

    plan = gbx_zarr_vacuum(spark, table, dry_run=True)  # lists; deletes nothing
    plan.groupBy("action", "reason").count().orderBy("action", "reason").show()
    gbx_zarr_vacuum(spark, table)


# --- chunk rows and shard rows --------------------------------------------------------


def count_manifest_rows(spark, table):
    """The manifest is a table: count the rows that point at one array's data."""
    spark.sql(f"""
        SELECT kind, count(*) AS rows FROM {table}
        WHERE array = 't2m' AND kind IN ('shard', 'chunk') AND tag IS NULL
        GROUP BY kind ORDER BY kind""").show()


def optimize_the_store(spark, table):
    """Fold the chunk rows that appends and updates left into whole shards."""
    from databricks.labs.gbx.zarr import gbx_zarr_optimize

    version = gbx_zarr_optimize(spark, table)
    return version
