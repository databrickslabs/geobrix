"""Fixtures for the Zarr suites: a local Delta session and synthetic ERA5-like stores.

``spark_delta`` needs the Delta Spark jars on the JVM classpath *before* the first
SparkSession starts. The root conftest puts them on ``PYSPARK_SUBMIT_ARGS`` when this
directory is collected (see ``pytest_collection_finish`` in ``test/conftest.py``);
``gbx:test:python`` fetches the jars into the Maven repo when they are missing.
"""

import pytest

_DELTA_EXT = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


@pytest.fixture(scope="module")
def spark_delta(tmp_path_factory):
    """A local-mode Spark session with Delta Lake enabled (OSS delta-spark 4.0.x).

    The Delta extension is a static conf, so a live session without it is stopped
    first; a session that already has it is reused. Without the Delta jars on the
    classpath this fails loudly instead of skipping.
    """
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
            .appName("gbx-zarr-tests")
            .config("spark.sql.extensions", _DELTA_EXT)
            .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("delta_wh")))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as e:  # the JVM could not load the Delta classes
        raise RuntimeError(
            "Could not start a Delta-enabled Spark session; the delta-spark jars are "
            "not on the JVM classpath. Fix: run the suite through "
            "scripts/commands/gbx-test-python.sh (it fetches the jars), or set "
            "PYSPARK_SUBMIT_ARGS to include them before pytest starts."
        ) from e
    yield session
    session.stop()


@pytest.fixture(scope="session")
def era5_v3_sharded(tmp_path_factory):
    """Zarr v3 store with sharding: chunks (12, 37, 72) inside shards (24, 74, 144)."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v3_sharded.zarr",
        zarr_format=3,
        sharded=True,
    )


@pytest.fixture(scope="session")
def era5_v3_unsharded(tmp_path_factory):
    """Zarr v3 store without sharding: one object per (12, 37, 72) chunk."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v3_unsharded.zarr",
        zarr_format=3,
        sharded=False,
    )


@pytest.fixture(scope="session")
def era5_v2(tmp_path_factory):
    """Zarr v2 store: blosc-lz4 chunks (12, 37, 72), keys like ``t2m/0.0.0``."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v2.zarr", zarr_format=2, sharded=False
    )


@pytest.fixture(scope="session")
def dtype_matrix(tmp_path_factory):
    """A Zarr v3 store with one variable per table column kind, on a (3, 4) grid.

    Dimensions ``lev`` (an int32 coordinate) and ``x`` (no coordinate). Returns
    ``(path, expected)`` where ``expected`` maps each column to its 12 values in C order,
    ``None`` for a masked cell and a float NaN for NaN data.
    """
    import numpy as np
    import zarr

    path = tmp_path_factory.mktemp("matrix") / "m.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    lev = g.create_array(
        "lev", shape=(3,), chunks=(3,), dtype="int32", dimension_names=("lev",)
    )
    lev[:] = [10, 20, 30]
    shape, n = (3, 4), 12
    nan = float("nan")
    expected = {"lev": [10] * 4 + [20] * 4 + [30] * 4, "x": [0, 1, 2, 3] * 3}

    def put(name, values, dtype, attrs=None, shown=None):
        arr = g.create_array(
            name,
            shape=shape,
            chunks=shape,
            dtype=dtype,
            dimension_names=("lev", "x"),
            attributes=attrs or {},
        )
        arr[:] = np.asarray(values, dtype=dtype).reshape(shape)
        expected[name] = list(values) if shown is None else shown

    put("i1", [-5 + i for i in range(n)], "int8")
    put("i2", [-300 + i for i in range(n)], "int16")
    put("i4", [2**31 - 1 - i for i in range(n)], "int32")
    put(
        "i8", [2**53 + 1 + i for i in range(n)], "int64"
    )  # beyond float64's exact range
    put("u1", [250 + i % 6 for i in range(n)], "uint8")
    put("u2", [65535 - i for i in range(n)], "uint16")
    put("u4", [2**32 - 1 - i for i in range(n)], "uint32")
    put("u8", [2**64 - 1 - i for i in range(n)], "uint64")
    u8m = [2**64 - 1 if i in (0, 5) else 2**63 + i for i in range(n)]
    put(
        "u8m",
        u8m,
        "uint64",
        {"_FillValue": 2**64 - 1},  # a fill beyond what int64 or float64 can hold
        [None if v == 2**64 - 1 else v for v in u8m],
    )
    put("b", [bool(i % 3) for i in range(n)], "bool")
    put("f2", [0.5 * i for i in range(n)], "float16")
    put("f4", [0.25 * i for i in range(n)], "float32")
    put("f8", [0.1 * i for i in range(n)], "float64")
    masked = [5, -1, 7, -1, 9, 10, 11, 12, -1, 14, 15, 16]
    put(
        "m2",
        masked,
        "int16",
        {"_FillValue": -1},
        [None if v == -1 else v for v in masked],
    )
    fl = [1.5, nan, -9999.0, 2.5, -9999.0, 3.5, nan, 4.5, 5.5, 6.5, 7.5, 8.5]
    put(
        "mf",
        fl,
        "float64",
        {"_FillValue": -9999.0},
        [None if v == -9999.0 else v for v in fl],
    )
    packed = [2, -1, 4, 6, 8, -1, 12, 14, 16, 18, 20, 22]
    put(
        "pk",
        packed,
        "int16",
        {"_FillValue": -1, "scale_factor": 0.5, "add_offset": 10.0},
        [None if v == -1 else v * 0.5 + 10.0 for v in packed],
    )
    return path, expected
