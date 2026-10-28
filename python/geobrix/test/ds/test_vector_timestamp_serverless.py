"""Timestamp columns work on Spark-Connect / Serverless workers.

There pyspark's ``to_arrow_type`` / ``to_arrow_schema`` take ``timezone=`` and assert
it for ``TimestampType`` (a worker has no session timezone), so a vector reader or
writer that lets pyspark type a timestamp column fails there. The local PySpark 4.0
always emits UTC and never asserts, so the ``serverless_arrow`` fixture reproduces
the newer behaviour.
"""

import datetime

import pyarrow as pa
import pyogrio
import pytest
from pyspark.sql.types import (
    BinaryType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from shapely import Point, to_wkb

from databricks.labs.gbx.ds.vector import _writer_arrow_table


@pytest.fixture
def serverless_arrow(monkeypatch):
    import pyspark.sql.pandas.types as pst

    real_type = pst.to_arrow_type

    def to_arrow_type(
        dt,
        error_on_duplicated_field_names_in_struct=False,
        timezone=None,
        prefers_large_types=False,
    ):
        if isinstance(dt, TimestampType):
            assert timezone is not None
            return pa.timestamp("us", tz=timezone)
        return real_type(dt)

    def to_arrow_schema(
        schema,
        error_on_duplicated_field_names_in_struct=False,
        timezone=None,
        prefers_large_types=False,
    ):
        return pa.schema(
            [
                pa.field(
                    f.name, to_arrow_type(f.dataType, timezone=timezone), f.nullable
                )
                for f in schema.fields
            ]
        )

    monkeypatch.setattr(pst, "to_arrow_type", to_arrow_type)
    monkeypatch.setattr(pst, "to_arrow_schema", to_arrow_schema)


def test_writer_arrow_table_types_timestamp_as_utc(serverless_arrow):
    schema = StructType(
        [
            StructField("seen", TimestampType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    cols = {
        "seen": [datetime.datetime(2020, 1, 2, 3, 4, 5)],
        "geom_0": [bytes(to_wkb(Point(1.0, 2.0)))],
        "geom_0_srid": ["4326"],
        "geom_0_srid_proj": [""],
    }
    tbl = _writer_arrow_table(cols, schema, "geom_0")
    assert tbl.schema.field("seen").type == pa.timestamp("us", tz="UTC")


@pytest.mark.parametrize(
    "fmt,driver,name",
    [("gpkg_gbx", "GPKG", "ts.gpkg"), ("file_gdb_gbx", "OpenFileGDB", "ts.gdb")],
)
def test_reader_streams_timestamp_column(tmp_path, serverless_arrow, fmt, driver, name):
    from databricks.labs.gbx.ds.register import _SOURCES

    path = str(tmp_path / name)
    seen = pa.array(
        [
            datetime.datetime(2020, 1, 2, 3, 4, 5),
            datetime.datetime(2021, 6, 7, 8, 9, 10),
        ],
        pa.timestamp("ms"),
    )
    tbl = pa.table(
        {
            "seen": seen,
            "geom": pa.array([to_wkb(Point(i, i)) for i in range(2)], pa.binary()),
        }
    )
    pyogrio.write_arrow(
        tbl,
        path,
        driver=driver,
        geometry_name="geom",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    rdr = {s.name(): s for s in _SOURCES}[fmt]({"path": path}).reader(None)
    batches = [b for p in rdr.partitions() for b in rdr.read(p)]
    assert sum(b.num_rows for b in batches) == 2
    # UTC-typed, and the values are the instants that were written
    assert batches[0].schema.field("seen").type.tz == "UTC"
    utc = datetime.timezone.utc
    assert pa.concat_arrays([b.column("seen") for b in batches]).to_pylist() == [
        datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=utc),
        datetime.datetime(2021, 6, 7, 8, 9, 10, tzinfo=utc),
    ]


# ---------------------------------------------------------------------------
# One shared timestamp -> UTC mapping (ds/_arrow_utc.py), used by every DataSource
# ---------------------------------------------------------------------------

import os  # noqa: E402
import time  # noqa: E402

from databricks.labs.gbx.ds import _arrow_utc  # noqa: E402


@pytest.fixture
def new_york(monkeypatch):
    """The process runs on US Eastern time (UTC-5 in winter, UTC-4 in summer)."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    yield
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


def test_arrow_type_utc_needs_no_session_timezone(serverless_arrow):
    assert _arrow_utc.arrow_type_utc(TimestampType()) == pa.timestamp("us", tz="UTC")
    schema = StructType(
        [StructField("seen", TimestampType(), False), StructField("n", StringType())]
    )
    got = _arrow_utc.arrow_schema_utc(schema)
    assert got.field("seen").type == pa.timestamp("us", tz="UTC")
    assert not got.field("seen").nullable  # nullability is kept
    assert got.field("n").type == pa.string()


def test_arrow_type_utc_passes_the_zone_where_pyspark_takes_it(monkeypatch):
    """A timestamp nested in another type still goes through PySpark: it is given the
    zone when PySpark takes one (>= 4.1) and nothing on 4.0, which always emits UTC."""
    import pyspark.sql.pandas.types as pst
    from pyspark.sql.types import ArrayType

    seen = []

    def with_zone(dt, error_on_duplicated_field_names_in_struct=False, timezone=None):
        seen.append(timezone)
        return pa.list_(pa.timestamp("us", tz=timezone))

    monkeypatch.setattr(pst, "to_arrow_type", with_zone)
    assert _arrow_utc.arrow_type_utc(ArrayType(TimestampType())) == pa.list_(
        pa.timestamp("us", tz="UTC")
    )
    assert seen == ["UTC"]

    def without_zone(dt, error_on_duplicated_field_names_in_struct=False):
        seen.append("no-zone")
        return pa.list_(pa.timestamp("us", tz="UTC"))

    monkeypatch.setattr(pst, "to_arrow_type", without_zone)
    _arrow_utc.arrow_type_utc(ArrayType(TimestampType()))
    assert seen == ["UTC", "no-zone"]


def test_to_utc_reads_a_naive_datetime_as_local_time(new_york):
    assert _arrow_utc.to_utc(datetime.datetime(2020, 1, 2, 3, 4, 5)) == (
        datetime.datetime(2020, 1, 2, 8, 4, 5, tzinfo=datetime.timezone.utc)
    )
    assert _arrow_utc.to_utc(datetime.datetime(2020, 7, 1, 12, 0, 0)) == (
        datetime.datetime(2020, 7, 1, 16, 0, 0, tzinfo=datetime.timezone.utc)
    )


def test_to_utc_leaves_aware_datetimes_and_other_values_alone(new_york):
    aware = datetime.datetime(
        2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    assert _arrow_utc.to_utc(aware) == aware  # the same instant
    assert _arrow_utc.to_utc(aware).utcoffset() == datetime.timedelta(0)
    assert _arrow_utc.to_utc(None) is None
    assert _arrow_utc.to_utc("x") == "x"


def test_every_consumer_maps_timestamps_through_the_shared_helper(monkeypatch):
    """exif, read_paths and the vector writer/reader each type a timestamp through
    ``_arrow_utc.arrow_type_utc`` rather than keeping their own mapping."""
    from databricks.labs.gbx.ds import exif, paths

    calls = []
    real = _arrow_utc.arrow_type_utc

    def spy(dt):
        calls.append(type(dt).__name__)
        return real(dt)

    monkeypatch.setattr(_arrow_utc, "arrow_type_utc", spy)
    ts_schema = StructType([StructField("seen", TimestampType(), True)])

    exif._exif_arrow_schema(ts_schema)
    assert "TimestampType" in calls
    calls.clear()

    paths._arrow_schema(ts_schema)
    assert "TimestampType" in calls
    calls.clear()

    _writer_arrow_table(
        {"seen": [None], "geom_0": [None]},
        StructType(
            [
                StructField("seen", TimestampType(), True),
                StructField("geom_0", BinaryType(), True),
            ]
        ),
        "geom_0",
    )
    assert "TimestampType" in calls


# ---------------------------------------------------------------------------
# a timestamp is exact when the worker is not on UTC
# ---------------------------------------------------------------------------


def test_writer_arrow_table_converts_local_time_to_utc(serverless_arrow, new_york):
    schema = StructType(
        [
            StructField("seen", TimestampType(), True),
            StructField("geom_0", BinaryType(), True),
        ]
    )
    cols = {
        "seen": [datetime.datetime(2020, 1, 2, 3, 4, 5), None],
        "geom_0": [b"", b""],
    }
    tbl = _writer_arrow_table(cols, schema, "geom_0")
    assert tbl.column("seen").to_pylist() == [
        datetime.datetime(2020, 1, 2, 8, 4, 5, tzinfo=datetime.timezone.utc),
        None,
    ]


@pytest.mark.parametrize(
    "driver,name", [("GPKG", "ts.gpkg"), ("OpenFileGDB", "ts.gdb")]
)
def test_a_written_timestamp_is_exact_on_a_non_utc_worker(
    tmp_path, serverless_arrow, new_york, driver, name
):
    import pandas as pd

    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("seen", TimestampType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    rows = [
        (
            datetime.datetime(2020, 1, 2, 3, 4, 5),
            bytearray(to_wkb(Point(1, 2))),
            "4326",
            "",
        )
    ]
    w = VectorGbxWriter(str(tmp_path / name), schema, driver, {}, True)
    w.commit([w.write(iter(rows))])
    got = pyogrio.read_dataframe(w.path)["seen"].iloc[0]
    if got.tzinfo is None:  # a date-time field carries no offset: UTC wall clock
        got = got.tz_localize("UTC")
    assert got == pd.Timestamp("2020-01-02 08:04:05", tz="UTC")


# ---------------------------------------------------------------------------
# netcdf_gbx point writer: the same Serverless hazard
# ---------------------------------------------------------------------------


def test_netcdf_point_writer_types_a_timestamp_attribute_as_utc(
    tmp_path, serverless_arrow, new_york
):
    import pyarrow.feather as feather
    from pyspark.sql import Row
    from pyspark.sql.types import DoubleType

    from databricks.labs.gbx.ds._write_netcdf import NetcdfVectorGbxWriter

    schema = StructType(
        [
            StructField("ch4", DoubleType(), True),
            StructField("seen", TimestampType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    w = NetcdfVectorGbxWriter(
        {"path": str(tmp_path / "out"), "singleFile": "true"}, schema, True
    )
    row = Row(
        ch4=1.5,
        seen=datetime.datetime(2020, 1, 2, 3, 4, 5),
        geom_0=bytearray(to_wkb(Point(10.0, 50.0))),
        geom_0_srid="4326",
        geom_0_srid_proj="",
    )
    msg = w.write(iter([row]))
    tbl = feather.read_table(msg.frag_path)
    assert tbl.schema.field("seen").type == pa.timestamp("us", tz="UTC")
    assert tbl.column("seen").to_pylist() == [
        datetime.datetime(2020, 1, 2, 8, 4, 5, tzinfo=datetime.timezone.utc)
    ]


# ---------------------------------------------------------------------------
# the ends of the datetime range (open end dates, minimum dates) still write
# ---------------------------------------------------------------------------


@pytest.fixture(params=["UTC", "America/New_York"])
def local_tz(request):
    """The process runs on UTC, then on US Eastern time."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield request.param
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


_UTC = datetime.timezone.utc
_RANGE_ENDS = [
    datetime.datetime(9999, 12, 31, 23, 59, 59),
    datetime.datetime(9999, 12, 31, 0, 0, 0),
    datetime.datetime(1, 1, 1, 0, 0, 0),
    datetime.datetime(1, 1, 1, 23, 59, 59),
]


@pytest.mark.parametrize("value", _RANGE_ENDS, ids=lambda d: d.isoformat())
def test_to_utc_survives_the_ends_of_the_datetime_range(local_tz, value):
    """``astimezone`` on a naive datetime probes the local offset and raises ``year
    10000 / year 0 is out of range`` at the range ends. These sentinels are common (open
    end dates, minimum dates), so they are read as UTC: the offset is immaterial there.
    """
    assert _arrow_utc.to_utc(value) == value.replace(tzinfo=_UTC)


def test_to_utc_leaves_an_aware_range_end_alone(local_tz):
    aware = datetime.datetime(9999, 12, 31, 23, 59, 59, tzinfo=_UTC)
    assert _arrow_utc.to_utc(aware) == aware


@pytest.mark.parametrize(
    "value,expected",
    [
        (datetime.datetime(9999, 12, 31, 23, 59, 59), "9999-12-31T23:59:59Z"),
        (datetime.datetime(1, 1, 1, 0, 0, 0), "0001-01-01T00:00:00Z"),
    ],
    ids=["max", "min"],
)
@pytest.mark.parametrize(
    "driver,name", [("GPKG", "ts.gpkg"), ("OpenFileGDB", "ts.gdb")]
)
def test_range_end_timestamps_write_and_read_back_unchanged(
    tmp_path, local_tz, driver, name, value, expected
):
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("seen", TimestampType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    rows = [(value, bytearray(to_wkb(Point(1, 2))), "4326", "")]
    w = VectorGbxWriter(str(tmp_path / name), schema, driver, {}, True)
    w.commit([w.write(iter(rows))])
    got = pyogrio.raw.read(w.path, datetime_as_string=True)[3][0]
    assert [str(x) for x in got] == [expected]
