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
    assert pa.types.is_timestamp(batches[0].schema.field("seen").type)
