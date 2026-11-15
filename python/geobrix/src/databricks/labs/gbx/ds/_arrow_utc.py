"""Spark -> Arrow types that never need a session timezone.

PySpark's ``to_arrow_type`` / ``to_arrow_schema`` type a ``TimestampType`` column with
the session timezone. A Spark-Connect / Serverless DataSource worker has none: PySpark
4.1 and the Databricks builds take ``timezone=`` and ``assert`` it is set, while 4.0
always emits UTC. Every DataSource that maps a Spark schema to Arrow goes through
``arrow_type_utc`` / ``arrow_schema_utc`` here, so a timestamp is one UTC Arrow type on
every PySpark.

``to_utc`` is the value side: PySpark hands a Python worker a ``TimestampType`` value as a
naive ``datetime`` in the worker's local time, so declaring the column UTC without
converting it shifts every value by the worker's offset on a worker that is not on UTC.
"""

import datetime
import inspect

import pyarrow as pa
from pyspark.sql.types import StructType, TimestampType


def arrow_type_utc(dt) -> "pa.DataType":
    """Arrow type of a Spark ``DataType``; ``TimestampType`` is ``timestamp[us, tz=UTC]``."""
    if isinstance(dt, TimestampType):
        return pa.timestamp("us", tz="UTC")
    from pyspark.sql.pandas.types import to_arrow_type

    # A timestamp nested in a struct/array still goes through PySpark: pass the zone where
    # it is a parameter (>= 4.1); 4.0 has no such parameter and always emits UTC.
    if "timezone" in inspect.signature(to_arrow_type).parameters:
        return to_arrow_type(dt, timezone="UTC")
    return to_arrow_type(dt)


def arrow_schema_utc(schema: StructType) -> "pa.Schema":
    """Arrow schema of a Spark ``StructType`` (nullability kept), timestamps in UTC."""
    return pa.schema(
        [
            pa.field(f.name, arrow_type_utc(f.dataType), nullable=f.nullable)
            for f in schema.fields
        ]
    )


def to_utc(value):
    """A ``datetime`` as an aware UTC ``datetime`` (naive means the worker's local time).

    ``astimezone`` on a naive value makes CPython probe the local offset up to a day either
    way, which raises ``ValueError`` / ``OverflowError`` at the ends of the datetime range
    (``9999-12-31``, ``0001-01-01``): common sentinels (open end dates, minimum dates) that
    GDAL stores exactly. There the offset is immaterial, so such a naive value is read as UTC.
    """
    if not isinstance(value, datetime.datetime):
        return value
    try:
        return value.astimezone(datetime.timezone.utc)
    except (ValueError, OverflowError):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value  # an aware value at the range end: keep its own offset
