"""Arrow tables into Spark without silently losing rows.

``spark.createDataFrame(pyarrow.Table)`` ends at the first zero-row record batch and drops
every row after it, with no error: Spark's JVM Arrow iterator (``ArrowConverters``) reads
batches until one is empty. A table concatenated from parts has such a chunk whenever one
part came out empty, so any Arrow table GeoBrix builds from parts must reach Spark through
:func:`to_spark_df`, not through ``createDataFrame`` directly.

Measured: classic PySpark 4.0.0 loses the rows; Serverless env 6 (Spark Connect, PySpark
4.3 dev) keeps them. The helper is a cheap no-op where Spark is fixed, and the guard where it
is not, so callers do not branch on the runtime.

Light by design: no PySpark import, no ``sparkContext``/``_jvm``/``.rdd``/``spark.conf``,
so it is safe on Serverless and Spark Connect. ``pyarrow`` is imported when a table is
rebuilt, not at import time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa


def drop_empty_chunks(table: "pa.Table") -> "pa.Table":
    """``table`` without its zero-row record batches; the same rows, order and schema.

    Returns ``table`` itself when it has none, so the common case copies nothing. Only
    chunk boundaries change: the buffers behind the rows are shared, not copied.
    """
    import pyarrow as pa

    batches = table.to_batches()
    if all(b.num_rows for b in batches):
        return table
    return pa.Table.from_batches(
        [b for b in batches if b.num_rows], schema=table.schema
    )


def to_spark_df(spark: Any, table: "pa.Table", schema: Any = None):
    """A Spark DataFrame of ``table``, with every row.

    ``schema`` is anything ``createDataFrame`` takes (a ``StructType``, a DDL string or a
    list of column names); ``None`` infers it from the Arrow schema.
    """
    return spark.createDataFrame(drop_empty_chunks(table), schema=schema)
