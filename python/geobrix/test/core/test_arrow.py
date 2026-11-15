"""Arrow tables reach Spark without silently losing rows (``core._arrow``).

PySpark 4.0's ``spark.createDataFrame(pyarrow.Table)`` ends at the first zero-row record
batch and drops every row after it, with no error. A table concatenated from parts has
such a chunk whenever one part came out empty. ``to_spark_df`` is the one way GeoBrix hands
an Arrow table to Spark.

The helper's matrix is here, once. Each consumer has ONE wiring test that fails if the
consumer goes back to calling ``createDataFrame`` on the raw table.
"""

import pyarrow as pa
import pytest

from databricks.labs.gbx.core._arrow import drop_empty_chunks, to_spark_df


def _ints(*values):
    return pa.table({"k": pa.array(values, pa.int64())})


def _empty():
    return pa.table({"k": pa.array([], pa.int64())})


def _chunk_sizes(table):
    return [len(c) for c in table.column("k").chunks]


def _batch_sizes(table):
    return [b.num_rows for b in table.to_batches()]


# --- the helper's matrix (once) -----------------------------------------------------------


@pytest.mark.parametrize(
    "parts, expected",
    [
        pytest.param(
            [_ints(1, 2), _empty(), _ints(3, 4, 5)], [1, 2, 3, 4, 5], id="middle"
        ),
        pytest.param([_empty(), _ints(1, 2), _ints(3)], [1, 2, 3], id="leading"),
        pytest.param([_ints(1, 2), _ints(3), _empty()], [1, 2, 3], id="trailing"),
        pytest.param(
            [_empty(), _ints(1), _empty(), _empty(), _ints(2, 3), _empty()],
            [1, 2, 3],
            id="several",
        ),
        pytest.param([_ints(1), _ints(2), _ints(3)], [1, 2, 3], id="none empty"),
        pytest.param([_empty(), _empty()], [], id="all empty"),
    ],
)
def test_no_zero_row_batch_survives_and_no_row_is_lost(parts, expected):
    table = pa.concat_tables(parts)
    out = drop_empty_chunks(table)
    assert out.column("k").to_pylist() == expected  # the same rows, in order
    assert all(n > 0 for n in _batch_sizes(out))


def test_an_empty_chunk_is_present_before_and_gone_after():
    """The premise: ``concat_tables`` keeps a zero-row chunk; the helper drops it."""
    table = pa.concat_tables([_ints(1, 2), _empty(), _ints(3, 4, 5)])
    assert _chunk_sizes(table) == [2, 0, 3]
    assert _chunk_sizes(drop_empty_chunks(table)) == [2, 3]


def test_the_schema_with_its_metadata_is_kept_when_every_chunk_is_empty():
    schema = pa.schema(
        [pa.field("k", pa.int64(), nullable=False, metadata={b"unit": b"m"})],
        metadata={b"origin": b"test"},
    )
    table = pa.concat_tables([schema.empty_table(), schema.empty_table()])
    out = drop_empty_chunks(table)
    assert out.num_rows == 0
    assert out.schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {"s": ["a", None, "c"], "b": [b"x", b"yy", None]}, id="string-binary"
        ),
        pytest.param(
            {"l": [[1, 2], [], None], "d": [1.5, None, 2.5]}, id="list-double"
        ),
        pytest.param(
            {"st": [{"a": 1, "b": "p"}, None, {"a": 3, "b": "r"}]}, id="struct"
        ),
    ],
)
def test_nested_and_nullable_columns_are_kept_exactly(data):
    part = pa.table(data)
    table = pa.concat_tables([part, part.schema.empty_table(), part])
    out = drop_empty_chunks(table)
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == part.to_pylist() * 2


def test_a_table_without_an_empty_chunk_is_returned_as_it_is():
    table = pa.concat_tables([_ints(1, 2), _ints(3)])
    assert drop_empty_chunks(table) is table  # nothing to rebuild, nothing copied


class _RecordingSpark:
    """Stands in for a SparkSession: records what ``createDataFrame`` was handed."""

    def __init__(self):
        self.calls = []

    def createDataFrame(self, data, schema=None):  # noqa: N802 (PySpark's name)
        self.calls.append((data, schema))
        return "df"


def test_to_spark_df_hands_spark_the_table_without_empty_chunks_and_the_schema():
    spark = _RecordingSpark()
    table = pa.concat_tables([_ints(1, 2), _empty(), _ints(3)])
    assert to_spark_df(spark, table, schema="k BIGINT") == "df"
    ((data, schema),) = spark.calls
    assert data.column("k").to_pylist() == [1, 2, 3]
    assert all(n > 0 for n in _batch_sizes(data))
    assert schema == "k BIGINT"


def test_to_spark_df_schema_defaults_to_none_so_spark_infers_it():
    spark = _RecordingSpark()
    to_spark_df(spark, _ints(1))
    assert spark.calls[0][1] is None


# --- real Spark ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    import logging

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    return (
        SparkSession.builder.master("local[2]")
        .appName("gbx-core-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _with_empty_chunks():
    return pa.concat_tables(
        [_empty(), _ints(1, 2), _empty(), _ints(3, 4, 5), _empty(), _ints(6)]
    )


def test_spark_keeps_every_row_when_the_table_has_empty_chunks(spark):
    table = _with_empty_chunks()
    df = to_spark_df(spark, table)
    assert sorted(r["k"] for r in df.collect()) == [1, 2, 3, 4, 5, 6]


def test_spark_takes_an_explicit_schema_and_an_all_empty_table(spark):
    from pyspark.sql import types as T

    schema = T.StructType([T.StructField("k", T.LongType(), True)])
    assert (
        to_spark_df(spark, pa.concat_tables([_empty(), _empty()]), schema).count() == 0
    )
    df = to_spark_df(spark, _with_empty_chunks(), schema)
    assert df.schema == schema
    assert df.count() == 6


def test_the_premise_raw_create_dataframe_drops_rows_after_an_empty_chunk(spark):
    """Why the helper exists. When a PySpark release fixes this, the test skips and the
    helper becomes a no-op worth retiring."""
    table = _with_empty_chunks()
    raw = spark.createDataFrame(table).count()
    if raw == table.num_rows:
        pytest.skip("this PySpark keeps rows after a zero-row chunk")
    assert raw < table.num_rows  # rows lost, and no error raised


# --- consumers: one wiring test each ------------------------------------------------------


def test_the_zarr_manifest_view_is_created_from_a_table_without_empty_chunks():
    """``_with_view`` (every manifest commit, key lookup and conflict check) must go
    through ``to_spark_df``: a raw ``createDataFrame`` here loses the commit's rows."""
    from databricks.labs.gbx.zarr import _manifest as M

    class _Spark(_RecordingSpark):
        def __init__(self):
            super().__init__()
            self.catalog = self

        def createDataFrame(self, data, schema=None):  # noqa: N802
            super().createDataFrame(data, schema)
            return self

        def createOrReplaceTempView(self, name):  # noqa: N802
            self.view = name

        def dropTempView(self, name):  # noqa: N802
            self.dropped = name

    schema = pa.schema([pa.field("kind", pa.string()), pa.field("key", pa.string())])
    part = pa.table({"kind": ["chunk"], "key": ["c/0"]}, schema=schema)
    rows = pa.concat_tables([part, schema.empty_table(), part, part])
    assert _batch_sizes(rows) == [1, 0, 1, 1]

    spark = _Spark()
    seen = {}
    M._with_view(spark, rows, schema, lambda view: seen.update(view=view))

    ((data, _),) = spark.calls
    assert data.num_rows == 3
    assert all(n > 0 for n in _batch_sizes(data))
    assert spark.dropped == seen["view"]  # the view is still dropped
