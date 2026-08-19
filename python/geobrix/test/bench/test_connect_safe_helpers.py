"""Connect-safety unit tests for the light spark-path bench helpers.

The spark-path bench runner used SparkContext + RDD APIs the Spark Connect
(Serverless) surface omits. `runner.py` now routes those through three helpers
that branch on `_is_spark_connect`. These tests pin BOTH branches:

- the classic branch runs against the real ``local[2]`` Spark fixture (so the
  SparkContext / ``.rdd`` code paths are actually executed), and
- the Connect branch's DataFrame-native replacement is forced on the same real
  session (by monkeypatching `_is_spark_connect` -> True) so we prove the
  ``spark_partition_id()`` + ``dropDuplicates`` + drop-tag logic executes and
  preserves the schema. Every op it uses (spark_partition_id, dropDuplicates,
  drop, conf.get) is part of the Spark Connect DataFrame surface.

We can't stand up a real Serverless/Connect session in unit tests, so the module
detection itself is checked with lightweight fakes whose ``type(x).__module__``
matches the real Connect vs classic session modules.
"""

from pyspark.sql import functions as F

from databricks.labs.gbx.bench.runner import (
    _bench_parallelism,
    _is_spark_connect,
    _one_row_per_partition,
)


# --- module detection -------------------------------------------------------
class _FakeConnectSession:  # placed so __module__ is this test module; patched below
    pass


def _obj_with_module(module_name):
    """Return an instance whose type().__module__ == module_name."""
    t = type("S", (), {})
    t.__module__ = module_name
    return t()


def test_is_spark_connect_detects_connect_module():
    connect_like = _obj_with_module("pyspark.sql.connect.session")
    classic_like = _obj_with_module("pyspark.sql.session")
    assert _is_spark_connect(connect_like) is True
    assert _is_spark_connect(classic_like) is False


def test_is_spark_connect_false_on_real_local_session(spark):
    # The real fixture is a classic SparkSession, not Connect.
    assert _is_spark_connect(spark) is False


# --- _bench_parallelism -----------------------------------------------------
def test_bench_parallelism_classic_uses_default_parallelism(spark):
    # local[2] -> defaultParallelism == 2; helper must return exactly that.
    assert _bench_parallelism(spark) == spark.sparkContext.defaultParallelism


def test_bench_parallelism_connect_reads_shuffle_partitions():
    class _Conf:
        def get(self, key):
            assert key == "spark.sql.shuffle.partitions"
            return "16"

    class _ConnectSpark:
        conf = _Conf()

        @property
        def sparkContext(self):  # must NOT be touched on the Connect branch
            raise AssertionError("sparkContext accessed on Connect branch")

    _ConnectSpark.__module__ = "pyspark.sql.connect.session"
    assert _bench_parallelism(_ConnectSpark()) == 16


def test_bench_parallelism_connect_falls_back_to_default_when_conf_raises():
    class _Conf:
        def get(self, key):
            raise RuntimeError("CONFIG_NOT_AVAILABLE")

    class _ConnectSpark:
        conf = _Conf()

    _ConnectSpark.__module__ = "pyspark.sql.connect.session"
    assert _bench_parallelism(_ConnectSpark(), default=8) == 8
    assert _bench_parallelism(_ConnectSpark(), default=13) == 13


# --- _one_row_per_partition -------------------------------------------------
def _make_partitioned_df(spark, n, parts):
    return (
        spark.range(0, n)
        .select(F.col("id").alias("v"), (F.col("id") % 5).alias("g"))
        .repartition(parts, F.col("v"))
    )


def test_one_row_per_partition_classic_branch(spark):
    df = _make_partitioned_df(spark, 40, 4)
    warm = _one_row_per_partition(spark, df)
    # Schema preserved (no helper column leaks).
    assert warm.columns == df.columns
    rows = warm.collect()
    # One row per non-empty partition: <= parts, >= 1, and no duplicate source rows.
    assert 1 <= len(rows) <= 4
    vs = [r["v"] for r in rows]
    assert len(vs) == len(set(vs))


def test_one_row_per_partition_connect_branch_executes(spark, monkeypatch):
    # Force the Connect branch onto the real local session to prove the
    # DataFrame-native replacement executes + preserves schema.
    import databricks.labs.gbx.bench.runner as runner_mod

    monkeypatch.setattr(runner_mod, "_is_spark_connect", lambda _s: True)
    df = _make_partitioned_df(spark, 40, 4)
    warm = _one_row_per_partition(spark, df)
    assert warm.columns == df.columns  # _bench_pid tag dropped
    rows = warm.collect()
    # spark_partition_id + dropDuplicates -> exactly one row per distinct partition id.
    assert 1 <= len(rows) <= 4
    vs = [r["v"] for r in rows]
    assert len(vs) == len(set(vs))
