"""run_sfm sizes its work by partition: a keyed extraction fan-out (one partition count per image), a
keyed matching fan-out, and no session conf (a Serverless session refuses one, and the lightweight
package sets none).
"""

import inspect

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.conf import RuntimeConfig

from databricks.labs.gbx.rasterx import sfm


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    wh = tmp_path_factory.mktemp("sfm-part-wh")
    return (
        SparkSession.builder.master("local[4]")
        .appName("sfm-partitioning")
        .config("spark.sql.warehouse.dir", str(wh))
        .getOrCreate()
    )


def _rows_per_partition(df):
    return [
        r["count"]
        for r in df.withColumn("pid", F.spark_partition_id())
        .groupBy("pid")
        .count()
        .collect()
    ]


@pytest.mark.parametrize("n", [1, 2, 7, 25, 100, 333])
def test_extraction_fan_out_is_keyed_on_source_and_honours_the_count(spark, n):
    # an input spread over a few partitions, as a real df_qc is
    df = spark.createDataFrame([(f"/v/img_{i:05d}.jpg",) for i in range(n)], ["source"])
    out = sfm._one_image_per_partition(df.repartition(5), n)
    plan = out._jdf.queryExecution().executedPlan().toString()
    # one partition is Spark's SinglePartition exchange; more are hash-partitioned on the source
    expected = "SinglePartition" if n == 1 else "hashpartitioning(source#"
    assert expected in plan and (n == 1 or f", {n})" in plan)
    assert out.rdd.getNumPartitions() == n
    assert sorted(r.source for r in out.collect()) == sorted(
        r.source for r in df.collect()
    )
    assert out.columns == ["source"]


def test_extraction_fan_out_spreads_images_over_many_partitions(spark):
    n = 64
    df = spark.createDataFrame([(f"/v/img_{i:05d}.jpg",) for i in range(n)], ["source"])
    counts = _rows_per_partition(sfm._one_image_per_partition(df, n))
    assert sum(counts) == n and len(counts) > n // 3


def test_matching_fan_out_is_keyed_on_the_pair_and_honours_the_count(spark):
    pairs = spark.createDataFrame(
        [(f"/v/{i}.jpg", f"/v/{i + 1}.jpg", b"kp") for i in range(500)],
        "src1 string, src2 string, kp1 binary",
    )
    out = sfm._fan_out_pairs(pairs, 8)
    plan = out._jdf.queryExecution().executedPlan().toString()
    assert "hashpartitioning(src1" in plan and ", 8)" in plan
    assert len(_rows_per_partition(out)) == 8


def test_no_session_conf_machinery_is_left():
    assert not hasattr(sfm, "_batch_of_one")
    assert not hasattr(sfm, "_one_image_per_batch")
    assert not hasattr(sfm.run_sfm, "__wrapped__")
    assert list(inspect.signature(sfm.run_sfm).parameters)[0] == "spark"
    assert "conf.set" not in inspect.getsource(sfm)


def test_run_sfm_sets_no_spark_conf_and_fans_out_by_image_count(
    spark, tmp_path, monkeypatch
):
    sets, fanned = [], []
    real_set = RuntimeConfig.set
    monkeypatch.setattr(
        RuntimeConfig,
        "set",
        lambda self, *a, **k: (sets.append(a), real_set(self, *a, **k))[1],
    )
    real_fan = sfm._one_image_per_partition
    monkeypatch.setattr(
        sfm,
        "_one_image_per_partition",
        lambda df, n, *a, **k: (fanned.append(n), real_fan(df, n, *a, **k))[1],
    )
    sources = [str(tmp_path / f"missing_{i}.jpg") for i in range(4)]
    df = spark.createDataFrame([(s,) for s in sources], ["source"])
    with pytest.raises(RuntimeError):  # no such images: extraction yields nothing
        sfm.run_sfm(
            spark,
            df,
            str(tmp_path),
            str(tmp_path / "out"),
            feature_table="sfm_part_feat",
            match_table="sfm_part_match",
            max_pair_dist_m=50.0,
            force_features=True,
        )
    assert sets == []
    assert fanned == [4]
