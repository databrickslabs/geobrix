"""Test measure_parallelism: Connect-safe (input_partitions, slots_available)."""


def test_measure_parallelism_reports_partitions_and_slots(spark):
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench.readers import measure_parallelism

    df = spark.range(300).repartition(3, F.col("id"))
    parts, slots = measure_parallelism(spark, df)
    assert parts == 3
    assert slots >= 1
