"""Smoke test of the local Spark Connect tier (see spark_connect_local in conftest.py)."""


def test_a_local_connect_session_runs_a_dataframe_op(spark_connect_local):
    from pyspark.sql.connect.session import SparkSession as ConnectSession

    assert isinstance(
        spark_connect_local, ConnectSession
    )  # a Connect client, not classic
    rows = spark_connect_local.range(5).selectExpr("id * 2 AS d").collect()
    assert [r.d for r in rows] == [0, 2, 4, 6, 8]
