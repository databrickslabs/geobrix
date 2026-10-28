import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-manager")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
