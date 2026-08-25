import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("pyvx-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield s
    s.stop()
