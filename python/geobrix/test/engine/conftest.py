import pytest

from ..manager.conftest import STAGED_JARS, payload_jvm_conf


@pytest.fixture(scope="session")
def spark8():
    """A local[8] session (eight concurrent tasks), with the staged JAR and the payload's GDAL in its JVM when both
    are there (see manager/conftest.payload_jvm_conf)."""
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[8]")
        .appName("gbx-engine")
        .config("spark.sql.shuffle.partitions", "8")
    )
    if STAGED_JARS and SparkSession.getActiveSession() is None:
        b = b.config("spark.jars", str(STAGED_JARS[-1]))
        for k, v in payload_jvm_conf().items():
            b = b.config(k, v)
    yield b.getOrCreate()
