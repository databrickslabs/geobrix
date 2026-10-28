from pathlib import Path

import pytest

# The staged assembly JAR (gbx-test-parity.sh builds and stages it); absent on the host and in light CI.
STAGED_JARS = sorted(
    (Path(__file__).resolve().parents[2] / "lib").glob(
        "geobrix-*-jar-with-dependencies.jar"
    )
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-manager")
        .config("spark.sql.shuffle.partitions", "2")
    )
    if STAGED_JARS and SparkSession.getActiveSession() is None:
        # spark.jars only takes effect when the JVM starts, so it is set before any session exists.
        b = b.config("spark.jars", str(STAGED_JARS[-1]))
    yield b.getOrCreate()


@pytest.fixture(scope="session")
def jvm_spark(spark):
    """The session when it carries the staged JAR; else the test skips."""
    if not (STAGED_JARS and str(STAGED_JARS[-1]) in spark.conf.get("spark.jars", "")):
        pytest.skip("no staged JAR in this session (run gbx-test-parity.sh)")
    return spark
