import os
from pathlib import Path

import pytest

# The staged assembly JAR (gbx-test-parity.sh builds and stages it); absent on the host and in light CI.
STAGED_JARS = sorted(
    (Path(__file__).resolve().parents[2] / "lib").glob(
        "geobrix-*-jar-with-dependencies.jar"
    )
)
REPO = Path(__file__).resolve().parents[4]


def payload_jvm_conf() -> dict:
    """The Spark conf that runs a test JVM on the env wheel payload's GDAL, when GBX_NATIVE_ROOT names a payload.

    The JAR's GDALManager loads the system JNI until it learns to load the payload's (a natives-plan task), so a
    session started with GBX_NATIVE_ROOT puts the payload's JNI first on the JVM's library path and the same
    build's GDAL Java classes ahead of the JAR's. The JNI the GDAL classes bind is then the payload's, the GDAL
    the light tier's osgeo runs; the system libgdal is still mapped (GDALManager loads it by path) but no
    class calls into it. Empty when there is no payload, or no Java classes built from it.
    """
    root = os.environ.get("GBX_NATIVE_ROOT")
    if not root:
        return {}
    from databricks.labs.gbx import _native, manifest

    d = _native.native_dir(root)
    if not d or not os.path.isfile(os.path.join(d, "lib", "libgdalalljni.so")):
        return {}
    arch = os.path.basename(d)
    jar = (
        REPO
        / "build/native/out"
        / arch
        / "stage-native/gdal-native/java"
        / f"gdal-{manifest.load().gdal.pin}.jar"
    )
    if not jar.is_file():
        return {}
    return {
        "spark.driver.extraLibraryPath": os.path.join(d, "lib"),
        "spark.driver.extraClassPath": str(jar),
    }


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
        for k, v in payload_jvm_conf().items():
            b = b.config(k, v)
    yield b.getOrCreate()


@pytest.fixture(scope="session")
def jvm_spark(spark):
    """The session when it carries the staged JAR; else the test skips."""
    if not (STAGED_JARS and str(STAGED_JARS[-1]) in spark.conf.get("spark.jars", "")):
        pytest.skip("no staged JAR in this session (run gbx-test-parity.sh)")
    return spark
