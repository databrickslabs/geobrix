"""The L1 engine on the JVM layouts a user runs it on: the staged JAR alone on the user's classpath, and on the
driver's system classpath. Each layout is one JVM, so each row runs in a subprocess.

The JAR bundles GDAL's Java classes from the native build that also makes the env wheel's JNI (pom.xml
gdal.java.version), so on these layouts the JVM runs the payload's GDAL (GBX_NATIVE_ROOT) and the L1 engine runs.
Neither layout sets the payload's lib/ on the library path: GDAL's classes load the first libgdalalljni.so there by
name, which in the dev container is the payload's (gbx:docker:start links /usr/lib/libgdalalljni.so to it). The
harness layout (conftest.payload_jvm_conf) is the one the other L1 rows run on.
"""

import os
import subprocess
import sys

import pytest

from ..manager.conftest import REPO, STAGED_JARS

SYSTEM_JNI = "/usr/lib/libgdalalljni.so"

# One script, run as `python -c SCRIPT <layout>` in a fresh JVM: the L1 engine's slope on a 64 x 64 DEM.
PROBE = r"""
import sys
import numpy as np
from pyspark.sql import SparkSession
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.manifest import TILE_SQL

layout = sys.argv[1]
conf = {
    "user-jars": {"spark.jars": sys.argv[2]},
    "sys-jar": {"spark.driver.extraClassPath": sys.argv[2]},
}[layout]
b = SparkSession.builder.master("local[1]").appName("gbx-layout-" + layout)
for k, v in conf.items():
    b = b.config(k, v)
spark = b.getOrCreate()
try:
    from databricks.labs.gbx import register

    register(spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": ("L1", "engine")})
    yy, xx = np.mgrid[0:64, 0:64].astype("float64")
    tile = oracles._tile(oracles._gtiff((0.5 * xx + 0.3 * yy + 100.0)[np.newaxis], "EPSG:3857"))
    spark.createDataFrame([(tile,)], f"tile {TILE_SQL}").createOrReplaceTempView("_layout_t")
    (r,) = spark.sql("SELECT gbx_rst_slope(tile, 'degrees') AS o FROM _layout_t").collect()
    o = r["o"]
    if o["raster"] is not None:
        print("L1 engine slope: OK", flush=True)
    else:
        print("L1 engine slope: ERROR-TILE " + str(dict(o["metadata"] or {}).get("last_error")), flush=True)
except Exception as exc:  # noqa: BLE001 - the refusal is the outcome under test
    print("L1 engine slope: RAISED " + str(exc).splitlines()[0][:400], flush=True)
"""


def _staged_jar():
    if not STAGED_JARS:
        pytest.skip(
            "PRECONDITION MISSING: no staged geobrix JAR (run gbx-test-parity.sh); layout rows not run"
        )
    if not os.environ.get("GBX_NATIVE_ROOT"):
        pytest.skip(
            "PRECONDITION MISSING: GBX_NATIVE_ROOT (the env wheel payload); layout rows not run"
        )
    # Neither layout puts the payload's lib/ on the JVM's library path, so the first libgdalalljni.so there must be
    # the payload's own (gbx:docker:start links SYSTEM_JNI to it once the env6 payload is built); another one is a
    # second GDAL, which GDALManager refuses.
    from databricks.labs.gbx import _native

    d = _native.native_dir(os.environ["GBX_NATIVE_ROOT"])
    jni = os.path.join(d or "", "lib", "libgdalalljni.so")
    if os.path.exists(SYSTEM_JNI) and os.path.realpath(SYSTEM_JNI) != os.path.realpath(
        jni
    ):
        pytest.skip(
            f"PRECONDITION MISSING: {SYSTEM_JNI} is not the payload's JNI ({jni}); run gbx:docker:start after "
            "building the env6 payload; layout rows not run"
        )
    return str(STAGED_JARS[-1])


@pytest.mark.integration
@pytest.mark.parametrize("layout", ["user-jars", "sys-jar"])
def test_the_l1_engine_runs_on_the_layout(layout):
    jar = _staged_jar()
    env = {**os.environ}
    proc = subprocess.run(
        [sys.executable, "-c", PROBE, layout, jar],
        capture_output=True,
        text=True,
        timeout=900,
        env=env,
        cwd=str(REPO),
    )
    out = proc.stdout + proc.stderr
    assert "L1 engine slope: OK" in proc.stdout, out[-3000:]
