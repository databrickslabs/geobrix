"""The L1 engine on the JVM layouts a user runs it on: the staged JAR alone on the user's classpath, and on the
driver's system classpath. Each layout is one JVM, so each row runs in a subprocess.

The JAR bundles GDAL's Java classes built against GDAL 3.11, so on these layouts GDALManager refuses the JVM's GDAL
("a different GDAL is ..."), and the L1 engine cannot run. The rows are strict xfails until B2's pom switch (Step 4)
makes the JAR carry the payload's GDAL 3.13.3 classes; then they XPASS, and the xfail goes. The harness layout
(conftest.payload_jvm_conf) is the one the green L1 rows run on.
"""

import os
import subprocess
import sys

import pytest

from ..manager.conftest import REPO, STAGED_JARS

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
    return str(STAGED_JARS[-1])


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason="needs the B2 pom switch (Step 4): the JAR bundles GDAL 3.11's Java classes, so GDALManager refuses "
    "the payload's GDAL on this layout",
)
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
