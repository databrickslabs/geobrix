"""Light raster writer round-trip pixel parity (Docker / integration).

Reads a real raster via the light reader, writes it via the light writer, and
re-reads — asserting the written tile decodes to the same pixels as the source.
The source raster is the pixel ground truth that both tiers must match (a live
side-by-side vs the heavy ``gtiff_gdal`` writer is skipped because the heavy GDAL
path does not run in the local dev container; the cluster is where heavy runs).

Runs in Docker only: needs the geobrix JAR for the light DS register + sample
data mounted at ``/Volumes``. Marked ``integration``; execute with::

    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/pyrx/ds/test_writer_parity.py \
        --with-integration --log writer-parity.log
"""

import logging
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio

pytestmark = pytest.mark.integration

SAMPLE = os.environ.get(
    "GBX_PARITY_SAMPLE",
    "/Volumes/main/default/test-data/geobrix-examples/london/sentinel2/london_sentinel2_red.tif",
)
REL_TOL = 1e-3
ABS_TOL = 1e-3
_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[3] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    if not os.path.exists(SAMPLE):
        pytest.skip(f"sample not mounted at {SAMPLE}")
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyrx-ds-writer-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.register import register

    register(session)
    yield session


def test_light_write_roundtrips_to_same_pixels_as_source(spark_with_jar, tmp_path):
    out_dir = str(tmp_path / "light_out")
    light = spark_with_jar.read.format("raster_gbx").option("virtualTiles", "false").load(SAMPLE)
    light.write.format("gtiff_gbx").mode("overwrite").save(out_dir)

    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert written, "light writer produced no files"
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        out_arr = ds.read()
    with rasterio.open(SAMPLE) as src:
        truth = src.read()

    assert (
        out_arr.shape == truth.shape
    ), f"shape differs: {out_arr.shape} vs {truth.shape}"
    np.testing.assert_allclose(out_arr, truth, rtol=REL_TOL, atol=ABS_TOL)
