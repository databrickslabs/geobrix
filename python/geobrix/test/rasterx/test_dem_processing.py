"""End-to-end Python test for the terrain-analysis functions.

One parameterized round-trip across all 7 ``gdal.DEMProcessing`` wrappers:
load an SRTM elevation tile, apply the function, assert the JVM bindings
fire and a non-empty raster tile comes back. To keep the suite lean,
we deliberately cover all 7 functions in one parametrized test
rather than 7 near-identical copies.
"""

import logging
import os
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# An SRTM elevation tile shipped in the essential sample-data bundle.
SRTM_PATH = (
    "/Volumes/main/default/test-data/geobrix-examples/london/elevation/srtm_n51w001.tif"
)


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


@pytest.fixture(scope="module")
def color_table_path():
    """Write a tiny gdaldem color table covering elevations 0..1500 m."""
    fd, path = tempfile.mkstemp(prefix="gbx_dem_color_", suffix=".txt")
    os.close(fd)
    Path(path).write_text("0 0 0 255\n" "500 0 255 0\n" "1500 255 0 0\n")
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.mark.parametrize(
    "expression, extra_args",
    [
        ("gbx_rst_slope(t)", ""),
        ("gbx_rst_aspect(t)", ""),
        ("gbx_rst_hillshade(t)", ""),
        ("gbx_rst_tri(t)", ""),
        ("gbx_rst_tpi(t)", ""),
        ("gbx_rst_roughness(t)", ""),
        # color_relief is the only one requiring an extra arg.
        ("gbx_rst_color_relief(t, '__COLOR_TABLE__')", ""),
    ],
)
def test_dem_processing_roundtrip(spark, color_table_path, expression, extra_args):
    """Each DEM-processing function: SQL invocation returns a non-empty tile.

    Loads the SRTM tile via gbx_rst_fromcontent, applies the terrain function,
    then asserts the resulting tile struct has non-empty raster bytes / path
    and a metadata map stamped by RST_DEMProcessingHelper.
    """
    if not Path(SRTM_PATH).exists():
        pytest.skip(f"sample DEM not present: {SRTM_PATH}")

    sql_expr = expression.replace("__COLOR_TABLE__", color_table_path)
    # gbx_rst_fromfile is lightweight-only (issue #34, pyrx UDF). The heavy tier
    # loads the local DEM by reading its bytes via the binaryFile reader and
    # decoding with the Scala/GDAL gbx_rst_fromcontent -- no pandas/rasterio.
    spark.read.format("binaryFile").load(str(SRTM_PATH)).createOrReplaceTempView(
        "_rasterx_src"
    )
    df = spark.sql(
        f"SELECT {sql_expr} AS out "
        f"FROM (SELECT gbx_rst_fromcontent(content, 'GTiff') AS t FROM _rasterx_src)"
    )
    rows = df.collect()
    assert len(rows) == 1
    out = rows[0]["out"]
    assert out is not None, f"{expression} returned null tile"
    # Tile struct = (cellid, raster, metadata)
    raster = out["raster"]
    assert raster is not None
    # raster is either bytes (BinaryType) or a path string (StringType).
    if isinstance(raster, (bytes, bytearray)):
        assert len(raster) > 0, f"{expression} returned empty raster bytes"
    else:
        assert len(str(raster)) > 0
    md = out["metadata"]
    assert md is not None
    # Helper stamps driver=GTiff for all 7 functions.
    assert md.get("driver") == "GTiff" or md.get("format") == "GTiff"
