"""End-to-end Python test for the spectral-index functions.

One parameterized round-trip across all 5 wrappers: load a multi-band MODIS
tile, apply the function, assert the JVM bindings fire and a non-empty raster
tile comes back. To keep the suite lean we deliberately cover
all 5 functions in one parametrized test rather than 5 near-identical copies.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# Single-band SRTM elevation tile shipped in the essential bundle. The Wave
# 8b Python test only verifies that the JVM bindings fire and a non-empty
# raster tile comes back; numerical correctness of each formula is tested in
# the Scala suite (SpectralIndicesTest). So we point every "band index" arg
# at band 1 of this single-band raster - the math degenerates (e.g. NDVI =
# 0 when NIR == Red), but the end-to-end SQL -> Scala -> gdal_calc -> tile
# round-trip is what we're exercising here.
SAMPLE_TILE_PATH = (
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


@pytest.mark.parametrize(
    "expression",
    [
        # EVI with all 4 doubles defaulted at SQL level (Scala builder picks
        # L=1.0, C1=6.0, C2=7.5, G=2.5) - 4 band-index args only.
        "gbx_rst_evi(t, 1, 1, 1)",
        # SAVI with default L=0.5.
        "gbx_rst_savi(t, 1, 1)",
        # NDWI: green + NIR.
        "gbx_rst_ndwi(t, 1, 1)",
        # NBR: NIR + SWIR.
        "gbx_rst_nbr(t, 1, 1)",
        # Generic dispatcher: NDVI by name + SQL MAP literal.
        "gbx_rst_index(t, 'ndvi', map('red', 1, 'nir', 1))",
    ],
)
def test_spectral_indices_roundtrip(spark, expression):
    """Each spectral-index function: SQL invocation returns a non-empty tile.

    Loads the sample multi-band tile via ``gbx_rst_fromcontent``, applies the
    spectral-index expression, then asserts the resulting tile struct has
    non-empty raster bytes / path and a metadata map stamped by gdal_calc.
    """
    if not Path(SAMPLE_TILE_PATH).exists():
        pytest.skip(f"sample raster not present: {SAMPLE_TILE_PATH}")
    # gbx_rst_fromfile is lightweight-only (issue #34, pyrx UDF). The heavy tier
    # loads the local multi-band raster by reading its bytes via the binaryFile
    # reader and decoding with the Scala/GDAL gbx_rst_fromcontent -- no pandas/rasterio.
    spark.read.format("binaryFile").load(str(SAMPLE_TILE_PATH)).createOrReplaceTempView(
        "_rasterx_src"
    )
    df = spark.sql(
        f"SELECT {expression} AS out "
        f"FROM (SELECT gbx_rst_fromcontent(content, 'GTiff') AS t FROM _rasterx_src)"
    )
    rows = df.collect()
    assert len(rows) == 1
    out = rows[0]["out"]
    assert out is not None, f"{expression} returned null tile"
    md = out["metadata"]
    assert md is not None, f"{expression} returned tile with null metadata"
    # Tile struct = (cellid, raster, metadata)
    raster = out["raster"]
    assert raster is not None, f"{expression} returned None raster; metadata={dict(md)}"
    if isinstance(raster, (bytes, bytearray)):
        assert (
            len(raster) > 0
        ), f"{expression} returned empty raster bytes; metadata={dict(md)}"
    else:
        assert len(str(raster)) > 0
    # gdal_calc output is always GTiff under the hood (RST_MapAlgebra).
    assert (
        md.get("driver") == "GTiff" or md.get("format") == "GTiff"
    ), f"unexpected driver in metadata: {md.get('driver')}"
