"""Cross-tier (lightweight vs heavyweight JVM) value parity for ``gbx_rst_proximity`` and ``gbx_rst_cog_convert``.

* ``rst_proximity``: the heavyweight function is the exact Euclidean distance, as the lightweight one is
  (``scipy.ndimage.distance_transform_edt``); GDAL's ``ComputeProximity`` was up to ~6% off.
* ``rst_cog_convert``: both tiers write a valid COG (GDAL's validator, no warnings) with the same number of
  overviews and the same pixels.

The light leg is collected before the heavy registration overwrites the SQL name (load-bearing order).

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-pyrx.sh --docker --with-integration \\
        --path python/geobrix/test/pyrx/test_proximity_cog_cross_tier.py
"""

import logging
from pathlib import Path
from test._heavy import heavy_register  # noqa: E402
from test.pyrx.conftest import cog_errors  # noqa: E402

import pytest

rasterio = pytest.importorskip("rasterio", reason="rasterio not installed")
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged under python/geobrix/lib/")
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and str(_JARS[-1]) not in active.conf.get("spark.jars", ""):
        pytest.skip(
            "A JAR-free Spark session is already live; run this test in isolation"
        )
    yield (
        SparkSession.builder.master("local[2]")
        .appName("gbx-proximity-cog-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )


def _gtiff(arr, *, px=30.0, py=12.5, nodata=None):
    arr = np.asarray(arr)
    h, w = arr.shape
    profile = dict(
        driver="GTiff", width=w, height=h, count=1, dtype=str(arr.dtype),
        crs="EPSG:32633", transform=from_origin(500000.0, 5000000.0, px, py), nodata=nodata,
    )  # fmt: skip
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr, 1)
        return mf.read()


def _run(spark, content, expr):
    """The decoded (array, profile) of ``expr`` over the tile of ``content`` on each tier: (light, heavy)."""
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    base = spark.createDataFrame([(content,)], "content binary")
    base = base.selectExpr("gbx_rst_fromcontent(content, 'GTiff') AS tile")
    tile_row, tile_type = base.collect()[0]["tile"], base.schema["tile"].dataType
    df = spark.createDataFrame(
        [{"tile": tile_row}], schema=StructType([StructField("tile", tile_type, True)])
    )
    df.createOrReplaceTempView("_prox_cog_in")
    sql = f"SELECT {expr} AS r FROM _prox_cog_in"
    light = spark.sql(sql).collect()[0]["r"]
    heavy_register(spark, ("gbx_rst_",))
    heavy = spark.sql(sql).collect()[0]["r"]
    assert light is not None and heavy is not None
    return bytes(light["raster"]), bytes(heavy["raster"])


def _decode(data):
    with MemoryFile(data) as mf, mf.open() as ds:
        return ds.read(1), ds.nodata, len(ds.overviews(1)), ds.profile.get("tiled")


@pytest.mark.parametrize(
    "args, label",
    [
        ("", "default: any non-zero pixel, GEO"),
        ("'2,3', 'PIXEL', 6.5", "targets, PIXEL, capped"),
        ("NULL, 'GEO', 700.0", "GEO, capped"),
    ],
)
def test_proximity_value_parity(spark_with_jar, args, label):
    rng = np.random.default_rng(3)
    arr = np.zeros((41, 53), dtype="float32")
    arr[rng.random(arr.shape) < 0.02] = 2.0
    arr[rng.random(arr.shape) < 0.01] = 3.4
    sql_args = f", {args}" if args else ""
    light, heavy = _run(
        spark_with_jar, _gtiff(arr), f"gbx_rst_proximity(tile{sql_args})"
    )
    la, lnd, _, _ = _decode(light)
    ha, hnd, _, _ = _decode(heavy)
    assert lnd == hnd == -1.0, label
    assert la.shape == ha.shape
    # exact on both tiers: a Float32 ulp apart at most (an approximate distance is off by tenths of a pixel)
    assert np.allclose(la, ha, rtol=1e-6, atol=1e-6), (
        label,
        float(np.abs(la - ha).max()),
    )


def test_cog_convert_valid_cog_and_same_overviews(spark_with_jar):
    rng = np.random.default_rng(5)
    arr = (rng.random((512, 512)) * 200).astype("float32")
    light, heavy = _run(
        spark_with_jar,
        _gtiff(arr, px=1.0, py=1.0),
        "gbx_rst_cog_convert(tile, 'DEFLATE', 128, 'AVERAGE')",
    )
    la, _, lovr, ltiled = _decode(light)
    ha, _, hovr, htiled = _decode(heavy)
    assert hovr >= 1, "the heavyweight COG has no overview pyramid"
    assert hovr == lovr
    assert ltiled and htiled
    assert np.array_equal(la, ha) and np.array_equal(ha, arr), "pixels changed"
    assert (
        cog_errors(heavy) == []
    ), "the heavyweight COG is not valid (errors or warnings)"
    assert cog_errors(light) == []
