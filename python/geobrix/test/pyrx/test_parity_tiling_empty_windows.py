"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for retile-family window handling.

Both tiers must emit EVERY window, including windows whose pixels are all NoData, and
every retile-family tile must carry the PARENT tile's cellid.

Fixture: a 2048x2048 uint16 raster with nodata=0 cut into 512-pixel windows. Three of the
sixteen windows are entirely NoData, and a partial hole leaves window (r3, c0) non-empty.
A second fixture empties the whole left window column to expose the extent loss that
dropping windows causes on a retile -> merge round trip.

Heavy requires the geobrix JAR and GDAL JNI libraries; both present in the geobrix-dev
Docker container. Auto-skips when the JAR is absent or a JAR-free Spark session is
already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_tiling_empty_windows.py
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (the lightweight tier required)",
)
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_bounds  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix  (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_PARENT_CELLID = 7


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded, module scope
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live; run this test in isolation: "
                "gbx:test:parity --path "
                "python/geobrix/test/pyrx/test_parity_tiling_empty_windows.py"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-combine-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def _fixture(left_col_empty=False):
    """2048x2048 uint16 nodata=0. Default: 512-windows (r0,c0),(r0,c1),(r2,c3) all-NoData (3/16),
    plus a partial hole that keeps (r3,c0) non-empty. left_col_empty: whole window column 0 empty.
    """
    a = (np.arange(2048 * 2048, dtype="uint32").reshape(2048, 2048) % 4000 + 1).astype(
        "uint16"
    )
    if left_col_empty:
        a[:, 0:512] = 0
    else:
        a[0:512, 0:1024] = 0
        a[1024:1536, 1536:2048] = 0
        a[1600:1700, 0:300] = 0
    prof = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=1,
        dtype="uint16",
        crs="EPSG:32618",
        transform=from_bounds(500000, 4490000, 520480, 4510480, 2048, 2048),
        nodata=0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a, 1)
        return mf.read()


def _all_nodata(b):
    with MemoryFile(bytes(b)) as mf, mf.open() as d:
        return all(not d.read_masks(i).any() for i in range(1, d.count + 1))


# (sql fn, every-window count, non-empty-window count)
CASES = [
    ("gbx_rst_retile(tile, 512, 512)", 16, 13),
    ("gbx_rst_tooverlappingtiles(tile, 512, 512, 10)", 25, 23),
    ("gbx_rst_maketiles(tile, 1)", 16, 13),
]


@pytest.fixture(scope="module")
def outputs(spark_with_jar):
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    spark = spark_with_jar
    tile_t = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    for name, b in (("rt", _fixture()), ("lce", _fixture(left_col_empty=True))):
        spark.createDataFrame(
            [
                {
                    "tile": {
                        "cellid": _PARENT_CELLID,
                        "raster": bytearray(b),
                        "metadata": {},
                    }
                }
            ],
            StructType([StructField("tile", tile_t)]),
        ).createOrReplaceTempView(name)
    fns = [c[0] for c in CASES]
    retile = CASES[0][0]

    # Light first, then heavy (heavy register overwrites the same gbx_* names).
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    light = {
        f: [
            r.raster
            for r in spark.sql(f"SELECT t.raster FROM rt, LATERAL {f} t").collect()
        ]
        for f in fns
    }
    light_cellids = [
        r.cellid
        for r in spark.sql(f"SELECT t.cellid FROM rt, LATERAL {retile} t").collect()
    ]

    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    heavy = {
        f: [
            r.raster
            for r in spark.sql(
                f"SELECT g.raster FROM (SELECT {f} AS g FROM rt)"
            ).collect()
        ]
        for f in fns
    }
    heavy_cellids = [
        r.cellid
        for r in spark.sql(
            f"SELECT g.cellid FROM (SELECT {retile} AS g FROM rt)"
        ).collect()
    ]
    heavy_merge_w = (
        spark.sql(
            "SELECT gbx_rst_width(gbx_rst_merge_agg(g)) w FROM "
            "(SELECT gbx_rst_retile(tile, 512, 512) AS g FROM lce)"
        )
        .collect()[0]
        .w
    )
    return light, heavy, heavy_merge_w, light_cellids, heavy_cellids


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fn,keep,nonempty", CASES)
def test_tiers_agree_on_window_count(outputs, fn, keep, nonempty):
    light, heavy, *_ = outputs
    assert len(light[fn]) == len(heavy[fn])


@pytest.mark.parametrize("fn,keep,nonempty", CASES)
def test_every_window_emitted(outputs, fn, keep, nonempty):
    light, heavy, *_ = outputs
    assert len(light[fn]) == keep and len(heavy[fn]) == keep
    assert sum(map(_all_nodata, heavy[fn])) == keep - nonempty


def test_retile_merge_preserves_extent(outputs):
    assert outputs[2] == 2048


def test_cellid_matches_parent_in_both_tiers(outputs):
    _, _, _, light_cellids, heavy_cellids = outputs
    assert len(light_cellids) == len(heavy_cellids) == 16
    assert set(light_cellids) == {_PARENT_CELLID}
    assert set(heavy_cellids) == {_PARENT_CELLID}
