"""Cross-tier parity: light (pygx) vs heavy (gridx.bng) for BNG geometry-aware kring/kloop.

Tests all 6 dilation modes on both a simple polygon and a holed polygon, comparing
light ``_bng.geometry_k_ring/loop`` against heavy ``gbx_bng_geomkring/geomkloop``
registered functions.

Simple fixture: London BNG box (530000,180000)→(533000,183000) at resolution 3 (1km).
BNG coords are EPSG:27700; heavy registers the same SQL names as light.

Holed fixture: larger BNG box (500000,150000)→(560000,210000) with a 20km×20km
interior hole (520000,170000)→(540000,190000) at resolution 3 (1km cells ≈1km²).
At res 3 the hole spans ~20 cells wide × ~20 cells tall; cells fully inside the hole
polygon populate hCore so hole-in/hole-out genuinely exercise inward fill.

Heavy requires the geobrix JAR. Mark integration; skips when no JAR under python/geobrix/lib/.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_bng_geomk.py \\
        --with-integration --log parity-bng-geomk.log
"""

import logging
from pathlib import Path

import pytest
from shapely import to_wkb
from shapely.geometry import box
from shapely.geometry.polygon import Polygon

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# --- Fixtures ---

# Simple London box at res 3 (1km cells)
_LONDON_BOX = box(530000.0, 180000.0, 533000.0, 183000.0)
_RES_SIMPLE = 3  # 1km resolution

# Holed polygon: larger BNG box with a 20km×20km interior hole.
# At res 3 (1km cells) the hole spans ~20 cells wide × ~20 cells tall.
# Cells fully inside the hole polygon are in hCore → hole-in/hole-out exercise inward fill.
_OUTER_H = [
    (500000.0, 150000.0),
    (560000.0, 150000.0),
    (560000.0, 210000.0),
    (500000.0, 210000.0),
]
_HOLE_H = [
    (520000.0, 170000.0),
    (540000.0, 170000.0),
    (540000.0, 190000.0),
    (520000.0, 190000.0),
]
_HOLED_POLY = Polygon(_OUTER_H, [_HOLE_H])
_RES_HOLED = 3  # 1km resolution

_MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)

_COVERAGES = ("coveras", "polyfill", "core")


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "PRECONDITION MISSING: staged JAR for heavy parity; "
            "run in geobrix-dev Docker (no geobrix JAR under python/geobrix/lib/)"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: skip if a JAR-free session is already live.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run in isolation: "
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_bng_geomk.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-bng-geomk-parity")
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


def _wkb(geom) -> bytes:
    return bytes(to_wkb(geom))


def _collect_light(geom, res, k, mode, coverage="coveras"):
    """Collect light result for one (geom, res, k, mode, coverage) combination."""
    from databricks.labs.gbx.pygx import _bng

    return set(_bng.geometry_k_ring_str(to_wkb(geom), res, k, mode, coverage))


def _collect_light_loop(geom, res, k, mode, coverage="coveras"):
    from databricks.labs.gbx.pygx import _bng

    return set(_bng.geometry_k_loop_str(to_wkb(geom), res, k, mode, coverage))


def _collect_heavy(spark, geom, res, k, mode, fn_name, coverage="coveras"):
    """Collect heavy result for one (geom, res, k, mode, coverage) combination.

    BNG resolution is passed as an integer (1=100km … 6=1m; 3=1km).
    Heavy returns an array of strings; light returns strings too.
    Coverage is forwarded as a SQL literal (5th positional arg after mode).
    """
    from pyspark.sql import functions as f

    geom_wkb = _wkb(geom)
    df = spark.createDataFrame(
        [(geom_wkb, res, k, mode)], "geom binary, res int, k int, mode string"
    )
    row = df.select(
        f.call_function(
            fn_name,
            f.col("geom"),
            f.col("res"),
            f.col("k"),
            f.col("mode"),
            f.lit(coverage),
        ).alias("cells")
    ).collect()[0]
    if row["cells"] is None:
        return set()
    return set(str(c) for c in row["cells"])


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_bng_geomkring_simple_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkring over all 6 modes × 3 coverage bases, simple BNG polygon."""
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Register light FIRST, collect all light results, then register heavy.
    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light(
            _LONDON_BOX, _RES_SIMPLE, 1, mode, coverage
        )

    # Now register heavy (overwrites light SQL names).
    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _LONDON_BOX, _RES_SIMPLE, 1, mode, "gbx_bng_geomkring", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_bng_geomkloop_simple_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkloop over all 6 modes × 3 coverage bases, simple BNG polygon."""
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light_loop(
            _LONDON_BOX, _RES_SIMPLE, 1, mode, coverage
        )

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _LONDON_BOX, _RES_SIMPLE, 1, mode, "gbx_bng_geomkloop", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_bng_geomkring_holed_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkring over all 6 modes × 3 coverage bases, holed BNG polygon."""
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light(_HOLED_POLY, _RES_HOLED, 1, mode, coverage)

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_bng_geomkring", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring holed mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_bng_geomkloop_holed_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkloop over all 6 modes × 3 coverage bases, holed BNG polygon."""
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light_loop(
            _HOLED_POLY, _RES_HOLED, 1, mode, coverage
        )

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_bng_geomkloop", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop holed mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )
