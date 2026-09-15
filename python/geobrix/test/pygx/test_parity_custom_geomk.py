"""Cross-tier parity: light (pygx) vs heavy (gridx.custom) for custom geometry-aware kring/kloop.

Tests all 6 dilation modes on both a simple polygon and a holed polygon, comparing
light ``_custom.geometry_k_ring/loop`` against heavy ``gbx_custom_geomkring/geomkloop``
registered functions.

Fixtures mirror test_custom_geomk.py exactly:
  Simple fixture: 5 000 × 5 000 box at (530 000, 180 000) at resolution 0.
  Holed fixture: 60 km outer box with interior hole inset 500 units from cell-grid
    lines; at res 0 h_core non-empty (38 × 38 = 1 444 cells).

Grid: 0..1 000 000 × 0..1 000 000, cell_splits=2, root_size=1 000.

Heavy requires the geobrix JAR. Mark integration; skips when no JAR found.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_custom_geomk.py \\
        --with-integration --log parity-custom-geomk.log
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

_SIMPLE_GEOM = box(530000, 180000, 535000, 185000)
_SIMPLE_RES = 0

_OUTER_COORDS = [
    (500000, 100000),
    (560000, 100000),
    (560000, 160000),
    (500000, 160000),
]
_HOLE_COORDS = [
    (510500, 110500),
    (549500, 110500),
    (549500, 149500),
    (510500, 149500),
]
_HOLED_POLY = Polygon(_OUTER_COORDS, [_HOLE_COORDS])
_RES_HOLED = 0

_MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)

_COVERAGES = ("coveras", "polyfill", "core")

# Grid struct constants matching the fixture conf.
_GRID_ARGS = dict(
    bound_x_min=0,
    bound_x_max=1_000_000,
    bound_y_min=0,
    bound_y_max=1_000_000,
    cell_splits=2,
    root_cell_size_x=1000,
    root_cell_size_y=1000,
    srid=-1,
)


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "PRECONDITION MISSING: staged JAR for heavy parity; "
            "run in geobrix-dev Docker (no geobrix JAR under python/geobrix/lib/)"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run in isolation: "
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_custom_geomk.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-custom-geomk-parity")
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


def _collect_light(conf, geom, res, k, mode, coverage="coveras"):
    """Collect light result for one (geom, res, k, mode, coverage) combination."""
    from databricks.labs.gbx.pygx import _custom

    return set(
        _custom.geometry_k_ring(conf, _wkb(geom), res, k, mode, coverage=coverage)
    )


def _collect_light_loop(conf, geom, res, k, mode, coverage="coveras"):
    from databricks.labs.gbx.pygx import _custom

    return set(
        _custom.geometry_k_loop(conf, _wkb(geom), res, k, mode, coverage=coverage)
    )


def _collect_heavy(spark, geom, res, k, mode, fn_name, coverage="coveras"):
    """Collect heavy result for one (geom, res, k, mode, coverage) combination via registered SQL fn.

    Coverage forwarded as a SQL literal (6th positional arg after mode).
    """
    from pyspark.sql import functions as f

    geom_wkb = _wkb(geom)
    df = spark.createDataFrame(
        [(geom_wkb, res, k, mode)], "geom binary, res int, k int, mode string"
    ).withColumn(
        "grid",
        f.lit(None).cast(
            "struct<bound_x_min:bigint,bound_x_max:bigint,bound_y_min:bigint,bound_y_max:bigint,cell_splits:int,root_cell_size_x:int,root_cell_size_y:int,srid:int>"
        ),
    )

    # Build the grid struct via the registered function.
    df = df.withColumn(
        "grid",
        f.call_function(
            "gbx_custom_grid",
            f.lit(_GRID_ARGS["bound_x_min"]),
            f.lit(_GRID_ARGS["bound_x_max"]),
            f.lit(_GRID_ARGS["bound_y_min"]),
            f.lit(_GRID_ARGS["bound_y_max"]),
            f.lit(_GRID_ARGS["cell_splits"]),
            f.lit(_GRID_ARGS["root_cell_size_x"]),
            f.lit(_GRID_ARGS["root_cell_size_y"]),
            f.lit(_GRID_ARGS["srid"]),
        ),
    )
    row = df.select(
        f.call_function(
            fn_name,
            f.col("geom"),
            f.col("grid"),
            f.col("res"),
            f.col("k"),
            f.col("mode"),
            f.lit(coverage),
        ).alias("cells")
    ).collect()[0]
    if row["cells"] is None:
        return set()
    return set(int(c) for c in row["cells"])


def _make_conf():
    from databricks.labs.gbx.pygx._custom import CustomGridConf

    return CustomGridConf(**_GRID_ARGS)


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_custom_geomkring_simple_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkring over all 6 modes × 3 coverage bases, simple polygon."""
    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar
    conf = _make_conf()

    # Register light first, collect results, then register heavy.
    gx.register(spark)
    light_results = {
        mode: _collect_light(conf, _SIMPLE_GEOM, _SIMPLE_RES, 1, mode, coverage)
        for mode in _MODES
    }

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _SIMPLE_GEOM, _SIMPLE_RES, 1, mode, "gbx_custom_geomkring", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring simple mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_custom_geomkloop_simple_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkloop over all 6 modes × 3 coverage bases, simple polygon."""
    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar
    conf = _make_conf()

    gx.register(spark)
    light_results = {
        mode: _collect_light_loop(conf, _SIMPLE_GEOM, _SIMPLE_RES, 1, mode, coverage)
        for mode in _MODES
    }

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _SIMPLE_GEOM, _SIMPLE_RES, 1, mode, "gbx_custom_geomkloop", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop simple mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_custom_geomkring_holed_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkring over all 6 modes × 3 coverage bases, holed polygon (hCore non-empty)."""
    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar
    conf = _make_conf()

    gx.register(spark)
    light_results = {
        mode: _collect_light(conf, _HOLED_POLY, _RES_HOLED, 1, mode, coverage)
        for mode in _MODES
    }

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_custom_geomkring", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring holed mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


@pytest.mark.parametrize("coverage", _COVERAGES)
def test_parity_custom_geomkloop_holed_all_modes(spark_with_jar, coverage):
    """Light vs heavy geomkloop over all 6 modes × 3 coverage bases, holed polygon."""
    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar
    conf = _make_conf()

    gx.register(spark)
    light_results = {
        mode: _collect_light_loop(conf, _HOLED_POLY, _RES_HOLED, 1, mode, coverage)
        for mode in _MODES
    }

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_custom_geomkloop", coverage
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop holed mode={mode} coverage={coverage}: "
            f"light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )
