"""Every two-tier bench leg checks, before timing, that its query runs its own tier.

Both tiers register the same ``gbx_*`` SQL names. The scalar, array and aggregate
functions share Spark's ``functionRegistry``, where the later registration replaces
the earlier one, so a leg that registers its own tier first times that tier. These
tests make that enforced rather than assumed: each leg must hand the DataFrame it is
about to time to ``readers._assert_tier``, and the plan of that DataFrame must run the
leg's tier. The plan is read here a different way than the bench reads it (the JVM
physical plan's node classes, not the printed ``explain`` text).

Each leg runs lightweight then heavyweight, the cluster cells' order, so the heavy leg
runs in a session where the light functions are registered too. Heavy needs the
geobrix JAR, so the module skips when none is staged under ``python/geobrix/lib/``.
Run in geobrix-dev Docker, on its own (``spark.jars`` only applies to the first
session of a process):

    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/bench/test_leg_tier_resolution.py \\
        --with-integration
"""

import logging
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_GRID = dict(n_rows=12)
_LEGS = (
    [
        (f"run_quadbin_{n}", _GRID)
        for n in (
            "pointascell",
            "polyfill",
            "tessellate",
            "cellunion_agg",
            "resolution",
            "kring",
            "distance",
            "aswkb",
            "centroid",
            "cellunion",
        )
    ]
    + [
        (f"run_bng_{n}", _GRID)
        for n in (
            "pointascell",
            "polyfill",
            "tessellate",
            "kring",
            "cellunion_agg",
            "aswkb",
            "aswkt",
            "centroid",
            "cellarea",
            "distance",
            "euclideandistance",
            "eastnorthasbng",
            "kloop",
            "geomkring",
            "geomkloop",
            "cellintersection",
            "cellunion",
            "cellintersection_agg",
        )
    ]
    + [
        (f"run_custom_{n}", _GRID)
        for n in (
            "pointascell",
            "polyfill",
            "kring",
            "cellaswkb",
            "cellaswkt",
            "centroid",
            "grid",
        )
    ]
    + [
        ("run_triangulate", dict(n_rows=2)),
        ("run_interp_bbox", dict(n_rows=2)),
        ("run_interp_geom", dict(n_rows=2)),
        ("run_legacy_aswkb", _GRID),
        ("run_mvt_agg", dict(n_features=40, n_tiles=3)),
        ("run_pmtiles_agg", dict(n_tiles=20, n_groups=2)),
    ]
)


def _fanout_legs():
    from databricks.labs.gbx.bench import readers as rd

    return [("run_fanout_udtf", dict(fn=fn, scale=0.1)) for fn in rd.FANOUT_FUNCTIONS]


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and str(_JARS[-1]) not in active.conf.get("spark.jars", ""):
        pytest.skip(
            "A JAR-free Spark session is already live in this process; run this file "
            "on its own with --with-integration"
        )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-bench-leg-tier")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session


def _jvm_tier(df):
    """'lightweight' when a physical-plan node is a Python exec class, else 'heavyweight'."""
    classes, stack = [], [df._jdf.queryExecution().sparkPlan()]
    while stack:
        node = stack.pop()
        classes.append(node.getClass().getName())
        children = node.children()
        stack.extend(children.apply(i) for i in range(children.size()))
    python = [
        c for c in classes if c.startswith("org.apache.spark.sql.execution.python.")
    ]
    return "lightweight" if python else "heavyweight"


def _record_checks(monkeypatch):
    """Wrap readers._assert_tier: record (api, fn, tier the plan runs) for each check."""
    from databricks.labs.gbx.bench import readers as rd

    calls = []
    real = rd._assert_tier

    def _checked(api, fn, df, generator=False):
        calls.append((api, fn, _jvm_tier(df)))
        return real(api, fn, df, generator)

    monkeypatch.setattr(rd, "_assert_tier", _checked)
    return calls


@pytest.mark.parametrize(
    "leg,kwargs", _LEGS + _fanout_legs(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_each_tier_of_a_leg_checks_the_plan_it_times(
    spark_with_jar, monkeypatch, leg, kwargs
):
    from databricks.labs.gbx.bench import readers as rd

    calls = _record_checks(monkeypatch)
    for api in ("lightweight", "heavyweight"):
        row = getattr(rd, leg)(spark_with_jar, "tier-test", 0, 1, api=api, **kwargs)
        assert row.api == api
        assert row.status == "ok", f"{leg} {api}: {row.note}"
    assert [c[0] for c in calls] == ["lightweight", "heavyweight"], calls
    assert all(api == tier for api, _fn, tier in calls), calls


@pytest.mark.parametrize(
    "leg,register",
    [
        ("run_bng_pointascell", "_register_bng"),
        ("run_bng_cellunion_agg", "_register_bng"),
        ("run_quadbin_kring", "_register_quadbin"),
        ("run_custom_centroid", "_register_custom"),
    ],
)
def test_a_heavy_leg_that_reaches_the_light_function_raises(
    spark_with_jar, monkeypatch, leg, register
):
    """The failure the guard exists for: heavy timed while light is what resolves.

    The heavyweight leg's registration is replaced by the lightweight one, so the
    shared SQL name resolves to the Python implementation. The leg must stop rather
    than record a heavyweight row.
    """
    from databricks.labs.gbx.bench import readers as rd

    real = getattr(rd, register)
    monkeypatch.setattr(rd, register, lambda spark, api: real(spark, "lightweight"))
    with pytest.raises(rd.TierResolutionError, match="lightweight implementation"):
        getattr(rd, leg)(spark_with_jar, "tier-test", 0, 1, api="heavyweight", n_rows=6)
