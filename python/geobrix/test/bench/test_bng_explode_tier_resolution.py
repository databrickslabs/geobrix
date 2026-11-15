"""Each BNG explode bench leg times the tier it is recorded under.

Both tiers register the same ``gbx_bng_*explode`` SQL names, but in different
registries: the lightweight tier registers Python table functions (Spark's
``tableFunctionRegistry``) and the heavyweight tier registers Catalyst generators
(``functionRegistry``). Registering one does not replace the other, and
``FROM v, LATERAL f(...)`` resolves table functions only. A bench session that ran
the lightweight legs first therefore still reached the lightweight UDTF from a
heavyweight leg written in that form, and recorded its timing as heavyweight.

These tests register both tiers in one session, in the order the cluster bench uses
(lightweight legs first, then heavyweight), run each explode leg, and read the plan
of the query the leg timed: a lightweight query must plan a Python UDTF, a
heavyweight query a plain ``Generate`` with no Python UDTF. With both tiers
registered the leg also runs the other tier's form once and compares row counts. The plan check here is
written independently of the bench's own guard so the guard is not tested with itself.

Heavy needs the geobrix JAR, so the module skips when none is staged under
``python/geobrix/lib/``. Run in geobrix-dev Docker, on its own (``spark.jars`` only
applies to the first session of a process):

    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/bench/test_bng_explode_tier_resolution.py \\
        --with-integration
"""

import logging
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/bench -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# (leg name in readers, SQL function it times, kwargs) -- the cluster cell's arguments.
_LEGS = [
    ("run_bng_kringexplode", "gbx_bng_kringexplode", dict(res="1km", k=1)),
    ("run_bng_kloopexplode", "gbx_bng_kloopexplode", dict(res="1km", k=1)),
    ("run_bng_geomkringexplode", "gbx_bng_geomkringexplode", dict(res="1km", k=1)),
    ("run_bng_geomkloopexplode", "gbx_bng_geomkloopexplode", dict(res="1km", k=1)),
    ("run_bng_tessellateexplode", "gbx_bng_tessellateexplode", dict(res="1km")),
]


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
        .appName("gbx-bench-bng-explode-tier")
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


@pytest.fixture(scope="module")
def combined_session(spark_with_jar):
    """Both tiers registered, lightweight first (the cluster cell's order)."""
    from databricks.labs.gbx.bench import readers as rd

    rd._register_bng(spark_with_jar, "lightweight")
    rd._register_bng(spark_with_jar, "heavyweight")
    return spark_with_jar


def _planned_tier(spark, sql):
    """'lightweight' when the plan runs a Python UDTF, 'heavyweight' for a JVM generator."""
    plan = spark.sql(f"EXPLAIN EXTENDED {sql}").collect()[0][0]
    if "PythonUDTF" in plan:
        return "lightweight"
    if "Generate" in plan:
        return "heavyweight"
    return f"unknown plan:\n{plan}"


def _run_leg(spark, monkeypatch, leg, sql_name, kwargs, api):
    """Run one bench leg on a small corpus; return (row, queries it issued for sql_name)."""
    from databricks.labs.gbx.bench import readers as rd

    issued = []
    real_sql = spark.sql

    def _recording_sql(query, *a, **kw):
        if sql_name in query and not query.lstrip().upper().startswith("EXPLAIN"):
            issued.append(query)
        return real_sql(query, *a, **kw)

    monkeypatch.setattr(spark, "sql", _recording_sql)
    try:
        row = getattr(rd, leg)(spark, "tier-test", 0, 1, api=api, n_rows=12, **kwargs)
    finally:
        monkeypatch.setattr(spark, "sql", real_sql)
    return row, issued


@pytest.mark.parametrize("api", ["heavyweight", "lightweight"])
@pytest.mark.parametrize("leg,sql_name,kwargs", _LEGS, ids=[x[0] for x in _LEGS])
def test_explode_leg_times_its_own_tier_when_both_tiers_are_registered(
    combined_session, monkeypatch, leg, sql_name, kwargs, api
):
    spark = combined_session
    row, issued = _run_leg(spark, monkeypatch, leg, sql_name, kwargs, api)

    assert row.api == api
    assert row.status == "ok", row.note
    assert issued, f"{leg} issued no query that calls {sql_name}"
    # The timed query runs most often (validation + timed iterations); the only
    # other query is the other tier's form, run once to compare row counts.
    timed = max(set(issued), key=issued.count)
    assert _planned_tier(spark, timed) == api, (
        f"the {api} leg of {leg} timed a query that reaches another implementation:\n"
        f"{timed}"
    )
    other = "heavyweight" if api == "lightweight" else "lightweight"
    for query in set(issued) - {timed}:
        assert _planned_tier(spark, query) == other, query
    # Both tiers are registered here, so the leg compares the two forms' row counts.
    assert "same row count as the other tier's form" in row.note, row.note


def test_a_leg_whose_query_reaches_the_other_tier_raises_instead_of_recording(
    combined_session, monkeypatch
):
    """The leg checks the plan before timing and fails loudly on a mismatch.

    Forcing the heavyweight leg to build the lightweight query (the old,
    tier-agnostic form) must raise, not return a row for the results table.
    """
    from databricks.labs.gbx.bench import readers as rd

    real_builder = rd._bng_explode_sql
    monkeypatch.setattr(
        rd,
        "_bng_explode_sql",
        lambda api, view, call, limit=False: real_builder(
            "lightweight", view, call, limit=limit
        ),
    )
    with pytest.raises(rd.TierResolutionError, match="lightweight"):
        rd.run_bng_kringexplode(
            combined_session, "tier-test", 0, 1, api="heavyweight", n_rows=4
        )
