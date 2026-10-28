"""Unit tests (no Spark, no JAR) for the bench's tier-resolution checks.

The plans below are trimmed from real ``EXPLAIN EXTENDED`` output: a lightweight
explode (Python table function), a heavyweight explode (JVM generator), pandas and
plain Python UDFs, a grouped-aggregate pandas UDF and a JVM expression.
"""

import pytest

from databricks.labs.gbx.bench import readers as rd

LIGHT_UDTF = (
    "== Optimized Logical Plan ==\n"
    "BatchEvalPythonUDTF gbx_bng_kringexplode(cell#0, 1)#6, [cellid#7]\n"
    "== Physical Plan ==\n"
    "BatchEvalPythonUDTF gbx_bng_kringexplode(cell#0, 1)#6, [cellid#7]\n"
)
HEAVY_GENERATOR = (
    "== Physical Plan ==\n"
    "Generate bng_kringexplode(cell#0, 1), false, [cellid#48]\n"
    "+- *(1) Scan ExistingRDD[cell#0]\n"
)
HEAVY_SCALAR = (
    "== Physical Plan ==\n"
    "*(1) Project [invoke(BNG_AsWKB(cell#0)) AS g#3]\n"
    "+- *(1) Scan ExistingRDD[cell#0]\n"
)
LIGHT_PANDAS = "== Physical Plan ==\n*(2) Project [pythonUDF0#9 AS g#3]\n+- ArrowEvalPython [gbx_bng_aswkb(cell#0)#2]\n"
LIGHT_PLAIN_UDF = "== Physical Plan ==\n*(2) Project [pythonUDF0#9 AS g#3]\n+- BatchEvalPython [gbx_bng_kring(cell#0, 1)#2]\n"
LIGHT_GROUPED_AGG = "== Physical Plan ==\nArrowAggregatePython [group#1], [gbx_bng_cellunion_agg(chip#2)]\n"
LIGHT_AGG_IN_PANDAS = "== Physical Plan ==\nAggregateInPandas [group#1], [gbx_bng_cellunion_agg(chip#2)]\n"


class _Rows:
    def __init__(self, plan):
        self._plan = plan

    def collect(self):
        return [(self._plan,)]


class _FakeSpark:
    """``spark.sql`` that answers every query with one plan string."""

    def __init__(self, plan):
        self.plan = plan
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        return _Rows(self.plan)


class _FakeDF:
    """A DataFrame whose ``explain`` prints a fixed plan, as PySpark's does."""

    def __init__(self, plan):
        self.plan = plan

    def explain(self, mode=None):
        assert mode == "extended"
        print(self.plan)


@pytest.mark.parametrize(
    "plan,expected",
    [
        (LIGHT_UDTF, "lightweight"),
        (HEAVY_GENERATOR, "heavyweight"),
        (HEAVY_SCALAR, "unknown"),
    ],
)
def test_explode_impl_reads_the_tier_from_the_plan(plan, expected):
    spark = _FakeSpark(plan)
    assert rd._explode_impl(spark, "SELECT 1") == expected
    assert spark.queries == ["EXPLAIN EXTENDED SELECT 1"]


@pytest.mark.parametrize(
    "plan,expected",
    [
        (LIGHT_UDTF, "lightweight"),
        (LIGHT_PANDAS, "lightweight"),
        (LIGHT_PLAIN_UDF, "lightweight"),
        (LIGHT_GROUPED_AGG, "lightweight"),
        (LIGHT_AGG_IN_PANDAS, "lightweight"),
        (HEAVY_SCALAR, "heavyweight"),
        (HEAVY_GENERATOR, "heavyweight"),
    ],
)
def test_assert_tier_reads_the_printed_plan_of_the_dataframe(plan, expected):
    other = "heavyweight" if expected == "lightweight" else "lightweight"
    rd._assert_tier(expected, "fn", _FakeDF(plan))  # passes
    with pytest.raises(
        rd.TierResolutionError, match=f"runs the {expected} implementation"
    ) as refused:
        rd._assert_tier(other, "fn", _FakeDF(plan))
    # a lightweight plan says which Python node made it so; a heavyweight one has none
    named = [
        m for m in rd._PYTHON_PLAN_MARKERS if f"plan node: {m}" in str(refused.value)
    ]
    assert bool(named) == (expected == "lightweight"), str(refused.value)


def test_bng_explode_sql_takes_each_tiers_form():
    call = "gbx_bng_kringexplode(cell, 1)"
    assert (
        rd._bng_explode_sql("lightweight", "v", call)
        == "SELECT t.cellid FROM v, LATERAL gbx_bng_kringexplode(cell, 1) t"
    )
    assert rd._bng_explode_sql("heavyweight", "v", call) == (
        "SELECT gbx_bng_kringexplode(cell, 1) FROM v"
    )
    assert rd._bng_explode_sql("heavyweight", "v", call, limit=True).endswith(
        " LIMIT 1"
    )


def test_a_tier_mismatch_is_not_caught_by_the_legs_error_handlers():
    """Legs turn Exception into an error row; the mismatch must get past that."""
    with pytest.raises(rd.TierResolutionError, match="refusing to time it"):
        try:
            rd._require_tier(
                "heavyweight", "bng_kringexplode", "lightweight", "SELECT 1"
            )
        except Exception:  # noqa: BLE001 -- the handler every leg has
            pytest.fail("TierResolutionError was caught as an Exception")


def test_the_explode_leg_raises_before_timing_when_its_query_reaches_the_other_tier(
    monkeypatch,
):
    class _DF:
        def cache(self):
            return self

        def count(self):
            return 1

        def createOrReplaceTempView(self, name):
            pass

    class _Spark(_FakeSpark):
        def createDataFrame(self, data, schema=None):
            return _DF()

    timed = []
    monkeypatch.setattr(rd, "_register_bng", lambda spark, api: None)
    monkeypatch.setattr(rd, "capture_env", lambda where: {})
    monkeypatch.setattr(rd, "time_iters", lambda *a, **k: timed.append(a))
    spark = _Spark(LIGHT_UDTF)  # the heavy leg's query plans the light UDTF
    with pytest.raises(rd.TierResolutionError, match="lightweight implementation"):
        rd.run_bng_kringexplode(spark, "r", 0, 1, api="heavyweight", n_rows=2)
    assert not timed
    assert spark.queries == [
        "EXPLAIN EXTENDED SELECT gbx_bng_kringexplode(cell, 1) FROM _bng_cell_bench_v"
    ]


@pytest.mark.parametrize(
    "plan,note",
    [
        (
            "== Analyzed Logical Plan ==\n[UNRESOLVED_ROUTINE] Cannot resolve routine "
            "`gbx_bng_kringexplode` on search path [`system`.`builtin`]. SQLSTATE: 42883",
            "not registered",
        ),
        (RuntimeError("EXPLAIN failed"), "EXPLAIN failed"),
    ],
    ids=["unresolved-routine-in-the-plan", "explain-raises"],
)
def test_the_explode_leg_writes_an_error_row_when_its_query_cannot_be_planned(
    monkeypatch, plan, note
):
    """EXPLAIN of an unregistered function does not raise: its plan holds the
    UNRESOLVED_ROUTINE error. Nothing can be mis-timed either way, so the run goes on with
    an error row, whether the plan says so or EXPLAIN itself fails.
    """

    class _DF:
        def cache(self):
            return self

        def count(self):
            return 1

        def createOrReplaceTempView(self, name):
            pass

    class _Spark(_FakeSpark):
        def createDataFrame(self, data, schema=None):
            return _DF()

        def sql(self, query):
            if isinstance(self.plan, Exception):
                raise self.plan
            return super().sql(query)

    timed = []
    monkeypatch.setattr(rd, "_register_bng", lambda spark, api: None)
    monkeypatch.setattr(rd, "time_iters", lambda *a, **k: timed.append(a))
    row = rd.run_bng_kringexplode(_Spark(plan), "r", 0, 1, api="heavyweight", n_rows=2)
    assert row.status == "error" and row.api == "heavyweight"
    assert note in row.note
    assert not timed


class _CountSpark(_FakeSpark):
    """EXPLAIN answers with a plan; any other query is a DataFrame of ``n_other`` rows."""

    def __init__(self, plan, n_other):
        super().__init__(plan)
        self.n_other = n_other

    def sql(self, query):
        if query.startswith("EXPLAIN"):
            if isinstance(self.plan, Exception):
                raise self.plan
            return super().sql(query)
        return type("DF", (), {"count": lambda _: self.n_other})()


@pytest.mark.parametrize(
    "plan,n_other,expected",
    [
        (HEAVY_GENERATOR, 4, True),  # the other tier is registered and agrees
        (HEAVY_GENERATOR, 5, "gives 4 rows, the heavyweight form 5"),  # and disagrees
        (LIGHT_UDTF, 4, False),  # its form plans the same tier: not registered here
        (
            RuntimeError("no such function"),
            4,
            False,
        ),  # EXPLAIN fails: nothing to compare
    ],
    ids=["agree", "mismatch", "other-tier-not-registered", "explain-raises"],
)
def test_the_row_count_check_compares_with_the_other_tiers_form(
    plan, n_other, expected
):
    spark = _CountSpark(plan, n_other)
    args = (spark, "lightweight", "bng_kringexplode", "v", "gbx_bng_kringexplode(c, 1)")
    if isinstance(expected, str):
        with pytest.raises(RuntimeError, match=expected):
            rd._explode_rows_match(*args, 4)
    else:
        assert rd._explode_rows_match(*args, 4) is expected
