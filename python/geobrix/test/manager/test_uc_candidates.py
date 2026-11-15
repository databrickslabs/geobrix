"""The SQL kernels of gbx_bng_cellarea and gbx_bng_eastnorthasbng (manifest L0/L4 `sql`), and the candidate mechanism
(uc.candidates): judged on a local Spark against the rows of their oracle tables and against the Python core over a
sweep. Both bodies use only core built-ins (string, bit and arithmetic functions, raise_error), so the host proves them.
Whether one beats Scala or Python is a timing; whether it is equal is decided here.
"""

import random

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.uc import candidates, ddl, handlers, sqlbody

AREA, ENAB = "gbx_bng_cellarea", "gbx_bng_eastnorthasbng"


def _kernel(name):
    """(body, helpers) of the row's own SQL kernel."""
    return sqlbody.l4_source(manifest.load().functions[name])


def _rows(name):
    return [
        c
        for t in ("GRID_SCALAR", "MVP_S")
        for c in oracles.TABLES[t]
        if c.fn == name and "uc" in c.surfaces
    ]


@pytest.fixture
def installed(spark):
    """Both kernels as temporary functions under their product names; dropped afterwards."""
    man = manifest.load()
    for name in (AREA, ENAB):
        spark.sql(ddl.local_function_ddl(man.functions[name], _kernel(name)))
    try:
        yield spark
    finally:
        for name in (AREA, ENAB):
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")


# ---- the adopted kernels: both surfaces run one text -------------------------------------------------------------


def test_the_two_rows_run_as_sql_on_uc_and_in_the_session_with_one_text():
    """Fails when a row loses its `sql`, or when the session text and the UC text drift apart."""
    man = manifest.load()
    for name in (AREA, ENAB):
        fn = man.functions[name]
        assert ddl.chosen_form(fn, None) == "sql", name
        assert ddl.chosen_form(fn, "/Volumes/c/s/v/x.jar") == "sql", name
        assert fn.levels["L4"]["sql"] == "L0" and "sql" in fn.levels["L0"], name
        assert (
            "handler" in fn.levels["L4"]
        ), name  # the Python twin stays the oracle's reference
    # an adopted body leaves the candidate table; the two BNG rings wait for warehouse numbers (2.6x slower than light locally)
    assert set(candidates.CANDIDATES) == {"gbx_bng_kring", "gbx_bng_kloop"}


# ---- mechanism: a candidate is data and never changes the default install ----------------------------------------

_PROBE = "gbx_bng_distance"  # a row with no SQL kernel


def _probe_candidate():
    return candidates.Candidate(_PROBE, "CAST(NULL AS BIGINT)", summary="probe")


def test_a_candidate_is_not_a_manifest_key_and_the_default_install_is_unchanged():
    """Fails when a candidate body is moved onto its row: the row would then install as a SQL function."""
    fn = manifest.load().functions[_PROBE]
    assert "sql" not in fn.levels["L4"] and "L0" not in fn.levels
    assert ddl.chosen_form(fn, None) == "python"
    # the candidate as a row copy is a SQL function; the row itself stays a handler
    assert (
        ddl.chosen_form(candidates.with_candidate(fn, _probe_candidate()), None)
        == "sql"
    )
    assert "sql" not in fn.levels["L4"]


def test_a_row_with_its_own_sql_kernel_wins_over_a_candidate():
    fn = manifest.load().functions[AREA]
    assert candidates.with_candidate(fn, _probe_candidate()) is fn


# ---- the candidates against the oracle rows ----------------------------------------------------------------------


@pytest.mark.parametrize("name", [AREA, ENAB])
def test_a_kernel_passes_every_oracle_row_of_its_function(spark, tmp_path, name):
    cases = _rows(name)
    assert len(cases) >= 4, name
    ran = oracles.run_sql_kernels(spark, cases, tmpdir=str(tmp_path))
    assert len(ran) == len(cases)
    assert [m for _, m in ran if m] == []


# ---- gbx_bng_cellarea against the Python core ----------------------------------------------------------------------


def _cells():
    rnd = random.Random(11)
    out = []
    for res in sorted(_bng.RESOLUTIONS):
        for _ in range(60):
            e, n = rnd.uniform(0, 700000), rnd.uniform(0, 1300000)
            out.append(_bng.point_as_cell(e, n, res))
    return out


# strings that are no cell, or are one in an odd shape: the Python parse decides each
ODD_CELLS = [
    "T", "S", "N", "O", "H", "J", "X", "", "TQ", "SV", "HZ", "JX", "II", "XX9999", "TQSW", "TQ3080SW", "TQ30A0",
    "tq3080", "TQ308", "TQ3", "TSW", "TQ38SW", "TQ3000080000", "TQ300008000SE", "HA", "JA", "TD", "OD", "SX1234",
]  # fmt: skip


def _sql_area(spark, cell):
    return spark.sql(f"SELECT {AREA}({_lit(cell)}) AS a").first()["a"]


def _lit(text):
    """A SQL string literal for ``text`` (the parser unescapes backslashes, so each is doubled)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def test_cellarea_equals_the_python_core_for_every_cell_the_library_issues(installed):
    cells = _cells() + ODD_CELLS
    got = {
        r["c"]: r["a"]
        for r in installed.createDataFrame([(c,) for c in cells], "c string")
        .selectExpr("c", f"{AREA}(c) AS a")
        .collect()
    }
    assert len(got) == len(set(cells))
    wrong = [
        (c, got[c], handlers.bng_cellarea(c))
        for c in cells
        if got[c] != handlers.bng_cellarea(c)
    ]
    assert wrong == []


def test_cellarea_raises_below_one_metre_as_the_handler_does(installed):
    """The limit that was a NULL is closed: a cell finer than 1 m raises on every level, naming the cell."""
    assert _bng.get_resolution_str(7) == ""  # the Python core has no 7th resolution
    for cell in ("TQ300008000012", "TQ3000080000SW", "TQ30000800001234"):
        with pytest.raises(ValueError, match="finer than 1 m"):
            handlers.bng_cellarea(cell)
        with pytest.raises(Exception, match=f"{cell}.*finer than 1 m"):
            _sql_area(installed, cell)


def test_cellarea_digits_python_reads_as_numbers_are_the_documented_divergence(
    installed,
):
    """Pinned: a sign, white space or underscore in the digits gives NULL in SQL. The Python area for such an id comes
    from the digit count of an integer id that the signed number has carried into, so 'SV-9-9' is a 100 km2 cell while
    'TQ+3+0' is 1 km2. No SQL form of the area equals that, and no library issues such an id.
    """
    assert handlers.bng_cellarea("TQ+3+0") == 1.0
    assert handlers.bng_cellarea("SV-9-9") == 100.0
    for cell in ("TQ+3+0", "TQ 3 0", "SV-9-9"):
        assert handlers.bng_cellarea(cell) is not None
        assert _sql_area(installed, cell) is None


# ---- gbx_bng_eastnorthasbng against the Python core ----------------------------------------------------------------

RES_FORMS = [
    -6,
    -5,
    -4,
    -3,
    -2,
    -1,
    1,
    2,
    3,
    4,
    5,
    6,
    "100m",
    "500km",
    "1km",
    " 3 ",
    "3.0",
    "03",
    "-3",
    "\t3\n",
    "\u00a0-3\u3000",
    "\u20031km\u2028",
    "\x1c500m\x1f",
]


def test_eastnorthasbng_equals_the_python_core_for_every_resolution_form(installed):
    rnd = random.Random(5)
    # inside the grid, past its east and north edges (the letter index clamps), and a little west and south of the origin
    rows = [
        (rnd.uniform(-90000, 3_000_000), rnd.uniform(-90000, 3_000_000), str(res))
        for res in RES_FORMS
        for _ in range(120)
    ]
    rows += [(rnd.uniform(0, 700000), rnd.uniform(0, 1300000), "3") for _ in range(400)]
    rows += [
        (530000.0, 180000.0, "3"),
        (0.0, 0.0, "3"),
        (699999.9999, 1299999.9999, "6"),
    ]
    got = (
        installed.createDataFrame(
            [(i, *r) for i, r in enumerate(rows)], "i int, e double, n double, r string"
        )
        .selectExpr("i", f"{ENAB}(e, n, r) AS cell")
        .collect()
    )
    wrong = []
    for r in got:
        e, n, res = rows[r["i"]]
        want = handlers.bng_eastnorthasbng(e, n, res)
        if r["cell"] != want:
            wrong.append((e, n, res, r["cell"], want))
    assert wrong[:5] == []


@pytest.mark.parametrize(
    "bad",
    [
        "3km",
        "1001",
        "0",
        "abc",
        "",
        "3.5",
        "+3",
        "99999999999999999999",
        "0001001",
        "-0",
        "\u00a01001\t",
    ],
)
def test_eastnorthasbng_refuses_a_resolution_bng_does_not_define_with_the_python_text(
    installed, bad
):
    with pytest.raises(ValueError) as want:  # the Python core's refusal
        handlers.bng_eastnorthasbng(530000.0, 180000.0, bad)
    with pytest.raises(Exception) as err:
        installed.sql(f"SELECT {ENAB}(530000D, 180000D, {_lit(bad)}) AS c").collect()
    # the same message, semicolons included (the SQL text spells them chr(59): a statement file splits on a literal one)
    assert str(want.value) in str(err.value)


def test_eastnorthasbng_is_null_for_a_null_or_non_finite_input_before_the_resolution_is_read(
    installed,
):
    out = installed.sql(
        f"""SELECT {ENAB}(CAST(NULL AS DOUBLE), 1D, '3'), {ENAB}(1D, CAST(NULL AS DOUBLE), '3'),
        {ENAB}(1D, 1D, CAST(NULL AS STRING)), {ENAB}(CAST('NaN' AS DOUBLE), 1D, 'bad'),
        {ENAB}(CAST('Infinity' AS DOUBLE), 1D, 'bad'), {ENAB}(1D, CAST('-Infinity' AS DOUBLE), 'bad')"""
    ).first()
    assert tuple(out) == (None,) * 6


def test_the_cellarea_prefix_pattern_accepts_exactly_the_112_letter_pairs(installed):
    import string

    valid = {p for row in _bng.LETTER_MAP for p in row}
    assert len(valid) == 112
    pairs = [a + b for a in string.ascii_uppercase for b in string.ascii_uppercase]
    got = {
        r["c"]: r["a"]
        for r in installed.createDataFrame([(p,) for p in pairs], "c string")
        .selectExpr("c", f"{AREA}(c) AS a")
        .collect()
    }
    assert {p for p, a in got.items() if a is not None} == valid
    assert all(got[p] == 10000.0 for p in valid)


def test_the_sweep_is_not_vacuous_a_changed_quadrant_letter_is_caught(spark):
    body, helpers = _kernel(ENAB)
    helpers = tuple(
        {**h, "sql": h["sql"].replace("'SW', 'NW'", "'NW', 'SW'")} for h in helpers
    )
    spark.sql(ddl.local_function_ddl(manifest.load().functions[ENAB], (body, helpers)))
    try:
        got = spark.sql(f"SELECT {ENAB}(530000D, 180000D, '-3') AS c").first()["c"]
    finally:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {ENAB}")
    assert got == "TQ38NW" != handlers.bng_eastnorthasbng(530000.0, 180000.0, "-3")


def test_eastnorthasbng_equals_the_python_core_to_the_edge_of_its_window(installed):
    """Equal from -99999 m to 9 999 999 m on both axes (the id fields of the core hold there), every resolution."""
    edge = [-99999.9, -99999.0, -50000.5, -1.0, 0.0, 700000.0, 1299999.5, 9_999_999.0]
    rows = [
        (e, n, str(r)) for e in edge for n in edge for r in sorted(_bng.RESOLUTIONS)
    ]
    got = (
        installed.createDataFrame(
            [(i, *r) for i, r in enumerate(rows)], "i int, e double, n double, r string"
        )
        .selectExpr("i", f"{ENAB}(e, n, r) AS cell")
        .collect()
    )
    wrong = [
        (rows[r["i"]], r["cell"], handlers.bng_eastnorthasbng(*rows[r["i"]]))
        for r in got
        if r["cell"] != handlers.bng_eastnorthasbng(*rows[r["i"]])
    ]
    assert wrong[:5] == []


def test_eastnorthasbng_outside_the_window_is_the_documented_divergence(installed):
    """Pinned: 100 km west or south of the origin, and 10 000 km out, the core's integer id overflows its fields and
    returns a letter pair read from the wrong digits. The SQL form clamps the letter index instead.
    """
    for e, n in (
        (-150000.0, 180000.0),
        (530000.0, -250000.0),
        (10_500_000.0, 180000.0),
    ):
        py = handlers.bng_eastnorthasbng(e, n, "3")
        sql = installed.sql(f"SELECT {ENAB}({e}D, {n}D, '3') AS c").first()["c"]
        assert sql != py, (e, n)
