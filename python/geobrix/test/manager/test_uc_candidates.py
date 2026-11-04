"""The SQL candidates for gbx_bng_cellarea and gbx_bng_eastnorthasbng (uc.candidates): judged on a local Spark against the
rows of their oracle tables and against the Python core over a sweep. Both bodies use only core built-ins (string, bit and
arithmetic functions, raise_error), so the host proves them. Whether one beats Scala or Python is the workspace
measurement; whether it is equal is decided here.
"""

import random

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.uc import candidates, ddl, handlers

AREA, ENAB = "gbx_bng_cellarea", "gbx_bng_eastnorthasbng"


def _rows(name):
    return [
        c
        for t in ("GRID_SCALAR", "MVP_S")
        for c in oracles.TABLES[t]
        if c.fn == name and "uc" in c.surfaces
    ]


@pytest.fixture
def installed(spark):
    """Both candidates as temporary functions under their product names; dropped afterwards."""
    man = manifest.load()
    for name in (AREA, ENAB):
        spark.sql(ddl.local_function_ddl(man.functions[name], candidates.source(name)))
    try:
        yield spark
    finally:
        for name in (AREA, ENAB):
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")


# ---- mechanism: a candidate is data and never changes the default install ----------------------------------------


def test_a_candidate_is_not_a_manifest_key_and_the_default_install_is_unchanged():
    """Fails when a candidate body is moved onto its row: the row would then install as a SQL function."""
    man = manifest.load()
    assert set(candidates.CANDIDATES) == {AREA, ENAB}
    for name, cand in candidates.CANDIDATES.items():
        fn = man.functions[name]
        assert "sql" not in fn.levels["L4"] and "L0" not in fn.levels, name
        assert ddl.chosen_form(fn, None) == "python", name
        assert ddl.chosen_form(fn, "/Volumes/c/s/v/x.jar") == "scala", name
        assert cand.function == name
        # the candidate as a row copy is a SQL function; the row itself stays a handler
        assert ddl.chosen_form(candidates.with_candidate(fn), None) == "sql"
        assert "sql" not in fn.levels["L4"]


def test_a_row_with_its_own_sql_kernel_wins_over_a_candidate():
    fn = manifest.load().functions[AREA]
    own = type(fn)(
        **{
            **fn.__dict__,
            "levels": {**fn.levels, "L4": {**fn.levels["L4"], "sql": "1.0"}},
        }
    )
    assert candidates.with_candidate(own) is own


# ---- the candidates against the oracle rows ----------------------------------------------------------------------


@pytest.mark.parametrize("name", [AREA, ENAB])
def test_a_candidate_passes_every_existing_oracle_row_of_its_function(
    spark, tmp_path, name
):
    cases = _rows(name)
    assert len(cases) >= 4, name
    ran = oracles.run_sql_kernels(
        spark, cases, sources={name: candidates.source(name)}, tmpdir=str(tmp_path)
    )
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


def test_cellarea_equals_the_python_core_for_every_cell_the_library_issues(installed):
    cells = _cells() + ODD_CELLS
    got = {
        r["c"]: r["a"]
        for r in installed.createDataFrame([(c,) for c in cells], "c string")
        .selectExpr("c", f"{AREA}(c) AS a")
        .collect()
    }
    wrong = []
    for c in cells:
        try:
            want = handlers.bng_cellarea(c)
        except (
            KeyError
        ):  # finer than 1 m: the handler raises, the candidate is NULL (a documented limit)
            want = None
        if got[c] != want:
            wrong.append((c, got[c], want))
    assert wrong == []


def test_cellarea_is_null_below_one_metre_where_the_handler_raises(installed):
    assert _bng.get_resolution_str(7) == ""  # the Python core has no 7th resolution
    (row,) = installed.sql(f"SELECT {AREA}('TQ300008000012') AS a").collect()
    assert row["a"] is None


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
    "bad", ["3km", "1001", "0", "abc", "", "3.5", "+3", "99999999999999999999"]
)
def test_eastnorthasbng_refuses_a_resolution_bng_does_not_define_naming_it(
    installed, bad
):
    with pytest.raises(ValueError):  # the Python core's refusal
        handlers.bng_eastnorthasbng(530000.0, 180000.0, bad)
    with pytest.raises(Exception) as err:
        installed.sql(f"SELECT {ENAB}(530000D, 180000D, '{bad}') AS c").collect()
    assert bad.strip() in str(err.value) or bad == ""


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
    body, helpers = candidates.source(ENAB)
    helpers = tuple(
        {**h, "sql": h["sql"].replace("'SW', 'NW'", "'NW', 'SW'")} for h in helpers
    )
    spark.sql(ddl.local_function_ddl(manifest.load().functions[ENAB], (body, helpers)))
    try:
        got = spark.sql(f"SELECT {ENAB}(530000D, 180000D, '-3') AS c").first()["c"]
    finally:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {ENAB}")
    assert got == "TQ38NW" != handlers.bng_eastnorthasbng(530000.0, 180000.0, "-3")
