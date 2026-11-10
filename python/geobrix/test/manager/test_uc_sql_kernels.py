"""The first SQL-kernel functions (gbx_quadbin_resolution, gbx_custom_grid, gbx_rst_memsize) and the mechanisms that
judge them: the one-body rule of L0, the host runner of UC SQL-kernel rows, and recipe rows.

A kernel uses only the engine's core built-ins (bit operations, named_struct, raise_error, length), so the host's local
Spark proves every body here. A body that calls a product built-in (`h3_*`, `st_*`) is not provable on the host (the
local engine has none); the workspace pass judges those.
"""

import copy
import json
import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracle_tables._uc_tile import recipe
from databricks.labs.gbx.manager import levels, sqlwrap
from databricks.labs.gbx.manifest import sql_type
from databricks.labs.gbx.uc import ddl, sqlbody

RAW = json.loads((Path(manifest.__file__).with_name("functions.json")).read_text())
QUADBIN, CUSTOM, MEMSIZE = (
    "gbx_quadbin_resolution",
    "gbx_custom_grid",
    "gbx_rst_memsize",
)
KERNELS = (QUADBIN, CUSTOM, MEMSIZE)


def _uc_cases(name):
    fn = manifest.load().functions[name]
    return [c for c in oracles.TABLES[fn.oracle] if c.fn == name and "uc" in c.surfaces]


def _fail(results):
    return [m for _, m in results if m]


# ---- the contract ----------------------------------------------------------------------------------------------


def test_the_three_functions_are_sql_kernels_on_uc_and_two_are_l0_in_the_session():
    man = manifest.load()
    for name in (QUADBIN, CUSTOM):
        fn = man.functions[name]
        assert fn.levels["L4"] == {"sql": "L0"}, name  # one body, the session's
        assert "sql" in fn.levels["L0"], name
    mem = man.functions[MEMSIZE]
    assert (
        set(mem.levels["L4"]) == {"sql"} and "L0" not in mem.levels
    )  # the session keeps its level
    assert mem.params[0].uc == "TILE" and mem.returns_uc == "BIGINT"
    # the rows exist and each kernel has the UC rows the contract names
    for name in KERNELS:
        assert _uc_cases(name), name
    kinds = {c.cid for c in _uc_cases(MEMSIZE)}
    assert {
        "memsize",
        "uc_memsize_virtual_refused",
        "uc_memsize_null_tile",
        "uc_memsize_error_tile",
    } <= kinds


def test_the_sql_kernel_install_is_a_language_sql_function_with_no_python():
    man = manifest.load()
    for name in KERNELS:
        (text,) = ddl.function_ddl(
            man.functions[name], "c", "s", "file:///w.whl", man.uc_dependencies, "0.6"
        )
        assert "LANGUAGE SQL\nDETERMINISTIC" in text and "PYTHON" not in text, name
    # the DEFAULT of the custom grid's SRID is declared on the SQL function itself
    (text,) = ddl.function_ddl(
        man.functions[CUSTOM], "c", "s", "file:///w.whl", man.uc_dependencies, "0.6"
    )
    assert "srid INT DEFAULT -1" in text


# ---- mechanism: one L0 body serves the session and UC ------------------------------------------------------------


def test_every_l0_row_has_one_body_for_the_session_and_uc():
    """Fails when a row's UC text drifts from its session text (an L4 body of its own beside an L0 one)."""
    rows = [f for f in manifest.load().functions.values() if "L0" in f.levels]
    assert {QUADBIN, CUSTOM} <= {f.sql_name for f in rows}
    for fn in rows:
        uc_text = sqlbody.inlined(*sqlbody.l4_source(fn))
        session_text = sqlbody.inlined(*sqlbody.l0_source(fn))
        assert uc_text == session_text, fn.sql_name
        assert sqlwrap.sql_kernel(fn).endswith(f"RETURN {session_text}"), fn.sql_name
        assert ddl.local_function_ddl(fn).endswith(f"RETURN {uc_text}"), fn.sql_name


def test_the_drift_check_sees_an_l4_body_of_its_own():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == QUADBIN)
    row["levels"]["L4"] = {"sql": "CAST(cell AS INT)"}  # a second body: the drift
    fn = manifest.parse(raw).functions[QUADBIN]
    assert sqlbody.inlined(*sqlbody.l4_source(fn)) != sqlbody.inlined(
        *sqlbody.l0_source(fn)
    )


def test_l0_is_not_a_default_for_the_new_kernels():
    from databricks.labs.gbx.manager import capabilities

    caps = capabilities.Capabilities(
        connect=False,
        serverless=False,
        jar=None,
        jar_reason="no JAR",
        gdal_reason="",
        sql_functions=True,
    )
    for name in (QUADBIN, CUSTOM):
        assert levels.select(manifest.load().functions[name], caps).level != "L0", name


def test_l0_replaces_a_function_an_earlier_install_registered_as_a_udf(spark):
    """Fails without the DROP before the CREATE: Spark cannot replace a registered UDF with a SQL function, so an L0
    install after an L3 one in the same session would refuse."""
    from databricks.labs.gbx import register

    register(spark, only=[QUADBIN], _levels={QUADBIN: ("L3", "c0")})
    try:
        register(spark, only=[QUADBIN], _levels={QUADBIN: ("L0", "-")})
        assert spark.sql(f"SELECT {QUADBIN}(5233961839712272383)").first()[0] == 10
    finally:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {QUADBIN}")


# ---- mechanism: the UC rows of a SQL kernel run on the host --------------------------------------------------------


@pytest.mark.parametrize("name", KERNELS)
def test_the_uc_rows_of_a_sql_kernel_pass_on_a_local_spark(spark, tmp_path, name):
    cases = _uc_cases(name)
    ran = oracles.run_sql_kernels(spark, cases, tmpdir=str(tmp_path))
    assert len(ran) == len(cases)  # every UC row ran: none was skipped
    assert _fail(ran) == []


def test_the_host_runner_fails_a_wrong_body_and_drops_what_it_made(spark, tmp_path):
    """Not vacuous: a body that is off by one fails the quadbin rows, and no temporary function is left behind."""
    cases = _uc_cases(QUADBIN)
    bad = {QUADBIN: ("CAST(((cell >> 52) & 31) + 1 AS INT)", ())}
    ran = oracles.run_sql_kernels(spark, cases, sources=bad, tmpdir=str(tmp_path))
    red = {c.cid for c, m in ran if m}
    assert {"qb_resolution", "qb_resolution_z5", "qb_resolution_z0"} <= red
    assert "qb_resolution_null" not in red  # NULL in is still NULL out
    assert not spark.catalog.functionExists(QUADBIN)


def test_the_kernel_text_is_the_session_semantics_at_the_edges(spark):
    """The ends the rows pin, asked of the engine directly: the zoom field of an id is read as it is."""
    spark.sql(ddl.local_function_ddl(manifest.load().functions[QUADBIN], name="qb_res"))
    try:
        got = spark.sql(
            "SELECT qb_res(5192650370358181887), qb_res(5305923650554237558), qb_res(0), qb_res(CAST(NULL AS BIGINT))"
        ).first()
        assert tuple(got) == (0, 26, 0, None)
    finally:
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS qb_res")


def test_a_virtual_tile_is_refused_with_the_fix_and_a_materialized_one_is_its_length(
    spark, tmp_path
):
    ran = dict(
        (c.cid, m)
        for c, m in oracles.run_sql_kernels(
            spark, _uc_cases(MEMSIZE), tmpdir=str(tmp_path)
        )
    )
    assert ran["uc_memsize_virtual_refused"] is None  # "Fix:" in the refusal
    assert ran["memsize"] is None  # the buffer length, BIGINT


# ---- mechanism: recipe rows ------------------------------------------------------------------------------------


def test_a_recipe_row_is_one_statement_judged_on_its_out_column(spark):
    good = recipe(
        "r_sum",
        QUADBIN,
        "SELECT sum(id) AS out FROM range(5)",
        "exact",
        10,
    )
    bad = recipe(
        "r_sum_wrong", QUADBIN, "SELECT sum(id) AS out FROM range(5)", "exact", 11
    )
    raised = recipe(
        "r_raise",
        QUADBIN,
        "SELECT raise_error('nope: Fix: x') AS out",
        "uc_refused",
    )
    two = recipe("r_two", QUADBIN, "SELECT id AS out FROM range(2)", "exact", 0)
    ran = dict(
        (c.cid, m)
        for c, m in oracles.run_session(spark, [good, bad, raised, two], surface="uc")
    )
    assert ran["r_sum"] is None and ran["r_raise"] is None
    assert "got 10, want 11" in ran["r_sum_wrong"]
    assert "2 rows" in ran["r_two"]


def test_a_recipe_statement_carries_the_schema_qualifier_on_uc_and_none_in_a_session(
    spark,
):
    row = recipe("r_q", QUADBIN, "SELECT {q}gbx_x(1) AS out", "exact", 1)
    ((cid, stmt),) = oracles.uc_sql([row], "cat", "sch", "/tmp")
    assert cid == "r_q" and stmt == "SELECT `cat`.`sch`.gbx_x(1) AS out"
    assert oracles.render_recipe(row.sql) == "SELECT gbx_x(1) AS out"


def test_a_recipe_must_name_its_answer_and_a_manifest_function():
    with pytest.raises(ValueError, match="AS out"):
        recipe("r_noout", QUADBIN, "SELECT 1", "exact", 1)
    with pytest.raises(ValueError, match="no manifest function"):
        recipe("r_nofn", "gbx_not_a_function", "SELECT 1 AS out", "exact", 1)


def test_the_recipe_row_of_the_grid_table_runs_on_the_host_through_the_kernel(
    spark, tmp_path
):
    rows = [
        c
        for c in oracles.TABLES["GRID_SCALAR"]
        if c.sql is not None and c.fn == QUADBIN
    ]
    assert [c.cid for c in rows] == ["recipe_qb_finest_zoom"]
    ran = oracles.run_sql_kernels(spark, _uc_cases(QUADBIN), tmpdir=str(tmp_path))
    assert "recipe_qb_finest_zoom" in {c.cid for c, _ in ran}
    assert _fail(ran) == []


def test_a_helper_is_created_after_every_helper_it_calls():
    """Unity Catalog resolves a SQL body when it is created, so a helper that calls another must come after it
    (gbx__h3_cellfill_run calls gbx__h3_cellfill_apply: the install died at the first on Serverless and the warehouse).
    """
    checked = 0
    for fn in manifest.load().functions.values():
        src = sqlbody.l4_source(fn)
        if not src:
            continue
        seen = set()
        for h in src[1]:
            called = set(sqlbody._CALL.findall(h["sql"]))
            assert called <= seen, (fn.sql_name, h["name"], sorted(called - seen))
            seen.add(h["name"])
            checked += 1
    assert checked > 10


def test_the_dependency_order_keeps_the_declared_order_otherwise():
    a, b, c = (
        {"name": "gbx__a", "sql": "{q}gbx__c(x)"},
        {"name": "gbx__b", "sql": "x"},
        {"name": "gbx__c", "sql": "x"},
    )
    assert [h["name"] for h in sqlbody.in_dependency_order((a, b, c))] == [
        "gbx__c",
        "gbx__a",
        "gbx__b",
    ]


# ---- mechanism: Unity Catalog binds a helper's arguments first ---------------------------------------------------

_PRODUCT_CALL = re.compile(r"(?<![\w.])(?:h3|st)_\w+\(", re.IGNORECASE)


def _host_kernels():
    for fn in sorted(manifest.load().functions.values(), key=lambda f: f.sql_name):
        src = sqlbody.l4_source(fn)
        if (
            src
            and src[1]
            and not any(
                _PRODUCT_CALL.search(t) for t in [src[0]] + [h["sql"] for h in src[1]]
            )
        ):
            yield fn.sql_name


@pytest.mark.parametrize("name", list(_host_kernels()))
def test_the_uc_rows_hold_with_each_helper_a_function_of_its_own(spark, name):
    """On Unity Catalog a helper is a function, and the engine computes its arguments before the body that calls it
    (DBSQL raised for `gbx_bng_eastnorthasbng(e, n, NULL)`: the NULL resolution went into gbx__bng_resindex, ahead of
    the body's own NULL test). The host's inlined form evaluates them where the call is, so it cannot see that; this
    installs the helpers as temporary functions of their own and runs the UC rows the same way.
    """
    fn = manifest.load().functions[name]
    body, helpers = sqlbody.l4_source(fn)
    made = []
    try:
        for h in helpers:
            sig = ", ".join(f"{n} {sql_type(t)}" for n, t in h["params"])
            spark.sql(
                f"CREATE OR REPLACE TEMPORARY FUNCTION {h['name']}({sig}) RETURNS {sql_type(h['returns'])} "
                f"RETURN {sqlbody.render(h['sql'], '')}"
            )
            made.append(h["name"])
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
        spark.sql(
            f"CREATE OR REPLACE TEMPORARY FUNCTION {name}({ddl._params_sig(fn)}) "
            f"RETURNS {sql_type(fn.returns_uc)} RETURN {sqlbody.render(body, '')}"
        )
        made.append(name)
        cases = [
            c
            for t in oracles.TABLES.values()
            for c in t
            if c.fn == name and "uc" in c.surfaces
        ]
        assert cases, name
        assert _fail(oracles.run_session(spark, cases, surface="uc")) == []
    finally:
        for n in made:
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
