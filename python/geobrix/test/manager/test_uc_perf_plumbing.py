"""The plumbing of the UC performance forms: SQL kernels (L4 `sql`, L0), table-function inlining, `stubs=True`,
`deps="shared"`. One fail-on-revert check per mechanism, on synthetic manifest rows (no function changes here).
"""

import copy
import hashlib
import json
import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest, uc
from databricks.labs.gbx.uc import ddl, sqlbody

RAW = json.loads((Path(manifest.__file__).with_name("functions.json")).read_text())
WHEEL = "file:///Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"
JAR = "/Volumes/c/s/v/geobrix-0.6.0-jar-with-dependencies.jar"


def _row(raw, name):
    return next(r for r in raw["functions"] if r["sql_name"] == name)


def perf_manifest(edit):
    """The packaged manifest with ``edit(raw)`` applied: a synthetic SQL kernel, stub or helper on real rows."""
    raw = copy.deepcopy(RAW)
    edit(raw)
    return manifest.parse(raw)


HELPER = {
    "name": "gbx__ring_width",
    "params": [["k", "INT"]],
    "returns": "INT",
    "sql": "2 * k + 1",
}


def _kring_sql(raw):
    """gbx_bng_kring gets a SQL kernel with one helper (the inner function of gbx_bng_kringexplode)."""
    r = _row(raw, "gbx_bng_kring")
    r["levels"]["L4"][
        "sql"
    ] = "transform(sequence(1, {q}gbx__ring_width(k)), i -> concat(cellid, cast(i AS STRING)))"
    r["levels"]["L4"]["sql_helpers"] = [HELPER]


@pytest.fixture
def kring_sql(monkeypatch):
    man = perf_manifest(_kring_sql)
    monkeypatch.setattr(manifest, "load", lambda: man)
    return man


def _ddl(fn, **kw):
    return ddl.function_ddl(
        fn, "cat", "sch", WHEEL, manifest.load().uc_dependencies, "0.6.0", **kw
    )


def test_an_l4_sql_row_is_a_deterministic_sql_function_after_its_helpers(kring_sql):
    helper, fn = _ddl(kring_sql.functions["gbx_bng_kring"])
    assert helper.startswith(
        "CREATE OR REPLACE FUNCTION `cat`.`sch`.gbx__ring_width(k INT)"
    )
    assert "LANGUAGE SQL\nDETERMINISTIC" in helper and helper.endswith(
        "RETURN 2 * k + 1"
    )
    assert fn.startswith(
        "CREATE OR REPLACE FUNCTION `cat`.`sch`.gbx_bng_kring(cellid STRING, k INT)\n"
        "RETURNS array<string>\nLANGUAGE SQL\nDETERMINISTIC\n"
    )
    assert "RETURN transform(sequence(1, `cat`.`sch`.gbx__ring_width(k))" in fn
    assert "ENVIRONMENT" not in fn and "HANDLER" not in fn and "{q}" not in fn
    # the preference: sql over scala over python, and a forced form wins
    assert (
        "LANGUAGE SQL"
        in _ddl(kring_sql.functions["gbx_bng_kring"], jar_uri=JAR, helpers=False)[0]
    )
    assert len(_ddl(kring_sql.functions["gbx_bng_kring"], helpers=False)) == 1
    py = _ddl(kring_sql.functions["gbx_bng_kring"], form="python")
    assert len(py) == 1 and "LANGUAGE PYTHON" in py[0]
    sc = _ddl(kring_sql.functions["gbx_bng_kring"], form="scala", jar_uri=JAR)
    assert "LANGUAGE SCALA" in sc[0]
    with pytest.raises(
        ValueError
    ):  # a row with no SQL kernel cannot be forced into one
        _ddl(manifest.load().functions["gbx_bng_cellarea"], form="sql")


def test_a_sql_function_declares_its_defaults_and_install_creates_helpers_once(
    kring_sql, monkeypatch
):
    def edit(raw):
        _kring_sql(raw)
        _row(raw, "gbx_bng_kring")["params"][1]["default"] = "1"
        r = _row(raw, "gbx_bng_kringexplode")
        r["levels"]["L4"] = {"tvf_over": "gbx_bng_kring"}

    man = perf_manifest(edit)
    monkeypatch.setattr(manifest, "load", lambda: man)
    _, fn = _ddl(man.functions["gbx_bng_kring"])
    assert (
        "k INT DEFAULT 1)" in fn
    )  # the SQL function holds the DEFAULT: no gbx__ wrapper pair
    stmts = uc.install(
        "c", "s", "/Volumes/c/s/v", dry_run=True, only=["bng_kring", "bng_kringexplode"]
    )
    helpers = [s for s in stmts if "gbx__ring_width(" in s.split("\n")[0]]
    assert len(helpers) == 1
    assert stmts.index(helpers[0]) < min(
        i for i, s in enumerate(stmts) if "gbx_bng_kring(" in s.split("\n")[0]
    )


def test_a_table_function_over_a_sql_kernel_inlines_it_helpers_and_parameter_names(
    monkeypatch,
):
    def edit(raw):
        _kring_sql(raw)
        # the table function's parameters are named differently from the kernel's: the inlined text follows the TVF's
        tvf = _row(raw, "gbx_bng_kringexplode")
        tvf["params"][0]["name"] = "cell"
        tvf["params"][1]["name"] = "ring"

    man = perf_manifest(edit)
    monkeypatch.setattr(manifest, "load", lambda: man)
    (tvf,) = _ddl(man.functions["gbx_bng_kringexplode"])
    body = tvf.split("RETURN SELECT ", 1)[1]
    assert (
        "gbx__ring_width" not in body and "{q}" not in body
    )  # no SQL function inside a generator
    assert "`cat`.`sch`.gbx_bng_kring" not in body
    assert body == (
        "explode((transform(sequence(1, (2 * (ring) + 1)), i -> concat(cell, cast(i AS STRING))))) AS cellid"
    )
    # the same row forced back to a call keeps the call (the Python array function)
    (called,) = _ddl(man.functions["gbx_bng_kringexplode"], form="python")
    assert (
        "RETURN SELECT explode(`cat`.`sch`.gbx_bng_kring(cell, ring)) AS cellid"
        in called
    )


def test_inlining_leaves_quoted_text_and_field_names_alone_and_refuses_an_undeclared_helper():
    body = "named_struct('k', k, 'v', x.k, 'q', {q}gbx__h(k + 1))"
    h = [{"name": "gbx__h", "params": [["k", "INT"]], "returns": "INT", "sql": "k * 2"}]
    assert (
        sqlbody.inlined(body, h) == "named_struct('k', k, 'v', x.k, 'q', ((k + 1) * 2))"
    )
    with pytest.raises(ValueError, match="not a declared helper"):
        sqlbody.inlined("{q}gbx__nope(1)", h)
    with pytest.raises(ValueError, match="takes 1 argument"):
        sqlbody.inlined("{q}gbx__h(1, 2)", h)
    nested = h + [
        {
            "name": "gbx__g",
            "params": [["a", "INT"]],
            "returns": "INT",
            "sql": "{q}gbx__h(a) + 1",
        }
    ]
    assert sqlbody.inlined("{q}gbx__g(3)", nested) == "((((3)) * 2) + 1)"


def test_the_session_and_uc_share_one_l0_body():
    def edit(raw):
        r = _row(raw, "gbx_quadbin_resolution")
        r["levels"]["L0"] = {"sql": "bit_and(shiftright(cell, 52), 31)"}
        r["levels"]["L4"] = {"sql": "L0"}

    man = perf_manifest(edit)
    fn = man.functions["gbx_quadbin_resolution"]
    assert sqlbody.l4_source(fn) == sqlbody.l0_source(fn)
    (text,) = ddl.function_ddl(fn, "cat", "sch", WHEEL, man.uc_dependencies, "0.6.0")
    assert text.endswith("RETURN bit_and(shiftright(cell, 52), 31)")
    assert "L4" in fn.levels and "L0" in fn.levels


def test_the_sql_keys_have_rules_a_load_enforces():
    def fails(edit, match):
        with pytest.raises(ValueError, match=match):
            perf_manifest(edit)

    def l4_from_l0_without_l0(raw):
        _row(raw, "gbx_quadbin_resolution")["levels"]["L4"] = {"sql": "L0"}

    def helpers_without_body(raw):
        _row(raw, "gbx_quadbin_resolution")["levels"]["L0"] = {"sql_helpers": [HELPER]}

    def bad_helper_name(raw):
        r = _row(raw, "gbx_quadbin_resolution")
        r["levels"]["L0"] = {"sql": "1", "sql_helpers": [{**HELPER, "name": "helper"}]}

    def duplicate_helper(raw):
        r = _row(raw, "gbx_quadbin_resolution")
        r["levels"]["L0"] = {"sql": "1", "sql_helpers": [HELPER, HELPER]}

    def stub_with_handler(raw):
        _row(raw, "gbx_bng_kring")["levels"]["L4"]["stub"] = "x"

    def tvf_with_body(raw):
        _row(raw, "gbx_bng_kringexplode")["levels"]["L4"]["sql"] = "1"

    fails(l4_from_l0_without_l0, "no L0 sql")
    fails(helpers_without_body, "sql_helpers without sql")
    fails(bad_helper_name, "must be gbx__<name>")
    fails(duplicate_helper, "declared twice")
    fails(stub_with_handler, "Fix text alone")
    fails(tvf_with_body, "no sql body of its own")
    assert {"sql", "sql_helpers", "stub"} <= manifest.LEVEL_KEYS["L4"]
    assert manifest.LEVEL_KEYS["L0"] == {"sql", "sql_helpers"}


def _stub_manifest(monkeypatch):
    def edit(raw):
        _row(raw, "gbx_h3_cellfill")["levels"] = {
            **_row(raw, "gbx_h3_cellfill")["levels"],
            "L4": {"stub": "use gbx_h3_cellfill_array."},
        }
        _row(raw, "gbx_st_crs")["levels"]["L4"] = {"stub": "use st_srid(geom)."}

    man = perf_manifest(edit)
    monkeypatch.setattr(manifest, "load", lambda: man)
    return man


def test_stubs_are_opt_in_scalars_only_and_raise_with_the_fix(monkeypatch):
    man = _stub_manifest(monkeypatch)
    assert "L4" not in man.functions["gbx_st_crs"].levels  # a stub is no UC function
    assert "uc" not in man.functions["gbx_st_crs"].surfaces
    plain = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True)
    assert not any("raise_error" in s for s in plain)
    assert not any("gbx_st_crs(" in s.split("\n")[0] for s in plain)
    with_stubs = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, stubs=True)
    assert with_stubs[: len(plain)] == plain  # the same statements, then the stubs
    (stub,) = [s for s in with_stubs[len(plain) :]]  # the aggregate gets none
    assert stub.startswith(
        "CREATE OR REPLACE FUNCTION `c`.`s`.gbx_st_crs(geom GEOMETRY(ANY))\nRETURNS STRING\nLANGUAGE SQL"
    )
    assert stub.endswith(
        "RETURN raise_error('gbx_st_crs runs in a notebook or job session. Fix: use st_srid(geom).')"
    )
    only = uc.install(
        "c",
        "s",
        "/Volumes/c/s/v",
        dry_run=True,
        stubs=True,
        only=["st_crs", "bng_kring"],
    )
    assert len(only) == 2 and "gbx_bng_kring(" in only[0] and "raise_error" in only[1]
    with pytest.raises(ValueError, match="stubs=True"):
        uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, only=["st_crs"])
    with pytest.raises(ValueError):
        ddl.stub_ddl(
            man.functions["gbx_h3_cellfill"], "c", "s", "0.6.0"
        )  # an aggregate


def test_shared_deps_gives_every_python_function_the_whole_list():
    deps = manifest.load().uc_dependencies
    default = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, procedures=True)
    shared = uc.install(
        "c", "s", "/Volumes/c/s/v", dry_run=True, procedures=True, deps="shared"
    )
    assert len(default) == len(shared)

    def env(stmt):
        m = re.search(r"dependencies = '(\[.*?\])'", stmt)
        return None if m is None else json.loads(m.group(1).replace("\\'", "'"))

    envs_default = {tuple(e[1:]) for e in map(env, default) if e}
    envs_shared = {tuple(e[1:]) for e in map(env, shared) if e}
    assert len(envs_default) > 1  # today each distinct list is its own environment
    assert envs_shared == {tuple(f"{n}=={v}" for n, v in deps.items())}
    # only the environment text differs; names, order and every other statement are the same
    for a, b in zip(default, shared):
        assert re.sub(r"dependencies = '.*?'", "", a) == re.sub(
            r"dependencies = '.*?'", "", b
        )
    with pytest.raises(ValueError, match="per-function"):
        uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, deps="all")


def test_the_default_install_is_unchanged_for_every_function_without_a_sql_kernel():
    """A dry-run install with the default flags emits, for each routine it emitted before the SQL-kernel work, the same
    statement (pinned by hash); only a row that gained an L4 `sql` key may differ."""
    golden = json.loads(
        (Path(__file__).parent / "fixtures" / "uc_install_default.json").read_text()
    )
    got = {}
    for s in uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, procedures=True):
        name = re.match(
            r"CREATE OR REPLACE (?:FUNCTION|PROCEDURE) `c`\.`s`\.(\w+)", s
        ).group(1)
        got[name] = hashlib.sha256(s.encode()).hexdigest()
    man = manifest.load()
    for name, digest in golden.items():
        fn = man.functions.get(name)
        if fn is not None and "sql" in fn.levels.get("L4", {}):
            continue  # a row moved to a SQL kernel is a deliberate change
        assert got.get(name) == digest, name


# ---- L0: the session's SQL kernel level ------------------------------------------------------------------------


def _caps(**kw):
    from databricks.labs.gbx.manager import capabilities as caps_mod

    base = dict(
        connect=False,
        serverless=False,
        jar=None,
        jar_reason="no JAR",
        gdal_reason="",
        sql_functions=True,
    )
    base.update(kw)
    return caps_mod.Capabilities(**base)


class _RecordingSpark:
    def __init__(self, fail=False):
        self.stmts, self.fail = [], fail

    def sql(self, text):
        if self.fail:
            raise RuntimeError("temporary SQL functions are not allowed here")
        self.stmts.append(text)


def _l0_manifest(helpers=False):
    def edit(raw):
        r = _row(raw, "gbx_quadbin_resolution")
        r["levels"]["L0"] = {
            "sql": (
                "{q}gbx__res_bits(cell)"
                if helpers
                else "bit_and(shiftright(cell, 52), 31)"
            )
        }
        if helpers:
            r["levels"]["L0"]["sql_helpers"] = [
                {
                    "name": "gbx__res_bits",
                    "params": [["c", "BIGINT"]],
                    "returns": "INT",
                    "sql": "bit_and(shiftright(c, 52), 31)",
                }
            ]
        r["levels"]["L4"] = {"sql": "L0"}

    return perf_manifest(edit)


def test_l0_is_chosen_only_where_the_level_table_prefers_it_and_installs_a_temporary_sql_function(
    monkeypatch,
):
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.manager import levels

    fn = _l0_manifest(helpers=True).functions["gbx_quadbin_resolution"]
    # unpreferred: the level exists in the manifest, yet the session keeps its measured level
    assert levels.select(fn, _caps()).level == "L3"
    monkeypatch.setitem(levels.PREFERENCE, "gbx_quadbin_resolution", ("L0", "L3"))
    choice = levels.select(fn, _caps())
    assert (choice.level, choice.variant) == ("L0", "-")
    # where SQL functions cannot be created the next level serves, and says why
    no_sql = levels.select(fn, _caps(sql_functions=False))
    assert no_sql.level == "L3" and "SQL functions are unavailable" in no_sql.reasons[0]
    # install: one temporary function, the session signature, the helper inlined (no helper function in the session)
    spark, lines = _RecordingSpark(), []
    manager._install(spark, fn, choice, _caps(), forced=False, lines=lines)
    assert spark.stmts == [
        "CREATE OR REPLACE TEMPORARY FUNCTION gbx_quadbin_resolution(cell BIGINT) "
        "RETURNS INT RETURN (bit_and(shiftright((cell), 52), 31))"
    ]
    assert not lines


def test_an_l0_only_function_is_served_and_a_failed_l0_install_names_the_reason(
    monkeypatch,
):
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.manager import levels

    base = _l0_manifest().functions["gbx_quadbin_resolution"]
    only_l0 = type(base)(**{**base.__dict__, "levels": {"L0": base.levels["L0"]}})
    assert levels.select(only_l0, _caps()).level == "L0"  # no other level to prefer
    # the SQL engine refuses the statement: forced, the error says so; unforced with an L3 behind it, the L3 serves
    spark = _RecordingSpark(fail=True)
    with pytest.raises(RuntimeError, match="forced L0 failed"):
        manager._install(
            spark, base, levels.Choice("L0", "-", ()), _caps(), forced=True, lines=[]
        )
    with pytest.raises(RuntimeError, match="no other level"):
        manager._install(
            spark,
            only_l0,
            levels.Choice("L0", "-", ()),
            _caps(),
            forced=False,
            lines=[],
        )


# ---- twin(): an oracle row for a UC form that is not the session's shape --------------------------------------


def _agg_rows():
    from databricks.labs.gbx.bench.oracles import Case

    return (
        Case("agg_a", "gbx_rst_merge_agg", ([(1, "x"), (2, "y")], 3), "exact", "ok"),
        Case(
            "agg_b",
            "gbx_rst_merge_agg",
            ([(5, "z")], 3),
            "exact",
            "ok",
            variants=(([(6, "w")], 3),),
        ),
        Case("agg_py", "gbx_rst_merge_agg", ([],), "exact", None, surfaces=("python",)),
    )


def test_twin_copies_a_session_row_to_the_uc_form_with_adapted_arguments_and_output():
    from databricks.labs.gbx.bench.oracle_tables._uc_tile import (
        Adapt,
        collect_group,
        twin,
    )

    rows = _agg_rows()
    out = twin(
        rows,
        "gbx_rst_merge",
        Adapt(args=collect_group(lambda m: m[1]), expect=lambda e: e.upper()),
        "agg_a",
        "agg_b",
    )
    a, b = out
    assert (a.cid, a.fn, a.surfaces, a.twin_of) == (
        "agg_a__rst_merge",
        "gbx_rst_merge",
        ("uc",),
        "agg_a",
    )
    assert a.args == (["x", "y"], 3) and a.expect == "OK" and a.kind == "exact"
    assert b.variants == (
        (["w"], 3),
    )  # every argument row is adapted, variants included
    # the kind of the check can differ on UC (the session payload is decoded there)
    (c,) = twin(rows, "gbx_rst_merge", Adapt(kind="approx"), "agg_a")
    assert c.kind == "approx" and c.args == rows[0].args
    # nothing else is copied: the originals are untouched and the python-only row is not a candidate
    assert rows[0].surfaces == ("session", "uc") and rows[0].twin_of is None


def test_twin_refuses_a_row_that_is_not_there_a_python_only_row_a_same_name_form_and_a_missing_uc_form():
    from databricks.labs.gbx.bench.oracle_tables._uc_tile import Adapt, twin

    rows = _agg_rows()
    with pytest.raises(AssertionError, match="no such oracle rows"):
        twin(rows, "gbx_rst_merge", Adapt(), "nope")
    with pytest.raises(ValueError, match="session"):
        twin(rows, "gbx_rst_merge", Adapt(), "agg_py")
    same = (_agg_rows()[0].__class__("same", "gbx_rst_merge", ([],), "exact", None),)
    with pytest.raises(ValueError, match="share"):
        twin(same, "gbx_rst_merge", Adapt(), "same")
    with pytest.raises(ValueError, match="no UC function"):
        twin(rows, "gbx_does_not_exist", Adapt(), "agg_a")
    with pytest.raises(ValueError, match="no UC function"):
        twin(
            rows, "gbx_quadbin_cellunion", Adapt(), "agg_a"
        )  # a session function, no L4
