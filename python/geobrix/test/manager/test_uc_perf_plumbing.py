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
        _ddl(manifest.load().functions["gbx_bng_distance"], form="sql")


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


def test_render_substitutes_only_in_code_and_inlined_agrees():
    body = "concat('{q}x', {q}gbx__h(1), `{q}y`)"
    assert (
        sqlbody.render(body, "`c`.`s`.") == "concat('{q}x', `c`.`s`.gbx__h(1), `{q}y`)"
    )
    # a {q} inside a string is text for inlining too: it is no leftover
    assert sqlbody.inlined("concat('{q}x', 1)", []) == "concat('{q}x', 1)"


def test_rename_is_case_sensitive_whole_word_and_leaves_types_alone():
    # a parameter that spells a type never rewrites a CAST target, in either case
    assert sqlbody.rename("CAST(x AS STRING)", {"string": "(1)"}) == "CAST(x AS STRING)"
    assert sqlbody.rename("cast(x as string)", {"string": "(1)"}) == "cast(x as string)"
    assert sqlbody.rename("string + 1", {"string": "(2)"}) == "(2) + 1"
    # case-sensitive and word-bounded: K and kk are other names
    assert sqlbody.rename("K + k + kk + k2", {"k": "(1)"}) == "K + (1) + kk + k2"
    # a backtick-quoted parameter is the parameter; any other quoted text is not
    assert sqlbody.rename("`k` + 'k' + `other`", {"k": "(5)"}) == "(5) + 'k' + `other`"


def test_rename_leaves_a_backticked_alias_and_field_alone():
    # `expr AS `k`` names a column, `t.`k`` is a field: neither is the parameter, a bare `k` still is
    assert sqlbody.rename("x AS `k`", {"k": "(1)"}) == "x AS `k`"
    assert sqlbody.rename("t.`k` + `k`", {"k": "(1)"}) == "t.`k` + (1)"
    assert sqlbody.rename("x as   `k`, `k`", {"k": "(1)"}) == "x as   `k`, (1)"


def test_a_body_that_spells_a_parameter_in_another_case_is_refused_at_load():
    # Spark reads K as k, the inliner does not: the body is refused with the parameter and the spelling found
    def edit(raw):
        _kring_sql(raw)
        _row(raw, "gbx_bng_kring")["levels"]["L4"]["sql"] += " + K"

    with pytest.raises(
        ValueError, match=r"gbx_bng_kring L4\.sql: parameter 'k' is spelled 'K'"
    ):
        perf_manifest(edit)

    def helper_edit(raw):
        _kring_sql(raw)
        _row(raw, "gbx_bng_kring")["levels"]["L4"]["sql_helpers"] = [
            {**HELPER, "sql": "2 * K + 1"}
        ]

    with pytest.raises(
        ValueError, match=r"gbx__ring_width: parameter 'k' is spelled 'K'"
    ):
        perf_manifest(helper_edit)
    # a type after AS and a field are not the parameter, whatever their case
    assert (
        sqlbody.case_mismatches("CAST(x AS STRING) + t.K + K(1)", ["string", "k"]) == []
    )
    assert sqlbody.case_mismatches("`K` + 'K' + k", ["k"]) == [("k", "K")]
    assert perf_manifest(_kring_sql)  # the spelling as declared loads


def test_a_parameter_with_no_session_type_is_refused_not_an_index_error():
    bad = manifest.Param("p", (), "INT")
    with pytest.raises(ValueError, match="parameter p has no session type"):
        manifest._check_shared_l0_signature("gbx_x", (bad,), "INT", "INT")


def test_helper_calls_nested_in_an_argument_inline_like_they_render():
    h = [
        {"name": "gbx__h", "params": [["k", "INT"]], "returns": "INT", "sql": "k * 2"},
        {"name": "gbx__g", "params": [["a", "INT"]], "returns": "INT", "sql": "a + 1"},
    ]
    body = "{q}gbx__g({q}gbx__h(3))"
    assert sqlbody.inlined(body, h) == "((((3) * 2)) + 1)"
    assert (
        sqlbody.render(body, "`c`.`s`.") == "`c`.`s`.gbx__g(`c`.`s`.gbx__h(3))"
    )  # the UC call keeps the nesting


def test_the_inlining_guards_name_a_cycle_an_unbalanced_call_and_a_leftover_qualifier():
    loop = [
        {
            "name": "gbx__h",
            "params": [["k", "INT"]],
            "returns": "INT",
            "sql": "{q}gbx__h(k)",
        }
    ]
    with pytest.raises(ValueError, match="nest too deeply"):
        sqlbody.inlined("{q}gbx__h(1)", loop)
    with pytest.raises(ValueError, match="unbalanced"):
        sqlbody.inlined("{q}gbx__h(1", loop)
    with pytest.raises(ValueError, match="not a declared helper call"):
        sqlbody.inlined("{q}x", loop)


def test_the_session_and_uc_share_one_l0_body():
    def edit(raw):
        r = _row(raw, "gbx_quadbin_resolution")
        r["levels"]["L0"] = {"sql": "CAST((cell >> 52) & 31 AS INT)"}
        r["levels"]["L4"] = {"sql": "L0"}

    man = perf_manifest(edit)
    fn = man.functions["gbx_quadbin_resolution"]
    assert sqlbody.l4_source(fn) == sqlbody.l0_source(fn)
    (text,) = ddl.function_ddl(fn, "cat", "sch", WHEEL, man.uc_dependencies, "0.6.0")
    assert text.endswith("RETURN CAST((cell >> 52) & 31 AS INT)")
    assert "L4" in fn.levels and "L0" in fn.levels


def test_the_sql_keys_have_rules_a_load_enforces():
    def fails(edit, match):
        with pytest.raises(ValueError, match=match):
            perf_manifest(edit)

    def l4_from_l0_without_l0(raw):
        _row(raw, "gbx_bng_distance")["levels"]["L4"] = {"sql": "L0"}

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

    def aggregate_with_body(raw):
        _row(raw, "gbx_h3_cellfill")["levels"]["L4"] = {"sql": "1"}

    def generator_with_body(raw):
        _row(raw, "gbx_bng_kloopexplode")["levels"]["L4"] = {"sql": "1"}

    def one_l0_body_two_signatures(raw):
        # the session takes BINARY (or STRING) and UC takes GEOMETRY: one body cannot serve both
        levels = _row(raw, "gbx_st_crs")["levels"]
        levels["L0"], levels["L4"] = {"sql": "'x'"}, {"sql": "L0"}

    fails(aggregate_with_body, "scalar SQL function.*aggregate")
    fails(generator_with_body, "scalar SQL function.*generator")
    fails(one_l0_body_two_signatures, "types differ.*geom: BINARY in the session")
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
    assert not any(
        "runs in a notebook or job session" in s for s in plain
    )  # no stub text
    assert not any("gbx_st_crs(" in s.split("\n")[0] for s in plain)
    with_stubs = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, stubs=True)
    assert with_stubs[: len(plain)] == plain  # the same statements, then the stubs
    extra = with_stubs[len(plain) :]
    # the aggregate gets none; the shipped stub rows come along
    (stub,) = [s for s in extra if ".gbx_st_crs(" in s]
    assert len(extra) == 5 and not any("gbx_h3_cellfill" in s for s in extra)
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


def test_the_shipped_stub_rows_are_the_product_mappings_and_the_session_only_scalars():
    man = manifest.load()
    # an aggregate or generator also carries route text, but install(stubs=True) makes no
    # function for it; only the session-only scalars become raising functions
    stubs = {
        n: f.stub for n, f in man.functions.items() if f.stub and f.kind == "scalar"
    }
    assert set(stubs) == {
        "gbx_st_crs",
        "gbx_st_setcrs",
        "gbx_st_transformcrs",
        "gbx_rst_fromfile",
        "gbx_rst_derivedband",
    }
    assert "st_srid" in stubs["gbx_st_crs"]
    assert "st_setsrid" in stubs["gbx_st_setcrs"]
    assert "st_transform(" in stubs["gbx_st_transformcrs"]
    assert "gbx_rst_fromcontent" in stubs["gbx_rst_fromfile"]
    assert "gbx_rst_mapalgebra" in stubs["gbx_rst_derivedband"]
    assert "gbx_probe_rst_materialize" not in stubs  # a probe name gets none
    # opt-in: a plain install creates none, stubs=True adds one statement for each
    plain = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True)
    with_stubs = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, stubs=True)
    assert not any("runs in a notebook or job session" in s for s in plain)
    added = with_stubs[len(plain) :]
    assert with_stubs[: len(plain)] == plain and len(added) == 5
    for n, fix in stubs.items():
        (stmt,) = [s for s in added if f".{n}(" in s.split("\n")[0]]
        assert (
            f"runs in a notebook or job session. Fix: {fix.replace(chr(39), chr(39) * 2)}"
            in stmt
        )


def test_only_may_be_a_generator_and_an_aggregate_stub_hint_names_its_session_route(
    monkeypatch,
):
    _stub_manifest(monkeypatch)
    names = (n for n in ["st_crs", "bng_kring"])  # used up by one pass
    stmts = uc.install("c", "s", "/Volumes/c/s/v", dry_run=True, stubs=True, only=names)
    assert (
        len(stmts) == 2 and "gbx_bng_kring(" in stmts[0] and "raise_error" in stmts[1]
    )
    # an aggregate gets no stub: the message points at its session route, not at stubs=True
    with pytest.raises(ValueError) as exc:
        uc.install(
            "c", "s", "/Volumes/c/s/v", dry_run=True, stubs=True, only=["h3_cellfill"]
        )
    text = str(exc.value)
    assert "no stub" in text and "use gbx_h3_cellfill_array." in text
    assert "install(stubs=True) creates" not in text


def test_verify_checks_the_helpers_and_the_stubs_that_install_created(monkeypatch):
    def edit(raw):
        _kring_sql(raw)
        _row(raw, "gbx_st_crs")["levels"]["L4"] = {"stub": "use st_srid(geom)."}

    man = perf_manifest(edit)
    monkeypatch.setattr(manifest, "load", lambda: man)
    monkeypatch.setattr(uc, "_require_env6", lambda: None)
    monkeypatch.setattr(uc, "_single_wheel", lambda d: "/Volumes/c/s/v/w.whl")
    verified, ran = [], []
    monkeypatch.setattr(
        uc, "verify", lambda spark, c, s, names: verified.append(list(names))
    )
    spark = type("S", (), {"sql": lambda self, text: ran.append(text)})()
    uc.install(
        "c",
        "s",
        "/Volumes/c/s/v",
        spark=spark,
        only=["bng_kring", "st_crs"],
        stubs=True,
    )
    assert len(ran) == 3  # the helper, the function, the stub
    assert verified == [["gbx_bng_kring", "gbx__ring_width", "gbx_st_crs"]]


def _env_of(stmt):
    m = re.search(r"dependencies = '(\[.*?\])'", stmt)
    return None if m is None else json.loads(m.group(1).replace("\\'", "'"))


def test_shared_deps_is_the_default_and_per_function_keeps_each_list():
    deps = manifest.load().uc_dependencies
    args = ("c", "s", "/Volumes/c/s/v")
    default = uc.install(*args, dry_run=True, procedures=True)
    shared = uc.install(*args, dry_run=True, procedures=True, deps="shared")
    own = uc.install(*args, dry_run=True, procedures=True, deps="per-function")
    assert default == shared
    assert len(own) == len(shared)

    envs_own = {tuple(e[1:]) for e in map(_env_of, own) if e}
    envs_shared = {tuple(e[1:]) for e in map(_env_of, shared) if e}
    assert len(envs_own) > 1  # per-function: each distinct list is its own environment
    assert envs_shared == {tuple(f"{n}=={v}" for n, v in deps.items())}
    # only the environment text differs; names, order and every other statement are the same
    for a, b in zip(own, shared):
        assert re.sub(r"dependencies = '.*?'", "", a) == re.sub(
            r"dependencies = '.*?'", "", b
        )
    with pytest.raises(ValueError, match="per-function"):
        uc.install(*args, dry_run=True, deps="all")


def test_the_default_install_is_unchanged_for_every_function_without_a_sql_kernel():
    """A dry-run install with the default flags emits, for each routine it emitted before the SQL-kernel work, the same
    statement (pinned by hash); only a row that gained an L4 `sql` key may differ. The hashes of the Python routines
    were refreshed when `deps="shared"` became the default: the only change is the dependency list.
    """
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
                else "CAST((cell >> 52) & 31 AS INT)"
            )
        }
        if helpers:
            r["levels"]["L0"]["sql_helpers"] = [
                {
                    "name": "gbx__res_bits",
                    "params": [["c", "BIGINT"]],
                    "returns": "INT",
                    "sql": "CAST((c >> 52) & 31 AS INT)",
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
        "DROP TEMPORARY FUNCTION IF EXISTS gbx_quadbin_resolution",
        "CREATE OR REPLACE TEMPORARY FUNCTION gbx_quadbin_resolution(cell BIGINT) "
        "RETURNS INT RETURN (CAST(((cell) >> 52) & 31 AS INT))",
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


def test_an_unforced_l0_failure_falls_back_to_l3_and_records_the_reason(monkeypatch):
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.manager import l3, levels

    fn = _l0_manifest().functions["gbx_quadbin_resolution"]
    registered = []
    monkeypatch.setattr(
        l3,
        "register_l3",
        lambda spark, f, variant, name=None: registered.append(variant),
    )
    monkeypatch.setattr(l3, "l3_variant", lambda f: ("c0", None))
    lines = []
    got = manager._install(
        _RecordingSpark(fail=True),
        fn,
        levels.Choice("L0", "-", ()),
        _caps(),
        forced=False,
        lines=lines,
    )
    assert (got.level, got.variant) == ("L3", "c0") and registered == ["c0"]
    assert any(
        "unavailable: RuntimeError" in text and "using" in text for _, text in lines
    )
    assert any("unavailable: RuntimeError" in r for r in got.reasons)


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


def test_twin_without_names_copies_every_session_row_and_skips_python_only_rows():
    from databricks.labs.gbx.bench.oracle_tables._uc_tile import Adapt, twin

    out = twin(_agg_rows(), "gbx_rst_merge", Adapt())  # agg_py has no session surface
    assert [c.cid for c in out] == ["agg_a__rst_merge", "agg_b__rst_merge"]


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
            rows, "gbx_bng_cellunion_agg", Adapt(), "agg_a"
        )  # a session function, no L4


# ---- the collision gate covers the new names -------------------------------------------------------------------


def test_the_collision_gate_covers_suffixed_names_sql_helpers_and_stubs():
    import dataclasses
    import importlib.util
    import sys
    from types import SimpleNamespace

    path = Path(__file__).resolve().parents[4] / "scripts/manifest/check_surfaces.py"
    spec = importlib.util.spec_from_file_location("gbx_check_surfaces", path)
    gates = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gates
    spec.loader.exec_module(gates)
    man = manifest.load()
    assert gates.form_findings(man) == []  # the manifest as it stands
    base = man.functions["gbx_bng_kring"]
    agg = man.functions["gbx_h3_cellfill"]

    def rename(fn, name, **kw):
        return dataclasses.replace(fn, sql_name=name, **kw)

    def findings(*fns):
        return gates.form_findings(
            SimpleNamespace(functions={f.sql_name: f for f in fns})
        )

    # a suffixed name is a scalar; a finalize needs its partial
    assert any(
        "must be a scalar" in f for f in findings(rename(agg, "gbx_h3_cellfill_array"))
    )
    fin = rename(base, "gbx_rst_binpoints_finalize")
    assert any("gbx_rst_binpoints_partial" in f for f in findings(fin))
    assert findings(fin, rename(base, "gbx_rst_binpoints_partial")) == []
    # a SQL helper named like a public function, or two different helpers of one name
    h = {"name": "gbx_bng_cellarea", "params": (), "returns": "INT", "sql": "1"}
    clash = rename(
        base, "gbx_x", levels={**base.levels, "L4": {"sql": "1", "sql_helpers": (h,)}}
    )
    assert any(
        "is a function name" in f
        for f in findings(clash, man.functions["gbx_bng_cellarea"])
    )
    h1 = {"name": "gbx__w", "params": (), "returns": "INT", "sql": "1"}
    one = rename(
        base,
        "gbx_a",
        levels={"L3": {}, "L4": {"sql": "{q}gbx__w()", "sql_helpers": (h1,)}},
    )
    two = rename(
        base,
        "gbx_b",
        levels={
            "L3": {},
            "L4": {"sql": "{q}gbx__w()", "sql_helpers": ({**h1, "sql": "2"},)},
        },
    )
    assert any("gbx__w" in f and "differently" in f for f in findings(one, two))
    # a stub's Fix text is the sentence after "Fix:", not a second "Fix:"
    stub = rename(man.functions["gbx_st_crs"], "gbx_st_crs", stub="Fix: use st_srid.")
    assert any("Fix:" in f for f in findings(stub))
    # `gbx__<name>` is reserved only by a row that creates that wrapper (a Python or Scala function with a DEFAULT)
    withdefault = dataclasses.replace(base.params[1], default="1")
    wrapper = rename(base, "gbx_w", params=(base.params[0], withdefault))
    kernel = rename(
        base,
        "gbx_w",
        params=(base.params[0], withdefault),
        levels={**base.levels, "L4": {"sql": "1"}},
    )
    h = {"name": "gbx__w", "params": (), "returns": "INT", "sql": "1"}
    holder = rename(
        base, "gbx_h", levels={**base.levels, "L4": {"sql": "1", "sql_helpers": (h,)}}
    )
    assert any("is a function name" in f for f in findings(wrapper, holder))
    assert findings(kernel, holder) == []  # a SQL kernel creates no gbx__k wrapper
    # a SQL kernel that keeps a handler can be forced to python, which creates the wrapper: the name is reserved
    forced = rename(
        base,
        "gbx_w",
        params=(base.params[0], withdefault),
        levels={**base.levels, "L4": {"sql": "1", "handler": "m:f"}},
    )
    assert any("is a function name" in f for f in findings(forced, holder))
