"""The files and commands of the batched workspace pass (bench.t28): every run of the checklist has what it needs, every
file passes its static checks, every printed command names a script and flags that exist. No Databricks.
"""

import json
import re
import shlex
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles, probes, t28, uc_perf

REPO = Path(__file__).resolve().parents[4]
CHECKLIST_RUNS = {
    "S6-A", "S6-B", "S6-C", "S6-D", "S6-E", "S6-F", "S6-G", "S6-H",
    "W-A", "W-B", "W-C", "W-D", "W-E", "D-A", "D-B", "STD-A", "S5-A", "X-1",
}  # fmt: skip
CLUSTER = "0000-000000-abcdefgh"


@pytest.fixture(scope="module")
def plan():
    p = t28.Params(cluster_id=CLUSTER)
    files, runs = t28.build(p)
    return p, files, runs


def test_the_plan_has_every_run_of_the_checklist_and_the_two_it_adds(plan):
    _p, _files, runs = plan
    ids = [r.id for r in runs]
    assert len(ids) == len(set(ids))
    assert set(ids) == CHECKLIST_RUNS | {"S6-0", "END"}


def test_every_file_is_used_by_a_run_and_every_run_file_exists(plan):
    _p, files, runs = plan
    names = [f.name for f in files]
    assert len(names) == len(set(names))
    used = {n for r in runs for n in r.files}
    assert used == set(names)


def test_every_file_passes_its_static_checks(plan):
    p, files, _runs = plan
    assert t28.validate(files, p) == {}


def test_every_run_but_the_held_one_says_how_to_start_it(plan):
    _p, _files, runs = plan
    for r in runs:
        assert r.items and r.wall, r.id
        if r.id != "X-1":
            assert r.commands and r.needs, r.id
    held = {r.id: r for r in runs}["X-1"]
    assert not held.commands and "1016-152458-i5qbli6q" in held.needs[0]


def _flags(line: str):
    return [w for w in shlex.split(line) if w.startswith("--")]


def test_every_command_names_a_script_that_exists_with_flags_it_knows(plan):
    p, files, runs = plan
    written = {p.path(f.name) for f in files}
    for r in runs:
        for line in r.commands:
            if line.startswith(("#", "databricks ")):
                continue
            words = shlex.split(line.split("  #")[0])
            assert words[0] == "bash", line
            script = REPO / words[1]
            assert script.is_file(), line
            text = script.read_text()
            for flag in _flags(line):
                assert flag in text, (r.id, flag, words[1])
            for flag in ("--notebook", "--file"):
                if flag in words:
                    path = words[words.index(flag) + 1]
                    assert path in written or (REPO / path).is_file(), (r.id, path)


def test_the_warehouse_commands_pass_no_schema_so_the_first_create_schema_can_run(plan):
    _p, _files, runs = plan
    for r in runs:
        for line in r.commands:
            if "gbx-uc-sql.sh" in line:
                assert "--schema" not in line, line


def test_the_series_notebooks_and_their_dependencies_exist():
    for series, nb, dep in t28.SERIES:
        assert (REPO / "examples/notebooks" / series / nb).is_file(), (series, nb)
        if dep:
            assert (REPO / "examples/notebooks" / series / dep).is_file(), (series, dep)


def test_the_install_file_creates_the_schema_first_and_names_the_jar_for_the_scala_forms(
    plan,
):
    _p, files, _runs = plan
    pairs = oracles.parse_uc_sql({f.name: f for f in files}["w_install.sql"].content)
    assert [c for c, _ in pairs[:2]] == ["schema:create", "floor:select1"]
    stmts = [s for c, s in pairs if c.startswith("install:")]
    assert len(stmts) >= 250
    assert sum("LANGUAGE SCALA" in s for s in stmts) == 5  # the JVM-only BNG functions
    assert all(t28.Params().jar in s for s in stmts if "LANGUAGE SCALA" in s)
    assert sum(s.startswith("CREATE OR REPLACE PROCEDURE") for s in stmts) == 11


def test_the_slice_files_hold_every_slice_case_once(plan):
    _p, files, _runs = plan
    by = {f.name: oracles.parse_uc_sql(f.content) for f in files if f.kind == "sql"}
    for name, tables in (
        ("w_slice.sql", probes.UC_CORE_TABLES),
        ("w_tierb.sql", probes.UC_TIER_B_TABLES),
    ):
        cids = [c for c, _ in by[name] if not c.startswith(("schema:", "floor:"))]
        want = [c.cid for c in probes.uc_slice_cases(tables)]
        assert sorted(cids) == sorted(want) and len(set(cids)) == len(cids)
    # a MAP parameter (rst_index) is a map(...) literal, not a tile struct
    index = [s for c, s in by["w_tierb.sql"] if c == "index_ndvi"][0]
    assert "map('red', 1, 'nir', 2)" in index


def test_the_warehouse_performance_files_are_one_measurement_each_with_the_floor(plan):
    _p, files, _runs = plan
    names = {f.name for f in files}
    assert {f"w_{u}.sql" for u in t28.W_PERF_UNITS} <= names
    assert (
        "w_m9.sql" not in names and "w_m11.sql" not in names
    )  # M9 has its own pair, M11 is dropped
    for u in t28.W_PERF_UNITS:
        pairs = oracles.parse_uc_sql({f.name: f for f in files}[f"w_{u}.sql"].content)
        assert [c for c, _ in pairs[:2]] == ["schema:create", "floor:select1"]
        assert all(c.startswith(f"{u}:") for c, _ in pairs[2:]), u


def test_the_idle_file_follows_the_calls_file_and_owns_the_drops(plan):
    _p, files, _runs = plan
    by = {
        f.name: [c for c, _ in oracles.parse_uc_sql(f.content)]
        for f in files
        if f.kind == "sql"
    }
    assert "m9:call_1" in by["w_m9_calls.sql"] and "m9:call_3" in by["w_m9_calls.sql"]
    assert not [
        c
        for c in by["w_m9_calls.sql"]
        if "drop_gbx" in c or c.startswith("m9:drop_gbx")
    ]
    assert "m9:call_after_idle" in by["w_m9_idle.sql"]
    assert [
        c for c in by["w_m9_idle.sql"] if "warm_src" in c
    ]  # the source table is dropped last


def test_the_cleanup_drops_the_four_scratch_schemas_and_nothing_else(plan):
    p, files, _runs = plan
    pairs = oracles.parse_uc_sql({f.name: f for f in files}["w_cleanup.sql"].content)
    drops = [s for _c, s in pairs if s.startswith("DROP")]
    assert drops == [
        f"DROP SCHEMA IF EXISTS `{p.catalog}`.`{p.schema_of(k)}` CASCADE"
        for k in t28.SCHEMA_KEYS
    ]
    assert all("t28" in d for d in drops)  # never a schema that is not a scratch schema


def test_the_p1_notebooks_need_a_cluster_id():
    files, runs = t28.build(t28.Params())
    assert not {f.name for f in files} & {"p1_install.ipynb", "p1_after.ipynb"}
    d_b = {r.id: r for r in runs}["D-B"]
    assert "--cluster-id" in d_b.needs[0] and not d_b.commands
    files, _ = t28.build(t28.Params(cluster_id=CLUSTER))
    nb = json.loads({f.name: f for f in files}["p1_install.ipynb"].content)
    assert (
        CLUSTER in nb["cells"][-1]["source"]
        and "'phase': 'install'" in nb["cells"][-1]["source"]
    )


def test_only_selects_runs_and_their_files_and_refuses_an_unknown_run(plan):
    _p, files, runs = plan
    f2, r2 = t28.select(files, runs, ["s6-c", "W-E"])
    assert [r.id for r in r2] == ["S6-C", "W-E"]
    assert {f.name for f in f2} == {"uc_slice_s6.ipynb", "w_engine_m6.sql"}
    with pytest.raises(ValueError, match="names no run"):
        t28.select(files, runs, ["S6-Z"])


# Three rows embed a netCDF or VRT that GDAL stamps with the time it was written: their bytes differ per build.
STAMPED = {
    "getsubdataset_bytes",
    "uc_getsubdataset_missing",
    "uc_fromcontent_vrt_refused",
}


def test_the_plan_is_deterministic_but_for_the_rows_that_embed_a_timestamp(plan):
    p, files, _runs = plan
    again, _ = t28.build(p)
    assert [f.name for f in again] == [f.name for f in files]
    for f, g in zip(files, again):
        if f.content == g.content:
            continue
        assert f.kind == "sql"
        a, b = dict(oracles.parse_uc_sql(f.content)), dict(
            oracles.parse_uc_sql(g.content)
        )
        assert a.keys() == b.keys()
        assert {c for c in a if a[c] != b[c]} <= STAMPED, f.name


# ---- the checks themselves: each one fails on the defect it names -----------------------------------------------


Q = "`c`.`s`."


def test_validate_sql_names_each_kind_of_defect():
    ok = oracles.uc_sql_text([("a", f"SELECT {Q}gbx_rst_width(NULL)")])
    assert t28.validate_sql(ok, "c", "s") == []
    cases = {
        "SELECT gbx_rst_width(NULL)": "is not qualified",
        f"SELECT {Q}gbx_rst_nonsense(NULL)": "no function of the manifest",
        f"SELECT {Q}gbx_mvp_t28_x(1)": "called before the file creates it",
        "SELECT `other`.`s`.gbx_rst_width(NULL)": "is not qualified",
    }
    for sql, why in cases.items():
        bad = t28.validate_sql(oracles.uc_sql_text([("a", sql)]), "c", "s")
        assert any(why in b for b in bad), (sql, bad)
    created = oracles.uc_sql_text([
        ("a", f"CREATE OR REPLACE FUNCTION {Q}gbx_mvp_t28_x(x INT) RETURNS INT RETURN x"),
        ("b", f"SELECT {Q}gbx_mvp_t28_x(1)"),
    ])  # fmt: skip
    assert t28.validate_sql(created, "c", "s") == []
    dup = oracles.uc_sql_text([("a", "SELECT 1"), ("a", "SELECT 2")])
    assert any("duplicate cids" in b for b in t28.validate_sql(dup, "c", "s"))
    body = oracles.uc_sql_text(
        [
            (
                "a",
                f"CREATE OR REPLACE FUNCTION {Q}gbx_mvp_t28_y(x INT) RETURNS INT LANGUAGE PYTHON AS $$\nreturn (\n$$",
            )
        ]
    )
    assert any("does not compile" in b for b in t28.validate_sql(body, "c", "s"))
    # a gbx_ name inside a string or a Python body is data, not a reference
    data = oracles.uc_sql_text([("a", "SELECT 'gbx_nonsense' AS x")])
    assert t28.validate_sql(data, "c", "s") == []


def test_validate_notebook_names_each_kind_of_defect():
    good = json.dumps(
        probes.notebook_json(
            [
                "x = 1",
                "import json\nfrom databricks.labs.gbx.bench import probes\nprint(json.dumps(probes.run_detect(spark, **{}), default=str, indent=1))",
            ]
        )
    )
    assert t28.validate_notebook(good) == []
    assert "not JSON" in t28.validate_notebook("{")[0]
    assert any(
        "cell 0" in b
        for b in t28.validate_notebook(json.dumps(probes.notebook_json(["def ("])))
    )
    bad_call = "from databricks.labs.gbx.bench import probes\nprint(json.dumps(probes.run_nonsense(spark, **{}), default=str, indent=1))"
    assert any(
        "does not exist" in b
        for b in t28.validate_notebook(json.dumps(probes.notebook_json([bad_call])))
    )
    bad_kw = bad_call.replace(
        "run_nonsense(spark, **{})", "run_detect(spark, **{'cluster': 1})"
    )
    assert any(
        "does not take" in b
        for b in t28.validate_notebook(json.dumps(probes.notebook_json([bad_kw])))
    )
    scala = "%scala\nprintln(1"
    assert any(
        "unbalanced" in b
        for b in t28.validate_notebook(json.dumps(probes.notebook_json([scala])))
    )


# ---- the perf plan's skips: a function that drops out of a block is named ------------------------------------------

EXPECTED_SKIPS = {
    "m2": {"bng_cellarea_batch"},  # the handler has no _batch twin
    "m3": {"polyfill_batch"},  # gbx_bng_polyfill has no _batch handler
    "m11": {"partial"},  # the fused zonal leg is excluded with the engine
}


def test_the_perf_plan_skips_exactly_the_expected_legs():
    wheel = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"
    for surface in ("w", "s6"):
        plan = uc_perf.perf_plan(
            "c", "s", jar_path="/Volumes/c/s/v/j.jar", wheel_path=wheel, surface=surface
        )
        got = {u: set(legs) for u, legs in uc_perf.skipped(plan).items()}
        assert got == EXPECTED_SKIPS, (
            f"{surface}: a block lost a function or a leg changed: "
            f"unexpected {sorted((u, k) for u, ks in got.items() for k in ks - EXPECTED_SKIPS.get(u, set()))}, "
            f"missing {sorted((u, k) for u, ks in EXPECTED_SKIPS.items() for k in ks - got.get(u, set()))}"
        )
    # without a JAR the Scala legs are skipped too, and nothing else is
    plan = uc_perf.perf_plan("c", "s", jar_path=None, wheel_path=wheel)
    extra = {k for ks in uc_perf.skipped(plan).values() for k in ks} - {
        k for ks in EXPECTED_SKIPS.values() for k in ks
    }
    assert extra == {"bng_eastnorthasbng_scala", "bng_cellarea_scala"}


def test_the_perf_plan_names_a_function_that_drops_out_of_a_block(monkeypatch):
    real = uc_perf._has_uc
    monkeypatch.setattr(
        uc_perf,
        "_has_uc",
        lambda c, *names: [n for n in real(c, *names)]
        + (["gbx_h3_cellfill_array"] if "gbx_h3_cellfill_array" in names else []),
    )
    plan = uc_perf.perf_plan(
        "c",
        "s",
        jar_path="/Volumes/c/s/v/j.jar",
        wheel_path="/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl",
    )
    assert uc_perf.skipped(plan).get(
        "m6"
    ), "a function missing from M6 must show in the skipped ledger"


# ---- the local parse -----------------------------------------------------------------------------------------------


def test_every_select_of_the_slice_files_parses_with_sparks_own_parser(spark, plan):
    """After the two Databricks-only forms are rewritten (a GEOMETRY(n) type, a qualified table function)."""
    _p, files, _runs = plan
    bad = t28.parse_locally(
        [f for f in files if f.name in ("w_slice.sql", "w_tierb.sql", "w_cleanup.sql")]
    )
    assert bad == {}, list(bad.items())[:5]


def test_spark_view_rewrites_only_the_two_databricks_forms():
    sql = "SELECT CAST(g AS GEOMETRY(ANY)), CAST(h AS GEOMETRY(4326)) FROM `c`.`s`.gbx_bng_kringexplode('x', 1) AS t, LATERAL `c`.`s`.gbx_x(t.a) u"
    want = "SELECT CAST(g AS BINARY), CAST(h AS BINARY) FROM gbx_bng_kringexplode('x', 1) AS t, LATERAL gbx_x(t.a) u"
    assert t28.spark_view(sql) == want
    scalar = "SELECT `c`.`s`.gbx_rst_width(t) FROM tbl"
    assert t28.spark_view(scalar) == scalar  # a scalar call keeps its qualifier


def test_what_sparks_parser_rejects_is_only_databricks_only_create_statements(
    spark, plan
):
    """Every SELECT, DROP FUNCTION, CALL and USE of every file parses locally. What does not is CREATE FUNCTION / PROCEDURE with
    ENVIRONMENT, PARAMETER STYLE or LANGUAGE PYTHON, and DROP PROCEDURE: the workspace is the parser for those.
    """
    _p, files, _runs = plan
    rejected = t28.parse_locally(files)
    by_cid = {
        (f.name, c): s
        for f in files
        if f.kind == "sql"
        for c, s in oracles.parse_uc_sql(f.content)
    }
    assert rejected
    for key in rejected:
        name, cid = key.split(":", 1)
        sql = by_cid[(name, cid)]
        assert re.match(
            r"CREATE OR REPLACE (?:TEMPORARY )?(?:FUNCTION|PROCEDURE) |DROP PROCEDURE ",
            sql,
        ), key
        assert re.search(
            r"ENVIRONMENT|PARAMETER STYLE|LANGUAGE (?:PYTHON|SCALA)|PROCEDURE", sql
        ), key


# ---- judging a results file ----------------------------------------------------------------------------------------


def test_judge_reads_the_engine_and_mechanism_results(tmp_path, plan):
    p, _files, _runs = plan
    good = {
        "m6:call": {"value": "1000|ok|valid:min:max=100:0.0:9.5|pyspark_imported=False"}
    }
    path = tmp_path / "w_engine_m6.json"
    oracles.dump_uc_results(good, str(path))
    bad, summary = t28.judge(str(path), p)
    assert bad == [] and "pass" in summary
    oracles.dump_uc_results({"m6:call": {"error": "boom"}}, str(path))
    bad, _ = t28.judge(str(path), p)
    assert bad and "boom" in bad[0]
    empty = tmp_path / "w_slice.json"
    oracles.dump_uc_results({}, str(empty))
    bad, summary = t28.judge(str(empty), p)
    assert (
        len(bad) == len(probes.uc_slice_cases(probes.UC_CORE_TABLES))
        and "0 of" in summary
    )  # a missing result fails
    other = tmp_path / "w_m2.json"
    oracles.dump_uc_results({"m2:x": {"value": 1}}, str(other))
    assert t28.judge(str(other), p)[0] == []  # a timing file has no judge: read it


def test_judge_grades_a_slice_row_the_way_the_oracle_does(tmp_path, plan):
    p, _files, _runs = plan
    case = next(
        c
        for c in probes.uc_slice_cases(probes.UC_CORE_TABLES)
        if c.fn == "gbx_bng_cellarea" and c.kind != "null"
    )
    stmts = dict(oracles.uc_sql([case], p.catalog, p.schema_of("w"), str(tmp_path)))
    assert case.cid in stmts
    path = tmp_path / "w_slice.json"
    oracles.dump_uc_results({case.cid: {"value": "definitely not the area"}}, str(path))
    bad, _ = t28.judge(str(path), p)
    assert any(case.cid in b for b in bad)


def test_the_manifest_lists_every_file_with_its_runs(tmp_path, plan):
    p, files, runs = plan
    t28.write(str(tmp_path), files, runs, p)
    meta = json.loads((tmp_path / "manifest.json").read_text())
    assert set(meta["files"]) == {f.name for f in files} and len(meta["runs"]) == len(
        runs
    )
    text = (tmp_path / "manifest.md").read_text()
    for r in runs:
        assert f"## {r.id} -" in text
    assert all((tmp_path / f.name).is_file() for f in files)
    assert manifest.load()  # the plan used this wheel's manifest


def test_ws_root_is_the_current_users_geobrix_folder(monkeypatch):
    import sys
    import types

    class _Client:
        def __init__(self, profile=None):
            self.current_user = types.SimpleNamespace(
                me=lambda: types.SimpleNamespace(user_name="someone@example.com")
            )

    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = _Client
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    assert t28.resolve_ws_root("p") == "/Users/someone@example.com/GeoBrix"


def test_ws_root_is_required_when_there_is_no_session(monkeypatch, capsys):
    def _no_session(profile):
        raise RuntimeError("cannot resolve; pass --ws-root")

    monkeypatch.setattr(t28, "resolve_ws_root", _no_session)
    assert t28.main(["--dry-run"]) == 2
    assert "--ws-root" in capsys.readouterr().err


def test_every_notebook_puts_its_oracle_fixtures_on_the_volume(plan):
    """The workers read the virtual-tile fixtures the driver writes: on a Volume they see them, in the driver's /tmp not."""
    p, files, _runs = plan
    seen = 0
    for f in files:
        if not f.name.endswith(".ipynb"):
            continue
        text = f.content if isinstance(f.content, str) else json.dumps(f.content)
        for m in re.finditer(r"'tmpdir': '([^']+)'", text):
            assert m.group(1).startswith(p.volume_dir), (f.name, m.group(1))
            seen += 1
    assert seen >= 5


def test_the_uc_judge_reads_a_scalar_that_returns_chips_as_cells(tmp_path):
    """A scalar tessellate row has kind "cells": the judge read it with the one-value check and raised ValueError."""
    man = manifest.load()
    case = next(
        c
        for rows in oracles.TABLES.values()
        for c in rows
        if c.kind == "cells"
        and "uc" in c.surfaces
        and man.functions[c.fn].kind == "scalar"
    )
    out = oracles.check_uc_results([case], {case.cid: {"value": []}}, str(tmp_path))
    assert len(out) == 1 and case.cid in out[0]


def test_nested_cells_of_a_statement_result_get_their_declared_types():
    """The API gives nested BINARY as base64 and nested BIGINT / BOOLEAN / DOUBLE as text; the judge needs real values."""
    got = oracles.coerce_uc_value(
        [{"cellid": "TQ3080", "core": "true", "chip": "AQID"}],
        "array<struct<cellid:string,core:boolean,chip:binary>>",
    )
    assert got == [{"cellid": "TQ3080", "core": True, "chip": b"\x01\x02\x03"}]
    assert oracles.coerce_uc_value(["1", "2"], "array<bigint>") == [1, 2]
    assert oracles.coerce_uc_value({"xmin": "1.5"}, "struct<xmin:double>") == {
        "xmin": 1.5
    }
    tile = oracles.coerce_uc_value(
        {"cellid": "7", "raster": "AQ==", "metadata": {"a": "b"}}, "TILE"
    )
    assert (
        tile["cellid"] == 7
        and tile["raster"] == b"\x01"
        and tile["metadata"] == {"a": "b"}
    )
