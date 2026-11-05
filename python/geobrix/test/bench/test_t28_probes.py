"""The probes the batched workspace pass runs (bench.probes, bench.t28): the slice tables, the JAR argument, the
register / oracle / P1 runners on a recording session, the mechanism and engine probes, and the file writer.
Nothing here touches Databricks.
"""

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles, probes

# The MVP oracle tables run from their own probe (run_uc_s6), not from the UC slice.
OWN_FILES = {"MVP_S", "MVP_H", "MVP_G", "MVP_A", "MVP_IO"}


def _uc_tables():
    man = manifest.load()
    return {
        t
        for t, cases in oracles.TABLES.items()
        if not t.startswith("_")
        and t not in OWN_FILES
        and any(
            "uc" in c.surfaces
            and (c.sql is not None or "L4" in man.functions[c.fn].levels)
            for c in cases
        )
    }


def test_the_slice_holds_every_oracle_table_with_uc_rows():
    """A table with UC rows that the slice leaves out is never run on the workspace: the test names it."""
    assert set(probes.UC_SLICE_TABLES) == _uc_tables()
    assert len(set(probes.UC_SLICE_TABLES)) == len(probes.UC_SLICE_TABLES)
    assert set(probes.UC_CORE_TABLES) | set(probes.UC_TIER_B_TABLES) == set(
        probes.UC_SLICE_TABLES
    )


def test_the_slice_tables_are_the_tables_the_handler_cross_tier_test_runs():
    """Both name the tables whose UC rows run: a table added to one and not the other is a gap the pass would carry."""
    import re
    from pathlib import Path

    src = (
        Path(__file__).resolve().parents[1] / "manager" / "test_family_cross_tier.py"
    ).read_text()
    block = re.search(r"_UC_TABLES = \((.*?)\)\n", src, re.S).group(1)
    assert set(re.findall(r'"(\w+)"', block)) == set(probes.UC_SLICE_TABLES)


def test_the_slice_cases_include_the_recipe_rows_of_functions_with_no_uc_form_of_their_own():
    man = manifest.load()
    cases = probes.uc_slice_cases()
    recipes = [c for c in cases if c.sql is not None]
    assert recipes and any("L4" not in man.functions[c.fn].levels for c in recipes)
    assert len({c.cid for c in cases}) == len(cases)  # one statement id per case


class _Spark:
    def __init__(self):
        self.sql_calls = []

    def sql(self, text):
        self.sql_calls.append(text)
        return self


def test_run_uc_slice_installs_from_the_named_jar_and_routes_recipes_to_the_scalar_runner(
    monkeypatch,
):
    from databricks.labs.gbx import uc

    seen = {}

    def install(catalog, schema, volume_dir, **kw):
        seen["install"] = (catalog, schema, volume_dir, kw)
        return [
            "CREATE FUNCTION a LANGUAGE SCALA x",
            "CREATE FUNCTION b LANGUAGE PYTHON y",
        ]

    monkeypatch.setattr(uc, "install", install)
    monkeypatch.setattr("databricks.labs.gbx.register", lambda *a, **k: {})
    monkeypatch.setattr(
        oracles,
        "run_session",
        lambda spark, cases, **kw: seen.setdefault("scalar", list(cases)) and [],
    )
    monkeypatch.setattr(
        oracles,
        "run_rows",
        lambda spark, cases, **kw: seen.setdefault("rows", list(cases)) and [],
    )
    out = probes.run_uc_slice(
        _Spark(), "c", "s", "/Volumes/c/s/v/f", "/tmp/x", jar="/Volumes/c/s/v/j.jar"
    )
    assert seen["install"][3]["jar"] == "/Volumes/c/s/v/j.jar"
    assert out["scala"] == 1 and out["statements"] == 2 and out["failures"] == []
    man = manifest.load()
    assert all(
        c.sql is None for c in seen["rows"]
    )  # a recipe is a statement, never a table-function shape
    assert all(man.functions[c.fn].kind == "generator" for c in seen["rows"])
    assert any(c.sql is not None for c in seen["scalar"])
    probes.run_uc_slice(_Spark(), "c", "s", "/Volumes/c/s/v/f", "/tmp/x")
    assert seen["install"][3]["jar"] is None  # no jar: the folder's own, as before


# ---- run_register_all / run_family_oracles / run_p1 --------------------------------------------------------------


def test_run_register_all_reports_wall_lines_levels_and_the_docs_limit(
    spark, monkeypatch
):
    out = probes.run_register_all(spark, only=["gbx_bng_cellarea", "gbx_bng_aswkb"])
    assert out["names"] == 2 and out["skipped"] == []
    assert set(out["variants"]) == {"gbx_bng_cellarea", "gbx_bng_aswkb"}
    assert sorted(n for names in out["by_level"].values() for n in names) == sorted(
        out["variants"]
    )
    assert out["wall_s"] >= 0 and out["over_docs_limit"] is False
    assert isinstance(out["lines"], list)
    monkeypatch.setattr(probes, "REGISTER_DOCS_LIMIT_S", -1.0)
    assert (
        probes.run_register_all(spark, only=["gbx_bng_cellarea"])["over_docs_limit"]
        is True
    )


def test_run_register_all_names_what_no_level_served(spark, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.register",
        lambda spark_, target="session", **kw: {},
    )
    out = probes.run_register_all(spark, only=["gbx_bng_cellarea"])
    assert out["names"] == 0 and out["skipped"] == ["gbx_bng_cellarea"]


def test_the_fromfile_cap_check_records_a_refusal_and_an_acceptance(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx.bench import uc_perf

    monkeypatch.setattr(
        uc_perf,
        "perf_stage_tiles",
        lambda d, mibs: {
            f"perf_{mibs[0]}MiB.tif": {"path": "/Volumes/c/s/v/x.tif", "bytes": 5}
        },
    )

    class Refusing:
        def sql(self, text):
            raise RuntimeError(
                "the raster decodes to 128 MiB, over the 64 MiB read cap. Fix: ..."
            )

    class Accepting:
        def sql(self, text):
            return self

        def collect(self):
            return []

    refused = probes._fromfile_cap(Refusing(), str(tmp_path), 128)
    assert refused["refused"] is True and refused["names_cap"] is True
    assert probes._fromfile_cap(Accepting(), str(tmp_path), 128)["refused"] is False


def test_run_family_oracles_runs_a_table_at_the_manager_level_and_reports_by_table(
    spark, tmp_path
):
    out = probes.run_family_oracles(
        spark,
        str(tmp_path),
        tables=["GRID_SCALAR"],
        only=["gbx_bng_cellarea", "gbx_bng_aswkb"],
    )
    rec = out["tables"]["GRID_SCALAR"]
    assert rec["functions"] == 2 and rec["cases"] > 2 and rec["skipped"] == {}
    assert out["failures"] == 0 and out["red"] == {}
    assert set(out["levels"]) == {"gbx_bng_cellarea", "gbx_bng_aswkb"}
    assert out["level"] == "manager"


def test_run_family_oracles_reports_a_red_row_with_its_message(
    spark, tmp_path, monkeypatch
):
    real = oracles.run_session

    def broken(spark_, cases, **kw):
        return [
            (c, f"{c.cid}: forced red" if i == 0 else None) for i, c in enumerate(cases)
        ]

    monkeypatch.setattr(oracles, "run_session", broken)
    out = probes.run_family_oracles(
        spark, str(tmp_path), tables=["GRID_SCALAR"], only=["gbx_bng_cellarea"]
    )
    assert out["failures"] == 1 and "forced red" in next(iter(out["red"].values()))
    assert out["tables"]["GRID_SCALAR"]["failures"] == 1
    assert real is not broken


def test_run_family_oracles_counts_a_table_that_raises_as_a_failure(
    spark, tmp_path, monkeypatch
):
    def boom(*a, **k):
        raise RuntimeError("table fell over")

    monkeypatch.setattr(probes, "_oracle_function", boom)
    out = probes.run_family_oracles(
        spark, str(tmp_path), tables=["GRID_SCALAR"], only=["gbx_bng_cellarea"]
    )
    assert list(out["crashed"]) == ["GRID_SCALAR"]
    assert "table fell over" in out["crashed"]["GRID_SCALAR"]
    assert out["failures"] == 1 and out["red"] == {}
    assert "table fell over" in out["tables"]["GRID_SCALAR"]["crashed"]


def test_run_family_oracles_with_a_level_runs_only_functions_that_have_it(
    spark, tmp_path
):
    man = manifest.load()
    names = [n for n in ("gbx_bng_cellarea", "gbx_bng_eastnorthasbng", "gbx_bng_aswkb")]
    out = probes.run_family_oracles(
        spark, str(tmp_path), level="L2", tables=["GRID_SCALAR"], only=names
    )
    rec = out["tables"]["GRID_SCALAR"]
    without = {n for n in names if "L2" not in man.functions[n].levels}
    assert without and {n for n in rec["skipped"] if n in names} >= without
    assert all(rec["skipped"][n] == "no L2 form" for n in without)
    assert out["level"] == "L2"
    assert all(v.startswith("L2/") for v in out["levels"].values())


class _Lib:
    def __init__(self, jar, status):
        self.library, self.status, self.messages = (
            type("L", (), {"jar": jar})(),
            type("S", (), {"value": status})(),
            [],
        )


class _FakeCluster:
    def __init__(self, statuses):
        self.statuses, self.calls = list(statuses), []
        self.libraries, self.clusters = self, self

    def install(self, cluster_id, libs):
        self.calls.append(("install", cluster_id, libs))

    def cluster_status(self, cluster_id):
        st = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return [_Lib("/Volumes/c/s/v/j.jar", st)]

    def restart(self, cluster_id):
        self.calls.append(("restart", cluster_id))


def _caps(usable):
    return lambda spark: {
        "jar_usable": usable,
        "jar": {"version": "0.6.0"} if usable else None,
    }


def test_run_p1_install_phase_waits_for_the_library_and_restarts_when_the_jar_is_not_usable(
    monkeypatch, capsys
):
    monkeypatch.setattr(probes, "_p1_library", lambda jar: ("lib", jar))
    seq = iter([False, False])
    monkeypatch.setattr(probes, "_p1_caps", lambda spark: _caps(next(seq))(spark))
    cluster = _FakeCluster(["PENDING", "INSTALLED"])
    out = probes.run_p1(None, "c-1", "/Volumes/c/s/v/j.jar", client=cluster, poll_s=0)
    assert out["library"]["status"] == "INSTALLED" and out["usable_at_once"] is False
    assert out["restart_requested"] is True and cluster.calls[-1] == ("restart", "c-1")
    assert cluster.calls[0][2] == [("lib", "/Volumes/c/s/v/j.jar")]
    assert (
        "[T28 P1] RESULT" in capsys.readouterr().out
    )  # printed before the restart ends the run


def test_run_p1_does_not_restart_when_the_jar_is_usable_at_once_or_when_asked_not_to(
    monkeypatch,
):
    monkeypatch.setattr(probes, "_p1_library", lambda jar: jar)
    seq = iter([False, True, False, False])
    monkeypatch.setattr(probes, "_p1_caps", lambda spark: _caps(next(seq))(spark))
    cluster = _FakeCluster(["INSTALLED"])
    out = probes.run_p1(None, "c", "/Volumes/c/s/v/j.jar", client=cluster, poll_s=0)
    assert out["usable_at_once"] is True and out["restart_requested"] is False
    cluster = _FakeCluster(["INSTALLED"])
    out = probes.run_p1(
        None, "c", "/Volumes/c/s/v/j.jar", client=cluster, poll_s=0, restart=False
    )
    assert out["restart_requested"] is False and ("restart", "c") not in cluster.calls


def test_run_p1_after_phase_reports_the_library_and_the_capabilities(monkeypatch):
    monkeypatch.setattr(probes, "_p1_caps", _caps(True))
    out = probes.run_p1(
        None,
        "c",
        "/Volumes/c/s/v/j.jar",
        phase="after",
        client=_FakeCluster(["INSTALLED"]),
    )
    assert (
        out["library"]["status"] == "INSTALLED" and out["usable_after_restart"] is True
    )
    import pytest

    with pytest.raises(ValueError, match="phase must be"):
        probes.run_p1(None, "c", "j", phase="restart", client=_FakeCluster(["x"]))


def test_a_step_that_returns_counts_prints_them_on_its_done_line(capsys):
    from databricks.labs.gbx.bench.uc_perf import _Run

    run_ = _Run("T28 oracles", "t28-test", 1, 60.0)
    try:
        run_.step("GRID_SCALAR", lambda: {"functions": 2, "cases": 9, "failures": 0})
    finally:
        run_.stop()
    out = capsys.readouterr().out
    assert "[T28 oracles] GRID_SCALAR START" in out
    assert "DONE" in out and "ok failures=0 cases=9 functions=2" in out


# ---- the mechanism checks and the engine probe ------------------------------------------------------------------

import re  # noqa: E402

import pytest  # noqa: E402

WHEEL_URI = "file:///Volumes/c/s/v/f/geobrix-0.6.0+env6-py3-none-any.whl"
TILE_DIR = "/Volumes/c/s/v/f/mech_tiles"


def _units():
    return probes.t28_mech_units(
        "cat", "sch", TILE_DIR, WHEEL_URI, manifest.load().uc_dependencies
    )


def test_mechanism_units_have_unique_ids_and_statement_ids():
    units = _units()
    ids = [u["id"] for u in units]
    assert ids == [f"f{i}" for i in range(1, 11)]
    cids = [c for u in units for c, _s, _e in u["statements"]] + [
        c for u in units for c, _s in u["drops"]
    ]
    assert len(cids) == len(set(cids))
    # START/DONE labels of the runner are the unit ids: one pair each
    assert len({u["id"] for u in units}) == len(units)


def _outside_literals(sql: str) -> str:
    """The statement without its $$ bodies and quoted strings (a ';' inside either is data, not a separator)."""
    return re.sub(
        r"'(?:[^']|'')*'", "''", re.sub(r"\$\$.*?\$\$", "$$$$", sql, flags=re.S)
    )


def test_every_mechanism_statement_is_one_statement_and_every_routine_a_probe():
    for u in _units():
        for cid, sql, _e in u["statements"]:
            assert "\n\n" not in sql, cid
            assert not any(ln.lstrip().startswith("--") for ln in sql.splitlines()), cid
            if cid != "f6:create":  # a procedure body holds statements of its own
                assert ";" not in _outside_literals(sql), cid
        for cid, sql in u["drops"]:
            assert sql.startswith(
                (
                    "DROP FUNCTION IF EXISTS",
                    "DROP TEMPORARY FUNCTION IF EXISTS",
                    "DROP PROCEDURE IF EXISTS",
                )
            ), cid
        for cid, sql, _e in u["statements"]:
            m = re.match(
                r"CREATE OR REPLACE (?:TEMPORARY )?(?:FUNCTION|PROCEDURE) (\S+?)[( ]",
                sql,
            )
            if m:
                assert m.group(1).split(".")[-1].startswith("gbx_mvp_t28_"), cid
        for _cid, sql in u["drops"]:
            assert re.search(r"gbx_mvp_t28_", sql)


def test_product_functions_named_by_the_mechanism_checks_exist_and_are_qualified():
    man = manifest.load()
    for u in _units():
        for cid, sql, _e in u["statements"]:
            code = re.sub(r"\$\$.*?\$\$", "", _outside_literals(sql), flags=re.S)
            for m in re.finditer(r"(?<![\w.])(`cat`\.`sch`\.)?(gbx_[a-z0-9_]+)", code):
                qual, name = m.groups()
                if name.startswith("gbx_mvp_t28_"):
                    continue
                assert name in man.functions and "L4" in man.functions[name].levels, (
                    cid,
                    name,
                )
                assert qual, (cid, name)  # 3-level where it is a schema function


def test_the_surfaces_are_where_the_checklist_runs_them():
    on = {u["id"]: u["surfaces"] for u in _units()}
    assert (
        on["f1"] == on["f2"] == ("s6",)
    )  # a session temporary function has no warehouse form
    assert all(on[f"f{i}"] == ("s6", "w") for i in range(3, 11))
    w = probes.mech_statements(
        "cat", "sch", TILE_DIR, WHEEL_URI, manifest.load().uc_dependencies
    )
    assert not any(c.startswith(("f1:", "f2:")) for c, _ in w)
    kinds = [sql.startswith("DROP") for _c, sql in w]
    assert any(kinds) and kinds == sorted(
        kinds
    )  # every DROP comes after every statement


def test_mech_expectation_kinds():
    ok = probes.mech_expectation
    assert ok(("eq", "42"), {"value": 42}) is None
    assert "want '42'" in ok(("eq", "42"), {"value": "41"})
    assert ok(("error", "stub"), {"error": "x stub y"}) is None
    assert "expected an error" in ok(("error", "stub"), {"value": 1})
    assert "without" in ok(("error", "stub"), {"error": "other"})
    assert ok(("contains", "|True"), {"value": "a.B|True"}) is None
    assert ok(("any",), {"value": None}) is None
    assert "error" in ok(("any",), {"error": "boom"})
    assert ok(("any",), None) == "no result"


def test_check_mech_results_names_the_failed_statement_of_a_unit():
    args = ("cat", "sch", TILE_DIR, WHEEL_URI, manifest.load().uc_dependencies)
    pairs = probes.mech_statements(*args)
    good = {c: {"value": "x"} for c, _ in pairs}
    good["f3:call"] = {"error": "gbx_mvp_t28 stub: runs in a session"}
    good["f4:call"] = {"value": "2|4.0"}
    good["f5:call"] = {"value": "m.Geom|True~m.Geom|True"}
    done = probes.check_mech_results(good, *args)
    assert all(r["ok"] for r in done.values()), done
    bad = dict(good, **{"f4:call": {"error": "UNSUPPORTED_SQL_UDF_USAGE"}})
    done = probes.check_mech_results(bad, *args)
    assert done["f4"]["ok"] is False and "f4:call" in done["f4"]["failures"]
    assert done["f3"]["ok"] is True  # one unit's failure does not spread


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class _MechSpark:
    """Answers each mechanism statement as a good workspace would, and records them."""

    def __init__(self, fail=()):
        self.seen, self.fail = [], fail

    def sql(self, text):
        self.seen.append(text)
        if any(f in text for f in self.fail):
            raise RuntimeError("UNSUPPORTED_SQL_UDF_USAGE " + text[:40])
        if "current_catalog()" in text:
            return _Rows([("cat0", "sch0")])
        if "gbx_mvp_t28_tvf_stub(1)" in text:
            raise RuntimeError("gbx_mvp_t28 stub: runs in a session")
        if "tmp_add(41)" in text:
            return _Rows([("42",)])
        if "shadow(1)" in text:
            return _Rows([("schema" if "`sch`." in text else "temp",)])
        if "proj(3)" in text:
            return _Rows([("2|4.0",)])
        if "geom_array" in text and text.startswith("SELECT"):
            return _Rows([("m.Geom|True~m.Geom|True",)])
        return _Rows([("x",)])


def test_run_t28_mechanisms_runs_every_serverless_unit_and_drops_every_probe(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx import uc

    wheel = tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl"
    wheel.write_bytes(b"w")
    monkeypatch.setattr(uc, "_single_wheel", lambda d: str(wheel))
    monkeypatch.setattr(probes, "stage_mech_tiles", lambda d, mibs=(): {"ok": 1})
    spark = _MechSpark()
    out = probes.run_t28_mechanisms(spark, "cat", "sch", str(tmp_path))
    assert out["ok"] is True and set(out["units"]) == {f"f{i}" for i in range(1, 11)}
    assert out["units"]["f3"]["statements"]["f3:call"]["error"].startswith(
        "gbx_mvp_t28 stub"
    )
    drops = [t for t in spark.seen if t.startswith("DROP")]
    creates = {
        m.group(1)
        for t in spark.seen
        if (
            m := re.match(
                r"CREATE OR REPLACE (?:TEMPORARY )?(?:FUNCTION|PROCEDURE) (\S+?)[( ]", t
            )
        )
    }
    dropped = {m.group(1) for t in drops if (m := re.search(r"IF EXISTS (\S+)", t))}
    assert creates == dropped  # nothing created is left behind
    assert spark.seen[-1].startswith("DROP") or any(
        t.startswith("USE `cat0`") for t in spark.seen
    )


def test_run_t28_mechanisms_records_a_refused_shape_and_still_drops(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx import uc

    wheel = tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl"
    wheel.write_bytes(b"w")
    monkeypatch.setattr(uc, "_single_wheel", lambda d: str(wheel))
    monkeypatch.setattr(probes, "stage_mech_tiles", lambda d, mibs=(): {})
    spark = _MechSpark(fail=("proj(n INT)",))
    out = probes.run_t28_mechanisms(spark, "cat", "sch", str(tmp_path))
    assert (
        out["units"]["f4"]["ok"] is False
        and "f4:create_tvf" in out["units"]["f4"]["failures"]
    )
    assert out["units"]["f1"]["ok"] is True and out["ok"] is False
    assert any(
        "DROP FUNCTION IF EXISTS" in t and "gbx_mvp_t28_partial" in t
        for t in spark.seen
    )


def test_the_m6_probe_is_a_gbx_mvp_function_that_runs_the_slope_engine_and_is_dropped():
    stmts = dict(
        probes.m6_statements(
            "cat",
            "sch",
            "/Volumes/c/s/v/f/m6_tiles",
            WHEEL_URI,
            manifest.load().uc_dependencies,
        )
    )
    assert list(stmts) == ["m6:create", "m6:call", "m6:drop"]
    assert (
        "gbx_mvp_se_l4_probe" in stmts["m6:create"] and "RETURNS " in stmts["m6:create"]
    )
    assert (
        "functions['gbx_rst_slope']" in stmts["m6:create"]
        and "engine_run.run(" in stmts["m6:create"]
    )
    assert (
        "'unit': 'degrees'" in stmts["m6:create"]
        and "environment_version = '6'" in stmts["m6:create"]
    )
    assert "p2_1MiB.bin" in stmts["m6:call"] and "binaryFile" in stmts["m6:call"]
    assert stmts["m6:drop"] == "DROP FUNCTION IF EXISTS `cat`.`sch`.gbx_mvp_se_l4_probe"
    assert probes.m6_ok("123456|ok|valid:min:max=1000:0.0:12.5|pyspark_imported=False")
    assert not probes.m6_ok("123|ok|valid:min:max=1:0:1|pyspark_imported=True")
    # a sandbox that preloads pyspark: judged by what the handler added
    assert probes.m6_ok(
        "1|ok|valid:min:max=1:0:1|pyspark_imported=True|pyspark_before=40|pyspark_new=0"
    )
    assert not probes.m6_ok(
        "1|ok|valid:min:max=1:0:1|pyspark_imported=True|pyspark_before=0|pyspark_new=40"
    )
    assert not probes.m6_ok("123|error|a GDAL failure")


def _run_m6_handler(tmp_path, block_pyspark: bool) -> str:
    """The handler body in a fresh interpreter (the test process has pyspark loaded, which would hide the answer the probe
    exists to give). ``block_pyspark`` makes ``import pyspark`` raise, as it does in a UC sandbox (a meta-path finder: a ``sys.modules['pyspark'] = None``
    stub would leave the key behind and fail the handler's own ``'pyspark' in sys.modules`` check). The result line, or the
    end of the error."""
    import subprocess
    import sys

    from databricks.labs.gbx.engine import run as engine_run

    why = engine_run.unavailable_reason()
    if why:
        pytest.skip(why)
    tile = tmp_path / "ref.tif"
    tile.write_bytes(probes.p2_tile_bytes("1MiB", 1 << 20))
    block = (
        "class _Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in ('pyspark', 'py4j'):\n"
        "            raise ImportError('blocked: ' + name)\n"
        "sys.meta_path.insert(0, _Block())\n"
        if block_pyspark
        else ""
    )
    script = (
        "import sys\n"
        + block
        + "def handler(t):\n"
        + "".join("    " + ln + "\n" for ln in probes._M6_BODY.splitlines())
        + f"out = handler({{'raster': open({str(tile)!r}, 'rb').read(), 'metadata': {{'src': 'm6'}}}})\n"
        + "print('RESULT|' + out['metadata']['m6'])\n"
    )
    res = subprocess.run(
        [sys.executable, "-I", "-c", script],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    return next(
        (ln for ln in res.stdout.splitlines() if ln.startswith("RESULT|")),
        res.stderr[-600:],
    )


def test_the_m6_handler_gives_the_engine_a_slope_tile_when_pyspark_is_importable(
    tmp_path,
):
    line = _run_m6_handler(tmp_path, block_pyspark=False)
    assert line.startswith("RESULT|ok|valid:min:max="), line


def test_the_m6_handler_needs_no_pyspark(tmp_path):
    line = _run_m6_handler(tmp_path, block_pyspark=True)
    assert probes.m6_ok("1|" + line[len("RESULT|") :]), line


# ---- the warehouse combine reference ---------------------------------------------------------------------------


def test_no_warehouse_combine_reference_combines_more_tiles_than_one_uc_call_takes():
    from collections import Counter

    from databricks.labs.gbx.uc.procedures import TREE_FAN_IN

    sizes = Counter(k for k, _ in probes._proc_combine_inputs())
    wide = probes._COMBINE_WIDE_KEY
    assert sizes[wide] > TREE_FAN_IN  # the key that cannot have a one-call reference
    assert max(n for k, n in sizes.items() if k != wide) <= TREE_FAN_IN
    stmts = dict(probes.proc_warehouse_statements("c", "s", "/Volumes/c/s/v/scratch"))
    for op, _d in probes._COMBINE_PROBE:
        diff = stmts[f"gbx_combine_by_key:{op}:diff"]
        assert diff.count(f"tile_key <> '{wide}'") == 4  # both sides of the two EXCEPTs
        stats = stmts[f"gbx_combine_by_key:{op}:{wide}_stats"]
        assert f"tile_key = '{wide}'" in stats
        assert f"`c`.`s`.gbxproc_cmb_{op}" in stats


def test_the_wide_key_statistics_are_the_same_cell_on_both_surfaces():
    warehouse = probes._wide_stats_expr("`c`.`s`.", "tile")
    session = probes._wide_stats_expr("", "c")
    assert warehouse.replace("`c`.`s`.", "").replace("tile", "c") == session


def test_compare_combine_many_holds_the_warehouse_to_the_session_result():
    ops = [op for op, _d in probes._COMBINE_PROBE]
    wh = {
        f"gbx_combine_by_key:{op}:many_stats": {"value": f"4|4|[1.0]|[0.0]|[2.0]{op}"}
        for op in ops
    }
    s6 = {
        "recipes": {
            f"gbx_combine_by_key:{op}": {"wide_stats": f"4|4|[1.0]|[0.0]|[2.0]{op}"}
            for op in ops
        }
    }
    assert probes.compare_combine_many(wh, s6) == []
    wh["gbx_combine_by_key:sum:many_stats"] = {"value": "4|4|[9.0]|[0.0]|[2.0]sum"}
    del s6["recipes"]["gbx_combine_by_key:min"]
    del wh["gbx_combine_by_key:max:many_stats"]
    bad = probes.compare_combine_many(wh, s6)
    assert len(bad) == 3
    assert any(b.startswith("sum: warehouse") for b in bad)
    assert any(b.startswith("min: missing (serverless") for b in bad)
    assert any(b.startswith("max: missing (warehouse") for b in bad)


def test_the_serverless_run_records_the_session_statistics_of_the_wide_key():
    seen = []

    class S:
        def sql(self, text):
            seen.append(text)
            return _Rows([("4|4|[1.0]|[0.0]|[2.0]",)])

    out = probes._wide_session_stats(S(), "c", "s", "gbx_combine_by_key:avg")
    assert out == {"wide_stats": "4|4|[1.0]|[0.0]|[2.0]"}
    assert (
        "gbx_rst_combineavg(collect_list(tile))" in seen[0]
        and "`c`.`s`.gbxproc_cmb_src" in seen[0]
    )

    class Bad:
        def sql(self, text):
            raise RuntimeError("boom")

    assert (
        "boom"
        in probes._wide_session_stats(Bad(), "c", "s", "gbx_combine_by_key:avg")[
            "wide_error"
        ]
    )


def test_run_stage_inputs_stages_three_folders_under_the_volume_dir(monkeypatch):
    from databricks.labs.gbx.bench import uc_perf

    seen = {}
    monkeypatch.setattr(
        uc_perf,
        "perf_stage_tiles",
        lambda d, mibs: seen.setdefault("perf", (d, tuple(mibs))) and {"a": 1},
    )
    monkeypatch.setattr(
        probes,
        "stage_mech_tiles",
        lambda d, mibs=(): seen.setdefault("mech", d) and {"b": 2},
    )
    monkeypatch.setattr(
        probes,
        "p2_stage_tiles",
        lambda d, sizes: seen.setdefault("m6", (d, tuple(sizes))) and {"c": 3},
    )
    out = probes.run_stage_inputs(None, "/Volumes/c/s/v/f/", m10=(16, 64, 256))
    assert out == {"perf_tiles": {"a": 1}, "mech_tiles": {"b": 2}, "m6_tiles": {"c": 3}}
    assert seen["perf"] == ("/Volumes/c/s/v/f/perf_tiles", (1, 64, 16, 64, 256))
    assert seen["mech"] == "/Volumes/c/s/v/f/mech_tiles"
    assert seen["m6"] == (
        "/Volumes/c/s/v/f/m6_tiles",
        (probes.M6_TILE,),
    )  # the 1 MiB tile the M-6 call reads


def test_a_failed_staging_step_is_recorded_not_raised(monkeypatch):
    from databricks.labs.gbx.bench import uc_perf

    def boom(*a, **k):
        raise OSError("read-only file system")

    monkeypatch.setattr(uc_perf, "perf_stage_tiles", boom)
    monkeypatch.setattr(probes, "stage_mech_tiles", lambda d, mibs=(): {})
    monkeypatch.setattr(probes, "p2_stage_tiles", lambda d, sizes: {})
    out = probes.run_stage_inputs(None, "/Volumes/c/s/v/f")
    assert "read-only" in out["perf_tiles"]["error"] and out["mech_tiles"] == {}


def test_a_map_parameter_is_a_map_literal_not_a_tile_struct():
    man = manifest.load()
    param = man.functions["gbx_rst_index"].params[2]
    case = next(c for c in oracles.TABLES["SPEC_CALC"] if c.fn == "gbx_rst_index")
    assert (
        oracles._sql_literal({"red": 1, "nir": 2}, param, case)
        == "map('red', 1, 'nir', 2)"
    )
    assert oracles._sql_literal({}, param, case) == "CAST(map() AS MAP<STRING,INT>)"


def test_the_mixed_language_and_temp_share_cells_are_what_the_checklist_asks():
    mixed = probes.mixed_language_cells()
    assert (
        mixed[0].startswith("import databricks.labs.gbx as gbx")
        and "gbx.register(spark" in mixed[0]
    )
    scala = mixed[1]
    assert scala.startswith("%scala")
    for label in (
        "scalar_sql",
        "scalar_expr",
        "scalar_call_function",
        "generator_select_list",
        "generator_lateral",
        "aggregate_group_by",
    ):
        assert f'probe("{label}")' in scala
    assert str(probes.H3_CELL) in scala
    import h3

    assert (
        h3.get_resolution(h3.int_to_str(probes.H3_CELL)) == 8
    )  # a real cell, so a failure is the routine's
    share = probes.temp_share_cells()
    assert share[1].startswith("%scala") and "gbx_mvp_t28_share_py" in share[1]
    assert (
        "DROP TEMPORARY FUNCTION IF EXISTS" in share[2]
        and "gbx_mvp_t28_share_scala" in share[2]
    )


class _M6Spark:
    """Records each statement; the call answers ``answer`` (or raises it when it is an exception)."""

    def __init__(self, answer, fail_create=False):
        self.seen, self.answer, self.fail_create = [], answer, fail_create

    def sql(self, text):
        self.seen.append(text)
        if text.startswith("CREATE OR REPLACE FUNCTION") and self.fail_create:
            raise RuntimeError("create refused")
        if text.startswith("SELECT"):
            if isinstance(self.answer, Exception):
                raise self.answer
            return _Rows([(self.answer,)])
        return _Rows([])


def _m6(monkeypatch, tmp_path, spark):
    from databricks.labs.gbx import uc

    wheel = tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl"
    wheel.write_bytes(b"w")
    monkeypatch.setattr(uc, "_single_wheel", lambda d: str(wheel))
    monkeypatch.setattr(probes, "p2_stage_tiles", lambda d, sizes: {})
    return probes.run_m6(spark, "cat", "sch", str(tmp_path))


def test_run_m6_creates_calls_and_drops_the_probe_and_passes_on_a_slope_tile(
    monkeypatch, tmp_path
):
    spark = _M6Spark("1048576|ok|valid:min:max=0:1|pyspark_imported=False")
    out = _m6(monkeypatch, tmp_path, spark)
    kinds = [t.split(" ", 1)[0] for t in spark.seen]
    assert kinds == [
        "CREATE",
        "CREATE",
        "SELECT",
        "DROP",
    ]  # the schema, the probe, the call, the drop
    assert f"`cat`.`sch`.{probes.M6_FN}" in spark.seen[1]
    assert out["pass"] is True and out["errors"] == {} and out["dropped"] is True


def test_run_m6_records_a_failed_call_and_still_drops_the_probe(monkeypatch, tmp_path):
    spark = _M6Spark(RuntimeError("UNSUPPORTED_SQL_UDF_USAGE"))
    out = _m6(monkeypatch, tmp_path, spark)
    assert "UNSUPPORTED_SQL_UDF_USAGE" in out["errors"]["call"]
    assert out["pass"] is False and out["dropped"] is True
    assert spark.seen[-1].startswith("DROP FUNCTION IF EXISTS")


def test_run_m6_skips_the_call_when_the_create_is_refused_and_still_drops(
    monkeypatch, tmp_path
):
    spark = _M6Spark("unused", fail_create=True)
    out = _m6(monkeypatch, tmp_path, spark)
    assert "create refused" in out["errors"]["create"] and "call" not in out
    assert not [t for t in spark.seen if t.startswith("SELECT")]
    assert out["pass"] is False and spark.seen[-1].startswith("DROP")


def test_stage_mech_tiles_writes_the_files_with_their_sizes_through_the_guarded_writer(
    tmp_path,
):
    import os

    out = probes.stage_mech_tiles(str(tmp_path), (1,))
    mech = tmp_path / "mech_1MiB.tif"
    assert out["mech_1MiB.tif"] == {"path": str(mech), "bytes": os.path.getsize(mech)}
    assert (
        0.9 * (1 << 20) < mech.stat().st_size < 1.1 * (1 << 20)
    )  # about 1 MiB ON DISK
    perf = out["perf_1MiB.tif"]  # the decoded-size tile comes from the perf writer
    assert os.path.getsize(perf["path"]) == perf["bytes"]
    assert not list(tmp_path.glob("*.part"))  # the temp name was renamed away


def test_p1_caps_forgets_the_cached_probe_then_flattens_the_capabilities(monkeypatch):
    from databricks.labs.gbx.manager import capabilities as cap

    seen = []
    jar = cap.JarIdentity("0.6.0", "abc")
    found = cap.Capabilities(True, False, jar, "", "", True)
    monkeypatch.setattr(cap, "forget", lambda s: seen.append(("forget", s)))
    monkeypatch.setattr(
        cap, "detect", lambda s, need_jvm: seen.append(("detect", need_jvm)) or found
    )
    out = probes._p1_caps("sess")
    assert seen == [("forget", "sess"), ("detect", True)]  # re-probed, JVM required
    assert out["jar"] == {"version": "0.6.0", "sha256": "abc"}
    assert out["jar_usable"] is True and out["connect"] is True
    monkeypatch.setattr(
        cap,
        "detect",
        lambda s, need_jvm: cap.Capabilities(True, True, None, "r", "r", False),
    )
    none = probes._p1_caps("sess")
    assert none["jar"] is None and none["jar_usable"] is False


def test_oracle_capped_lowers_the_cap_per_row_runs_it_and_restores_it(monkeypatch):
    import databricks.labs.gbx as gbx
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY

    events = []

    class Conf:
        calls = 0

        def set(self, key, value):
            Conf.calls += 1
            if Conf.calls == 1:  # the first row's session refuses the cap
                raise RuntimeError("CONFIG_NOT_AVAILABLE")
            events.append(("set", key, value))

        def unset(self, key):
            events.append(("unset", key))

    class Sess:
        conf = Conf()

    ok = type("C", (), {"cid": "decoded_over_cap_error_tile"})()
    refused = type("C", (), {"cid": "dem_decoded_over_cap"})()
    fn = type("F", (), {"sql_name": "gbx_rst_x"})()
    monkeypatch.setattr(
        gbx, "register", lambda s, only, **pin: events.append(("register", only, pin))
    )
    monkeypatch.setattr(
        probes,
        "_oracle_run_cases",
        lambda s, f, cases, tmp: events.append(("run", [c.cid for c in cases]))
        or [(cases[0], None)],
    )
    rec = {"skipped": {}}
    ran = probes._oracle_capped(Sess(), fn, [refused, ok], {"_levels": 1}, "/t", rec)
    cap = str(oracles.READ_CAP_ROWS["decoded_over_cap_error_tile"])
    assert ran == [(ok, None)]  # the refused row never ran
    assert rec["skipped"] == {
        "dem_decoded_over_cap": "cannot lower the read cap: CONFIG_NOT_AVAILABLE"
    }
    assert events == [  # lowered, registered, run, then unset and registered again
        ("set", READ_CAP_KEY, cap),
        ("register", ["gbx_rst_x"], {"_levels": 1}),
        ("run", ["decoded_over_cap_error_tile"]),
        ("unset", READ_CAP_KEY),
        ("register", ["gbx_rst_x"], {"_levels": 1}),
    ]
