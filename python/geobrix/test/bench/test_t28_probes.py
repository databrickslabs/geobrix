"""The probes the batched workspace pass runs (bench.probes, bench.t28): the slice tables, the JAR argument, the
register / oracle / P1 runners on a recording session, the mechanism and engine probes, and the file writer.
Nothing here touches Databricks.
"""

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles, probes

# The oracle tables that run from their own files, not from the UC slice.
OWN_FILES = {"MVP_S", "MVP_H", "MVP_G", "MVP_A", "MVP_IO", "SPEC_SE"}


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
