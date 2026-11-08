"""Tests for gbx:test:mutation-databricks and its pieces: the command's shell, the launcher's argument handling and
plan, and the pure row, status and summary logic (no network, no mutmut run)."""

import importlib.util
import json
import subprocess
import sys

import pytest
from conftest import REPO_ROOT

CMD = REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-databricks.sh"
MUT = REPO_ROOT / "scripts" / "mutation"
sys.path.insert(0, str(MUT))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


rows_mod = _load("guidance_rows", MUT / "guidance_rows.py")
sys.path.insert(0, str(REPO_ROOT / "examples" / "tests"))
launcher = _load("push_and_run_mutation_on_cluster", REPO_ROOT / "examples" / "tests" / "push_and_run_mutation_on_cluster.py")
gate = _load("mutation_gate_for_guidance", MUT / "gate.py")


def _sh(*args):
    return subprocess.run(["bash", str(CMD), *args], capture_output=True, text=True, cwd=REPO_ROOT)


# ---- the command's shell


def test_help_exits_zero_and_names_every_option():
    res = _sh("--help")
    assert res.returncode == 0
    for opt in ("--family", "--modules", "--tests", "--purpose", "--profile", "--node-type", "--run-id", "--explain-only", "--keep-stage", "--log"):
        assert opt in res.stdout


def test_an_unknown_option_is_refused():
    res = _sh("--bogus")
    assert res.returncode == 2
    assert "Unknown option" in res.stderr


def test_a_launch_without_a_purpose_is_refused():
    res = _sh("--family", "f", "--modules", "pyvx/_validity", "--tests", "test/pyvx")
    assert res.returncode == 2
    assert "--purpose" in res.stderr
    res = _sh("--family", "f", "--modules", "pyvx/_validity", "--tests", "test/pyvx", "--purpose", "  ")
    assert res.returncode == 2


def test_the_command_has_its_registration_and_is_executable():
    assert (REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-databricks.md").exists()
    assert CMD.stat().st_mode & 0o111


def test_explain_only_prints_the_plan_and_stages_nothing():
    res = _sh(
        "--family", "pyvx", "--modules", "pyvx/_validity,pyvx/_cleaning", "--tests", "test/pyvx",
        "--node-type", "m5d.8xlarge", "--run-id", "unit-1", "--purpose", "unit test", "--explain-only",
    )
    assert res.returncode == 0, res.stderr
    plan = json.loads(res.stdout[res.stdout.index("{"): res.stdout.rindex("}") + 1])
    assert plan["modules"] == ["pyvx/_validity", "pyvx/_cleaning"]
    assert plan["cores"] == 32 and plan["workers"] == 30
    assert plan["stage"].endswith("/mutation-runs/unit-1")
    assert plan["table"].endswith(".mutation_results")
    assert plan["spark_version"] == "19.x-scala2.13"
    assert plan["compute"] == {"mode": "pool", "pool": "gbx-test-1n"}
    assert plan["pip_extra"] == "mercantile>=1.2,<2"
    assert "nothing staged or started" in res.stdout


# ---- the launcher


def test_the_launcher_refuses_a_bad_run_id_and_missing_arguments():
    a = launcher.parse(["--family", "f", "--modules", "m", "--tests", "t", "--purpose", "p", "--run-id", "bad id/x"])
    assert any("--run-id" in e for e in launcher.problems(a))
    a = launcher.parse(["--explain-only"])
    assert {"--family is required", "--modules is required", "--tests is required"} <= set(launcher.problems(a))


def test_the_launcher_pins_are_the_gates():
    assert launcher.MUTMUT_PIN == gate.MUTMUT_PIN and launcher.PYTEST_PIN == gate.PYTEST_PIN


def test_the_runner_notebook_starts_with_the_four_line_header_and_installs_the_pins():
    a = launcher.parse(["--family", "f", "--modules", "pyvx/_validity", "--tests", "test/pyvx", "--purpose", "why", "--run-id", "r1", "--explain-only"])
    plan = launcher.make_plan(a, "a" * 40, launcher.Path("/x/geobrix-0.6.0+env6-py3-none-any.whl"), "/Volumes/c/s/v", "c.s.mutation_results")
    nb = launcher.build_notebook(plan, "tok")
    head = "".join(nb["cells"][0]["source"])
    for key in ("**Output:**", "**Time:**", "**Monitoring:**", "why"):
        assert key in head
    assert len(head.splitlines()) == 4
    install = "".join(nb["cells"][1]["source"])
    assert "mutmut==3.8.0" in install and "pytest==9.1.1" in install and "geobrix @ file:///Volumes/c/s/v/mutation-runs/r1/" in install
    assert '"mercantile>=1.2,<2"' in install
    assert "--wall-cap-min" in "".join(nb["cells"][2]["source"])
    assert "os.cpu_count() or 4) - 2" in "".join(nb["cells"][2]["source"])  # the workers come from the node it runs on


def test_fresh_cluster_is_an_option_and_the_pool_is_the_default():
    res = _sh("--family", "f", "--modules", "pyvx/_validity", "--tests", "test/pyvx", "--fresh-cluster", "--pool", "p9", "--explain-only")
    assert res.returncode == 0, res.stderr
    plan = json.loads(res.stdout[res.stdout.index("{"): res.stdout.rindex("}") + 1])
    assert plan["compute"] == {"mode": "fresh job cluster", "pool": None}
    a = launcher.parse(["--pool", "p9"])
    assert a.pool == "p9" and not a.fresh_cluster


def test_the_launcher_has_no_direct_jobs_submit():
    assert "jobs.submit(" not in (REPO_ROOT / "examples" / "tests" / "push_and_run_mutation_on_cluster.py").read_text()


def test_cores_of_node_types():
    assert [launcher.cores_of(n) for n in ("m5d.16xlarge", "m5d.8xlarge", "m5d.xlarge", "i4i.32xlarge", "weird")] == [64, 32, 4, 128, 0]


# ---- statuses, rows and the summary


@pytest.mark.parametrize(
    "code,status",
    [(1, "killed"), (3, "killed"), (0, "survived"), (-24, "timeout"), (152, "timeout"), (-11, "segfault"), (33, "absent"), (None, "unchecked"), (7, "killed")],
)
def test_status_of_exit_codes(code, status):
    assert rows_mod.status_of(code) == status


def test_function_and_module_of_a_mutant_key():
    p = "databricks.labs.gbx.pyvx._validity."
    assert rows_mod.function_of(p + "x_st_makevalid__mutmut_12") == "st_makevalid"
    assert rows_mod.function_of(p + "x__finish__mutmut_1") == "_finish"
    assert rows_mod.function_of(p + "xǁFooǁbar__mutmut_2") == "Foo.bar"
    assert rows_mod.module_of(p + "x_a__mutmut_1", ["pyvx/_cleaning", "pyvx/_validity"]) == "pyvx/_validity"


def _run():
    return {"run_id": "r", "run_ts": "2026-11-08T00:00:00", "git_sha": "abc", "family": "pyvx", "tests": "test/pyvx", "node_type": "n", "purpose": "p", "cores": 4}


def test_rows_and_summary_group_survivors_by_function_and_rate_the_modules():
    p = "databricks.labs.gbx.pyvx._validity."
    codes = {p + "x_a__mutmut_1": 1, p + "x_a__mutmut_2": 0, p + "x_b__mutmut_1": 33, p + "x_a__mutmut_3": -24, p + "x_c__mutmut_1": None}
    rows = rows_mod.rows_from_run(codes, modules=["pyvx/_validity"], run=_run(), killers={p + "x_a__mutmut_1": "test/t.py::test_a"}, diffs={p + "x_a__mutmut_2": "--- a\n+++ b\n-x = 1\n+x = 2\n"})
    by = {r["mutant"].rsplit(".", 1)[1]: r for r in rows}
    assert by["x_a__mutmut_1"]["killing_test"] == "test/t.py::test_a"
    assert by["x_a__mutmut_2"]["status"] == "survived" and by["x_a__mutmut_2"]["killing_test"] is None
    assert by["x_b__mutmut_1"]["status"] == "absent" and by["x_c__mutmut_1"]["status"] == "unchecked"
    counts = rows_mod.per_module(rows)["pyvx/_validity"]
    assert rows_mod.kill_rate(counts) == pytest.approx(200 / 3)  # 2 detected (killed, timeout), 1 survived
    md = rows_mod.summary_md(rows, dict(_run(), wall_s=5, stopped=""), trend={"pyvx/_validity": 50.0})
    assert "| pyvx/_validity | 5 | 1 | 1 | 1 | 0 | 1 | 1 | 66.7% |" in md
    assert "+16.7" in md
    assert "`pyvx/_validity` `a`: 1 survivor(s)" in md and "    +x = 2" in md
    assert "`pyvx/_validity` `b`: 1 mutant(s)" in md
    assert "1  test/t.py::test_a" in md


def test_killers_keep_the_first_failure_of_a_mutant(tmp_path):
    f = tmp_path / "k.jsonl"
    f.write_text('{"mutant": "m", "test": "a"}\n{"mutant": "m", "test": "b"}\n')
    assert rows_mod.killers_from_jsonl(f) == {"m": "a"}
    assert rows_mod.killers_from_jsonl(tmp_path / "none.jsonl") == {}


def test_the_table_ddl_names_every_column():
    ddl = rows_mod.table_ddl("c.s.mutation_results")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS c.s.mutation_results")
    assert all(f"{n} {t}" in ddl for n, t in rows_mod.COLUMNS)


def test_write_delta_replaces_the_runs_rows_and_appends_with_schema_merge():
    class Writer:
        def __init__(self, log):
            self.log = log

        def format(self, f):
            self.log.append(("format", f)); return self

        def mode(self, m):
            self.log.append(("mode", m)); return self

        def option(self, k, v):
            self.log.append(("option", k, v)); return self

        def saveAsTable(self, t):
            self.log.append(("save", t))

    class DF:
        def __init__(self, log):
            self.write = Writer(log)

    class Spark:
        def __init__(self):
            self.log = []

        def sql(self, q):
            self.log.append(("sql", q.split("\n")[0][:40]))

        def createDataFrame(self, data, schema):
            self.log.append(("rows", len(data), schema.count(",") + 1))
            return DF(self.log)

    rows = rows_mod.rows_from_run({"databricks.labs.gbx.pyvx._validity.x_a__mutmut_1": 0}, modules=["pyvx/_validity"], run=_run())
    spark = Spark()
    assert rows_mod.write_delta(spark, rows, "c.s.t") == 1
    log = spark.log
    assert log[0][0] == "sql" and log[1] == ("sql", "DELETE FROM c.s.t WHERE run_id = 'r'")
    assert ("rows", 1, len(rows_mod.COLUMNS)) in log
    assert ("option", "mergeSchema", "true") in log and ("mode", "append") in log and ("save", "c.s.t") in log


def test_the_runner_borrows_only_helpers_the_gate_has():
    for name in ("norm_module", "split_tests", "build_work_dir", "collect_codes", "tmp_root", "aborted_reason", "PKG_ROOT", "GBX_SRC", "PLUGIN", "MUTMUT_PIN", "PYTEST_PIN"):
        assert hasattr(gate, name), name
    guid = _load("guidance_runner", MUT / "guidance_runner.py")
    assert guid.gate.PLUGIN == gate.PLUGIN


def test_the_runner_section_adds_the_killers_plugin_and_the_timeouts():
    guid = _load("guidance_runner2", MUT / "guidance_runner.py")
    args = guid.parse(["--family", "f", "--modules", "pyvx/_validity", "--tests", "test/pyvx", "--run-id", "r", "--out-dir", "/tmp/o"])
    sec = guid.guidance_section(["src/databricks/labs/gbx/pyvx/_validity.py"], ["test/pyvx"], args)
    assert "gbx_mut_killers" in sec and "gbx_mut_tier0" in sec and "timeout_multiplier = 5.0" in sec and "mutate_only_covered_lines = true" in sec


def test_baseline_failures_are_read_from_the_short_summary():
    guid = _load("guidance_runner3", MUT / "guidance_runner.py")
    text = "....\nFAILED test/a.py::t[x - y] - assert 1\nERROR test/b.py::u - boom\nFAILED test/a.py::t[x - y] - dup\nFAILED test/c.py::v[a] - x[0]\n1 failed"
    assert guid.baseline_failed_ids(text) == ["test/a.py::t[x - y]", "test/b.py::u", "test/c.py::v[a]"]


def test_the_trend_compares_only_runs_with_the_same_tests():
    class Row(dict):
        pass

    class Spark:
        def __init__(self):
            self.sql_seen = []

        def sql(self, q):
            self.sql_seen.append(q)
            return types_namespace([Row(module="m", run_id="old", ts="2026-11-07", d=3, s=1), Row(module="m", run_id="older", ts="2026-11-01", d=1, s=1)])

    def types_namespace(rows):
        return type("R", (), {"collect": lambda self: rows})()

    spark = Spark()
    assert rows_mod.previous_rates(spark, "c.s.t", "pyvx", "new", "test/pyvx") == {"m": 75.0}
    assert "AND tests = 'test/pyvx'" in spark.sql_seen[0] and "run_id <> 'new'" in spark.sql_seen[0]
