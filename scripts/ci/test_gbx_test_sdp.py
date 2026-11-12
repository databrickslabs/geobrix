"""gbx:test:sdp (examples/tests/run_sdp_bundle.py): the bundle checks, the plan guard, the poll and the table check.

A fake workspace and a fake `databricks` CLI record every call; nothing reaches a workspace.
"""

import importlib
import json
import subprocess
import sys
import types
from types import SimpleNamespace as NS

import pytest
from conftest import REPO_ROOT

SH = REPO_ROOT / "scripts" / "commands" / "gbx-test-sdp.sh"


@pytest.fixture
def sdp(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("run_sdp_bundle")


def test_help_names_the_options_and_an_unknown_option_exits_2():
    res = subprocess.run(["bash", str(SH), "--help"], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    for opt in ("--bundle", "--profile", "--wheel", "--var", "--only", "--pipeline-only", "--warehouse-id",
                "--timeout-min", "--log"):
        assert opt in res.stdout
    assert subprocess.run(["bash", str(SH), "--nope"], capture_output=True, text=True).returncode == 2


def test_the_known_bundles_resolve_to_their_databricks_yml(sdp):
    for name, path in sdp.BUNDLES.items():
        assert sdp.resolve_bundle(name) == path
        assert sdp.bundle_name(sdp.bundle_text(path)) == name
        assert sdp.environment_versions(sdp.bundle_text(path)) == {"5"}  # the example jobs run Serverless env 5
    with pytest.raises(SystemExit, match="no databricks.yml"):
        sdp.resolve_bundle(str(REPO_ROOT / "docs"))


def test_the_default_wheel_is_the_versioned_staged_env5_wheel(sdp):
    w = sdp.default_wheel(5)
    assert "/sample-data/geobrix/" in w and w.endswith("/env5/geobrix-%s+env5-py3-none-any.whl" % w.split("/")[-3])
    text = sdp.bundle_text(sdp.BUNDLES["wireless_coverage_lf"])
    assert sdp.wheel_problem(w, text) is None
    assert "refuses to import" in sdp.wheel_problem(w.replace("env5", "env6"), text)
    assert "not an env wheel" in sdp.wheel_problem("/Volumes/x/geobrix-0.6.0-py3-none-any.whl", text)


def test_the_cli_lines_carry_the_profile_target_and_vars(sdp):
    cmd = sdp.cli(["validate"], "p", "dev", {"gbx_wheel": "/V/w.whl", "full_aoi": "false"}, output_json=True)
    assert cmd == ["databricks", "bundle", "validate", "--profile", "p", "--target", "dev",
                   "--var=gbx_wheel=/V/w.whl", "--var=full_aoi=false", "-o", "json"]


@pytest.mark.parametrize("plan,gone", [
    (json.dumps({"plan": {"resources.pipelines.p": {"action": "recreate"}, "resources.jobs.j": {"action": "update"}}}),
     ["recreate resources.pipelines.p"]),
    (json.dumps({"plan": {"resources.jobs.j": {"action": "update"}}}), []),
    ("update jobs.j\ndelete pipelines.p\n", ["delete pipelines.p"]),
])
def test_a_plan_that_deletes_or_recreates_is_caught(sdp, plan, gone):
    assert sdp.destructive_actions(plan) == gone


def test_a_failure_only_a_drop_fixes_is_one_line_for_the_orchestrator(sdp):
    msg = ("Table `geospatial_docs`.`wireless_coverage_lf`.`wc_lidar_pts` already exists as a MATERIALIZED_VIEW; "
           "a STREAMING_TABLE cannot replace it")
    hint = sdp.drop_hint(msg)
    assert hint.startswith("DROP NEEDED: DROP MATERIALIZED VIEW geospatial_docs.wireless_coverage_lf.wc_lidar_pts")
    assert "drops nothing" in hint
    assert sdp.drop_hint("flow silver_x failed: ZeroDivisionError") is None


def test_summary_ids(sdp):
    s = {"resources": {"jobs": {"j": {"id": "11"}}, "pipelines": {"p": {"id": "abc"}, "q": {}}}}
    assert sdp.summary_ids(s) == ({"j": "11"}, {"p": "abc"})


# ---------------------------------------------------------------------------------------------- the poll


class Clock:
    def __init__(self):
        self.t = 1000.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def _run(life, result=None, tasks=()):
    return NS(state=NS(life_cycle_state=life, result_state=result, state_message=""), tasks=list(tasks),
              run_page_url="u")


class FakeW:
    def __init__(self, runs, updates, errors=(), tables=(), counts=None):
        self.runs, self.updates, self.errors = list(runs), list(updates), list(errors)
        self.cancelled, self.stopped, self.sql = [], [], []
        self.jobs = NS(get_run=self._get_run, cancel_run=lambda run_id: self.cancelled.append(run_id))
        self.pipelines = NS(
            list_updates=lambda pipeline_id, max_results: NS(updates=[NS(update_id="u1", creation_time=10**13)]),
            get_update=lambda pipeline_id, update_id: NS(update=NS(state=self._update())),
            list_pipeline_events=lambda **k: iter(self.errors),
            stop=lambda pipeline_id: self.stopped.append(pipeline_id),
            get=lambda pipeline_id: NS(spec=NS(catalog="c", schema="s")),
        )
        self.tables = NS(list=lambda catalog_name, schema_name: list(tables))
        self.warehouses = NS(get=lambda wid: NS(state=NS(value="RUNNING")))
        counts = counts or {}

        def execute(statement, warehouse_id, wait_timeout):
            self.sql.append((statement, warehouse_id))
            n = counts[statement.split()[-1]]
            return NS(status=NS(state="SUCCEEDED"), result=NS(data_array=[[str(n)]]), statement_id="s")

        self.statement_execution = NS(execute_statement=execute)

    def _get_run(self, run_id):
        return self.runs.pop(0) if len(self.runs) > 1 else self.runs[0]

    def _update(self):
        return self.updates.pop(0) if len(self.updates) > 1 else self.updates[0]


def _poll(sdp, w, clock, **kw):
    lines = []
    out = sdp.poll(w, pipeline_ids={"p": "pid"}, since_ms=0, timeout_s=kw.pop("timeout_s", 600), poll_s=30,
                   say=lines.append, now=clock.now, sleep=clock.sleep, **kw)
    return out, lines


def test_a_job_run_that_succeeds_is_ok(sdp):
    w = FakeW([_run("RUNNING"), _run("TERMINATED", "SUCCESS")], ["RUNNING", "COMPLETED"])
    (ok, problems), lines = _poll(sdp, w, Clock(), run_id=5)
    assert ok and problems == [] and w.cancelled == []
    assert any("pipeline p update u1: RUNNING" in x for x in lines)


def test_a_failed_flow_stops_the_job_run_at_once(sdp):
    err = NS(origin=NS(update_id="u1", flow_name="silver_surfaces"), event_type="flow_progress",
             message="Flow 'silver_surfaces' has FAILED", error=NS(exceptions=[NS(message="ZeroDivisionError")]))
    w = FakeW([_run("RUNNING")], ["RUNNING"], errors=[err])
    (ok, problems), _ = _poll(sdp, w, Clock(), run_id=5)
    assert not ok and w.cancelled == [5]
    assert "silver_surfaces" in problems[0] and "ZeroDivisionError" in problems[0]


def test_an_error_event_of_another_update_does_not_count(sdp):
    old = NS(origin=NS(update_id="older", flow_name="x"), event_type="flow_progress", message="old failure", error=None)
    w = FakeW([_run("RUNNING"), _run("TERMINATED", "SUCCESS")], ["RUNNING", "COMPLETED"], errors=[old])
    (ok, _), _ = _poll(sdp, w, Clock(), run_id=5)
    assert ok


def test_the_time_limit_cancels_the_run(sdp):
    clock = Clock()
    w = FakeW([_run("RUNNING")], ["RUNNING"])
    (ok, problems), _ = _poll(sdp, w, clock, run_id=5, timeout_s=90)
    assert not ok and w.cancelled == [5] and "cancelled" in problems[0] and clock.t - 1000.0 <= 150


def test_a_pipeline_only_update_is_polled_and_stopped_on_failure(sdp):
    w = FakeW([], ["RUNNING", "COMPLETED"])
    (ok, _), _ = _poll(sdp, w, Clock(), update=("pid", "u1"))
    assert ok
    w = FakeW([], ["FAILED"])
    (ok, problems), _ = _poll(sdp, w, Clock(), update=("pid", "u1"))
    assert not ok and "FAILED" in problems[0]


def test_a_failed_job_task_is_reported(sdp):
    task = NS(task_key="siting", state=NS(result_state="FAILED", life_cycle_state="TERMINATED", state_message="boom"),
              run_page_url="page")
    w = FakeW([_run("TERMINATED", "FAILED", [task])], ["COMPLETED"])
    (ok, problems), _ = _poll(sdp, w, Clock(), run_id=5)
    assert not ok and any("task siting: FAILED boom" in p for p in problems)


# ---------------------------------------------------------------------------------------------- the tables


def test_every_owned_table_must_hold_rows(sdp):
    tables = [NS(name="gold", pipeline_id="pid", table_type="MATERIALIZED_VIEW"),
              NS(name="quarantine", pipeline_id="pid", table_type="STREAMING_TABLE"),
              NS(name="other", pipeline_id="another", table_type="MANAGED")]
    w = FakeW([], ["COMPLETED"], tables=tables, counts={"c.s.gold": 7, "c.s.quarantine": 0})
    lines = []
    assert sdp.check_tables(w, "pid", "wh", say=lines.append) == ["c.s.quarantine is empty"]
    assert [s for s, _ in w.sql] == ["SELECT count(*) FROM c.s.gold", "SELECT count(*) FROM c.s.quarantine"]
    assert sdp.check_tables(w, "pid", "wh", allow_empty=["quarantine"], say=lines.append) == []
    assert sdp.check_tables(FakeW([], ["COMPLETED"]), "pid", "wh") == ["no table of c.s is owned by pipeline pid"]


# ---------------------------------------------------------------------------------------------- main


def _main(sdp, monkeypatch, argv, cli_results, w=None):
    calls = []

    def fake_run(cmd, cwd, say=print):
        calls.append(cmd[2])
        return cli_results.get(cmd[2], subprocess.CompletedProcess(cmd, 0, "{}", ""))

    monkeypatch.setattr(sdp, "run_cli", fake_run)
    if w is not None:
        sdk = types.ModuleType("databricks.sdk")
        sdk.WorkspaceClient = lambda profile: w
        service = types.ModuleType("databricks.sdk.service")
        jobs = types.ModuleType("databricks.sdk.service.jobs")
        jobs.PipelineParams = lambda full_refresh: NS(full_refresh=full_refresh)
        service.jobs = jobs
        for name, mod in (("databricks.sdk", sdk), ("databricks.sdk.service", service),
                          ("databricks.sdk.service.jobs", jobs)):
            monkeypatch.setitem(sys.modules, name, mod)
    return sdp.main(argv), calls


def test_the_profile_is_required(sdp, monkeypatch):
    with pytest.raises(SystemExit) as exc:
        _main(sdp, monkeypatch, ["--bundle", "wireless_coverage_lf", "--validate-only"], {})
    assert exc.value.code == 2


def test_validate_only_validates_and_plans_and_deploys_nothing(sdp, monkeypatch):
    code, calls = _main(sdp, monkeypatch, ["--bundle", "wireless_coverage_lf", "--profile", "p", "--validate-only"], {})
    assert code == 0 and calls == ["validate", "plan"]


def test_a_destructive_plan_stops_before_the_deploy(sdp, monkeypatch, capsys):
    plan = subprocess.CompletedProcess([], 0, json.dumps({"plan": {"pipelines.p": {"action": "delete"}}}), "")
    code, calls = _main(sdp, monkeypatch, ["--bundle", "wireless_coverage_lf", "--profile", "p", "--no-run"],
                        {"plan": plan})
    assert code == 1 and calls == ["validate", "plan"] and "DESTRUCTIVE PLAN" in capsys.readouterr().out


def test_a_full_run_deploys_runs_the_job_and_checks_the_tables(sdp, monkeypatch):
    summary = subprocess.CompletedProcess([], 0, json.dumps(
        {"resources": {"jobs": {"wc_job": {"id": "42"}}, "pipelines": {"wc_pipe": {"id": "pid"}}}}), "")
    w = FakeW([_run("TERMINATED", "SUCCESS")], ["COMPLETED"],
              tables=[NS(name="gold", pipeline_id="pid", table_type="MATERIALIZED_VIEW")], counts={"c.s.gold": 3})
    started = []
    w.jobs.run_now = lambda **kw: started.append(kw) or NS(run_id=5)
    code, calls = _main(sdp, monkeypatch, ["--bundle", "wireless_coverage_lf", "--profile", "p", "--warehouse-id",
                                           "wh", "--only", "land,pipeline", "--full-refresh", "--poll-secs", "0",
                                           "--var", "full_aoi=false"], {"summary": summary}, w=w)
    assert code == 0 and calls == ["validate", "plan", "deploy", "summary"]
    assert started[0]["job_id"] == 42 and started[0]["only"] == ["land", "pipeline"]
    assert started[0]["pipeline_params"].full_refresh is True and len(started[0]["idempotency_token"]) == 32
    assert w.sql == [("SELECT count(*) FROM c.s.gold", "wh")]


def test_a_run_needs_a_warehouse_for_the_table_check(sdp, monkeypatch):
    with pytest.raises(SystemExit) as exc:
        _main(sdp, monkeypatch, ["--bundle", "wireless_coverage_lf", "--profile", "p"], {})
    assert exc.value.code == 2
