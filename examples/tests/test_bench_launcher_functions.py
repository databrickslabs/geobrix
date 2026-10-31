"""The bench launcher stops on a mistyped ``bng_*`` name before it submits anything.

The on-cluster notebook rejects such a name too, but only after the job has started. Pure: the
launcher reads the leg table and the registry as text. Run:
  gbx:test:notebooks --path test_bench_launcher_functions.py
"""

import re

import pytest
import push_and_run_bench_on_cluster as launcher


def test_a_mistyped_bng_name_is_refused_and_every_real_one_is_not():
    # names the bench knows: a leg (bng_kringexplode), a registered function (bng_cellfill)
    assert launcher._unknown_bng_names("bng_kringexplode,bng_cellfill,rst_width") == []
    assert launcher._unknown_bng_names("bng_kringexplod,rst_width,bng_nope") == [
        "bng_kringexplod",
        "bng_nope",
    ]
    # a list typed with spaces after the commas is the same list
    assert launcher._unknown_bng_names("rst_width, bng_nope , bng_cellfill") == [
        "bng_nope"
    ]
    assert launcher._function_names(" rst_width, bng_nope ,, ") == [
        "rst_width",
        "bng_nope",
    ]

    # every leg the bench can run is read from the table (a reformatted table would flag them all)
    readers = (launcher._BENCH_SRC / "readers.py").read_text()
    legs = [n[len("run_") :] for n in re.findall(r"^def (run_bng_\w+)\(", readers, re.M)]
    assert len(legs) >= 20 and launcher._unknown_bng_names(",".join(legs)) == []


def test_the_bench_installs_the_env_wheel_of_the_clusters_runtime():
    # DBR 17.3 / 18 run the env5 wheel, DBR 19 the env6 wheel (the other family's refuses to import)
    for key, env in (("17.3.x-scala2.13", 5), ("18.x-aarch64-scala2.13", 5), ("19.x-scala2.13", 6),
                     ("19.x-cpu-ml-scala2.13", 6)):  # fmt: skip
        assert launcher.env_of_runtime(key) == env, key


# ---- run hardening: the preflight, the smoke verdict and the watchdog wiring (no workspace)

import io
import json
import types

import _preflight
import _run_watchdog

WHEEL = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"


def test_the_preflight_refuses_a_spec_without_the_wheel_a_wrong_family_and_a_staged_wheel_that_is_not_the_build(tmp_path):
    assert _preflight.check_env_spec(_preflight.serverless_dependencies(WHEEL), WHEEL, 6) == []
    assert "does not name the wheel" in _preflight.check_env_spec(["markdown"], WHEEL, 6)[0]
    assert "+env6 family" in _preflight.check_env_spec([WHEEL], WHEEL, 5)[0]
    # the staged bytes against the local build
    (tmp_path / "dist" / "env6").mkdir(parents=True)
    built = tmp_path / "dist" / "env6" / "geobrix-0.6.0+env6-py3-none-any.whl"
    built.write_bytes(b"the build")
    sha = _preflight.file_sha256(built)
    local = _preflight.local_wheel(tmp_path, built.name)
    assert _preflight.check_wheel(local, sha, WHEEL) == []
    assert "not the local build" in _preflight.check_wheel(local, "0" * 64, WHEEL)[0]
    assert "no local build" in _preflight.check_wheel(None, sha, WHEEL)[0]


def test_a_plain_staged_wheel_is_checked_when_dist_has_it_and_skipped_with_a_reason_when_not(tmp_path, capsys):
    """A wheel without +env<N> has no env family to look up: it is not a launch failure by itself."""
    plain = "/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"

    class Files:
        @staticmethod
        def download(path):
            return types.SimpleNamespace(contents=io.BytesIO(b"the build"))

    w = types.SimpleNamespace(files=Files)
    assert _preflight.wheel_problems(w, plain, tmp_path) == []  # nothing in dist/: skipped, and says why
    assert "no +env<N> in the name" in capsys.readouterr().out
    (tmp_path / "dist").mkdir()
    built = tmp_path / "dist" / "geobrix-0.6.0-py3-none-any.whl"
    built.write_bytes(b"the build")
    assert _preflight.wheel_problems(w, plain, tmp_path) == []  # in dist/: the bytes are compared
    built.write_bytes(b"another build")
    assert "not the local build" in _preflight.wheel_problems(w, plain, tmp_path)[0]


def run_outcome(payload, result="SUCCESS"):
    state = types.SimpleNamespace(life_cycle_state="TERMINATED", result_state=result, state_message="")
    return launcher.Outcome(run=types.SimpleNamespace(state=state), payload=json.dumps(payload))


def test_a_smoke_is_clean_only_when_the_import_check_passes_and_no_form_failed():
    ok = {"import_check": {"ok": True, "version": "0.6.0", "arch": "x86_64"}, "forms": [{"fn": "f"}], "errors": []}
    assert launcher.smoke_problems(run_outcome(ok), require_forms=True) == []
    bad = launcher.smoke_problems(
        run_outcome({**ok, "import_check": {"ok": False, "error": "ModuleNotFoundError: databricks.labs"}}),
        require_forms=True,
    )
    assert "worker-side import check failed" in bad[0] and "databricks.labs" in bad[0]
    err = {"fn": "gbx_bng", "level": "L4", "form": "uc-scalar", "failure": "UDF_USER_CODE_ERROR"}
    failed = launcher.smoke_problems(run_outcome({**ok, "errors": [err]}), require_forms=True)
    assert "gbx_bng L4/uc-scalar failed" in failed[0]
    assert launcher.smoke_problems(run_outcome({**ok, "errors": [err]}), require_forms=True, allow_errors=True) == []
    assert launcher.smoke_problems(run_outcome({**ok, "forms": []}), require_forms=True)  # nothing ran
    assert launcher.smoke_problems(run_outcome({}), require_forms=True)  # no exit payload


def test_a_run_whose_heartbeat_names_a_stuck_form_is_cancelled_by_the_launcher_and_reported(capsys):
    cancelled = []
    stuck = {"seq": 1, "state": "running", "form": "gbx_bng:L4:uc-scalar", "form_elapsed_s": 0.0,
             "stack": ["File levels_forms.py, line 470, in uc_scalar"]}  # fmt: skip

    class Files:
        @staticmethod
        def download(path):
            return types.SimpleNamespace(contents=types.SimpleNamespace(read=lambda: json.dumps(stuck).encode()))

    class Jobs:
        state = ("RUNNING", "")

        def get_run(self, run_id):
            life = types.SimpleNamespace(value=self.state[0])
            return types.SimpleNamespace(state=types.SimpleNamespace(life_cycle_state=life, result_state=None), tasks=[])

        def cancel_run(self, run_id):
            cancelled.append(run_id)
            self.state = ("TERMINATED", "CANCELED")

        def export_run(self, **kw):
            raise RuntimeError("no export in the fake")

    w = types.SimpleNamespace(jobs=Jobs(), files=Files)
    out = launcher.watch_to_end(w, 77, hb_path="/Volumes/x/heartbeat.json", form_cap_s=0.05, stale_s=60, poll_secs=0.02)
    text = capsys.readouterr().out
    assert cancelled == [77] and out.stall is not None and out.run is None
    assert "STALLED: gbx_bng:L4:uc-scalar after" in text and "uc_scalar" in text
    assert _run_watchdog.EXIT_STALLED == 3


# ---- the smoke stage as the launcher runs it: a bad smoke blocks the real run


class SmokeWorkspace:
    """A workspace whose smoke run ends with ``payload`` as the notebook's exit value."""

    def __init__(self, payload):
        life = types.SimpleNamespace(value="TERMINATED")
        state = types.SimpleNamespace(life_cycle_state=life, result_state=types.SimpleNamespace(value="SUCCESS"), state_message="")
        run = types.SimpleNamespace(state=state, tasks=[types.SimpleNamespace(run_id=5)])
        out = types.SimpleNamespace(notebook_output=types.SimpleNamespace(result=json.dumps(payload)))
        self.jobs = types.SimpleNamespace(get_run=lambda rid: run, get_run_output=lambda rid: out)
        self.files = types.SimpleNamespace(delete=lambda p: None, download=lambda p: (_ for _ in ()).throw(FileNotFoundError(p)))


CLEAN = {"import_check": {"ok": True, "version": "0.6.0", "arch": "x86_64"}, "forms": [{"fn": "f", "level": "L3", "form": "c0", "status": "ok", "tasks": 8, "median_s": 1.0}], "errors": []}  # fmt: skip
BAD = {**CLEAN, "errors": [{"fn": "gbx_st_makevalid", "level": "L4", "form": "uc-scalar", "failure": "ISOLATED_COMMAND_EXEC_ERROR: pip install returned 1"}]}  # fmt: skip


def smoke_stage(payload, capsys, **over):
    submitted = []

    def submit(path, name, timeout_s):
        submitted.append((path, name, timeout_s))
        return 11

    kw = dict(
        do_smoke=True, no_smoke=False, smoke_only=False, smoke_path="/nb/smoke", run_id="r1", out_dir="/Volumes/x/r1",
        smoke_rows=16, smoke_timeout_min=15, form_cap_s=900.0, stale_s=300.0, token="tok", levels_mvp="S", allow_errors=False,
    )  # fmt: skip
    kw.update(over)
    rc = launcher.run_smoke_stage(SmokeWorkspace(payload), submit, **kw)
    cap = capsys.readouterr()
    return rc, submitted, cap.out, cap.err


def test_a_smoke_with_a_failed_form_blocks_the_real_run_with_exit_4_and_names_the_failure(capsys):
    rc, submitted, out, err = smoke_stage(BAD, capsys)
    assert rc == launcher.EXIT_SMOKE_FAILED == 4
    assert [s[1] for s in submitted] == ["geobrix-bench-r1-smoke"]  # only the smoke was submitted
    assert submitted[0][2] == 15 * 60  # with its own time limit
    assert "SMOKE STAGE FAILED: nothing else is submitted" in err
    assert "gbx_st_makevalid L4/uc-scalar failed" in err and "pip install returned 1" in err


def test_a_clean_smoke_lets_the_real_run_go_on_and_smoke_only_stops_there(capsys):
    rc, submitted, out, _ = smoke_stage(CLEAN, capsys)
    assert rc is None and len(submitted) == 1 and "SMOKE STAGE CLEAN" in out
    rc, submitted, out, _ = smoke_stage(CLEAN, capsys, smoke_only=True)
    assert rc == 0 and "the real run is not submitted" in out
    rc, submitted, out, _ = smoke_stage(BAD, capsys, allow_errors=True)  # --smoke-allow-errors
    assert rc is None


def test_no_smoke_submits_no_smoke_run_and_logs_that_it_was_skipped(capsys):
    rc, submitted, out, _ = smoke_stage(BAD, capsys, do_smoke=False, no_smoke=True)
    assert rc is None and submitted == []
    assert "SMOKE STAGE SKIPPED (--no-smoke)" in out


# ---- the time limits are refused at argument-parse time


@pytest.mark.parametrize(
    "flag,value",
    [("--timeout-min", "0"), ("--timeout-min", "-5"), ("--timeout-min", "abc"), ("--timeout-min", "1.5"),
     ("--smoke-timeout-min", "0"), ("--smoke-timeout-min", "-1"), ("--smoke-timeout-min", "x")],
)  # fmt: skip
def test_a_time_limit_that_is_not_a_positive_whole_number_of_minutes_is_refused_before_any_work(
    flag, value, monkeypatch, capsys
):
    monkeypatch.setattr(launcher.sys, "argv", ["push_and_run_bench_on_cluster.py", flag, value])
    monkeypatch.setattr(launcher, "WorkspaceClient", lambda *a, **k: pytest.fail("work started before the check"), raising=False)
    assert launcher.main() == 2
    err = capsys.readouterr().err
    assert flag in err and "whole number of minutes" in err and err.count("\n") == 1


def test_the_default_se_ladder_is_refused_up_front_with_the_scales_that_fit(monkeypatch, capsys):
    """SE builds 4 MiB DEM tiles on the client: only 1x fits under the 64 MiB cap, so 1,4,16 can never pass."""
    pytest.importorskip("pyspark")  # the check reads the form table (levels_forms), which imports pyspark
    base = ["push_and_run_bench_on_cluster.py", "--levels", "SE"]
    monkeypatch.setattr(launcher, "WorkspaceClient", lambda *a, **k: pytest.fail("work started before the check"), raising=False)
    monkeypatch.setattr(launcher.sys, "argv", base)
    assert launcher.main() == 2
    err = capsys.readouterr().err
    assert "--levels-scales" in err and "SE" in err and "1,4,16" in err and "--levels-scales 1" in err
    assert launcher.levels_scales_problem("SE", (1,), launcher.TESTS_DIR.parent.parent) is None  # 1x fits
    assert launcher.levels_scales_problem("S", (1, 4, 16), launcher.TESTS_DIR.parent.parent) is None  # other sets are fine


def test_the_levels_run_gets_the_same_default_time_limit_as_any_other(monkeypatch):
    assert launcher.DEFAULT_RUN_TIMEOUT_MIN == 360


def test_a_failed_run_keeps_its_notebook_for_a_repair_run(capsys):
    deleted = []

    class Workspace:
        def delete(self, path):
            deleted.append(path)

    class W:
        workspace = Workspace()

    launcher._remove_notebook(W(), "/nb/failed", succeeded=False)
    assert deleted == [] and "/nb/failed" in capsys.readouterr().out
    launcher._remove_notebook(W(), "/nb/ok")
    assert deleted == ["/nb/ok"]
