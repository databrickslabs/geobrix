"""Tests for examples/tests/push_and_run_corpus_gen_on_serverless.py (gbx:bench:gen-data --on-databricks).

No workspace and no real SDK: a stub `databricks.sdk` hands the launcher an in-memory
workspace that records the notebooks it uploads, the jobs it submits and what it deletes, and
answers each run from a script. Tiles are 16 px so the host-side sha256 reference is instant.

  main   -- refusals before any client is built, the happy path (upload, submit, watch, verify),
            and a failing smoke that blocks the real run
  watch  -- run-state transitions, progress lines decoded from the export view's notebook
            model, and the stall watchdog
"""

import base64
import importlib
import json
import sys
import types
import urllib.parse
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT

NOTEBOOK_ARGS = ("--rows-from", "0", "--rows-to", "3", "--row-tile-px", "16", "--row-bands", "1")


class _Rec:
    """A stand-in for an SDK dataclass: keeps its keyword arguments as attributes."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def _html(*lines):
    """A `jobs export-run` view: the notebook as a URL-quoted base64 JSON model."""
    model = {"commands": [{"results": {"data": [{"data": f"{line}\n"} for line in lines]}}]}
    blob = base64.b64encode(urllib.parse.quote(json.dumps(model)).encode()).decode()
    return f"<script>__DATABRICKS_NOTEBOOK_MODEL = '{blob}';</script>"


def _run(life, result=None, task=7, message=""):
    state = SimpleNamespace(
        life_cycle_state=SimpleNamespace(value=life),
        result_state=SimpleNamespace(value=result) if result else None,
        state_message=message,
    )
    return SimpleNamespace(state=state, tasks=[SimpleNamespace(run_id=task)], run_id=1, run_page_url="https://example/run")


@pytest.fixture
def sdk(monkeypatch):
    """A stub databricks.sdk. `state.runs` maps a run name's suffix (smoke/gen) to its scripted
    end (result, payload); `state.uploads/submits/deleted/cancelled/clients` record the calls."""
    state = SimpleNamespace(
        uploads=[], submits=[], deleted=[], cancelled=[], clients=[], staged_wheel=b"the env wheel",
        smoke=("SUCCESS", {"ok": True, "import_check": {"ok": True, "version": "0.6.0", "arch": "x86_64"}}),
        gen=("SUCCESS", {}), html="",
    )  # fmt: skip

    class NotFound(OSError):
        pass

    class Files:
        def get_metadata(self, path):
            raise NotFound(path)  # not staged under geobrix/<ver>/env<N>/: the flat path is used

        def download(self, path):
            import io

            return SimpleNamespace(contents=io.BytesIO(state.staged_wheel))

    class Workspace:
        def mkdirs(self, path):
            pass

        def upload(self, path, content, format=None, overwrite=False):
            state.uploads.append((path, json.loads(content.read().decode("utf-8"))))

        def delete(self, path):
            state.deleted.append(path)

    class Jobs:
        def submit(self, **kw):
            state.submits.append(kw)
            return SimpleNamespace(run_id=10 + len(state.submits))

        def _end(self, rid):
            kind = "smoke" if state.submits[rid - 11]["run_name"].endswith("-smoke") else "gen"
            return getattr(state, kind)

        def get_run(self, rid):
            result, _ = self._end(rid)
            return _run("TERMINATED", result, task=rid + 100, message="boom" if result != "SUCCESS" else "")

        def get_run_output(self, task_run_id):
            _, payload = self._end(task_run_id - 100)
            return SimpleNamespace(notebook_output=SimpleNamespace(result=json.dumps(payload)))

        def export_run(self, run_id, views_to_export):
            return SimpleNamespace(views=[SimpleNamespace(content=state.html)])

        def cancel_run(self, rid):
            state.cancelled.append(rid)

    class WorkspaceClient:
        def __init__(self, profile=None, **kw):
            state.clients.append(profile)
            self.files, self.workspace, self.jobs = Files(), Workspace(), Jobs()
            self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@example.com"))

    mods = {n: types.ModuleType(n) for n in (
        "databricks.sdk", "databricks.sdk.errors", "databricks.sdk.service", "databricks.sdk.service.compute",
        "databricks.sdk.service.jobs", "databricks.sdk.service.workspace")}  # fmt: skip
    mods["databricks.sdk"].WorkspaceClient = WorkspaceClient
    mods["databricks.sdk.errors"].NotFound = NotFound
    compute, jobs, workspace = (mods[f"databricks.sdk.service.{n}"] for n in ("compute", "jobs", "workspace"))
    compute.Environment = _Rec
    jobs.JobEnvironment = jobs.SubmitTask = jobs.NotebookTask = _Rec
    jobs.Source = SimpleNamespace(WORKSPACE="WORKSPACE")
    jobs.ViewsToExport = SimpleNamespace(CODE="CODE")
    workspace.ImportFormat = SimpleNamespace(JUPYTER="JUPYTER")
    for name, mod in mods.items():
        monkeypatch.setitem(sys.modules, name, mod)
    return state


@pytest.fixture
def launcher(monkeypatch, tmp_path, sdk):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    for key in ("GBX_BUNDLE_VOLUME_CATALOG", "GBX_BUNDLE_VOLUME_SCHEMA", "GBX_BUNDLE_VOLUME_NAME"):
        monkeypatch.delenv(key, raising=False)
    mod = importlib.import_module("push_and_run_corpus_gen_on_serverless")
    monkeypatch.setattr(mod, "ROOT", tmp_path)  # dist/ (the local wheel) and test-logs/ are the test's own
    return mod


@pytest.fixture
def wheel(launcher, tmp_path):
    """The local build of the env6 wheel the launcher stages from (same bytes as the stub Volume's)."""
    name = f"geobrix-{launcher.bench._geobrix_version()}+env6-py3-none-any.whl"
    built = tmp_path / "dist" / "env6" / name
    built.parent.mkdir(parents=True)
    built.write_bytes(b"the env wheel")
    return built


def go(monkeypatch, launcher, tmp_path, *args):
    argv = ["gen", "--profile", "p", "--local-corpus", str(tmp_path / "corpus"), "--poll-secs", "0", *args]
    monkeypatch.setattr(sys, "argv", argv)
    return launcher.main()


def good_payload(launcher, tmp_path, *, complete=4):
    """The run notebook's exit payload for rows 0..3 with the host's own sha256 for the sampled tiles."""
    from databricks.labs.gbx.bench import datagen_remote as dr

    spec = dr.RowSpec(1234, (4326, 3857, 32618, 27700), 16, 1, "float32")
    verify = dr.default_verify_indices(0, 3)
    shas = launcher._local_sha(tmp_path / "corpus", verify, spec, dr)
    return {
        "ok": True,
        "summary": {"written": 4},
        "inventory": {"expected": 4, "complete": complete, "short": [], "missing": [], "bytes": 4096, "tif_files_in_dir": 4},
        "sha": {j: {"sha256": s, "size": 1024} for j, s in shas.items()},
    }  # fmt: skip


# ---- main: refusals before anything is built or submitted


@pytest.mark.parametrize(
    "args,msg",
    [
        (("--rows-from", "5", "--rows-to", "2"), "need 0 <= rows_from <= rows_to"),
        (("--rows-from", "-1"), "need 0 <= rows_from <= rows_to"),
        (("--rows-from", "0", "--rows-to", "3", "--verify-indices", "9"), "fall outside 0..3"),
        (("--smoke-only", "--no-smoke"), "exclude each other"),
        (("--bogus",), "unknown option"),
    ],
)
def test_main_refuses_bad_input_with_exit_2_before_any_client_is_built(monkeypatch, launcher, tmp_path, sdk, capsys, args, msg):
    assert go(monkeypatch, launcher, tmp_path, *args) == launcher.EXIT_USAGE == 2
    assert msg in capsys.readouterr().err
    assert sdk.clients == [] and sdk.uploads == [] and sdk.submits == []


def test_main_requires_a_profile(monkeypatch, launcher, sdk):
    monkeypatch.setattr(sys, "argv", ["gen", "--rows-from", "0"])
    with pytest.raises(SystemExit) as exc:
        launcher.main()
    assert exc.value.code == 2 and sdk.clients == []


def test_main_refuses_a_staged_wheel_that_is_not_the_local_build_and_submits_nothing(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    sdk.staged_wheel = b"another build"
    assert go(monkeypatch, launcher, tmp_path, *NOTEBOOK_ARGS) == 2
    assert "PREFLIGHT FAILED" in capsys.readouterr().err and sdk.submits == [] and sdk.uploads == []


# ---- main: smoke first, then the real run, then the verdict


def test_main_runs_the_smoke_then_the_real_run_and_verifies_the_sampled_sha256(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    sdk.gen = ("SUCCESS", good_payload(launcher, tmp_path))
    sdk.html = _html("[GENCORPUS] progress: done 4/4 (written 4, skipped 0)")

    assert go(monkeypatch, launcher, tmp_path, "--run-id", "t1", *NOTEBOOK_ARGS) == 0

    out = capsys.readouterr().out
    assert [s["run_name"] for s in sdk.submits] == ["geobrix-gen-corpus-t1-smoke", "geobrix-gen-corpus-t1"]
    assert [p for p, _ in sdk.uploads] == [
        "/Users/me@example.com/geobrix_gen_corpus_t1_smoke.ipynb",
        "/Users/me@example.com/geobrix_gen_corpus_t1.ipynb",
    ]
    modes = ['"mode": "smoke"' in "".join(nb["cells"][1]["source"]) for _, nb in sdk.uploads]
    assert modes == [True, False]  # the first notebook is the smoke, the second the real run
    (env,) = sdk.submits[1]["environments"]
    assert env.spec.environment_version == "6" and env.spec.dependencies[0].endswith("+env6-py3-none-any.whl")
    assert sdk.submits[1]["tasks"][0].max_retries == 0 and sdk.submits[1]["timeout_seconds"] == 240 * 60
    assert sdk.deleted == [p for p, _ in sdk.uploads]  # both notebooks removed once their runs succeeded
    assert "SMOKE CLEAN" in out and "VERIFIED: count and sha256 sample match the local corpus." in out
    assert (tmp_path / "test-logs" / "bench" / "t1" / "notebook-output.html").is_file()


def test_main_stops_with_exit_4_when_the_smoke_fails_and_never_submits_the_real_run(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    sdk.smoke = ("SUCCESS", {"ok": False, "import_check": {"ok": True}, "mismatch": [2]})

    assert go(monkeypatch, launcher, tmp_path, "--run-id", "t2", *NOTEBOOK_ARGS) == launcher.EXIT_SMOKE == 4

    err = capsys.readouterr().err
    assert "SMOKE FAILED: nothing else is submitted" in err and "mismatch" in err
    assert [s["run_name"] for s in sdk.submits] == ["geobrix-gen-corpus-t2-smoke"]
    assert [p for p, _ in sdk.uploads] == ["/Users/me@example.com/geobrix_gen_corpus_t2_smoke.ipynb"]


def test_main_stops_with_exit_4_when_the_smoke_run_itself_ends_failed(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    sdk.smoke = ("FAILED", {})
    assert go(monkeypatch, launcher, tmp_path, *NOTEBOOK_ARGS) == 4
    assert "the smoke run ended TERMINATED/FAILED: boom" in capsys.readouterr().err and len(sdk.submits) == 1


def test_smoke_only_stops_after_the_smoke_and_no_smoke_goes_straight_to_the_run(monkeypatch, launcher, tmp_path, sdk, wheel):
    assert go(monkeypatch, launcher, tmp_path, "--smoke-only", *NOTEBOOK_ARGS) == 0
    assert len(sdk.submits) == 1 and sdk.submits[0]["run_name"].endswith("-smoke")

    sdk.submits.clear()
    sdk.gen = ("SUCCESS", good_payload(launcher, tmp_path))
    assert go(monkeypatch, launcher, tmp_path, "--no-smoke", *NOTEBOOK_ARGS) == 0
    assert len(sdk.submits) == 1 and not sdk.submits[0]["run_name"].endswith("-smoke")


def test_no_wait_submits_the_run_and_keeps_its_notebook(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    status = tmp_path / "status.md"
    assert go(monkeypatch, launcher, tmp_path, "--no-smoke", "--no-wait", "--run-id", "t3", "--status-file", str(status), *NOTEBOOK_ARGS) == 0
    assert len(sdk.submits) == 1 and sdk.deleted == []
    assert "--no-wait: run 11 submitted" in capsys.readouterr().out and "state: SUBMITTED" in status.read_text()


def test_a_failed_run_exits_1_and_keeps_its_notebook_for_a_repair(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    sdk.gen = ("FAILED", {})
    assert go(monkeypatch, launcher, tmp_path, "--no-smoke", "--run-id", "t4", *NOTEBOOK_ARGS) == launcher.EXIT_FAILED == 1
    assert "FAILED: boom" in capsys.readouterr().err and sdk.deleted == []


def test_a_sha256_difference_or_an_incomplete_count_fails_the_verification(monkeypatch, launcher, tmp_path, sdk, wheel, capsys):
    payload = good_payload(launcher, tmp_path, complete=3)
    payload["sha"]["0"]["sha256"] = "0" * 64
    sdk.gen = ("SUCCESS", payload)

    assert go(monkeypatch, launcher, tmp_path, "--no-smoke", *NOTEBOOK_ARGS) == 1

    err = capsys.readouterr().err
    assert "VERIFICATION FAILED" in err and "3 of 4 tiles complete" in err and "r0.tif sha256" in err


# ---- watch: run states, progress lines, the stall watchdog


def watching(launcher, w, *, label="gen", progress_s=0.0, stale_s=3600.0, status_file="", poll_s=0.0):
    from databricks.labs.gbx.bench import datagen_remote as dr

    return launcher.watch(w, 5, label=label, poll_s=poll_s, progress_s=progress_s, stale_s=stale_s,
                          status_file=status_file, dr=dr, bench_run_id="r1")  # fmt: skip


class ScriptedJobs:
    """get_run answers the scripted runs in order (the last repeats); export_run the scripted views."""

    def __init__(self, runs, views=(), export_error=None):
        self.runs, self.views, self.export_error = list(runs), list(views), export_error
        self.polls, self.exports, self.cancelled = 0, 0, []

    def get_run(self, rid):
        run = self.runs[min(self.polls, len(self.runs) - 1)]
        self.polls += 1
        if self.polls > 500:  # a watch that never gives up must fail the test, not hang it
            return _run("TERMINATED", "FAILED")
        if isinstance(run, Exception):
            raise run
        return run

    def export_run(self, run_id, views_to_export):
        self.exports += 1
        if self.export_error:
            raise self.export_error
        html = self.views[min(self.exports - 1, len(self.views) - 1)]
        return SimpleNamespace(views=[SimpleNamespace(content=html)])

    def cancel_run(self, rid):
        self.cancelled.append(rid)


@pytest.mark.parametrize("result", ["SUCCESS", "FAILED", "TIMEDOUT"])
def test_watch_returns_the_run_when_it_terminates_with_any_result(launcher, sdk, capsys, result):
    jobs = ScriptedJobs([_run("PENDING"), _run("RUNNING"), _run("TERMINATED", result)], views=[_html("[GENCORPUS] start 4 tiles")])
    run, stalled = watching(launcher, SimpleNamespace(jobs=jobs))
    assert stalled is False and jobs.cancelled == []
    assert launcher._life_result(run) == ("TERMINATED", result)
    out = capsys.readouterr().out
    assert "[gen PENDING" in out and "[gen RUNNING" in out


@pytest.mark.parametrize("life", ["SKIPPED", "INTERNAL_ERROR"])
def test_watch_also_ends_on_a_skipped_or_internal_error_run(launcher, sdk, life):
    run, stalled = watching(launcher, SimpleNamespace(jobs=ScriptedJobs([_run(life)])))
    assert stalled is False and launcher._life_result(run)[0] == life


def test_watch_prints_the_latest_progress_line_decoded_from_the_export_notebook_model_and_writes_the_status(launcher, sdk, tmp_path, capsys):
    view = _html("[GENCORPUS] start 8 tiles", "[GENCORPUS] progress: done 3/8 (written 3, skipped 0) elapsed 4s")
    jobs = ScriptedJobs([_run("RUNNING"), _run("TERMINATED", "SUCCESS")], views=[view])
    status = tmp_path / "status.md"

    watching(launcher, SimpleNamespace(jobs=jobs), status_file=str(status))

    assert "[gen RUNNING 0s] [GENCORPUS] progress: done 3/8 (written 3, skipped 0) elapsed 4s" in capsys.readouterr().out
    text = status.read_text()
    assert "state: gen RUNNING" in text and "latest progress: [GENCORPUS] progress: done 3/8" in text
    assert jobs.exports == 1 and jobs.polls == 2


def test_watch_survives_a_failed_export_and_a_transient_poll_error(launcher, sdk, capsys):
    jobs = ScriptedJobs([OSError("reset"), _run("RUNNING"), _run("TERMINATED", "SUCCESS")], export_error=RuntimeError("no view"))
    run, stalled = watching(launcher, SimpleNamespace(jobs=jobs))
    cap = capsys.readouterr()
    assert not stalled and launcher._life_result(run)[1] == "SUCCESS"
    assert "(poll error: reset)" in cap.err and "(export-run: RuntimeError)" in cap.out


def test_watch_cancels_a_run_whose_progress_line_stops_changing(launcher, sdk, capsys):
    jobs = ScriptedJobs([_run("RUNNING")], views=[_html("[GENCORPUS] progress: done 3/8 (written 3)")])
    run, stalled = watching(launcher, SimpleNamespace(jobs=jobs), stale_s=0.05, poll_s=0.02)
    assert stalled is True and jobs.cancelled == [5]
    assert "STALLED: no new progress line" in capsys.readouterr().err


def test_watch_does_not_cancel_a_run_whose_progress_line_keeps_changing(launcher, sdk):
    views = [_html(f"[GENCORPUS] progress: done {i}/9 (written {i})") for i in range(1, 9)]
    runs = [_run("RUNNING")] * 8 + [_run("TERMINATED", "SUCCESS")]
    jobs = ScriptedJobs(runs, views=views)
    run, stalled = watching(launcher, SimpleNamespace(jobs=jobs), stale_s=0.5, poll_s=0.01)
    assert stalled is False and jobs.cancelled == [] and jobs.exports == 8
