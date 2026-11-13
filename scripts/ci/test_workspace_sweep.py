"""The deferred workspace cleanup (examples/tests/_workspace_sweep.py, gbx:workspace:sweep).

A launcher keeps a run's notebooks when the run ends, so the run page still opens them. The sweep removes
them once they are older than the window (60 min; 24 h for a run that did not succeed). A fake workspace
records every call: nothing here talks to Databricks.
"""

import importlib
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest
from conftest import REPO_ROOT

ME = "me@example.com"
ROOT = f"/Users/{ME}/GeoBrix"
NOW = 2_000_000_000.0
MIN = 60.0
HOST = "https://example.cloud.databricks.com"


class NotFound(Exception):
    pass


class FakeW:
    """workspace (list, get_status, delete), files (a Volume tree), jobs.list_runs: as much as the sweep uses."""

    def __init__(self):
        self.objects = {}  # path -> (object_type, modified_at in ms or None)
        self.volume = set()  # Volume file paths
        self.active = []  # notebook paths of active runs
        self.deleted = []
        self.list_runs_error = None
        self.config = SimpleNamespace(host=HOST)
        w = self

        class Workspace:
            def list(self, path, recursive=False):
                if path not in w.objects or w.objects[path][0] != "DIRECTORY":
                    raise NotFound(f"Path ({path}) doesn't exist.")
                kids = [p for p in w.objects if p.rsplit("/", 1)[0] == path]
                return [w._info(p) for p in sorted(kids)]

            def get_status(self, path):
                if path not in w.objects:
                    raise NotFound(f"Path ({path}) doesn't exist.")
                return w._info(path)

            def delete(self, path, recursive=False):
                kids = [p for p in w.objects if p.startswith(path + "/")]
                if kids and not recursive:
                    raise RuntimeError(f"{path} is not empty")
                w.deleted.append(path)
                for p in kids + [path]:
                    w.objects.pop(p, None)

        class Files:
            def list_directory_contents(self, path):
                seen = {}
                for f in w.volume:
                    if f.startswith(path + "/"):
                        head = f[len(path) + 1 :].split("/", 1)
                        seen[f"{path}/{head[0]}"] = len(head) > 1
                if not seen:
                    raise NotFound(path)
                return [SimpleNamespace(path=p, is_directory=d) for p, d in sorted(seen.items())]

            def delete(self, path):
                w.deleted.append(path)
                w.volume.discard(path)

            def delete_directory(self, path):
                w.deleted.append(path + "/")

        class Jobs:
            def list_runs(self, active_only=False, expand_tasks=False, **kw):
                assert active_only and expand_tasks
                if w.list_runs_error:
                    raise w.list_runs_error
                return [
                    SimpleNamespace(tasks=[SimpleNamespace(notebook_task=SimpleNamespace(notebook_path=p), spark_python_task=None)])
                    for p in w.active
                ]

        self.workspace, self.files, self.jobs = Workspace(), Files(), Jobs()
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name=ME))

    def _info(self, path):
        kind, mtime = self.objects[path]
        return SimpleNamespace(path=path, object_type=SimpleNamespace(value=kind), modified_at=mtime)

    def add(self, path, age_min=None, kind="NOTEBOOK"):
        """An object and the folders above it; ``age_min`` is how long ago it was modified."""
        parts = path.strip("/").split("/")
        for i in range(1, len(parts)):
            self.objects.setdefault("/" + "/".join(parts[:i]), ("DIRECTORY", None))
        self.objects[path] = (kind, None if age_min is None else int((NOW - age_min * MIN) * 1000))
        return path


@pytest.fixture
def sw(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_WS_SWEEP_LEDGER", str(tmp_path / "ledger.jsonl"))
    monkeypatch.delenv("GBX_WS_SWEEP_AT_LAUNCH", raising=False)
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    sys.modules.pop("_workspace_sweep", None)
    return importlib.import_module("_workspace_sweep")


def _sweep(sw, w, **kw):
    lines = []
    kw.setdefault("now", NOW)
    report = sw.sweep(w, me=ME, out=lines.append, **kw)
    return report, "\n".join(lines)


# ---- the window ----------------------------------------------------------------------------------------------------


def test_an_item_younger_than_the_window_is_kept(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=30)
    report, out = _sweep(sw, w)
    assert w.deleted == [] and nb in w.objects
    assert re.search(rf"KEPT {re.escape(nb)}: younger than 60 min", out)


def test_an_item_older_than_the_window_is_deleted_and_named(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=61)
    tst = w.add(f"{ROOT}/runners/tests_r2_serverless.ipynb", age_min=90)
    report, out = _sweep(sw, w)
    assert sorted(w.deleted) == sorted([nb, tst])
    assert f"DELETED {nb}" in out and f"DELETED {tst}" in out
    assert sorted(report.deleted) == sorted([nb, tst])


def test_older_than_moves_the_window(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=30)
    _sweep(sw, w, older_than_s=20 * MIN)
    assert w.deleted == [nb]


def test_dry_run_deletes_nothing_and_says_what_it_would(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=120)
    report, out = _sweep(sw, w, dry_run=True)
    assert w.deleted == [] and nb in w.objects
    assert f"WOULD DELETE {nb}" in out and report.deleted == [nb]


# ---- the prefixes ---------------------------------------------------------------------------------------------------


def test_an_item_outside_the_prefixes_is_never_deleted(sw):
    w = FakeW()
    keep = [
        w.add(f"/Users/{ME}/geobrix_bench_runner_r1.ipynb", age_min=999),  # the home root, not GeoBrix/bench
        w.add(f"/Users/{ME}/Other/tests_r1_serverless.ipynb", age_min=999),  # a runner name in a folder not ours
        w.add(f"{ROOT}/bench/gbx_hs_diag", age_min=999),  # a hand-made notebook in the bench folder
        w.add(f"{ROOT}/eo-series/01_ingest", age_min=999),  # a user's own folder under GeoBrix
        w.add("/Users/other@example.com/GeoBrix/bench/geobrix_bench_runner_r1.ipynb", age_min=999),  # another user
        w.add("/Shared/GeoBrix/bench/geobrix_bench_runner_r1.ipynb", age_min=999),
    ]
    # a recorded path outside GeoBrix/ is refused too (a --ws-dir somewhere else)
    sw.record(w, [keep[0], keep[1], "/Shared/x"], scope="notebooks", now=NOW - 999 * MIN)
    report, out = _sweep(sw, w)
    assert w.deleted == []
    assert all(p in w.objects for p in keep)
    assert f"SKIPPED {keep[0]}: outside" in out


def test_the_tests_folder_from_gbx_runner_dir_is_a_prefix(sw):
    w = FakeW()
    rd = f"/Workspace{ROOT}/gbx-beta-dbr-v0.2.0/sample-data/tests"
    nb = w.add(f"{ROOT}/gbx-beta-dbr-v0.2.0/sample-data/tests/tests_r1_serverless.ipynb", age_min=120)
    other = w.add(f"{ROOT}/gbx-beta-dbr-v0.2.0/sample-data/tests/_stage_distinct_var_corpus", age_min=999)
    _sweep(sw, w, runner_dir=rd)
    assert w.deleted == [nb] and other in w.objects


def test_a_runner_dir_outside_geobrix_is_refused(sw):
    w = FakeW()
    nb = w.add(f"/Users/{ME}/elsewhere/tests_r1_serverless.ipynb", age_min=999)
    report, out = _sweep(sw, w, runner_dir=f"/Users/{ME}/elsewhere")
    assert w.deleted == [] and nb in w.objects and "outside" in out


def test_scope_limits_the_sweep_to_its_own_prefix(sw):
    w = FakeW()
    b = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=120)
    t = w.add(f"{ROOT}/runners/tests_r1_serverless.ipynb", age_min=120)
    _sweep(sw, w, scopes=("bench",))
    assert w.deleted == [b] and t in w.objects


# ---- active runs ----------------------------------------------------------------------------------------------------


def test_an_item_used_by_an_active_run_is_kept(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=300)
    w.active = [f"/Workspace{nb}"]  # a run may name it with the /Workspace prefix
    report, out = _sweep(sw, w)
    assert w.deleted == [] and f"KEPT {nb}: used by an active run" in out


def test_a_folder_holding_an_active_runs_notebook_is_kept(sw):
    w = FakeW()
    folder = f"{ROOT}/uc-it-0123456789ab"
    w.add(f"{folder}/uc_it_install", age_min=300)
    w.active = [f"{folder}/uc_it_install"]
    _sweep(sw, w)
    assert w.deleted == [] and folder in w.objects


def test_no_active_run_listing_means_nothing_is_deleted(sw):
    w = FakeW()
    w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=300)
    w.list_runs_error = RuntimeError("503 Service Unavailable")
    report, out = _sweep(sw, w)
    assert w.deleted == [] and report.error and "nothing deleted" in out


# ---- the ledger: run-end windows and recorded paths ---------------------------------------------------------------


def test_a_succeeded_run_is_kept_60_min_after_it_ends(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=300)  # imported 5 h ago; the run just ended
    sw.release(w, [nb], scope="bench", succeeded=True, now=NOW, out=lambda s: None)
    _sweep(sw, w, now=NOW + 59 * MIN)
    assert w.deleted == []
    _sweep(sw, w, now=NOW + 61 * MIN)
    assert w.deleted == [nb]


def test_a_failed_run_keeps_its_notebook_24_hours(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/runners/mutation_r1.ipynb", age_min=120)
    sw.release(w, [nb], scope="runners", succeeded=False, now=NOW, out=lambda s: None)
    report, out = _sweep(sw, w, now=NOW + 23 * 60 * MIN)
    assert w.deleted == [] and f"KEPT {nb}: held until" in out
    _sweep(sw, w, now=NOW + 25 * 60 * MIN)
    assert w.deleted == [nb]


def test_a_recorded_notebooks_serverless_import_is_swept_but_its_neighbours_are_not(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/serverless-run/probe_x", age_min=120)
    mine = w.add(f"{ROOT}/serverless-run/hand_made", age_min=999)  # never recorded: not the runner's
    sw.record(w, [nb], scope="notebooks", now=NOW - 120 * MIN)
    _sweep(sw, w)
    assert w.deleted == [nb] and mine in w.objects


def test_a_forgotten_import_is_never_swept_by_record(sw):
    """--keep-notebooks drops an earlier run's record of the same path."""
    w = FakeW()
    nb = w.add(f"{ROOT}/serverless-run/probe_x", age_min=120)
    sw.record(w, [nb], scope="notebooks", now=NOW - 120 * MIN)
    sw.forget(w, [nb])
    _sweep(sw, w)
    assert w.deleted == [] and nb in w.objects


def test_a_recorded_import_beside_an_active_run_is_kept(sw):
    """A notebooks-serverless dependency (a %run target) sits beside the notebook that runs it."""
    w = FakeW()
    dep = w.add(f"{ROOT}/eo-jobrun/config_nb", age_min=300)
    main = w.add(f"{ROOT}/eo-jobrun/01_main", age_min=300)
    sw.record(w, [dep, main], scope="notebooks", now=NOW - 300 * MIN)
    w.active = [main]
    _sweep(sw, w)
    assert w.deleted == []


def test_the_ledger_forgets_what_was_deleted_or_is_gone(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/serverless-run/probe_x", age_min=120)
    sw.record(w, [nb, f"{ROOT}/serverless-run/gone"], scope="notebooks", now=NOW - 120 * MIN)
    _sweep(sw, w)
    assert sw.load_ledger(HOST) == {}


def test_another_workspaces_record_is_left_alone(sw):
    w = FakeW()
    nb = w.add(f"{ROOT}/serverless-run/probe_x", age_min=120)
    other = FakeW()
    other.config.host = "https://other.cloud.databricks.com"
    sw.record(other, [nb], scope="notebooks", now=NOW - 120 * MIN)
    _sweep(sw, w)
    assert w.deleted == [] and len(sw.load_ledger(other.config.host)) == 1


def test_a_uc_gate_folder_is_removed_whole_after_its_window(sw):
    w = FakeW()
    folder = f"{ROOT}/uc-it-0123456789ab"
    w.add(f"{folder}/uc_it_install", age_min=120)
    young = f"{ROOT}/uc-it-ba9876543210"
    w.add(f"{young}/uc_it_install", age_min=10)
    _sweep(sw, w)
    assert w.deleted == [folder] and young in w.objects and f"{folder}/uc_it_install" not in w.objects


def test_a_uc_gate_folder_follows_the_hold_on_its_notebook(sw):
    w = FakeW()
    folder = f"{ROOT}/uc-it-0123456789ab"
    nb = w.add(f"{folder}/uc_it_install", age_min=120)
    sw.release(w, [nb], scope="notebooks", succeeded=False, now=NOW, out=lambda s: None)
    _sweep(sw, w)
    assert w.deleted == []


def test_a_failed_mutation_stage_on_the_volume_is_held_then_swept(sw):
    w = FakeW()
    stage = "/Volumes/c/s/v/mutation-runs/mut-r1"
    w.volume |= {f"{stage}/geobrix.whl", f"{stage}/out/rows.json"}
    sw.release(w, [stage], scope="runners", succeeded=False, kind="volume", now=NOW, out=lambda s: None)
    _sweep(sw, w, now=NOW + 60 * MIN)
    assert w.deleted == []
    _sweep(sw, w, now=NOW + 25 * 60 * MIN)
    assert f"{stage}/geobrix.whl" in w.deleted and f"{stage}/" in w.deleted and not w.volume


def test_a_recorded_volume_path_outside_a_stage_folder_is_refused(sw):
    w = FakeW()
    w.volume |= {"/Volumes/c/s/v/sample-data/geobrix.whl"}
    sw.release(w, ["/Volumes/c/s/v/sample-data"], scope="runners", succeeded=True, kind="volume", now=NOW - 999 * MIN, out=lambda s: None)
    report, out = _sweep(sw, w)
    assert w.deleted == [] and "outside" in out


def test_max_deletes_bounds_a_launch_time_sweep(sw):
    w = FakeW()
    for i in range(5):
        w.add(f"{ROOT}/bench/geobrix_bench_runner_r{i}.ipynb", age_min=120)
    _sweep(sw, w, max_deletes=2)
    assert len(w.deleted) == 2


def test_the_launch_time_sweep_never_fails_a_launch(sw, capsys):
    w = FakeW()
    w.list_runs_error = RuntimeError("boom")
    assert sw.sweep_at_launch(w, ("bench",)) is not None
    w.current_user = None  # a client that cannot even say who it is
    assert sw.sweep_at_launch(w, ("bench",)) is None
    assert "WARNING" in capsys.readouterr().err


def test_the_launch_time_sweep_removes_old_items_of_its_scope(sw, capsys):
    w = FakeW()
    old = w.add(f"{ROOT}/bench/geobrix_bench_runner_old.ipynb", age_min=3 * 60)
    sw.sweep_at_launch(w, ("bench",), now=NOW)
    assert w.deleted == [old] and old in capsys.readouterr().out


# ---- the launchers no longer delete at run end ---------------------------------------------------------------------


LAUNCHERS = [
    "push_and_run_tests_on_cluster.py",
    "push_and_run_bench_on_cluster.py",
    "push_and_run_mutation_on_cluster.py",
    "push_and_run_corpus_gen_on_serverless.py",
    "run_notebooks_serverless.py",
    "uc_integration.py",
]


@pytest.mark.parametrize("name", LAUNCHERS)
def test_a_launcher_no_longer_deletes_workspace_items_at_run_end(name):
    src = (REPO_ROOT / "examples" / "tests" / name).read_text()
    assert "workspace.delete(" not in src, f"{name} deletes a workspace item itself; record it for the sweep instead"


@pytest.mark.parametrize("name", LAUNCHERS)
def test_a_launcher_records_its_notebooks_and_sweeps_at_launch(name):
    src = (REPO_ROOT / "examples" / "tests" / name).read_text()
    if name == "uc_integration.py":  # the notebooks-serverless run it starts records and sweeps
        return
    assert "sweep_at_launch(" in src and ("release(" in src or "_release_notebook(" in src), name


def test_the_bench_launcher_keeps_and_records_the_notebook_of_a_finished_run(sw, monkeypatch, capsys):
    monkeypatch.setenv("GBX_WS_SWEEP_AT_LAUNCH", "0")
    bench = importlib.import_module("push_and_run_bench_on_cluster")
    w = FakeW()
    nb = w.add(f"{ROOT}/bench/geobrix_bench_runner_r1.ipynb", age_min=5)
    bench._release_notebook(w, nb, succeeded=True)
    assert w.deleted == [] and nb in w.objects
    assert nb in sw.load_ledger(HOST)
    assert "gbx:workspace:sweep" in capsys.readouterr().out


# ---- the command ----------------------------------------------------------------------------------------------------


CMD = REPO_ROOT / "scripts" / "commands" / "gbx-workspace-sweep.sh"


def test_the_command_prints_help():
    res = subprocess.run(["bash", str(CMD), "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    for opt in ("--profile", "--older-than", "--dry-run", "--scope", "--log", "--help"):
        assert opt in res.stdout, opt


def test_the_command_needs_a_profile():
    res = subprocess.run(["bash", str(CMD), "--dry-run"], capture_output=True, text=True)
    assert res.returncode == 2 and "--profile" in res.stderr


@pytest.mark.parametrize("text, secs", [("60m", 3600), ("90", 5400), ("2h", 7200), ("1d", 86400), ("45s", 45)])
def test_older_than_parses_minutes_hours_and_days(sw, text, secs):
    assert sw.parse_age(text) == secs


def test_a_bad_older_than_is_refused(sw):
    with pytest.raises(ValueError):
        sw.parse_age("soon")
