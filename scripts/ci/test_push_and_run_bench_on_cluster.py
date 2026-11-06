"""Tests for examples/tests/push_and_run_bench_on_cluster.py (the gbx:bench:cluster launcher).

No workspace and no real SDK: stub `databricks.sdk` modules record the notebook the launcher
uploads, the job it submits and what it deletes. Each row pins one fix the Zarr benchmark
runs needed: Serverless installs through the job environment, every launch gets its own
notebook, a job cluster carries its access mode and no retries, a finished run removes its
notebook, and --resume needs the run id it resumes.
"""

import importlib
import json
import os
import sys
import types
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT


class _Rec:
    """A stand-in for an SDK dataclass: keeps its keyword arguments as attributes."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


@pytest.fixture
def sdk(monkeypatch):
    """Stub databricks.sdk: `state.uploads`, `state.submits` and `state.deleted` record calls."""
    state = SimpleNamespace(uploads=[], submits=[], deleted=[], result="SUCCESS", staged=set(), profiles=[], files_error=None)

    class NotFound(OSError):  # databricks.sdk.errors.NotFound: a 404
        pass

    state.NotFound = NotFound

    class Files:
        def get_metadata(self, path):
            if state.files_error is not None:
                raise state.files_error
            if path not in state.staged:
                raise NotFound(f"{path} does not exist")
            return SimpleNamespace(content_length=1)

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
            return SimpleNamespace(run_id=11)

        def get_run(self, run_id):
            return SimpleNamespace(
                state=SimpleNamespace(
                    life_cycle_state=SimpleNamespace(value="TERMINATED"),
                    result_state=SimpleNamespace(value=state.result),
                ),
                tasks=[SimpleNamespace(run_id=12)],
                run_page_url="https://example/run/11",
            )

        def get_run_output(self, run_id):
            return SimpleNamespace(notebook_output=None)

    class WorkspaceClient:
        def __init__(self, profile=None, **kwargs):
            state.profiles.append(profile)
            self.files = Files()
            self.workspace = Workspace()
            self.jobs = Jobs()
            self.current_user = SimpleNamespace(
                me=lambda: SimpleNamespace(user_name="me@example.com")
            )
            self.warehouses = SimpleNamespace(list=lambda: [])
            self.libraries = SimpleNamespace(cluster_status=lambda cid: [])
            self.clusters = SimpleNamespace(get=lambda cid: SimpleNamespace(spark_version="19.x-scala2.13"))

    sdk_mod = types.ModuleType("databricks.sdk")
    sdk_mod.WorkspaceClient = WorkspaceClient
    errors_mod = types.ModuleType("databricks.sdk.errors")
    errors_mod.NotFound = NotFound
    service = types.ModuleType("databricks.sdk.service")
    compute = types.ModuleType("databricks.sdk.service.compute")
    compute.Environment = compute.Library = compute.ClusterSpec = _Rec
    compute.DataSecurityMode = SimpleNamespace(
        SINGLE_USER="SINGLE_USER", USER_ISOLATION="USER_ISOLATION"
    )
    compute.AwsAttributes = _Rec
    compute.AwsAvailability = SimpleNamespace(ON_DEMAND="ON_DEMAND")
    jobs = types.ModuleType("databricks.sdk.service.jobs")
    jobs.JobEnvironment = jobs.SubmitTask = jobs.NotebookTask = _Rec
    jobs.Source = SimpleNamespace(WORKSPACE="WORKSPACE")
    workspace = types.ModuleType("databricks.sdk.service.workspace")
    workspace.ImportFormat = SimpleNamespace(JUPYTER="JUPYTER")
    service.compute, service.jobs, service.workspace = compute, jobs, workspace
    for name, mod in (
        ("databricks.sdk", sdk_mod),
        ("databricks.sdk.errors", errors_mod),
        ("databricks.sdk.service", service),
        ("databricks.sdk.service.compute", compute),
        ("databricks.sdk.service.jobs", jobs),
        ("databricks.sdk.service.workspace", workspace),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    return state


@pytest.fixture
def launch(monkeypatch, sdk):
    """``launch(*args, **env) -> exit code``: run the launcher's main() with these args."""
    saved = dict(os.environ)
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    for key in list(os.environ):
        if key.startswith(("GBX_", "DATABRICKS_")) or key == "CLUSTER_ID":
            monkeypatch.delenv(key)
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "test")
    monkeypatch.setenv("GBX_BUNDLE_VOLUME_CATALOG", "c")
    monkeypatch.setenv("GBX_BUNDLE_VOLUME_SCHEMA", "s")
    monkeypatch.setenv("GBX_BUNDLE_VOLUME_NAME", "v")
    module = importlib.import_module("push_and_run_bench_on_cluster")

    def run(*args, **env):
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        # the launcher's hardening (the smoke stage, the local preflight) has its own rows in
        # test_run_watchdog.py / test_job_submit.py; these rows pin the fixes of the Zarr runs
        monkeypatch.setattr(
            sys,
            "argv",
            ["push_and_run_bench_on_cluster.py", "--no-smoke", "--skip-preflight", *args],
        )
        return module.main()

    yield run
    os.environ.clear()
    os.environ.update(saved)


def test_serverless_zarr_installs_the_wheel_and_extras_through_the_job_environment(
    launch, sdk
):
    """A pip install from the notebook reaches only the Serverless driver; the tasks import
    zarr and xarray, so the job environment must install the wheel and the extras' pins."""
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t1") == 0

    (env,) = sdk.submits[0]["environments"]
    deps = env.spec.dependencies
    assert deps[0].endswith("geobrix-0.6.0+env6-py3-none-any.whl")  # the env wheel
    assert any(d.startswith("zarr") for d in deps) and deps[-1] == "markdown"
    assert not any(d.startswith("geobrix[") for d in deps)  # extras are expanded
    (_, notebook), = sdk.uploads
    install = "".join(notebook["cells"][0]["source"])
    assert "installed by the job environment" in install and "pip" not in install


def _levels_scales_cell(sdk):
    """The ``LEVELS_SCALES = ...`` line of the one notebook the launch uploaded."""
    (_, notebook), = sdk.uploads
    lines = [l for c in notebook["cells"] for l in "".join(c["source"]).splitlines() if l.startswith("LEVELS_SCALES =")]
    assert len(lines) == 1
    return lines[0]


def test_a_levels_run_without_levels_scales_leaves_the_ladder_to_its_set(launch, sdk):
    """The launcher once defaulted --levels-scales to 1,4,16, so an IO run timed 1,4,16 and not IO's own 1,4,10.
    "Not given" must reach the leg as None; an explicit value (even 1,4,16) stays exact."""
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--no-wait") == 0
    assert _levels_scales_cell(sdk) == "LEVELS_SCALES = None"
    sdk.uploads.clear()
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--levels-scales", "1,4,16", "--no-wait") == 0
    assert _levels_scales_cell(sdk) == "LEVELS_SCALES = (1, 4, 16)"


def test_every_launch_gets_its_own_notebook(launch, sdk):
    """A job reads its notebook when it is submitted: two launches on one path ran the later
    launch's notebook. Without --run-id each launch has its own id, and the env override is
    a prefix."""
    for _ in range(2):
        assert launch("--serverless", "--zarr-only", "--no-wait") == 0
    paths = [path for path, _ in sdk.uploads]
    assert len(set(paths)) == 2
    assert all(p.startswith("/Users/me@example.com/geobrix_bench_runner_cluster-") for p in paths)

    sdk.uploads.clear()
    prefix = dict(GBX_BENCH_RUNNER_NOTEBOOK_PATH="/Users/me@example.com/nb.ipynb")
    for _ in range(2):
        assert launch("--serverless", "--zarr-only", "--no-wait", **prefix) == 0
    paths = [path for path, _ in sdk.uploads]
    assert len(set(paths)) == 2 and all(p.startswith("/Users/me@example.com/nb_") for p in paths)


@pytest.mark.parametrize(
    "access, mode, user",
    [
        ("dedicated", "SINGLE_USER", "me@example.com"),
        ("standard", "USER_ISOLATION", None),
    ],
)
def test_a_job_cluster_carries_its_access_mode_and_no_retries(launch, sdk, access, mode, user):
    args = ("--new-cluster", "--zarr-only", "--no-wait", "--access-mode", access)
    assert launch(*args, "--run-id", "t3") == 0

    (task,) = sdk.submits[0]["tasks"]
    assert task.max_retries == 0 and not hasattr(task, "existing_cluster_id")
    spec = task.new_cluster
    assert (spec.data_security_mode, spec.single_user_name) == (mode, user)
    assert (spec.spark_version, spec.node_type_id, spec.num_workers) == (
        "19.x-scala2.13",
        "m6gd.xlarge",
        1,
    )


@pytest.mark.parametrize("on_demand", [False, True])
def test_a_job_cluster_is_on_demand_when_asked(launch, sdk, on_demand):
    """``--on-demand`` puts every node of the job cluster (driver and workers) on demand,
    so no spot node is taken back during a measurement; without it the workspace decides."""
    args = ("--new-cluster", "--zarr-only", "--no-wait", "--num-workers", "2")
    assert launch(*args, *(["--on-demand"] if on_demand else []), "--run-id", "t4") == 0
    (task,) = sdk.submits[0]["tasks"]
    aws = task.new_cluster.aws_attributes
    if on_demand:
        assert (aws.availability, aws.first_on_demand) == ("ON_DEMAND", 3)
    else:
        assert aws is None


@pytest.mark.parametrize("other", [["--serverless"], ["--existing-cluster-id", "0101-x"]])
def test_on_demand_without_a_job_cluster_is_refused(launch, sdk, other):
    assert launch("--on-demand", "--zarr-only", "--no-wait", *other, "--run-id", "t5") == 2
    assert sdk.submits == []


@pytest.mark.parametrize(
    "extra, env",
    [(["--env-version", "5"], 5), (["--env-version", "6"], 6)],
)
def test_a_custom_runtime_takes_the_env_family_from_env_version(launch, sdk, extra, env):
    """A DCS image's spark_version (custom:...) has no leading runtime number: --env-version decides."""
    args = ("--new-cluster", "--spark-version", "custom:dcs-image", "--zarr-only", "--no-wait", *extra)
    assert launch(*args, "--run-id", "t6") == 0
    (task,) = sdk.submits[0]["tasks"]
    assert task.new_cluster.spark_version == "custom:dcs-image"
    (_, notebook), = sdk.uploads
    assert f"+env{env}-py3-none-any.whl" in json.dumps(notebook)


def test_a_custom_runtime_without_env_version_is_one_clear_line_not_a_traceback(launch, sdk, capsys):
    args = ("--new-cluster", "--spark-version", "custom:dcs-image", "--zarr-only", "--no-wait", "--run-id", "t7")
    assert launch(*args) == 2
    err = capsys.readouterr().err
    assert "custom:dcs-image" in err and "--env-version" in err and "Traceback" not in err
    assert sdk.submits == [] and sdk.uploads == []


def test_a_finished_run_removes_its_notebook(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--run-id", "t4") == 0

    (path, _), = sdk.uploads
    assert path.endswith("_t4.ipynb") and sdk.deleted == [path]


def test_a_failed_run_keeps_its_notebook_for_a_repair_run(launch, sdk):
    sdk.result = "FAILED"
    assert launch("--serverless", "--zarr-only", "--run-id", "t5") == 1

    (path, _), = sdk.uploads
    assert path.endswith("_t5.ipynb") and sdk.deleted == []


def test_a_job_cluster_without_the_jar_refuses_the_heavyweight_tier(launch, sdk, capsys):
    assert launch("--new-cluster", "--no-wait", "--run-id", "t6") == 2
    err = capsys.readouterr().err
    assert "without the GeoBrix JAR" in err and "--lightweight-only" in err
    assert sdk.submits == [] and sdk.uploads == []


def test_resume_needs_the_run_id_it_resumes(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--resume", "--no-wait") == 2
    assert not sdk.submits


# ---- the staged wheel and the profile


VERSIONED = "/Volumes/c/s/v/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl"
FLAT = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"


def test_the_launcher_finds_the_versioned_staged_wheel_without_a_path_by_hand(launch, sdk):
    """`gbx:data:stage-wheel --env 6 --versioned` puts the wheel under <volume>/geobrix/<version>/env6/; the launcher
    used to look only at the flat <volume>/ path, so a run needed GBX_BUNDLE_WHEEL_VOLUME_PATH set to the real one."""
    sdk.staged.add(VERSIONED)
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t1") == 0
    assert sdk.submits[0]["environments"][0].spec.dependencies[0] == VERSIONED


def test_without_a_versioned_wheel_the_launcher_keeps_the_flat_path_and_an_explicit_path_wins(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t1") == 0
    assert sdk.submits[0]["environments"][0].spec.dependencies[0] == FLAT
    sdk.staged.add(VERSIONED)
    mine = "/Volumes/c/s/v/mine/geobrix-0.6.0+env6-py3-none-any.whl"
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t2", GBX_BUNDLE_WHEEL_VOLUME_PATH=mine) == 0
    assert sdk.submits[1]["environments"][0].spec.dependencies[0] == mine


def test_a_failed_wheel_lookup_that_is_not_a_not_found_surfaces_instead_of_reading_as_absent(launch, sdk, capsys):
    """A 401/403 or a network error used to print "no versioned wheel" and take the flat path."""

    class PermissionDenied(Exception):
        pass

    sdk.files_error = PermissionDenied("403: cannot read the volume")
    with pytest.raises(PermissionDenied):
        launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t1")
    out = capsys.readouterr()
    assert "wheel lookup failed (PermissionDenied: 403: cannot read the volume)" in out.err + out.out
    assert "no versioned wheel" not in out.err + out.out
    assert sdk.submits == []


def test_profile_option_beats_the_environment_variable(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t1") == 0
    assert launch("--serverless", "--zarr-only", "--no-wait", "--run-id", "t2", "--profile", "oauth-fe") == 0
    assert sdk.profiles == ["test", "oauth-fe"]  # the env var (test) is only the default


# ---- fail fast: the notebook's bars act before the watchdog's caps


def _stall_cap(sdk):
    (_, notebook), = sdk.uploads
    lines = [l for c in notebook["cells"] for l in "".join(c["source"]).splitlines() if l.startswith("STALL_CAP_S =")]
    assert len(lines) == 1
    return int(lines[0].split("=")[1])


def test_the_watchdog_cap_sits_above_the_notebooks_rung_bars_when_fail_fast_is_on(launch, sdk, capsys):
    """The watchdog once cancelled the whole run at 15 min on a 1x form while the notebook's rung cap was 20: the
    in-notebook bars (skip, over-bar, setup-bound) never got to act. With fail-fast the 1x cap is raised to the rung
    cap + the setup cap + 5 min (20 + 10 + 5 = 35 min); --no-fail-fast keeps today's cap."""
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--no-wait") == 0
    assert _stall_cap(sdk) == 35 * 60
    assert "NOTE" not in capsys.readouterr().err  # the cap is derived, not a warning
    sdk.uploads.clear()
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--rung-cap-min", "10", "--setup-cap-min", "20", "--no-wait") == 0
    assert _stall_cap(sdk) == 25 * 60  # the setup headroom is held to the rung cap: 10 + 10 + 5
    sdk.uploads.clear()
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--no-fail-fast", "--no-wait") == 0
    assert _stall_cap(sdk) == 15 * 60


def test_the_form_cap_ceiling_is_raised_to_the_bars_when_it_is_below_them(launch, sdk, capsys):
    from push_and_run_bench_on_cluster import effective_form_caps

    assert effective_form_caps(15, 60, True, 20, 10) == (35, 60, None)
    # a rung can run its setup cap and then its iterations to the rung cap: 2 x 20 + 5 at the largest scales
    cap, ceiling, note = effective_form_caps(15, 30, True, 20, 10)
    assert (cap, ceiling) == (35, 45) and "NOTE" in note and "--form-cap-max-min" in note
    assert effective_form_caps(15, 30, False, 20, 10) == (15, 30, None)  # today's caps
    assert effective_form_caps(45, 60, True, 20, 10) == (45, 60, None)  # a higher form cap wins
    # at every scale the watchdog's cap (1x cap x scale, held to the ceiling) stays above that rung's own bar
    for rung, setup in ((20, 10), (10, 20), (30, 5), (5, 60)):
        cap, ceiling, _ = effective_form_caps(15, 60, True, rung, setup)
        for scale in (1, 4, 16):
            bar = min(setup * scale, rung) + rung + 5
            assert min(cap * scale, ceiling) >= bar, (rung, setup, scale)
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--form-cap-max-min", "30", "--no-wait") == 0
    assert "NOTE" in capsys.readouterr().err


def test_a_levels_form_table_that_cannot_load_leaves_the_set_names_unfiltered_and_the_path_as_it_was(monkeypatch, capsys):
    """Importing levels_forms installs and loads the manifest: any failure of it, not only a missing pyspark, falls
    back to every manifest family (with a note), and the source path the launcher inserted is taken out again."""
    from push_and_run_bench_on_cluster import LEVELS_LETTER_SETS, levels_set_names

    src = str(REPO_ROOT / "python/geobrix/src")
    monkeypatch.syspath_prepend(src)
    import databricks.labs.gbx.bench as bench

    class Broken:
        FORMS = SimpleNamespace(get=lambda k: (_ for _ in ()).throw(RuntimeError("manifest did not load")))

    monkeypatch.setitem(sys.modules, "databricks.labs.gbx.bench.levels_forms", Broken)
    monkeypatch.setattr(bench, "levels_forms", Broken, raising=False)
    sys.path.remove(src)
    before = list(sys.path)
    names = levels_set_names(REPO_ROOT)
    assert names[: len(LEVELS_LETTER_SETS)] == LEVELS_LETTER_SETS and len(names) > len(LEVELS_LETTER_SETS)
    assert sys.path == before  # the inserted path is gone
    assert "manifest did not load" in capsys.readouterr().err


# ---- warmup skip, split by function


def _grab(sdk, name):
    (_, notebook), = sdk.uploads
    lines = [l for c in notebook["cells"] for l in "".join(c["source"]).splitlines() if l.startswith(f"{name} =")]
    assert len(lines) == 1
    return lines[0].split("=")[1].strip()


def test_the_warmup_skip_threshold_reaches_the_notebook(launch, sdk, capsys):
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--no-wait") == 0
    assert _grab(sdk, "WARMUP_SKIP_S") == "120.0"
    sdk.uploads.clear()
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--warmup-skip-s", "45", "--no-wait") == 0
    assert _grab(sdk, "WARMUP_SKIP_S") == "45.0"
    sdk.uploads.clear()
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--warmup-skip-s", "-1", "--no-wait") == 2


def test_split_by_fn_launches_one_job_per_function_with_its_own_run_id_and_forms(launch, sdk, monkeypatch):
    import push_and_run_bench_on_cluster as mod

    groups = {"gbx_rst_merge_agg": ["c0", "tvf"], "gbx_rst_bng_rasterize_agg": ["c0"]}
    monkeypatch.setattr(mod, "levels_functions", lambda mvp, root, only=(): groups)
    started = []

    class Proc:
        def __init__(self, cmd):
            started.append(cmd[2:])
            self.code = 3 if "bng_rasterize" in " ".join(cmd) else 0

        def wait(self):
            return self.code

    monkeypatch.setattr(mod.subprocess, "Popen", Proc)
    monkeypatch.setattr(sys, "argv", ["push_and_run_bench_on_cluster.py", "--levels", "IO", "--levels-only", "--run-id", "r1",
                                      "--levels-forms", "ignored", "--split-by", "fn", "--no-wait"])
    assert mod.main() == 3  # the worst exit code of the launches
    assert len(started) == 2 and all("--split-by" not in a for a in started)
    first, second = started
    assert first[first.index("--run-id") + 1] == "r1-gbx_rst_merge_agg" and first[first.index("--levels-forms") + 1] == "c0,tvf"
    assert second[second.index("--run-id") + 1] == "r1-gbx_rst_bng_rasterize_agg" and second[second.index("--levels-forms") + 1] == "c0"
    assert first.count("--run-id") == 1 and first.count("--levels-forms") == 1  # the originals are replaced, not repeated


def test_without_split_by_a_levels_run_is_one_job(launch, sdk, monkeypatch):
    assert launch("--serverless", "--levels", "IO", "--levels-only", "--no-wait") == 0
    assert len(sdk.submits) == 1
    import push_and_run_bench_on_cluster as mod

    monkeypatch.setattr(sys, "argv", ["x", "--levels", "IO", "--split-by", "other"])
    assert mod.main() == 2


# ---- --pool: a leased cluster, the jars the run needs, the job run on the lease


class _FakeLease:
    def __init__(self, log):
        self.cluster_id, self.keep, self.log = "0101-leased", False, log

    def sync_jars(self, jars):
        self.log.append(("sync", list(jars)))
        return {"action": "ok", "fingerprint": "abc"}

    def job_run(self, run_id):
        self.log.append(("job_run", run_id))


@pytest.fixture
def leased(monkeypatch, launch):
    import contextlib

    import _cluster_pool

    log = []

    @contextlib.contextmanager
    def fake(w, name, volroot, info=None, **kw):
        log.append(("lease", name, dict(info or {})))
        lease = _FakeLease(log)
        try:
            yield lease
        finally:
            log.append(("released", lease.keep))

    monkeypatch.setattr(_cluster_pool, "leased_cluster", fake)
    return log


def test_pool_runs_on_the_leased_cluster_with_its_jars_synced_not_attached(launch, sdk, leased):
    assert launch("--pool", "dev", "--levels", "IO", "--levels-only", "--with-jar", "--run-id", "p1") == 0
    (submit,) = sdk.submits
    (task,) = submit["tasks"]
    assert task.existing_cluster_id == "0101-leased" and task.libraries is None and not hasattr(task, "new_cluster")
    kinds = [k[0] for k in leased]
    assert kinds[0] == "lease" and leased[0][1] == "dev" and leased[0][2]["run_id"] == "p1"
    (sync,) = [k for k in leased if k[0] == "sync"]
    assert any(j.endswith("-jar-with-dependencies.jar") for j in sync[1])  # the jar the run needs
    assert ("job_run", 11) in leased  # the lease follows the job
    assert leased[-1] == ("released", False)  # given back at the end of the run


def test_pool_with_no_wait_leaves_the_lease_to_the_job(launch, sdk, leased):
    assert launch("--pool", "dev", "--levels", "IO", "--levels-only", "--no-wait") == 0
    assert leased[-1] == ("released", True)  # keep: the job still uses the cluster, its run id is on the lease
    assert [k for k in leased if k[0] == "sync"][0][1] == []  # a light run wants no jar


def test_pool_is_exclusive_with_a_cluster_of_its_own(launch, sdk, leased):
    for other in (("--new-cluster",), ("--existing-cluster-id", "x"), ("--serverless",)):
        assert launch("--pool", "dev", *other) == 2
    assert not leased and not sdk.submits


def test_a_pool_with_no_cluster_free_is_one_clear_line(launch, sdk, monkeypatch, capsys):
    import _cluster_pool

    def none_free(*a, **k):
        raise _cluster_pool.PoolError("every cluster of pool 'dev' is leased; none came free in 60s")

    monkeypatch.setattr(_cluster_pool, "lease_cluster", none_free)
    assert launch("--pool", "dev", "--lightweight-only", "--no-wait") == 2
    assert "every cluster of pool 'dev' is leased" in capsys.readouterr().err and not sdk.submits
