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
    state = SimpleNamespace(uploads=[], submits=[], deleted=[])

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
                    result_state=SimpleNamespace(value="SUCCESS"),
                ),
                tasks=[SimpleNamespace(run_id=12)],
                run_page_url="https://example/run/11",
            )

        def get_run_output(self, run_id):
            return SimpleNamespace(notebook_output=None)

    class WorkspaceClient:
        def __init__(self, profile=None, **kwargs):
            self.workspace = Workspace()
            self.jobs = Jobs()
            self.current_user = SimpleNamespace(
                me=lambda: SimpleNamespace(user_name="me@example.com")
            )
            self.warehouses = SimpleNamespace(list=lambda: [])
            self.libraries = SimpleNamespace(cluster_status=lambda cid: [])

    sdk_mod = types.ModuleType("databricks.sdk")
    sdk_mod.WorkspaceClient = WorkspaceClient
    service = types.ModuleType("databricks.sdk.service")
    compute = types.ModuleType("databricks.sdk.service.compute")
    compute.Environment = compute.Library = compute.ClusterSpec = _Rec
    compute.DataSecurityMode = SimpleNamespace(
        SINGLE_USER="SINGLE_USER", USER_ISOLATION="USER_ISOLATION"
    )
    jobs = types.ModuleType("databricks.sdk.service.jobs")
    jobs.JobEnvironment = jobs.SubmitTask = jobs.NotebookTask = _Rec
    jobs.Source = SimpleNamespace(WORKSPACE="WORKSPACE")
    workspace = types.ModuleType("databricks.sdk.service.workspace")
    workspace.ImportFormat = SimpleNamespace(JUPYTER="JUPYTER")
    service.compute, service.jobs, service.workspace = compute, jobs, workspace
    for name, mod in (
        ("databricks.sdk", sdk_mod),
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
        monkeypatch.setattr(sys, "argv", ["push_and_run_bench_on_cluster.py", *args])
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
    assert deps[0] == "/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"
    assert any(d.startswith("zarr") for d in deps) and deps[-1] == "markdown"
    assert not any(d.startswith("geobrix[") for d in deps)  # extras are expanded
    (_, notebook), = sdk.uploads
    install = "".join(notebook["cells"][0]["source"])
    assert "installed by the job environment" in install and "pip" not in install


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


def test_a_finished_run_removes_its_notebook(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--run-id", "t4") == 0

    (path, _), = sdk.uploads
    assert path.endswith("_t4.ipynb") and sdk.deleted == [path]


def test_resume_needs_the_run_id_it_resumes(launch, sdk):
    assert launch("--serverless", "--zarr-only", "--resume", "--no-wait") == 2
    assert not sdk.submits
