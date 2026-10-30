"""Tests for _job_submit: every job launcher submits each logical run exactly once.

Pure (no workspace, no databricks-sdk, no Spark). Run:
  gbx:test:notebooks --path test_job_submit.py

The fake workspace models the one server behaviour that matters: the Jobs API creates a
run for every submit, except that a repeated ``idempotency_token`` returns the run the
first request made. "Lost response" = the server accepted the request, then the client
saw an error and retried.
"""

import ast
import pathlib
import re
import sys
import types
from itertools import count
from types import SimpleNamespace

import _job_submit
import pytest
import run_notebooks_serverless as rns

TESTS_DIR = pathlib.Path(__file__).resolve().parent


class DatabricksError(OSError):
    """Stand-in for databricks.sdk.errors.DatabricksError (an OSError subclass)."""


class TemporarilyUnavailable(DatabricksError):
    pass


class InvalidParameterValue(DatabricksError):
    pass


class FakeJobs:
    """``jobs`` API of a workspace client whose server accepts every submit."""

    def __init__(self, fail_after_accept=0):
        self.runs = {}  # run_id -> submit kwargs
        self.by_token = {}  # idempotency_token -> run_id
        self.calls = []  # kwargs of every submit call
        self.fail_after_accept = fail_after_accept
        self._ids = count(1000)

    def submit(self, **kwargs):
        self.calls.append(kwargs)
        token = kwargs.get("idempotency_token")
        if token is not None and token in self.by_token:
            run_id = self.by_token[token]  # the server returns the existing run
        else:
            run_id = next(self._ids)
            self.runs[run_id] = kwargs
            if token is not None:
                self.by_token[token] = run_id
        if self.fail_after_accept > 0:
            self.fail_after_accept -= 1
            raise ConnectionError("connection reset: the response was lost")
        return SimpleNamespace(run_id=run_id)

    # the polling surface run_one uses once the run exists
    def get_run(self, run_id):
        return SimpleNamespace(
            run_page_url=f"https://ws.example/run/{run_id}",
            state=SimpleNamespace(
                life_cycle_state="TERMINATED", result_state="SUCCESS", state_message=""
            ),
            tasks=[],
        )


def fake_workspace(**kwargs):
    return SimpleNamespace(jobs=FakeJobs(**kwargs))


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr(_job_submit.time, "sleep", lambda s: None)


# --------------------------------------------------------------------------- the helper


def test_lost_response_is_retried_with_the_same_token_and_makes_one_run():
    w = fake_workspace(fail_after_accept=1)

    waiter = _job_submit.submit_once(w, timeout_seconds=60, run_name="nb", tasks=[])

    assert len(w.jobs.runs) == 1
    assert waiter.run_id in w.jobs.runs
    assert len(w.jobs.calls) == 2
    tokens = {c["idempotency_token"] for c in w.jobs.calls}
    assert len(tokens) == 1


def test_without_a_token_a_lost_response_makes_two_runs():
    """The defect, on the same fake server: a retry of a token-less submit duplicates."""
    w = fake_workspace(fail_after_accept=1)

    with pytest.raises(ConnectionError):
        w.jobs.submit(run_name="nb", tasks=[])
    w.jobs.submit(run_name="nb", tasks=[])  # the "retry"

    assert len(w.jobs.runs) == 2


def test_each_logical_run_gets_its_own_token():
    w = fake_workspace()

    a = _job_submit.submit_once(w, timeout_seconds=60, run_name="nb", tasks=[])
    b = _job_submit.submit_once(w, timeout_seconds=60, run_name="nb", tasks=[])

    assert a.run_id != b.run_id and len(w.jobs.runs) == 2
    t1, t2 = (c["idempotency_token"] for c in w.jobs.calls)
    assert t1 != t2
    assert 0 < len(t1) <= 64 and 0 < len(t2) <= 64  # the Jobs API limit


def test_a_supplied_token_is_used_as_given():
    w = fake_workspace()

    _job_submit.submit_once(w, timeout_seconds=60, idempotency_token="my-run-1", run_name="nb")

    assert w.jobs.calls[0]["idempotency_token"] == "my-run-1"


def test_submit_arguments_pass_through_unchanged():
    w = fake_workspace()

    _job_submit.submit_once(w, run_name="nb", timeout_seconds=60, tasks=["t"])

    call = dict(w.jobs.calls[0])
    call.pop("idempotency_token")
    assert call == {"run_name": "nb", "timeout_seconds": 60, "tasks": ["t"]}


# --------------------------------------------------------------------------- the time limit


@pytest.mark.parametrize("bad", [None, 0, -5, 1.5, "60", True])
def test_a_submit_with_no_usable_time_limit_is_refused_before_any_call(bad):
    """A run with no limit can hang for hours; the Jobs API reads 0 as "no limit" too."""
    w = fake_workspace()
    kw = {} if bad is None else {"timeout_seconds": bad}

    with pytest.raises(ValueError, match="timeout_seconds"):
        _job_submit.submit_once(w, run_name="nb", **kw)

    assert w.jobs.calls == []


def test_the_limit_goes_on_every_task_that_sets_none_and_not_on_one_that_does():
    w = fake_workspace()
    plain = SimpleNamespace(task_key="a", timeout_seconds=None)  # a jobs.SubmitTask's shape
    own = SimpleNamespace(task_key="b", timeout_seconds=30)

    _job_submit.submit_once(w, timeout_seconds=600, run_name="nb", tasks=[plain, own])

    sent = w.jobs.calls[0]
    assert sent["timeout_seconds"] == 600
    assert (plain.timeout_seconds, own.timeout_seconds) == (600, 30)


def test_a_permanent_error_is_not_retried():
    attempts = []

    def rejected(**kw):
        attempts.append(kw["idempotency_token"])
        raise InvalidParameterValue("bad task spec")

    w = SimpleNamespace(jobs=SimpleNamespace(submit=rejected))

    with pytest.raises(InvalidParameterValue):
        _job_submit.submit_once(w, timeout_seconds=60, run_name="nb")

    assert len(attempts) == 1


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError("reset"),
        TimeoutError("timed out"),
        TemporarilyUnavailable("503"),
    ],
    ids=["connection", "timeout", "temporarily-unavailable"],
)
def test_transient_errors_retry_with_one_token_until_attempts_run_out(exc):
    seen = []

    def always_fails(**kw):
        seen.append(kw["idempotency_token"])
        raise exc

    w = SimpleNamespace(jobs=SimpleNamespace(submit=always_fails))

    with pytest.raises(type(exc)):
        _job_submit.submit_once(w, timeout_seconds=60, attempts=3, run_name="nb")

    assert len(seen) == 3 and len(set(seen)) == 1


class HTTPError(OSError):
    """Stand-in for requests.HTTPError (an OSError); `response` is set when the server answered."""

    def __init__(self, status=None):
        super().__init__(f"HTTP {status}")
        self.response = None if status is None else SimpleNamespace(status_code=status)


@pytest.mark.parametrize(
    "status, retried",
    [(400, False), (403, False), (404, False), (408, True), (429, True), (503, True)],
)
def test_an_http_error_is_retried_only_when_the_server_verdict_can_change(
    status, retried
):
    assert _job_submit.is_transient(HTTPError(status)) is retried
    assert _job_submit.is_transient(HTTPError()) is True  # no response: no verdict


@pytest.mark.parametrize("status, retried", [(404, False), (503, True)])
def test_a_urllib_http_error_is_judged_by_its_code(status, retried):
    import urllib.error

    exc = urllib.error.HTTPError("http://x", status, "msg", {}, None)
    assert _job_submit.is_transient(exc) is retried


def test_attempts_must_be_positive():
    with pytest.raises(ValueError):
        _job_submit.submit_once(fake_workspace(), timeout_seconds=60, attempts=0, run_name="nb")


@pytest.mark.parametrize(
    "exc",
    [
        FileNotFoundError("no such file"),
        PermissionError("denied"),
        IsADirectoryError("is a directory"),
        NotADirectoryError("not a directory"),
        FileExistsError("exists"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_local_file_errors_are_not_retried(exc):
    """They are OSErrors, but no server verdict is missing: the same call fails the same way."""
    attempts = []

    def fails(**kw):
        attempts.append(1)
        raise exc

    w = SimpleNamespace(jobs=SimpleNamespace(submit=fails))

    with pytest.raises(type(exc)):
        _job_submit.submit_once(w, timeout_seconds=60, run_name="nb")

    assert len(attempts) == 1


# ------------------------------------------- where a launcher uploads its runner notebook


def _workspace_of(user_name, calls=None):
    """A workspace client whose current user is ``user_name`` (no network)."""

    def me():
        if calls is not None:
            calls.append("me")
        return SimpleNamespace(user_name=user_name)

    return SimpleNamespace(current_user=SimpleNamespace(me=me))


def test_the_runner_dir_defaults_to_the_callers_own_workspace_home(monkeypatch):
    monkeypatch.delenv("GBX_RUNNER_DIR", raising=False)

    path = _job_submit.runner_dir(_workspace_of("someone.else@example.com"))

    assert path.startswith("/Workspace/Users/someone.else@example.com/")
    assert path == "/Workspace/Users/someone.else@example.com/GeoBrix/runners"


@pytest.mark.parametrize("override", ["/Workspace/Users/me@x.com/mine", "/Shared/runners"])
def test_a_runner_dir_the_caller_passed_wins_and_asks_nobody(monkeypatch, override):
    monkeypatch.setenv("GBX_RUNNER_DIR", override)
    calls = []

    path = _job_submit.runner_dir(_workspace_of("someone.else@example.com", calls))

    assert path == override
    assert calls == []  # the current user is looked up only when it is needed


def test_an_empty_runner_dir_counts_as_not_passed(monkeypatch):
    monkeypatch.setenv("GBX_RUNNER_DIR", "")

    path = _job_submit.runner_dir(_workspace_of("a@b.com"))

    assert path.startswith("/Workspace/Users/a@b.com/")


def test_no_launcher_or_smoke_notebook_names_a_real_workspace_user():
    """A hard-coded home folder only works for its owner. The example env file's
    ``your-email@...`` placeholder and ``example.com`` addresses in usage text are fine."""
    pattern = re.compile(r"/Users/(?!your-email@)[\w.+-]+@databricks\.com")
    hits = [
        f"{path.name}:{n}"
        for path in sorted([*TESTS_DIR.glob("*.py"), *TESTS_DIR.glob("*.ipynb")])
        if path.name != pathlib.Path(__file__).name
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if pattern.search(line)
    ]
    assert hits == []


@pytest.mark.parametrize(
    "launcher",
    [
        "_netcdf_diag.py",
        "_stage_bench_corpora.py",
        "_stage_distinct_var_corpus.py",
        "_stage_s5p_netcdf_corpus.py",
        "netcdf_gdal_cluster_validate.py",
    ],
)
def test_the_launchers_that_upload_a_runner_notebook_ask_for_the_runner_dir(launcher):
    source = (TESTS_DIR / launcher).read_text(encoding="utf-8")

    assert "runner_dir(w)" in source
    assert 'environ.get("GBX_RUNNER_DIR"' not in source  # the override lives in the helper


# ------------------------------------------------------ the notebook runner end to end


@pytest.fixture
def fake_sdk(monkeypatch):
    """Minimal databricks.sdk.service.{jobs,compute}: containers for the submit spec."""

    def container(name):
        return type(name, (SimpleNamespace,), {})

    compute = types.ModuleType("databricks.sdk.service.compute")
    compute.Environment = container("Environment")
    compute.ClusterSpec = container("ClusterSpec")
    compute.Library = container("Library")
    compute.DataSecurityMode = SimpleNamespace(
        SINGLE_USER="SINGLE_USER", USER_ISOLATION="USER_ISOLATION"
    )
    jobs = types.ModuleType("databricks.sdk.service.jobs")
    for name in ("SubmitTask", "NotebookTask", "JobEnvironment", "Compute"):
        setattr(jobs, name, container(name))
    jobs.RunLifeCycleState = SimpleNamespace(
        TERMINATED="TERMINATED", SKIPPED="SKIPPED", INTERNAL_ERROR="INTERNAL_ERROR"
    )
    jobs.RunResultState = SimpleNamespace(SUCCESS="SUCCESS")
    service = types.ModuleType("databricks.sdk.service")
    service.compute, service.jobs = compute, jobs
    sdk = types.ModuleType("databricks.sdk")
    sdk.service = service
    for mod_name, mod in {
        "databricks.sdk": sdk,
        "databricks.sdk.service": service,
        "databricks.sdk.service.compute": compute,
        "databricks.sdk.service.jobs": jobs,
    }.items():
        monkeypatch.setitem(sys.modules, mod_name, mod)
    monkeypatch.setattr(rns, "_import_notebook", lambda *a, **k: "/ws/nb")


@pytest.mark.parametrize("existing_cluster_id", [None, "0101-000000-abcdefgh"])
def test_run_one_makes_one_run_when_the_first_submit_response_is_lost(
    fake_sdk, existing_cluster_id
):
    w = fake_workspace(fail_after_accept=1)

    ok = rns.run_one(
        w,
        pathlib.Path("my nb.ipynb"),
        "/ws",
        deps=[],
        env_version="6",
        poll_secs=0,
        strip_pip=True,
        existing_cluster_id=existing_cluster_id,
    )

    assert ok is True
    assert len(w.jobs.runs) == 1
    assert len({c["idempotency_token"] for c in w.jobs.calls}) == 1


@pytest.mark.parametrize(
    "access_mode, security, single_user",
    [("dedicated", "SINGLE_USER", "me@example.com"), ("standard", "USER_ISOLATION", None)],
)
def test_a_new_cluster_run_gets_a_job_cluster_in_its_access_mode(
    fake_sdk, access_mode, security, single_user
):
    w = fake_workspace()

    ok = rns.run_one(
        w,
        pathlib.Path("my nb.ipynb"),
        "/ws",
        deps=["geobrix.whl"],
        env_version="6",
        poll_secs=0,
        strip_pip=False,
        job_cluster=rns.JobCluster(access_mode=access_mode, user_name="me@example.com"),
    )

    assert ok is True
    (call,) = w.jobs.calls
    (task,) = call["tasks"]
    spec = task.new_cluster
    assert (spec.data_security_mode, spec.single_user_name) == (security, single_user)
    assert (spec.spark_version, spec.node_type_id, spec.num_workers) == (
        rns.DEFAULT_SPARK_VERSION,
        rns.DEFAULT_NODE_TYPE,
        1,
    )
    assert task.max_retries == 0
    assert not hasattr(task, "existing_cluster_id") and "environments" not in call


def _new_cluster_task(cluster):
    w = fake_workspace()
    assert rns.run_one(
        w,
        pathlib.Path("my nb.ipynb"),
        "/ws",
        deps=[],
        env_version="6",
        poll_secs=0,
        strip_pip=False,
        job_cluster=cluster,
    )
    (call,) = w.jobs.calls
    (task,) = call["tasks"]
    return task


def test_a_new_cluster_run_carries_the_jar_as_a_library_of_the_task(fake_sdk):
    jar = "/Volumes/c/s/v/geobrix-0.6.0-jar-with-dependencies.jar"

    task = _new_cluster_task(rns.JobCluster(user_name="me@example.com", jar=jar))

    (lib,) = task.libraries
    assert lib.jar == jar
    assert task.new_cluster is not None  # the library rides on the cluster made for the run


def test_a_new_cluster_run_without_a_jar_attaches_no_library(fake_sdk):
    task = _new_cluster_task(rns.JobCluster(user_name="me@example.com"))

    assert not hasattr(task, "libraries")


def _run_main(monkeypatch, tmp_path, *flags):
    """rns.main() on a fake workspace; returns (exit, the JobCluster the run was made for)."""
    nb = tmp_path / "a.ipynb"
    nb.write_text("{}")
    made = []
    w = fake_workspace()
    w.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@example.com"))
    sys.modules["databricks.sdk"].WorkspaceClient = lambda **k: w
    monkeypatch.setattr(rns, "run_one", lambda **kw: made.append(kw["job_cluster"]) or True)
    monkeypatch.setattr(sys, "argv", ["run_notebooks_serverless.py", "--notebook", str(nb), *flags])
    return rns.main(), made


def test_the_jar_option_reaches_the_cluster_made_for_a_new_cluster_run(fake_sdk, monkeypatch, tmp_path):
    jar = "/Volumes/c/s/v/j.jar"

    code, made = _run_main(monkeypatch, tmp_path, "--new-cluster", "--jar", jar)

    assert code == 0 and [c.jar for c in made] == [jar]
    code, made = _run_main(monkeypatch, tmp_path, "--new-cluster")
    assert code == 0 and [c.jar for c in made] == [None]


@pytest.mark.parametrize("other", [[], ["--existing-cluster-id", "0101-x"]])
def test_the_jar_option_without_a_new_cluster_or_jar_task_is_refused(fake_sdk, monkeypatch, tmp_path, other):
    """It would be silently ignored on Serverless and on a warm cluster: refuse it before any call."""
    with pytest.raises(SystemExit) as stop:
        _run_main(monkeypatch, tmp_path, "--jar", "/Volumes/c/s/v/j.jar", *other)

    assert stop.value.code == 2


def test_an_option_missing_its_value_stops_the_runner_before_any_workspace_call(fake_sdk, monkeypatch, tmp_path):
    """exit 2 (argparse) is a refusal of the command line: it comes before the first import, never after a submit.

    A bare ``--existing-cluster-id`` is what an unset shell variable leaves in
    ``--existing-cluster-id $CID --dep-notebook cfg.ipynb``; the runs that then finished were another invocation's.
    """
    reached = []
    nb, dep = tmp_path / "a.ipynb", tmp_path / "cfg.ipynb"
    nb.write_text("{}")
    dep.write_text("{}")
    sys.modules["databricks.sdk"].WorkspaceClient = lambda **k: reached.append("client")
    monkeypatch.setattr(rns, "_import_notebook", lambda *a, **k: reached.append("import"))
    monkeypatch.setattr(rns, "run_one", lambda **kw: reached.append("run") or True)
    monkeypatch.setattr(
        sys, "argv", ["run_notebooks_serverless.py", "--notebook", str(nb), "--dep-notebook", str(dep), "--existing-cluster-id"]
    )

    with pytest.raises(SystemExit) as stop:
        rns.main()

    assert stop.value.code == 2 and reached == []


def test_a_dep_notebook_run_exits_zero_after_it_submits(fake_sdk, monkeypatch, tmp_path):
    nb, dep = tmp_path / "a.ipynb", tmp_path / "cfg.ipynb"
    nb.write_text("{}")
    dep.write_text("{}")
    w = fake_workspace()
    w.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@example.com"))
    sys.modules["databricks.sdk"].WorkspaceClient = lambda **k: w
    for flags in ([], ["--new-cluster"], ["--existing-cluster-id", "0101-x"]):
        monkeypatch.setattr(
            sys, "argv", ["run_notebooks_serverless.py", "--notebook", str(nb), "--dep-notebook", str(dep), "--poll-secs", "0", *flags]
        )
        assert rns.main() == 0, flags
    assert len(w.jobs.runs) == 3


# ----------------------------------------------------- every launcher goes through it


# Where a launcher may live: the notebook/bench runners, scripts, and the bench package.
LAUNCHER_ROOTS = [
    TESTS_DIR,
    TESTS_DIR.parents[1] / "scripts",
    TESTS_DIR.parents[1] / "python/geobrix/src/databricks/labs/gbx/bench",
]
SUBMITS = ("submit", "submit_and_wait")


def _launcher_files(roots=None):
    files = []
    for root in roots or LAUNCHER_ROOTS:
        for path in root.rglob("*.py"):
            if path.name == "_job_submit.py" or path.name.startswith(
                ("test_", "_fake")
            ):
                continue
            if (
                path.name == "conftest.py"
                or "node_modules" in path.parts
                or "m2" in path.parts
            ):
                continue
            files.append(path)
    return sorted(files)


def _is_jobs_api(node):
    """``<expr>.jobs``."""
    return isinstance(node, ast.Attribute) and node.attr == "jobs"


def _direct_submit_lines(source):
    """Lines that call ``jobs.submit`` / ``jobs.submit_and_wait`` without submit_once.

    Catches ``w.jobs.submit(...)``, ``api = w.jobs; api.submit(...)`` and
    ``fn = w.jobs.submit; fn(...)``. A ``ThreadPool.submit`` is not a Jobs call.
    """
    tree = ast.parse(source)
    apis, bound = set(), set()  # names bound to ``<x>.jobs`` / to ``<x>.jobs.submit``
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
            if isinstance(target, ast.Name):
                if _is_jobs_api(value):
                    apis.add(target.id)
                elif (
                    isinstance(value, ast.Attribute)
                    and value.attr in SUBMITS
                    and (
                        _is_jobs_api(value.value)
                        or getattr(value.value, "id", None) in apis
                    )
                ):
                    bound.add(target.id)
    lines = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr in SUBMITS:
            if _is_jobs_api(f.value) or getattr(f.value, "id", None) in apis:
                lines.append(node.lineno)
        elif isinstance(f, ast.Name) and f.id in bound:
            lines.append(node.lineno)
    return lines


@pytest.mark.parametrize(
    "source",
    [
        "w.jobs.submit(run_name='x')",
        "self.client.jobs.submit(run_name='x')",
        "w.jobs.submit_and_wait(run_name='x')",
        "api = w.jobs\napi.submit(run_name='x')",
        "api = w.jobs\napi.submit_and_wait(run_name='x')",
        "fn = w.jobs.submit\nfn(run_name='x')",
        "api = w.jobs\nfn = api.submit\nfn(run_name='x')",
    ],
)
def test_the_guard_flags_every_way_of_calling_jobs_submit(source):
    assert _direct_submit_lines(source) != []


@pytest.mark.parametrize(
    "source",
    [
        "submit_once(w, run_name='x')",
        "pool.submit(work)",
        "ex = ThreadPoolExecutor()\nex.submit(work)",
        "w.jobs.get_run(run_id=1)",
    ],
)
def test_the_guard_ignores_other_submit_calls(source):
    assert _direct_submit_lines(source) == []


def test_the_guard_scans_subdirectories(tmp_path):
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    (nested / "launcher.py").write_text("w.jobs.submit(run_name='x')\n")
    (tmp_path / "test_x.py").write_text("w.jobs.submit()\n")

    files = _launcher_files([tmp_path])

    assert [f.name for f in files] == ["launcher.py"]


def test_the_guard_covers_the_known_launcher_roots():
    names = {f.name for f in _launcher_files()}
    assert {
        "run_notebooks_serverless.py",
        "stage_jar_to_volume.py",
        "datagen.py",
    } <= names


def test_no_launcher_calls_jobs_submit_directly():
    """A direct ``w.jobs.submit(...)`` has no idempotency token: a lost response duplicates."""
    direct = {
        str(p.relative_to(TESTS_DIR.parents[1])): _direct_submit_lines(p.read_text())
        for p in _launcher_files()
    }
    assert {name: lines for name, lines in direct.items() if lines} == {}


def test_the_known_launchers_use_submit_once():
    users = {
        p.name
        for p in _launcher_files()
        if any(
            isinstance(n, ast.Call)
            and getattr(n.func, "id", getattr(n.func, "attr", None)) == "submit_once"
            for n in ast.walk(ast.parse(p.read_text()))
        )
    }
    assert users >= {
        "_netcdf_diag.py",
        "_stage_bench_corpora.py",
        "_stage_distinct_var_corpus.py",
        "_stage_s5p_netcdf_corpus.py",
        "netcdf_gdal_cluster_validate.py",
        "push_and_run_bench_on_cluster.py",
        "push_and_run_bundle_on_cluster.py",
        "push_and_run_primitive_on_cluster.py",
        "run_notebooks_serverless.py",
        "serverless_light_smoke.py",
    }


def _submit_once_calls_without_a_limit(source):
    """Lines of ``submit_once(...)`` calls that pass no ``timeout_seconds=`` keyword."""
    return [
        n.lineno
        for n in ast.walk(ast.parse(source))
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", getattr(n.func, "attr", None)) == "submit_once"
        and not any(k.arg == "timeout_seconds" or k.arg is None for k in n.keywords)
    ]


def test_the_guard_flags_a_submit_once_with_no_limit():
    assert _submit_once_calls_without_a_limit("submit_once(w, run_name='x')") == [1]
    assert _submit_once_calls_without_a_limit("submit_once(w, timeout_seconds=9)") == []


def test_every_launcher_passes_a_time_limit_to_submit_once():
    """No ``jobs.submit`` path may omit the limit: a hung run must end on its own."""
    missing = {
        str(p.relative_to(TESTS_DIR.parents[1])): lines
        for p in _launcher_files()
        if (lines := _submit_once_calls_without_a_limit(p.read_text()))
    }
    assert missing == {}
