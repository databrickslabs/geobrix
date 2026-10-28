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

    waiter = _job_submit.submit_once(w, run_name="nb", tasks=[])

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

    a = _job_submit.submit_once(w, run_name="nb", tasks=[])
    b = _job_submit.submit_once(w, run_name="nb", tasks=[])

    assert a.run_id != b.run_id and len(w.jobs.runs) == 2
    t1, t2 = (c["idempotency_token"] for c in w.jobs.calls)
    assert t1 != t2
    assert 0 < len(t1) <= 64 and 0 < len(t2) <= 64  # the Jobs API limit


def test_a_supplied_token_is_used_as_given():
    w = fake_workspace()

    _job_submit.submit_once(w, idempotency_token="my-run-1", run_name="nb")

    assert w.jobs.calls[0]["idempotency_token"] == "my-run-1"


def test_submit_arguments_pass_through_unchanged():
    w = fake_workspace()

    _job_submit.submit_once(w, run_name="nb", timeout_seconds=60, tasks=["t"])

    call = dict(w.jobs.calls[0])
    call.pop("idempotency_token")
    assert call == {"run_name": "nb", "timeout_seconds": 60, "tasks": ["t"]}


def test_a_permanent_error_is_not_retried():
    attempts = []

    def rejected(**kw):
        attempts.append(kw["idempotency_token"])
        raise InvalidParameterValue("bad task spec")

    w = SimpleNamespace(jobs=SimpleNamespace(submit=rejected))

    with pytest.raises(InvalidParameterValue):
        _job_submit.submit_once(w, run_name="nb")

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
        _job_submit.submit_once(w, attempts=3, run_name="nb")

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
        _job_submit.submit_once(fake_workspace(), attempts=0, run_name="nb")


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
        _job_submit.submit_once(w, run_name="nb")

    assert len(attempts) == 1


# ------------------------------------------------------ the notebook runner end to end


@pytest.fixture
def fake_sdk(monkeypatch):
    """Minimal databricks.sdk.service.{jobs,compute}: containers for the submit spec."""

    def container(name):
        return type(name, (SimpleNamespace,), {})

    compute = types.ModuleType("databricks.sdk.service.compute")
    compute.Environment = container("Environment")
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
