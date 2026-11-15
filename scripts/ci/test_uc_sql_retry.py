"""Host tests for the one-call retry of the UC gate's warehouse runner (examples/tests/uc_sql.py).

The gate once stopped at 295/320 when the SDK gave up on ONE HTTP call (DeadlineExceeded): the session aborted and the
last 25 cases got "no result" although none had failed. A transport error retries that one call; a statement that RAN
is never run again; a call that keeps failing is a harness error of its own, apart from the case failures.
Nothing here calls a workspace: the SDK is replaced by a fake Statement API.
"""

import enum
import sys
import types
from argparse import Namespace

import pytest

from conftest import REPO_ROOT


class DatabricksError(Exception):
    """Stands in for databricks.sdk.errors.DatabricksError (matched by class name, like the SDK's own subclasses)."""

    def __init__(self, msg="", error_code=None, status_code=None):
        super().__init__(msg)
        self.error_code, self.status_code = error_code, status_code


class DeadlineExceeded(DatabricksError):
    pass


class PermissionDenied(DatabricksError):
    pass


class HTTPError(OSError):  # requests' HTTPError is an OSError that carries the response
    def __init__(self, status):
        super().__init__(f"{status} error")
        self.response = Namespace(status_code=status)


class State(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@pytest.fixture
def uc_sql(monkeypatch):
    import importlib

    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    mod = importlib.import_module("uc_sql")
    monkeypatch.setattr(mod, "BACKOFF_S", (0.0, 0.0))
    monkeypatch.setattr(mod, "POLL_SECS", 0.0)
    sdk = types.ModuleType("databricks.sdk")
    service = types.ModuleType("databricks.sdk.service")
    sql = types.ModuleType("databricks.sdk.service.sql")
    sql.StatementState = State
    for name, m in (("databricks.sdk", sdk), ("databricks.sdk.service", service), ("databricks.sdk.service.sql", sql)):
        monkeypatch.setitem(sys.modules, name, m)
    return mod


def _resp(sid, state, cell=None, error=None):
    cols = [Namespace(type_name=Namespace(value="BIGINT"))]
    return Namespace(
        statement_id=sid,
        status=Namespace(state=state, error=Namespace(message=error) if error else None),
        result=Namespace(data_array=[[cell]]) if cell is not None else None,
        manifest=Namespace(schema=Namespace(columns=cols), total_row_count=1),
    )


class FakeApi:
    """A Statement API whose calls play a script: each entry is a response, or an exception to raise."""

    def __init__(self, submits, polls=()):
        self.submits, self.polls = list(submits), list(polls)
        self.submitted, self.polled = [], []

    def execute_statement(self, statement, **kw):
        self.submitted.append(statement)
        step = self.submits.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def get_statement(self, sid):
        self.polled.append(sid)
        step = self.polls.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def cancel_execution(self, sid):
        pass


def _w(api):
    return Namespace(statement_execution=api)


def _args(**kw):
    return Namespace(**{**dict(warehouse_id="w", catalog=None, schema=None, repeat=1, timeout_s=5.0, decode_binary=True,
                               out=None, batch=0, concurrency=1, batchable=()), **kw})


# ---- what counts as transient ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        DeadlineExceeded("request failed"),
        DatabricksError("x", status_code=429),
        DatabricksError("x", status_code=502),
        DatabricksError("x", status_code=503),
        DatabricksError("x", status_code=504),
        DatabricksError("x", error_code="BAD_GATEWAY"),
        ConnectionResetError("reset by peer"),
        TimeoutError("timed out"),
        HTTPError(502),
        HTTPError(429),
    ],
)
def test_transport_errors_are_transient(uc_sql, exc):
    assert uc_sql.is_transient_api_error(exc)


@pytest.mark.parametrize(
    "exc",
    [
        PermissionDenied("no", status_code=403),
        DatabricksError("bad request", error_code="INVALID_PARAMETER_VALUE", status_code=400),
        HTTPError(404),
        FileNotFoundError("x"),
        ValueError("x"),
        KeyError("x"),
    ],
)
def test_other_errors_are_not_transient(uc_sql, exc):
    assert not uc_sql.is_transient_api_error(exc)


# ---- one API call is tried again ------------------------------------------------------------------------------------


def test_a_failed_status_fetch_repolls_the_same_statement_and_never_resubmits(uc_sql, capsys):
    api = FakeApi([_resp("s1", State.RUNNING)], [DeadlineExceeded("request failed"), DeadlineExceeded("again"), _resp("s1", State.SUCCEEDED, "7")])
    entry = uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())
    assert entry["value"] == "7" or entry["value"] == 7
    assert len(api.submitted) == 1 and api.polled == ["s1", "s1", "s1"]
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("retry:")]
    assert lines == [
        "retry: statement s1 DeadlineExceeded attempt 1/2",
        "retry: statement s1 DeadlineExceeded attempt 2/2",
    ]


def test_a_submit_that_returned_no_id_is_submitted_again(uc_sql, capsys):
    api = FakeApi([ConnectionResetError("reset"), _resp("s1", State.SUCCEEDED, "7")])
    entry = uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())
    assert "error" not in entry and "harness_error" not in entry
    assert len(api.submitted) == 2
    assert "retry: statement not submitted ConnectionResetError attempt 1/2" in capsys.readouterr().out


def test_retries_end_after_two_more_tries_as_a_harness_error_not_a_case_error(uc_sql):
    api = FakeApi([_resp("s1", State.RUNNING)], [DeadlineExceeded("a"), DeadlineExceeded("b"), DeadlineExceeded("c")])
    entry = uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())
    assert set(entry) == {"harness_error"} and "DeadlineExceeded" in entry["harness_error"] and "s1" in entry["harness_error"]
    assert len(api.submitted) == 1 and len(api.polled) == 3  # 1 try + 2 more; the statement was never submitted twice


def test_a_submit_that_keeps_failing_is_a_harness_error_after_three_tries(uc_sql):
    api = FakeApi([DatabricksError("x", status_code=503)] * 3)
    entry = uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())
    assert set(entry) == {"harness_error"} and len(api.submitted) == 3


def test_fail_on_revert_a_statement_that_ran_and_failed_is_never_retried(uc_sql):
    api = FakeApi([_resp("s1", State.FAILED, error="UNRESOLVED_ROUTINE: f")])
    entry = uc_sql.run_single(_w(api), "c1", "SELECT f()", _args())
    assert entry == {"error": "UNRESOLVED_ROUTINE: f"} and len(api.submitted) == 1 and api.polled == []


def test_a_wrong_result_is_a_result_and_a_non_transient_api_error_is_not_retried(uc_sql):
    api = FakeApi([_resp("s1", State.SUCCEEDED, "8")])
    assert uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())["value"] in ("8", 8) and len(api.submitted) == 1
    api = FakeApi([PermissionDenied("no", status_code=403)])
    with pytest.raises(PermissionDenied):
        uc_sql.run_single(_w(api), "c1", "SELECT 7", _args())
    assert len(api.submitted) == 1


# ---- the session goes on --------------------------------------------------------------------------------------------


def test_the_session_goes_on_after_an_exhausted_statement_and_records_every_case(uc_sql, tmp_path, capsys):
    api = FakeApi(
        [_resp("s1", State.SUCCEEDED, "1"), _resp("s2", State.RUNNING), _resp("s3", State.SUCCEEDED, "3")],
        [DeadlineExceeded("a"), DeadlineExceeded("b"), DeadlineExceeded("c")],
    )
    pairs = [("c1", "SELECT 1"), ("c2", "SELECT 2"), ("c3", "SELECT 3")]
    out = tmp_path / "r.json"
    results = uc_sql.run_statements(_w(api), pairs, _args(out=str(out)))
    assert list(results) == ["c1", "c2", "c3"]
    assert "value" in results["c1"] and "value" in results["c3"] and set(results["c2"]) == {"harness_error"}
    assert uc_sql.harness_errors(results).keys() == {"c2"}
    assert "HARNESS ERROR (transient API)" in capsys.readouterr().out
    from databricks.labs.gbx.bench import oracles

    assert set(oracles.load_uc_results(str(out))) == {"c1", "c2", "c3"}  # written to the file too


def test_a_batch_lost_to_the_api_runs_its_cases_one_by_one(uc_sql, monkeypatch):
    monkeypatch.setattr(uc_sql, "run_batch_once", lambda *a, **k: (_ for _ in ()).throw(uc_sql.TransientApiError("lost")))
    monkeypatch.setattr(uc_sql.oracles, "uc_batch_item", lambda s: s.replace("SELECT ", ""))
    api = FakeApi([_resp("s1", State.SUCCEEDED, "1"), _resp("s2", State.SUCCEEDED, "2")])
    pairs = [("c1", "SELECT 1"), ("c2", "SELECT 2")]
    results = uc_sql.run_statements(_w(api), pairs, _args(batch=2, batchable={"c1", "c2"}))
    assert [k for k in results] == ["c1", "c2"] and all("value" in r for r in results.values()) and len(api.submitted) == 2


def test_main_exits_1_with_a_harness_error_line_and_still_writes_every_case(uc_sql, monkeypatch, tmp_path, capsys):
    f = tmp_path / "a.sql"
    f.write_text("-- cid: c1\nSELECT 1\n\n-- cid: c2\nSELECT 2\n")
    api = FakeApi([DeadlineExceeded("x")] * 3 + [_resp("s2", State.SUCCEEDED, "2")])
    monkeypatch.setitem(sys.modules, "databricks.sdk", types.SimpleNamespace(WorkspaceClient=lambda profile=None: _w(api)))
    monkeypatch.setattr(uc_sql, "warehouse_problem", lambda *a, **k: None)
    out = tmp_path / "o.json"
    rc = uc_sql.main(["--file", str(f), "--profile", "p", "--warehouse-id", "w", "--out", str(out)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "harness error (transient API): 1 of 2" in err and "c1" in err
    from databricks.labs.gbx.bench import oracles

    assert set(oracles.load_uc_results(str(out))) == {"c1", "c2"}
