"""Submit a one-time Databricks run exactly once.

``jobs.submit`` creates a new run on every call. When the server accepts a request but the
response is lost (a connection reset, a gateway timeout) the client cannot tell, and a retry
-- by the SDK's own retry loop or by the caller -- creates a second, orphan run. The Jobs API
prevents that with an ``idempotency_token``: a repeated token returns the run the first
request made instead of creating another.

``submit_once`` is the one place the launchers in this directory call ``jobs.submit``:

* it makes ONE token per logical run (a fresh uuid, so a later re-run is a new run), and
  sends it on every attempt, so the SDK's own retries and the retries here are all
  idempotent;
* it retries only transient failures (network errors, 429/5xx), with the same token;
* anything else raises at once;
* it REFUSES a submit that names no ``timeout_seconds``: a run with no time limit can sit for
  hours on a hung driver. The limit goes on the run and on every task that sets none, and each
  caller passes a value sized to its work (``DEFAULT_TIMEOUT_SECS`` is the fallback for a caller
  with nothing better).

``runner_dir`` is the other thing the launchers share: the workspace folder they upload
their runner notebook into. It is ``GBX_RUNNER_DIR`` when the caller set it, and the
caller's own workspace home otherwise.

Pure Python: no databricks-sdk import, so it is testable without a workspace.
"""

from __future__ import annotations

import os
import time
import uuid

DEFAULT_ATTEMPTS = 4
DEFAULT_BACKOFF_SECS = 2.0
# The limit a caller uses when it cannot size one to its work: 45 minutes.
DEFAULT_TIMEOUT_SECS = 45 * 60

# Databricks SDK errors (databricks.sdk.errors) that mean "try again": throttling,
# unavailability, timeouts and server faults. Matched by class name so this module needs no
# SDK import; the SDK maps HTTP 429/500/503/504 and the matching error codes to these.
_TRANSIENT_API_ERRORS = frozenset(
    {
        "TemporarilyUnavailable",
        "DeadlineExceeded",
        "InternalError",
        "Unknown",
        "DataLoss",
        "TooManyRequests",
        "RequestLimitExceeded",
        "ResourceExhausted",
    }
)


_LOCAL_FILE_ERRORS = (
    FileNotFoundError,
    PermissionError,
    IsADirectoryError,
    NotADirectoryError,
    FileExistsError,
)


# Under the caller's workspace home, version-less like the other launchers' folders
# (run_notebooks_serverless.py uses GeoBrix/serverless-run).
RUNNER_SUBDIR = "GeoBrix/runners"


def runner_dir(w) -> str:
    """The workspace folder a launcher uploads its runner notebook into.

    ``GBX_RUNNER_DIR`` when the caller set it (an empty value counts as not set); otherwise
    ``/Workspace/Users/<you>/GeoBrix/runners`` for the user ``w`` is signed in as. The user is looked
    up (``w.current_user.me()``) only when there is no override, so an override makes no call.
    """
    override = os.environ.get("GBX_RUNNER_DIR")
    if override:
        return override
    return f"/Workspace/Users/{w.current_user.me().user_name}/{RUNNER_SUBDIR}"


def new_idempotency_token() -> str:
    """A fresh token for one logical run (32 hex chars; the API allows up to 64)."""
    return uuid.uuid4().hex


def is_transient(exc: BaseException) -> bool:
    """True when ``exc`` is worth retrying with the same idempotency token."""
    names = {c.__name__ for c in type(exc).__mro__}
    if "DatabricksError" in names:
        # An API error is retryable only when it is one of the transient classes; a bad
        # request, a permission error or a missing resource will fail the same way again.
        return bool(names & _TRANSIENT_API_ERRORS)
    if "HTTPError" in names:
        # requests' HTTPError (an OSError) is raised for a response the server DID send, so it
        # carries a verdict: only throttling, a request timeout and server faults can differ
        # next time; a 400/401/403/404 fails the same way again.
        # (urllib's HTTPError has no response: its status is `code`.)
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status is None:
            status = getattr(exc, "code", None)
        return status is None or status in (408, 429) or status >= 500
    # Network-level failures: builtin ConnectionError/TimeoutError and requests' exceptions
    # (all OSError subclasses) carry no server verdict, so the request may have landed. Local
    # file errors are OSErrors too, but retrying cannot change them.
    return isinstance(exc, OSError) and not isinstance(exc, _LOCAL_FILE_ERRORS)


def _require_timeout(timeout_seconds) -> int:
    """The run's time limit in seconds, or ValueError. The Jobs API reads 0 as "no limit", so 0 is refused too."""
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        raise ValueError(
            f"submit_once needs a positive integer timeout_seconds (got {timeout_seconds!r}): a run with no "
            f"time limit can hang for hours. Size one to the work, or pass DEFAULT_TIMEOUT_SECS "
            f"({DEFAULT_TIMEOUT_SECS})."
        )
    return timeout_seconds


def submit_once(
    w,
    *,
    timeout_seconds=None,
    attempts: int = DEFAULT_ATTEMPTS,
    backoff_secs: float = DEFAULT_BACKOFF_SECS,
    idempotency_token: str | None = None,
    **submit_kwargs,
):
    """``w.jobs.submit(**submit_kwargs)`` that creates exactly one run.

    Returns what ``jobs.submit`` returns (the ``Wait[Run]`` waiter). ``idempotency_token``
    is generated unless given; a caller that retries the whole launch itself can pass its own
    to keep one token across those retries.

    ``timeout_seconds`` is required (see ``_require_timeout``): it is the run's limit, and the
    limit of every task that sets none.
    """
    timeout_seconds = _require_timeout(timeout_seconds)
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1 (got {attempts})")
    for task in submit_kwargs.get("tasks") or ():
        if getattr(task, "timeout_seconds", 0) is None:  # a SubmitTask with no limit of its own
            task.timeout_seconds = timeout_seconds
    submit_kwargs["timeout_seconds"] = timeout_seconds
    token = idempotency_token or new_idempotency_token()
    print(f"    jobs.submit idempotency_token={token}", flush=True)
    for attempt in range(1, attempts + 1):
        try:
            return w.jobs.submit(idempotency_token=token, **submit_kwargs)
        except Exception as exc:  # noqa: BLE001 - classified by is_transient
            if attempt == attempts or not is_transient(exc):
                raise
            print(
                f"    jobs.submit attempt {attempt}/{attempts} failed "
                f"({type(exc).__name__}: {exc}); retrying with the same "
                f"idempotency_token={token}",
                flush=True,
            )
            time.sleep(backoff_secs * attempt)
    raise AssertionError("unreachable")  # pragma: no cover
