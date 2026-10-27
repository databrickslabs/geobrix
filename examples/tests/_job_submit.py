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
* anything else raises at once.

Pure Python: no databricks-sdk import, so it is testable without a workspace.
"""

from __future__ import annotations

import time
import uuid

DEFAULT_ATTEMPTS = 4
DEFAULT_BACKOFF_SECS = 2.0

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
    # Network-level failures: builtin ConnectionError/TimeoutError and requests' exceptions
    # (all OSError subclasses) carry no server verdict, so the request may have landed.
    return isinstance(exc, OSError)


def submit_once(
    w,
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    backoff_secs: float = DEFAULT_BACKOFF_SECS,
    idempotency_token: str | None = None,
    **submit_kwargs,
):
    """``w.jobs.submit(**submit_kwargs)`` that creates exactly one run.

    Returns what ``jobs.submit`` returns (the ``Wait[Run]`` waiter). ``idempotency_token``
    is generated unless given; a caller that retries the whole launch itself can pass its own
    to keep one token across those retries.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1 (got {attempts})")
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
