#!/usr/bin/env python3
"""Run a file of `-- cid: <id>` statements on a SQL warehouse and write what came back.

The Unity Catalog functions are exercised through the warehouse Statement API, one SELECT per
case, so a refusal in one case cannot hide another. Each statement runs `--repeat` times (the
wall time of each run is recorded); the value, or the error message, comes from the first run.

    {cid: {"value": ..., "wall_s": [...]} | {"error": message}}

With `--batch N`, the value statements a file marks `-- batch` run N at a time as ONE statement of one row
and N columns (the warehouse pays a fixed sandbox cost per statement, ~10 s, whatever it computes): column i
answers case i, and each case is still judged on its own. A statement that must be refused is never marked, and
a batch that fails as a whole is rerun case by case, so a failure still names its case. `--concurrency N` keeps
N warehouse statements in flight.

BINARY cells come back base64 in the API's JSON: they are decoded to bytes (`--decode-binary`, the
default), and a tile struct's `raster` field too. `bench.oracles.load_uc_results` reads the file
back, and `bench.oracles.check_uc_results` judges it against the oracle rows.

Run it through `gbx:uc:sql` (scripts/commands/gbx-uc-sql.sh), which picks an interpreter that has
the Databricks SDK. `--dry-run` parses the file and prints the plan: no workspace call, no auth.
"""

from __future__ import annotations

import argparse
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python" / "geobrix" / "src"))

from databricks.labs.gbx.bench import oracles  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))  # _cluster_config.py, _uc_warehouse.py
from _cluster_config import cluster_config_file  # noqa: E402
from _uc_warehouse import warehouse_problem  # noqa: E402

POLL_SECS = 1.0
DEFAULT_TIMEOUT_S = 600.0
# pip prints "Looking in indexes: <url>" in some failures; an index URL never reaches a log or a file.
_INDEX_LINE = re.compile(r"(?m)^.*Looking in indexes:.*$")


def scrub(text: str) -> str:
    return _INDEX_LINE.sub("[package index line removed]", text)


class StatementTimeout(Exception):
    """A statement was still pending or running when its deadline passed (it has been cancelled)."""


class TransientApiError(Exception):
    """One Statement API call kept failing in transit (a transport error, not a SQL result) after its retries.

    The statement itself is not judged: it is a harness error, recorded as `{"harness_error": message}`.
    """


# One API call (a submit, or a status fetch of a statement that has an id) is tried again this many times after a
# transient failure, waiting BACKOFF_S[i] seconds before try i + 2. A statement that RAN is never run again.
API_RETRIES = 2
BACKOFF_S = (1.0, 3.0)
_TRANSIENT_NAMES = frozenset(
    {"DeadlineExceeded", "TemporarilyUnavailable", "TooManyRequests", "RequestLimitExceeded", "BadGateway", "GatewayTimeout"}
)
_TRANSIENT_CODES = frozenset({"BAD_GATEWAY", "GATEWAY_TIMEOUT", "TEMPORARILY_UNAVAILABLE", "DEADLINE_EXCEEDED"})
_TRANSIENT_STATUS = frozenset({429, 502, 503, 504})
_LOCAL_FILE_ERRORS = (FileNotFoundError, PermissionError, IsADirectoryError, NotADirectoryError, FileExistsError)


def is_transient_api_error(exc: BaseException) -> bool:
    """True for a failure of the HTTP call itself: the SDK's DeadlineExceeded, a reset or timed-out connection, or a
    429/502/503/504. A SQL error never raises (it comes back as a FAILED statement), and an API error that would fail
    the same way again (bad request, permission, not found) is not transient. Matched by class name: no SDK import.
    """
    names = {c.__name__ for c in type(exc).__mro__}
    if "DatabricksError" in names:
        status = getattr(exc, "status_code", None)
        return bool(
            names & _TRANSIENT_NAMES
            or str(getattr(exc, "error_code", "") or "").upper() in _TRANSIENT_CODES
            or status in _TRANSIENT_STATUS
        )
    if "HTTPError" in names:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status is None:
            status = getattr(exc, "code", None)
        return status in _TRANSIENT_STATUS
    # builtin ConnectionError/TimeoutError and requests' ConnectionError/Timeout are all OSErrors
    return isinstance(exc, (TimeoutError, OSError)) and not isinstance(exc, _LOCAL_FILE_ERRORS)


def _api_call(call, statement_id=None):
    """`call()`, tried again up to API_RETRIES times after a transient failure; one log line per retry.

    `statement_id` is the id of the statement a status fetch asks about (None for a submit, which has none yet).
    Raises TransientApiError when the retries are used up; any other exception passes through untouched.
    """
    for attempt in range(API_RETRIES + 1):
        try:
            return call()
        except Exception as exc:  # noqa: BLE001 - classified below; a non-transient one is re-raised as is
            if not is_transient_api_error(exc):
                raise
            if attempt == API_RETRIES:
                raise TransientApiError(
                    f"{type(exc).__name__}: {scrub(str(exc))} (statement {statement_id or 'not submitted'}, "
                    f"{API_RETRIES + 1} tries)"
                ) from exc
            print(
                f"retry: statement {statement_id or 'not submitted'} {type(exc).__name__} "
                f"attempt {attempt + 1}/{API_RETRIES}",
                flush=True,
            )
            time.sleep(BACKOFF_S[min(attempt, len(BACKOFF_S) - 1)])


def _execute(w, statement, warehouse_id, catalog, schema, timeout_s):
    """Submit a statement and wait for a terminal state; return the response.

    A submit that fails in transit is tried again (nothing ran, or no id came back); once the statement has an id, a
    failed status fetch asks about that same id again and never submits it twice.
    Raises StatementTimeout when it is still not terminal `timeout_s` after it was submitted,
    and TransientApiError when a call keeps failing in transit.
    """
    from databricks.sdk.service.sql import StatementState

    api = w.statement_execution
    resp = _api_call(
        lambda: api.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            wait_timeout="50s",
        )
    )
    deadline = time.monotonic() + timeout_s
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() >= deadline:
            try:
                api.cancel_execution(resp.statement_id)
            except Exception:  # noqa: BLE001 - best effort: the timeout is what gets reported
                pass
            raise StatementTimeout(
                f"statement still {resp.status.state.value} after {timeout_s:g}s; cancelled "
                f"(raise --timeout-s if the warehouse is cold or the statement is slow)"
            )
        time.sleep(POLL_SECS)
        sid = resp.statement_id
        resp = _api_call(lambda: api.get_statement(sid), sid)
    return resp


def _failure(resp) -> Optional[str]:
    from databricks.sdk.service.sql import StatementState

    state = resp.status.state
    if state == StatementState.SUCCEEDED:
        return None
    err = resp.status.error
    return err.message if err and err.message else f"statement {state.value}"


def _type_of(col) -> str:
    return getattr(col.type_name, "value", str(col.type_name))


def run_once(
    w,
    statement: str,
    warehouse_id: str,
    catalog: Optional[str],
    schema: Optional[str],
    timeout_s: float = DEFAULT_TIMEOUT_S,
    info: Optional[dict] = None,
):
    """Run one statement to a terminal state; return (state, error message, first cell, its type).

    Raises StatementTimeout when it is still not terminal `timeout_s` after it was submitted.
    When `info` is a dict, it gets `rows` (the number of rows the statement returned) and `first_row`.
    """
    resp = _execute(w, statement, warehouse_id, catalog, schema, timeout_s)
    state = resp.status.state
    error = _failure(resp)
    if error is not None:
        return state, error, None, None
    rows = (resp.result.data_array if resp.result else None) or []
    cell = rows[0][0] if rows and rows[0] else None
    if info is not None:
        total = getattr(resp.manifest, "total_row_count", None) if resp.manifest else None
        info["rows"] = total if total is not None else len(rows)
        info["first_row"] = rows[0] if rows else None
    cols = (resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or []
    if not cols:  # a statement with no result set (CREATE FUNCTION, DROP ...): no value
        return state, None, None, None
    return state, None, cell, _type_of(cols[0])


def run_batch_once(w, statement: str, warehouse_id: str, catalog, schema, timeout_s: float = DEFAULT_TIMEOUT_S):
    """Run a one-row statement; return (error message, [(cell, type name)] of its row, one per column)."""
    resp = _execute(w, statement, warehouse_id, catalog, schema, timeout_s)
    error = _failure(resp)
    if error is not None:
        return error, []
    rows = (resp.result.data_array if resp.result else None) or []
    cols = (resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or []
    if len(rows) != 1 or len(rows[0]) != len(cols):
        return f"a batch statement must return one row of {len(cols)} column(s), got {len(rows)} row(s)", []
    return None, [(cell, _type_of(col)) for cell, col in zip(rows[0], cols)]


# ---- planning: which statements share a warehouse statement --------------------------------------------------------


@dataclass
class Unit:
    """One warehouse statement: `members` is [(cid, single statement)] (more than one means a batch)."""

    members: list
    items: Optional[list] = None  # a batch's one-value expressions, in member order

    @property
    def is_batch(self) -> bool:
        return self.items is not None


def plan_units(pairs, batchable=(), size: int = 0) -> list:
    """The units that run `pairs`: batchable statements in chunks of `size`, every other statement alone.

    Every cid of `pairs` is in exactly one unit. A statement without a one-value form (`oracles.uc_batch_item`) and a
    chunk of one are single units. `size` below 2 plans every statement alone.
    """
    units, chunk = [], []

    def flush():
        if len(chunk) >= 2:
            units.append(Unit([(c, s) for c, s, _ in chunk], [i for _, _, i in chunk]))
        else:
            units.extend(Unit([(c, s)]) for c, s, _ in chunk)
        chunk.clear()

    for cid, stmt in pairs:
        item = oracles.uc_batch_item(stmt) if size > 1 and cid in batchable else None
        if item is None:
            units.append(Unit([(cid, stmt)]))
            continue
        chunk.append((cid, stmt, item))
        if len(chunk) >= size:
            flush()
    flush()
    return units


def batch_entries(unit: Unit, cells, wall: float, decode_binary: bool) -> dict:
    """{cid: entry} for a batch's row: column i answers member i (the mapping the judge relies on)."""
    return {
        cid: {"value": oracles.decode_uc_cell(cell, type_name, decode_binary=decode_binary), "wall_s": [wall], "batch": len(unit.members)}
        for (cid, _), (cell, type_name) in zip(unit.members, cells)
    }


def run_single(w, cid: str, statement: str, args) -> dict:
    walls, entry = [], None
    info = {} if getattr(args, "record_rows", False) else None
    for _ in range(args.repeat):
        t0 = time.perf_counter()
        try:
            state, error, cell, type_name = run_once(
                w, statement, args.warehouse_id, args.catalog, args.schema, args.timeout_s, info
            )
        except StatementTimeout as exc:
            state, error, cell, type_name = None, str(exc), None, None
        except TransientApiError as exc:  # not a result of the case: a harness error, and the next statements go on
            return {"harness_error": scrub(str(exc))}
        walls.append(round(time.perf_counter() - t0, 4))
        if error is not None:
            entry = {"error": scrub(error)}
            break
        if entry is None:
            value = oracles.decode_uc_cell(cell, type_name, decode_binary=args.decode_binary)
            entry = {"value": value}
    if "value" in entry:
        entry["wall_s"] = walls
        if info is not None:
            entry["rows"] = info.get("rows", 0)
            entry["first_row"] = info.get("first_row")
    return entry


def run_unit(w, unit: Unit, args) -> dict:
    """{cid: entry} for a unit. A batch that fails as a whole runs its members one by one, so a failure names its case."""
    if not unit.is_batch:
        (cid, stmt), = unit.members
        return {cid: run_single(w, cid, stmt, args)}
    t0 = time.perf_counter()
    try:
        error, cells = run_batch_once(
            w, oracles.uc_batch_statement(unit.items), args.warehouse_id, args.catalog, args.schema, args.timeout_s
        )
    except StatementTimeout as exc:
        error, cells = str(exc), []
    except TransientApiError as exc:
        print(f"batch of {len(unit.members)} ({unit.members[0][0]} ...) lost to the API: {scrub(str(exc))[:160]}; running its cases one by one", flush=True)
        return {cid: run_single(w, cid, stmt, args) for cid, stmt in unit.members}
    if error is None:
        return batch_entries(unit, cells, round(time.perf_counter() - t0, 4), args.decode_binary)
    print(f"batch of {len(unit.members)} ({unit.members[0][0]} ...) failed: {' '.join(scrub(error).split())[:160]}; running its cases one by one", flush=True)
    return {cid: run_single(w, cid, stmt, args) for cid, stmt in unit.members}


def run_statements(w, pairs, args) -> dict:
    """Run `pairs`; return {cid: entry} in the order of `pairs`.

    `args.batch` (statements per batch, default off) and `args.batchable` (the cids that may be batched) group value
    statements into one warehouse statement each; `args.concurrency` (default 1) runs that many units at once.
    """
    units = plan_units(pairs, getattr(args, "batchable", ()), getattr(args, "batch", 0))
    workers = max(1, min(getattr(args, "concurrency", 1), len(units) or 1))
    if len(units) != len(pairs):
        print(f"{len(pairs)} statements as {len(units)} warehouse statements ({sum(u.is_batch for u in units)} batches)", flush=True)
    results, lock = {}, threading.Lock()

    def work(unit):
        entries = run_unit(w, unit, args)
        with lock:
            results.update(entries)
            for cid, _ in unit.members:
                print(summary_line(cid, entries[cid]), flush=True)
            if args.out:  # after every unit: a failure later in the run keeps what is already known
                oracles.dump_uc_results(results, args.out)

    if workers == 1:
        for unit in units:
            work(unit)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for f in [pool.submit(work, u) for u in units]:
                f.result()
    return {cid: results[cid] for cid, _ in pairs if cid in results}


def harness_errors(results: dict) -> dict:
    """{cid: message} of the entries that are harness errors (a transient API failure), not results of their case."""
    return {cid: e["harness_error"] for cid, e in results.items() if "harness_error" in e}


def summary_line(cid: str, entry: dict) -> str:
    if "harness_error" in entry:
        return f"{cid:28s} HARNESS ERROR (transient API)  {' '.join(entry['harness_error'].split())[:160]}"
    if "error" in entry:
        return f"{cid:28s} ERROR  {' '.join(entry['error'].split())[:160]}"
    value = entry["value"]
    shown = f"<{len(value)} bytes>" if isinstance(value, (bytes, bytearray)) else repr(value)[:60]
    return f"{cid:28s} ok     {shown}  wall_s={entry['wall_s']}"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", required=True, help="statements, each after a `-- cid: <id>` line")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse id (gbx:uc:sql reads GBX_UC_WAREHOUSE_ID)")
    p.add_argument("--catalog", default=None, help="default catalog for the statements")
    p.add_argument("--schema", default=None, help="default schema for the statements")
    p.add_argument("--out", default=None, help="write the results here as JSON")
    p.add_argument("--repeat", type=int, default=1, help="runs per statement; wall time of each is recorded")
    p.add_argument(
        "--timeout-s",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help="give up on (and cancel) a statement still running after this many seconds (default 600)",
    )
    p.add_argument(
        "--batch",
        type=int,
        default=0,
        help="put up to N value statements (those marked `-- batch` in the file) in one warehouse statement; "
        "0 runs each alone (default). A batch that fails runs its cases one by one",
    )
    p.add_argument("--concurrency", type=int, default=1, help="warehouse statements in flight at once (default 1)")
    p.add_argument("--profile", default=None, help="Databricks config profile (never chosen for you)")
    p.add_argument(
        "--decode-binary",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="decode base64 BINARY cells to bytes (default on)",
    )
    p.add_argument("--dry-run", action="store_true", help="parse the file and print the plan; no workspace call")
    args = p.parse_args(argv)
    if args.repeat < 1:
        p.error("--repeat must be at least 1")
    if args.timeout_s <= 0:
        p.error("--timeout-s must be positive")
    if args.batch < 0 or args.concurrency < 1:
        p.error("--batch must be 0 or more and --concurrency at least 1")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    text = Path(args.file).read_text()
    pairs = oracles.parse_uc_sql(text)
    args.batchable = oracles.parse_uc_batchable(text)
    if not pairs:
        print(f"{args.file}: no '-- cid: <id>' statements found", file=sys.stderr)
        return 2
    if args.dry_run:
        units = plan_units(pairs, args.batchable, args.batch)
        print(
            f"dry run: {len(pairs)} statement(s) as {len(units)} warehouse statement(s) "
            f"(batch {args.batch}, concurrency {args.concurrency}) x {args.repeat} on warehouse {args.warehouse_id or '-'} "
            f"(profile {args.profile or '-'}, catalog {args.catalog or '-'}, schema {args.schema or '-'}); "
            f"results {'-> ' + args.out if args.out else 'not written'}"
        )
        for cid, statement in pairs:
            print(f"  {cid:28s} {statement[:90]}{'...' if len(statement) > 90 else ''}")
        return 0
    missing = [o for o, v in (("--profile", args.profile), ("--warehouse-id", args.warehouse_id)) if not v]
    if missing:
        print(
            f"{', '.join(missing)} required (a profile is never chosen for you: see `databricks auth profiles`)",
            file=sys.stderr,
        )
        return 2
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)
    problem = warehouse_problem(w, args.warehouse_id, cluster_config_file(Path(__file__).resolve().parent), args.profile)
    if problem:
        print(f"refused: {problem}", file=sys.stderr)
        return 1
    try:
        results = run_statements(w, pairs, args)
    except Exception as exc:  # noqa: BLE001 - an SDK or network failure: report it, keep the results written so far
        print(f"stopped: {scrub(f'{type(exc).__name__}: {exc}')}", file=sys.stderr)
        return 1
    if args.out:
        print(f"wrote {args.out}")
    harness = harness_errors(results)
    if harness:  # no verdict can come from these statements: the run fails, apart from the case failures
        print(
            f"harness error (transient API): {len(harness)} of {len(results)} statement(s) got no answer: "
            f"{', '.join(list(harness)[:10])}{' ...' if len(harness) > 10 else ''}",
            file=sys.stderr,
        )
        return 1
    return 0  # a statement's error is a result (a refusal is an expected one): the oracle judges the file


if __name__ == "__main__":
    sys.exit(main())
