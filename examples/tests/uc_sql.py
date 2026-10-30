#!/usr/bin/env python3
"""Run a file of `-- cid: <id>` statements on a SQL warehouse and write what came back.

The Unity Catalog functions are exercised through the warehouse Statement API, one SELECT per
case, so a refusal in one case cannot hide another. Each statement runs `--repeat` times (the
wall time of each run is recorded); the value, or the error message, comes from the first run.

    {cid: {"value": ..., "wall_s": [...]} | {"error": message}}

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
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python" / "geobrix" / "src"))

from databricks.labs.gbx.bench import oracles  # noqa: E402

POLL_SECS = 1.0
DEFAULT_TIMEOUT_S = 600.0
# pip prints "Looking in indexes: <url>" in some failures; an index URL never reaches a log or a file.
_INDEX_LINE = re.compile(r"(?m)^.*Looking in indexes:.*$")


def scrub(text: str) -> str:
    return _INDEX_LINE.sub("[package index line removed]", text)


class StatementTimeout(Exception):
    """A statement was still pending or running when its deadline passed (it has been cancelled)."""


def run_once(
    w,
    statement: str,
    warehouse_id: str,
    catalog: Optional[str],
    schema: Optional[str],
    timeout_s: float = DEFAULT_TIMEOUT_S,
):
    """Run one statement to a terminal state; return (state, error message, first cell, its type).

    Raises StatementTimeout when it is still not terminal `timeout_s` after it was submitted.
    """
    from databricks.sdk.service.sql import StatementState

    api = w.statement_execution
    resp = api.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="50s",
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
        resp = api.get_statement(resp.statement_id)
    state = resp.status.state
    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        return state, (err.message if err and err.message else f"statement {state.value}"), None, None
    rows = (resp.result.data_array if resp.result else None) or []
    cell = rows[0][0] if rows and rows[0] else None
    cols = (resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or []
    if not cols:  # a statement with no result set (CREATE FUNCTION, DROP ...): no value
        return state, None, None, None
    type_name = cols[0].type_name
    return state, None, cell, getattr(type_name, "value", str(type_name))


def run_statements(w, pairs, args) -> dict:
    results = {}
    for cid, statement in pairs:
        walls, entry = [], None
        for _ in range(args.repeat):
            t0 = time.perf_counter()
            try:
                state, error, cell, type_name = run_once(
                    w, statement, args.warehouse_id, args.catalog, args.schema, args.timeout_s
                )
            except StatementTimeout as exc:
                state, error, cell, type_name = None, str(exc), None, None
            walls.append(round(time.perf_counter() - t0, 4))
            if error is not None:
                entry = {"error": scrub(error)}
                break
            if entry is None:
                value = oracles.decode_uc_cell(cell, type_name, decode_binary=args.decode_binary)
                entry = {"value": value}
        if "value" in entry:
            entry["wall_s"] = walls
        results[cid] = entry
        print(summary_line(cid, entry), flush=True)
        if args.out:  # after every statement: a failure later in the run keeps what is already known
            oracles.dump_uc_results(results, args.out)
    return results


def summary_line(cid: str, entry: dict) -> str:
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
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    pairs = oracles.parse_uc_sql(Path(args.file).read_text())
    if not pairs:
        print(f"{args.file}: no '-- cid: <id>' statements found", file=sys.stderr)
        return 2
    if args.dry_run:
        print(
            f"dry run: {len(pairs)} statement(s) x {args.repeat} on warehouse {args.warehouse_id or '-'} "
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
    try:
        results = run_statements(w, pairs, args)
    except Exception as exc:  # noqa: BLE001 - an SDK or network failure: report it, keep the results written so far
        print(f"stopped: {scrub(f'{type(exc).__name__}: {exc}')}", file=sys.stderr)
        return 1
    if args.out:
        print(f"wrote {args.out}")
    return 0  # a statement's error is a result (a refusal is an expected one): the oracle judges the file


if __name__ == "__main__":
    sys.exit(main())
