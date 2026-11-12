#!/usr/bin/env python3
"""Validate, deploy and run a Lakeflow SDP example bundle, then check what its pipeline published (gbx:test:sdp).

Steps, each stopping the command on a failure:

1. ``databricks bundle validate`` with ``--var gbx_wheel=<wheel>`` (default: the versioned staged env6 wheel; the
   example pipelines run on the Lakeflow PREVIEW channel, DBR 19, and their job tasks on Serverless environment 6) and every
   ``--var``. The wheel's environment must be the bundle's ``environment_version``.
2. ``databricks bundle plan``: a plan that deletes or recreates a resource stops here. Deleting a pipeline drops its
   tables, so this command never does it; it prints one line for the orchestrator.
3. ``databricks bundle deploy`` (no ``--auto-approve``), then ``bundle summary`` for the job and pipeline ids.
4. The run: the bundle's job (``run_now``, one idempotency token; ``--only`` for some tasks), or with
   ``--pipeline-only`` one pipeline update. It is polled to its end within ``--timeout-min``; on time-out it is
   cancelled. A failed flow or update of the bundle's pipeline (an ERROR event of this run) stops it at once.
   A failure that needs a table dropped (a materialized view becoming a streaming table, a table owned by another
   pipeline) is printed as one ``DROP NEEDED:`` line: this command drops nothing.
5. The tables: every Unity Catalog table the pipeline owns (``pipeline_id``) in its catalog and schema must exist
   and hold rows (``SELECT count(*)`` on ``--warehouse-id``); ``--allow-empty NAME`` for a table that may be empty.
   A bundle's own validate task (wireless coverage: ``validate``) runs as part of its job.

Usage: see scripts/commands/gbx-test-sdp.md.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
import time
import uuid
from typing import Callable, Optional

_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = (_SCRIPT_DIR / ".." / "..").resolve()

BUNDLES = {
    "vapor_eyes_lf": PROJECT_ROOT / "examples" / "notebooks" / "vapor-eyes" / "lakeflow",
    "wireless_coverage_lf": PROJECT_ROOT / "examples" / "notebooks" / "wireless-coverage" / "lakeflow",
}
DEFAULT_TIMEOUT_MIN = 30
DEFAULT_POLL_SECS = 30
_DESTRUCTIVE = ("delete", "recreate")


# ---------------------------------------------------------------------------------------------- the bundle


def resolve_bundle(arg: str) -> pathlib.Path:
    """A bundle name (``vapor_eyes_lf``) or a directory holding a ``databricks.yml``."""
    if arg in BUNDLES:
        return BUNDLES[arg]
    path = pathlib.Path(arg).expanduser().resolve()
    if path.is_file() and path.name == "databricks.yml":
        path = path.parent
    if not (path / "databricks.yml").is_file():
        raise SystemExit(f"--bundle {arg}: no databricks.yml there; known bundles: {', '.join(sorted(BUNDLES))}")
    return path


def bundle_text(bundle_dir: pathlib.Path) -> str:
    return (bundle_dir / "databricks.yml").read_text(encoding="utf-8")


def bundle_name(text: str) -> Optional[str]:
    m = re.search(r"^bundle:\s*\n(?:\s+#.*\n)*\s+name:\s*['\"]?([\w.-]+)", text, re.M)
    return m.group(1) if m else None


# The env wheel the example bundles take by default: the PREVIEW channel is DBR 19, the job tasks Serverless env 6.
DEFAULT_WHEEL_ENV = 6


def environment_versions(text: str) -> set[str]:
    """The ``environment_version`` values of the bundle's job environments."""
    return set(re.findall(r"^\s*environment_version:\s*['\"]?(\d+)", text, re.M))


def wheel_env(wheel: str) -> Optional[int]:
    m = re.search(r"geobrix-[^/]*\+env([56])-py3-none-any\.whl$", wheel or "")
    return int(m.group(1)) if m else None


def wheel_problem(wheel: str, text: str) -> Optional[str]:
    """Why ``wheel`` cannot serve this bundle, or None."""
    env = wheel_env(wheel)
    if env is None:
        return f"--wheel {wheel}: not an env wheel (geobrix-<version>+env5|6-py3-none-any.whl); the bundle needs one"
    want = environment_versions(text)
    if want and want != {str(env)}:
        return (f"--wheel {wheel} is the env{env} wheel; the bundle's job environments are version "
                f"{', '.join(sorted(want))}: the other family's wheel refuses to import. Fix: pass the "
                f"+env{min(want)} wheel")
    return None


def default_wheel(env: int = DEFAULT_WHEEL_ENV) -> str:
    sys.path.insert(0, str(_SCRIPT_DIR))
    from run_notebooks_serverless import default_wheel as _dw  # the one place the staged layout is spelled

    return _dw(env)


# ---------------------------------------------------------------------------------------------- the CLI


def cli(sub: list[str], profile: str, target: Optional[str], variables: dict[str, str], output_json=False) -> list[str]:
    cmd = ["databricks", "bundle", *sub, "--profile", profile]
    if target:
        cmd += ["--target", target]
    for k, v in variables.items():
        cmd.append(f"--var={k}={v}")
    if output_json:
        cmd += ["-o", "json"]
    return cmd


def run_cli(cmd: list[str], cwd: pathlib.Path, say=print) -> subprocess.CompletedProcess:
    say("    $ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)


def destructive_actions(plan_out: str) -> list[str]:
    """The resources a ``bundle plan`` would delete or recreate (JSON output, else its text lines)."""
    found: list[str] = []
    try:
        data = json.loads(plan_out)
    except (json.JSONDecodeError, TypeError):
        data = None
    if data is not None:
        def walk(node, key=""):
            if isinstance(node, dict):
                action = node.get("action")
                if isinstance(action, str) and action.lower() in _DESTRUCTIVE:
                    found.append(f"{action.lower()} {key}")
                for k, v in node.items():
                    walk(v, k if isinstance(v, dict) else key)
            elif isinstance(node, list):
                for v in node:
                    walk(v, key)

        walk(data)
        return found
    for line in (plan_out or "").splitlines():
        m = re.match(r"^\s*(delete|recreate)\s+(\S+)", line, re.I)
        if m:
            found.append(f"{m.group(1).lower()} {m.group(2)}")
    return found


def summary_ids(summary: dict) -> tuple[dict, dict]:
    """({job key: id}, {pipeline key: id}) from ``bundle summary -o json``."""
    res = summary.get("resources") or {}

    def ids(kind):
        return {k: str(v.get("id")) for k, v in (res.get(kind) or {}).items() if isinstance(v, dict) and v.get("id")}

    return ids("jobs"), ids("pipelines")


_TABLE_KIND = re.compile(r"MATERIALIZED[ _]VIEW|STREAMING[ _]TABLE", re.I)
_TABLE_NAME = re.compile(r"`?([\w-]+)`?\.`?([\w-]+)`?\.`?([\w-]+)`?")
_DROP_CAUSE = re.compile(r"already exists|cannot be (?:changed|converted)|is managed by (?:another|a different) pipeline"
                         r"|change the type|not a (?:streaming table|materialized view)|TABLE_OR_VIEW_ALREADY_EXISTS", re.I)


def drop_hint(message: str) -> Optional[str]:
    """The one ``DROP NEEDED`` line for a failure that only dropping a table fixes, else None."""
    if not message or not _DROP_CAUSE.search(message):
        return None
    if not _TABLE_KIND.search(message) and "managed by" not in message.lower():
        return None
    m = _TABLE_NAME.search(message)
    name = ".".join(m.groups()) if m else "<the table named in the error>"
    kind = _TABLE_KIND.search(message)
    what = kind.group(0).upper().replace("_", " ") if kind else "TABLE"
    if what == "STREAMING TABLE" and re.search(r"MATERIALIZED[ _]VIEW", message, re.I):
        what = "MATERIALIZED VIEW"  # the old object is the one to drop
    return (f"DROP NEEDED: DROP {what} {name}  -- the pipeline cannot replace it ({message.strip()[:200]}); "
            "this command drops nothing: the orchestrator decides")


# ---------------------------------------------------------------------------------------------- the run


def _state(obj, *names):
    for n in names:
        obj = getattr(obj, n, None) if obj is not None else None
    return getattr(obj, "value", obj)


def pipeline_errors(w, pipeline_id: str, update_id: Optional[str], limit: int = 200) -> list[tuple[str, str]]:
    """(flow or event type, message) of the ERROR events of one update (newest first)."""
    import itertools

    out = []
    try:
        events = w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, filter="level='ERROR'",
                                                  order_by=["timestamp desc"])
        for ev in itertools.islice(events, limit):
            origin = getattr(ev, "origin", None)
            if update_id and getattr(origin, "update_id", None) not in (None, update_id):
                continue
            where = getattr(origin, "flow_name", None) or getattr(ev, "event_type", None) or "pipeline"
            msg = getattr(ev, "message", "") or ""
            err = getattr(ev, "error", None)
            for exc in getattr(err, "exceptions", None) or []:
                if getattr(exc, "message", None):
                    msg += " | " + exc.message
            out.append((where, msg))
    except Exception as exc:  # noqa: BLE001 - the poll goes on; the update state still decides
        out.append(("events", f"(could not list the pipeline events: {type(exc).__name__}: {str(exc)[:200]})"))
    return [e for e in out if not e[1].startswith("(could not")] or []


def latest_update(w, pipeline_id: str, since_ms: int):
    """The newest update of the pipeline created at or after ``since_ms`` (epoch ms), or None."""
    try:
        resp = w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=5)
    except Exception:  # noqa: BLE001
        return None
    ups = [u for u in (getattr(resp, "updates", None) or []) if (getattr(u, "creation_time", 0) or 0) >= since_ms]
    return max(ups, key=lambda u: u.creation_time, default=None)


_UPDATE_END = {"COMPLETED", "FAILED", "CANCELED"}
_RUN_END = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def poll(
    w,
    *,
    pipeline_ids: dict,
    run_id: Optional[int] = None,
    update: Optional[tuple[str, str]] = None,
    since_ms: int,
    timeout_s: int,
    poll_s: int,
    say=print,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[bool, list[str]]:
    """Poll the job run (``run_id``) or one pipeline update (``update`` = (pipeline_id, update_id)) to its end.

    Returns (ok, problems). Stops early (and cancels) on a failed flow of any of the bundle's pipelines or at the
    time limit."""
    start = now()
    last = None
    while True:
        line, problems, ended, ok = "", [], False, False
        if run_id is not None:
            r = w.jobs.get_run(run_id=run_id)
            life = _state(r, "state", "life_cycle_state")
            tasks = " ".join(f"{t.task_key}={_state(t, 'state', 'result_state') or _state(t, 'state', 'life_cycle_state')}"
                             for t in (r.tasks or []))
            line = f"job run {run_id}: {life} [{tasks}]"
            if life in _RUN_END:
                ended, ok = True, _state(r, "state", "result_state") == "SUCCESS"
                if not ok:
                    problems.append(f"job run {run_id} ended {_state(r, 'state', 'result_state')}: "
                                    f"{(_state(r, 'state', 'state_message') or '')[:300]}")
                    for t in r.tasks or []:
                        if _state(t, "state", "result_state") not in (None, "SUCCESS"):
                            problems.append(f"task {t.task_key}: {_state(t, 'state', 'result_state')} "
                                            f"{(_state(t, 'state', 'state_message') or '')[:300]} page={t.run_page_url}")
        for key, pid in pipeline_ids.items():
            if update is not None and pid != update[0]:
                continue
            up_id = update[1] if update is not None else None
            if up_id is None:
                u = latest_update(w, pid, since_ms)
                up_id = getattr(u, "update_id", None)
            if not up_id:
                continue
            st = _state(w.pipelines.get_update(pipeline_id=pid, update_id=up_id), "update", "state")
            line += f"  pipeline {key} update {up_id[:8]}: {st}"
            errors = pipeline_errors(w, pid, up_id) if st not in ("COMPLETED",) else []
            if errors or st == "FAILED":
                problems += [f"pipeline {key}: {where}: {msg[:400]}" for where, msg in errors[:5]]
                if st == "FAILED" and not errors:
                    problems.append(f"pipeline {key} update {up_id} FAILED")
            if update is not None and st in _UPDATE_END:
                ended, ok = True, st == "COMPLETED" and not errors
        if line != last:
            say(f"    [{int(now() - start)}s] {line.strip()}")
            last = line
        if problems and not ended:
            say("    a flow failed: stopping the run now (fail fast)")
            _cancel(w, run_id, update, say)
            return False, problems
        if ended:
            return ok and not problems, problems
        if now() - start > timeout_s:
            _cancel(w, run_id, update, say)
            return False, [f"no end within {timeout_s // 60} min (--timeout-min): cancelled"]
        sleep(poll_s)


def _cancel(w, run_id, update, say):
    try:
        if run_id is not None:
            w.jobs.cancel_run(run_id=run_id)
            say(f"    cancelled job run {run_id}")
        elif update is not None:
            w.pipelines.stop(pipeline_id=update[0])
            say(f"    stopped pipeline {update[0]}")
    except Exception as exc:  # noqa: BLE001
        say(f"    (could not cancel: {type(exc).__name__}: {str(exc)[:200]})")


# ---------------------------------------------------------------------------------------------- the tables


def _count(w, warehouse_id: str, table: str, wait_s: int = 120, sleep=time.sleep) -> int:
    resp = w.statement_execution.execute_statement(statement=f"SELECT count(*) FROM {table}", warehouse_id=warehouse_id,
                                                   wait_timeout="50s")
    waited = 0
    while _state(resp, "status", "state") in ("PENDING", "RUNNING") and waited < wait_s:
        sleep(5)
        waited += 5
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = _state(resp, "status", "state")
    if state != "SUCCEEDED":
        err = _state(resp, "status", "error", "message")
        raise RuntimeError(f"{state}: {(err or '')[:200]}")
    return int(resp.result.data_array[0][0])


def check_tables(w, pipeline_id: str, warehouse_id: str, allow_empty=(), say=print) -> list[str]:
    """Problems with the tables the pipeline owns in its catalog and schema: none found, or an empty one."""
    spec = getattr(w.pipelines.get(pipeline_id=pipeline_id), "spec", None)
    catalog, schema = getattr(spec, "catalog", None), getattr(spec, "schema", None) or getattr(spec, "target", None)
    if not catalog or not schema:
        return [f"pipeline {pipeline_id} names no catalog and schema: cannot find its tables"]
    owned = sorted((t for t in w.tables.list(catalog_name=catalog, schema_name=schema)
                    if getattr(t, "pipeline_id", None) == pipeline_id), key=lambda t: t.name)
    if not owned:
        return [f"no table of {catalog}.{schema} is owned by pipeline {pipeline_id}"]
    problems = []
    allow = {a.split(".")[-1] for a in allow_empty}
    for t in owned:
        full = f"{catalog}.{schema}.{t.name}"
        try:
            n = _count(w, warehouse_id, full)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{full}: cannot count ({type(exc).__name__}: {str(exc)[:200]})")
            continue
        kind = _state(t, "table_type") or ""
        say(f"    {full} ({kind}): {n} row(s)")
        if n == 0 and t.name not in allow:
            problems.append(f"{full} is empty")
    return problems


# ---------------------------------------------------------------------------------------------- main


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--bundle", required=True, help=f"a bundle name ({', '.join(sorted(BUNDLES))}) or its directory")
    p.add_argument("--profile", default=None, help="Databricks CLI profile (required)")
    p.add_argument("--target", default=None, help="bundle target (default: the bundle's default target)")
    p.add_argument("--wheel", default=None, help="the env wheel for --var gbx_wheel (default: the versioned staged env6 wheel)")
    p.add_argument("--var", action="append", default=[], metavar="NAME=VALUE", help="a bundle variable (repeatable)")
    p.add_argument("--job", default=None, help="the job to run (default: the bundle's only job)")
    p.add_argument("--only", default=None, metavar="TASKS", help="comma-separated task keys of the job to run")
    p.add_argument("--pipeline-only", action="store_true", help="start one update of the bundle's pipeline, not the job")
    p.add_argument("--full-refresh", action="store_true", help="a full refresh of the pipeline (job or update)")
    p.add_argument("--validate-only", action="store_true", help="validate and plan; deploy and run nothing")
    p.add_argument("--no-run", action="store_true", help="validate, plan and deploy; run nothing")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse for the row counts")
    p.add_argument("--no-table-check", action="store_true", help="skip the table check")
    p.add_argument("--allow-empty", action="append", default=[], metavar="TABLE", help="a table that may be empty")
    p.add_argument("--timeout-min", type=int, default=DEFAULT_TIMEOUT_MIN, help="the run's limit; cancelled after it")
    p.add_argument("--poll-secs", type=int, default=DEFAULT_POLL_SECS)
    args = p.parse_args(argv)

    if not args.profile:
        p.error("--profile is required: no profile is picked for you")
    if args.timeout_min < 1:
        p.error("--timeout-min must be at least 1")
    bundle_dir = resolve_bundle(args.bundle)
    text = bundle_text(bundle_dir)
    variables = {}
    for kv in args.var:
        k, sep, v = kv.partition("=")
        if not sep or not k.strip():
            p.error(f"--var {kv!r}: expected NAME=VALUE")
        variables[k.strip()] = v
    if "gbx_wheel" in variables and args.wheel:
        p.error("give the wheel once: --wheel or --var gbx_wheel=...")
    wheel = variables.pop("gbx_wheel", None) or args.wheel or default_wheel()
    problem = wheel_problem(wheel, text)
    if problem:
        p.error(problem)
    variables = {"gbx_wheel": wheel, **variables}
    if not args.validate_only and not args.no_run and not args.no_table_check and not args.warehouse_id:
        p.error("--warehouse-id is required for the table check (or --no-table-check)")

    name = bundle_name(text) or bundle_dir.name
    mode = "validate + plan" if args.validate_only else "deploy" if args.no_run else (
        "pipeline update" if args.pipeline_only else f"job {args.job or '(the only one)'}"
        + (f" tasks {args.only}" if args.only else ""))
    print(f"=== gbx:test:sdp {name} ===")
    print(f"    bundle   : {bundle_dir}")
    print(f"    profile  : {args.profile}   target: {args.target or '(default)'}")
    print(f"    wheel    : {wheel}")
    print(f"    vars     : {', '.join(f'{k}={v}' for k, v in variables.items() if k != 'gbx_wheel') or '(defaults)'}")
    print(f"    runs     : {mode}{' (full refresh)' if args.full_refresh else ''}; limit {args.timeout_min} min, "
          "cancelled after it; a failed flow stops it")
    print("    expects  : every table the pipeline owns exists and holds rows"
          + ("" if not args.no_table_check else " (table check skipped)"))

    steps = [("validate", ["validate"], True), ("plan", ["plan"], True)]
    for step, sub, as_json in steps:
        res = run_cli(cli(sub, args.profile, args.target, variables, output_json=as_json), bundle_dir)
        if res.returncode != 0:
            print(f"!!! bundle {step} failed (exit {res.returncode}):\n{(res.stderr or res.stdout)[-3000:]}")
            return 1
        if step == "plan":
            gone = destructive_actions(res.stdout)
            if gone:
                print("DESTRUCTIVE PLAN: " + "; ".join(gone) + " -- deleting a pipeline drops its tables; this "
                      "command deploys nothing destructive: the orchestrator decides")
                return 1
            print("    plan: no delete or recreate")
    if args.validate_only:
        print("=== VALIDATED ===")
        return 0

    w = None
    if not args.no_run:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(profile=args.profile)
        if not args.no_table_check:  # a long run must not end on a deleted warehouse
            sys.path.insert(0, str(_SCRIPT_DIR))
            from _uc_warehouse import warehouse_problem

            problem = warehouse_problem(w, args.warehouse_id, profile=args.profile)
            if problem:
                print(f"!!! {problem}")
                return 2

    res = run_cli(cli(["deploy"], args.profile, args.target, variables), bundle_dir)
    if res.returncode != 0:
        out = (res.stderr or "") + (res.stdout or "")
        hint = drop_hint(out)
        print(f"!!! bundle deploy failed (exit {res.returncode}):\n{out[-3000:]}")
        if hint:
            print(hint)
        return 1
    res = run_cli(cli(["summary"], args.profile, args.target, variables, output_json=True), bundle_dir)
    if res.returncode != 0:
        print(f"!!! bundle summary failed:\n{(res.stderr or res.stdout)[-2000:]}")
        return 1
    jobs_, pipelines = summary_ids(json.loads(res.stdout))
    print(f"    deployed: jobs {jobs_ or '{}'}  pipelines {pipelines or '{}'}")
    if args.no_run:
        print("=== DEPLOYED ===")
        return 0
    if not pipelines:
        print("!!! the bundle deployed no pipeline")
        return 1

    from databricks.sdk.service import jobs as jobs_api

    since_ms = int(time.time() * 1000) - 5_000
    run_id, update = None, None
    if args.pipeline_only:
        if len(pipelines) != 1:
            print(f"!!! --pipeline-only needs one pipeline; the bundle has {sorted(pipelines)}")
            return 1
        pid = next(iter(pipelines.values()))
        up = w.pipelines.start_update(pipeline_id=pid, full_refresh=args.full_refresh)
        update = (pid, up.update_id)
        print(f"    started pipeline update {up.update_id} of {pid}")
    else:
        key = args.job or (next(iter(jobs_)) if len(jobs_) == 1 else None)
        if key not in jobs_:
            print(f"!!! --job: the bundle's jobs are {sorted(jobs_)}")
            return 1
        kw = {"job_id": int(jobs_[key]), "idempotency_token": uuid.uuid4().hex}
        if args.only:
            kw["only"] = [t.strip() for t in args.only.split(",") if t.strip()]
        if args.full_refresh:
            kw["pipeline_params"] = jobs_api.PipelineParams(full_refresh=True)
        run_id = w.jobs.run_now(**kw).run_id
        print(f"    job run {run_id}: {w.jobs.get_run(run_id=run_id).run_page_url}")

    ok, problems = poll(w, pipeline_ids=pipelines, run_id=run_id, update=update, since_ms=since_ms,
                        timeout_s=args.timeout_min * 60, poll_s=args.poll_secs)
    if not ok:
        print("!!! the run failed:")
        for prob in problems:
            print("    - " + prob)
        hints = {h for h in (drop_hint(x) for x in problems) if h}
        for h in sorted(hints):
            print(h)
        return 1
    print("    the run SUCCEEDED")
    if args.no_table_check:
        return 0
    problems = []
    for key, pid in pipelines.items():
        print(f"    tables of pipeline {key}:")
        problems += check_tables(w, pid, args.warehouse_id, args.allow_empty)
    if problems:
        print("!!! table check failed:")
        for prob in problems:
            print("    - " + prob)
        return 1
    print("=== SDP RUN AND TABLES OK ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
