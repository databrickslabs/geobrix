#!/usr/bin/env python3
"""
Push the heavy-vs-light benchmark notebook to the workspace and run it on a configured cluster.

Both APIs (heavyweight Scala/Spark + lightweight pyrx) run on the SAME cluster, giving a
true same-hardware comparison. The script builds a bench notebook (via the bench cluster
notebook builder), uploads it, and runs it as a one-off job. Results append to the
bench_results Delta table and land comparison.csv / summary.md under the out_dir on the
configured Volume.

The cluster + artifacts must be provisioned by the operator (see the installation docs):
- heavyweight: a dedicated cluster on DBR 17.3, 18 or 19 (x86; Graviton on DBR 19) with the env wheel
  and the production geobrix fat JAR installed as cluster libraries, and the bench geobrix-*-tests.jar
  staged on a Volume (the tests.jar is attached here as a job library; the production fat JAR is
  NOT attached here).
- lightweight (incl. ARM): the env wheel for the compute's runtime, geobrix-<version>+env5 (DBR 17.3 /
  18, Serverless env 5) or +env6 (DBR 19, Serverless env 6), installed by the notebook's install cell
  (no light extra). Without GBX_BUNDLE_WHEEL_VOLUME_PATH the launcher picks it from the cluster's
  Databricks Runtime (or --env-version on Serverless). Use --lightweight-only on a cluster
  that has no GeoBrix JAR.

Requires: databricks-sdk, and env config (see databricks_cluster_config.example.env).

Usage:
  1. Copy databricks_cluster_config.example.env to databricks_cluster_config.env.
  2. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*.
  3. Optional: set GBX_BENCH_CORPUS, GBX_BENCH_RESULTS_TABLE, GBX_BENCH_TESTS_JAR_VOLUME_PATH,
     GBX_BUNDLE_WHEEL_VOLUME_PATH.
  4. Run: python push_and_run_bench_on_cluster.py [options]
     Options: --no-wait, --heavyweight-only, --lightweight-only, --run-id, --functions,
              --modes, --row-counts, --warmup, --measured,
              --existing-cluster-id <id> (attach to a warm all-purpose cluster; skips
                4-8 min job-cluster startup; overrides the CLUSTER_ID env var),
              --truncate-results (clear only this run_id + this invocation's tier(s)),
              --truncate-all (empty the whole table -> only the current run remains)

Hardening (every run): a SMOKE stage runs first (--no-smoke skips it, logged): each form of the chosen --levels
set once at --smoke-rows rows plus a worker-side import check; only a clean smoke submits the real run. A local
preflight refuses the launch with no compute (--skip-preflight skips it, logged). A watchdog polls the notebook's
heartbeat and cancels a run whose heartbeat stopped (--stale-min) or whose form passed --form-cap-min (times the scale, held to --form-cap-max-min), printing
`STALLED: <form> after <m> min` and the driver stack, exit 3. --smoke-only stops after a clean smoke. Every run has a time limit (--timeout-min, default 360;
the smoke stage --smoke-timeout-min, default 15). --inject-stall <secs> adds a form that
sleeps on the driver (test only; proves the watchdog).

     Without --run-id a launch gets an id of its own (cluster-<date>-<time>-<4 hex>), so
     the rows of an earlier run are reached only by naming its id: --resume and
     --truncate-results need an explicit --run-id (with a fresh id, --truncate-results
     has nothing to clear).
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Optional
from _job_submit import submit_once  # sibling module (script dir is on sys.path)
from _cli import exit_if_help  # sibling module (script dir is on sys.path)
import _preflight  # sibling module (script dir is on sys.path)
import _run_watchdog  # sibling module (script dir is on sys.path)
from _preflight import serverless_dependencies

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"


def _strip_invisible(s: str) -> str:
    """Remove BOM and common invisible Unicode so env-derived paths are clean."""
    s = (s or "").strip()
    for c in ("﻿", "​", "‌", "‍", "\r"):
        s = s.replace(c, "")
    return s.strip()


if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = _strip_invisible(v)


def _geobrix_version() -> str:
    """Read version from python package __init__.py (avoid heavy imports)."""
    init_py = (
        TESTS_DIR.parent.parent
        / "python"
        / "geobrix"
        / "src"
        / "databricks"
        / "labs"
        / "gbx"
        / "__init__.py"
    )
    if init_py.exists():
        with open(init_py) as f:
            for line in f:
                line = line.strip()
                if line.startswith("__version__"):
                    # __version__ = "0.4.0"
                    if "=" in line:
                        v = line.split("=", 1)[1].strip().strip("'\"").strip()
                        if v:
                            return v
    return "0.4.0"


_BENCH_SRC = TESTS_DIR.parent.parent / "python/geobrix/src/databricks/labs/gbx/bench"


def _function_names(functions: str) -> list:
    """The names in a ``--functions`` list: comma-separated, any blanks around a name dropped."""
    return [x.strip() for x in functions.split(",") if x.strip()]


def _unknown_bng_names(functions: str) -> list:
    """The ``bng_*`` names in a ``--functions`` list that no BNG bench leg or registered bench
    function answers to.

    The on-cluster notebook rejects such a name too, but only after the job has started; this
    stops a typo before anything is submitted. The names come from the same source files the
    cluster run reads (the leg table in ``cluster.py``, the registry in ``spec.py``), read as text
    so the launcher needs no Spark.
    """
    import re

    cluster = (_BENCH_SRC / "cluster.py").read_text()
    spec = (_BENCH_SRC / "spec.py").read_text()
    legs = re.findall(r'^\s*\("(bng_\w+)", dict\(', cluster, re.M)
    registered = re.findall(r'^    "(bng_\w+)": FnSpec\(', spec, re.M)
    known = set(legs) | set(registered)
    return [x for x in _function_names(functions) if x.startswith("bng_") and x not in known]


def env_of_runtime(spark_version: str, fallback=None) -> int:
    """The env wheel family of a Databricks Runtime ``spark_version`` key: 6 for DBR 19 and later, else 5
    (DBR 17.3 and 18). A key with no leading runtime number (a ``custom:`` DCS image) takes ``fallback``
    (``--env-version``); with none given, ValueError names the key and the flag."""
    try:
        return 6 if int(spark_version.split(".", 1)[0]) >= 19 else 5
    except ValueError:
        if fallback is not None:
            return int(fallback)
        raise ValueError(
            f"ERROR: cannot tell the env wheel from spark_version {spark_version!r}: "
            "pass --env-version 5|6 (or a wheel with GBX_BUNDLE_WHEEL_VOLUME_PATH)."
        ) from None


def _arg(flag: str, default: str) -> str:
    """Read --flag <value> from argv, else default."""
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def _minutes_arg(flag: str, default: int) -> int:
    """A time limit in whole minutes (at least 1) from argv. ValueError, one line, when it is anything else:
    0 would reach the Jobs API as "no limit" and a bad value must stop before any work."""
    raw = _arg(flag, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        raise ValueError(f"ERROR: {flag} must be a whole number of minutes, at least 1 (got {raw!r}).")
    return value


def _default_run_id() -> str:
    """A run id of its own for a launch that names none: its rows, its out_dir and its
    notebook are then never another launch's."""
    return f"cluster-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def _remove_notebook(w, path: str, succeeded: bool = True) -> None:
    """Delete the run's notebook once the run has succeeded (its output stays with the run).

    A run that did not succeed keeps its notebook: a Jobs repair run re-runs it."""
    if not succeeded:
        print(f"Kept the run's notebook {path} (the run did not succeed; delete it when done).")
        return
    try:
        w.workspace.delete(path)
        print(f"Removed the run's notebook {path}")
    except Exception as e:  # noqa: BLE001 - best effort; say what is left
        print(f"WARNING: could not remove {path} ({e}); delete it by hand.", file=sys.stderr)


def _discover_warehouse(w):
    """First RUNNING SQL warehouse (else any) for live progress counts; None if none.
    Used only for the host-side progress heartbeat -- never required for the run."""
    try:
        whs = list(w.warehouses.list())
    except Exception:
        return None
    if not whs:
        return None

    def _st(x):
        s = getattr(x, "state", None)
        return s.value if s is not None and hasattr(s, "value") else str(s)

    running = [x for x in whs if _st(x) == "RUNNING"]
    return (running or whs)[0].id


def _count_run_rows(w, warehouse_id: str, table: str, run_id: str, apis=None) -> int:
    """COUNT(*) of THIS run + THIS invocation's tier(s) in the table; -1 on any error.
    Filters by run_id AND api so a live count is accurate even when light and heavy
    share a run_id in the same table (else it would mix both tiers). Interim rows stream
    in as each function finishes, so this climbs during the run."""
    api_clause = ""
    if apis:
        api_clause = " AND api IN (" + ", ".join(f"'{a}'" for a in apis) + ")"
    try:
        # count(*) over a small filtered table returns well under this; the wait_timeout
        # is just the cap before execute_statement would return PENDING. Keep it modest
        # so a tight poll loop isn't blocked behind a long wait.
        res = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT count(*) FROM {table} WHERE run_id = '{run_id}'{api_clause}",
            wait_timeout="10s",
        )
        data = res.result.data_array if res and res.result else None
        return int(data[0][0]) if data else 0
    except Exception:
        return -1


def _local_size_sweep_count() -> int:
    """# of size-sweep tiles in the locally-staged corpus (for pure-core expected rows).
    None if the local corpus.json can't be read."""
    try:
        local = (
            TESTS_DIR.parent.parent
            / "sample-data"
            / "Volumes"
            / "main"
            / "default"
            / "bench-corpus"
            / "corpus.json"
        )
        if not local.exists():
            return None
        with open(local) as fh:
            c = json.load(fh)
        return len(c.get("size_sweep", []))
    except Exception:
        return None


def _expected_rows(
    functions: str,
    sel: str,
    modes: str,
    row_counts: str,
    lightweight: bool,
    heavyweight: bool,
    benchmark_readers: bool = False,
    readers_only: bool = False,
    benchmark_pmtiles: bool = False,
    pmtiles_only: bool = False,
    benchmark_vector: bool = False,
    vector_only: bool = False,
) -> int:
    """Best-effort total rows this run will stream, for an 'N of EXPECTED' progress
    display. Only reliable for a LIGHTWEIGHT-ONLY run: the lightweight fn set + corpus
    are known host-side. The heavyweight fn count is decided in Scala (BenchDispatch),
    not visible here, so any run that includes heavy returns None (count shown without a
    denominator) rather than a misleading number."""
    # Reader + writer rows: 2 per tier that is active (1 read + 1 write; light + heavy = up to 4).
    reader_rows = 2 * ((1 if lightweight else 0) + (1 if heavyweight else 0))
    # PMTiles rows: 1 per active tier (1 write per tier).
    pmtiles_rows = (1 if lightweight else 0) + (1 if heavyweight else 0)
    # Vector rows: 4 formats × up to 2 tiers (1 read per format per tier).
    _n_vformats = 4
    vector_rows = _n_vformats * ((1 if lightweight else 0) + (1 if heavyweight else 0))

    if readers_only:
        return reader_rows or None

    if pmtiles_only:
        return pmtiles_rows or None

    if vector_only:
        return vector_rows or None

    if not lightweight or heavyweight:
        # heavyweight included -> fn count unknown -> return None, but add reader rows if requested
        if benchmark_readers and reader_rows:
            return None  # still unknown due to heavyweight fn count
        return None
    try:
        sys.path.insert(0, str(TESTS_DIR.parent.parent / "python" / "geobrix" / "src"))
        from databricks.labs.gbx.bench import spec as _spec

        fns = _spec.select(
            functions=_function_names(functions) or None, set=sel
        )
    except Exception:
        return None
    rc = [int(x) for x in row_counts.split(",") if x]
    total = 0
    if modes in ("spark-path", "both"):
        total += sum(1 for f in fns if "spark-path" in f.modes) * max(1, len(rc))
    if modes in ("pure-core", "both"):
        n_size = _local_size_sweep_count()
        if n_size is None:
            return None  # can't be exact -> don't show a misleading denominator
        total += sum(1 for f in fns if "pure-core" in f.modes) * n_size
    if benchmark_readers:
        total += reader_rows
    if benchmark_pmtiles:
        total += pmtiles_rows
    if benchmark_vector:
        total += vector_rows
    return total or None



EXIT_STALLED = _run_watchdog.EXIT_STALLED
EXIT_SMOKE_FAILED = 4
EXIT_PREFLIGHT = 5
SMOKE_FORM_CAP_MIN = 5.0  # a form at the smoke's few rows that runs longer than this is stuck, not slow
HEARTBEAT_READ_SECS = 15.0
DEFAULT_RUN_TIMEOUT_MIN = 360  # the real run's time limit, a levels leg included (its 1x/4x/16x ladder runs long)
DEFAULT_SMOKE_TIMEOUT_MIN = 15


def levels_scales_problem(levels_mvp, levels_scales, root) -> Optional[str]:
    """Why the levels ladder can never pass the client-input cap, or None when every scale fits.

    The SE inputs are 4 MiB DEM tiles built on the client (cap: 64 MiB), so only 1x fits; the default ladder 1,4,16
    would fail on the cluster, after the cold start. The message names ``--levels-scales`` and the scales that fit.
    """
    if not levels_mvp:
        return None
    sys.path.insert(0, str(Path(root) / "python/geobrix/src"))
    try:
        from databricks.labs.gbx.bench import clientcap, preflight
    except ImportError:  # no pyspark here: the form table cannot be read, and the preflight says so
        return None

    if not preflight.client_cap_problems(levels_mvp, levels_scales):
        return None
    fitting = [s for s in levels_scales if not preflight.client_cap_problems(levels_mvp, (s,))]
    wanted = ",".join(str(s) for s in levels_scales)
    if fitting:
        fix = f"use --levels-scales {','.join(str(s) for s in fitting)} (the requested scales that fit)"
    else:
        fix = "use --levels-scales 1 (no requested scale fits; 1x is the base row count)"
    return (
        f"--levels {levels_mvp} with --levels-scales {wanted} builds inputs on the client above the "
        f"{clientcap.MAX_CLIENT_BYTES // 2**20} MiB cap and would fail on the cluster: {fix}"
    )


def launch_problems(w, *, serverless, wheel, env_version, levels_mvp, levels_scales, root) -> list:
    """What makes this launch fail, found with no compute: the Serverless environment spec, the staged wheel against
    the local build, and (for a levels run) the probe SQL on a local Spark session and the client-side input sizes."""
    problems = []
    if serverless:
        problems += _preflight.check_env_spec(serverless_dependencies(wheel), wheel, env_version)
    if wheel.startswith("/Volumes/"):
        try:
            problems += _preflight.wheel_problems(w, wheel, root)
        except Exception as exc:  # noqa: BLE001 - an unreadable staged wheel is a launch problem too
            problems.append(f"cannot read the staged wheel {wheel}: {type(exc).__name__}: {exc}")
    if levels_mvp:
        sys.path.insert(0, str(Path(root) / "python/geobrix/src"))
        problems += _preflight.probe_problems(levels_mvp, levels_scales, root)
    return problems


class Outcome:
    """How a watched run ended: its final ``run``, the notebook's exit ``payload``, the ``stall`` (or None)."""

    def __init__(self, run=None, payload=None, stall=None, seconds=0.0):
        self.run, self.payload, self.stall, self.seconds = run, payload, stall, seconds


def _life_and_result(run):
    st = run.state

    def val(x):
        return x.value if x is not None and hasattr(x, "value") else (str(x) if x is not None else "")

    return (val(st.life_cycle_state) if st else "?"), (val(st.result_state) if st else "")


def watch_to_end(
    w, run_id, *, hb_path, form_cap_s, stale_s, token=None, count=None, poll_secs=5, form_cap_max_s=None
) -> Outcome:
    """Poll the run to its end with the watchdog; a stalled run is cancelled and reported (exit 3, not retried).

    ``count`` is ``(warehouse_id, table, run_id, apis, expected)`` for the live row count of a real run.
    """
    import time

    try:
        w.files.delete(hb_path)  # a file an earlier run left there (the token below is the guard if this fails)
    except Exception:  # noqa: BLE001 - absent is the usual case
        pass
    t0 = time.monotonic()
    last = {"beat": None, "at": -1e9, "rows": -1, "stale": 0, "run": None, "read_error": None}

    def get_state():
        try:
            last["run"] = w.jobs.get_run(run_id)
        except Exception as exc:  # noqa: BLE001 - transient: keep polling
            print(f"  (poll error: {exc})", file=sys.stderr)
            return ("RUNNING", "")
        return _life_and_result(last["run"])

    def beat():
        if time.monotonic() - last["at"] >= HEARTBEAT_READ_SECS:
            last["at"] = time.monotonic()
            try:
                got = _run_watchdog.read_beat(w, hb_path)
            except _run_watchdog.BeatReadError as exc:
                last["read_error"] = exc  # stays until a read works: the polls between reads are failures too
            else:
                last["read_error"] = None
                if got is not None:
                    last["beat"] = got
        if last["read_error"] is not None:
            raise last["read_error"]
        return last["beat"]

    def on_poll(lc):
        if not count or not count[0]:
            return
        warehouse_id, table, rid, apis, expected = count
        of = f" (of {expected})" if expected else ""
        c = _count_run_rows(w, warehouse_id, table, rid, apis)
        if c > last["rows"]:
            print(f"  [{lc}] {c}{of} rows streamed to table so far")
            last["rows"], last["stale"] = c, 0
        else:
            last["stale"] += 1
            if last["stale"] % 6 == 0:  # ~30s with no new rows -> show we're alive
                b = last["beat"] or {}
                where = f"; heartbeat: {b['form']} {b.get('form_elapsed_s', 0):.0f} s" if b.get("form") else ""
                print(f"  [{lc}] still {max(last['rows'], 0)}{of} rows (current fn taking a while{where})")

    kind, what = _run_watchdog.watch_run(
        get_state=get_state,
        read_beat_fn=beat,
        cancel=lambda: w.jobs.cancel_run(run_id),
        on_stall_dump=lambda: _run_watchdog.faulthandler_tail(w, run_id),
        on_poll=on_poll,
        watchdog=_run_watchdog.Watchdog(
            form_cap_s=form_cap_s, stale_s=stale_s, token=token, form_cap_max_s=form_cap_max_s
        ),
        poll_secs=poll_secs,
        run_id=run_id,
    )
    seconds = time.monotonic() - t0
    if kind == "stalled":
        return Outcome(stall=what, seconds=seconds)
    run = w.jobs.get_run(run_id)
    payload = None
    try:
        out = w.jobs.get_run_output(run.tasks[0].run_id) if run.tasks else None
        if out and getattr(out, "notebook_output", None) and out.notebook_output.result:
            payload = out.notebook_output.result
    except Exception:  # noqa: BLE001 - the payload is a convenience
        pass
    return Outcome(run=run, payload=payload, seconds=seconds)


def _smoke_result(outcome) -> dict:
    try:
        return json.loads(outcome.payload) if outcome.payload else {}
    except ValueError:
        return {}


def smoke_problems(outcome, *, require_forms: bool, allow_errors: bool = False) -> list:
    """Why a finished smoke run is not clean (empty when it is)."""
    lc, result = _life_and_result(outcome.run)
    problems = []
    if result != "SUCCESS":
        problems.append(f"the smoke run ended {lc}/{result or '?'}: {outcome.run.state.state_message if outcome.run.state else ''}")
    res = _smoke_result(outcome)
    if not res:
        problems.append("the smoke run returned no result (the notebook did not reach its exit)")
        return problems
    chk = res.get("import_check") or {}
    if not chk.get("ok"):
        problems.append(f"worker-side import check failed: {chk.get('error', 'no result')}")
    if require_forms and chk.get("ok") and not res.get("forms"):
        problems.append("no form ran at the smoke (nothing in the chosen set runs on this compute)")
    if res.get("errors") and not allow_errors:
        for e in res["errors"]:
            problems.append(f"form {e['fn']} {e['level']}/{e['form']} failed: {e['failure']}")
    return problems


def print_smoke(outcome) -> None:
    res = _smoke_result(outcome)
    chk = res.get("import_check") or {}
    if chk.get("ok"):
        print(f"  worker import check: ok (geobrix {chk.get('version')} on {chk.get('arch')})")
    forms = res.get("forms") or []
    if forms:
        print(f"  {'fn':<34}{'level':<9}{'form':<22}{'status':<14}{'tasks':>6}{'median_s':>10}")
        for f in forms:
            print(
                f"  {f['fn']:<34}{f['level']:<9}{f['form']:<22}{f['status']:<14}{f.get('tasks', 0):>6}{f.get('median_s', 0):>10}"
            )
        few = [f for f in forms if f["status"] == "ok" and 0 < f.get("tasks", 0) <= 2]
        if few:
            print(
                f"  NOTE: {len(few)} form(s) run on 1-2 Spark tasks whatever the cluster size "
                "(their input is one or two files): their times are per task, not cluster throughput."
            )


def run_smoke_stage(
    w, submit, *, do_smoke, no_smoke, smoke_only, smoke_path, run_id, out_dir, smoke_rows, smoke_timeout_min,
    form_cap_s, stale_s, token, levels_mvp, allow_errors,
) -> Optional[int]:  # fmt: skip
    """Run (or skip) the smoke stage before the real run. Returns an exit code when the launch must end here (a
    stalled or failed smoke, or ``--smoke-only``), None when the real run goes on. A bad smoke submits nothing else."""
    if not do_smoke:
        print(
            "SMOKE STAGE SKIPPED (--no-smoke): the real run is submitted without proving that each form can run."
            if no_smoke
            else "SMOKE STAGE SKIPPED (--explain-only runs no timed form)."
        )
        return None
    print("=" * 64)
    print(
        f"SMOKE STAGE: each form of the chosen set once at {smoke_rows} rows, plus a worker-side import check "
        f"(time limit {smoke_timeout_min} min, form cap {min(form_cap_s / 60.0, SMOKE_FORM_CAP_MIN):g} min)"
    )
    smoke_id = submit(smoke_path, f"geobrix-bench-{run_id}-smoke", smoke_timeout_min * 60)
    outcome = watch_to_end(
        w, smoke_id, hb_path=f"{out_dir}-smoke/heartbeat.json",
        form_cap_s=min(form_cap_s, SMOKE_FORM_CAP_MIN * 60.0), stale_s=stale_s, token=token,
    )  # fmt: skip
    if outcome.stall:
        print("SMOKE STAGE STALLED: nothing else is submitted.", file=sys.stderr)
        return EXIT_STALLED
    problems = smoke_problems(outcome, require_forms=bool(levels_mvp), allow_errors=allow_errors)
    print_smoke(outcome)
    if problems:
        print("SMOKE STAGE FAILED: nothing else is submitted.", file=sys.stderr)
        for pr in problems:
            print(f"  - {pr}", file=sys.stderr)
        return EXIT_SMOKE_FAILED
    if smoke_only:
        print(f"SMOKE STAGE CLEAN ({outcome.seconds:.0f} s): --smoke-only, the real run is not submitted.")
        return 0
    print(f"SMOKE STAGE CLEAN ({outcome.seconds:.0f} s): submitting the real run.")
    return None


def main() -> int:
    exit_if_help(__doc__)
    # Force line-buffered stdout so the run URL + interim progress heartbeats appear
    # LIVE, not all at once at exit. Python block-buffers stdout when it isn't a TTY
    # (piped to a file, a log, or a background task), which otherwise hides every
    # interim print until the process ends.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    do_wait = "--no-wait" not in sys.argv
    # Run hardening (see the module docstring): the smoke stage, the preflight, the watchdog and the time limits.
    no_smoke = "--no-smoke" in sys.argv
    smoke_only = "--smoke-only" in sys.argv
    if smoke_only and no_smoke:
        print("ERROR: --smoke-only and --no-smoke exclude each other.", file=sys.stderr)
        return 2
    skip_preflight = "--skip-preflight" in sys.argv
    smoke_allow_errors = "--smoke-allow-errors" in sys.argv
    smoke_rows = int(_arg("--smoke-rows", "16"))
    inject_stall = float(_arg("--inject-stall", "0"))
    form_cap_min = float(_arg("--form-cap-min", "15"))
    # --form-cap-max-min: the ceiling on a form's cap after the scale multiply (15 min x 16 would be 4 h).
    form_cap_max_min = float(_arg("--form-cap-max-min", "60"))
    if form_cap_max_min <= 0:
        print("ERROR: --form-cap-max-min must be a positive number of minutes.", file=sys.stderr)
        return 2
    stale_min = float(_arg("--stale-min", "5"))
    try:  # the run time limits are checked here, before the preflight, the uploads or the smoke
        smoke_timeout_min = _minutes_arg("--smoke-timeout-min", DEFAULT_SMOKE_TIMEOUT_MIN)
        timeout_min = _minutes_arg("--timeout-min", DEFAULT_RUN_TIMEOUT_MIN)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2
    heavyweight = "--lightweight-only" not in sys.argv
    lightweight = "--heavyweight-only" not in sys.argv
    # --serverless: submit to Serverless compute instead of an existing cluster. Light-only (no JAR).
    # --env-version N: Serverless environment version to request (default 6, the +env6 wheel's).
    # Skips the cluster_id requirement.
    serverless = "--serverless" in sys.argv
    env_version = _arg("--env-version", "6")
    if serverless:
        # Serverless has no JAR; force light-only regardless of --heavyweight-only.
        heavyweight = False
        lightweight = True
    # --explain-only: print/persist each spark-path fn's physical plan, run nothing timed.
    # It's a lightweight spark-path-only diagnostic -- force that scope so the heavy JVM
    # path (which can't .explain) and the pure-core path stay out entirely.
    explain_only = "--explain-only" in sys.argv
    if explain_only:
        heavyweight = False
        lightweight = True
    # --benchmark-readers: also run the reader benchmark (raster_gbx vs gdal) on-cluster.
    # --readers-only: ONLY run the reader benchmark, skip all fn benchmarks.
    benchmark_readers = "--benchmark-readers" in sys.argv
    readers_only = "--readers-only" in sys.argv
    # --benchmark-pmtiles: also run the pmtiles writer benchmark on-cluster.
    # --pmtiles-only: ONLY run the pmtiles benchmark, skip all fn benchmarks.
    benchmark_pmtiles = "--benchmark-pmtiles" in sys.argv
    pmtiles_only = "--pmtiles-only" in sys.argv
    # --benchmark-vector: also run the vector reader benchmark on-cluster.
    # --vector-only: ONLY run the vector reader benchmark, skip all fn benchmarks.
    # --vector-scale: read the scaled 1M-seed corpus (copies dir + seed file) instead
    #   of the tiny 4-file corpus. Requires the scaled corpus staged at
    #   {CORPUS}/vector-scale/<fmt>/. Only meaningful when benchmark_vector or vector_only.
    benchmark_vector = "--benchmark-vector" in sys.argv
    vector_only = "--vector-only" in sys.argv
    vector_scale = "--vector-scale" in sys.argv
    # --benchmark-mvt: also run the st_asmvt benchmark (light pyvx vs heavy vectorx).
    # --mvt-only: ONLY run the MVT benchmark, skip all fn benchmarks.
    benchmark_mvt = "--benchmark-mvt" in sys.argv
    mvt_only = "--mvt-only" in sys.argv
    # --benchmark-pmtiles-agg: also run the pmtiles_agg grouped-agg benchmark (light vs heavy).
    # --pmtiles-agg-only: ONLY run the pmtiles_agg benchmark, skip all fn benchmarks.
    benchmark_pmtiles_agg = "--benchmark-pmtiles-agg" in sys.argv
    pmtiles_agg_only = "--pmtiles-agg-only" in sys.argv
    # --benchmark-vector-tin: also run the TIN + legacy benchmark (light pyvx vs heavy vectorx).
    # --vector-tin-only: ONLY run the TIN + legacy benchmark, skip all fn benchmarks.
    benchmark_vector_tin = "--benchmark-vector-tin" in sys.argv
    vector_tin_only = "--vector-tin-only" in sys.argv
    # --benchmark-grid-quadbin: also run the quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
    # --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip all fn benchmarks.
    benchmark_grid_quadbin = "--benchmark-grid-quadbin" in sys.argv
    grid_quadbin_only = "--grid-quadbin-only" in sys.argv
    # --benchmark-grid-bng: also run the BNG grid benchmark (light pygx vs heavy gridx.bng).
    # --grid-bng-only: ONLY run the BNG grid benchmark, skip all fn benchmarks.
    benchmark_grid_bng = "--benchmark-grid-bng" in sys.argv
    grid_bng_only = "--grid-bng-only" in sys.argv
    # --benchmark-grid-custom: also run the custom grid benchmark (light pygx vs heavy gridx.custom).
    # --grid-custom-only: ONLY run the custom grid benchmark, skip all fn benchmarks.
    benchmark_grid_custom = "--benchmark-grid-custom" in sys.argv
    grid_custom_only = "--grid-custom-only" in sys.argv
    # --benchmark-fanout: also run the fan-out UDTF benchmark (rst_polygonize +
    #   rst_h3_rastertogridcount), light pyrx LATERAL vs heavy rasterx explode.
    # --fanout-only: ONLY run the fanout benchmark, skip all fn benchmarks.
    benchmark_fanout = "--benchmark-fanout" in sys.argv
    fanout_only = "--fanout-only" in sys.argv
    # --benchmark-netcdf: also run the NetCDF reader benchmark (light netcdf_gbx vs heavy
    #   netcdf_gdal) over the same staged .nc pool at {CORPUS}/netcdf.
    # --netcdf-only: ONLY run the NetCDF reader benchmark, skip all fn benchmarks.
    benchmark_netcdf = "--benchmark-netcdf" in sys.argv
    netcdf_only = "--netcdf-only" in sys.argv
    # --benchmark-netcdf-writer: also run the NetCDF WRITER benchmark (light netcdf_gbx write
    #   throughput, raster + vector legs) reusing {CORPUS}/netcdf + {CORPUS}/netcdf-swath.
    # --netcdf-writer-only: ONLY run the NetCDF writer benchmark, skip all fn benchmarks.
    # LIGHT-ONLY: there is no heavy NetCDF writer.
    benchmark_netcdf_writer = "--benchmark-netcdf-writer" in sys.argv
    netcdf_writer_only = "--netcdf-writer-only" in sys.argv
    # --benchmark-lidar: also run the LiDAR reader+writer benchmark (light lidar_gbx only,
    #   staged .laz pool at {CORPUS}/lidar).
    # --lidar-only: ONLY run the LiDAR reader+writer benchmark, skip all fn benchmarks.
    # LIGHT-ONLY: there is no heavy LiDAR tier.
    benchmark_lidar = "--benchmark-lidar" in sys.argv
    lidar_only = "--lidar-only" in sys.argv
    # --benchmark-exif: also run the EXIF reader benchmark (light exif_gbx only,
    #   staged JPEG pool at {CORPUS}/exif).
    # --exif-only: ONLY run the EXIF reader benchmark, skip all fn benchmarks.
    # LIGHT-ONLY: there is no heavy EXIF tier.
    benchmark_exif = "--benchmark-exif" in sys.argv
    exif_only = "--exif-only" in sys.argv
    # --benchmark-zarr: also run the Zarr benchmark (light only: gbx_zarr_read / sample /
    #   write against one GeoTIFF per hour, plus the shard-index and sample-density probes).
    # --zarr-only: ONLY run the Zarr benchmark, skip all fn benchmarks. Zarr is light-tier
    #   only, so either forces --lightweight-only.
    benchmark_zarr = "--benchmark-zarr" in sys.argv
    zarr_only = "--zarr-only" in sys.argv
    if benchmark_zarr or zarr_only:
        heavyweight = False
        lightweight = True
    # --fanout-scale F: dial the synthetic fan-out size for each function (default 1.0 ->
    #   meaningful but ~couple minutes on ~20 workers). Larger = more output rows.
    fanout_scale = float(_arg("--fanout-scale", "1.0"))
    writer_rows = int(_arg("--writer-rows", "14000000"))
    # --vector-legs reader|writer|both: run the scaled vector reader-ingest legs, the
    # writer-export leg, or both (default). Lets each be a separate isolated cluster job.
    vector_legs = _arg("--vector-legs", "both")
    # --vector-formats csv: restrict the scaled vector run to these light formats (e.g.
    # geojson_gbx). Empty = all. With --vector-legs + --lightweight-only/--heavyweight-only,
    # runs ONE (format x tier x leg) per job for cold isolation.
    vector_formats = _arg("--vector-formats", "")
    if not heavyweight and not lightweight:
        print(
            "ERROR: --heavyweight-only and --lightweight-only are mutually exclusive "
            "(nothing to run). Pass at most one.",
            file=sys.stderr,
        )
        return 2

    # --resume: keep existing rows for this run_id and skip already-complete (tier x mode)
    # sections (load them from the table instead of re-running). For picking up after a
    # timeout/crash on the SAME cluster. Mutually exclusive with the truncate modes, which
    # would wipe the rows resume relies on. NOTE: spark-path timings depend on cluster size,
    # so only resume on an unchanged cluster config (pure-core is size-independent).
    resume = "--resume" in sys.argv
    if resume and ("--truncate-all" in sys.argv or "--truncate-results" in sys.argv):
        print(
            "ERROR: --resume is mutually exclusive with --truncate-all / "
            "--truncate-results (truncate would wipe the rows resume reuses).",
            file=sys.stderr,
        )
        return 2
    # On resume, fns whose only row is an error are re-run by default (purged + retried,
    # e.g. after a code/JAR fix); --no-fix-errors keeps them as-is and skips them.
    fix_errors = "--no-fix-errors" not in sys.argv

    run_id = _arg("--run-id", "") or _default_run_id()
    if resume and "--run-id" not in sys.argv:
        print(
            "ERROR: --resume needs the --run-id of the run to resume (a launch without "
            "--run-id gets an id of its own).",
            file=sys.stderr,
        )
        return 2
    # Keep reader/writer benchmarks SEPARABLE from the function benchmarks: an *-only run
    # gets its own run_id suffix (unless --run-id was given explicitly) so its rows land in
    # a distinct partition of the results table instead of commingling with the function
    # rows under 'cluster'. This also makes the live "N of EXPECTED" count accurate (it
    # polls by run_id) rather than counting leftover function rows from a prior run.
    if "--run-id" not in sys.argv:
        if readers_only:
            run_id = f"{run_id}-readers"
        elif pmtiles_only:
            run_id = f"{run_id}-pmtiles"
        elif vector_only:
            run_id = f"{run_id}-vector"
        elif mvt_only:
            run_id = f"{run_id}-mvt"
        elif pmtiles_agg_only:
            run_id = f"{run_id}-pmtiles-agg"
        elif vector_tin_only:
            run_id = f"{run_id}-vector-tin"
        elif grid_quadbin_only:
            run_id = f"{run_id}-grid-quadbin"
        elif grid_bng_only:
            run_id = f"{run_id}-grid-bng"
        elif grid_custom_only:
            run_id = f"{run_id}-grid-custom"
        elif fanout_only:
            run_id = f"{run_id}-fanout"
        elif netcdf_only:
            run_id = f"{run_id}-netcdf"
        elif netcdf_writer_only:
            run_id = f"{run_id}-netcdf-writer"
        elif zarr_only:
            run_id = f"{run_id}-zarr"
    functions = ",".join(_function_names(_arg("--functions", "")))
    sel = _arg("--set", "core")
    unknown_bng = _unknown_bng_names(functions)
    if unknown_bng:
        print(
            "ERROR: --functions names no BNG leg or bench function: "
            + ", ".join(unknown_bng),
            file=sys.stderr,
        )
        return 2

    # --redo-functions <csv>: force re-run this explicit list of fns for the selected
    # (run_id, api, mode) by purging their existing rows first, leaving every other fn's rows
    # intact. INDEPENDENT of --set/--functions, so one run can resume the never-ran fns AND
    # force-redo this named subset (e.g. re-measure the aggregators with a new wheel while the
    # rest of the run completes normally). Exclusive with the truncates (which clear broadly).
    redo_functions = _arg("--redo-functions", "")
    if redo_functions.strip() and (
        "--truncate-all" in sys.argv or "--truncate-results" in sys.argv
    ):
        print(
            "ERROR: --redo-functions is mutually exclusive with --truncate-all / "
            "--truncate-results.",
            file=sys.stderr,
        )
        return 2
    if sel not in ("core", "full"):
        print(f"ERROR: --set must be 'core' or 'full' (got '{sel}')", file=sys.stderr)
        return 2
    modes = _arg("--modes", "both")
    row_counts = _arg("--row-counts", "10,100,1000,10000")
    # Pure-core defaults: 1 warm-up + 3 measured. One warm-up absorbs the cold cost (e.g.
    # imports and GDAL driver set-up on the FIRST call), and 3 measured gives a stable median of a fast
    # single-tile op without overpaying. Override with --warmup / --measured.
    warmup = int(_arg("--warmup", "1"))
    measured = int(_arg("--measured", "3"))
    # Spark-path iteration counts are SEPARATE from pure-core: the N-tile sweep over the whole
    # partitioned job IS the averaging, so spark-path defaults to 1 warm-up + 1 measured.
    # Override with --spark-warmup / --spark-measured.
    spark_warmup = int(_arg("--spark-warmup", "1"))
    spark_measured = int(_arg("--spark-measured", "1"))
    # Tiles per spark-path partition; 0 = auto (n / (slots*4): oversubscribe slots ~4x so
    # finished slots grab pending tasks rather than idling on the straggler tail).
    partition_size = int(_arg("--override-partition-size", "0"))
    # --input-tile materialized|virtual: input tile mode for the light spark-path leg.
    # materialized = bytes column (default, matches all prior bench runs).
    # virtual = path+window tile (tests the new virtual-tile reader path).
    input_tile = _arg("--input-tile", "materialized")
    if input_tile not in ("materialized", "virtual"):
        print(
            f"ERROR: --input-tile must be 'materialized' or 'virtual' (got '{input_tile}')",
            file=sys.stderr,
        )
        return 2
    # --disable-file: set GBX_DISABLE_FILE=1 in the notebook so virtual-tile reads
    # fall back to plain-path I/O (FILE type bypassed). Intended for the FILE-off A/B leg.
    # Only meaningful when --input-tile virtual; silently a no-op for materialized inputs.
    disable_file = "--disable-file" in sys.argv
    # --disable-fuse-direct: set GBX_DISABLE_FUSE_DIRECT=1 in the notebook so whole-file
    # /Volumes tiles take the FileRef byte-range stream instead of the scoped FUSE-direct
    # default. This is the stream-baseline leg of the FUSE-direct A/B (run once without the
    # flag = FUSE-direct default, once with it = stream, and compare).
    disable_fuse_direct = "--disable-fuse-direct" in sys.argv
    # --grouped-file: also run the grouped FILE-amortization benchmark (rst_clip_grouped +
    #   pixel-op _grouped fns) over a MULTIWINDOW COG corpus, across three tile modes
    #   (materialized / virtual+FILE-off / virtual+FILE-on). This is the leg that actually
    #   exercises grouped_tile_map's per-source open-cost amortization -- the FILE win the
    #   scalar bench misses. The leg toggles FILE on/off internally per mode, so --disable-file
    #   is NOT needed (and should not be combined with it).
    # --grouped-file-only: ONLY run the grouped-file benchmark, skip all fn benchmarks.
    benchmark_grouped_file = "--grouped-file" in sys.argv
    grouped_file_only = "--grouped-file-only" in sys.argv
    # --levels <S|G|A|H|IO>: also run the levels leg for one set of functions (S session functions,
    #   G generators, A aggregates, H DataFrame helpers; IO has no forms yet): every form of the set
    #   at each execution level the compute offers, with the attribution split. --levels-only: ONLY
    #   that leg (no fn benchmarks, no heavy tests JAR). --levels-uc <catalog.schema>: where the
    #   L4 (Unity Catalog) functions live; without it the L4 forms record a skipped row.
    levels_mvp = _arg("--levels", "")
    levels_only = "--levels-only" in sys.argv
    levels_uc = _arg("--levels-uc", "")
    # --levels-scales <csv>: the multiples of each form's base row count the leg times (default 1,4,16).
    try:
        levels_scales = tuple(int(x) for x in _arg("--levels-scales", "1,4,16").split(",") if x.strip())
    except ValueError:
        levels_scales = ()
    # --levels-forms <csv>: time only the forms with these names (the `form` column of the results).
    levels_forms = tuple(x.strip() for x in _arg("--levels-forms", "").split(",") if x.strip())
    if not levels_scales or min(levels_scales) < 1:
        print("ERROR: --levels-scales must be positive integers, e.g. 1,4,16", file=sys.stderr)
        return 2
    if levels_mvp not in ("", "S", "G", "A", "H", "SE", "IO"):
        print(
            f"ERROR: --levels must be one of S, G, A, H, SE, IO (got '{levels_mvp}')",
            file=sys.stderr,
        )
        return 2
    if (levels_only or levels_uc) and not levels_mvp:
        print(
            "ERROR: --levels-only and --levels-uc need --levels <S|G|A|H|SE|IO>.",
            file=sys.stderr,
        )
        return 2
    scales_problem = levels_scales_problem(
        levels_mvp, levels_scales, Path(os.environ.get("GBX_SOURCE_ROOT") or TESTS_DIR.parent.parent)
    )
    if scales_problem:
        print(f"ERROR: {scales_problem}", file=sys.stderr)
        return 2
    if levels_only:
        # The leg installs its own levels through register(); it never uses the tests JAR.
        heavyweight, lightweight = False, True
    # --multiwindow-corpus PATH: the multiwindow COG corpus dir (holding
    #   cog_multiwindow_manifest.json). Default (empty) -> CORPUS/bench-corpus-cog-multiwindow.
    multiwindow_corpus = _arg("--multiwindow-corpus", "")
    # Phase-2 FILE legs (light-only, serialized, each *-only-capable):
    # --file-matrix: sweep file_mode in (fuse, external, managed) for GeoTIFF + GeoPackage.
    file_matrix = "--file-matrix" in sys.argv
    file_matrix_only = "--file-matrix-only" in sys.argv
    # --gpkg-chunksize: sweep chunkSize (1k/10k/100k) for GeoPackage fuse reads.
    gpkg_chunksize = "--gpkg-chunksize" in sys.argv
    gpkg_chunksize_only = "--gpkg-chunksize-only" in sys.argv
    # --layout-sweep: sweep GeoTIFF + GeoPackage write layouts (order/cluster/plain).
    layout_sweep = "--layout-sweep" in sys.argv
    layout_sweep_only = "--layout-sweep-only" in sys.argv
    # --layout-scan: compare sequential-scan cost across write layouts (FILE tables).
    layout_scan = "--layout-scan" in sys.argv
    layout_scan_only = "--layout-scan-only" in sys.argv
    # --vector-table-read: read a FILE-column table back via vector_file_read TABLE mode
    #   (managed/external), across the layout-sweep's gpkg FILE tables.
    vector_table_read = "--vector-table-read" in sys.argv
    vector_table_read_only = "--vector-table-read-only" in sys.argv
    # --raster-decode-read: pixel-decode throughput (rst_avg) over raster FILE tiles across
    #   managed/external/fuse, plus the auto-order-vs-skipOrdering amortization comparison.
    raster_decode_read = "--raster-decode-read" in sys.argv
    raster_decode_read_only = "--raster-decode-read-only" in sys.argv
    # --file-filespace <id>: filespace identifier for FILE EXTERNAL/MANAGED table creation.
    file_filespace = _arg("--file-filespace", "")
    # --gpkg-corpus <path>: GeoPackage bench corpus base dir (staged by stage_gpkg_bench_corpus).
    gpkg_corpus = _arg("--gpkg-corpus", "")
    # --max-partition-bytes <v>: cap bytes-per-partition (e.g. "32m") to reduce rows/task on
    # file/Delta scans. Settable on Serverless (unlike AQE). Empty = leave the platform default.
    max_partition_bytes = _arg("--max-partition-bytes", "")
    # --extras <csv>: the geobrix extras the notebook installs after the wheel (default: the
    # env wheel's zarr extra for a run with the Zarr cell, else none).
    extras = _arg("--extras", "")
    # --new-cluster: run on a job cluster made for this run (it ends with the run) instead of
    # an existing cluster: --spark-version (default 19.x-scala2.13), --node-type (default
    # m6gd.xlarge), --num-workers (default 1), --access-mode dedicated|standard (default
    # dedicated), --on-demand (AWS: every node on demand, so no spot node is taken back
    # during a measurement; default: the workspace's spot/on-demand policy).
    new_cluster = "--new-cluster" in sys.argv
    on_demand = "--on-demand" in sys.argv
    spark_version = _arg("--spark-version", "19.x-scala2.13")
    node_type = _arg("--node-type", "m6gd.xlarge")
    num_workers = int(_arg("--num-workers", "1"))
    access_mode = _arg("--access-mode", "dedicated")
    if access_mode not in ("dedicated", "standard"):
        print(
            f"ERROR: --access-mode must be 'dedicated' or 'standard' (got '{access_mode}')",
            file=sys.stderr,
        )
        return 2
    if new_cluster and serverless:
        print("ERROR: --new-cluster and --serverless are exclusive.", file=sys.stderr)
        return 2
    if new_cluster and heavyweight:
        print(
            "ERROR: a --new-cluster job cluster is made without the GeoBrix JAR, so the "
            "heavyweight tier cannot run on it. Fix: add --lightweight-only (or --zarr-only).",
            file=sys.stderr,
        )
        return 2
    if on_demand and not new_cluster:
        print(
            "ERROR: --on-demand applies to a job cluster made for the run: pass "
            "--new-cluster with it.",
            file=sys.stderr,
        )
        return 2

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print(
            "Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE",
            file=sys.stderr,
        )
        return 2

    # --existing-cluster-id <id>: attach to this all-purpose cluster (overrides CLUSTER_ID env).
    # Skips the 4-8 min job-cluster startup by reusing a warm persistent cluster.
    cluster_id = _strip_invisible(
        _arg("--existing-cluster-id", "") or os.environ.get("CLUSTER_ID") or ""
    )
    if new_cluster:
        cluster_id = ""  # a job cluster: CLUSTER_ID from the config file is not used
    if not serverless and not new_cluster and not cluster_id:
        print(
            "Set CLUSTER_ID env var or pass --existing-cluster-id <id>",
            file=sys.stderr,
        )
        return 2

    catalog = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main")
    schema = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default")
    volume = _strip_invisible(
        os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples"
    )
    volroot = f"/Volumes/{catalog}/{schema}/{volume}"
    ver = _geobrix_version()

    # Wheel path: explicit or derived under the Volume root.
    wheel = _strip_invisible(
        os.environ.get("GBX_BUNDLE_WHEEL_VOLUME_PATH") or ""
    ).strip()
    explicit_wheel = bool(wheel)  # else: the env wheel for the compute's runtime, chosen below

    corpus = (
        _strip_invisible(os.environ.get("GBX_BENCH_CORPUS") or "").strip()
        or f"{volroot}/bench-corpus"
    )
    table = (
        _strip_invisible(os.environ.get("GBX_BENCH_RESULTS_TABLE") or "").strip()
        or f"{catalog}.{schema}.bench_results"
    )
    tests_jar = (
        _strip_invisible(
            os.environ.get("GBX_BENCH_TESTS_JAR_VOLUME_PATH") or ""
        ).strip()
        or f"{volroot}/geobrix-{ver}-tests.jar"
    )
    out_dir = f"{volroot}/bench-out/{run_id}"
    # Zarr bench locations (--zarr-store/--zarr-cog-dir/--zarr-scratch/--zarr-schema): the
    # ERA5 January 2023 v3 sharded store, its GeoTIFF-per-hour copy, a scratch directory the
    # write legs fill and empty, and the schema of their (dropped) manifest tables.
    zarr_store = _arg(
        "--zarr-store", f"{volroot}/_probe/zarr-spike/read/era5/era5_v3_shard_zstd.zarr"
    )
    zarr_cog_dir = _arg("--zarr-cog-dir", f"{volroot}/_probe/zarr/bench/cog_hourly")
    zarr_scratch = _arg("--zarr-scratch", f"{volroot}/_probe/zarr/bench/scratch/{run_id}")
    zarr_schema = _arg("--zarr-schema", f"{catalog}.gbx_zarr_scratch")
    zarr_cases = _arg("--zarr-cases", "R1,R2,R3,W1,W2")
    zarr_reps = int(_arg("--zarr-reps", "3"))

    cfg = dict(
        wheel=wheel,
        corpus=corpus,
        out_dir=out_dir,
        table=table,
        run_id=run_id,
        functions=functions,
        set=sel,
        modes=modes,
        row_counts=row_counts,
        warmup=warmup,
        measured=measured,
        spark_warmup=spark_warmup,
        spark_measured=spark_measured,
        partition_size=partition_size,
        heavyweight=heavyweight,
        lightweight=lightweight,
        # Two truncate modes (default: neither -> rows accumulate across runs):
        #  --truncate-results: SCOPED -- clear only this run_id + the tier(s) this
        #    invocation writes, so the paired tier / other runs survive (coexist).
        #  --truncate-all: WHOLE TABLE -- empty bench_results so ONLY the current run's
        #    rows remain. (Takes precedence if both are passed.)
        truncate_results=("--truncate-results" in sys.argv),
        truncate_all=("--truncate-all" in sys.argv),
        #  --resume: keep existing rows; function-granular (load done fns, run missing).
        resume=resume,
        #  fix_errors (default True): on resume, re-run fns whose only row is an error.
        fix_errors=fix_errors,
        #  --redo-functions: force re-run this explicit fn list (purge their rows first),
        #  independent of --set/--functions, layered on the normal resume run.
        redo_functions=redo_functions,
        #  --explain-only: print/persist spark-path physical plans, no timing/no rows.
        explain_only=explain_only,
        #  --benchmark-readers: also run reader benchmark (raster_gbx vs gdal).
        benchmark_readers=benchmark_readers,
        #  --readers-only: ONLY run the reader benchmark, skip fn benchmarks.
        readers_only=readers_only,
        #  --benchmark-pmtiles: also run pmtiles writer benchmark.
        benchmark_pmtiles=benchmark_pmtiles,
        #  --pmtiles-only: ONLY run the pmtiles benchmark, skip fn benchmarks.
        pmtiles_only=pmtiles_only,
        #  --benchmark-vector: also run vector reader benchmark (light *_gbx vs heavy *_ogr).
        benchmark_vector=benchmark_vector,
        #  --vector-only: ONLY run the vector reader benchmark, skip fn benchmarks.
        vector_only=vector_only,
        #  --vector-scale: use the scaled 1M-seed corpus instead of the tiny 4-file corpus.
        vector_scale=vector_scale,
        #  --writer-rows N: row count for the shared writer-source Delta table (default 14M).
        writer_rows=writer_rows,
        #  --vector-legs reader|writer|both: which scaled vector legs to run (default both).
        vector_legs=vector_legs,
        #  --vector-formats csv: restrict scaled vector run to these light formats (default all).
        vector_formats=vector_formats,
        #  --benchmark-mvt: also run st_asmvt benchmark (light pyvx vs heavy vectorx).
        benchmark_mvt=benchmark_mvt,
        #  --mvt-only: ONLY run the MVT benchmark, skip fn benchmarks.
        mvt_only=mvt_only,
        #  --benchmark-pmtiles-agg: also run pmtiles_agg grouped-agg benchmark (light vs heavy).
        benchmark_pmtiles_agg=benchmark_pmtiles_agg,
        #  --pmtiles-agg-only: ONLY run the pmtiles_agg benchmark, skip fn benchmarks.
        pmtiles_agg_only=pmtiles_agg_only,
        #  --benchmark-vector-tin: also run TIN + legacy benchmark (light pyvx vs heavy vectorx).
        benchmark_vector_tin=benchmark_vector_tin,
        #  --vector-tin-only: ONLY run the TIN + legacy benchmark, skip fn benchmarks.
        vector_tin_only=vector_tin_only,
        #  --benchmark-grid-quadbin: also run quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
        benchmark_grid_quadbin=benchmark_grid_quadbin,
        #  --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip fn benchmarks.
        grid_quadbin_only=grid_quadbin_only,
        #  --benchmark-grid-bng: also run BNG grid benchmark (light pygx vs heavy gridx.bng).
        benchmark_grid_bng=benchmark_grid_bng,
        #  --grid-bng-only: ONLY run the BNG grid benchmark, skip fn benchmarks.
        grid_bng_only=grid_bng_only,
        #  --benchmark-grid-custom: also run custom grid benchmark (light pygx vs heavy gridx.custom).
        benchmark_grid_custom=benchmark_grid_custom,
        #  --grid-custom-only: ONLY run the custom grid benchmark, skip fn benchmarks.
        grid_custom_only=grid_custom_only,
        #  --benchmark-fanout: also run fan-out UDTF benchmark (all 8 streaming UDTFs).
        benchmark_fanout=benchmark_fanout,
        #  --fanout-only: ONLY run the fanout benchmark, skip fn benchmarks.
        fanout_only=fanout_only,
        #  --fanout-scale F: dial the synthetic fan-out size (default 1.0).
        fanout_scale=fanout_scale,
        #  --benchmark-netcdf: also run NetCDF reader benchmark (light netcdf_gbx vs heavy netcdf_gdal).
        benchmark_netcdf=benchmark_netcdf,
        #  --netcdf-only: ONLY run the NetCDF reader benchmark, skip fn benchmarks.
        netcdf_only=netcdf_only,
        #  --benchmark-netcdf-writer: also run NetCDF WRITER benchmark (light-only throughput).
        benchmark_netcdf_writer=benchmark_netcdf_writer,
        #  --netcdf-writer-only: ONLY run the NetCDF writer benchmark, skip fn benchmarks.
        netcdf_writer_only=netcdf_writer_only,
        #  --benchmark-lidar: also run LiDAR reader+writer benchmark (light lidar_gbx only).
        benchmark_lidar=benchmark_lidar,
        #  --lidar-only: ONLY run the LiDAR reader+writer benchmark, skip fn benchmarks.
        lidar_only=lidar_only,
        #  --benchmark-exif: also run EXIF reader benchmark (light exif_gbx only).
        benchmark_exif=benchmark_exif,
        #  --exif-only: ONLY run the EXIF reader benchmark, skip fn benchmarks.
        exif_only=exif_only,
        #  --input-tile materialized|virtual: input tile mode for the light spark-path leg.
        input_tile=input_tile,
        #  --disable-file: set GBX_DISABLE_FILE=1 in the notebook (FILE-off A/B leg).
        disable_file=disable_file,
        #  --disable-fuse-direct: set GBX_DISABLE_FUSE_DIRECT=1 (FUSE-direct stream-baseline A/B leg).
        disable_fuse_direct=disable_fuse_direct,
        #  --grouped-file: also run the grouped FILE-amortization benchmark (multiwindow COG,
        #  3 tile modes). --grouped-file-only: ONLY that leg. --multiwindow-corpus: its corpus dir.
        benchmark_grouped_file=benchmark_grouped_file,
        grouped_file_only=grouped_file_only,
        multiwindow_corpus=multiwindow_corpus,
        #  --levels <S|G|A|H|IO>: the levels leg for one set of functions. --levels-only: ONLY that leg.
        #  --levels-uc <catalog.schema>: where its L4 (Unity Catalog) functions live.
        levels_mvp=levels_mvp,
        levels_only=levels_only,
        levels_uc=levels_uc,
        levels_scales=levels_scales,
        levels_forms=levels_forms,
        #  Liveness: the heartbeat period, the driver stack dump cap, the test-only stall.
        heartbeat_secs=30,
        launch_token=uuid.uuid4().hex,
        stall_cap_s=int(form_cap_min * 60),
        inject_stall=inject_stall,
        #  Phase-2 FILE legs (light-only, serialized):
        #  --file-matrix: file_mode sweep (fuse/external/managed) for GeoTIFF + GeoPackage reads.
        file_matrix=file_matrix,
        file_matrix_only=file_matrix_only,
        #  --gpkg-chunksize: GeoPackage chunkSize sweep (1k/10k/100k; fanout-invariance check).
        gpkg_chunksize=gpkg_chunksize,
        gpkg_chunksize_only=gpkg_chunksize_only,
        #  --layout-sweep: GeoTIFF + GeoPackage write layout sweep (order/cluster/plain).
        layout_sweep=layout_sweep,
        layout_sweep_only=layout_sweep_only,
        #  --layout-scan: sequential-scan + shuffle-input cost across write layouts (FILE tables).
        layout_scan=layout_scan,
        layout_scan_only=layout_scan_only,
        #  --vector-table-read: vector_file_read TABLE mode over the layout-sweep's gpkg FILE tables.
        vector_table_read=vector_table_read,
        vector_table_read_only=vector_table_read_only,
        #  --raster-decode-read: rst_avg pixel-decode over raster FILE tiles + auto-vs-skip ordering.
        raster_decode_read=raster_decode_read,
        raster_decode_read_only=raster_decode_read_only,
        #  --file-filespace: filespace identifier for FILE EXTERNAL/MANAGED tables.
        file_filespace=file_filespace,
        #  --gpkg-corpus: GeoPackage bench corpus base dir.
        gpkg_corpus=gpkg_corpus,
        #  --max-partition-bytes: cap bytes-per-partition (Serverless-settable) to tune rows/task.
        max_partition_bytes=max_partition_bytes,
        #  --serverless: use Serverless compute (light-only, no JAR). --env-version N selects
        #  the environment version to pin (default 6, the +env6 wheel's).
        serverless=serverless,
        env_version=env_version,
        #  --benchmark-zarr / --zarr-only and the Zarr bench locations (see above).
        benchmark_zarr=benchmark_zarr,
        zarr_only=zarr_only,
        zarr_store=zarr_store,
        zarr_cog_dir=zarr_cog_dir,
        zarr_scratch=zarr_scratch,
        zarr_schema=zarr_schema,
        zarr_cases=zarr_cases,
        zarr_reps=zarr_reps,
        #  --extras: what the install cell adds after the wheel.
        extras=extras,
    )
    if explain_only:
        # Plans are a spark-path concern only; never run the pure-core sections.
        cfg["modes"] = "spark-path"
    if pmtiles_only:
        # PMTiles writer is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if vector_only:
        # Vector reader benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if mvt_only:
        # MVT agg benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if pmtiles_agg_only:
        # PMTiles agg benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if vector_tin_only:
        # TIN + legacy benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_quadbin_only:
        # Quadbin grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_bng_only:
        # BNG grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_custom_only:
        # Custom grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if fanout_only:
        # Fan-out UDTF benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if netcdf_only:
        # NetCDF reader benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if netcdf_writer_only:
        # NetCDF writer benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if lidar_only:
        # LiDAR reader+writer benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if exif_only:
        # EXIF reader benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grouped_file_only:
        # Grouped FILE-amortization benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if levels_only:
        # The levels leg is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if file_matrix_only:
        # FILE matrix benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if gpkg_chunksize_only:
        # GeoPackage chunkSize sweep is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if layout_sweep_only:
        # Layout sweep benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if layout_scan_only:
        # Layout scan benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if vector_table_read_only:
        # Vector table-read benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if raster_decode_read_only:
        # Raster decode-read benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if zarr_only:
        # Zarr benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"

    # Import the notebook builder from the repo source (this runs on the HOST, not the cluster).
    sys.path.insert(0, str(TESTS_DIR.parent.parent / "python" / "geobrix" / "src"))
    try:
        from databricks.labs.gbx.bench.cluster import (
            build_bench_notebook,
            install_extras,
        )
    except ImportError as e:
        print(
            f"Could not import build_bench_notebook from repo source (python/geobrix/src): {e}",
            file=sys.stderr,
        )
        print(
            "Run this from the repo root so 'python/geobrix/src' resolves.",
            file=sys.stderr,
        )
        return 2

    try:
        import io

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import compute, jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    w = (
        WorkspaceClient(profile=profile)
        if profile
        else WorkspaceClient(host=host, token=token)
    )
    if not explicit_wheel:
        # The env wheel for the compute's runtime: the other family's wheel refuses to import there.
        if serverless:
            env = int(env_version)
        else:
            try:
                env = env_of_runtime(
                    spark_version if new_cluster else w.clusters.get(cluster_id).spark_version,
                    env_version if "--env-version" in sys.argv else None,
                )
            except ValueError as exc:
                print(exc, file=sys.stderr)
                return 2
        wheel = f"{volroot}/geobrix-{ver}+env{env}-py3-none-any.whl"
        cfg["wheel"] = wheel

    # Pre-flight: show the operator exactly what will run before submitting.
    print("=" * 64)
    if serverless:
        print("gbx:bench:cluster pre-flight (SERVERLESS)")
        print(f"  env_version: {env_version}")
    elif new_cluster:
        print("gbx:bench:cluster pre-flight (JOB CLUSTER, ends with the run)")
        print(
            f"  new cluster: {spark_version} {node_type} x {num_workers} worker(s), "
            f"{access_mode} access{', on demand' if on_demand else ''}"
        )
    else:
        print("gbx:bench:cluster pre-flight")
        print(f"  cluster_id : {cluster_id}")
    print(f"  scope      : heavyweight={heavyweight}  lightweight={lightweight}")
    print(f"  run_id     : {run_id}")
    print(f"  functions  : {functions or f'(set={sel})'}")
    print(f"  modes      : {modes}   row_counts={row_counts}")
    print(f"  warmup/meas: {warmup}/{measured}")
    print(f"  corpus     : {corpus}")
    print(f"  table      : {table}")
    print(f"  out_dir    : {out_dir}")
    print(f"  wheel      : {wheel}")
    if benchmark_zarr or zarr_only:
        print(f"  zarr       : store={zarr_store}")
        print(f"               cog_dir={zarr_cog_dir} scratch={zarr_scratch}")
        print(f"               schema={zarr_schema} cases={zarr_cases} reps={zarr_reps}")
    if heavyweight:
        print(
            f"  tests.jar  : {tests_jar}  (attached as job library)"
        )
    print("  NOTE: this submits a job to a real cluster and consumes compute.")
    print("=" * 64)

    # Serverless + the Zarr legs or --extras: the job environment installs the wheel and the
    # extras' requirements (expanded from pyproject.toml, as gbx:test:notebooks-serverless
    # does). A pip install from the notebook reaches the Serverless driver only, and the
    # Zarr legs' tasks import zarr, numcodecs and xarray on the executors.
    env_deps = None
    if serverless and (benchmark_zarr or zarr_only or extras):
        from run_notebooks_serverless import _build_dep_list  # sibling module

        # the env wheel's own extras (envs/env<N>/extra-<name>.txt), as gbx:test:notebooks-serverless does
        env_deps = _build_dep_list(wheel, install_extras(cfg), [], env_version) + ["markdown"]
        cfg["env_deps"] = True
        print(f"  env deps   : {len(env_deps)} (wheel + {', '.join(install_extras(cfg))})")
    nb = build_bench_notebook(cfg)

    # Local preflight: refuse a launch that cannot work, with no compute (the checks of _preflight.py).
    if skip_preflight:
        print("PREFLIGHT SKIPPED (--skip-preflight): the launch is not checked locally.")
    else:
        problems = launch_problems(
            w, serverless=serverless, wheel=wheel, env_version=env_version, levels_mvp=levels_mvp,
            levels_scales=levels_scales, root=Path(os.environ.get("GBX_SOURCE_ROOT") or TESTS_DIR.parent.parent),
        )  # fmt: skip
        if problems:
            print("PREFLIGHT FAILED: nothing is submitted.", file=sys.stderr)
            for pr in problems:
                print(f"  - {pr}", file=sys.stderr)
            return EXIT_PREFLIGHT
        print("preflight: ok (environment spec, staged wheel, probe SQL, client input sizes)")

    # Pre-flight: REFUSE to submit a spark-path run the corpus row pool can't fill. The
    # spark-path draws max(--row-counts) DISTINCT tiles from the pool; a smaller pool would
    # silently UNDER-FILL (report rows=max while processing fewer tiles -> misleading numbers).
    # Require pool >= the largest requested row count. (Skipped for --explain-only, which
    # builds plans and draws no tiles; pure-core uses the size-sweep, not the row pool. Also
    # skipped for the *-only reader/writer/pmtiles/vector benchmarks: those read the WHOLE pool
    # (or their own vector corpus), not the --row-counts function ladder, so the ladder max is
    # irrelevant to them -- gating on it would falsely refuse a valid 1000-tile reader run.)
    _only_run = (
        cfg.get("readers_only")
        or cfg.get("pmtiles_only")
        or cfg.get("vector_only")
        or cfg.get("mvt_only")
        or cfg.get("pmtiles_agg_only")
        or cfg.get("vector_tin_only")
        or cfg.get("grid_quadbin_only")
        or cfg.get("grid_bng_only")
        or cfg.get("grid_custom_only")
        or cfg.get("fanout_only")
        or cfg.get("netcdf_only")
        or cfg.get("grouped_file_only")
        or cfg.get("levels_only")
        or cfg.get("file_matrix_only")
        or cfg.get("gpkg_chunksize_only")
        or cfg.get("layout_sweep_only")
        or cfg.get("layout_scan_only")
        or cfg.get("vector_table_read_only")
        or cfg.get("raster_decode_read_only")
        or cfg.get("zarr_only")
    )
    if (
        cfg["modes"] in ("spark-path", "both")
        and not cfg.get("explain_only")
        and not _only_run
    ):
        _max_rc = max(
            (int(x) for x in str(cfg["row_counts"]).split(",") if x), default=0
        )
        try:
            _cj = json.loads(
                w.files.download(f"{corpus}/corpus.json")
                .contents.read()
                .decode("utf-8")
            )
            _pool = len(_cj.get("row_pool", {}).get("tiles", []))
        except Exception as _e:
            _e_str = str(_e)
            if "disabled for users without account admin" in _e_str or "API is disabled" in _e_str:
                print(
                    f"WARNING: cannot read {corpus}/corpus.json (Files API admin-gated on this workspace); "
                    f"skipping pool-size validation. Ensure --row-counts <= corpus pool size.",
                    file=sys.stderr,
                )
                _pool = _max_rc  # bypass the size check below
            else:
                print(
                    f"ERROR: cannot read {corpus}/corpus.json to validate the row pool size: {_e}",
                    file=sys.stderr,
                )
                return 2
        if _max_rc > _pool:
            print(
                f"ERROR: spark-path --row-counts max ({_max_rc}) exceeds the corpus row pool "
                f"({_pool} tiles) at {corpus}. The pool must have >= {_max_rc} tiles or the run "
                f"under-fills. Generate a larger pool (gbx:bench:gen-data --row-rows {_max_rc}, "
                f"then stage it) or lower --row-counts. Refusing to submit.",
                file=sys.stderr,
            )
            return 2

    # Notebook path: one per run id, under the per-user default or the env override (a
    # prefix). A job reads its notebook when it is submitted, so two launches sharing one
    # path made the earlier job run the later launch's notebook.
    notebook_path_from_env = (
        _strip_invisible(os.environ.get("GBX_BENCH_RUNNER_NOTEBOOK_PATH") or "").strip()
        or None
    )
    me = w.current_user.me()
    base = notebook_path_from_env or f"/Users/{me.user_name}/geobrix_bench_runner"
    base = base.rstrip("/").removesuffix(".ipynb")
    notebook_path = f"{base}_{run_id}.ipynb"
    # Ensure it doesn't have the /Workspace prefix for the SDK calls.
    notebook_path = "/" + notebook_path.strip().removeprefix("/Workspace").lstrip("/")
    smoke_path = notebook_path[: -len(".ipynb")] + "_smoke.ipynb"

    # Ensure parent directory exists (workspace API).
    notebook_parent = Path(notebook_path).parent
    try:
        w.workspace.mkdirs(str(notebook_parent))
    except Exception:
        pass

    def upload(path, notebook):
        print(f"Uploading bench notebook to {path}...")
        # CRITICAL: ImportFormat.JUPYTER creates a NOTEBOOK, not a FILE.
        w.workspace.upload(
            path,
            io.BytesIO(json.dumps(notebook, indent=1).encode("utf-8")),
            format=ImportFormat.JUPYTER,
            overwrite=True,
        )

    # Both notebooks are built and uploaded now, from the code this launch started with (the launcher itself
    # runs from a snapshot, see gbx-bench-cluster.sh): a later edit cannot change the real run after the smoke.
    upload(notebook_path, nb)
    do_smoke = not no_smoke and not explain_only
    if do_smoke:
        smoke_cfg = dict(
            cfg, smoke=True, run_id=f"{run_id}-smoke", out_dir=f"{out_dir}-smoke", smoke_rows=smoke_rows,
            truncate_results=False, truncate_all=False, resume=False, redo_functions="",
            stall_cap_s=int(min(form_cap_min, SMOKE_FORM_CAP_MIN) * 60), launch_token=uuid.uuid4().hex,
        )  # fmt: skip
        upload(smoke_path, build_bench_notebook(smoke_cfg))

    # Attach the bench tests.jar only for the heavyweight leg (it carries the bench Scala classes).
    # On an all-purpose cluster, job-attached libraries PERSIST as cluster libraries, so a second
    # run fails with "Duplicate package installation detected" before the notebook ever starts.
    # Guard: when an existing cluster is in use, query its installed libraries first.
    #   - No geobrix *-tests.jar on cluster → attach as usual.
    #   - Same path already installed → skip attachment (same bytes on classpath; safe reuse).
    #   - Different geobrix tests.jar installed → REFUSE to submit.  The driver JVM caches classes
    #     by name and won't reload them without a restart; running with the wrong JAR silently
    #     executes stale Scala bench code.  Tell the operator to restart the cluster.
    if heavyweight and cluster_id:
        _existing_tests = []
        try:
            _existing_tests = [
                fs.library.jar
                for fs in w.libraries.cluster_status(cluster_id)
                if fs.library
                and fs.library.jar
                and "geobrix" in fs.library.jar.lower()
                and fs.library.jar.endswith("-tests.jar")
            ]
        except Exception as _lib_err:
            print(
                f"WARNING: could not read cluster library status ({_lib_err}); "
                "proceeding with JAR attachment.",
                file=sys.stderr,
            )

        if not _existing_tests:
            # Nothing installed yet: attach as usual.
            libraries = [compute.Library(jar=tests_jar)]
        elif _existing_tests[0] == tests_jar:
            # Exact same path already on cluster → same bytes; skip to avoid duplicate error.
            print(
                "  NOTE: tests.jar already installed on cluster (same path) — "
                "skipping job-lib attachment to avoid duplicate-package error."
            )
            print(f"         installed : {_existing_tests[0]}")
            libraries = None
        else:
            # Different geobrix tests.jar on cluster → stale-bytes risk; refuse loudly.
            print("=" * 64, file=sys.stderr)
            print(
                "ERROR: a different geobrix tests.jar is already installed on this cluster.",
                file=sys.stderr,
            )
            print(f"  installed : {_existing_tests[0]}", file=sys.stderr)
            print(f"  this run  : {tests_jar}", file=sys.stderr)
            print("", file=sys.stderr)
            print(
                "The driver JVM caches classes by name; running with the wrong JAR",
                file=sys.stderr,
            )
            print(
                "silently executes stale Scala bench code from the old tests.jar.",
                file=sys.stderr,
            )
            print("", file=sys.stderr)
            print(
                "ACTION REQUIRED: restart the cluster in the Databricks UI, then re-run.",
                file=sys.stderr,
            )
            print(
                "  A cluster restart flushes the JVM class cache and the installed-library",
                file=sys.stderr,
            )
            print(
                "  list, letting the fresh tests.jar attach cleanly.",
                file=sys.stderr,
            )
            print("=" * 64, file=sys.stderr)
            return 2
    else:
        libraries = [compute.Library(jar=tests_jar)] if heavyweight else None

    def submit(path, name, timeout_s):
        """Submit one notebook run (Serverless or the cluster) with a time limit; returns the run id."""
        if serverless:
            # Serverless submission: environment_version-pinned compute, no cluster_id, no JAR.
            # max_retries=0: a failed bench task must surface immediately, not auto-retry and mask
            # the failure (standing bench rule).
            env_version_str = str(env_version)
            print(f"Submitting {name} on Serverless (env v{env_version_str}), time limit {timeout_s // 60} min...")
            waiter = submit_once(
                w,
                run_name=name,
                timeout_seconds=timeout_s,
                environments=[
                    jobs.JobEnvironment(
                        environment_key="bench_env",
                        # The wheel goes in the environment spec: that is what reaches the Python UDF workers.
                        # With the Zarr legs or --extras it also names the extras' requirements, so the
                        # executors have them too.
                        # The notebook's own pip install reaches the driver only (every geobrix UDF then failed
                        # on the workers with "No module named 'databricks.labs'", measured on env 6).
                        spec=compute.Environment(
                            environment_version=env_version_str,
                            dependencies=env_deps or serverless_dependencies(wheel),
                        ),
                    )
                ],
                tasks=[
                    jobs.SubmitTask(
                        task_key="run_bench",
                        environment_key="bench_env",
                        notebook_task=jobs.NotebookTask(notebook_path=path, source=jobs.Source.WORKSPACE),
                        max_retries=0,
                    )
                ],
            )
        elif new_cluster:
            print(f"Submitting {name} on a new job cluster, time limit {timeout_s // 60} min...")
            mode = (
                compute.DataSecurityMode.SINGLE_USER
                if access_mode == "dedicated"
                else compute.DataSecurityMode.USER_ISOLATION
            )
            waiter = submit_once(
                w,
                run_name=name,
                timeout_seconds=timeout_s,
                tasks=[
                    jobs.SubmitTask(
                        task_key="run_bench",
                        new_cluster=compute.ClusterSpec(
                            spark_version=spark_version,
                            node_type_id=node_type,
                            num_workers=num_workers,
                            data_security_mode=mode,
                            single_user_name=(me.user_name if access_mode == "dedicated" else None),
                            custom_tags={"gbx_bench": run_id[:64]},
                            aws_attributes=(
                                compute.AwsAttributes(
                                    availability=compute.AwsAvailability.ON_DEMAND,
                                    first_on_demand=num_workers + 1,
                                )
                                if on_demand
                                else None
                            ),
                        ),
                        notebook_task=jobs.NotebookTask(notebook_path=path, source=jobs.Source.WORKSPACE),
                        libraries=libraries,
                        max_retries=0,
                    )
                ],
            )
        else:
            print(f"Submitting {name} on cluster {cluster_id}, time limit {timeout_s // 60} min...")
            waiter = submit_once(
                w,
                run_name=name,
                timeout_seconds=timeout_s,
                tasks=[
                    jobs.SubmitTask(
                        task_key="run_bench",
                        existing_cluster_id=cluster_id,
                        notebook_task=jobs.NotebookTask(notebook_path=path, source=jobs.Source.WORKSPACE),
                        libraries=libraries,
                    )
                ],
            )
        run_id_remote = waiter.run_id
        print("=" * 64)
        print(f"Run submitted: run_id={run_id_remote}")
        try:
            _r0 = w.jobs.get_run(run_id_remote)
            if getattr(_r0, "run_page_url", None):
                print(f"  run URL : {_r0.run_page_url}")
        except Exception:
            pass
        return run_id_remote

    form_cap_s = form_cap_min * 60.0
    stale_s = stale_min * 60.0
    warehouse_id = _strip_invisible(os.environ.get("GBX_BENCH_SQL_WAREHOUSE_ID") or "") or (
        _discover_warehouse(w) if do_wait else None
    )

    rc = run_smoke_stage(
        w, submit, do_smoke=do_smoke, no_smoke=no_smoke, smoke_only=smoke_only, smoke_path=smoke_path,
        run_id=run_id, out_dir=out_dir, smoke_rows=smoke_rows, smoke_timeout_min=smoke_timeout_min,
        form_cap_s=form_cap_s, stale_s=stale_s, token=smoke_cfg["launch_token"] if do_smoke else "",
        levels_mvp=levels_mvp, allow_errors=smoke_allow_errors,
    )  # fmt: skip
    if rc is not None:
        return rc

    remote_run_id = submit(notebook_path, f"geobrix-bench-{run_id}", timeout_min * 60)

    # Results stream into the table as each function finishes -> queryable immediately.
    # Surface the run URL + the exact query right after submit (don't make the operator
    # wait until the end or hunt for it).
    print(f"  results : {table}  (streaming; filter run_id = '{run_id}')")
    print(
        f"  query   : SELECT fn, mode, rows, iter_median_ms, status "
        f"FROM {table} WHERE run_id = '{run_id}' ORDER BY fn"
    )
    print(f"  heartbeat: {out_dir}/heartbeat.json  (the watchdog cancels a stalled run)")
    print("=" * 64)

    if not do_wait:
        # Fire-and-forget: submit returns a waiter; we don't call .result().
        print("Run submitted (--no-wait). Poll the table above, or the Databricks UI.")
        return 0

    # The tier(s) this invocation writes -> filter the live count by (run_id, api) so it
    # stays accurate when light and heavy share a run_id in the same table.
    poll_apis = [
        a
        for a, on in (("lightweight", lightweight), ("heavyweight", heavyweight))
        if on
    ]
    expected = _expected_rows(
        functions,
        sel,
        modes,
        row_counts,
        lightweight,
        heavyweight,
        benchmark_readers=benchmark_readers,
        readers_only=readers_only,
        benchmark_pmtiles=benchmark_pmtiles,
        pmtiles_only=pmtiles_only,
        benchmark_vector=benchmark_vector,
        vector_only=vector_only,
    )
    if zarr_only:
        expected = 0  # the function-row count does not apply to the Zarr legs
    outcome = watch_to_end(
        w, remote_run_id, hb_path=f"{out_dir}/heartbeat.json", form_cap_s=form_cap_s, stale_s=stale_s,
        token=cfg["launch_token"], count=(warehouse_id, table, run_id, poll_apis, expected),
        form_cap_max_s=form_cap_max_min * 60.0,
    )  # fmt: skip
    if outcome.stall:
        print(
            f"Rows sunk before the stall are in {table} (run_id = '{run_id}'). Not retried.",
            file=sys.stderr,
        )
        return EXIT_STALLED

    run = outcome.run
    run_id_remote = remote_run_id
    state = run.state
    _result = getattr(state, "result_state", None)
    # the run has ended; its output stays with it, and a failed run keeps its notebook
    _remove_notebook(w, notebook_path, getattr(_result, "value", str(_result)) == "SUCCESS")
    if state and getattr(state, "life_cycle_state", None):
        lc = (
            state.life_cycle_state.value
            if hasattr(state.life_cycle_state, "value")
            else str(state.life_cycle_state)
        )
        if lc == "TERMINATED":
            result_state = (
                (
                    state.result_state.value
                    if state.result_state and hasattr(state.result_state, "value")
                    else str(state.result_state)
                )
                if state.result_state
                else "UNKNOWN"
            )
            if outcome.payload:
                print("Notebook output:", outcome.payload)
            if result_state == "SUCCESS":
                # Point at the summary file that actually exists for this scope:
                # both tiers -> comparison summary.md; lightweight-only ->
                # lightweight.summary.md; heavyweight-only -> heavyweight.jsonl.
                if heavyweight and lightweight:
                    summary_file = "summary.md"
                elif lightweight:
                    summary_file = "lightweight.summary.md"
                else:
                    summary_file = "heavyweight.jsonl"
                print(f"Run {run_id_remote} finished: result_state=SUCCESS.")
                print(f"Results -> table {table}")
                print(f"Summary  -> {out_dir}/{summary_file}")
                return 0
            print(
                f"Run {run_id_remote} finished with result_state={result_state}",
                file=sys.stderr,
            )
            print(
                f"Check the Delta table {table} and out_dir {out_dir} for partial results.",
                file=sys.stderr,
            )
    else:
        print(f"Run state: {state}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
