"""gbx:test:databricks -- run GeoBrix's test shards on warm Databricks pool clusters, side by side.

    push_and_run_tests_on_cluster.py --profile oauth-fe --purpose "why this run" \\
        [--shards all-light|all-heavy|affected|<name,name>] [--sha REV] [--max-parallel 5] [--explain-only]

The shards are CI's: the `light` matrix of .github/workflows/build_main.yml, `scripts/ci`, and one heavy shard per test dir
the light tier leaves out. They are read from those files on every run (examples/tests/_test_shards.py), so CI and a
cluster run cannot drift. The Scala suites stay local (`gbx:test:scala`); a cluster has no Maven build.

A pool's job has one runner task per slot (clusters x --max-parallel). Each runner claims the next shard from a queue the
runners share (a Delta table, longest expected first; examples/tests/_shard_runner.py) and runs it as a notebook run of its own,
so in its own Python process and Spark session, until the queue is empty: no slot idles while a shard waits. The session is
the cluster's own (test/_provided_spark.py).
Pools (DBR 19, single node, 60 idle minutes): gbx-test-1n has no JAR and runs the light shards and scripts/ci;
gbx-test-1n-jar carries the staged JAR and runs the heavy shards. A run leases a cluster, keeps nothing on it, and gives
it back: the pool stays warm for the next run (`gbx:bench:pool down` ends it).

What it stages, to an isolated Volume folder `<volume>/test-runs/<run id>/` (removed at the end): the env6 wheel (the
dependencies and the native payload; sha256 checked by downloading it back), a tarball of the COMMITTED repo files the
tests read at --sha (`git archive`) and the run plan. The tests run from the tarball's package, not the wheel's copy. The
JAR (heavy shards) goes to `<volume>/test-pool-jars/<sha256 prefix>/`, a folder per JAR, so the pool reinstalls only when
the JAR differs. Uncommitted edits are not in the run; the launcher says how many.

Results: junit XML and the pytest log per shard, downloaded to `test-logs/databricks-tests/<run id>/<shard>/`, one summary
table (`summary.md`: shard, passed, failed, errors, skipped, wall time, the skip reasons) and the first lines of every
failure. The exit code is non-zero when any shard failed, errored or did not finish.

Options:
  --profile <name>       Databricks profile (required; default DATABRICKS_CONFIG_PROFILE)
  --purpose "<why>"      REQUIRED on a launch (heads each runner notebook, rides on the run name)
  --shards <spec>        affected (default, the pre-push pass) | all-light | all-heavy | shard names or groups ("pyrx", "zarr 2/4"), comma separated
  --base <rev>           `affected`: the files changed between this and --sha pick the tests (default origin/streamline/0.6.0)
  --allow-broad          let `affected` run a selection over a quarter of the light bank (it refuses one without this)
  --sha <rev>            the commit to test (default HEAD)
  --wheel <path>         the env6 wheel or the folder that holds it (newest by mtime; default dist/env6/); it carries dependencies and native payload, not the code under test
  --jar-path <path>      the JAR of the heavy shards, or its folder (default python/geobrix/lib/; copied out and sha-checked)
  --max-parallel <n>     runners per cluster, so shards side by side (default: the node's cores / 8; 4 on 32 vCPU)
  --serverless           run the selected light shards on Serverless env 6 (a fidelity leg; its workers run the
                         wheel, so a Spark test needs a --wheel built at --sha)
  --fresh-cluster        use a new cluster that ends with the run, not the pools
  --pool-light <name>    default gbx-test-1n          --pool-jar <name>  default gbx-test-1n-jar
  --node-type <t>        node of a pool cluster made by this run (default m5d.8xlarge, 32 vCPU)
  --shard-cap-min <n>    stop a shard after this long (default 45); its results so far are kept
  --stall-min <n>        stop a shard when no test finished for this long (default 10)
  --start-cap-min <n>    cancel when no shard has beaten this long after the submit (default 25)
  --pool-wait-min <n>    wait this long for a free pool cluster (default 30)
  --pip-extra "<reqs>"   more pip requirements the tests import
  --env KEY=VAL          an environment variable every shard sets before its tests (repeatable), e.g.
                         GBX_CAP_ARROW_TILE_TRANSPORT=0 to run the light tile UDFs with their pickled transport
  --spark-conf KEY=VAL   a Spark conf every shard's session sets before its tests (repeatable), e.g.
                         spark.sql.execution.arrow.maxBytesPerBatch=16m (smaller Arrow batches of tile rows)
  --run-id <id>          default tst-<date>-<time>-<4 hex>
  --keep-stage           leave the Volume folder, the notebooks and the queue table
  --explain-only         print the plan and exit; no network, nothing staged or started
"""

from __future__ import annotations

import argparse
import contextlib
import inspect
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple

try:  # the junit files are our own, but a parser that refuses entities costs nothing
    from defusedxml import ElementTree as ET
except ImportError:  # pragma: no cover - the launcher venv may lack it
    import xml.etree.ElementTree as ET  # noqa: S405

from _cli import exit_if_help  # sibling module (script dir is on sys.path)
from _cluster_config import cluster_config_file  # sibling module
import _affected as af  # sibling module: the test files and oracle tables a change reaches (the import graph)
import _shard_runner as sr  # sibling module: the runners' claim-and-run loop (its source rides in the runner notebook)
import _test_shards as ts  # sibling module: the shards, read from CI's files
import _timings as tm  # sibling module: the timing history every run merges into
import push_and_run_mutation_on_cluster as mut  # sibling module: staging, download and wheel helpers the guidance run uses
import _wheel_guard as wg  # sibling module: force the staged wheel in, fail the run if another geobrix is imported

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = mut.REPO_ROOT
DEFAULT_NODE = "m5d.8xlarge"
SPARK_VERSION = mut.SPARK_VERSION
PYTEST_PIN = mut.PYTEST_PIN
EXTRAS = "stac,vizx,zarr,databricks"  # the env wheel's feature extras the suites import
# The tarball is every file committed at --sha but two folders: the tests read files anywhere in the checkout (.mvn, pom.xml,
# rust-toolchain.toml, the CI workflow, the Scala sources), and a list of what to take missed some. Left out: the test resources
# (staged once per content, below) and the doc images under resources/, which no shard reads.
EXCLUDED = ("src/test/resources", "resources")
SUBSET = (".", *(f":(exclude){d}" for d in EXCLUDED))  # a git pathspec
RESOURCES = "src/test/resources"  # the test rasters, NetCDF and MODIS scenes the tests open: staged once per content, not once per run
DEPS_INPUTS = ("python/geobrix/pyproject.toml", "python/geobrix/envs", "scripts/native")  # a change here wants a new wheel
EXIT_FAILED, EXIT_USAGE, EXIT_NO_START, EXIT_STALLED = 1, 2, 4, 3
TIMINGS = "test-logs/databricks-tests/timings-history.json"
GRAPH_CACHE = "test-logs/databricks-tests/.import-graph"  # the import graph of a commit, built once (examples/tests/_affected.py)  # every run's samples (examples/tests/_timings.py)
OLD_TIMINGS = "test-logs/databricks-tests/timings.json"  # the last run's seconds only: read when there is no history yet
POLL_S = 15
STATUS_EVERY_S = 120  # one status line per pool every 2 min
BEAT_EVERY_S = 60  # heartbeats are read this often: a pool has up to 20 shards and each read is a Files call


# ---------------------------------------------------------------------------------------------- arguments


def parse(argv):
    p = argparse.ArgumentParser(prog="push_and_run_tests_on_cluster.py", add_help=False)
    p.add_argument("--profile", default="")
    p.add_argument("--purpose", default="")
    p.add_argument("--shards", default="affected")
    p.add_argument("--base", default="origin/streamline/0.6.0")
    p.add_argument("--sha", default="HEAD")
    p.add_argument("--wheel", default="")
    p.add_argument("--jar-path", default="")
    p.add_argument("--max-parallel", type=int, default=0, help="shards side by side per cluster (default: the node's cores / 8)")
    p.add_argument("--clusters", type=int, default=0, help="clusters to spread over (default: every cluster the pool has that is free)")
    p.add_argument("--split", default="auto", help="auto: cut shards longer than --target-min by file; off: CI's shards as they are")
    p.add_argument("--target-min", type=float, default=0.0, help="length a cut shard aims at (default: chosen so there are about 3.5 chunks per slot)")
    p.add_argument("--timings", default=TIMINGS)
    p.add_argument("--serverless", action="store_true")
    p.add_argument("--fresh-cluster", action="store_true")
    p.add_argument("--pool-light", default=os.environ.get("GBX_TEST_POOL_LIGHT", ts.POOL_LIGHT))
    p.add_argument("--pool-jar", default=os.environ.get("GBX_TEST_POOL_JAR", ts.POOL_JAR))
    p.add_argument("--node-type", default=DEFAULT_NODE)
    p.add_argument("--shard-cap-min", type=float, default=45.0)
    p.add_argument("--stall-min", type=float, default=10.0)
    p.add_argument("--start-cap-min", type=float, default=25.0)
    p.add_argument("--pool-wait-min", type=float, default=30.0)
    p.add_argument("--serverless-cap", type=int, default=SERVERLESS_WORKSPACE_CAP,
                   help="runner tasks of gbx Serverless test runs at once in the workspace; a launch waits for room (0: no gate)")
    p.add_argument("--pip-extra", default="")
    p.add_argument("--env", action="append", default=[], metavar="KEY=VAL", help="an environment variable every shard sets (repeatable)")
    p.add_argument("--spark-conf", action="append", default=[], metavar="KEY=VAL", help="a Spark conf every shard's session sets (repeatable)")
    p.add_argument("--run-id", default="")
    p.add_argument("--keep-stage", action="store_true")
    p.add_argument("--explain-only", action="store_true")
    p.add_argument("--allow-broad", action="store_true", help="let `affected` select over a quarter of the light bank")
    p.add_argument("--first-failed", type=int, default=sr.POLICY["first_k"], help="stop the run when the first k chunks to finish all failed (0: off)")
    p.add_argument("--max-failed-chunks", type=int, default=sr.POLICY["max_failed_chunks"], help="stop the run at n failed chunks (0: off)")
    p.add_argument("--max-failures", type=int, default=sr.POLICY["max_failures"], help="stop the run at n failed or erroring tests (0: off)")
    p.add_argument("--maxfail", type=int, default=None, help=f"pytest's --maxfail in each chunk (default {sr.POLICY['maxfail']}; 0: off)")
    p.add_argument("--no-fail-fast", action="store_true", help="run every chunk to its end: no stop rule, no --maxfail (the smoke stays)")
    p.add_argument("--no-smoke", action="store_true", help="start the whole queue at once, without the smoke chunk first")
    p.add_argument("--start-chunks", type=int, default=sr.POLICY["start_chunks"], help="runners that start at once; the rest wait for that many passed chunks (0: all; refused with --serverless)")
    p.add_argument("--test-timeout", type=float, default=TEST_TIMEOUT_S, help="fail a test that runs longer than this, in seconds (0: no limit)")
    p.add_argument("--overrun-x", type=float, default=OVERRUN_X, help="stop a chunk at this many times its estimate (0: off)")
    p.add_argument("--max-wall-min", type=float, default=MAX_WALL_MIN, help="stop the run when its wall time reaches this (0: no budget)")
    p.add_argument("--rerun-failed", default="", metavar="RUN_ID", help="run only the tests that failed in that earlier run (its local results)")
    p.add_argument("--ids-file", default="", metavar="PATH", help="run only the test node ids listed in this file, one per line")
    p.add_argument("--ids-per-chunk", type=int, default=8, help="node ids per chunk with --ids-file (default 8)")
    return p.parse_args(argv)


TEST_TIMEOUT_S = 120.0  # --test-timeout: a test (its call; setup and teardown get 3x) that runs longer fails
OVERRUN_X = 2.0  # --overrun-x: a chunk stops at 2x its estimate, never under OVERRUN_FLOOR_MIN
OVERRUN_FLOOR_MIN = 3.0
SERVERLESS_OVERRUN_FLOOR_MIN = 5.0  # a Serverless chunk spends 1.5-3 min before its first test (measured 2026-11-11)
MAX_WALL_MIN = 20.0  # --max-wall-min: the run stops itself at this wall time


def fail_fast(a) -> dict:
    """The run's fail-fast policy as the runners and the launcher read it (``_shard_runner.stop_rule``). ``--rerun-failed`` stops a
    chunk at its first failure."""
    default_maxfail = 1 if a.rerun_failed else sr.POLICY["maxfail"]
    maxfail = a.maxfail if a.maxfail is not None else (0 if a.no_fail_fast else default_maxfail)
    return {"first_k": a.first_failed, "max_failed_chunks": a.max_failed_chunks, "max_failures": a.max_failures, "maxfail": maxfail,
            "off": bool(a.no_fail_fast), "smoke": not a.no_smoke, "start_chunks": a.start_chunks, "test_timeout_s": a.test_timeout,
            "overrun_x": a.overrun_x, "overrun_floor_min": SERVERLESS_OVERRUN_FLOOR_MIN if a.serverless else OVERRUN_FLOOR_MIN,
            "max_wall_min": a.max_wall_min}


def fail_fast_text(ff: dict) -> str:
    if ff.get("off"):
        rules = "off (--no-fail-fast): every chunk runs to its end"
    else:
        parts = [f"the first {ff['first_k']} chunks to finish all failed" if ff.get("first_k") else "",
                 f"{ff['max_failed_chunks']} failed chunks" if ff.get("max_failed_chunks") else "",
                 f"{ff['max_failures']} failed tests" if ff.get("max_failures") else ""]
        rules = "stop the run at " + (", or ".join(x for x in parts if x) or "nothing (every rule set to 0)")
    more = [f"pytest --maxfail={ff['maxfail']} per chunk" if ff.get("maxfail") else "no --maxfail",
            f"a test fails past {ff['test_timeout_s']:g} s" if ff.get("test_timeout_s") else "no test timeout",
            f"a chunk stops at {ff['overrun_x']:g}x its estimate" if ff.get("overrun_x") else "no overrun stop",
            f"the run stops at {ff['max_wall_min']:g} min of wall" if ff.get("max_wall_min") else "no wall budget",
            f"{ff['start_chunks']} runner(s) start, the rest once {ff['start_chunks']} chunk(s) passed" if ff.get("start_chunks") else "every runner starts at once",
            "a smoke chunk runs first and its failure stops the run" if ff.get("smoke") else "no smoke chunk (--no-smoke)"]
    return rules + "; " + "; ".join(more)


def problems(a) -> list:
    out = []
    if not a.explain_only and not " ".join(a.purpose.split()):
        out.append("--purpose is required on a launch: say what the run tests and why (it heads the notebooks and rides on the run name)")
    if not a.explain_only and not (a.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")):
        out.append("--profile is required on a launch (the command never picks a profile)")
    if a.run_id and not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", a.run_id):
        out.append("--run-id may hold letters, digits, '.', '_' and '-' only (it names a Volume folder)")
    if a.max_parallel < 0 or a.clusters < 0:
        out.append("--max-parallel and --clusters must not be negative")
    if min(a.first_failed, a.max_failed_chunks, a.max_failures, a.maxfail if a.maxfail is not None else 0, a.start_chunks, a.test_timeout,
           a.overrun_x, a.max_wall_min) < 0:
        out.append("--first-failed, --max-failed-chunks, --max-failures, --maxfail, --start-chunks, --test-timeout, --overrun-x and --max-wall-min "
                   "must not be negative (0 turns one off)")
    if a.serverless and a.serverless_cap and a.max_parallel > a.serverless_cap:
        out.append(f"--max-parallel {a.max_parallel} is over --serverless-cap {a.serverless_cap}: the runner tasks of a Serverless job "
                   "share its 4-core driver hosts (tst-sls-p1-dem); lower it, or pass --serverless-cap 0 to lift the gate")
    if a.serverless and a.start_chunks == 0:
        out.append("--start-chunks 0 is refused with --serverless: every runner starting at once lost 5 of 8 to "
                   "ENVIRONMENT_DOWNLOAD_USER_ERROR.NOT_FOUND (2026-11-12); keep the default ramp, or give a positive n")
    if a.ids_file and a.rerun_failed:
        out.append("--ids-file and --rerun-failed each choose the tests to run: give one")
    if a.ids_file and not Path(a.ids_file).is_file():
        out.append(f"--ids-file {a.ids_file}: no such file")
    if a.ids_per_chunk < 1:
        out.append("--ids-per-chunk must be 1 or more")
    if a.rerun_failed and not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", a.rerun_failed):
        out.append("--rerun-failed takes the run id of an earlier run (its results under test-logs/databricks-tests/<run id>/)")
    if a.split not in ("auto", "off"):
        out.append("--split is auto or off")
    if a.target_min and a.target_min <= ts.FIXED_MIN * 2:
        out.append(f"--target-min must be well above the {ts.FIXED_MIN:g} min a task spends before its first test")
    for flag in ("shard_cap_min", "stall_min", "start_cap_min"):
        if getattr(a, flag) <= 0:
            out.append(f"--{flag.replace('_', '-')} must be positive")
    for kv in a.env:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", kv):
            out.append(f"--env takes KEY=VAL, not {kv!r}")
    for kv in a.spark_conf:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*=.*", kv):
            out.append(f"--spark-conf takes KEY=VAL, not {kv!r}")
    return out


def extra_env(a) -> dict:
    """The --env variables, as every shard sets them (after its own: GBX_SHARD, GBX_ORACLE_TABLES)."""
    return dict(kv.split("=", 1) for kv in a.env)


def chunk_env(s, a) -> dict:
    """A chunk's env: its own (GBX_SHARD, GBX_ORACLE_TABLES), then --env over it. A chunk's own oracle tables win: --env
    GBX_ORACLE_TABLES narrowed the family file to them, and the cut gave this chunk some of them (the whole --env list here
    would run every chunk on every table)."""
    own = ts.shard_env(s)
    env = {**own, **extra_env(a)}
    if own.get("GBX_ORACLE_TABLES"):
        env["GBX_ORACLE_TABLES"] = own["GBX_ORACLE_TABLES"]
    return env


def spark_confs(a) -> dict:
    """The --spark-conf settings, as every shard's session sets them before its tests."""
    return dict(kv.split("=", 1) for kv in a.spark_conf)


def default_run_id() -> str:
    return f"tst-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def load_config() -> None:
    f = cluster_config_file(TESTS_DIR)
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                if k.strip() and v.strip() and not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip()


# ---------------------------------------------------------------------------------------------- the plan


def pool_of(shard: ts.Shard, a) -> str:
    return a.pool_jar if shard.pool == ts.POOL_JAR else a.pool_light


def make_plan(a, sha: str, shards: List[ts.Shard], wheel: Optional[Path], jar: Optional[dict], volroot: str, root: Path, resources: Optional[dict] = None,
              fsolo: Optional[Dict[str, float]] = None) -> dict:
    run_id = a.run_id
    slugs = [sh.slug for sh in shards]
    dup = sorted({x for x in slugs if slugs.count(x) > 1})
    if dup:
        raise ValueError(f"shards whose names make the same task key: {dup}; the second would replace the first and never run")
    stage = f"{volroot}/test-runs/{run_id}"
    wheel_sha = mut.sha256_file(wheel) if wheel and Path(wheel).is_file() else "0" * 64  # explain-only may name a wheel that is not there
    caps = {"shard_min": a.shard_cap_min, "stall_min": a.stall_min, "start_min": a.start_cap_min}
    by_pool: Dict[str, List[ts.Shard]] = {}
    for s in shards:
        key = "serverless" if a.serverless else pool_of(s, a)
        by_pool.setdefault(key, []).append(s)
    pools = {}
    slots = plan_slots(a)
    ff = fail_fast(a)
    smokes: Dict[str, dict] = {}
    for name, items in by_pool.items():
        jar_pool = bool(any(sh.pool == ts.POOL_JAR for sh in items) and not a.serverless)
        kind = "serverless" if name == "serverless" else ("jar" if jar_pool else "light")
        pools[name] = {"jar": jar_pool, "table": queue_table(volroot, run_id, kind)}
        # Serverless: every runner task has compute of its own, so no chunk is heavy for its neighbours (no gate)
        heavy = set() if a.serverless else {s.slug for s in items if s.heavy}
        expected = {s.slug: s.expected_min for s in items}
        smoke = smoke_chunk(items, kind, sha, root, a, name) if ff["smoke"] and len(items) >= 2 else None
        if smoke:
            smokes[smoke["slug"]] = smoke
            expected[smoke["slug"]] = smoke["expected_min"]
        size_pool(pools[name], expected, slots, 1 if a.serverless else max(1, a.clusters), a.shard_cap_min, heavy,
                  smoke=smoke["slug"] if smoke else "", start=ff["start_chunks"])
    return {
        "run_id": run_id, "attempt": uuid.uuid4().hex[:6], "sha": sha, "purpose": " ".join(a.purpose.split()), "stage": stage, "caps": caps, "fail_fast": ff,
        "wheel": {"local": str(wheel) if wheel else "", "name": wheel.name if wheel else "",
                  "built_from": mut.wheel_git_sha(wheel) if wheel else "",
                  "sha256": wheel_sha,
                  "volume": f"{volroot}/test-pool-data/wheel-{wheel_sha[:12]}/{wheel.name}" if wheel else ""},
        "tarball": f"{stage}/{run_id}-src.tar.gz", "plan_file": f"{stage}/plan.json",
        "jar": jar, "resources": resources, "extras": EXTRAS, "pins": [PYTEST_PIN], "pip_extra": a.pip_extra,
        "pools": pools, "serverless": bool(a.serverless), "node_type": a.node_type, "fresh_cluster": bool(a.fresh_cluster),
        "local_out": f"test-logs/databricks-tests/{run_id}",
        "shards": {
            s.slug: {
                "name": s.name, "kind": s.kind, "args": ts.pytest_args(s), "env": chunk_env(s, a), "spark_conf": spark_confs(a), "expected_min": round(s.expected_min, 1),
                "test_part": s.test_part, "est_src": s.est_src or "ci", "heavy": bool(s.heavy and not a.serverless), "unit_max_min": round(s.unit_max_min, 1), "subset": bool(s.subset),
                "solo_min": round(s.solo_min or s.expected_min, 1),
                "contention": round(s.expected_min / s.solo_min, 2) if s.solo_min else 1.0,
                "unit_solo_s": unit_solo(s, root, fsolo or {}),
                "stall_min": ts.stall_min_for(s.unit_max_min, a.stall_min, a.shard_cap_min),
                "files": [] if s.oracle_tables else (ts.files_of(s, root) or []),
                "tables": [t for t in s.oracle_tables.split(",") if t],
                "cwd": "." if s.kind == "scripts" else ts.PKG, "pool": "serverless" if a.serverless else pool_of(s, a),
                "select_k": s.select_k,
                # may this chunk collect nothing? a part of a split shard (its unit is judged over all its parts) or a subset may; a
                # rerun or a whole shard may not (its run fails, and so its runner and the job)
                "empty_ok": bool(s.name.endswith("]") or s.subset),
            }
            for s in shards
        } | {slug: {k: v for k, v in sm.items() if k != "slug"} for slug, sm in smokes.items()},
    }


SMOKE_TESTS = 2  # the tests the smoke runs from the run's own fastest chunk
SMOKE_CANDIDATES = 3  # the fastest chunks it tries, in turn, for one that collects a test with its env, -k and markers


def smoke_chunk(items: List[ts.Shard], kind: str, sha: str, root: Path, a, pool: str) -> Optional[dict]:
    """The pool's smoke chunk, from the run's own selection: a collect-only of every test file its chunks run (an import error
    anywhere shows in seconds), then the first SMOKE_TESTS tests of its fastest chunk, collected and run with that chunk's env,
    ``-k``, markers and test part. It tries the next fastest (up to SMOKE_CANDIDATES) when one collects nothing; when none
    collects a test, the smoke fails as a selection error. None when the pool runs no test of the package (scripts/ci only)."""
    pkg = [s for s in items if s.kind != "scripts"]
    files = sorted({f for s in pkg for f in ((ts.files_of(s, root) or []) if not s.oracle_tables else [tm.FAMILY_FILE])})
    files += sorted({p.split("::")[0] for s in pkg if not s.oracle_tables and not ts.files_of(s, root) for p in s.paths})
    if not files:
        return None
    light = [s for s in items if not (s.heavy and not a.serverless)] or items
    cands = [s.slug for s in sorted(light, key=lambda s: (s.expected_min, s.slug))][:SMOKE_CANDIDATES]
    return {"slug": f"smoke-{kind}", "name": "smoke", "kind": "smoke", "args": [], "candidates": cands, "smoke_tests": SMOKE_TESTS,
            "collect": ["-m", "not integration"] + sorted(set(files)), "env": dict(extra_env(a)), "spark_conf": spark_confs(a),
            "expected_min": round(ts.FIXED_MIN + 1.0, 1), "test_part": "", "est_src": "ci", "heavy": False, "unit_max_min": 0.5,
            "subset": True, "solo_min": round(ts.FIXED_MIN + 1.0, 1), "contention": 1.0, "unit_solo_s": {},
            "stall_min": a.stall_min, "files": [], "tables": [], "cwd": ts.PKG, "pool": pool, "select_k": "", "smoke": True}


SRC_MARK = {"history": "h", "partial": "p", "lower bound": "lb", "ci": "ci"}
# Serverless: runners side by side by default. The runner tasks of one job share its driver hosts: tst-sls-p1-dem (run alone, 8
# runners) put every chunk's driver on one of 2 hosts with 4 cores, load1 median 15.9 (max 35), 165-1248 MB free, and raising rows
# took 15 s (median) with 3 or fewer chunks active against 28 s with 9-12. So a job runs 4 by default, and the launch waits until the
# workspace's gbx Serverless test runs leave room under SERVERLESS_WORKSPACE_CAP runner tasks (--serverless-cap).
SERVERLESS_SLOTS = 4
SERVERLESS_WORKSPACE_CAP = 8
SERVERLESS_TARGET = (3.0, 5.0)  # the chunk length a Serverless cut aims at, in minutes: a few minutes, so failures show early
POOL_TARGET = (3.0, 5.0)  # the same on a pool cluster; either is a hard cap: no chunk is planned over it


def plan_slots(a) -> int:
    """Runners per cluster (per pool on Serverless): --max-parallel, else SERVERLESS_SLOTS on Serverless, else the node's cores / 8."""
    if a.max_parallel:
        return a.max_parallel
    return SERVERLESS_SLOTS if a.serverless else ts.slots_for(mut.cores_of(a.node_type) or 32)


def unit_solo(s: ts.Shard, root: Path, fsolo: Dict[str, float]) -> Dict[str, float]:
    """The solo-expected seconds of the chunk's timed units (files, or ``table:<T>``): what a run compares their actual times with."""
    keys = tm.chunk_keys({"tables": [t for t in s.oracle_tables.split(",") if t], "files": [] if s.oracle_tables else (ts.files_of(s, root) or [])})
    return {k: round(fsolo[k], 1) for k in keys if k in fsolo}


def queue_table(volroot: str, run_id: str, kind: str) -> str:
    """The Delta table that holds a pool's queue: in the schema of the run's Volume, named for the run and the pool's kind."""
    parts = volroot.strip("/").split("/")
    catalog, schema = (parts[1], parts[2]) if len(parts) >= 3 and parts[0] == "Volumes" else ("main", "default")
    name = re.sub(r"[^A-Za-z0-9_]", "_", f"gbx_test_queue_{run_id}_{kind}")[:200]
    return f"`{catalog}`.`{schema}`.`{name}`"


def schedule_wall(expected: Dict[str, float], queue: List[str], heavy: set, runners: int, n_clusters: int) -> float:
    """The wall of the queue as the runners take it: the earliest-free runner takes the first chunk it may (runner i is on cluster
    i % n_clusters, and a heavy chunk may start only while no other heavy chunk runs there). A runner with nothing it may take waits
    for the end of the heavy chunk on its cluster."""
    free = [0.0] * runners
    busy_until = [0.0] * max(1, n_clusters)  # when the heavy chunk of each cluster ends
    todo = list(queue)
    while todo:
        r = min(range(runners), key=lambda i: free[i])
        c = r % max(1, n_clusters)
        now = free[r]
        pick = next((s for s in todo if s not in heavy or busy_until[c] <= now), None)
        if pick is None:  # only heavy chunks are left and one runs here: wait for it
            free[r] = busy_until[c]
            continue
        todo.remove(pick)
        free[r] = now + expected[pick]
        if pick in heavy:
            busy_until[c] = free[r]
    return max(free)


def ramp_wall(expected: Dict[str, float], queue: List[str], runners: int, start: int, need: int) -> float:
    """The wall of the queue with the ramp: runners ``0..start-1`` take chunks from the start, the others once ``need`` chunks have
    ended (the launch waits for clean results before it goes wide). List scheduling onto the runner that frees first."""
    if not start or start >= runners:
        start = runners
    free = [0.0] * start
    ends: List[float] = []
    todo = list(queue)
    opened = start == runners
    while todo:
        i = min(range(len(free)), key=lambda j: free[j])
        if not opened and len(ends) >= need and free[i] >= sorted(ends)[need - 1]:
            free += [sorted(ends)[need - 1]] * (runners - start)  # the held runners start when the need-th chunk ends
            opened = True
            continue
        s = todo.pop(0)
        free[i] += expected[s]
        ends.append(free[i])
    return max(free) if free else 0.0


def size_pool(spec: dict, expected: Dict[str, float], slots: int, n_clusters: int, shard_cap_min: float, heavy: Optional[set] = None,
              smoke: str = "", start: int = 0) -> None:
    """Fill a pool's plan: the queue (longest expected first), the runners (clusters x slots, never more than the shards), and the
    expected wall: the longest-first schedule onto the slot that frees first, which is what the runners do. The cap is the
    runner task's timeout: every shard of the busiest runner at the shard cap, plus one more when the shards outnumber the runners.
    ``heavy`` names the chunks that run one at a time per cluster: the wall waits for them in turn, and the cap allows for it.
    ``smoke`` (a slug of ``expected``) heads the queue and runs alone first; the wall adds its time. ``start``: the ramp (only that
    many runners start; the rest once that many chunks passed), which the wall and the runners' cap allow for."""
    heavy = {s for s in (heavy or ()) if s in expected}
    first = smoke if smoke in expected else ""
    rest = {s: m for s, m in expected.items() if s != first}
    order = sr.queue_order(rest)
    queue = ([first] if first else []) + order
    runners = max(1, min(slots * n_clusters, len(rest)))
    est = [ts.Shard(name=slug, kind="light", est_min=m) for slug, m in rest.items()]
    load = [sum(s.expected_min for s in lane) for lane in ts.lanes(est, runners)]
    wall = max(load)
    per_runner = math.ceil(len(rest) / runners) + (1 if len(rest) > runners else 0)
    cap = per_runner * (shard_cap_min + 10)
    if start and start < runners:
        wall = max(wall, ramp_wall(rest, order, runners, start, start))
        cap += math.ceil(start / max(1, start)) * (shard_cap_min + 10)  # a held runner waits for the first chunks to pass
    if heavy:
        wall = max(wall, schedule_wall(rest, order, heavy, runners, n_clusters))
        cap += math.ceil(len(heavy) / max(1, n_clusters)) * (shard_cap_min + 10)  # a runner may wait out the heavy chunks of its cluster
    spec.update(
        queue=queue, slots=slots, clusters=n_clusters, runners=runners, heavy=[s for s in queue if s in heavy], smoke=first,
        start=min(start, runners) if start else runners,
        serial_min=round(sum(expected.values()), 1), longest_min=round(max(expected.values()), 1),
        expected_min=round(wall + (expected[first] if first else 0.0), 1), cap_min=cap,
        smoke_cap_min=(shard_cap_min + 10) if first else 0,
    )


def explain(plan: dict, local_only: List[ts.Shard], notes: List[str]) -> str:
    c = plan["caps"]
    lines = [f"gbx:test:databricks plan {plan['run_id']} for commit {plan['sha'][:10]}: {plan['purpose'] or '(no purpose given)'}"]
    for n in notes:
        lines.append(f"  NOTE: {n}")
    for pool, spec in plan["pools"].items():
        where = "Serverless env 6 (no cluster)" if pool == "serverless" else f"pool {pool} ({'with' if spec['jar'] else 'no'} JAR, single node {plan['node_type']})"
        if plan["fresh_cluster"] and pool != "serverless":
            where = f"a new single-node cluster {plan['node_type']} ({'with' if spec['jar'] else 'no'} JAR) that ends with the run"
        lines.append(f"  {where}: {len(spec['queue'])} shard(s) on {spec['clusters']} cluster(s) x {spec['slots']} slots = {spec['runners']} runner(s); expected wall about "
                     f"{spec['expected_min']:g} min after the start (the longest shard is {spec['longest_min']:g} min, the serial sum {spec['serial_min']:g} min), at most {spec['cap_min']:g} min")
        lines.append(f"    queue (longest first; each runner takes the next when it is free): "
                     + ", ".join(f"{plan['shards'][s]['name']} (~{plan['shards'][s]['expected_min']:g} min, {SRC_MARK.get(plan['shards'][s].get('est_src'), 'ci')}"
                                 + (", heavy" if plan["shards"][s].get("heavy") else "")
                                 + (f", part {plan['shards'][s]['test_part']}" if plan["shards"][s].get("test_part") else "")
                                 + (f", -k {plan['shards'][s]['select_k']}" if plan["shards"][s].get("select_k") else "")
                                 + (f", x{plan['shards'][s]['contention']:g}" if plan["shards"][s].get("contention", 1.0) >= 1.2 else "") + ")" for s in spec["queue"]))
        lines.append("    (estimates: h = history, p = history for some of its files, lb = a lower bound from a stopped chunk, ci = CI's minutes; "
                     "xN = the estimate carries its units' contention factor, N times the solo time)")
        if spec.get("heavy"):
            lines.append(f"    heavy: {len(spec['heavy'])} chunk(s) ({', '.join(plan['shards'][s]['name'] for s in spec['heavy'])}) hold the GDAL engine's tables, a "
                         f"unit recorded at {ts.HEAVY_UNIT_MIN:g} min or more, or a contention-heavy unit ({tm.CONTENTION_HEAVY:g}x or more beside other chunks); "
                         "at most one runs on a cluster at a time, the other slots of the node take light chunks meanwhile")
        longer = [s for s in spec["queue"] if plan["shards"][s].get("stall_min", c["stall_min"]) > c["stall_min"]]
        if longer:
            lines.append(f"    stall cap: {c['stall_min']:g} min, and {ts.STALL_SHARE:g} of the longest file or table of a chunk when that is more: "
                         + ", ".join(f"{plan['shards'][s]['name']} {plan['shards'][s]['stall_min']:g} min" for s in longer))
        lines.append(f"    queue table: {spec['table']} (made by the job's first task, dropped at the end)")
    lines.append(f"  staging: {plan['stage']}/ (source tarball, plan.json); removed at the end")
    lines.append(f"  wheel: {plan['wheel']['name'] or '-'}"
                 + (f", built from {plan['wheel']['built_from'][:10]}" if plan["wheel"]["built_from"] else "")
                 + (f" -> {plan['wheel']['volume']} (a folder per wheel content, staged once and checked by sha256)" if plan["wheel"]["volume"] else ""))
    if plan.get("resources"):
        lines.append(f"  test resources: tree {plan['resources']['tree'][:12]} -> {plan['resources']['volume']} (staged once per content)")
    if plan["jar"]:
        lines.append(f"  JAR: {plan['jar']['name']} sha256 {plan['jar']['sha256'][:16]} -> {plan['jar']['volume']} (the pool reinstalls only when it differs)")
    lines.append(f"  caps: {c['shard_min']:g} min per shard, {c['stall_min']:g} min without a finished test, {c['start_min']:g} min to the first beat")
    if plan.get("fail_fast"):
        lines.append(f"  fail fast: {fail_fast_text(plan['fail_fast'])}")
        budget = plan["fail_fast"].get("max_wall_min") or 0
        worst = max((spec["expected_min"] for spec in plan["pools"].values()), default=0)
        if budget and worst > budget:
            lines.append(f"  WARNING: the expected wall ({worst:g} min) passes --max-wall-min {budget:g}: the run stops at the budget. Narrow "
                         "--shards, add clusters or runners, or raise the budget")
    for pool, spec in plan["pools"].items():
        if spec.get("smoke"):
            sm = plan["shards"][spec["smoke"]]
            lines.append(f"  smoke ({pool}, ~{sm['expected_min']:g} min, alone first; the runners start when it passes): collect-only of "
                         f"{len(sm['collect']) - 2} file(s), then the first {sm['smoke_tests']} test(s) of the fastest chunk that collects one, "
                         f"with its env and options (of: {', '.join(plan['shards'][c]['name'] for c in sm['candidates'])})")
    lines.append(f"  results: {plan['local_out']}/ (junit.xml + pytest.log per shard, summary.md)")
    for s in local_only:
        lines.append(f"  local only: {s.name}: {s.local_only}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------- the runner notebook


def header_md(plan: dict, pool: str) -> str:
    spec = plan["pools"][pool]
    work = [x for x in spec["queue"] if x != spec.get("smoke")] or spec["queue"]
    c = plan["caps"]
    return (
        f"**Test shards `{plan['run_id']}`** (commit `{plan['sha'][:10]}`): {plan['purpose']}  \n"
        f"**Output:** per shard, `junit.xml` and `pytest.log` in `{plan['stage']}/out/<shard>/` (the launcher downloads them); "
        f"the verdict per shard is passed / failed / errors / skipped, and the notebook's result is that count as JSON.  \n"
        f"**Time:** each shard about {min(plan['shards'][x]['expected_min'] for x in work):g}-{max(plan['shards'][x]['expected_min'] for x in work):g} min "
        f"(recorded or CI's times); {spec['runners']} runner(s) take the shards from one queue, longest first; the run about {spec['expected_min']} min after the start, "
        f"at most {spec['cap_min']} min.  \n"
        f"**Monitoring:** caps: {c['shard_min']:g} min per shard, {c['stall_min']:g} min without a finished test (the shard then dumps its stacks and stops; its runner "
        f"takes the next); heartbeats `{plan['stage']}/hb/<shard>.json` and `{plan['stage']}/runners/<runner>.json` (the launcher cancels a run whose shards are all stale); "
        f"a one-line result per chunk the moment it ends, `{plan['stage']}/results/<shard>.json`. "
        f"Fail fast: {fail_fast_text(plan.get('fail_fast') or {}) if plan.get('fail_fast') else 'off'}. "
        f"Cancel: `databricks jobs cancel-run <run id> --profile <profile>` (the id is printed at submit)."
    )


def install_source(plan: dict) -> str:
    """The wheel is forced in first (--force-reinstall --no-deps, geobrix only): a same-version geobrix already on the
    cluster makes a plain install of the staged wheel a no-op. The second line adds the extras and the pins."""
    reqs = " ".join(f'"{r}"' for r in plan["pins"] + plan["pip_extra"].split())
    return (wg.force_line(plan["wheel"]["volume"], quiet=True) + "\n"
            + f'%pip install --quiet "geobrix[{plan["extras"]}] @ file://{plan["wheel"]["volume"]}" {reqs}\n')


def guard_source(plan: dict) -> str:
    """The cell after the install: the driver and the Python workers must import the staged wheel (before the checkout's
    package is laid over it), else the shard fails here with the fix."""
    built = plan["wheel"]["built_from"]
    # driver only: a Python worker that imports geobrix here would keep the wheel's modules in memory after the checkout is
    # laid over them; the workers are checked after the copy (run_source), against the checkout
    return wg.guard_source(wheel=plan["wheel"]["volume"], git_sha=built if re.fullmatch(r"[0-9a-f]{7,40}", built or "") else None, workers=False)


def run_source(plan: dict) -> str:
    """The cell that unpacks the checkout, runs one shard's pytest in this process and writes its files."""
    # the chunk fails its own run by the rule its runner and fail fast use (_shard_runner.chunk_failed, inlined: no copy on the node)
    helpers = ("import json, os, re\nfrom typing import Callable\n\n\n" + inspect.getsource(sr.chunk_failed) + "\n\n" + inspect.getsource(sr.node_checkout)
               + "\n\n" + inspect.getsource(sr.interrupt_session) + "\n\n" + inspect.getsource(sr.compute_probe))
    return helpers + '''
import faulthandler, json, os, shutil, signal, sys, tarfile, threading, time, _thread

STAGE = %(stage)r
SLUG = dbutils.widgets.get("shard")
PLAN = json.load(open(STAGE + "/plan.json"))
S = PLAN["shards"][SLUG]
CAPS = PLAN["caps"]
STALL_MIN = S.get("stall_min") or CAPS["stall_min"]  # scaled to the chunk's longest file or table (see _test_shards.stall_min_for)
t0 = time.time()
PHASES = globals().get("_GBX_PHASES") or {}  # the start cell's marks (the runner's call, the notebook's start), then this cell's
PHASES["run_cell"] = t0
PHASE_T0 = PHASES.get("call") or PHASES.get("nb_start") or t0


def phase(name):  # a setup phase ends: its time since the runner's call, printed and kept for the result (the first mark of a name wins)
    PHASES.setdefault(name, time.time())
    print("phase=%%s t=%%.1f" %% (name, PHASES[name] - PHASE_T0), flush=True)


def PHASES_OUT():
    return {k: round(v - PHASE_T0, 1) for k, v in sorted(PHASES.items(), key=lambda kv: kv[1])}


try:  # the node's local disk; Serverless has none to write
    os.makedirs("/local_disk0/gbxtest", exist_ok=True)
    scratch = "/local_disk0/gbxtest"
except OSError:  # Serverless: the temp disk, at a path every chunk of the runner finds again (its checkout is there)
    import tempfile

    scratch = tempfile.gettempdir() + "/gbxtest"


def _shared_dir(d):  # a folder the next chunks may write even when they run as another user (as /tmp is: sticky, everyone)
    os.makedirs(d, exist_ok=True)
    try:
        os.chmod(d, 0o1777)
    except OSError:
        pass


# a folder per launch attempt (run id + attempt): a relaunch under the same run id never meets the files, or the tables that
# own them, of a cancelled attempt
base = scratch + "/%%s-%%s" %% (PLAN["run_id"], PLAN.get("attempt", "0"))
OWN_BASE = False  # True: a temp folder of this chunk's own, removed at its end with everything in it
STALE_SCRATCH_S = 6 * 3600  # an earlier launch's scratch on this node older than this is removed (no run lasts that long)
# On Serverless each chunk's notebook run is a user of its own (its uid), so a runner's checkout is never reused there, and the
# platform recycles those uids (1008-1013 again in tst-sls-b7). A folder another chunk's user made (the shared scratch, the
# launch's folder) went with its files, chunks beside it lost their trees within seconds of a run ending (tst-sls-b2, -b6, -b7).
# So there each chunk works in a temp folder its own user makes.
OWN_SCRATCH = bool(PLAN.get("serverless"))
try:
    if OWN_SCRATCH:
        raise OSError("Serverless: a temp folder of this chunk's own")
    _shared_dir(scratch)
    for _old in os.listdir(scratch):  # earlier launches' folders on this node (a pool node lives on): gone after STALE_SCRATCH_S
        _p = scratch + "/" + _old
        try:
            if _p != base and time.time() - os.path.getmtime(_p) > STALE_SCRATCH_S:
                shutil.rmtree(_p, ignore_errors=True)
        except OSError:
            pass
    _shared_dir(base)
    root = base + "/" + SLUG
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root + "/out")
except OSError:  # Serverless, or a folder this chunk cannot write: one of its own (the checkout is then unpacked for this chunk alone)
    import tempfile

    base = tempfile.mkdtemp(prefix="gbxtest-")
    OWN_BASE = True
    root = base + "/" + SLUG
    os.makedirs(root + "/out")
try:  # the runner that runs this chunk: its chunks share one checkout, one after another
    RUNNER = dbutils.widgets.get("runner")
except Exception:
    RUNNER = ""
phase("scratch")
# On Serverless the Python workers are other machines: they run the wheel (the environment spec's), never the checkout laid
# over the driver's copy, so their check needs no overlay. It starts here, in a thread, and runs while the checkout unpacks
# (its first UDF also starts the workers, about 20 s); the guard after the overlay compares what it found. On a pool node the
# workers share the driver's environment, so there it runs after the overlay, as before.
_EARLY = {}


def _collect_early():
    try:
        _EARLY["seen"] = collect_workers(spark, exclude=("_build_info.py",))
    except Exception as exc:  # the guard reports it as a failed worker check
        _EARLY["error"] = exc


_WORKER_CHECK = threading.Thread(target=_collect_early, daemon=True)
EARLY_WORKERS = bool(PLAN.get("serverless"))
if EARLY_WORKERS:
    _WORKER_CHECK.start()


def _unpack(repo):
    co = os.path.dirname(repo)
    shutil.copy(PLAN["tarball"], co + "/src.tar.gz")  # one read of the Volume file, then local disk
    with tarfile.open(co + "/src.tar.gz") as tf:
        tf.extractall(repo, filter="data")
    os.remove(co + "/src.tar.gz")
    if PLAN.get("resources"):  # the test resources, shared by every run of the same content
        shutil.copy(PLAN["resources"]["volume"], co + "/res.tar.gz")
        with tarfile.open(co + "/res.tar.gz") as tf:
            tf.extractall(repo, filter="data")
        os.remove(co + "/res.tar.gz")
    # some tests ask git for the checkout's root, HEAD or its files (git ls-files, ls-tree): the tarball has no history, so make it
    # a one-commit repository that holds every file, as the commit under test does
    for cmd in (["init", "-q"], ["add", "-A", "-f"], ["-c", "user.name=gbx", "-c", "user.email=gbx@example.com", "commit", "-q", "-m", "run"]):
        _git(repo, *cmd)


def _git(repo, *args):  # safe.directory: a later chunk may run as another user than the one that made the tree
    import subprocess

    return subprocess.run(["git", "-c", "safe.directory=*", "-C", repo, *args], capture_output=True, text=True)


def _reset(repo):  # what the runner's last chunk wrote into the tree goes; the JAR in lib/ stays. True, or what failed
    for cmd in (["clean", "-fdxq", "-e", "python/geobrix/lib/"], ["checkout", "-q", "--", "."]):
        r = _git(repo, *cmd)
        if r.returncode != 0:
            return "git %%s: rc %%s %%s" %% (cmd[0], r.returncode, (r.stderr or r.stdout).strip()[-160:])
    return True


t_co = time.time()
CHECKOUT, repo, CHECKOUT_WHY = node_checkout(base, RUNNER or SLUG, {"tarball": PLAN["tarball"], "resources": (PLAN.get("resources") or {}).get("tree", "")},
                                             _unpack, _reset)
CHECKOUT_S = round(time.time() - t_co, 1)
phase("checkout")
print("checkout:", CHECKOUT, "for runner", RUNNER or "-", "in", CHECKOUT_S, "s:", repo, ("(" + CHECKOUT_WHY + ")") if CHECKOUT_WHY else "", flush=True)
try:  # said at once, beside child.json: a chunk that fails later (the wheel guard, before pytest) still says how it got its tree
    with open(STAGE + "/out/" + SLUG + "/checkout.json", "w") as _f:
        json.dump({"checkout": CHECKOUT, "checkout_s": CHECKOUT_S, "runner": RUNNER, "repo": repo, "not_reused": CHECKOUT_WHY, "uid": os.getuid()}, _f)
except OSError:
    pass
pkg = repo + "/python/geobrix"
if PLAN.get("jar") and not os.path.exists(pkg + "/lib/" + PLAN["jar"]["name"]):  # the heavy tests look for the JAR in lib/ (it is a cluster library as well)
    os.makedirs(pkg + "/lib", exist_ok=True)
    shutil.copy(PLAN["jar"]["volume"], pkg + "/lib/" + PLAN["jar"]["name"])
OUT = STAGE + "/out/" + SLUG
os.makedirs(OUT, exist_ok=True)
os.makedirs(STAGE + "/hb", exist_ok=True)
HB = STAGE + "/hb/" + SLUG + ".json"

# The wheel brought the dependencies and the native payload. A notebook session does not read the wheel's .pth at its start,
# so do what the .pth does (it puts the payload's rasterio, pyproj ... ahead of the runtime's), then put the checkout's
# package over the installed one: the driver and the Python workers of the node (which use this session's environment)
# then run the code at the commit under test, with the payload beside it.
import _gbx_native_path
sitedir = os.path.dirname(os.path.abspath(_gbx_native_path.__file__))
print("native payload active:", _gbx_native_path.ACTIVE, "| sitedir", sitedir, flush=True)
inst, src_pkg = sitedir + "/databricks/labs/gbx", pkg + "/src/databricks/labs/gbx"
for dp, dn, fn in os.walk(inst):  # a module the commit deleted or renamed must not stay importable from the wheel's copy
    rel = os.path.relpath(dp, inst)
    if rel.split(os.sep)[0] in ("_vendor", "__pycache__"):
        continue
    for f in fn:
        if f.endswith(".py") and f != "_build_info.py" and not os.path.exists(os.path.join(src_pkg, rel, f)):
            os.remove(os.path.join(dp, f))
shutil.copytree(src_pkg, inst, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
# A test that starts a Python subprocess gets a plain interpreter that, like this session, never reads the wheel's .pth:
# a sitecustomize on PYTHONPATH does what the .pth does there.
os.makedirs(root + "/sc", exist_ok=True)
open(root + "/sc/sitecustomize.py", "w").write("try:\\n    import _gbx_native_path\\nexcept Exception:\\n    pass\\n")
os.environ["PYTHONPATH"] = os.pathsep.join(x for x in [root + "/sc", sitedir, os.environ.get("PYTHONPATH", "")] if x)
sys.path.insert(0, pkg)
for m in [m for m in sys.modules if m == "test" or m.startswith("test.") or m == "databricks.labs" or m.startswith("databricks.labs.")]:
    del sys.modules[m]
phase("overlay")
# On a cluster `databricks` is a regular package of the runtime whose path was fixed when the REPL started: add the sitedir's.
import databricks, pkgutil, filecmp
databricks.__path__ = pkgutil.extend_path(databricks.__path__, "databricks")
import databricks.labs.gbx as _gbx
assert os.path.realpath(_gbx.__file__).startswith(os.path.realpath(sitedir)) and filecmp.cmp(_gbx.__file__, pkg + "/src/databricks/labs/gbx/__init__.py", shallow=False), "the package under test is not the checkout's: " + _gbx.__file__
phase("import_gbx")
# The checkout wins, in the driver AND in the Python workers: every .py file of the imported package is the checkout's (the wheel's
# _build_info.py aside); the shard fails here, naming the differing files, rather than test the wheel's older copy
def _early_workers():  # what the early worker check found (it raises what the check raised)
    _WORKER_CHECK.join(600)
    if "error" in _EARLY:
        raise _EARLY["error"]
    if "seen" not in _EARLY:
        raise RuntimeError("the early worker check did not return in 600 s")
    return _EARLY["seen"]


if EARLY_WORKERS:
    WHEEL_GUARD_TREE = run_guard({"tree": src_pkg}, spark=spark, workers=_early_workers, exclude=("_build_info.py",))
else:
    WHEEL_GUARD_TREE = run_guard({"tree": src_pkg}, spark=spark, exclude=("_build_info.py",))
phase("guard_tree")
os.chdir(repo + "/" + S["cwd"])
os.environ.update(S["env"])
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
import pytest
phase("import_pytest")

log = open(root + "/out/pytest.log", "w")
class Tee:
    def __init__(self, a, b): self.a, self.b = a, b
    def write(self, s):
        self.b.write(s); self.b.flush(); return self.a.write(s)
    def flush(self): self.a.flush(); self.b.flush()
    def isatty(self): return False
    def __getattr__(self, n): return getattr(self.a, n)
sys.stdout = Tee(sys.stdout, log)
sys.stderr = Tee(sys.stderr, log)  # a usage error of pytest is only on stderr
# the phases before the log began, in it too: a chunk the watchdog kills leaves no result, but its log reaches the Volume
print("phases so far:", json.dumps(PHASES_OUT()), flush=True)

FF = PLAN.get("fail_fast") or {}
MAXFAIL = int(FF.get("maxfail") or 0)  # pytest stops this chunk at its MAXFAIL-th failure (0: never)
TEST_TIMEOUT = float(FF.get("test_timeout_s") or 0)  # a test that runs longer fails (0: no limit)
# the chunk stops at OVERRUN times its estimate (never under OVERRUN_FLOOR_MIN): its times are then a lower bound, and the next plan
# cuts it finer
OVERRUN_S = max(float(FF.get("overrun_x") or 0) * float(S.get("expected_min") or 0), float(FF.get("overrun_floor_min") or 0)) * 60 if FF.get("overrun_x") else 0
OVERRUN_S = 0 if S.get("smoke") else OVERRUN_S  # the smoke's estimate is a guess (a collect of every file): its caps are the shard and stall caps


def child_run():  # this notebook run's own ids (a child run of the runner task), for the queue table and the launcher's table
    try:
        ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()).get("attributes") or {}
    except Exception as exc:  # the ids are a report; the shard runs without them
        return {"error": type(exc).__name__}

    def pick(*names):
        for n in names:
            v = ctx.get(n)
            v = v.get("id") if isinstance(v, dict) else v
            if v not in (None, "", "None"):
                return str(v)
        return ""

    return {"job_id": pick("jobId"), "run_id": pick("currentRunId", "runId", "idInJob"), "org": pick("orgId"),
            "host": pick("browserHostName"), "keys": sorted(ctx)[:40]}


CHILD = child_run()


def note_child():  # the queue table's row of this chunk names its run (the runner's UPDATE claimed it; this one adds the id)
    table = (PLAN["pools"].get(S["pool"]) or {}).get("table")
    if not table or not CHILD.get("run_id"):
        return
    q = ("UPDATE " + table + " SET child_run_id = '" + CHILD["run_id"] + "', child_job_id = '" + CHILD.get("job_id", "")
         + "' WHERE slug = '" + SLUG + "'")
    for i in range(4):
        try:
            spark.sql(q)
            return
        except Exception:  # a concurrent claim commit: try again; the id is a report, never a reason to fail
            time.sleep(1 + i)


state = {"passed": 0, "failed": 0, "errors": 0, "skipped": 0, "total": 0, "last": "", "last_t": time.time(), "reason": "", "watchdog": "", "done": False,
         "first_failure": {}, "child": CHILD, "cur": "", "cur_t": 0.0, "phase": "", "timed_out": [], "overrun": False}
beat_lock = threading.Lock()  # the watchdog thread and the pytest hooks both beat: one write at a time
def beat():
    with beat_lock:
        d = dict(state, ts=time.time(), elapsed_s=round(time.time() - t0), shard=S["name"])
        tmp = root + "/hb.json"
        json.dump(d, open(tmp, "w"))
        shutil.copy(tmp, HB)

def keep_log():  # what the shard has printed so far reaches the Volume, so a killed or cancelled shard leaves its log
    try:
        log.flush()
        shutil.copy(root + "/out/pytest.log", OUT + "/pytest.log")
    except OSError:
        pass

def first_failure(nodeid, report):  # the chunk's first failure, for its one-line result: the launcher prints it within seconds
    if state["first_failure"]:
        return
    crash = getattr(getattr(report, "longrepr", None), "reprcrash", None)
    msg = getattr(crash, "message", "") or ""
    if not msg:
        lines = [x for x in str(getattr(report, "longrepr", "") or "").strip().splitlines() if x.strip()]
        msg = lines[-1] if lines else ""
    state["first_failure"] = {"id": nodeid or "(collection)", "msg": " ".join(msg.split())[:300]}

class Counter:
    def pytest_collection_finish(self, session):
        state["total"] = len(session.items); state["last_t"] = time.time(); beat()
        phase("collected")
    def pytest_runtest_logstart(self, nodeid, location):
        state["last_t"] = time.time()  # a test that starts counts as progress; only a test that outlasts stall_min is a stall
        if "first_test_start" not in PHASES:
            phase("first_test_start")

    def _phase(self, item, phase):  # the per-test timeout counts each phase: the call gets TEST_TIMEOUT, setup and teardown 3x it
        state["cur"], state["cur_t"], state["phase"] = item.nodeid, time.time(), phase

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        self._phase(item, "setup")
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item):
        self._phase(item, "call")
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item):
        self._phase(item, "teardown")
        yield

    def pytest_runtest_logfinish(self, nodeid, location):
        state["cur"] = ""
        if "first_test_end" not in PHASES:  # the first test's setup holds the session fixtures (the Spark session, register)
            phase("first_test_end")

    def pytest_collectreport(self, report):  # an import error of a test file: the shape of an environment-wide failure
        if report.failed:
            state["errors"] += 1
            first_failure(report.nodeid, report)
            beat()

    def pytest_internalerror(self, excrepr, excinfo):  # a conftest hook that raised: pytest stops (rc 3) with no test report
        state["errors"] += 1
        if not state["first_failure"]:
            state["first_failure"] = {"id": "(pytest internal error)", "msg": " ".join(("%%s: %%s" %% (excinfo.typename, excinfo.value)).split())[:300]}
        beat()

    def pytest_runtest_logreport(self, report):
        if report.failed:
            first_failure(report.nodeid, report)
        if report.when == "call":
            state["passed" if report.passed else "failed" if report.failed else "skipped"] += 1
        elif report.when == "setup" and report.skipped:
            state["skipped"] += 1
        elif report.failed:
            state["errors"] += 1
        else:
            return
        state["last"] = report.nodeid; state["last_t"] = time.time()
        if report.failed or (state["passed"] + state["failed"] + state["skipped"]) %% 50 == 0:
            beat()

class TestTimeout(BaseException):
    """Raised inside a test that ran past --test-timeout: the test fails, and the chunk goes on. A BaseException, as pytest's own
    outcomes are: a harness that catches Exception per case (the family oracles re-run a failed batch case by case) must not
    swallow it and run on (tst-sls-b6, -b8, -b9, -b10: each such test ran 60 s on, and the watchdog then killed its chunk)."""

    def __init__(self, *args):
        BaseException.__init__(self, *(args or (state.get("timeout_msg") or "the test ran past --test-timeout",)))


MAIN_THREAD = threading.get_ident()


def time_out_test(nodeid):  # an exception in the main thread, at its next Python bytecode: a test hung in C code gets it on return
    import ctypes

    state["timed_out"].append(nodeid)
    state["timeout_msg"] = "%%s ran past %%g s (--test-timeout)" %% (nodeid, TEST_TIMEOUT)
    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(MAIN_THREAD), ctypes.py_object(TestTimeout))
    # a Spark Connect session's blocked collect returns once its operation ends: interrupt it (and the work it left running)
    print("TEST TIMEOUT: Spark session:", interrupt_session(spark), flush=True)


LOADS = []  # (s since the start, 1-minute load) every 30 s: the driver's load through the chunk, beside its start and end probes


def watchdog():
    n = 0
    while not state["done"]:
        time.sleep(5)
        n += 1
        if n %% 6 == 0:
            try:
                LOADS.append((round(time.time() - t0), round(os.getloadavg()[0], 2)))
            except OSError:
                pass
        if n %% 3 == 0:
            beat()
        if n %% 12 == 0:
            keep_log()
        why = ""
        took = time.time() - state["cur_t"] if state["cur"] else 0
        limit = TEST_TIMEOUT * (1 if state.get("phase") == "call" else 3)
        if TEST_TIMEOUT and state["cur"] and took > limit and state["cur"] not in state["timed_out"]:
            print("\\nTEST TIMEOUT: %%s ran past %%g s; failing it" %% (state["cur"], TEST_TIMEOUT), flush=True)
            time_out_test(state["cur"])
        if time.time() - t0 > CAPS["shard_min"] * 60:
            why = "shard cap %%g min" %% CAPS["shard_min"]
        elif OVERRUN_S and time.time() - t0 > OVERRUN_S:
            state["overrun"] = True
            why = "ran past %%g min, %%gx its estimate of %%g min (a lower bound; the next plan cuts it finer)" %% (
                OVERRUN_S / 60, FF.get("overrun_x"), S.get("expected_min"))
        elif TEST_TIMEOUT and state["cur"] and took > limit + 60:
            why = "test %%s hung past %%g s and did not stop when timed out" %% (state["cur"], TEST_TIMEOUT)
        elif time.time() - state["last_t"] > STALL_MIN * 60:
            why = "no test finished for %%g min (last: %%s)" %% (STALL_MIN, state["last"])
        if why:
            # the label says who ended the shard: Databricks reports a process the watchdog kills as "ran out of memory"
            state["watchdog"] = why
            state["reason"] = "killed by watchdog: " + why
            print("\\nWATCHDOG: " + why + "; stacks follow", flush=True)
            faulthandler.dump_traceback(file=log, all_threads=True); log.flush()
            beat(); keep_log()
            _thread.interrupt_main()
            time.sleep(90)
            if not state["done"]:
                os.kill(os.getpid(), signal.SIGKILL)
threading.Thread(target=watchdog, daemon=True).start()
note_child()
phase("note_child")
print("chunk run:", CHILD, flush=True)

# Isolation probe: a shard before this one in the same runner leaves a temp view and a conf in its own session. Seeing
# neither here shows this shard has a session (and a Python process) of its own, as a job task of its own had.
iso = {"pid": os.getpid()}
try:
    iso["view_fresh"] = not spark.catalog.tableExists("gbx_isolation_probe")
    iso["conf_fresh"] = spark.conf.get("gbx.isolation.probe", "") == ""
except Exception as exc:  # the probe never decides a shard
    iso["error"] = str(exc)[:200]
CONF_ERR = ""  # a --spark-conf this session refuses (Serverless refuses many): the chunk fails, saying which and why, before its tests
for _k, _v in S.get("spark_conf", {}).items():  # --spark-conf: set before the tests, in this shard's own session
    try:
        spark.conf.set(_k, _v)
    except Exception as exc:
        CONF_ERR = CONF_ERR or "--spark-conf %%s=%%s refused by this session: %%s: %%s" %% (_k, _v, type(exc).__name__, " ".join(str(exc).split())[:240])
print("shard", S["name"], "| args", S["args"], "| env", S["env"], "| spark conf", S.get("spark_conf", {}), "| cores", os.cpu_count(), "| session", type(spark).__name__, "| isolation", iso, flush=True)


def worker_probe(spark, n=4):  # the hosts the Python workers of a small query run on (a probe: never decides the chunk)
    from pyspark.sql.functions import udf
    probe = udf(lambda _i: json.dumps(compute_probe()), "string")
    try:
        return [json.loads(r[0]) for r in spark.range(0, n, 1, n).select(probe("id")).collect()]
    except Exception as exc:
        return [{"error": type(exc).__name__ + ": " + " ".join(str(exc).split())[:160]}]


COMPUTE = {"start": compute_probe(), "workers": worker_probe(spark)}
print("compute:", json.dumps(COMPUTE), flush=True)
phase("probe")
# a test's tmp_path holds the files its virtual tiles name; on Serverless the Python workers are other machines that
# cannot see the driver's disk, so there tmp_path is under the run's Volume folder, which every worker reads
basetemp = root + "/tmp" if scratch == "/local_disk0/gbxtest" else STAGE + "/tmp/" + PLAN.get("attempt", "0") + "/" + SLUG
os.makedirs(os.path.dirname(basetemp), exist_ok=True)  # pytest makes basetemp itself, not its parent
args = S["args"] + ["-p", "no:cacheprovider", "-v", "--tb=short", "-rfE", "--durations=15", "--basetemp=" + basetemp,
                    "--junitxml=" + root + "/out/junit.xml"] + (["--maxfail=" + str(MAXFAIL)] if MAXFAIL else [])
import importlib.util, zlib
if TEST_TIMEOUT and importlib.util.find_spec("pytest_timeout") and threading.current_thread() is threading.main_thread():
    # pytest-timeout where the env has it (its signal method needs the main thread), on the call phase; the watchdog stays the backstop
    args += ["--timeout=%%d" %% TEST_TIMEOUT, "--timeout-method=signal", "-o", "timeout_func_only=true"]


class TestPart:  # --target-min cut this chunk's one file or table into parts: keep the tests whose node id hashes to this one
    def __init__(self, spec):
        self.i, self.n = (int(x) for x in spec.split("/"))

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, config, items):
        keep = [it for it in items if zlib.crc32(it.nodeid.encode()) %% self.n == self.i]
        drop = [it for it in items if zlib.crc32(it.nodeid.encode()) %% self.n != self.i]
        if drop:
            config.hook.pytest_deselected(items=drop)
            items[:] = keep


plugins = [Counter()] + ([TestPart(S["test_part"])] if S.get("test_part") else [])


def fresh_tests():  # each pytest session of the smoke imports the test modules again, under its own env (the family file reads it)
    for m in [m for m in sys.modules if m == "test" or m.startswith("test.")]:
        del sys.modules[m]


class Ids:  # the node ids a collect-only session keeps after its -k, markers and test part
    def __init__(self):
        self.ids = []

    def pytest_collection_finish(self, session):
        self.ids = [it.nodeid for it in session.items]


def smoke_pick():  # the first tests of the run's fastest chunk that collects any, with that chunk's own env and options
    base_env = dict(os.environ)
    for cand in S["candidates"]:
        C = PLAN["shards"][cand]
        os.environ.clear(); os.environ.update(base_env); os.environ.update(C["env"])
        os.chdir(repo + "/" + C["cwd"])
        got = Ids()
        fresh_tests()
        pytest.main(C["args"] + ["--collect-only", "-q", "-p", "no:cacheprovider"], plugins=[got] + ([TestPart(C["test_part"])] if C.get("test_part") else []))
        print("smoke: chunk %%s collects %%d test(s)" %% (C["name"], len(got.ids)), flush=True)
        if got.ids:
            marker = [x for i, x in enumerate(C["args"]) if C["args"][i - 1:i] == ["-m"]]
            return C, got.ids[: int(S.get("smoke_tests") or 2)] + (["-m", marker[0]] if marker else [])
    os.environ.clear(); os.environ.update(base_env)
    return None, []


rc = 0
phase("pytest_start")
try:
    if CONF_ERR:
        rc = 2
        state["reason"] = CONF_ERR
        state["first_failure"] = {"id": "(--spark-conf)", "msg": CONF_ERR}
        print(CONF_ERR, flush=True)
    elif S.get("collect"):  # the smoke chunk: collect every selected file first (an import error anywhere shows here, in seconds)
        rc = int(pytest.main(["--collect-only", "-q", "-p", "no:cacheprovider"] + S["collect"], plugins=[Counter()]))
        print("smoke: collect-only of %%d path(s) rc %%d, %%d collection error(s)" %% (len(S["collect"]), rc, state["errors"]), flush=True)
        if rc in (0, 5) and not state["errors"]:
            chosen, ids = smoke_pick()
            if not chosen:
                rc = 5
                state["reason"] = ("selection error: none of the %%d fastest chunks (%%s) collects a test with its env, -k and markers; "
                                   "check --shards, --env and the oracle tables" %% (len(S["candidates"]), ", ".join(S["candidates"])))
                state["first_failure"] = {"id": "(selection)", "msg": state["reason"]}
            else:
                print("smoke: runs", ids, "from", chosen["name"], flush=True)
                fresh_tests()
                rc = int(pytest.main(ids + args[len(S["args"]):], plugins=[Counter()]))
    elif rc in (0, 5) and not state["errors"]:
        rc = int(pytest.main(args, plugins=plugins))
except BaseException as exc:  # an interrupt from the watchdog, or a crash of the harness itself
    state["reason"] = state["reason"] or "%%s: %%s" %% (type(exc).__name__, exc)
    rc = 130
state["done"] = True
phase("pytest_end")
sys.stdout, sys.stderr = sys.stdout.a, sys.stderr.a
log.close()
try:  # what the next shard's probe looks for
    spark.range(1).createOrReplaceTempView("gbx_isolation_probe")
    spark.conf.set("gbx.isolation.probe", SLUG)
except Exception as exc:
    iso["error"] = str(exc)[:200]
ran_n = state["passed"] + state["failed"] + state["skipped"]
# pytest stopped at --maxfail before the end of the chunk: its times are partial (lower bounds), not a full sample
maxfail_hit = bool(MAXFAIL) and state["failed"] + state["errors"] >= MAXFAIL and ran_n < state["total"]
LOST = None  # 2026-11-12 (tst-sls-b2, Serverless): the chunk's scratch went during its run; say so, and what is still there
if not os.path.isdir(root + "/out"):
    LOST = {"root": os.path.isdir(root), "base": os.path.isdir(base), "scratch": os.path.isdir(scratch), "uid": os.getuid(),
            "own_base": OWN_BASE}
    os.makedirs(root + "/out", exist_ok=True)
    _lost = "the chunk's scratch %%s went during its run, its junit and log with it (%%s)" %% (root, LOST)
    state["reason"] = state["reason"] + "; " + _lost if state["reason"] else _lost  # also after a test it broke (cwd gone)
    print(_lost, flush=True)
result = dict(shard=S["name"], slug=SLUG, rc=rc, wall_s=round(time.time() - t0), reason=state["reason"], pid=os.getpid(), isolation=iso,
              passed=state["passed"], failed=state["failed"], errors=state["errors"], skipped=state["skipped"], total=state["total"],
              first_failure=state["first_failure"], maxfail_hit=maxfail_hit, overrun=state["overrun"], child=CHILD,
              timed_out=state["timed_out"], test_part=S.get("test_part", ""), checkout=CHECKOUT, checkout_s=CHECKOUT_S, runner=RUNNER,
              empty_ok=S.get("empty_ok", True), phases=PHASES_OUT(), phase_origin="call" if "call" in PHASES else "nb_start", lost=LOST,
              compute=COMPUTE)
COMPUTE["end"] = compute_probe()
COMPUTE["load_samples"] = LOADS[-60:]
json.dump(result, open(root + "/out/result.json", "w"))
for f in os.listdir(root + "/out"):
    shutil.copy(root + "/out/" + f, OUT + "/" + f)
beat()
print("shard rc:", rc, "after", result["wall_s"], "s", flush=True)
# its files are on the Volume now: the chunk's scratch goes (a temp folder of its own whole; else its folder, the runner's checkout stays)
shutil.rmtree(base if OWN_BASE else root, ignore_errors=True)
if chunk_failed(result):
    # this run's status tells the truth: it fails (the runner reads the result file it just wrote). A chunk stopped at --overrun-x with
    # no failed test is an overrun (a timing miss, OVERRUN in the summary), not a failed run
    ff = state["first_failure"] or {}
    raise RuntimeError("chunk %%s: rc %%s, %%d failed, %%d errors%%s; first: %%s %%s" %% (
        S["name"], rc, state["failed"], state["errors"], (", " + state["reason"]) if state["reason"] else "", ff.get("id", ""), ff.get("msg", "")))
dbutils.notebook.exit(json.dumps(result))
''' % {"stage": plan["stage"]}


RUNNER_DRIVER = '''
# ---- the runner task: seed the queue (role "seed"), run the one smoke chunk (role "smoke": its failure stops the job before the
# queue), or claim and run shards until the queue is empty or the run's fail-fast policy fires (role "run")
import json, os, shutil, tempfile, threading, time

STAGE = __STAGE__
ROLE = dbutils.widgets.get("role")
POOL = dbutils.widgets.get("pool")
PLAN = json.load(open(STAGE + "/plan.json"))
SPEC = PLAN["pools"][POOL]
TABLE = SPEC["table"]


def sql(q):
    return [tuple(r) for r in spark.sql(q).collect()]


if ROLE == "seed":
    sql(DeltaQueue.create_sql(TABLE, SPEC["queue"], {s: PLAN["shards"][s]["expected_min"] for s in SPEC["queue"]}))
    n = sql("SELECT count(*) FROM " + TABLE)[0][0]
    assert n == len(SPEC["queue"]), "the queue holds " + str(n) + " shard(s), the plan " + str(len(SPEC["queue"]))
    print("queue", TABLE, "holds", n, "shard(s), longest first:", SPEC["queue"], flush=True)
    dbutils.notebook.exit(json.dumps({"queued": n}))
else:
    WHO = dbutils.widgets.get("runner")
    INDEX = int(dbutils.widgets.get("index"))
    SHARD_NB = dbutils.widgets.get("shard_nb")
    TIMEOUT_S = int((PLAN["caps"]["shard_min"] + 10) * 60)  # the shard's own watchdog stops it first; this is the backstop
    T0 = time.time()
    os.makedirs(STAGE + "/runners/" + POOL, exist_ok=True)
    REC = STAGE + "/runners/" + POOL + "/" + WHO + ".json"
    tmp_dir = tempfile.mkdtemp(prefix="gbx-runner-")  # local disk: the record is written here, then copied to the Volume whole
    lock = threading.Lock()
    box = {"rec": None}

    def save(rec=None):  # the runner's record is its heartbeat too: the launcher reads which shard it is on
        with lock:
            if rec is not None:
                box["rec"] = json.loads(json.dumps(rec))
            if box["rec"] is None:
                return
            tmp = tmp_dir + "/" + WHO + ".json"
            with open(tmp, "w") as f:
                json.dump(dict(box["rec"], ts=time.time(), pool=POOL), f)
            shutil.copy(tmp, REC)

    def beat():
        while not (box["rec"] or {}).get("done"):
            time.sleep(15)
            try:
                save()
            except OSError:
                pass

    threading.Thread(target=beat, daemon=True).start()

    def execute(slug):
        print("+%ds" % (time.time() - T0), WHO, "runs", PLAN["shards"][slug]["name"], flush=True)
        t_call = time.time()  # the chunk measures its setup phases from here (its notebook run's start included)
        res = run_shard(lambda: dbutils.notebook.run(SHARD_NB, TIMEOUT_S, {"shard": slug, "runner": WHO, "t_call": "%.3f" % t_call}), TIMEOUT_S,
                        watchdog=lambda: heartbeat_watchdog(STAGE + "/hb/" + slug + ".json"),
                        result=lambda: read_json_file(STAGE + "/out/" + slug + "/result.json"))
        if not res.get("child"):  # a chunk that failed before pytest: its first cell named its run, so the launcher can read why
            res["child"] = read_json_file(STAGE + "/out/" + slug + "/child.json") or {}
        if isinstance(res.get("phases"), dict):  # the notebook run's exit reached the runner: the last phase
            res["phases"]["returned"] = round(time.time() - t_call, 1)
        print("+%ds" % (time.time() - T0), WHO, "ran", PLAN["shards"][slug]["name"], "rc", res.get("rc"), res.get("reason") or "", flush=True)
        return res

    HEAVY = SPEC.get("heavy") or []
    q = DeltaQueue(sql, TABLE, heavy=HEAVY)
    # the runners of one cluster share the node's disk: one lock there lets one heavy chunk run at a time. The path is the same for
    # every runner of the node (never a folder of one runner's own, which would gate nothing); a gate that cannot make it runs ungated
    try:  # the node's local disk; Serverless has none to write
        os.makedirs("/local_disk0/gbxtest", exist_ok=True)
        gate_root = "/local_disk0/gbxtest"
    except OSError:
        gate_root = tempfile.gettempdir() + "/gbxtest"
    gate = HeavyGate(gate_root + "/" + PLAN["run_id"] + "-" + POOL + "/heavy.lock") if HEAVY else None
    SMOKE = ROLE == "smoke"
    RESULTS = STAGE + "/results"
    os.makedirs(RESULTS, exist_ok=True)
    POLICY = PLAN.get("fail_fast") or {}
    last = {}

    def write_result(slug, res, start, end):  # the chunk's one-line result, the moment it ends: the launcher polls these
        line = result_line(slug, dict(res, smoke=SMOKE), start, end, WHO)
        last.update(line)
        tmp = tmp_dir + "/" + slug + ".result.json"
        with open(tmp, "w") as f:
            json.dump(line, f)
        shutil.copy(tmp, RESULTS + "/" + slug + ".json")

    def should_stop():  # before each claim: the launcher's stop marker, else the run's policy over the results so far
        if os.path.exists(STAGE + "/stop.json"):
            try:
                with open(STAGE + "/stop.json") as f:
                    return "stopped early: " + json.load(f).get("rule", "the launcher stopped the run")
            except (OSError, ValueError):
                return "stopped early: the launcher stopped the run"
        deadline = SPEC.get("deadline") or POLICY.get("deadline")  # set when this pool's job was submitted, after its leases
        if deadline and time.time() > deadline:
            return "stopped early: the wall budget of " + str(POLICY.get("max_wall_min")) + " min is spent (--max-wall-min)"
        why = stop_rule(read_results(RESULTS), POLICY)
        return "stopped early: " + why if why else ""

    START = int(POLICY.get("start_chunks") or 0)  # the ramp: runners from this index on wait until START chunks have passed

    def held():
        queued = sql("SELECT count(*) FROM " + TABLE + " WHERE owner IS NULL")[0][0]
        return not ramp_open(read_results(RESULTS), START, int(queued))

    rec = run_loop(q, WHO, execute, offset=INDEX, on_change=save, heavy=HEAVY, gate=gate, stop=should_stop, on_result=write_result,
                   limit=1 if SMOKE else 0, hold=held if (START and not SMOKE and INDEX >= START) else None,
                   stagger_s=ramp_stagger_s(INDEX, START, serverless=bool(PLAN.get("serverless"))) if not SMOKE else 0.0)
    rec["conflicts"] = q.conflicts
    rec["compute"] = compute_probe()  # the runner task's own host: its chunks' hosts show whether they run beside it
    save(rec)
    print(WHO, "ran", len(rec["shards"]), "shard(s);", q.conflicts, "lost claim commit(s);", rec["stopped"] or "the queue is empty", flush=True)
    # the checkout its chunks shared (the shard cell's node_checkout, on the node's disk) and its own record folder go with it
    shutil.rmtree(gate_root + "/" + PLAN["run_id"] + "-" + PLAN.get("attempt", "0") + "/_checkout-" + WHO, ignore_errors=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    if rec["error"]:
        raise RuntimeError(rec["error"])
    if SMOKE and last.get("failed_chunk"):  # the runners depend on this task: a failed smoke stops the job before the queue
        ff = last.get("first_failure") or {}
        raise RuntimeError("stopped early: the smoke chunk " + last["slug"] + " failed (" + str(last.get("failed")) + " failed, "
                           + str(last.get("errors")) + " errors); first: " + (ff.get("id") or "") + " " + (ff.get("msg") or ""))
    # the task's status tells the truth: it fails when a chunk it ran failed or did not finish (each chunk's own run failed too)
    bad = [x for x in rec["shards"] if chunk_failed(x)]  # an overrun with no failed test is a timing miss, not a failed runner
    if bad:
        raise RuntimeError(str(len(bad)) + " of " + str(len(rec["shards"])) + " chunk(s) this runner ran failed or did not finish: "
                           + ", ".join(PLAN["shards"][x["slug"]]["name"] + " (" + (x.get("reason") or "rc " + str(x.get("rc"))) + ")" for x in bad[:5]))
    dbutils.notebook.exit(json.dumps({"runner": WHO, "shards": len(rec["shards"]), "stopped": rec["stopped"]}))
'''


def runner_source(plan: dict) -> str:
    """The runner task's cell: the claim-and-run loop (_shard_runner.py, inlined: the node has no copy of examples/) and the
    code that seeds the queue or drives the loop. Needs no wheel: it runs the shards' own notebook, which installs it."""
    return Path(sr.__file__).read_text() + RUNNER_DRIVER.replace("__STAGE__", repr(plan["stage"]))


def build_runner_notebook(plan: dict, pool: str) -> dict:
    cells = [cell(header_md(plan, pool), "markdown"), cell(runner_source(plan))]
    return {"cells": cells, "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}


def cell(source: str, kind: str = "code") -> dict:
    c = {"cell_type": kind, "metadata": {}, "source": source.splitlines(keepends=True)}
    if kind == "code":
        c.update(outputs=[], execution_count=None)
    return c


START_SOURCE = '''# the chunk names its own run first: if a later cell fails before pytest, the launcher reads that run's error
import json, os
import time as _time

# the setup phases (epoch seconds), from the runner's call (widget t_call) on: the run cell adds its own, the result carries them
_GBX_PHASES = {"nb_start": _time.time()}
try:
    _GBX_PHASES["call"] = float(dbutils.widgets.get("t_call"))
except Exception:  # a chunk run by hand, or by a runner that predates the widget
    pass
_ctx = {}
try:
    _ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()).get("attributes") or {}
except Exception as _exc:
    _ctx = {"error": type(_exc).__name__}
_id = lambda v: v.get("id") if isinstance(v, dict) else v
_child = {"job_id": str(_id(_ctx.get("jobId")) or ""), "run_id": str(_id(_ctx.get("currentRunId")) or _id(_ctx.get("runId")) or ""),
          "org": str(_ctx.get("orgId") or ""), "error": _ctx.get("error", "")}
_out = __STAGE__ + "/out/" + dbutils.widgets.get("shard")
os.makedirs(_out, exist_ok=True)
with open(_out + "/child.json", "w") as _f:
    json.dump(_child, _f)
print("chunk run:", _child)
print("phase=nb_start t=%.1f" % (_GBX_PHASES["nb_start"] - _GBX_PHASES.get("call", _GBX_PHASES["nb_start"])), flush=True)
_GBX_PHASES["start_cell_end"] = _time.time()
'''


def start_source(plan: dict) -> str:
    return START_SOURCE.replace("__STAGE__", repr(plan["stage"]))


def build_notebook(plan: dict, pool: str) -> dict:
    cells = [cell(header_md(plan, pool), "markdown"), cell(start_source(plan))]
    if not plan["serverless"]:  # a Serverless job installs from the environment spec; %pip is not allowed there
        cells.append(cell(install_source(plan)))
    if plan["wheel"]["volume"]:
        cells.append(cell(guard_source(plan)))
    cells.append(cell(run_source(plan)))
    return {"cells": cells, "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}


# ---------------------------------------------------------------------------------------------- results


_SELECTED = re.compile(r"collected (\d+) items?(?: / (\d+) deselected)?(?: / (\d+) selected)?")
_ENDED = re.compile(r"\b(PASSED|FAILED|SKIPPED|ERROR|XFAIL|XPASS)\b[^\n\[]*\[\s*\d+%\]")


def progress_of(log: Path) -> Optional[Tuple[int, int]]:
    """(tests that ended, tests selected) from a chunk's pytest.log (pytest -v), None when it has no collection line. A chunk the
    watchdog killed leaves no junit, but its log reaches the Volume: how far it got grows its lower bounds."""
    try:
        text = re.sub(r"\x1b\[[0-9;]*m", "", Path(log).read_text(errors="replace"))
    except OSError:
        return None
    m = _SELECTED.search(text)
    if not m:
        return None
    selected = int(m.group(3)) if m.group(3) else int(m.group(1)) - int(m.group(2) or 0)
    return len(_ENDED.findall(text)), selected


def parse_junit(path: Path) -> dict:
    """Counts, the failures (name, first lines of the message) and the skip reasons of one junit.xml."""
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    out = {"tests": 0, "failed": 0, "errors": 0, "skipped": 0, "time": 0.0, "failures": [], "skip_reasons": Counter()}
    for s in suites:
        out["tests"] += int(s.get("tests", 0))
        out["failed"] += int(s.get("failures", 0))
        out["errors"] += int(s.get("errors", 0))
        out["skipped"] += int(s.get("skipped", 0))
        out["time"] += float(s.get("time", 0) or 0)
        for case in s.iter("testcase"):
            name = f"{case.get('classname', '')}::{case.get('name', '')}"
            for child in case:
                if child.tag in ("failure", "error"):
                    msg = (child.get("message") or child.text or "").strip().splitlines()
                    out["failures"].append((name, " | ".join(x.strip() for x in msg[:3])[:300]))
                elif child.tag == "skipped":
                    out["skip_reasons"][(child.get("message") or "").strip()[:140]] += 1
    out["passed"] = out["tests"] - out["failed"] - out["errors"] - out["skipped"]
    return out


def shard_verdict(res: Optional[dict], junit: Optional[dict]) -> str:
    """ok | FAILED | ERRORS (pytest stopped on a collection or usage error) | OVERRUN (stopped at --overrun-x times its estimate
    with no failed test: a timing miss) | INCOMPLETE (no result, another watchdog stop or a cancel: the shard did not finish) | EMPTY
    (nothing collected)."""
    if not res:
        return "INCOMPLETE"
    if res.get("overrun") and not (res.get("failed") or res.get("errors") or (junit and (junit["failed"] or junit["errors"]))):
        return "OVERRUN"
    if res.get("reason") or res.get("rc") == 130:
        return "INCOMPLETE"
    if res.get("rc") == 5 or (res.get("rc") == 0 and not res.get("total")):
        return "EMPTY"
    failed = res.get("failed") or (junit and junit["failed"])
    if res.get("rc") != 0:
        return "FAILED" if failed else "ERRORS"
    if res.get("errors") or res.get("failed"):
        return "FAILED"
    return "ok"


def is_failure(row: dict) -> bool:
    """Whether a shard's row fails the run. EMPTY (nothing collected) fails, but for an intended empty selection: a part of a split
    shard that holds only tests the marker deselects (integration files; CI's whole shard would pass), a test part whose unit's other
    parts collected tests (the node ids hash unevenly), an ``affected`` subset. A unit no part of which collected a test, a rerun
    that collects nothing and a whole shard that collects nothing fail. OVERRUN fails the run (its later tests did not run), though
    not as a failed test: its own run and its runner went on."""
    if row["verdict"] == "ok":
        return False
    if row["verdict"] == "EMPTY":
        return bool(row.get("empty_unit")) or not (row["name"].endswith("]") or row.get("subset"))
    return True


def mark_empty_units(plan: dict, rows: List[dict]) -> None:
    """Flag the rows of a test-part unit (one file or table cut by node id) when no part of it collected a test: a cut that
    lost its unit's tests, which an EMPTY part alone would hide."""
    units: Dict[tuple, List[dict]] = {}
    for r in rows:
        c = plan["shards"].get(r["slug"]) or {}
        if c.get("test_part"):
            units.setdefault((tuple(c.get("files") or []), tuple(c.get("tables") or []), c["test_part"].split("/")[1]), []).append(r)
    for (files, tables, n), parts in units.items():
        if all(r["verdict"] == "EMPTY" for r in parts):
            for r in parts:
                r["empty_unit"] = True
                r["reason"] = r.get("reason") or f"EMPTY: no part of {', '.join(files or tables)} (cut in {n}) collected a test"


def pool_metrics(records: List[dict]) -> dict:
    """What the schedule did, from the runners' records (``_shard_runner.slot_metrics``): the chunks and slots, the longest shard
    against the mean, the span, the ideal wall (the sum of the shards over the slots), the tail (the last slot ended after the
    first), the slot time left unused, and per slot its busy time, the time it waited while the queue still held a shard, and
    its idle time at the end."""
    return sr.slot_metrics(records)


def schedule_lines(info: dict) -> List[str]:
    if "chunks" not in info:
        return []
    out = [
        f"  schedule: {info['chunks']} chunks on {info['slots']} slot(s), taken from one queue; longest chunk {info['task_longest_s']} s, "
        f"mean {info['task_mean_s']} s (longest/mean {info['task_longest_s'] / max(1, info['task_mean_s']):.1f}x)",
        f"  first start to last end {info['span_s']} s; ideal (sum of chunks over the slots) {info['ideal_s']} s; tail (last slot ended after the first) "
        f"{info['tail_s']} s; idle slot time {info['idle_slot_s']} s, of it {info['waited_s']} s while the queue still held a chunk",
    ]
    for p in info.get("per_slot", []):
        out.append(f"    slot {p['runner']}: {p['shards']} chunk(s), busy {p['busy_s']} s, waited {p['waited_s']} s while chunks were queued, "
                   f"idle {p['idle_end_s']} s at the end (started +{p['start_offset_s']} s)" + (f"; {p['error']}" if p.get("error") else ""))
    return out


def compute_line(rows: List[dict]) -> str:
    """How many hosts the chunks' drivers and Python workers ran on, their cores and cgroup limits, and the drivers' load at start
    and end (each chunk's ``compute`` record): runner tasks that share a node show as fewer hosts than chunks, and as load."""
    comp = [r.get("compute") for r in rows if r.get("compute") and (r["compute"].get("start") or {}).get("host")]
    if not comp:
        return ""
    import statistics
    hosts = {c["start"]["host"] for c in comp}
    workers = {w.get("host") for c in comp for w in c.get("workers") or [] if w.get("host")}
    limits = sorted({(c["start"].get("cpus"), c["start"].get("cpu_max"), c["start"].get("mem_max")) for c in comp}, key=str)
    l0 = [float(c["start"].get("load1", -1)) for c in comp if float(c["start"].get("load1", -1)) >= 0]
    l1 = [float((c.get("end") or {}).get("load1", -1)) for c in comp if float((c.get("end") or {}).get("load1", -1)) >= 0]
    med = lambda xs: f"{statistics.median(xs):.1f}" if xs else "-"  # noqa: E731
    return (f"compute: {len(hosts)} driver host(s) for {len(comp)} chunk(s), {len(workers)} worker host(s); cores, cpu.max, memory.max "
            f"{'; '.join(f'{a}, {b}, {m}' for a, b, m in limits[:3])}; driver load1 at start median {med(l0)}, at end median {med(l1)}")


def isolation_line(rows: List[dict]) -> str:
    """Whether every shard had a Python process and a Spark session of its own (the probe each shard notebook runs)."""
    iso = [r.get("isolation") for r in rows if r.get("isolation")]
    if not iso:
        return ""
    pids = [i.get("pid") for i in iso]
    fresh = sum(1 for i in iso if i.get("view_fresh") and i.get("conf_fresh"))
    return (f"isolation: {len(set(pids))} distinct Python process(es) for {len(iso)} shard(s); {fresh} of {len(iso)} saw no temp view or conf "
            f"of an earlier shard")


def speedup_line(info: dict) -> str:
    """Wall time of the pool's job against the longest task and against running the tasks one after another."""
    wall, longest, serial = info.get("wall_s"), info.get("longest_s"), info.get("serial_s")
    if not all(isinstance(x, int) for x in (wall, longest, serial)) or not wall:
        return f"wall {wall} s"
    return f"wall {wall} s = {wall / longest:.1f}x the longest task ({longest} s); the tasks one after another would take {serial} s: speedup {serial / wall:.1f}x"


def summary_md(plan: dict, rows: List[dict], pool_info: dict) -> str:
    out = [f"# Test shards on Databricks: {plan['run_id']}", "",
           f"- commit `{plan['sha'][:10]}`; wheel `{plan['wheel']['name']}` (built from `{plan['wheel']['built_from'][:10]}`)",
           f"- purpose: {plan['purpose']}"]
    if plan.get("stopped_early"):
        out.append(f"- **stopped early: {plan['stopped_early']}**; the chunks it stopped or never ran are INCOMPLETE, and their times are lower bounds")
    if plan.get("fail_fast"):
        out.append(f"- fail fast: {fail_fast_text(plan['fail_fast'])}")
    if plan.get("stopped_early") and not_run_line(rows):
        out.append(f"- {not_run_line(rows)}")
    for pool, info in pool_info.items():
        out.append(f"- {pool}: cluster(s) {info.get('cluster_id', '-')} ({info.get('node_type', '-')}), job run {info.get('job_run', '-')}; {speedup_line(info)}")
        out += ["  - " + x.strip() for x in schedule_lines(info)]
        if info.get("peaks"):
            out.append("  - peak concurrency: " + ", ".join(f"cluster {c}: {n} chunk(s) at once" for c, n in sorted(info["peaks"].items())))
    if isolation_line(rows):
        out.append(f"- {isolation_line(rows)}")
    if compute_line(rows):
        out.append(f"- {compute_line(rows)}")
    if plan["jar"]:
        out.append(f"- JAR `{plan['jar']['name']}` sha1 {plan['jar'].get('sha1', '')[:12]} sha256 {plan['jar']['sha256'][:16]}")
    out += ["", "| shard | pool | verdict | passed | failed | errors | skipped | wall s | predicted s | actual s | error % | estimate | contention | beside |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        out.append(f"| {r['name']} | {r['pool']} | {r['verdict']} | {r['passed']} | {r['failed']} | {r['errors']} | {r['skipped']} | {r['wall_s']} | "
                   f"{predicted_s(r)} | {r.get('actual_s') if r.get('actual_s') is not None else '-'} | {error_pct(r)} | {r.get('est_src', 'ci')} | "
                   f"{contention_cell(r)} | {r.get('nb', '-')} |")
    tot = {k: sum(r[k] for r in rows if isinstance(r[k], int)) for k in ("passed", "failed", "errors", "skipped")}
    out.append(f"| **total** | | | {tot['passed']} | {tot['failed']} | {tot['errors']} | {tot['skipped']} | | | | {prediction_line(rows)} | | | |")
    out.append("")
    out.append(f"contention: the chunk's actual time over its solo-expected time (the estimate without contention factors); beside: the chunks "
               f"that overlapped it on its cluster. A unit at {tm.CONTENTION_HEAVY:g}x or more over its last runs with neighbours is contention-heavy "
               "and joins the heavy gate.")
    for r in rows:
        if r["failures"]:
            out += ["", f"## {r['name']}: failures", ""]
            out += [f"- `{n}`: {m}" for n, m in r["failures"][:25]]
            if len(r["failures"]) > 25:
                out.append(f"- ... {len(r['failures']) - 25} more in the junit.xml")
        if r.get("reason"):
            out += ["", f"## {r['name']}: stopped", "", f"- {r['reason']}"]
    out += phase_lines(rows)
    skips = [r for r in rows if r["skip_reasons"]]
    if skips:
        out += ["", "## Skipped (top reasons per shard)", ""]
        for r in skips:
            out.append(f"- {r['name']}: " + "; ".join(f"{n} x {m or '(no reason)'}" for m, n in r["skip_reasons"].most_common(4)))
    return "\n".join(out) + "\n"


# the setup phases a chunk marks (its notebook's start cell, then its run cell), in the order they happen
PHASE_ORDER = ("nb_start", "start_cell_end", "run_cell", "scratch", "checkout", "overlay", "import_gbx", "guard_tree", "import_pytest",
               "note_child", "probe", "pytest_start", "collected", "first_test_start", "first_test_end", "pytest_end", "returned")


def phase_lines(rows: List[dict]) -> List[str]:
    """Where each chunk's time went before (and around) its tests: per phase, the seconds since the phase before it, from the runner's
    call of the chunk's notebook (``call``). ``run_cell`` after ``start_cell_end`` is the wheel guard cell; ``first_test_end`` after
    ``first_test_start`` holds the session fixtures. The last column is the time from the call to the first test."""
    have = [r for r in rows if isinstance(r.get("phases"), dict) and r["phases"]]
    if not have:
        return []
    cols = [p for p in PHASE_ORDER if any(p in r["phases"] for r in have)]
    out = ["", "## Setup phases (s since the phase before; from the runner's call of the chunk's notebook)", "",
           "| chunk | " + " | ".join(cols) + " | to first test |", "|---|" + "---|" * (len(cols) + 1)]
    for r in have:
        ph, prev, cells = r["phases"], float(r["phases"].get("call", 0.0)), []
        for c in cols:
            if c in ph:
                cells.append(f"{float(ph[c]) - prev:.0f}")
                prev = float(ph[c])
            else:
                cells.append("-")
        first = ph.get("first_test_start")
        out.append(f"| {r['name']} | " + " | ".join(cells) + f" | {float(first) - float(ph.get('call', 0.0)):.0f} |" if first is not None
                   else f"| {r['name']} | " + " | ".join(cells) + " | - |")
    return out


def contention_cell(row: dict) -> str:
    x = row.get("x")
    return "-" if x is None else f"{x:g}x" + (" (heavy)" if x >= tm.CONTENTION_HEAVY else "")


def predicted_s(row: dict) -> int:
    return int(round(float(row["expected_min"]) * 60))


def error_pct(row: dict) -> str:
    """How far the actual time of a chunk was from its prediction, as a percentage of the prediction (+ = longer)."""
    act = row.get("actual_s")
    if act is None or not row.get("expected_min"):
        return "-"
    return f"{(act - predicted_s(row)) / predicted_s(row) * 100:+.0f}"


def prediction_line(rows: List[dict]) -> str:
    errs = [abs(row["actual_s"] - predicted_s(row)) / predicted_s(row) * 100 for row in rows if row.get("actual_s") is not None and row.get("expected_min")]
    return f"median abs error {sorted(errs)[len(errs) // 2]:.0f}%" if errs else ""


def table_text(rows: List[dict]) -> str:
    head = (f"{'shard':<22} {'verdict':<10} {'passed':>7} {'failed':>6} {'errors':>6} {'skipped':>7} {'wall s':>7} "
            f"{'pred s':>7} {'act s':>7} {'err %':>6} {'est':>4} {'contn':>6} {'nb':>3}")
    lines = [head, "-" * len(head)]
    for r in rows:
        act = r.get("actual_s")
        lines.append(f"{r['name']:<22} {r['verdict']:<10} {r['passed']:>7} {r['failed']:>6} {r['errors']:>6} {r['skipped']:>7} {r['wall_s']:>7} "
                     f"{predicted_s(r):>7} {act if act is not None else '-':>7} {error_pct(r):>6} {SRC_MARK.get(r.get('est_src'), 'ci'):>4} "
                     f"{contention_cell(r).split(' ')[0]:>6} {r.get('nb', '-'):>3}")
    if prediction_line(rows):
        lines.append(f"predicted vs actual per chunk: {prediction_line(rows)} (actual: the chunk's notebook run, install included)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------- staging


def git(*cmd) -> str:
    return subprocess.run(["git", *cmd], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout


def build_tarball(sha: str, dest: Path) -> int:
    subprocess.run(["git", "archive", "--format=tar.gz", "-o", str(dest), sha, *SUBSET], cwd=REPO_ROOT, check=True)
    return dest.stat().st_size


def uncommitted() -> list:
    return [ln[3:] for ln in git("status", "--porcelain", "--", *SUBSET).splitlines() if ln.strip()]


def wheel_notes(wheel: Path, sha: str) -> List[str]:
    """Say when the wheel was built before a change to what it carries (dependencies, native payload)."""
    built = mut.wheel_git_sha(wheel)
    notes = []
    try:
        full = git("rev-parse", "--verify", f"{built}^{{commit}}").strip()
    except subprocess.CalledProcessError:
        return [f"the wheel's build commit {built[:10]} is not in this repository: its dependency set is unchecked"]
    changed = git("diff", "--name-only", full, sha, "--", *DEPS_INPUTS).split()
    if changed:
        notes.append(f"{len(changed)} dependency/native input(s) changed since the wheel was built ({built[:10]}): {', '.join(changed[:4])}"
                     f"{' ...' if len(changed) > 4 else ''}; rebuild with gbx:build:wheel --env 6 or pass --wheel. The tests run the checkout's code either way")
    return notes


WHEEL_CODE = "python/geobrix/src"  # what a Serverless worker imports from the wheel


def serverless_wheel_problem(built: str, sha: str) -> str:
    """Why the wheel cannot serve a Serverless run of ``sha`` ("" when it can). Serverless workers run the wheel, never the checkout
    laid over the driver's copy, so a wheel whose package code differs from ``sha``'s fails every chunk's wheel guard, after the
    chunk's 1-3 min start (tst-sls-d9: a wheel built from 805f46fae8 for 842c971f42, one changed module)."""
    if not built:
        return ""
    try:
        full = git("rev-parse", "--verify", f"{built}^{{commit}}").strip()
        changed = git("diff", "--name-only", full, sha, "--", WHEEL_CODE).split()
    except subprocess.CalledProcessError:
        return f"the wheel's build commit {built[:10]} is not in this repository, so its package code is unchecked against {sha[:10]}"
    if not changed:
        return ""
    return (f"the wheel was built from {built[:10]}, and {len(changed)} file(s) of {WHEEL_CODE} differ at {sha[:10]} "
            f"({', '.join(changed[:3])}{' ...' if len(changed) > 3 else ''}): Serverless workers run the wheel, so every chunk's "
            "wheel guard would fail. Fix: rebuild with gbx:build:wheel --env 6 at that commit, or pass --sha of the wheel's commit")


def resources_entry(sha: str, volroot: str) -> Optional[dict]:
    """The test resources at ``sha``, named by their git tree id; None when the commit has none."""
    try:
        tree = git("rev-parse", "--verify", f"{sha}:{RESOURCES}").strip()
    except subprocess.CalledProcessError:
        return None
    d = f"{volroot}/test-pool-data/{tree[:12]}"
    return {"tree": tree, "volume": f"{d}/resources.tar.gz", "marker": f"{d}/resources.ok"}


def stage_resources(w, res: dict, sha: str, work: Path) -> None:
    """Upload the resources tarball unless this content is already there (its marker file says it was verified)."""
    try:
        w.files.get_metadata(res["marker"])
        print(f"  test resources {res['tree'][:12]} already staged", flush=True)
        return
    except Exception:  # noqa: BLE001 - absent: stage it
        pass
    tar = work / "resources.tar.gz"
    subprocess.run(["git", "archive", "--format=tar.gz", "-o", str(tar), sha, RESOURCES], cwd=REPO_ROOT, check=True)
    mut.stage_file(w, tar, res["volume"])
    put_text(w, res["marker"], mut.sha256_file(tar))
    tar.unlink()


def stage_jar(path: Path, root: Path, copy: bool = True) -> dict:
    """Copy the JAR out of the tree it was built in (another build may replace it) and name it by its content. ``copy=False``
    (a plan that only explains) hashes it where it is and makes no copy."""
    import hashlib
    import shutil
    import tempfile

    if copy:
        work = Path(tempfile.mkdtemp(prefix="gbx-test-jar-"))
        local = work / path.name
        shutil.copy(path, local)
    else:
        local = Path(path)
    sha256 = mut.sha256_file(local)
    sha1 = hashlib.sha1(local.read_bytes()).hexdigest()
    return {"local": str(local), "name": path.name, "sha256": sha256, "sha1": sha1, "dir": f"test-pool-jars/{sha256[:12]}"}


# ---------------------------------------------------------------------------------------------- the workspace side


def _version_key(name: str) -> tuple:
    """The numbers of a file name as ints, so ``0.10.0`` sorts after ``0.9.0`` (the tie-break of equal mtimes)."""
    return tuple(int(n) for n in re.findall(r"\d+", name))


def jar_source(jar_path: str) -> Optional[Path]:
    """The JAR ``--jar-path`` names: the file itself, or in a folder (``python/geobrix/lib/`` when unset) its
    ``geobrix-*-jar-with-dependencies.jar`` (the newest by mtime, then by version, should it hold several). None when there is none."""
    p = Path(jar_path) if jar_path else REPO_ROOT / "python/geobrix/lib"
    if p.is_dir():
        jars = sorted(p.glob("geobrix-*-jar-with-dependencies.jar"), key=lambda j: (j.stat().st_mtime, _version_key(j.name)))
        return jars[-1] if jars else None
    return p


def main(argv=None) -> int:
    exit_if_help(__doc__, argv)
    a = parse(argv)
    errs = problems(a)
    if errs:
        for e in errs:
            print(f"ERROR: {e}", file=sys.stderr)
        return EXIT_USAGE
    load_config()
    a.run_id = a.run_id or default_run_id()
    sha = git("rev-parse", "--verify", f"{a.sha}^{{commit}}").strip()
    all_shards = ts.load(REPO_ROOT)
    changed = None
    aff_shards, aff_lines = None, []
    if "affected" in a.shards and not a.rerun_failed:
        try:
            changed = ts.changed_files(REPO_ROOT, a.base, sha)
        except subprocess.CalledProcessError as exc:
            print(f"ERROR: cannot diff {a.base}...{sha[:10]} ({(exc.stderr or '').strip()[:120] or 'git failed'}); fetch the base or pass --base <rev>", file=sys.stderr)
            return EXIT_USAGE
        if changed:
            aff_shards, aff_lines, broad = affected_selection(a, sha, all_shards)
            print("\n".join(aff_lines))
            if broad and not a.allow_broad:
                print(f"\nERROR: {broad}. Pass --allow-broad to run it anyway, or name the shards or test files to run.", file=sys.stderr)
                return EXIT_USAGE
            if broad:
                print(f"\nWARNING: {broad}; running it (--allow-broad).", file=sys.stderr)
    try:
        shards = [] if a.rerun_failed else ts.select(all_shards, a.shards, changed, REPO_ROOT, aff_shards)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if a.rerun_failed:  # only the tests that failed in that run, a chunk per file, each stopping at its first failure
        shards = rerun_shards(REPO_ROOT / "test-logs/databricks-tests" / a.rerun_failed, REPO_ROOT, all_shards)
        print(f"--rerun-failed {a.rerun_failed}: {sum(len(s.paths) for s in shards)} failed test(s) in {len(shards)} file(s)")
        if not shards:
            print(f"No failed test in test-logs/databricks-tests/{a.rerun_failed}/*/junit.xml: nothing to run.")
            return 0
    if a.ids_file:  # the named node ids only, a chunk per --ids-per-chunk of them
        shards = ids_shards(a.ids_file, a.ids_per_chunk, extra_env(a).get("GBX_ORACLE_TABLES", ""), REPO_ROOT, all_shards)
        print(f"--ids-file {a.ids_file}: {sum(len(s.paths) for s in shards)} test id(s) in {len(shards)} chunk(s)")
    elif extra_env(a).get("GBX_ORACLE_TABLES"):  # --env narrows the family file to those tables: plan its shards by them
        shards = ts.narrow_tables(shards, extra_env(a)["GBX_ORACLE_TABLES"])
    split_notes: List[str] = []
    if not a.clusters and not (a.explain_only or a.serverless or a.fresh_cluster):
        a.clusters = planned_clusters(a)
    # Serverless reads its own samples (``sls:<key>``) and their contention factors (the runners of a job share its driver hosts),
    # else the pool's solo times; a runner task there is timed at slots 1
    hist = tm.view(history_for(REPO_ROOT / a.timings), serverless=a.serverless)
    time_slots = 1 if a.serverless else plan_slots(a)
    fsec = tm.seconds(hist, time_slots)  # per file / per oracle table, at this run's slots, times its contention factor
    fsolo = tm.seconds(hist, time_slots, adjust=False)  # the same without the factors: what the next run's ratios compare with
    hot = set() if a.serverless else tm.contended(hist)  # contention-heavy units: they join the heavy gate
    lower = tm.lower_bounds(hist, time_slots)
    if a.split == "auto" and shards and not (a.rerun_failed or a.ids_file):
        n_slots = plan_slots(a) * (1 if a.serverless else max(1, a.clusters))
        lo, hi = SERVERLESS_TARGET if a.serverless else POOL_TARGET
        target = a.target_min or ts.auto_target_min(shards, REPO_ROOT, fsec, n_slots, lo=lo, hi=hi)
        shards = ts.auto_split(shards, REPO_ROOT, fsec, target, lower, hot)
        split_notes.append(f"{len(shards)} chunks cut to about {target:g} min for {n_slots} slot(s)"
                           + (" (Serverless: its own timings where it has them, else the pools' solo times)" if a.serverless else ""))
    elif shards and not (a.rerun_failed or a.ids_file):  # a rerun chunk keeps its estimate: its few test ids, not its whole file or tables
        shards = [ts.annotate(ts.Shard(**{**s.__dict__, "est_min": round(e, 2), "est_src": src}), REPO_ROOT, fsec, hot)
                  for s in shards for e, src in [ts.estimate_src(s, REPO_ROOT, fsec, lower)]]
    shards = [ts.Shard(**{**s.__dict__, "solo_min": ts.solo_min(s, REPO_ROOT, fsec, fsolo)}) for s in shards]
    if shards:
        split_notes.append(estimate_note(shards, hist, a.timings))
        factors = tm.contention(hist)
        if hot:
            split_notes.append(f"contention: {len(hot)} unit(s) of the history run {tm.CONTENTION_HEAVY:g}x or more over their solo time beside "
                               f"other chunks, and join the heavy gate; their estimates carry the factor ("
                               + ", ".join(f"{k} {factors[k]:g}x" for k in sorted(hot, key=lambda k: -factors[k])[:6]) + (" ..." if len(hot) > 6 else "") + ")")
    if not shards:
        print(f"No test file or oracle table is reached by the {len(changed or [])} file(s) changed between {a.base} and {sha[:10]}: nothing to run.")
        return 0
    if a.serverless and any(s.pool == ts.POOL_JAR for s in shards):
        print("ERROR: --serverless runs light shards only (the heavy shards need the JAR on a cluster)", file=sys.stderr)
        return EXIT_USAGE
    wheel, wheel_problem = None, ""
    try:
        wheel = mut.find_wheel(a.wheel)
    except SystemExit as exc:  # find_wheel exits with the reason; a plan can still be shown without a wheel
        wheel_problem = str(exc)
    if wheel is None and not a.explain_only:
        print(wheel_problem or "PRECONDITION MISSING: no env6 wheel; run not started.", file=sys.stderr)
        return EXIT_USAGE
    notes = split_notes + (wheel_notes(wheel, sha) if wheel else [f"no usable env6 wheel ({wheel_problem[:140]}); a launch needs one"])
    stale = serverless_wheel_problem(mut.wheel_git_sha(wheel), sha) if wheel and a.serverless else ""
    if stale and not a.explain_only:
        print("PRECONDITION MISSING: " + stale + "; run not started.", file=sys.stderr)
        return EXIT_USAGE
    if stale:
        notes.append(stale)
    dirty = uncommitted()
    if dirty:
        notes.append(f"{len(dirty)} uncommitted path(s) under the staged subset are NOT in this run (it tests {sha[:10]}): "
                     + ", ".join(dirty[:5]) + (" ..." if len(dirty) > 5 else ""))
    volroot = _volroot()
    jar = None
    if any(s.pool == ts.POOL_JAR for s in shards) and not a.serverless:
        jar_src = jar_source(a.jar_path)
        if not jar_src or not jar_src.is_file():
            if a.explain_only:
                notes.append(f"no JAR found in {a.jar_path or 'python/geobrix/lib/'} (the heavy shards need one: gbx:build:jar, or --jar-path)")
            else:
                print(f"PRECONDITION MISSING: no JAR for the heavy shards in {a.jar_path or 'python/geobrix/lib/'}; heavy shards not run.", file=sys.stderr)
                return EXIT_USAGE
        else:
            jar = stage_jar(jar_src, REPO_ROOT, copy=not a.explain_only)
            jar["volume"] = f"{volroot}/{jar['dir']}/{jar['name']}"
    resources = resources_entry(sha, volroot)
    plan = make_plan(a, sha, shards, wheel, jar, volroot, REPO_ROOT, resources, fsolo)
    local_only = [s for s in all_shards if s.local_only]
    print(explain(plan, local_only, notes))
    if a.explain_only:
        print("\nexplain-only: nothing staged or started.")
        return 0
    return launch(a, plan, shards)


def rerun_shards(run_dir: Path, root: Path, shards: Optional[List[ts.Shard]] = None) -> List[ts.Shard]:
    """``--rerun-failed``: one chunk per test file that had a failed or erroring test in the earlier run's junit files, holding just
    those test ids (``file::[Class::]name[params]``). Empty when the run left no failures (or no results here).

    Each chunk runs where its file's CI shard runs (``_test_shards.owner_of``: a heavy dir's file on the JAR pool, with its
    integration tests), with the env of the chunk it failed in (``GBX_SHARD``, ``GBX_ORACLE_TABLES``: the run's ``plan.json``, kept
    beside its results). A rerun that collects nothing is a selection error, not a pass (never a subset)."""
    shards = shards if shards is not None else ts.load(root)
    try:
        old_plan = json.loads((Path(run_dir) / "plan.json").read_text()).get("shards") or {}
    except (OSError, ValueError, AttributeError):
        old_plan = {}
    by_file: Dict[str, List[str]] = {}
    env_of: Dict[str, dict] = {}
    for junit in sorted(Path(run_dir).glob("*/junit.xml")):
        try:
            cases = list(ET.parse(junit).getroot().iter("testcase"))
        except (OSError, ET.ParseError):
            continue
        for c in cases:
            if not any(ch.tag in ("failure", "error") for ch in c):
                continue
            cls = c.get("classname", "")
            f = tm.file_of_case(cls, root)
            if not f:
                continue
            inner = cls.split(".")[len(f[:-3].split("/")):]  # a class the test sits in
            nid = "::".join([f] + inner + [c.get("name", "")])
            if nid not in by_file.setdefault(f, []):
                by_file[f].append(nid)
            env_of.setdefault(f, dict((old_plan.get(junit.parent.name) or {}).get("env") or {}))
    # a test past --test-timeout that did not stop was killed with its chunk: the junit holds no failure for it, the result names it
    for res_file in sorted(Path(run_dir).glob("*/result.json")):
        try:
            timed_out = json.loads(res_file.read_text()).get("timed_out") or []
        except (OSError, ValueError, AttributeError):
            continue
        for nid in timed_out:
            f = str(nid).split("::", 1)[0]
            if not f.endswith(".py") or "::" not in str(nid):
                continue
            if nid not in by_file.setdefault(f, []):
                by_file[f].append(str(nid))
            env_of.setdefault(f, dict((old_plan.get(res_file.parent.name) or {}).get("env") or {}))
    out = []
    for f, ids in sorted(by_file.items()):
        env = env_of.get(f) or {}
        common = dict(name=f"rerun {f}", paths=ids, est_min=ts.FIXED_MIN + 0.5 * len(ids), est_src="ci",
                      part=str(env.get("GBX_SHARD") or ""), oracle_tables=str(env.get("GBX_ORACLE_TABLES") or ""))
        if f.startswith("scripts/"):
            out.append(ts.Shard(kind="scripts", marker="", **common))
            continue
        home = ts.owner_of(shards, f, root)
        out.append(ts.Shard(kind=home.kind if home else "light", pool=home.pool if home else ts.POOL_LIGHT,
                            marker=home.marker if home else "not integration", delta=bool(home and home.delta), **common))
    return out


def ids_shards(path, per_chunk: int, oracle_tables: str, root: Path, shards: Optional[List[ts.Shard]] = None) -> List[ts.Shard]:
    """``--ids-file``: the node ids listed in ``path`` (one per line; blank lines and ``#`` comments skipped), ``per_chunk`` to a chunk
    in their order, each chunk where its file's CI shard runs, with ``oracle_tables`` (the run's ``GBX_ORACLE_TABLES``). A table that
    does not fit one run's wall is finished this way: the ids an earlier run left unrun."""
    shards = shards if shards is not None else ts.load(root)
    ids = [ln.strip() for ln in Path(path).read_text().splitlines() if ln.strip() and not ln.strip().startswith("#")]
    by_file: Dict[str, List[str]] = {}
    for nid in ids:
        by_file.setdefault(nid.split("::", 1)[0], []).append(nid)
    out = []
    for f, fids in by_file.items():
        home = ts.owner_of(shards, f, root)
        parts = [fids[i:i + per_chunk] for i in range(0, len(fids), per_chunk)]
        for i, part in enumerate(parts, 1):
            out.append(ts.Shard(name=f"ids {f} [{i}/{len(parts)}]", paths=part, est_min=ts.FIXED_MIN + 0.5 * len(part), est_src="ci",
                                oracle_tables=oracle_tables, kind=home.kind if home else "light", pool=home.pool if home else ts.POOL_LIGHT,
                                marker=home.marker if home else "not integration", delta=bool(home and home.delta)))
    return out


def affected_selection(a, sha: str, all_shards: List[ts.Shard]):
    """The ``affected`` shards (the selected files and oracle tables, by the CI shard that runs them), the lines that explain
    them (per file and table, the changed file and the import chain), and why the selection is too broad ("" when it is not)."""
    sel, _ = af.affected(REPO_ROOT, a.base, sha, all_shards, cache_dir=REPO_ROOT / GRAPH_CACHE)
    out, lost = af.to_shards(sel, all_shards, REPO_ROOT)
    units, picked = af.light_units(all_shards, REPO_ROOT), af.light_selected(sel, all_shards, REPO_ROOT)
    lines = af.explain_lines(sel, units, picked)
    if lost:
        lines.append(f"  not run: {len(lost)} selected file(s) or table(s) no CI shard runs: " + ", ".join(lost[:8]) + (" ..." if len(lost) > 8 else ""))
    lines.append(f"  affected shards: " + (", ".join(f"{s.name} ({len(s.oracle_tables.split(',')) if s.oracle_tables else len(s.paths)} "
                                                      f"{'table' if s.oracle_tables else 'file'}(s))" for s in out) or "none"))
    broad = ""
    if picked > af.BROAD_SHARE * units:
        why = ", ".join(sel.everything[:3]) if sel.everything else "the changed files reach that many tests"
        broad = (f"!!! affected selects {picked} of the light bank's {units} units ({100.0 * picked / max(1, units):.0f}%, over "
                 f"{af.BROAD_SHARE:.0%}): {why}")
    return out, lines, broad


def planned_clusters(a) -> int:
    """How many clusters the plan is cut for: the light pool's cap, else the clusters it has (the launch leases the free ones, starts
    stopped ones and makes new ones up to the cap, and sizes the runners again for what it got). One when the pool cannot be read."""
    try:
        from databricks.sdk import WorkspaceClient
        import _cluster_pool as cp

        w = WorkspaceClient(profile=a.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or None)
        cfg = cp.read_config(w, _volroot(), a.pool_light)
        return max(1, int(cfg["cap"]) if cfg else len(cp.members(w, a.pool_light)))
    except Exception as exc:  # noqa: BLE001 - a plan for one cluster is a valid plan
        print(f"  (could not count the clusters of pool {a.pool_light}: {type(exc).__name__}; planning for one)", file=sys.stderr)
        return 1


def _volroot() -> str:
    return "/Volumes/%s/%s/%s" % tuple(
        (os.environ.get(k) or d).strip()
        for k, d in (("GBX_BUNDLE_VOLUME_CATALOG", "main"), ("GBX_BUNDLE_VOLUME_SCHEMA", "default"), ("GBX_BUNDLE_VOLUME_NAME", "geobrix_samples"))
    )


def jar_staged(w, jar: dict) -> bool:
    """The JAR's folder is named by its sha256, so a file of the right size there is this JAR (the upload was size-checked)."""
    try:
        return w.files.get_metadata(jar["volume"]).content_length == Path(jar["local"]).stat().st_size
    except Exception:  # noqa: BLE001
        return False


def stage_cached(w, local: Path, remote: str) -> None:
    """Stage ``local`` unless ``remote`` already holds it: a sidecar ``<remote>.sha256`` written after a verified upload
    names the content, so a rerun with --keep-stage and the same --run-id does not upload the wheel again."""
    digest = mut.sha256_file(local)
    try:
        if w.files.download(remote + ".sha256").contents.read().decode().strip() == digest and w.files.get_metadata(remote).content_length == local.stat().st_size:
            print(f"  already staged (sha256 {digest[:16]}...): {remote}", flush=True)
            return
    except Exception:  # noqa: BLE001 - absent: stage it
        pass
    mut.stage_file(w, local, remote)
    put_text(w, remote + ".sha256", digest)


def put_text(w, remote: str, text: str) -> None:
    import io

    w.files.upload(remote, io.BytesIO(text.encode()), overwrite=True)


@contextlib.contextmanager
def lease_pool(w, a, plan: dict, pool_name: str, *, jar: Optional[dict], volroot: str, me: str, wait_min: Optional[float] = None):
    """Lease the pool's cluster and make it carry what the pool's shards need (the JAR, or none). A fresh cluster is a pool of
    its own, named for the run, ended (deleted) at the end."""
    import _cluster_pool as pool

    name = fresh_pool_name(plan["run_id"], bool(jar)) if a.fresh_cluster else pool_name
    ns = SimpleNamespace(pool=name, node_type=a.node_type, pool_wait_min=a.pool_wait_min if wait_min is None else wait_min)
    try:
        with mut.lease_pool_cluster(w, ns, {"run_id": plan["run_id"]}, volroot=volroot, me=me) as lease:
            jars = [jar["volume"]] if jar else []
            res = lease.sync_jars(jars)
            print(f"pool {name}: cluster {lease.cluster_id} leased; JAR {'installed ' + jar['name'] if jar else 'none'} ({res['action']})", flush=True)
            yield lease
    finally:
        if a.fresh_cluster:
            try:
                pool.down(w, name, delete=True)
                print(f"fresh cluster pool {name} deleted")
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: could not delete {name}: {exc}", file=sys.stderr)


_TASK_ENDED = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def active_serverless_runners(w) -> int:
    """Runner tasks (``r_<i>``) not ended in the workspace's active gbx Serverless test runs (run name ``gbx-tests-<id> | serverless |``)."""
    n = 0
    for r in w.jobs.list_runs(active_only=True, expand_tasks=True):
        if not (r.run_name or "").startswith("gbx-tests-") or "| serverless |" not in (r.run_name or ""):
            continue
        for t in r.tasks or []:
            state = getattr(getattr(getattr(t, "state", None), "life_cycle_state", None), "value", "")
            if (t.task_key or "").startswith("r_") and state not in _TASK_ENDED:
                n += 1
    return n


def wait_for_serverless_room(count: Callable[[], int], want: int, cap: int, wait_min: float, *, sleep=time.sleep,
                             clock=time.monotonic, every_s: float = 60.0) -> Optional[int]:
    """Wait until ``want`` more runner tasks fit under ``cap`` across the workspace (``count()`` the ones running); the count it
    found then, or None when ``wait_min`` passed first. ``cap`` 0 is no gate (0 returned at once)."""
    if not cap:
        return 0
    t0 = clock()
    while True:
        busy = count()
        if busy + want <= cap:
            return busy
        if clock() - t0 > wait_min * 60:
            return None
        print(f"  waiting for Serverless room: {busy} runner task(s) of gbx test runs active, {want} more would pass --serverless-cap {cap}",
              flush=True)
        sleep(every_s)


def job_tasks(plan: dict, pool: str, nb_paths: dict, cluster_ids: List[str]) -> List[dict]:
    """The pool job's tasks, as plain dicts: a seed task that fills the queue table, then one runner task per slot, each depending
    on the seed only (a runner never waits for another). Runner i runs on cluster i % len(cluster_ids), so each cluster carries
    --max-parallel runners; with no cluster the tasks run on Serverless."""
    spec = plan["pools"][pool]
    desc = f"{plan['purpose']} (commit {plan['sha'][:10]})"[:1000]

    def task(key, params, timeout_s, i, depends=()):
        t = {"task_key": key, "timeout_seconds": timeout_s, "description": desc, "notebook_path": nb_paths["runner"],
             "base_parameters": {"pool": pool, **params}, "depends_on": list(depends)}
        if cluster_ids:
            t["existing_cluster_id"] = cluster_ids[i % len(cluster_ids)]
        else:
            t["environment_key"] = "gbx_env6"
        return t

    tasks = [task("q_seed", {"role": "seed"}, 15 * 60, 0)]
    first = ["q_seed"]
    if spec.get("smoke"):  # the smoke chunk runs alone first; the runners depend on it, so its failure stops the job before the queue
        tasks.append(task("smoke", {"role": "smoke", "runner": "smoke", "index": "0", "shard_nb": nb_paths["shard"]},
                          int(spec["smoke_cap_min"] * 60), 0, depends=["q_seed"]))
        first = ["smoke"]
    for i in range(spec["runners"]):
        tasks.append(task(f"r_{i}", {"role": "run", "runner": f"r{i}", "index": str(i), "shard_nb": nb_paths["shard"]},
                          int(spec["cap_min"] * 60), i, depends=first))
    return tasks


def submit_pool_job(w, plan: dict, pool: str, nb_paths: dict, leases: list):
    """Submit the pool's job (``job_tasks``); the run id. A runner depends on the seed (or the smoke task) with ALL_SUCCESS: no queue,
    or a failed smoke chunk, and no runner starts."""
    from databricks.sdk.service import compute, jobs
    from _job_submit import submit_once

    spec = plan["pools"][pool]
    tasks = []
    for t in job_tasks(plan, pool, nb_paths, [l.cluster_id for l in leases]):
        kw = {k: t[k] for k in ("existing_cluster_id", "environment_key") if k in t}
        if t["depends_on"]:
            kw["depends_on"] = [jobs.TaskDependency(task_key=d) for d in t["depends_on"]]
        tasks.append(jobs.SubmitTask(
            task_key=t["task_key"], max_retries=0, timeout_seconds=t["timeout_seconds"], description=t["description"],
            notebook_task=jobs.NotebookTask(notebook_path=t["notebook_path"], source=jobs.Source.WORKSPACE, base_parameters=t["base_parameters"]), **kw))
    extra = {}
    if not leases:
        deps = [f"geobrix[{plan['extras']}] @ file://{plan['wheel']['volume']}"] + plan["pins"] + plan["pip_extra"].split()
        extra["environments"] = [jobs.JobEnvironment(environment_key="gbx_env6", spec=compute.Environment(environment_version="6", dependencies=deps))]
        for t in tasks:
            t.disable_auto_optimization = True
    name = f"gbx-tests-{plan['run_id']} | {pool} | {plan['purpose'][:70]} | {plan['sha'][:10]}"
    waiter = submit_once(w, run_name=name, timeout_seconds=int(plan["caps"]["start_min"] * 60 + (spec["cap_min"] + spec.get("smoke_cap_min", 0)) * 60),
                         tasks=tasks, **extra)
    return waiter.run_id


def start_pool(w, a, plan: dict, pool: str, stack, *, volroot: str, me: str, base_dir: str, nbs: List[str], runs: Dict[str, int],
               pool_info: Dict[str, dict], t_submit: Dict[str, float], fmt=None) -> int:
    """Lease the pool's clusters (it may wait up to --pool-wait-min), size its runners for them, then arm its wall budget, write the
    plan, upload its notebooks and submit its job: the budget (``deadline``, which its runners stop claiming at, even with the launcher
    gone) starts when its tests can start, never during the lease wait. ``fmt``: the notebooks' import format (the SDK's
    ``ImportFormat.JUPYTER``). Returns the job's run id."""
    import base64

    run_id = plan["run_id"]
    pool_leases = []
    if pool != "serverless":
        pool_leases = lease_many(stack, w, a, plan, pool, volroot=volroot, me=me)
        cores = min(mut.cores_of(w.clusters.get(l.cluster_id).node_type_id or "") or 32 for l in pool_leases)
        relane(plan, pool, a, len(pool_leases), cores)
        pool_info[pool] = {"cluster_id": ", ".join(l.cluster_id for l in pool_leases), "node_type": w.clusters.get(pool_leases[0].cluster_id).node_type_id}
    if pool == "serverless":  # the workspace gate: the runner tasks of every gbx Serverless test run share a cap
        busy = wait_for_serverless_room(lambda: active_serverless_runners(w), plan["pools"][pool]["runners"], a.serverless_cap,
                                        a.pool_wait_min)
        if busy is None:
            raise RuntimeError(f"no Serverless room in {a.pool_wait_min:g} min: other gbx test runs hold the --serverless-cap of "
                               f"{a.serverless_cap} runner tasks; wait for them, or lower --max-parallel")
    budget = float((plan.get("fail_fast") or {}).get("max_wall_min") or 0)
    if budget:
        plan["pools"][pool]["deadline"] = time.time() + budget * 60
    put_text(w, plan["plan_file"], json.dumps(plan))  # the runners read the pool's queue, table and deadline from it
    spec = plan["pools"][pool]
    print(f"  {pool}: {len(spec['queue'])} shard(s) on {spec['clusters']} cluster(s) x {spec['slots']} slots = {spec['runners']} runner(s); expected wall about "
          f"{spec['expected_min']:g} min (longest shard {spec['longest_min']:g}, serial sum {spec['serial_min']:g})"
          + (f"; wall budget {budget:g} min from now" if budget else ""), flush=True)
    paths = {"shard": f"{base_dir}/tests_{run_id}_{pool}.ipynb", "runner": f"{base_dir}/runner_{run_id}_{pool}.ipynb"}
    for kind, nb in (("shard", build_notebook(plan, pool)), ("runner", build_runner_notebook(plan, pool))):
        body = base64.b64encode(json.dumps(nb, indent=1).encode()).decode()
        w.workspace.import_(paths[kind], format=fmt, content=body, overwrite=True)
        nbs.append(paths[kind])
        print(f"Uploaded {kind} notebook {paths[kind]}", flush=True)
    job_run = submit_pool_job(w, plan, pool, paths, pool_leases)
    runs[pool] = job_run
    pool_info.setdefault(pool, {})["job_run"] = job_run
    t_submit[pool] = time.monotonic()
    for l in pool_leases:
        l.job_run(job_run)
    try:
        print(f"  {pool}: run {job_run}  {w.jobs.get_run(job_run).run_page_url}", flush=True)
    except Exception:  # noqa: BLE001
        pass
    return job_run


def lease_many(stack, w, a, plan: dict, pool: str, *, volroot: str, me: str) -> list:
    """Lease the free clusters of the pool, up to --clusters (default: all it has): the first waits for one (and makes the
    pool's cluster when it has none), the others only take what is free now, so a busy shared pool gives fewer slots, not a
    failure. Every cluster is made to carry the pool's JAR (or none)."""
    import _cluster_pool as cp

    jar = plan["jar"] if plan["pools"][pool]["jar"] else None
    first = stack.enter_context(lease_pool(w, a, plan, pool, jar=jar, volroot=volroot, me=me))
    got = [first]
    if a.fresh_cluster:
        return got
    cfg = cp.read_config(w, volroot, pool_name_of(a, pool, bool(jar)))
    want = a.clusters or (int(cfg["cap"]) if cfg else len(cp.members(w, pool)))  # up to the pool's cap: free ones first, then stopped, then new
    tried = 0
    while len(got) < want and tried < want * 2:
        tried += 1
        try:
            got.append(stack.enter_context(lease_pool(w, a, plan, pool, jar=jar, volroot=volroot, me=me, wait_min=0.05)))
        except Exception as exc:  # noqa: BLE001 - an extra cluster is optional: whatever stops it, the run goes on with those it has
            print(f"  pool {pool}: {len(got)} of {want} cluster(s) free ({str(exc)[:100]}); running on those", flush=True)
            break
    return got


def fresh_pool_name(run_id: str, with_jar: bool) -> str:
    """The pool a --fresh-cluster run makes for itself: one per kind, so the light and the JAR pool of one run are two clusters."""
    return f"gbx-test-fresh-{run_id}-{'jar' if with_jar else 'light'}"[:60]


def pool_name_of(a, pool: str, with_jar: bool = False) -> str:
    return fresh_pool_name(a.run_id, with_jar) if a.fresh_cluster else pool


def relane(plan: dict, pool: str, a, n_clusters: int, cores: int) -> None:
    """Size the pool's runners again for the clusters actually leased and the cores they have (the queue stays as it is). When the
    slots per node differ from the plan's, each chunk's estimate, solo time and units' solo seconds move to the new slots
    (``_test_shards.slowdown``): they were taken at the plan's, and a run measures its chunks against them."""
    spec = plan["pools"][pool]
    before = list(spec["queue"])
    slots = a.max_parallel or ts.slots_for(cores)
    was = int(spec.get("time_slots") or spec.get("slots") or slots)
    if slots != was:
        k = ts.slowdown(slots) / ts.slowdown(was)
        for slug in before:
            c = plan["shards"][slug]
            if c.get("smoke"):
                continue  # a fixed guess, not a time at any slot count
            c["expected_min"] = round(c["expected_min"] * k, 2)
            c["solo_min"] = round(float(c.get("solo_min") or 0.0) * k, 2)
            c["unit_solo_s"] = {u: round(v * k, 1) for u, v in (c.get("unit_solo_s") or {}).items()}
    spec["time_slots"] = slots
    size_pool(spec, {slug: plan["shards"][slug]["expected_min"] for slug in before}, slots, n_clusters, a.shard_cap_min,
              {slug for slug in before if plan["shards"][slug].get("heavy")}, smoke=spec.get("smoke") or "",
              start=(plan.get("fail_fast") or {}).get("start_chunks", 0))
    assert sorted(spec["queue"]) == sorted(before), "relane lost or invented a shard"


def history_for(path: Path) -> dict:
    """The timing history at ``path``; with none yet, the last run's seconds of the old ``timings.json`` (one sample per file)."""
    h = tm.load(path)
    if not h["keys"] and Path(path) == REPO_ROOT / TIMINGS:
        h = tm.load(REPO_ROOT / OLD_TIMINGS)
    return h


def estimate_note(shards: List[ts.Shard], hist: dict, path: str) -> str:
    """Where the chunks' estimates came from: the history (every key timed), partly, a lower bound, or CI's minutes."""
    c = Counter(s.est_src or "ci" for s in shards)
    words = {"history": "history", "partial": "history, partly", "lower bound": "a lower bound (a chunk that was stopped)", "ci": "CI minutes"}
    parts = [f"{c[k]} from {words[k]}" for k in words if c[k]]
    return f"estimates: {', '.join(parts)}; the history {path} holds {len(hist.get('keys', {}))} key(s) from {len(hist.get('runs', []))} run(s)"


def chunk_busy(records: Dict[str, List[dict]]) -> Dict[str, float]:
    """Per chunk, the seconds its runner spent on it (its notebook run, start to end): the actual time a plan predicts."""
    return {x["slug"]: x["end"] - x["start"] for recs in records.values() for r in recs for x in r.get("shards", [])
            if x.get("end") is not None and x.get("start") is not None}


def actual_s(row: dict, busy: Dict[str, float]):
    if row["slug"] in busy:
        return int(busy[row["slug"]])
    return int(row["wall_s"] + ts.FIXED_MIN * 60) if isinstance(row["wall_s"], int) else None


def record_timings(path: Path, plan: dict, rows: List[dict], local_out: Path, records: Optional[Dict[str, List[dict]]] = None) -> dict:
    """Merge this run's samples into the history, whatever the run's outcome: the seconds per file (per oracle table for the
    family file) of every chunk from its junit, and lower bounds for what a stopped chunk did not finish (``_timings``); and per
    unit its actual over its solo-expected time with the chunks beside it (the contention records). Each row gains its chunk's
    measured contention (``x``: actual over solo-expected), its neighbours (``nb``) and the peak concurrency on its cluster;
    the result carries the peak per cluster of each pool (``peaks``)."""
    busy = chunk_busy(records or {})
    fn_table = tm.table_of_function(REPO_ROOT)
    sls = bool(plan.get("serverless"))
    stop = plan.get("stopped_early") or ""
    # on Serverless the runner tasks of one job share its driver hosts (tst-sls-p1-dem): its chunks are neighbours, one "cluster"
    per, peaks = tm.neighbours(records or {}, {pool: (1 if sls else plan["pools"].get(pool, {}).get("clusters", 1))
                                               for pool, recs in (records or {}).items()})
    samples, cont = [], []
    # the first chunk of each runner started cold (its runner's compute fresh; on Serverless few task slots): its samples say so
    firsts = {sorted(rec.get("shards") or [], key=lambda x: float(x.get("start") or 0))[0].get("slug")
              for recs in (records or {}).values() for rec in recs if rec.get("shards")}
    for r in rows:
        chunk = plan["shards"].get(r["slug"], {})
        slots = 1 if sls else plan["pools"].get(chunk.get("pool"), {}).get("slots", 1)
        junit = local_out / r["slug"] / "junit.xml"
        act = actual_s(r, busy)
        # a chunk pytest stopped at --maxfail did not run all its tests: its times are lower bounds, as a capped chunk's are
        verdict = "INCOMPLETE" if (r.get("maxfail_hit") and r["verdict"] == "FAILED") or r["verdict"] == "OVERRUN" else r["verdict"]
        got_s = [] if chunk.get("smoke") or chunk.get("select_k") else tm.run_samples(  # a smoke or -k chunk runs a few rows: no unit's time
            chunk, verdict, act, junit, REPO_ROOT, slots, fn_table, stop=stop if verdict == "INCOMPLETE" else "", prefix=tm.SLS if sls else "",
            progress=progress_of(local_out / r["slug"] / "pytest.log") if verdict == "INCOMPLETE" else None)
        n_parts = int(str(chunk.get("test_part") or "0/1").split("/")[1])
        for x in got_s:  # a test part times its own part only (``<key>#i/n``); a unit all of whose parts ran gets their sum below
            x["key"] = tm.part_key(x["key"], chunk.get("test_part") or "")
            if r["slug"] in firsts:
                x["first"] = True
        samples += got_s
        near = per.get(r["slug"], {})
        solo = float(chunk.get("solo_min") or chunk.get("expected_min") or 0.0)
        r.update(x=round(act / (solo * 60), 2) if act and solo else None, nb=near.get("nb", 0), peak=near.get("peak", 0))
        if verdict in ("ok", "FAILED", "INCOMPLETE") and n_parts == 1 and not chunk.get("smoke") and not chunk.get("select_k"):
            recs = tm.contention_records(chunk, act, tm.key_seconds(junit, REPO_ROOT, chunk, fn_table), near.get("nb", 0), near.get("peak", 0),
                                         incomplete=verdict == "INCOMPLETE")
            cont += [dict(c, key=tm.SLS + c["key"]) for c in recs] if sls else recs  # a Serverless record under its own key
    run = {"run": plan["run_id"], "sha": plan["sha"][:10], "slots": max((p.get("slots", 1) for p in plan["pools"].values()), default=1)}
    if sls:
        run["serverless"] = True
    if stop:
        run["stopped_early"] = stop
    got = tm.merge(path, samples + tm.whole_units(samples), run, cont)
    print(f"Timings: {got['keys']} sample(s), {got['lower_bounds']} lower bound(s) and {got['contention']} contention record(s) from this run merged into {path}")
    return dict(got, peaks=peaks)


def life_and_result(run) -> tuple:
    st = run.state

    def val(x):
        return x.value if x is not None and hasattr(x, "value") else (str(x) if x is not None else "")

    return val(st.life_cycle_state), val(st.result_state)


def launch(a, plan: dict, shards: List[ts.Shard]) -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat
    from _job_submit import runner_dir

    w = WorkspaceClient(profile=a.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or None)
    me = w.current_user.me().user_name
    run_id, stage = plan["run_id"], plan["stage"]
    volroot = stage.split("/test-runs/")[0]
    local_out = REPO_ROOT / plan["local_out"]
    local_out.mkdir(parents=True, exist_ok=True)
    base_dir = "/" + runner_dir(w).strip().removeprefix("/Workspace").lstrip("/")
    nbs: List[str] = []
    rc = EXIT_FAILED
    ok = False
    pool_info: Dict[str, dict] = {}
    runs: Dict[str, int] = {}
    recorded = False
    try:
        tar_local = local_out / f"{run_id}-src.tar.gz"
        size = build_tarball(plan["sha"], tar_local)
        print(f"Source tarball of {plan['sha'][:10]}: {size / 1e6:.1f} MB; wheel {plan['wheel']['name']} (built from {plan['wheel']['built_from'][:10]})", flush=True)
        stage_cached(w, Path(plan["wheel"]["local"]), plan["wheel"]["volume"])
        stage_cached(w, tar_local, plan["tarball"])
        tar_local.unlink()
        put_text(w, plan["plan_file"], json.dumps(plan))
        if plan["resources"]:
            stage_resources(w, plan["resources"], plan["sha"], local_out)
        if plan["jar"]:
            if jar_staged(w, plan["jar"]):
                print(f"  JAR {plan['jar']['name']} ({plan['jar']['sha256'][:12]}) already staged", flush=True)
            else:
                mut.stage_file(w, Path(plan["jar"]["local"]), plan["jar"]["volume"])

        with contextlib.ExitStack() as stack:
            t_submit: Dict[str, float] = {}
            finished: set = set()
            with cancel_on_error(w, runs, finished):  # inside the leases: a failed launch cancels its runs before the clusters are given back
                w.workspace.mkdirs(base_dir)
                for pool in plan["pools"]:
                    start_pool(w, a, plan, pool, stack, volroot=volroot, me=me, base_dir=base_dir, nbs=nbs, runs=runs, pool_info=pool_info,
                               t_submit=t_submit, fmt=ImportFormat.JUPYTER)
                print(f"  heartbeats: {stage}/hb/ and {stage}/runners/; cancel: " + "; ".join(f"databricks jobs cancel-run {r}" for r in runs.values()), flush=True)
                live = Live(w, plan, local_out)
                rc = watch(w, runs, plan, pool_info, live)
                finished.update(runs)
                live.poll()  # the chunks that ended between the last poll and the end of the jobs
                if not plan.get("stopped_early") and live.check():  # e.g. a failed smoke ended the job before the next poll
                    plan["stopped_early"] = live.rule
                    rc = rc or EXIT_FAILED
                records = runner_records(w, plan, local_out)
                rows = collect(w, plan, runs, local_out, records, live.fetched)
                mark_empty_units(plan, rows)
                for pool, info in pool_info.items():
                    walls = [r["wall_s"] for r in rows if r["pool"] == pool and isinstance(r["wall_s"], int)]
                    info["longest_s"] = max(walls) if walls else "-"
                    info["serial_s"] = sum(walls)
                    info["wall_s"] = int(time.monotonic() - t_submit[pool]) if pool in t_submit else "-"
                    info.update(pool_metrics(records.get(pool, [])))
                got = record_timings(REPO_ROOT / a.timings, plan, rows, local_out, records)
                for pool, info in pool_info.items():
                    info["peaks"] = got["peaks"].get(pool, {})
                recorded = True
                text = summary_md(plan, rows, pool_info)
                (local_out / "summary.md").write_text(text)
                (local_out / "plan.json").write_text(json.dumps(plan, indent=1))  # --rerun-failed reads each chunk's env from it
                print("\n" + table_text(rows))
                for pool, info in pool_info.items():
                    print(f"{pool}: {speedup_line(info)}")
                    for ln in schedule_lines(info):
                        print(ln)
                    if info.get("peaks"):
                        print("  peak concurrency: " + ", ".join(f"cluster {c}: {n}" for c, n in sorted(info["peaks"].items())))
                if isolation_line(rows):
                    print(isolation_line(rows))
                for r in rows:
                    for n, m in r["failures"][:5]:
                        print(f"  FAIL [{r['name']}] {n}: {m}")
                if plan.get("stopped_early"):
                    print(f"\nstopped early: {plan['stopped_early']} (the summary and the timing history record it; the chunks it stopped "
                          "leave lower bounds)", file=sys.stderr)
                    if not_run_line(rows):
                        print(not_run_line(rows), file=sys.stderr)
                print(f"\nWrote {local_out / 'summary.md'}")
                doubled = [r["name"] for r in rows if r.get("claims", 0) > 1]
                if doubled:
                    print(f"ERROR: shard(s) run more than once: {', '.join(doubled)}; the queue's claim was not exclusive", file=sys.stderr)
                bad = [r for r in rows if is_failure(r) or r.get("claims", 0) > 1]
                if rc == 0 and bad:
                    rc = EXIT_FAILED
                ok = rc == 0
    finally:
        if runs and not recorded:  # a launch that ended early (Ctrl-C, an error) still leaves what its chunks measured
            try:
                records = runner_records(w, plan, local_out)
                record_timings(REPO_ROOT / a.timings, plan, collect(w, plan, runs, local_out, records), local_out, records)
            except Exception as exc:  # noqa: BLE001 - the history is a by-product; the stage is removed regardless
                print(f"WARNING: the timings of this run were not merged ({type(exc).__name__}: {str(exc)[:120]})", file=sys.stderr)
        tables = [spec["table"] for spec in plan["pools"].values()]
        if a.keep_stage:
            print(f"Kept {stage}, {', '.join(nbs)} and the queue table(s) {', '.join(tables)} (--keep-stage).")
        else:
            mut.rm_volume_dir(w, stage)
            if nbs:  # a run was submitted: its queue tables may exist
                drop_queue_tables(w, tables)
            for nb in nbs:
                if ok or rc in (EXIT_FAILED, EXIT_NO_START, EXIT_STALLED):
                    try:
                        w.workspace.delete(nb)
                    except Exception as exc:  # noqa: BLE001
                        if "does not exist" not in str(exc).lower():
                            print(f"WARNING: could not remove {nb} ({exc}); delete it by hand.", file=sys.stderr)
        if plan["jar"]:
            shutil.rmtree(Path(plan["jar"]["local"]).parent, ignore_errors=True)  # the temporary copy and its folder
    return rc


def drop_queue_tables(w, tables: List[str]) -> None:
    for t in tables:
        name = t.replace("`", "")
        try:
            w.tables.delete(name)
            print(f"Dropped the queue table {name}")
        except Exception as exc:  # noqa: BLE001
            if "not found" not in str(exc).lower() and "does not exist" not in str(exc).lower():
                print(f"WARNING: could not drop {name} ({exc}); drop it by hand.", file=sys.stderr)


def runner_records(w, plan: dict, local_out: Path) -> Dict[str, List[dict]]:
    """Download the runners' records (``runners/<pool>/r<i>.json``), per pool: each holds when its runner started and ended
    and, per shard it ran, the claim, start and end times, rc and reason."""
    out: Dict[str, List[dict]] = {}
    for pool in plan["pools"]:
        d = local_out / "_runners" / pool
        for name in mut.download_dir(w, f"{plan['stage']}/runners/{pool}", d):
            try:
                out.setdefault(pool, []).append(json.loads((d / name).read_text()))
            except ValueError:
                continue
    return out


def verdict_of(line: dict) -> str:
    """The verdict word of a chunk's one-line result (as ``shard_verdict`` names the rows of the summary)."""
    if line.get("overrun") and not line.get("failed_chunk"):
        return "OVERRUN"
    if line.get("rc") == 5 and not (line.get("failed") or line.get("errors") or line.get("reason")):
        return "EMPTY"  # a failed chunk when it may not be empty (a rerun, a whole shard), else a part that may be
    if not line.get("failed_chunk"):
        return "ok"
    if line.get("reason") or line.get("rc") is None:
        return "INCOMPLETE"
    return "FAILED" if line.get("failed") else "ERRORS"


def child_url(child: Optional[dict], host: str = "") -> str:
    """The page of a chunk's own notebook run (a child run of its runner task), from the ids it recorded; "" without them."""
    child = child or {}
    if not (child.get("job_id") and child.get("run_id")):
        return ""
    org = f"?o={child['org']}" if child.get("org") else ""
    return f"{host.rstrip('/')}/{org}#job/{child['job_id']}/run/{child['run_id']}"


def live_row(line: dict, plan: dict, elapsed_s: float, host: str = "") -> str:
    """One row of the live table: when the launcher saw the chunk end, its verdict, counts and time, its own run's page, and its
    first failure."""
    name = plan["shards"].get(line.get("slug"), {}).get("name", line.get("slug"))
    took = int(float(line.get("end") or 0) - float(line.get("start") or 0)) if line.get("end") and line.get("start") else 0
    ff = line.get("first_failure") or {}
    tail = f"  first: {ff.get('id') or ''} {ff.get('msg') or ''}".rstrip() if line.get("failed_chunk") and (ff.get("id") or ff.get("msg")) else ""
    url = child_url(line.get("child"), host)
    return (f"  +{int(elapsed_s) // 60:3d}m{int(elapsed_s) % 60:02d}s  {verdict_of(line):<10} {name[:30]:<30} "
            f"passed {line.get('passed', 0):>4} failed {line.get('failed', 0):>3} errors {line.get('errors', 0):>3} "
            f"skipped {line.get('skipped', 0):>3}  {took:>5} s" + (" (smoke)" if line.get("smoke") else "")
            + (" (--maxfail hit)" if line.get("maxfail_hit") else "") + (" (2x its estimate: stopped)" if line.get("overrun") else "")
            + (f"  run {url}" if url else "") + tail)


class Live:
    """The run's results as they come: each chunk's one-line result (``results/<slug>.json``) is read once, printed as a row of the
    live table, and its files (junit.xml, pytest.log, result.json) are downloaded at once; the fail-fast policy is checked against
    every result so far (``_shard_runner.stop_rule``)."""

    def __init__(self, w, plan: dict, local_out: Optional[Path] = None, t0: Optional[float] = None, out=None):
        # flushed: the launcher's output is often a log file, where an unflushed row would show minutes late
        self.w, self.plan, self.local_out, self.out = w, plan, local_out, out or (lambda text: print(text, flush=True))
        self.t0 = time.monotonic() if t0 is None else t0
        self.lines: Dict[str, dict] = {}
        self.fetched: set = set()
        self.rule = ""
        self.host = str(getattr(getattr(w, "config", None), "host", "") or "")

    def poll(self, now: Optional[float] = None) -> List[dict]:
        """Read the results that are new since the last poll; returns them (and prints a row each)."""
        now = time.monotonic() if now is None else now
        try:
            entries = [e for e in self.w.files.list_directory_contents(f"{self.plan['stage']}/results") if not e.is_directory]
        except Exception:  # noqa: BLE001 - no folder until the first chunk ends
            return []
        new = []
        for e in sorted(entries, key=lambda e: e.name):
            slug = e.name[:-5] if e.name.endswith(".json") else ""
            if not slug or slug in self.lines:
                continue
            line = mut.read_json(self.w, e.path)
            if not isinstance(line, dict):
                continue  # being written: the next poll reads it
            if not self.lines:
                self.out("  live results (a row per chunk the moment it ends; failures show their first test):")
            self.explain_early_end(line)
            self.lines[slug] = line
            new.append(line)
            self.out(live_row(line, self.plan, now - self.t0, self.host))
            if self.local_out is not None and slug not in self.fetched:
                mut.download_dir(self.w, f"{self.plan['stage']}/out/{slug}", self.local_out / slug)
                self.fetched.add(slug)
        return new

    def explain_early_end(self, line: dict) -> None:
        """A chunk that ended without a result (its notebook failed before pytest, or was killed): its first failure becomes its own
        run's error (``get_run_output``), not the platform's "Workload failed"."""
        rid = (line.get("child") or {}).get("run_id")
        if line.get("rc") is not None or not rid or "without a result" not in str(line.get("reason") or ""):
            return
        try:
            err = (self.w.jobs.get_run_output(int(rid)).error or "").strip()
        except Exception:  # noqa: BLE001 - the platform's message stands
            return
        if err:
            line["first_failure"] = {"id": "(the chunk's run)", "msg": " ".join(err.split())[:300]}

    def status(self) -> str:
        """The one-line status: chunks done and failed, and the first failure's test id."""
        done, bad = self.counts()
        first = next((x for x in sorted(self.lines.values(), key=lambda x: float(x.get("end") or 0)) if x.get("failed_chunk")), None)
        ff = (first or {}).get("first_failure") or {}
        return f"{done} chunk(s) done, {bad} failed" + (f"; first failure: {ff.get('id') or ff.get('msg', '')[:80]}" if first else "")

    def check(self) -> str:
        """The fail-fast rule that fires now ("" when none does), against every result so far; once fired it stays."""
        if not self.rule:
            self.rule = sr.stop_rule(list(self.lines.values()), self.plan.get("fail_fast"))
        return self.rule

    def counts(self) -> tuple:
        bad = [x for x in self.lines.values() if x.get("failed_chunk")]
        return len(self.lines), len(bad)

    def failures(self) -> List[str]:
        """The failures seen so far, a line per failed chunk (its first failure)."""
        out = []
        for slug, x in sorted(self.lines.items(), key=lambda kv: float(kv[1].get("end") or 0)):
            if x.get("failed_chunk"):
                ff = x.get("first_failure") or {}
                name = self.plan["shards"].get(slug, {}).get("name", slug)
                out.append(f"  FAIL [{name}] {verdict_of(x)} ({x.get('failed', 0)} failed, {x.get('errors', 0)} errors): "
                           f"{ff.get('id') or ''} {ff.get('msg') or x.get('reason') or ''}".rstrip())
        return out


def wall_rule(plan: dict, elapsed_s: float) -> str:
    """The wall-budget rule ("" while the run is within --max-wall-min, or has no budget)."""
    budget = float((plan.get("fail_fast") or {}).get("max_wall_min") or 0)
    return f"the wall budget of {budget:g} min is spent (--max-wall-min)" if budget and elapsed_s > budget * 60 else ""


def not_run_line(rows: List[dict]) -> str:
    """The chunks a stop left unrun, by name ("" when it left none)."""
    names = [r["name"] for r in rows if str(r.get("reason") or "").startswith("not run: stopped early")]
    return f"not run ({len(names)} chunk(s)): " + ", ".join(names) if names else ""


def stop_early(w, runs: Dict[str, int], plan: dict, live: Live, rule: str) -> None:
    """Stop the whole run: the stop marker for the runners (they claim nothing more), then cancel every pool's job (the running
    chunks and the queue). Prints the failures seen so far."""
    plan["stopped_early"] = rule
    print(f"\nstopped early: {rule}. {len(live.lines)} chunk(s) ended, {live.counts()[1]} failed; cancelling the queue and the running chunks.",
          file=sys.stderr, flush=True)
    for ln in live.failures():
        print(ln, file=sys.stderr, flush=True)
    try:
        put_text(w, f"{plan['stage']}/stop.json", json.dumps({"rule": rule, "ts": time.time()}))
    except Exception as exc:  # noqa: BLE001 - the cancel below stops the runners anyway
        print(f"  (could not write the stop marker: {type(exc).__name__})", file=sys.stderr)
    for rid in runs.values():
        _cancel(w, rid)


def watch(w, runs: Dict[str, int], plan: dict, pool_info: dict, live: Optional[Live] = None) -> int:
    """Poll the pool jobs and the shard heartbeats until every job is over. One status line per pool about every 2-3 min.
    Cancels the runs of the pool whose shards are all stale (10 min), or none has begun in start_min. Every poll reads the chunks'
    one-line results (``Live``): a row per chunk as it ends, and the fail-fast policy, which stops the whole run when it fires."""
    c = plan["caps"]
    t0 = time.monotonic()
    live = live or Live(w, plan, t0=t0)
    last_say = 0.0
    alive: Dict[str, PoolWatch] = {}
    over: Dict[str, str] = {}
    last_beat: Dict[str, float] = {}
    changed: Dict[str, dict] = {}
    rc = 0
    while len(over) < len(runs):
        time.sleep(POLL_S)
        now = time.monotonic()
        live.poll(now)
        if not plan.get("stopped_early") and (live.check() or wall_rule(plan, now - t0)):
            stop_early(w, runs, plan, live, live.rule or wall_rule(plan, now - t0))
            rc = rc or EXIT_FAILED
        say = now - last_say >= STATUS_EVERY_S
        for pool, rid in runs.items():
            if pool in over:
                continue
            try:
                run = w.jobs.get_run(rid)
            except Exception as exc:  # noqa: BLE001 - transient
                print(f"  (poll error: {exc})", file=sys.stderr)
                continue
            life, res = life_and_result(run)
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                over[pool] = res or life
                print(f"  [{pool}] run {rid} finished: {res or life} after {int(now - t0)} s", flush=True)
                continue
            if not say and now - last_beat.get(pool, -BEAT_EVERY_S) < BEAT_EVERY_S:
                continue
            last_beat[pool] = now
            spec = plan["pools"][pool]
            names = (["smoke"] if spec.get("smoke") else []) + [f"r{i}" for i in range(spec["runners"])]
            pw = alive.setdefault(pool, PoolWatch())
            recs = pw.records({n: mut.read_json(w, f"{plan['stage']}/runners/{pool}/{n}.json") for n in names})
            current = [r["current"] for r in recs if r.get("current") and not r.get("done")]  # the shards the runners are on now
            beats = pw.beats({slug: mut.read_json(w, f"{plan['stage']}/hb/{slug}.json") for slug in current})
            ran = [x for r in recs for x in r.get("shards", [])]
            started = pw.started
            if say:
                tot = sum(x.get(k) or 0 for x in ran for k in ("passed", "failed", "skipped")) + sum(b.get("passed", 0) + b.get("failed", 0) + b.get("skipped", 0) for b in beats.values())
                bad = sum(x.get(k) or 0 for x in ran for k in ("failed", "errors")) + sum(b.get("failed", 0) + b.get("errors", 0) for b in beats.values())
                busy = sum(1 for r in recs if not r.get("done"))
                ended = sum(1 for slug in live.lines if slug in spec["queue"])
                failed_chunks = sum(1 for slug, x in live.lines.items() if slug in spec["queue"] and x.get("failed_chunk"))
                print(f"  [{pool}] {life} {int(now - t0) // 60}m{int(now - t0) % 60:02d}s: {ended}/{len(spec['queue'])} chunks done ({failed_chunks} failed), "
                      f"{len(current)} running, {max(0, len(spec['queue']) - ended - len(current))} queued; {busy} of {spec['runners']} runner(s) at work; "
                      f"{tot} tests finished, {bad} failing; {live.status()}", flush=True)
                runs_now = [f"{plan['shards'][slug]['name']} {child_url(b.get('child'), live.host)}" for slug, b in beats.items() if child_url(b.get("child"), live.host)]
                if runs_now:
                    print("    running: " + "; ".join(runs_now), flush=True)
            running = current
            if not started and now - t0 > c["start_min"] * 60:
                print(f"No shard of {pool} has beaten {c['start_min']:g} min after the submit. Cancelling run {rid}.", file=sys.stderr)
                _cancel(w, rid)
                rc = rc or EXIT_NO_START
            elif beats and running:
                if run_is_stuck(running, beats, changed.setdefault(pool, {}), now, (c["stall_min"] + 5) * 60.0):
                    print(f"Every running shard of {pool} has a heartbeat that has not moved for {c['stall_min'] + 5:g} min: a stuck run. Cancelling run {rid}.", file=sys.stderr)
                    _cancel(w, rid)
                    rc = rc or EXIT_STALLED
        if say:
            last_say = now
    return rc


class PoolWatch:
    """What the launcher knows of one pool's runners between polls. A runner record or a heartbeat is copied onto the Volume with a
    plain copy (a FUSE mount has no atomic rename), so a read can find it half written or not there for a moment: such a read keeps
    the last whole one, and a pool that has started (a record with a shard, or a beat) stays started. A read that fails can then
    never make a healthy run look as if it had not started, or leave its running shards out of the stuck check."""

    def __init__(self):
        self.last: Dict[str, dict] = {}
        self.last_beat: Dict[str, dict] = {}
        self.started = False

    def records(self, read: Dict[str, Optional[dict]]) -> List[dict]:
        for name, rec in read.items():
            if isinstance(rec, dict):
                self.last[name] = rec
        recs = list(self.last.values())
        self.started = self.started or any(r.get("shards") for r in recs)
        return recs

    def beats(self, read: Dict[str, Optional[dict]]) -> Dict[str, dict]:
        for slug, b in read.items():
            if isinstance(b, dict):
                self.last_beat[slug] = b
        got = {slug: self.last_beat[slug] for slug in read if slug in self.last_beat}
        self.started = self.started or bool(got)
        return got


def run_is_stuck(running: List[str], beats: Dict[str, dict], changed: Dict[str, tuple], now: float, limit_s: float) -> bool:
    """True when every shard running now has a heartbeat that stopped moving for ``limit_s`` seconds, on the launcher's clock (the
    node's clock is not compared). A shard with no heartbeat yet is starting (it installs and unpacks first), and the heartbeat of
    a shard that is not running (one the watchdog killed, one that finished) says nothing, so neither makes a run look stuck
    or healthy by itself. ``changed`` keeps, per shard, the last beat seen and when it changed."""
    for slug, b in beats.items():
        prev = changed.get(slug)
        if prev is None or prev[0] != b.get("ts"):
            changed[slug] = (b.get("ts"), now)
    if not running or any(slug not in beats for slug in running):
        return False
    return all(not beats[slug].get("done") and now - changed[slug][1] > limit_s for slug in running)


def cancel_unfinished(w, runs: Dict[str, int], finished: set) -> List[int]:
    """Cancel the pool runs this launch submitted that it has not seen end; the run ids it cancelled. A launcher that fails (a
    Ctrl-C, a lease that times out for a later pool, an error while collecting) must not leave its runs on clusters it gives back,
    reading a plan and a tarball it is about to delete."""
    out = []
    for pool, rid in runs.items():
        if pool in finished:
            continue
        print(f"Cancelling run {rid} of pool {pool}: the launch is ending before it saw it finish.", file=sys.stderr)
        _cancel(w, rid)
        out.append(rid)
    return out


@contextlib.contextmanager
def cancel_on_error(w, runs: Dict[str, int], finished: set):
    try:
        yield
    except BaseException:  # noqa: BLE001 - Ctrl-C included: cancel, then let it go on
        cancel_unfinished(w, runs, finished)
        raise


def _cancel(w, rid: int) -> None:
    try:
        w.jobs.cancel_run(rid)
    except Exception as exc:  # noqa: BLE001
        print(f"cancel FAILED: {exc}; cancel run {rid} by hand", file=sys.stderr)


def task_failures(w, runs: Dict[str, int]) -> Dict[str, str]:
    """Per pool, what its seed or runner tasks that did not succeed said (a shard nobody ran gets it as its reason)."""
    out: Dict[str, str] = {}
    for pool, rid in runs.items():
        try:
            bad = [t for t in (w.jobs.get_run(rid).tasks or []) if life_and_result(t)[1] not in ("SUCCESS", "")]
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: could not read run {rid}: {exc}", file=sys.stderr)
            continue
        msgs = []
        for t in bad[:3]:
            try:
                err = (w.jobs.get_run_output(t.run_id).error or "")[:200]
            except Exception:  # noqa: BLE001
                err = ""
            msgs.append(f"{t.task_key} {life_and_result(t)[1]}: {err or (t.state.state_message if t.state else '')}")
        if msgs:
            out[pool] = "; ".join(msgs)
    return out


def claims_of(records: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """Per shard, the runs the runners' records hold of it: one per shard when the claims were exclusive."""
    out: Dict[str, List[dict]] = {}
    for recs in records.values():
        for r in recs:
            for x in r.get("shards", []):
                out.setdefault(x["slug"], []).append(dict(x, runner=r.get("runner")))
            if r.get("current") and not r.get("done"):  # the chunk a runner was on when its task ended (a stop, a cancel)
                out.setdefault(r["current"], []).append({"slug": r["current"], "runner": r.get("runner"), "start": None, "end": None})
    return out


def collect(w, plan: dict, runs: Dict[str, int], local_out: Path, records: Optional[Dict[str, List[dict]]] = None,
            fetched: Optional[set] = None) -> List[dict]:
    """Download each shard's files (not again for one ``fetched`` live), and read its verdict; one row per shard, in queue order. A
    shard with no result takes its reason from the runner that ran it, or (never claimed) from the tasks that did not succeed, or
    from the fail-fast rule that stopped the run."""
    rows = []
    claims = claims_of(records or {})
    failed_tasks = task_failures(w, runs)
    stopped = plan.get("stopped_early") or ""
    smokes = {spec.get("smoke") for spec in plan["pools"].values() if spec.get("smoke")}
    for slug, s in plan["shards"].items():
        d = local_out / slug
        mine = claims.get(slug, [])
        if fetched and slug in fetched and d.is_dir():
            got = sorted(p.name for p in d.iterdir())
        elif stopped and not mine:  # the stop left it unrun: there are no files to fetch
            got = []
        else:
            got = mut.download_dir(w, f"{plan['stage']}/out/{slug}", d)
        res = json.loads((d / "result.json").read_text()) if (d / "result.json").exists() else None
        junit = None
        if (d / "junit.xml").exists():
            try:
                junit = parse_junit(d / "junit.xml")
            except ET.ParseError:
                junit = None
        verdict = shard_verdict(res, junit)
        reason = (res or {}).get("reason", "")
        if res is None:
            if mine:
                reason = mine[-1].get("reason") or f"runner {mine[-1]['runner']} ran it, but it left no result"
                if stopped and not mine[-1].get("reason"):
                    reason = f"stopped early ({stopped}) while it ran"
            elif stopped:
                reason = f"not run: stopped early ({stopped})"
            else:
                reason = "never claimed: " + (failed_tasks.get(s["pool"]) or "the runners ended before it")
        if len(mine) > 1:
            reason = f"claimed {len(mine)} times (by {', '.join(str(x['runner']) for x in mine)}): the queue's claim was not exclusive. " + reason
        rows.append({
            "name": s["name"], "slug": slug, "pool": s["pool"], "verdict": verdict, "expected_min": s["expected_min"],
            "passed": (junit or {}).get("passed", (res or {}).get("passed", 0)),
            "failed": (junit or {}).get("failed", (res or {}).get("failed", 0)),
            "errors": (junit or {}).get("errors", (res or {}).get("errors", 0)),
            "skipped": (junit or {}).get("skipped", (res or {}).get("skipped", 0)),
            "wall_s": (res or {}).get("wall_s", "-"), "reason": reason, "claims": len(mine), "est_src": s.get("est_src", "ci"),
            "subset": bool(s.get("subset")),
            "actual_s": int(mine[-1]["end"] - mine[-1]["start"]) if mine and mine[-1].get("end") is not None else None,
            "isolation": (res or {}).get("isolation"), "maxfail_hit": bool((res or {}).get("maxfail_hit")), "smoke": slug in smokes,
            "failures": (junit or {}).get("failures", []), "skip_reasons": (junit or {}).get("skip_reasons", Counter()),
            "phases": (res or {}).get("phases"), "compute": (res or {}).get("compute"),
        })
        print(f"  downloaded {len(got)} file(s) for {s['name']}", flush=True)
    return rows


if __name__ == "__main__":
    sys.exit(main())
