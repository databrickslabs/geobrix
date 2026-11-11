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
from typing import Dict, List, Optional

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
STATUS_EVERY_S = 150
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
    p.add_argument("--pip-extra", default="")
    p.add_argument("--env", action="append", default=[], metavar="KEY=VAL", help="an environment variable every shard sets (repeatable)")
    p.add_argument("--spark-conf", action="append", default=[], metavar="KEY=VAL", help="a Spark conf every shard's session sets (repeatable)")
    p.add_argument("--run-id", default="")
    p.add_argument("--keep-stage", action="store_true")
    p.add_argument("--explain-only", action="store_true")
    p.add_argument("--allow-broad", action="store_true", help="let `affected` select over a quarter of the light bank")
    return p.parse_args(argv)


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
    slots = a.max_parallel or ts.slots_for(mut.cores_of(a.node_type) or 32)
    for name, items in by_pool.items():
        jar_pool = bool(any(sh.pool == ts.POOL_JAR for sh in items) and not a.serverless)
        kind = "serverless" if name == "serverless" else ("jar" if jar_pool else "light")
        pools[name] = {"jar": jar_pool, "table": queue_table(volroot, run_id, kind)}
        size_pool(pools[name], {s.slug: s.expected_min for s in items}, slots, max(1, a.clusters), a.shard_cap_min, {s.slug for s in items if s.heavy})
    return {
        "run_id": run_id, "sha": sha, "purpose": " ".join(a.purpose.split()), "stage": stage, "caps": caps,
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
                "name": s.name, "kind": s.kind, "args": ts.pytest_args(s), "env": {**ts.shard_env(s), **extra_env(a)}, "spark_conf": spark_confs(a), "expected_min": round(s.expected_min, 1),
                "est_src": s.est_src or "ci", "heavy": bool(s.heavy), "unit_max_min": round(s.unit_max_min, 1), "subset": bool(s.subset),
                "solo_min": round(s.solo_min or s.expected_min, 1),
                "contention": round(s.expected_min / s.solo_min, 2) if s.solo_min else 1.0,
                "unit_solo_s": unit_solo(s, root, fsolo or {}),
                "stall_min": ts.stall_min_for(s.unit_max_min, a.stall_min, a.shard_cap_min),
                "files": [] if s.oracle_tables else (ts.files_of(s, root) or []),
                "tables": [t for t in s.oracle_tables.split(",") if t],
                "cwd": "." if s.kind == "scripts" else ts.PKG, "pool": "serverless" if a.serverless else pool_of(s, a),
            }
            for s in shards
        },
    }


SRC_MARK = {"history": "h", "partial": "p", "lower bound": "lb", "ci": "ci"}


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


def size_pool(spec: dict, expected: Dict[str, float], slots: int, n_clusters: int, shard_cap_min: float, heavy: Optional[set] = None) -> None:
    """Fill a pool's plan: the queue (longest expected first), the runners (clusters x slots, never more than the shards), and the
    expected wall: the longest-first schedule onto the slot that frees first, which is what the runners do. The cap is the
    runner task's timeout: every shard of the busiest runner at the shard cap, plus one more when the shards outnumber the runners.
    ``heavy`` names the chunks that run one at a time per cluster: the wall waits for them in turn, and the cap allows for it."""
    queue = sr.queue_order(expected)
    runners = max(1, min(slots * n_clusters, len(queue)))
    heavy = {s for s in (heavy or ()) if s in expected}
    est = [ts.Shard(name=slug, kind="light", est_min=m) for slug, m in expected.items()]
    load = [sum(s.expected_min for s in lane) for lane in ts.lanes(est, runners)]
    wall = max(load)
    per_runner = math.ceil(len(queue) / runners) + (1 if len(queue) > runners else 0)
    cap = per_runner * (shard_cap_min + 10)
    if heavy:
        wall = max(wall, schedule_wall(expected, queue, heavy, runners, n_clusters))
        cap += math.ceil(len(heavy) / max(1, n_clusters)) * (shard_cap_min + 10)  # a runner may wait out the heavy chunks of its cluster
    spec.update(
        queue=queue, slots=slots, clusters=n_clusters, runners=runners, heavy=[s for s in queue if s in heavy],
        serial_min=round(sum(expected.values()), 1), longest_min=round(max(expected.values()), 1),
        expected_min=round(wall, 1), cap_min=cap,
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
                     + ", ".join(f"{plan['shards'][s]['name']} (~{plan['shards'][s]['expected_min']:g} {SRC_MARK.get(plan['shards'][s].get('est_src'), 'ci')}"
                                 + (", heavy" if plan["shards"][s].get("heavy") else "")
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
    lines.append(f"  results: {plan['local_out']}/ (junit.xml + pytest.log per shard, summary.md)")
    for s in local_only:
        lines.append(f"  local only: {s.name}: {s.local_only}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------- the runner notebook


def header_md(plan: dict, pool: str) -> str:
    spec = plan["pools"][pool]
    c = plan["caps"]
    return (
        f"**Test shards `{plan['run_id']}`** (commit `{plan['sha'][:10]}`): {plan['purpose']}  \n"
        f"**Output:** per shard, `junit.xml` and `pytest.log` in `{plan['stage']}/out/<shard>/` (the launcher downloads them); "
        f"the verdict per shard is passed / failed / errors / skipped, and the notebook's result is that count as JSON.  \n"
        f"**Time:** each shard about {min(plan['shards'][x]['expected_min'] for x in spec['queue']):g}-{max(plan['shards'][x]['expected_min'] for x in spec['queue']):g} min "
        f"(recorded or CI's times); {spec['runners']} runner(s) take the shards from one queue, longest first; the run about {spec['expected_min']} min after the start, "
        f"at most {spec['cap_min']} min.  \n"
        f"**Monitoring:** caps: {c['shard_min']:g} min per shard, {c['stall_min']:g} min without a finished test (the shard then dumps its stacks and stops; its runner "
        f"takes the next); heartbeats `{plan['stage']}/hb/<shard>.json` and `{plan['stage']}/runners/<runner>.json` (the launcher cancels a run whose shards are all stale). "
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
    return '''import faulthandler, json, os, shutil, signal, sys, tarfile, threading, time, _thread

STAGE = %(stage)r
SLUG = dbutils.widgets.get("shard")
PLAN = json.load(open(STAGE + "/plan.json"))
S = PLAN["shards"][SLUG]
CAPS = PLAN["caps"]
STALL_MIN = S.get("stall_min") or CAPS["stall_min"]  # scaled to the chunk's longest file or table (see _test_shards.stall_min_for)
t0 = time.time()
try:  # the node's local disk; Serverless has none to write
    os.makedirs("/local_disk0/gbxtest", exist_ok=True)
    scratch = "/local_disk0/gbxtest"
except OSError:  # a temp dir of this process's own: the runner's notebook runs as another user there
    import tempfile

    scratch = tempfile.mkdtemp(prefix="gbxtest-")
root = scratch + "/%%s/%%s" %% (PLAN["run_id"], SLUG)
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root + "/out")
shutil.copy(PLAN["tarball"], root + "/src.tar.gz")  # one read of the Volume file, then local disk
with tarfile.open(root + "/src.tar.gz") as tf:
    tf.extractall(root + "/repo", filter="data")
repo = root + "/repo"
if PLAN.get("resources"):  # the test resources, shared by every run of the same content
    shutil.copy(PLAN["resources"]["volume"], root + "/res.tar.gz")
    with tarfile.open(root + "/res.tar.gz") as tf:
        tf.extractall(repo, filter="data")
pkg = repo + "/python/geobrix"
# some tests ask git for the checkout's root, HEAD or its files (git ls-files, ls-tree): the tarball has no history, so make it
# a one-commit repository that holds every file, as the commit under test does
os.system("cd %%s && git init -q && git add -A -f && git -c user.name=gbx -c user.email=gbx@example.com commit -q -m run >/dev/null 2>&1" %% repo)
if PLAN.get("jar"):  # the heavy tests look for the JAR in the checkout's lib/ (the cluster has it installed as a library as well)
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
# On a cluster `databricks` is a regular package of the runtime whose path was fixed when the REPL started: add the sitedir's.
import databricks, pkgutil, filecmp
databricks.__path__ = pkgutil.extend_path(databricks.__path__, "databricks")
import databricks.labs.gbx as _gbx
assert os.path.realpath(_gbx.__file__).startswith(os.path.realpath(sitedir)) and filecmp.cmp(_gbx.__file__, pkg + "/src/databricks/labs/gbx/__init__.py", shallow=False), "the package under test is not the checkout's: " + _gbx.__file__
# The checkout wins, in the driver AND in the Python workers: every .py file of the imported package is the checkout's (the wheel's
# _build_info.py aside); the shard fails here, naming the differing files, rather than test the wheel's older copy
WHEEL_GUARD_TREE = run_guard({"tree": src_pkg}, spark=spark, exclude=("_build_info.py",))
os.chdir(repo + "/" + S["cwd"])
os.environ.update(S["env"])
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
import pytest

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

state = {"passed": 0, "failed": 0, "errors": 0, "skipped": 0, "total": 0, "last": "", "last_t": time.time(), "reason": "", "watchdog": "", "done": False}
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

class Counter:
    def pytest_collection_finish(self, session):
        state["total"] = len(session.items); state["last_t"] = time.time(); beat()
    def pytest_runtest_logstart(self, nodeid, location):
        state["last_t"] = time.time()  # a test that starts counts as progress; only a test that outlasts stall_min is a stall

    def pytest_runtest_logreport(self, report):
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

def watchdog():
    n = 0
    while not state["done"]:
        time.sleep(15)
        beat()
        n += 1
        if n %% 4 == 0:
            keep_log()
        why = ""
        if time.time() - t0 > CAPS["shard_min"] * 60:
            why = "shard cap %%g min" %% CAPS["shard_min"]
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

# Isolation probe: a shard before this one in the same runner leaves a temp view and a conf in its own session. Seeing
# neither here shows this shard has a session (and a Python process) of its own, as a job task of its own had.
iso = {"pid": os.getpid()}
try:
    iso["view_fresh"] = not spark.catalog.tableExists("gbx_isolation_probe")
    iso["conf_fresh"] = spark.conf.get("gbx.isolation.probe", "") == ""
except Exception as exc:  # the probe never decides a shard
    iso["error"] = str(exc)[:200]
for _k, _v in S.get("spark_conf", {}).items():  # --spark-conf: set before the tests, in this shard's own session
    spark.conf.set(_k, _v)
print("shard", S["name"], "| args", S["args"], "| env", S["env"], "| spark conf", S.get("spark_conf", {}), "| cores", os.cpu_count(), "| session", type(spark).__name__, "| isolation", iso, flush=True)
args = S["args"] + ["-p", "no:cacheprovider", "-v", "--tb=short", "-rfE", "--durations=15", "--basetemp=" + root + "/tmp",
                    "--junitxml=" + root + "/out/junit.xml"]
try:
    rc = int(pytest.main(args, plugins=[Counter()]))
except BaseException as exc:  # an interrupt from the watchdog, or a crash of the harness itself
    state["reason"] = state["reason"] or "%%s: %%s" %% (type(exc).__name__, exc)
    rc = 130
state["done"] = True
sys.stdout, sys.stderr = sys.stdout.a, sys.stderr.a
log.close()
try:  # what the next shard's probe looks for
    spark.range(1).createOrReplaceTempView("gbx_isolation_probe")
    spark.conf.set("gbx.isolation.probe", SLUG)
except Exception as exc:
    iso["error"] = str(exc)[:200]
result = dict(shard=S["name"], slug=SLUG, rc=rc, wall_s=round(time.time() - t0), reason=state["reason"], pid=os.getpid(), isolation=iso,
              passed=state["passed"], failed=state["failed"], errors=state["errors"], skipped=state["skipped"], total=state["total"])
json.dump(result, open(root + "/out/result.json", "w"))
for f in os.listdir(root + "/out"):
    shutil.copy(root + "/out/" + f, OUT + "/" + f)
beat()
print("shard rc:", rc, "after", result["wall_s"], "s", flush=True)
dbutils.notebook.exit(json.dumps(result))
''' % {"stage": plan["stage"]}


RUNNER_DRIVER = '''
# ---- the runner task: seed the queue (role "seed"), or claim and run shards until it is empty (role "run")
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
        res = run_shard(lambda: dbutils.notebook.run(SHARD_NB, TIMEOUT_S, {"shard": slug}), TIMEOUT_S,
                        watchdog=lambda: heartbeat_watchdog(STAGE + "/hb/" + slug + ".json"))
        print("+%ds" % (time.time() - T0), WHO, "ran", PLAN["shards"][slug]["name"], "rc", res.get("rc"), res.get("reason") or "", flush=True)
        return res

    HEAVY = SPEC.get("heavy") or []
    q = DeltaQueue(sql, TABLE, heavy=HEAVY)
    # the runners of one cluster share the node's disk: one lock there lets one heavy chunk run at a time
    try:  # the node's local disk; Serverless has none to write
        os.makedirs("/local_disk0/gbxtest", exist_ok=True)
        scratch = "/local_disk0/gbxtest"
    except OSError:  # a temp dir of this runner's own: a shard notebook there runs as another user
        scratch = tempfile.mkdtemp(prefix="gbxtest-")
    gate = HeavyGate(scratch + "/" + PLAN["run_id"] + "-" + POOL + "/heavy.lock") if HEAVY else None
    rec = run_loop(q, WHO, execute, offset=INDEX, on_change=save, heavy=HEAVY, gate=gate)
    rec["conflicts"] = q.conflicts
    save(rec)
    print(WHO, "ran", len(rec["shards"]), "shard(s);", q.conflicts, "lost claim commit(s); the queue is empty", flush=True)
    if rec["error"]:
        raise RuntimeError(rec["error"])
    dbutils.notebook.exit(json.dumps({"runner": WHO, "shards": len(rec["shards"])}))
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


def build_notebook(plan: dict, pool: str) -> dict:
    cells = [cell(header_md(plan, pool), "markdown")]
    if not plan["serverless"]:  # a Serverless job installs from the environment spec; %pip is not allowed there
        cells.append(cell(install_source(plan)))
    if plan["wheel"]["volume"]:
        cells.append(cell(guard_source(plan)))
    cells.append(cell(run_source(plan)))
    return {"cells": cells, "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}


# ---------------------------------------------------------------------------------------------- results


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
    """ok | FAILED | ERRORS (pytest stopped on a collection or usage error) | INCOMPLETE (no result, a watchdog stop or a
    cancel: the shard did not finish) | EMPTY (nothing collected)."""
    if not res:
        return "INCOMPLETE"
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
    """Whether a shard's row fails the run. A part of a split shard that holds only tests the marker deselects (integration files)
    collects nothing; CI's whole shard would pass, so that part does too. A whole shard that collects nothing still fails."""
    if row["verdict"] == "ok":
        return False
    return not (row["verdict"] == "EMPTY" and (row["name"].endswith("]") or row.get("subset")))


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
    for pool, info in pool_info.items():
        out.append(f"- {pool}: cluster(s) {info.get('cluster_id', '-')} ({info.get('node_type', '-')}), job run {info.get('job_run', '-')}; {speedup_line(info)}")
        out += ["  - " + x.strip() for x in schedule_lines(info)]
        if info.get("peaks"):
            out.append("  - peak concurrency: " + ", ".join(f"cluster {c}: {n} chunk(s) at once" for c, n in sorted(info["peaks"].items())))
    if isolation_line(rows):
        out.append(f"- {isolation_line(rows)}")
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
    skips = [r for r in rows if r["skip_reasons"]]
    if skips:
        out += ["", "## Skipped (top reasons per shard)", ""]
        for r in skips:
            out.append(f"- {r['name']}: " + "; ".join(f"{n} x {m or '(no reason)'}" for m, n in r["skip_reasons"].most_common(4)))
    return "\n".join(out) + "\n"


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
    if "affected" in a.shards:
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
        shards = ts.select(all_shards, a.shards, changed, REPO_ROOT, aff_shards)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    split_notes: List[str] = []
    if not a.clusters and not (a.explain_only or a.serverless or a.fresh_cluster):
        a.clusters = planned_clusters(a)
    hist = history_for(REPO_ROOT / a.timings)
    plan_slots = a.max_parallel or ts.slots_for(mut.cores_of(a.node_type) or 32)
    fsec = tm.seconds(hist, plan_slots)  # per file / per oracle table, at this run's slots, times its contention factor
    fsolo = tm.seconds(hist, plan_slots, adjust=False)  # the same without the factors: what the next run's ratios compare with
    hot = tm.contended(hist)  # contention-heavy units: they join the heavy gate
    lower = tm.lower_bounds(hist, plan_slots)
    if a.split == "auto" and shards:
        n_slots = (a.max_parallel or ts.slots_for(mut.cores_of(a.node_type) or 32)) * max(1, a.clusters)
        target = a.target_min or ts.auto_target_min(shards, REPO_ROOT, fsec, n_slots)
        shards = ts.auto_split(shards, REPO_ROOT, fsec, target, lower, hot)
        split_notes.append(f"{len(shards)} chunks cut to about {target:g} min for {n_slots} slot(s)")
    elif shards:
        shards = [ts.annotate(ts.Shard(**{**s.__dict__, "est_min": round(e, 2), "est_src": src}), REPO_ROOT, fsec, hot)
                  for s in shards for e, src in [ts.estimate_src(s, REPO_ROOT, fsec, lower)]]
    shards = [ts.Shard(**{**s.__dict__, "solo_min": round(min(s.expected_min, ts.estimate(s, REPO_ROOT, fsolo)), 2)}) for s in shards]
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
    for i in range(spec["runners"]):
        tasks.append(task(f"r_{i}", {"role": "run", "runner": f"r{i}", "index": str(i), "shard_nb": nb_paths["shard"]},
                          int(spec["cap_min"] * 60), i, depends=["q_seed"]))
    return tasks


def submit_pool_job(w, plan: dict, pool: str, nb_paths: dict, leases: list):
    """Submit the pool's job (``job_tasks``); the run id. A runner depends on the seed with ALL_SUCCESS: no queue, no runner."""
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
    waiter = submit_once(w, run_name=name, timeout_seconds=int(plan["caps"]["start_min"] * 60 + spec["cap_min"] * 60), tasks=tasks, **extra)
    return waiter.run_id


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
    """Size the pool's runners again for the clusters actually leased and the cores they have (the queue stays as it is)."""
    spec = plan["pools"][pool]
    before = list(spec["queue"])
    size_pool(spec, {slug: plan["shards"][slug]["expected_min"] for slug in before}, a.max_parallel or ts.slots_for(cores), n_clusters, a.shard_cap_min,
              {slug for slug in before if plan["shards"][slug].get("heavy")})
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
    per, peaks = tm.neighbours(records or {}, {pool: spec.get("clusters", 1) for pool, spec in plan["pools"].items()})
    samples, cont = [], []
    for r in rows:
        chunk = plan["shards"].get(r["slug"], {})
        slots = plan["pools"].get(chunk.get("pool"), {}).get("slots", 1)
        junit = local_out / r["slug"] / "junit.xml"
        act = actual_s(r, busy)
        samples += tm.run_samples(chunk, r["verdict"], act, junit, REPO_ROOT, slots, fn_table)
        near = per.get(r["slug"], {})
        solo = float(chunk.get("solo_min") or chunk.get("expected_min") or 0.0)
        r.update(x=round(act / (solo * 60), 2) if act and solo else None, nb=near.get("nb", 0), peak=near.get("peak", 0))
        if r["verdict"] in ("ok", "FAILED", "INCOMPLETE"):
            cont += tm.contention_records(chunk, act, tm.key_seconds(junit, REPO_ROOT, chunk, fn_table), near.get("nb", 0), near.get("peak", 0),
                                          incomplete=r["verdict"] == "INCOMPLETE")
    got = tm.merge(path, samples, {"run": plan["run_id"], "sha": plan["sha"][:10],
                                   "slots": max((p.get("slots", 1) for p in plan["pools"].values()), default=1)}, cont)
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
    import base64

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
                    pool_leases = []
                    if pool != "serverless":
                        pool_leases = lease_many(stack, w, a, plan, pool, volroot=volroot, me=me)
                        cores = min(mut.cores_of(w.clusters.get(l.cluster_id).node_type_id or "") or 32 for l in pool_leases)
                        relane(plan, pool, a, len(pool_leases), cores)
                        put_text(w, plan["plan_file"], json.dumps(plan))  # the runners read the pool's queue and table from it
                        pool_info[pool] = {"cluster_id": ", ".join(l.cluster_id for l in pool_leases), "node_type": w.clusters.get(pool_leases[0].cluster_id).node_type_id}
                    spec = plan["pools"][pool]
                    print(f"  {pool}: {len(spec['queue'])} shard(s) on {spec['clusters']} cluster(s) x {spec['slots']} slots = {spec['runners']} runner(s); expected wall about "
                          f"{spec['expected_min']:g} min (longest shard {spec['longest_min']:g}, serial sum {spec['serial_min']:g})", flush=True)
                    paths = {"shard": f"{base_dir}/tests_{run_id}_{pool}.ipynb", "runner": f"{base_dir}/runner_{run_id}_{pool}.ipynb"}
                    for kind, nb in (("shard", build_notebook(plan, pool)), ("runner", build_runner_notebook(plan, pool))):
                        body = base64.b64encode(json.dumps(nb, indent=1).encode()).decode()
                        w.workspace.import_(paths[kind], format=ImportFormat.JUPYTER, content=body, overwrite=True)
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
                print(f"  heartbeats: {stage}/hb/ and {stage}/runners/; cancel: " + "; ".join(f"databricks jobs cancel-run {r}" for r in runs.values()), flush=True)
                rc = watch(w, runs, plan, pool_info)
                finished.update(runs)
                records = runner_records(w, plan, local_out)
                rows = collect(w, plan, runs, local_out, records)
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


def watch(w, runs: Dict[str, int], plan: dict, pool_info: dict) -> int:
    """Poll the pool jobs and the shard heartbeats until every job is over. One status line per pool about every 2-3 min.
    Cancels the runs of the pool whose shards are all stale (10 min), or none has begun in start_min."""
    c = plan["caps"]
    t0 = time.monotonic()
    last_say = 0.0
    over: Dict[str, str] = {}
    last_beat: Dict[str, float] = {}
    changed: Dict[str, dict] = {}
    rc = 0
    while len(over) < len(runs):
        time.sleep(POLL_S)
        now = time.monotonic()
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
            recs = [r for r in (mut.read_json(w, f"{plan['stage']}/runners/{pool}/r{i}.json") for i in range(spec["runners"])) if r]
            current = [r["current"] for r in recs if r.get("current") and not r.get("done")]  # the shards the runners are on now
            beats = {}
            for slug in current:
                b = mut.read_json(w, f"{plan['stage']}/hb/{slug}.json")
                if b:
                    beats[slug] = b
            ran = [x for r in recs for x in r.get("shards", [])]
            started = bool(beats or ran)
            if say:
                tot = sum(x.get(k) or 0 for x in ran for k in ("passed", "failed", "skipped")) + sum(b.get("passed", 0) + b.get("failed", 0) + b.get("skipped", 0) for b in beats.values())
                bad = sum(x.get(k) or 0 for x in ran for k in ("failed", "errors")) + sum(b.get("failed", 0) + b.get("errors", 0) for b in beats.values())
                live = sum(1 for r in recs if not r.get("done"))
                print(f"  [{pool}] {life} {int(now - t0)} s: {live} of {spec['runners']} runner(s) at work, {len(current)} shard(s) running, {len(ran)}/{len(spec['queue'])} shards done, "
                      f"{len(spec['queue']) - len(ran) - len(current)} queued, {tot} tests finished, {bad} failing/erroring", flush=True)
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
    return out


def collect(w, plan: dict, runs: Dict[str, int], local_out: Path, records: Optional[Dict[str, List[dict]]] = None) -> List[dict]:
    """Download each shard's files, and read its verdict; one row per shard, in queue order. A shard with no result takes its
    reason from the runner that ran it, or (never claimed) from the tasks that did not succeed."""
    rows = []
    claims = claims_of(records or {})
    failed_tasks = task_failures(w, runs)
    for slug, s in plan["shards"].items():
        d = local_out / slug
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
        mine = claims.get(slug, [])
        if res is None:
            if mine:
                reason = mine[-1].get("reason") or f"runner {mine[-1]['runner']} ran it, but it left no result"
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
            "isolation": (res or {}).get("isolation"),
            "failures": (junit or {}).get("failures", []), "skip_reasons": (junit or {}).get("skip_reasons", Counter()),
        })
        print(f"  downloaded {len(got)} file(s) for {s['name']}", flush=True)
    return rows


if __name__ == "__main__":
    sys.exit(main())
