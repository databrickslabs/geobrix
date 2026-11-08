"""gbx:test:databricks -- run GeoBrix's test shards on warm Databricks pool clusters, side by side.

    push_and_run_tests_on_cluster.py --profile oauth-fe --purpose "why this run" \\
        [--shards all-light|all-heavy|affected|<name,name>] [--sha REV] [--max-parallel 5] [--explain-only]

The shards are CI's: the `light` matrix of .github/workflows/build_main.yml, `scripts/ci`, and one heavy shard per test dir
the light tier leaves out. They are read from those files on every run (examples/tests/_test_shards.py), so CI and a
cluster run cannot drift. The Scala suites stay local (`gbx:test:scala`); a cluster has no Maven build.

Each shard is one task of a job on a leased pool cluster, so it runs in its own Python process; the tasks of a pool are
spread over at most --max-parallel lanes that run side by side. The session is the cluster's own (test/_provided_spark.py).
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
  --base <rev>           `affected`: the files changed between this and --sha pick the shards (default origin/streamline/0.6.0)
  --sha <rev>            the commit to test (default HEAD)
  --wheel <path>         the env6 wheel (default dist/env6/); it carries dependencies and native payload, not the code under test
  --jar-path <path>      the JAR of the heavy shards (default python/geobrix/lib/; copied out and sha-checked before use)
  --max-parallel <n>     shards run side by side per pool (default 5; one 32 vCPU node)
  --serverless           run the selected light shards on Serverless env 6 (a fidelity leg; Spark-free dirs only)
  --fresh-cluster        use a new cluster that ends with the run, not the pools
  --pool-light <name>    default gbx-test-1n          --pool-jar <name>  default gbx-test-1n-jar
  --node-type <t>        node of a pool cluster made by this run (default m5d.8xlarge, 32 vCPU)
  --shard-cap-min <n>    stop a shard after this long (default 45); its results so far are kept
  --stall-min <n>        stop a shard when no test finished for this long (default 10)
  --start-cap-min <n>    cancel when no shard has beaten this long after the submit (default 25)
  --pool-wait-min <n>    wait this long for a free pool cluster (default 30)
  --pip-extra "<reqs>"   more pip requirements the tests import
  --run-id <id>          default tst-<date>-<time>-<4 hex>
  --keep-stage           leave the Volume folder and the runner notebook
  --explain-only         print the plan and exit; no network, nothing staged or started
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
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
import _test_shards as ts  # sibling module: the shards, read from CI's files
import push_and_run_mutation_on_cluster as mut  # sibling module: staging, download and wheel helpers the guidance run uses

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = mut.REPO_ROOT
DEFAULT_NODE = "m5d.8xlarge"
SPARK_VERSION = mut.SPARK_VERSION
PYTEST_PIN = mut.PYTEST_PIN
EXTRAS = "stac,vizx,zarr,databricks"  # the env wheel's feature extras the suites import
# What the tests read besides python/geobrix: docs, examples, scripts and the Scala test resources the checks point at.
SUBSET = ("python/geobrix", "scripts", "docs", "examples", "sample-data", "src/main", "README.md")
RESOURCES = "src/test/resources/binary"  # 117 MB of test rasters and NetCDF: staged once per content, not once per run
DEPS_INPUTS = ("python/geobrix/pyproject.toml", "python/geobrix/envs", "scripts/native")  # a change here wants a new wheel
EXIT_FAILED, EXIT_USAGE, EXIT_NO_START, EXIT_STALLED = 1, 2, 4, 3
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
    p.add_argument("--timings", default="test-logs/databricks-tests/timings.json")
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
    p.add_argument("--run-id", default="")
    p.add_argument("--keep-stage", action="store_true")
    p.add_argument("--explain-only", action="store_true")
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
    return out


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


def make_plan(a, sha: str, shards: List[ts.Shard], wheel: Optional[Path], jar: Optional[dict], volroot: str, root: Path, resources: Optional[dict] = None) -> dict:
    run_id = a.run_id
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
        n_clusters = max(1, a.clusters)
        lane_list = ts.lanes(items, slots * n_clusters)
        pools[name] = {
            "jar": bool(items and items[0].pool == ts.POOL_JAR and not a.serverless),
            "lanes": [[s.slug for s in lane] for lane in lane_list],
            "slots": slots, "clusters": n_clusters,
            "serial_min": round(sum(s.expected_min for s in items), 1),
            "longest_min": round(max(s.expected_min for s in items), 1),
            "expected_min": round(max(sum(s.expected_min for s in lane) for lane in lane_list), 1),
            "cap_min": max(len(lane) for lane in lane_list) * (a.shard_cap_min + 10),
        }
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
                "name": s.name, "kind": s.kind, "args": ts.pytest_args(s), "env": ts.shard_env(s), "expected_min": round(s.expected_min, 1),
                "cwd": "." if s.kind == "scripts" else ts.PKG, "pool": "serverless" if a.serverless else pool_of(s, a),
            }
            for s in shards
        },
    }


def explain(plan: dict, local_only: List[ts.Shard], notes: List[str]) -> str:
    c = plan["caps"]
    lines = [f"gbx:test:databricks plan {plan['run_id']} for commit {plan['sha'][:10]}: {plan['purpose'] or '(no purpose given)'}"]
    for n in notes:
        lines.append(f"  NOTE: {n}")
    for pool, spec in plan["pools"].items():
        where = "Serverless env 6 (no cluster)" if pool == "serverless" else f"pool {pool} ({'with' if spec['jar'] else 'no'} JAR, single node {plan['node_type']})"
        if plan["fresh_cluster"] and pool != "serverless":
            where = f"a new single-node cluster {plan['node_type']} ({'with' if spec['jar'] else 'no'} JAR) that ends with the run"
        n = sum(len(x) for x in spec["lanes"])
        lines.append(f"  {where}: {n} task(s) on {spec['clusters']} cluster(s) x {spec['slots']} slots = {len(spec['lanes'])} lane(s); expected wall about "
                     f"{spec['expected_min']:g} min after the start (the longest task is {spec['longest_min']:g} min, the serial sum {spec['serial_min']:g} min), at most {spec['cap_min']:g} min")
        for i, lane in enumerate(spec["lanes"], 1):
            lines.append(f"    lane {i}: " + " -> ".join(f"{plan['shards'][s]['name']} (~{plan['shards'][s]['expected_min']:g} min)" for s in lane))
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
        f"**Time:** each shard about {min(s['expected_min'] for s in plan['shards'].values())}-{max(s['expected_min'] for s in plan['shards'].values())} min "
        f"(CI's times); the run about {spec['expected_min']} min after the start, at most {spec['cap_min']} min.  \n"
        f"**Monitoring:** caps: {c['shard_min']:g} min per shard, {c['stall_min']:g} min without a finished test (the shard then dumps its stacks and stops); "
        f"heartbeat `{plan['stage']}/hb/<shard>.json` (the launcher cancels a run whose shards are all stale). "
        f"Cancel: `databricks jobs cancel-run <run id> --profile <profile>` (the id is printed at submit)."
    )


def install_source(plan: dict) -> str:
    reqs = " ".join(f'"{r}"' for r in plan["pins"] + plan["pip_extra"].split())
    return f'%pip install --quiet "geobrix[{plan["extras"]}] @ file://{plan["wheel"]["volume"]}" {reqs}\n'


def run_source(plan: dict) -> str:
    """The cell that unpacks the checkout, runs one shard's pytest in this process and writes its files."""
    return '''import faulthandler, json, os, shutil, signal, sys, tarfile, threading, time, _thread

STAGE = %(stage)r
SLUG = dbutils.widgets.get("shard")
PLAN = json.load(open(STAGE + "/plan.json"))
S = PLAN["shards"][SLUG]
CAPS = PLAN["caps"]
t0 = time.time()
root = "/local_disk0/gbxtest/%%s/%%s" %% (PLAN["run_id"], SLUG)
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
# some tests ask git for the checkout's root or HEAD: the tarball has no history, so give it a one-commit repository
os.system("cd %%s && git init -q && git -c user.name=gbx -c user.email=gbx@example.com commit -q --allow-empty -m run >/dev/null 2>&1" %% repo)
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
shutil.copytree(pkg + "/src/databricks/labs/gbx", sitedir + "/databricks/labs/gbx", dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
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

state = {"passed": 0, "failed": 0, "errors": 0, "skipped": 0, "total": 0, "last": "", "last_t": time.time(), "reason": "", "done": False}
def beat():
    d = dict(state, ts=time.time(), elapsed_s=round(time.time() - t0), shard=S["name"])
    tmp = root + "/hb.json"
    json.dump(d, open(tmp, "w"))
    shutil.copy(tmp, HB)

class Counter:
    def pytest_collection_finish(self, session):
        state["total"] = len(session.items); state["last_t"] = time.time(); beat()
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
    while not state["done"]:
        time.sleep(15)
        beat()
        why = ""
        if time.time() - t0 > CAPS["shard_min"] * 60:
            why = "shard cap %%g min" %% CAPS["shard_min"]
        elif time.time() - state["last_t"] > CAPS["stall_min"] * 60:
            why = "no test finished for %%g min (last: %%s)" %% (CAPS["stall_min"], state["last"])
        if why:
            state["reason"] = why
            print("\\nWATCHDOG: " + why + "; stacks follow", flush=True)
            faulthandler.dump_traceback(file=log, all_threads=True); log.flush()
            beat()
            _thread.interrupt_main()
            time.sleep(90)
            if not state["done"]:
                os.kill(os.getpid(), signal.SIGKILL)
threading.Thread(target=watchdog, daemon=True).start()

print("shard", S["name"], "| args", S["args"], "| env", S["env"], "| cores", os.cpu_count(), "| session", type(spark).__name__, flush=True)
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
result = dict(shard=S["name"], slug=SLUG, rc=rc, wall_s=round(time.time() - t0), reason=state["reason"],
              passed=state["passed"], failed=state["failed"], errors=state["errors"], skipped=state["skipped"], total=state["total"])
json.dump(result, open(root + "/out/result.json", "w"))
for f in os.listdir(root + "/out"):
    shutil.copy(root + "/out/" + f, OUT + "/" + f)
beat()
print("shard rc:", rc, "after", result["wall_s"], "s", flush=True)
dbutils.notebook.exit(json.dumps(result))
''' % {"stage": plan["stage"]}


def cell(source: str, kind: str = "code") -> dict:
    c = {"cell_type": kind, "metadata": {}, "source": source.splitlines(keepends=True)}
    if kind == "code":
        c.update(outputs=[], execution_count=None)
    return c


def build_notebook(plan: dict, pool: str) -> dict:
    cells = [cell(header_md(plan, pool), "markdown")]
    if not plan["serverless"]:  # a Serverless job installs from the environment spec; %pip is not allowed there
        cells.append(cell(install_source(plan)))
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
    """ok | FAILED | ERRORS | INCOMPLETE (no result: the shard never finished) | EMPTY (nothing collected)."""
    if not res:
        return "INCOMPLETE"
    if res.get("rc") == 5 or (res.get("rc") == 0 and not res.get("total")):
        return "EMPTY"
    if res.get("rc") not in (0,) or res.get("reason"):
        return "FAILED" if (res.get("failed") or (junit and junit["failed"])) else "INCOMPLETE" if res.get("reason") or res.get("rc") in (2, 130) else "ERRORS"
    if res.get("errors") or res.get("failed"):
        return "FAILED"
    return "ok"


def pool_metrics(w, run_id: int, spec: dict) -> dict:
    """What the schedule did, from the start and end of each task: the chunks and slots, the longest task against the mean,
    when each lane ended (the tail is the last lane's end after the first lane's: slots that sat idle at the end), the
    ideal wall (the sum of the tasks over the lanes) and the slot time left unused."""
    try:
        tasks = {t.task_key: t for t in (w.jobs.get_run(run_id).tasks or [])}
    except Exception:  # noqa: BLE001
        return {}
    dur, lane_end, t_first = {}, [], None
    for lane in spec["lanes"]:
        ends = []
        for slug in lane:
            t = tasks.get(f"s_{slug}"[:100])
            if t is None or not t.start_time or not t.end_time:
                continue
            dur[slug] = (t.end_time - t.start_time) / 1000.0
            t_first = t.start_time if t_first is None else min(t_first, t.start_time)
            ends.append(t.end_time)
        if ends:
            lane_end.append(max(ends))
    if not dur or not lane_end:
        return {}
    lanes = len(spec["lanes"])
    span = (max(lane_end) - t_first) / 1000.0
    return {
        "chunks": len(dur), "lanes": lanes, "slots": spec["slots"], "task_longest_s": int(max(dur.values())),
        "task_mean_s": int(sum(dur.values()) / len(dur)), "task_sum_s": int(sum(dur.values())), "span_s": int(span),
        "ideal_s": int(sum(dur.values()) / lanes), "tail_s": int((max(lane_end) - min(lane_end)) / 1000.0),
        "idle_slot_s": int(max(0.0, lanes * span - sum(dur.values()))),
    }


def schedule_lines(info: dict) -> List[str]:
    if "chunks" not in info:
        return []
    return [
        f"  schedule: {info['chunks']} chunks on {info['lanes']} lane(s) ({info['slots']} slots per cluster); longest chunk {info['task_longest_s']} s, "
        f"mean {info['task_mean_s']} s (longest/mean {info['task_longest_s'] / max(1, info['task_mean_s']):.1f}x)",
        f"  first task to last end {info['span_s']} s; ideal (sum of tasks over the lanes) {info['ideal_s']} s; tail (last lane ended after the first) "
        f"{info['tail_s']} s; idle slot time {info['idle_slot_s']} s",
    ]


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
        out += [x.replace("  schedule", "  - schedule", 1).replace("  first task", "  - first task", 1) for x in schedule_lines(info)]
    if plan["jar"]:
        out.append(f"- JAR `{plan['jar']['name']}` sha1 {plan['jar'].get('sha1', '')[:12]} sha256 {plan['jar']['sha256'][:16]}")
    out += ["", "| shard | pool | verdict | passed | failed | errors | skipped | wall s | CI ~min |", "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        out.append(f"| {r['name']} | {r['pool']} | {r['verdict']} | {r['passed']} | {r['failed']} | {r['errors']} | {r['skipped']} | {r['wall_s']} | {r['expected_min']} |")
    tot = {k: sum(r[k] for r in rows if isinstance(r[k], int)) for k in ("passed", "failed", "errors", "skipped")}
    out.append(f"| **total** | | | {tot['passed']} | {tot['failed']} | {tot['errors']} | {tot['skipped']} | | |")
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


def table_text(rows: List[dict]) -> str:
    head = f"{'shard':<22} {'verdict':<10} {'passed':>7} {'failed':>6} {'errors':>6} {'skipped':>7} {'wall s':>7} {'CI ~min':>7}"
    lines = [head, "-" * len(head)]
    for r in rows:
        lines.append(f"{r['name']:<22} {r['verdict']:<10} {r['passed']:>7} {r['failed']:>6} {r['errors']:>6} {r['skipped']:>7} {r['wall_s']:>7} {r['expected_min']:>7}")
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


def stage_jar(path: Path, root: Path) -> dict:
    """Copy the JAR out of the tree it was built in (another build may replace it) and name it by its content."""
    import hashlib
    import shutil
    import tempfile

    work = Path(tempfile.mkdtemp(prefix="gbx-test-jar-"))
    local = work / path.name
    shutil.copy(path, local)
    sha256 = mut.sha256_file(local)
    sha1 = hashlib.sha1(local.read_bytes()).hexdigest()
    return {"local": str(local), "name": path.name, "sha256": sha256, "sha1": sha1, "dir": f"test-pool-jars/{sha256[:12]}"}


# ---------------------------------------------------------------------------------------------- the workspace side


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
    if "affected" in a.shards:
        changed = ts.changed_files(REPO_ROOT, a.base, sha)
    try:
        shards = ts.select(all_shards, a.shards, changed, REPO_ROOT)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    split_notes: List[str] = []
    if not a.clusters and not (a.explain_only or a.serverless or a.fresh_cluster):
        a.clusters = planned_clusters(a)
    timings = load_timings(REPO_ROOT / a.timings)
    plan_slots = a.max_parallel or ts.slots_for(mut.cores_of(a.node_type) or 32)
    fsec = ts.rescale_seconds(file_seconds_of(timings), int(timings.get("slots") or plan_slots), plan_slots)  # as with this run's slots
    if a.split == "auto" and shards:
        n_slots = (a.max_parallel or ts.slots_for(mut.cores_of(a.node_type) or 32)) * max(1, a.clusters)
        target = a.target_min or ts.auto_target_min(shards, REPO_ROOT, fsec, n_slots)
        shards = ts.auto_split(shards, REPO_ROOT, fsec, target)
        split_notes.append(f"{len(shards)} chunks cut to about {target:g} min for {n_slots} slot(s) ({'timings of ' + str(timings.get('run')) if fsec else 'no timings yet: the CI minutes spread over each shard'})")
    elif shards:
        shards = [ts.Shard(**{**s.__dict__, "est_min": round(ts.estimate(s, REPO_ROOT, fsec), 2)}) for s in shards]
    if not shards:
        print(f"No shard holds a test touched by the {len(changed or [])} file(s) changed between {a.base} and {sha[:10]}: nothing to run.")
        return 0
    if a.serverless and any(s.pool == ts.POOL_JAR for s in shards):
        print("ERROR: --serverless runs light shards only (the heavy shards need the JAR on a cluster)", file=sys.stderr)
        return EXIT_USAGE
    wheel = mut.find_wheel(a.wheel) if not a.explain_only or a.wheel or list(REPO_ROOT.glob("dist/env6/*.whl")) else None
    notes = split_notes + wheel_notes(wheel, sha) if wheel else split_notes + ["no env6 wheel found in dist/env6/ (a launch needs one: gbx:build:wheel --env 6, or --wheel)"]
    dirty = uncommitted()
    if dirty:
        notes.append(f"{len(dirty)} uncommitted path(s) under the staged subset are NOT in this run (it tests {sha[:10]}): "
                     + ", ".join(dirty[:5]) + (" ..." if len(dirty) > 5 else ""))
    volroot = _volroot()
    jar = None
    if any(s.pool == ts.POOL_JAR for s in shards) and not a.serverless:
        jar_src = Path(a.jar_path) if a.jar_path else next(iter(sorted((REPO_ROOT / "python/geobrix/lib").glob("geobrix-*-jar-with-dependencies.jar"))), None)
        if not jar_src or not jar_src.is_file():
            if a.explain_only:
                notes.append("no JAR found in python/geobrix/lib/ (the heavy shards need one: gbx:build:jar, or --jar-path)")
            else:
                print("PRECONDITION MISSING: no JAR for the heavy shards (python/geobrix/lib/ or --jar-path); heavy shards not run.", file=sys.stderr)
                return EXIT_USAGE
        else:
            jar = stage_jar(jar_src, REPO_ROOT)
            jar["volume"] = f"{volroot}/{jar['dir']}/{jar['name']}"
    resources = resources_entry(sha, volroot)
    plan = make_plan(a, sha, shards, wheel, jar, volroot, REPO_ROOT, resources)
    local_only = [s for s in all_shards if s.local_only]
    print(explain(plan, local_only, notes))
    if a.explain_only:
        print("\nexplain-only: nothing staged or started.")
        return 0
    if wheel is None:
        return EXIT_USAGE
    return launch(a, plan, shards)


def planned_clusters(a) -> int:
    """How many clusters the plan is cut for: the light pool's cap, else the clusters it has (the launch leases the free ones, starts
    stopped ones and makes new ones up to the cap, and spreads the lanes again over what it got). One when the pool cannot be read."""
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

    name = f"gbx-test-fresh-{plan['run_id']}"[:60] if a.fresh_cluster else pool_name
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


def submit_pool_job(w, plan: dict, pool: str, nb_path: str, leases: list):
    """One job for the pool: a task per shard, the tasks of a lane in a chain, the lanes side by side. Lane i runs on cluster
    i % len(leases), so each cluster carries about the same number of lanes. With no lease the tasks run on Serverless."""
    from databricks.sdk.service import compute, jobs
    from _job_submit import submit_once

    spec = plan["pools"][pool]
    cap_s = int(plan["caps"]["shard_min"] + 10) * 60
    tasks = []
    for li, lane in enumerate(spec["lanes"]):
        prev = None
        for slug in lane:
            kw = {}
            if prev:
                kw = dict(depends_on=[jobs.TaskDependency(task_key=prev)], run_if=jobs.RunIf.ALL_DONE)  # a failed shard must not skip the next
            if leases:
                kw["existing_cluster_id"] = leases[li % len(leases)].cluster_id
            else:
                kw["environment_key"] = "gbx_env6"
            tasks.append(jobs.SubmitTask(
                task_key=f"s_{slug}"[:100], max_retries=0, timeout_seconds=cap_s,
                description=f"{plan['shards'][slug]['name']}: {plan['purpose']} (commit {plan['sha'][:10]})"[:1000],
                notebook_task=jobs.NotebookTask(notebook_path=nb_path, source=jobs.Source.WORKSPACE, base_parameters={"shard": slug}), **kw))
            prev = f"s_{slug}"[:100]
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
    cfg = cp.read_config(w, volroot, pool_name_of(a, pool))
    want = a.clusters or (int(cfg["cap"]) if cfg else len(cp.members(w, pool)))  # up to the pool's cap: free ones first, then stopped, then new
    tried = 0
    while len(got) < want and tried < want * 2:
        tried += 1
        try:
            got.append(stack.enter_context(lease_pool(w, a, plan, pool, jar=jar, volroot=volroot, me=me, wait_min=0.05)))
        except cp.PoolError as exc:
            print(f"  pool {pool}: {len(got)} of {want} cluster(s) free ({str(exc)[:100]}); running on those", flush=True)
            break
    return got


def pool_name_of(a, pool: str) -> str:
    return f"gbx-test-fresh-{a.run_id}"[:60] if a.fresh_cluster else pool


def relane(plan: dict, pool: str, a, n_clusters: int, cores: int) -> None:
    """Spread the pool's shards again over the clusters actually leased and the cores they have."""
    shards = [ts.Shard(name=plan["shards"][slug]["name"], kind=plan["shards"][slug]["kind"], est_min=plan["shards"][slug]["expected_min"]) for lane in plan["pools"][pool]["lanes"] for slug in lane]
    by_slug = {sh.slug: sh for sh in shards}
    slots = a.max_parallel or ts.slots_for(cores)
    lane_list = ts.lanes(shards, slots * n_clusters)
    spec = plan["pools"][pool]
    spec.update(
        lanes=[[sh.slug for sh in lane] for lane in lane_list], slots=slots, clusters=n_clusters,
        expected_min=round(max(sum(sh.expected_min for sh in lane) for lane in lane_list), 1),
        cap_min=max(len(lane) for lane in lane_list) * (a.shard_cap_min + 10),
    )
    assert set(by_slug) == {slug for lane in spec["lanes"] for slug in lane}, "relane lost or invented a shard"


def load_timings(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def file_seconds_of(timings: dict) -> Dict[str, float]:
    return {k: float(v) for k, v in (timings.get("files") or {}).items()}


def write_timings(path: Path, plan: dict, rows: List[dict], local_out: Path) -> None:
    """Keep the seconds each test file took in this run (from the junit files) and each shard's wall time: the next run cuts
    and orders its tasks by them. Only shards that finished are taken; the latest run wins per file."""
    t = load_timings(path)
    shards = dict(t.get("shards") or {})
    run_sum: Dict[str, float] = {}
    for r in rows:
        f = local_out / r["slug"] / "junit.xml"
        if r["verdict"] in ("INCOMPLETE", "EMPTY") or not f.exists():
            continue
        try:
            cases = list(ET.parse(f).getroot().iter("testcase"))
        except ET.ParseError:
            continue
        for case in cases:
            mod = (case.get("classname") or "").split(".")
            for k in range(len(mod), 0, -1):  # the dotted module path, then a class when there is one: the file is the longest prefix
                cand = "/".join(mod[:k]) + ".py"
                if (REPO_ROOT / ts.PKG / cand).is_file() or (REPO_ROOT / cand).is_file():
                    run_sum[cand] = run_sum.get(cand, 0.0) + float(case.get("time") or 0)
                    break
        if isinstance(r["wall_s"], int):
            shards[r["name"]] = r["wall_s"]
    files = file_seconds_of(t)
    slots = max((spec.get("slots", 1) for spec in plan.get("pools", {}).values()), default=1)
    if (t.get("slots") or slots) != slots:  # files from runs at another slot count: bring the old ones to this run's
        files = ts.rescale_seconds(files, int(t["slots"]), slots)
    files.update({k: round(v, 2) for k, v in run_sum.items()})
    out = {"updated": time.strftime("%Y-%m-%d %H:%M:%S"), "run": plan["run_id"], "sha": plan["sha"][:10], "slots": slots,
           "files": {k: round(v, 2) for k, v in sorted(files.items())}, "shards": shards}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(out, indent=1))
    print(f"Timings: {len(run_sum)} file(s) from this run, {len(files)} in {path}")


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
    nbs: Dict[str, str] = {}
    rc = EXIT_FAILED
    ok = False
    pool_info: Dict[str, dict] = {}
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
            runs: Dict[str, int] = {}
            t_submit: Dict[str, float] = {}
            w.workspace.mkdirs(base_dir)
            for pool in plan["pools"]:
                pool_leases = []
                if pool != "serverless":
                    pool_leases = lease_many(stack, w, a, plan, pool, volroot=volroot, me=me)
                    cores = min(mut.cores_of(w.clusters.get(l.cluster_id).node_type_id or "") or 32 for l in pool_leases)
                    relane(plan, pool, a, len(pool_leases), cores)
                    pool_info[pool] = {"cluster_id": ", ".join(l.cluster_id for l in pool_leases), "node_type": w.clusters.get(pool_leases[0].cluster_id).node_type_id}
                spec = plan["pools"][pool]
                print(f"  {pool}: {sum(len(x) for x in spec['lanes'])} task(s) on {spec['clusters']} cluster(s) x {spec['slots']} slots; expected wall about "
                      f"{spec['expected_min']:g} min (longest task {spec['longest_min']:g}, serial sum {spec['serial_min']:g})", flush=True)
                nb_path = f"{base_dir}/tests_{run_id}_{pool}.ipynb"
                body = base64.b64encode(json.dumps(build_notebook(plan, pool), indent=1).encode()).decode()
                w.workspace.import_(nb_path, format=ImportFormat.JUPYTER, content=body, overwrite=True)
                nbs[pool] = nb_path
                print(f"Uploaded runner notebook {nb_path}", flush=True)
                job_run = submit_pool_job(w, plan, pool, nb_path, pool_leases)
                runs[pool] = job_run
                pool_info.setdefault(pool, {})["job_run"] = job_run
                t_submit[pool] = time.monotonic()
                for l in pool_leases:
                    l.job_run(job_run)
                try:
                    print(f"  {pool}: run {job_run}  {w.jobs.get_run(job_run).run_page_url}", flush=True)
                except Exception:  # noqa: BLE001
                    pass
            print(f"  heartbeats: {stage}/hb/; cancel: " + "; ".join(f"databricks jobs cancel-run {r}" for r in runs.values()), flush=True)
            rc = watch(w, runs, plan, pool_info)
            rows = collect(w, plan, runs, local_out)
            for pool, info in pool_info.items():
                walls = [r["wall_s"] for r in rows if r["pool"] == pool and isinstance(r["wall_s"], int)]
                info["longest_s"] = max(walls) if walls else "-"
                info["serial_s"] = sum(walls)
                info["wall_s"] = int(time.monotonic() - t_submit[pool]) if pool in t_submit else "-"
                info.update(pool_metrics(w, runs[pool], plan["pools"][pool]))
            write_timings(REPO_ROOT / a.timings, plan, rows, local_out)
            text = summary_md(plan, rows, pool_info)
            (local_out / "summary.md").write_text(text)
            print("\n" + table_text(rows))
            for pool, info in pool_info.items():
                print(f"{pool}: {speedup_line(info)}")
                for ln in schedule_lines(info):
                    print(ln)
            for r in rows:
                for n, m in r["failures"][:5]:
                    print(f"  FAIL [{r['name']}] {n}: {m}")
            print(f"\nWrote {local_out / 'summary.md'}")
            bad = [r for r in rows if r["verdict"] != "ok"]
            if rc == 0 and bad:
                rc = EXIT_FAILED
            ok = rc == 0
    finally:
        if a.keep_stage:
            print(f"Kept {stage} and {', '.join(nbs.values())} (--keep-stage).")
        else:
            mut.rm_volume_dir(w, stage)
            for pool, nb in nbs.items():
                if ok or rc in (EXIT_FAILED, EXIT_NO_START, EXIT_STALLED):
                    try:
                        w.workspace.delete(nb)
                    except Exception as exc:  # noqa: BLE001
                        if "does not exist" not in str(exc).lower():
                            print(f"WARNING: could not remove {nb} ({exc}); delete it by hand.", file=sys.stderr)
        try:
            Path(plan["jar"]["local"]).unlink() if plan["jar"] else None
        except OSError:
            pass
    return rc


def watch(w, runs: Dict[str, int], plan: dict, pool_info: dict) -> int:
    """Poll the pool jobs and the shard heartbeats until every job is over. One status line per pool about every 2-3 min.
    Cancels the runs of the pool whose shards are all stale (10 min), or none has begun in start_min."""
    c = plan["caps"]
    t0 = time.monotonic()
    last_say = 0.0
    over: Dict[str, str] = {}
    last_beat: Dict[str, float] = {}
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
            beats = {}
            for lane in plan["pools"][pool]["lanes"]:
                for slug in lane:
                    b = mut.read_json(w, f"{plan['stage']}/hb/{slug}.json")
                    if b:
                        beats[slug] = b
            running = [t for t in (run.tasks or []) if life_and_result(t)[0] == "RUNNING"]
            if say:
                tot = sum(b.get("passed", 0) + b.get("failed", 0) + b.get("skipped", 0) for b in beats.values())
                bad = sum(b.get("failed", 0) + b.get("errors", 0) for b in beats.values())
                done = sum(1 for b in beats.values() if b.get("done"))
                print(f"  [{pool}] {life} {int(now - t0)} s: {len(running)} task(s) running, {done}/{sum(len(x) for x in plan['pools'][pool]['lanes'])} shards done, "
                      f"{tot} tests finished, {bad} failing/erroring", flush=True)
            if not beats and now - t0 > c["start_min"] * 60:
                print(f"No shard of {pool} has beaten {c['start_min']:g} min after the submit. Cancelling run {rid}.", file=sys.stderr)
                _cancel(w, rid)
                rc = rc or EXIT_NO_START
            elif beats and running:
                stale = [b for b in beats.values() if not b.get("done") and time.time() - b.get("ts", 0) > 600]
                live = [b for b in beats.values() if not b.get("done") and time.time() - b.get("ts", 0) <= 600]
                if stale and not live and len(stale) >= len(running):
                    print(f"Every running shard of {pool} has a heartbeat older than 10 min: a stuck run. Cancelling run {rid}.", file=sys.stderr)
                    _cancel(w, rid)
                    rc = rc or EXIT_STALLED
        if say:
            last_say = now
    return rc


def _cancel(w, rid: int) -> None:
    try:
        w.jobs.cancel_run(rid)
    except Exception as exc:  # noqa: BLE001
        print(f"cancel FAILED: {exc}; cancel run {rid} by hand", file=sys.stderr)


def collect(w, plan: dict, runs: Dict[str, int], local_out: Path) -> List[dict]:
    """Download each shard's files, and read its verdict; one row per shard, in plan order."""
    rows = []
    task_state = {}
    for pool, rid in runs.items():
        try:
            for t in w.jobs.get_run(rid).tasks or []:
                task_state[t.task_key] = t
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: could not read run {rid}: {exc}", file=sys.stderr)
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
        t = task_state.get(f"s_{slug}"[:100])
        if res is None and t is not None:
            try:
                out = w.jobs.get_run_output(t.run_id)
                reason = (out.error or "")[:300] or (t.state.state_message if t.state else "") or "the shard produced no result"
            except Exception:  # noqa: BLE001
                reason = "the shard produced no result"
        rows.append({
            "name": s["name"], "slug": slug, "pool": s["pool"], "verdict": verdict, "expected_min": s["expected_min"],
            "passed": (junit or {}).get("passed", (res or {}).get("passed", 0)),
            "failed": (junit or {}).get("failed", (res or {}).get("failed", 0)),
            "errors": (junit or {}).get("errors", (res or {}).get("errors", 0)),
            "skipped": (junit or {}).get("skipped", (res or {}).get("skipped", 0)),
            "wall_s": (res or {}).get("wall_s", "-"), "reason": reason,
            "failures": (junit or {}).get("failures", []), "skip_reasons": (junit or {}).get("skip_reasons", Counter()),
        })
        print(f"  downloaded {len(got)} file(s) for {s['name']}", flush=True)
    return rows


if __name__ == "__main__":
    sys.exit(main())
