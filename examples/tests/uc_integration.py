#!/usr/bin/env python3
"""Run the Unity Catalog integration gate on Databricks and write its evidence file.

Run it through `gbx:uc:tests` (scripts/commands/gbx-uc-tests.sh), which picks an
interpreter that has the Databricks SDK. Steps, each with a progress line:

  1. refuse when a tracked UC source is dirty (the evidence must describe committed code);
  2. build the env 6 wheel (or reuse the one in dist/env6 when its commit has the same wheel inputs as HEAD);
  3. stage the wheel and its JAR alone into a fresh Volume folder `<volroot>/uc-it/<sha12>/`;
  4. install the functions with `gbx.uc.install(..., procedures=True)` from a Serverless environment 6 job
     into the schema `<catalog>.geobrix_uc_it_<sha12>`;
  5. on the SQL warehouse, run the UC oracle slice (bench.oracles.uc_sql, split over parallel shards,
     judged by bench.oracles.check_uc_results) and the example files of examples/uc-functions, all against that schema;
  6. drop the schema and delete the Volume folder (unless --keep);
  7. write test-logs/uc-integration/evidence.json (which records the scope: core or full).

`--run-part I/N` cuts a gated run into N parts run one after another (each can be rerun): parts 1 to N-1 deal the
slice, part N runs the example files. The first part to run installs; the others reuse the install, recorded in
`<run dir>/parts-<scope>-of-<N>.json`. evidence.json is written, and the schema dropped, when every part has run.

`--scope smoke|core|full` picks what runs: smoke is 12 slice statements plus example files 00 and 01, core is the
grid and vector tables, full adds the raster tables. `--against SCHEMA` runs the scope against an existing install
(no build, stage, install or cleanup) and writes evidence-adhoc.json, never evidence.json: the installed code
may not match HEAD, so it proves nothing about a commit.

The result is "pass" when the slice has no failure outside scripts/ci/uc_integration_known_failures.txt
and every example statement succeeded. `scripts/ci/check_uc_integration_evidence.py` reads the file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "ci"))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO_ROOT / "python" / "geobrix" / "src"))  # databricks.labs.gbx.bench

import check_uc_integration_evidence as gate  # noqa: E402
from _cluster_config import cluster_config_file  # noqa: E402
from _job_submit import jobs_root  # noqa: E402
from _uc_warehouse import warehouse_problem  # noqa: E402
import uc_sources_hash  # noqa: E402

CMD = REPO_ROOT / "scripts" / "commands"
OUT_DIR = REPO_ROOT / "test-logs" / "uc-integration"
EVIDENCE = OUT_DIR / "evidence.json"
ADHOC_EVIDENCE = OUT_DIR / "evidence-adhoc.json"
SMOKE_STATEMENTS = 12
SMOKE_EXAMPLES = "00,01"
KNOWN_FAILURES = REPO_ROOT / "scripts" / "ci" / "uc_integration_known_failures.txt"
CONFIG = cluster_config_file(HERE)  # a detached worktree: the one the command resolved
DEFAULT_CATALOG = "geospatial_docs"
DEFAULT_VOLROOT = "/Volumes/geospatial_docs/geobrix/sample-data"
# What a built wheel is made from, besides the UC sources: a wheel built at another commit is reused only when none of these changed.
WHEEL_INPUTS = tuple(p for p in uc_sources_hash.UC_PATHS if not p.startswith("examples/")) + (
    "python/geobrix/envs",
    "python/geobrix/pyproject.toml",
    "python/geobrix/requirements-build.txt",
    "python/geobrix/README.md",
    "scripts/native",
)
HEARTBEAT_S = 60
# The warehouse pays ~10 s of sandbox cost per statement that calls a UC Python function, whatever it computes:
# value cases share a statement (DEFAULT_BATCH per statement) and statements run side by side.
DEFAULT_SHARDS = 1
DEFAULT_BATCH = 25
DEFAULT_CONCURRENCY = 16
DEFAULT_EXAMPLE_PARALLEL = 6


class StepFailed(Exception):
    """A step failed; the message goes into the evidence."""


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(msg: str) -> None:
    print(f"[UC IT {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ---- pure helpers (host-tested) ------------------------------------------------------------------------------------


def parse_known_failures(text: str) -> Dict[str, str]:
    """`id: reason` lines (`#` starts a comment). A line without a reason is an error: every tolerated id says why."""
    known: Dict[str, str] = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip() if raw.lstrip().startswith("#") else raw.strip()
        if not line:
            continue
        cid, _, reason = line.partition(":")
        if not cid.strip() or not reason.strip():
            raise ValueError(f"known failures line {n}: expected `<id>: <reason>`, got {raw!r}")
        known[cid.strip()] = reason.strip()
    return known


def shard(pairs: Sequence, n: int) -> List[list]:
    """`pairs` dealt round-robin into at most `n` non-empty shards (statements differ in cost; neighbours often alike)."""
    n = max(1, min(n, len(pairs) or 1))
    shards = [list(pairs[i::n]) for i in range(n)]
    return [s for s in shards if s]


def parse_run_part(text: str) -> Tuple[int, int]:
    """`I/N` with 1 <= I <= N and N >= 2 (an argparse type)."""
    m = re.fullmatch(r"\s*(\d+)\s*/\s*(\d+)\s*", text or "")
    if not m or not (1 <= int(m.group(1)) <= int(m.group(2))) or int(m.group(2)) < 2:
        raise argparse.ArgumentTypeError(f"expected I/N with 1 <= I <= N and N >= 2, got {text!r}")
    return int(m.group(1)), int(m.group(2))


def part_work(pairs: Sequence, part: int, parts: int) -> Tuple[list, bool]:
    """(the slice statements of part `part` of `parts`, whether it runs the example files).

    Unsplit (`parts` 1): everything. Otherwise parts 1 to N-1 deal the slice round-robin and part N runs the examples.
    """
    if parts <= 1:
        return list(pairs), True
    if part == parts:
        return [], True
    dealt = shard(pairs, parts - 1)
    return (dealt[part - 1] if part - 1 < len(dealt) else []), False


def merge_parts(records: Dict[str, dict], expected: Sequence[str], parts: int) -> Tuple[dict, dict]:
    """(slice, examples) of a run in parts, as one unsplit run would judge them.

    Every case is judged on its own, so the union of the parts' verdicts is the run's verdict. A case of `expected`
    (the scope's UC cases) that no part ran is a failure, as `check_uc_results` counts a case with no result.
    """
    ran, failed, tolerated = set(), set(), set()
    for rec in records.values():
        ran.update(rec.get("cids") or [])
        failed.update(rec["slice"]["failed_ids"])
        tolerated.update(rec["slice"]["tolerated_ids"])
    failed.update(c for c in expected if c not in ran)
    total = len(set(expected) | ran)
    sl = {
        "total": total,
        "passed": total - len(failed) - len(tolerated - failed),
        "failed": len(failed),
        "failed_ids": sorted(failed),
        "tolerated_ids": sorted(tolerated - failed),
    }
    return sl, records[str(parts)]["examples"]


def failed_id(message: str) -> str:
    """The case id at the start of a `check_uc_results` failure line (`<id>: <what>`)."""
    return message.split(":", 1)[0].strip()


def judge_slice(failures: Sequence[str], total: int, known: Dict[str, str]) -> dict:
    """{passed, failed, failed_ids, tolerated_ids}: `failed` counts the failures not on the known list."""
    ids = sorted({failed_id(m) for m in failures})
    tolerated = [i for i in ids if i in known]
    unexpected = [i for i in ids if i not in known]
    return {
        "total": total,
        "passed": total - len(ids),
        "failed": len(unexpected),
        "failed_ids": unexpected,
        "tolerated_ids": tolerated,
    }


def decide(slice_: dict, examples: dict) -> str:
    return "pass" if slice_["failed"] == 0 and examples["statements_ok"] == examples["total"] and examples["total"] > 0 else "fail"


def wheel_info(wheel: Path) -> dict:
    """{name, size, sha256, git_sha, dirty, jar_size, jar_sha256} read from an env wheel."""
    info = {"name": wheel.name, "size": wheel.stat().st_size, "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()}
    with zipfile.ZipFile(wheel) as z:
        build = next(n for n in z.namelist() if n.endswith("gbx/_build_info.py"))
        text = z.read(build).decode()
        info["git_sha"] = (re.search(r'GIT_SHA = "([0-9a-f]+)"', text) or [None, ""])[1]
        info["dirty"] = "GIT_DIRTY = True" in text
        jar = next(n for n in z.namelist() if n.endswith("-jar-with-dependencies.jar"))
        data = z.read(jar)
        info["jar_name"] = Path(jar).name
        info["jar_size"] = len(data)
        info["jar_sha256"] = hashlib.sha256(data).hexdigest()
    return info


def wheel_reusable(info: dict, root: Path = REPO_ROOT) -> Tuple[bool, str]:
    """A built wheel stands in for a build at HEAD when it was built clean and no wheel input differs since its commit."""
    if info["dirty"] or not info["git_sha"]:
        return False, "the wheel was built from a dirty tree or records no commit"
    known = subprocess.run(["git", "-C", str(root), "cat-file", "-e", f"{info['git_sha']}^{{commit}}"], capture_output=True)
    if known.returncode != 0:
        return False, f"the wheel's commit {info['git_sha'][:12]} is not in this repository"
    diff = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", info["git_sha"], "HEAD", "--", *WHEEL_INPUTS],
        capture_output=True,
        text=True,
    )
    changed = [line for line in diff.stdout.splitlines() if line]
    if changed:
        return False, f"{len(changed)} wheel input(s) changed since {info['git_sha'][:12]}, such as {changed[0]}"
    return True, f"no wheel input changed since its commit {info['git_sha'][:12]}"


def inputs_changed(old_head: str, root: Optional[Path] = None) -> List[str]:
    """The UC sources and wheel inputs that differ between `old_head` and HEAD (a parted run resumes only when none)."""
    root = root or REPO_ROOT
    known = subprocess.run(["git", "-C", str(root), "cat-file", "-e", f"{old_head}^{{commit}}"], capture_output=True)
    if known.returncode != 0:
        return [f"<commit {old_head[:12]} is not in this repository>"]
    paths = tuple(dict.fromkeys(tuple(uc_sources_hash.UC_PATHS) + WHEEL_INPUTS))
    diff = subprocess.run(["git", "-C", str(root), "diff", "--name-only", old_head, "HEAD", "--", *paths], capture_output=True, text=True)
    return [line for line in diff.stdout.splitlines() if line]


def jar_stale(jar: Path, root: Path = REPO_ROOT) -> bool:
    """True when the built JAR is missing or older than a tracked Scala source or pom.xml."""
    if not jar.is_file():
        return True
    out = subprocess.run(["git", "-C", str(root), "ls-files", "src/main/scala", "pom.xml"], capture_output=True, text=True)
    newest = max((root / f).stat().st_mtime for f in out.stdout.split() if (root / f).exists())
    return jar.stat().st_mtime < newest


def warehouse_from_config(config: Path = CONFIG) -> Optional[str]:
    if not config.is_file():
        return None
    text = config.read_text()
    for key in ("GBX_UC_GATE_WAREHOUSE_ID", "GBX_UC_WAREHOUSE_ID"):  # the gate's own warehouse wins
        ids = re.findall(rf"^{key}=(.*)$", text, flags=re.M)
        if ids and ids[-1].strip().strip("\"'"):
            return ids[-1].strip().strip("\"'")
    return None


def install_notebook(catalog: str, schema: str, folder: str) -> dict:
    """The Serverless notebook that installs the functions and the procedures from `folder` into the schema."""
    code = f'''import time
t0 = time.time()
print("[UC IT] install START", flush=True)
from databricks.labs.gbx import uc
spark.sql("CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
res = uc.install({catalog!r}, {schema!r}, {folder!r}, procedures=True)
n = len(res) if hasattr(res, "__len__") else -1
print("[UC IT] install DONE %.1fs, %s statements" % (time.time() - t0, n), flush=True)
# One UC function call reports the build it imports; the install fails here when it is not the staged wheel's.
from databricks.labs.gbx.bench import uc_build_probe
uc_build_probe.assert_installed_build(spark, {catalog!r}, {schema!r}, {folder!r}, say=lambda m: print(m, flush=True))
cnt = spark.sql(
    "SELECT count(*) FROM `{catalog}`.information_schema.routines WHERE routine_schema = '{schema}'"
).collect()[0][0]
print("[UC IT] routines in the schema:", cnt, flush=True)
assert cnt > 0, "the install created no routine"
'''
    return {
        "cells": [{"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": code.splitlines(True)}],
        "metadata": {"language_info": {"name": "python"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# ---- process helpers ------------------------------------------------------------------------------------------------


def child_env(base: Optional[dict] = None) -> dict:
    """The environment of a child command: every virtualenv is dropped from PATH, and VIRTUAL_ENV is unset.

    The launcher runs the driver under `uv run --with databricks-sdk`, which puts its throwaway venvs (no pip)
    and the repo's .venv-pyrx first on PATH. A child such as gbx:build:wheel resolves `python3` from PATH and
    needs the host's. A directory is a venv's when the folder above it holds pyvenv.cfg.
    """
    env = dict(os.environ if base is None else base)
    keep = [p for p in env.get("PATH", "").split(os.pathsep) if p and not (Path(p).parent / "pyvenv.cfg").exists()]
    env["PATH"] = os.pathsep.join(keep)
    env.pop("VIRTUAL_ENV", None)
    return env


def run_cmd(argv: Sequence[str], log: Path, label: str, env: Optional[dict] = None, echo: bool = True) -> Tuple[int, str]:
    """Run a command, tee its output to `log` (and the console when `echo`), print a line every minute while it is quiet."""
    log.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        list(argv), cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env={**child_env(), **(env or {})}
    )
    lines: List[str] = []
    last = [time.time()]

    def pump():
        with log.open("w") as fh:
            for line in proc.stdout:  # type: ignore[union-attr]
                fh.write(line)
                fh.flush()
                lines.append(line)
                last[0] = time.time()
                if echo:
                    print("    " + line.rstrip(), flush=True)

    t = threading.Thread(target=pump, daemon=True)
    t.start()
    start = time.time()
    while proc.poll() is None:
        time.sleep(1)
        if time.time() - last[0] >= HEARTBEAT_S:
            say(f"{label}: still running, {int(time.time() - start)} s, log {log.name}")
            last[0] = time.time()
    t.join()
    return proc.returncode, "".join(lines)


def need(rc: int, what: str, out: str = "") -> None:
    if rc != 0:
        tail = " | ".join(out.strip().splitlines()[-3:]) if out else ""
        raise StepFailed(f"{what} failed (exit {rc}){': ' + tail[:400] if tail else ''}")


# ---- the gate -------------------------------------------------------------------------------------------------------


def _needs_osgeo(exc: BaseException) -> bool:
    return type(exc).__name__ == "OsgeoUnavailable"


def _slice_via_container(catalog: str, schema: str, tables) -> dict:
    """Build the slice statements inside the dev container, where osgeo exists (the raster engine rows need it).

    The container sees this checkout at /root/geobrix, so it writes the JSON where the host reads it.
    """
    from databricks.labs.gbx.bench import probes

    scope = "full" if tuple(tables) == tuple(probes.UC_SLICE_TABLES) else "core"
    key = (catalog, schema, scope)
    if key not in _CONTAINER_SLICE:
        out = OUT_DIR / f"slice-{scope}.json"
        rel = out.relative_to(REPO_ROOT).as_posix()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.unlink(missing_ok=True)
        say(f"the host has no osgeo (GDAL's Python bindings): building the {scope} statements in the dev container")
        inner = (
            f"cd /root/geobrix && PYTHONPATH=python/geobrix/src python3 examples/tests/uc_integration.py "
            f"--emit-slice {rel} --scope {scope} --catalog {catalog} --against {schema}"
        )
        rc, text = run_cmd(["bash", str(CMD / "gbx-docker-exec.sh"), inner], OUT_DIR / f"slice-{scope}.log", "slice statements", echo=False)
        if rc != 0 or not out.is_file():
            tail = " | ".join(text.strip().splitlines()[-3:])[:400]
            raise StepFailed(
                f"building the {scope} statements in the dev container failed (exit {rc}): {tail}\n"
                f"Fix: start the container (`gbx:docker:start`) and re-run; the log is {OUT_DIR / f'slice-{scope}.log'}"
            )
        _CONTAINER_SLICE[key] = json.loads(out.read_text())
    return _CONTAINER_SLICE[key]


_CONTAINER_SLICE: dict = {}
_OSGEO_JUDGE = "(OsgeoUnavailable:"


def judge_uc(cases, results: dict, scope: str) -> List[str]:
    """`check_uc_results` on the host; the cases whose reference needs osgeo are judged again in the dev container.

    The summary, polygonize and contour references need GDAL's Python bindings, which the host lacks. Only those
    cases go to the container, so a core run never needs it.
    """
    from databricks.labs.gbx.bench import oracles

    with tempfile.TemporaryDirectory(prefix="uc-it-judge-") as d:
        failures = oracles.check_uc_results(cases, results, d)
    redo = sorted({failed_id(m) for m in failures if _OSGEO_JUDGE in m})
    if not redo:
        return failures
    say(f"{len(redo)} case(s) need osgeo to be judged: judging them in the dev container")
    tag = f"judge-{os.getpid()}-{int(time.time())}"
    inp, out = OUT_DIR / f"{tag}-in.json", OUT_DIR / f"{tag}-out.json"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    oracles.dump_uc_results({cid: results[cid] for cid in redo}, str(inp))
    inner = (
        f"cd /root/geobrix && PYTHONPATH=python/geobrix/src python3 examples/tests/uc_integration.py "
        f"--judge-uc {inp.relative_to(REPO_ROOT).as_posix()} --judge-out {out.relative_to(REPO_ROOT).as_posix()} --scope {scope}"
    )
    try:
        rc, text = run_cmd(["bash", str(CMD / "gbx-docker-exec.sh"), inner], OUT_DIR / f"{tag}.log", "judge", echo=False)
        if rc != 0 or not out.is_file():
            tail = " | ".join(text.strip().splitlines()[-3:])[:400]
            raise StepFailed(
                f"judging {len(redo)} case(s) in the dev container failed (exit {rc}): {tail}\n"
                f"Fix: start the container (`gbx:docker:start`) and re-run; the log is {OUT_DIR / f'{tag}.log'}"
            )
        again = json.loads(out.read_text())["failures"]
    finally:
        inp.unlink(missing_ok=True)
        out.unlink(missing_ok=True)
    return [m for m in failures if failed_id(m) not in redo] + again


def slice_pairs(catalog: str, schema: str, tables) -> list:
    from databricks.labs.gbx.bench import t28

    notes: List[str] = []
    try:
        return t28._slice_pairs(t28.Params(catalog=catalog, schema=schema), schema, tables, notes)
    except Exception as exc:  # noqa: BLE001 - only a missing osgeo is handled here
        if not _needs_osgeo(exc):
            raise
        return [tuple(p) for p in _slice_via_container(catalog, schema, tables)["pairs"]]


def slice_batchable(tables) -> set:
    """The cids of the slice that may share a warehouse statement (not a refusal, not a recipe)."""
    from databricks.labs.gbx.bench import oracles, probes

    return oracles.uc_batchable(probes.uc_slice_cases(tables))


def warehouse_statements(pairs, batchable, size: int) -> int:
    """How many warehouse statements `pairs` take when batched `size` at a time (the plan the runner makes)."""
    import uc_sql

    return len(uc_sql.plan_units(pairs, batchable, size))


def slice_tables(scope: str) -> tuple:
    from databricks.labs.gbx.bench import probes

    return probes.UC_SLICE_TABLES if scope == "full" else probes.UC_CORE_TABLES  # smoke samples the core tables


def example_pairs(catalog: str, schema: str, prefixes: Optional[str] = None) -> Dict[Path, list]:
    import uc_examples_sql as ex
    from databricks.labs.gbx.bench import oracles

    return {
        f: [(cid, ex.retarget(s, catalog, schema)) for cid, s in oracles.parse_uc_sql(f.read_text())]
        for f in ex.example_files(ex.DEFAULT_DIR, prefixes)
    }


def count_results(path: Path) -> int:
    try:
        return len(json.loads(path.read_text()))
    except (OSError, ValueError):
        return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default=None, help="Databricks config profile (required; never chosen for you)")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse (default GBX_UC_WAREHOUSE_ID in the cluster config)")
    p.add_argument("--catalog", default=DEFAULT_CATALOG)
    p.add_argument("--volroot", default=DEFAULT_VOLROOT, help="Volume folder that holds the uc-it/<sha12>/ staging folder")
    p.add_argument("--scope", choices=("smoke", "core", "full"), default="core", help="smoke: 12 slice statements and example files 00 and 01; core: grid and vector tables and all examples; full: also the raster tables")
    p.add_argument("--shards", type=int, default=DEFAULT_SHARDS, help="separate warehouse sessions (processes) for the slice; each batches its own share, so 1 batches best")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="value cases per warehouse statement (1 runs each alone); a case that must be refused always runs alone")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="warehouse statements in flight at once, per slice session")
    p.add_argument("--example-files-parallel", type=int, default=DEFAULT_EXAMPLE_PARALLEL, help="example files run side by side (file 00, the setup, always runs first)")
    p.add_argument("--rebuild", action="store_true", help="build the wheel even when dist/env6 holds a reusable one")
    p.add_argument("--keep", action="store_true", help="keep the schema and the Volume folder")
    p.add_argument("--install-timeout-min", type=int, default=90)
    p.add_argument("--against", metavar="SCHEMA", default=None, help="run the scope against the existing install in SCHEMA: no build, stage, install or cleanup; writes evidence-adhoc.json, never evidence.json")
    p.add_argument("--run-part", metavar="I/N", type=parse_run_part, default=None, help="run part I of a gated run cut into N parts (N >= 2): parts 1 to N-1 deal the slice, part N runs the example files; evidence.json comes with the last part to finish")
    p.add_argument("--drop-parts", metavar="SHA12", default=None, help="drop the schema and the Volume and workspace folders of the unfinished parted run of commit SHA12, then stop")
    p.add_argument("--judge-uc", metavar="FILE", default=None, help=argparse.SUPPRESS)  # internal: judge these results (run in the dev container, where osgeo exists)
    p.add_argument("--judge-out", metavar="FILE", default=None, help=argparse.SUPPRESS)
    p.add_argument("--emit-slice", metavar="FILE", default=None, help=argparse.SUPPRESS)  # internal: write the slice statements as JSON and exit (run in the dev container, where osgeo exists)
    p.add_argument("--install-into", metavar="SCHEMA", default=None, help="install the functions and procedures into SCHEMA and keep them: stage into <volroot>/uc-install/SCHEMA/, install, probe; no warehouse step, no evidence")
    p.add_argument("--dry-run", action="store_true", help="print the plan (the slice and example counts); no workspace call, no build")
    return p.parse_args(argv)


class Gate:
    def __init__(self, args):
        self.a = args
        self.head = uc_sources_hash.head_sha(REPO_ROOT)
        self.sha12 = self.head[:12]
        install_into = getattr(args, "install_into", None)
        self.schema = args.against or install_into or f"geobrix_uc_it_{self.sha12}"
        # A kept install reads its wheel from this folder at every call: it gets a folder of its own, by schema.
        sub = f"uc-install/{install_into}" if install_into else f"uc-it/{self.sha12}"
        self.folder = f"{args.volroot.rstrip('/')}/{sub}"
        self.ws_dir: Optional[str] = None
        self.run_dir = OUT_DIR / self.sha12
        self.warehouse = args.warehouse_id or warehouse_from_config()
        self.w = None
        self.run_ids: List[str] = []
        self.ev: dict = {}
        self.created = {"folder": False, "schema": False, "ws": False}
        self.slice_expected: List[str] = []  # the scope's UC case ids (set by step_warehouse)
        self.part_cids: List[str] = []  # the UC case ids this run judged

    def schema_exists(self) -> bool:
        try:
            self.client().schemas.get(f"{self.a.catalog}.{self.schema}")
            return True
        except Exception as exc:  # noqa: BLE001 - only "not found" means gone; any other error stops the part
            if type(exc).__name__ in ("NotFound", "ResourceDoesNotExist"):
                return False
            raise

    # -- steps --
    def client(self):
        if self.w is None:
            from databricks.sdk import WorkspaceClient

            self.w = WorkspaceClient(profile=self.a.profile)
        return self.w

    def step_wheel(self) -> Path:
        say("step 2/6: env 6 wheel")
        dist = sorted((REPO_ROOT / "dist" / "env6").glob("geobrix-*+env6-py3-none-any.whl"))
        if dist and not self.a.rebuild and getattr(self.a, "install_into", None):
            # An install is not evidence: any clean build serves, whatever commit it records.
            info = wheel_info(dist[-1])
            if not info["dirty"] and info["git_sha"]:
                say(f"reuse {dist[-1].name}: built clean at {info['git_sha'][:12]}, sha256 {info['sha256']}")
                return dist[-1]
        if dist and not self.a.rebuild:
            ok, why = wheel_reusable(wheel_info(dist[-1]))
            say(f"reuse {dist[-1].name}: {'yes' if ok else 'no'}, {why}")
            if ok:
                return dist[-1]
        jar = REPO_ROOT / "target" / "geobrix-0.6.0-jar-with-dependencies.jar"
        if jar_stale(jar):
            say("building the JAR (target/ is missing or older than the Scala sources)")
            rc, out = run_cmd(["bash", str(CMD / "gbx-data-stage-jar.sh")], self.run_dir / "build-jar.log", "JAR build", env={"GBX_BUNDLE_SKIP_JAR_UPLOAD": "1"})
            need(rc, "the JAR build", out)
        say("building the wheel")
        rc, out = run_cmd(["bash", str(CMD / "gbx-build-wheel.sh"), "--env", "6"], self.run_dir / "build-wheel.log", "wheel build")
        need(rc, "the wheel build", out)
        dist = sorted((REPO_ROOT / "dist" / "env6").glob("geobrix-*+env6-py3-none-any.whl"))
        if not dist:
            raise StepFailed("the wheel build left no dist/env6 wheel")
        info = wheel_info(dist[-1])
        if info["dirty"] or info["git_sha"] != self.head:
            raise StepFailed(f"the built wheel records commit {info['git_sha'][:12]} dirty={info['dirty']}, not a clean {self.sha12}")
        return dist[-1]

    def step_stage(self, wheel: Path, info: dict) -> str:
        say(f"step 3/6: stage the wheel and its JAR alone into {self.folder}")
        self.created["folder"] = True
        rc, out = run_cmd(
            ["bash", str(CMD / "gbx-data-stage-wheel.sh"), "--env", "6", "--dest", self.folder, "--profile", self.a.profile],
            self.run_dir / "stage-wheel.log", "stage wheel", echo=False,
        )
        need(rc, "staging the wheel", out)
        rc, out = run_cmd(
            ["bash", str(CMD / "gbx-data-stage-jar.sh"), "--from-wheel", str(wheel), "--dest", self.folder, "--profile", self.a.profile],
            self.run_dir / "stage-jar.log", "stage JAR", echo=False,
        )
        need(rc, "staging the JAR", out)
        listed = {e.name: e.file_size for e in self.client().files.list_directory_contents(self.folder) if not e.is_directory}
        wheels = [n for n in listed if n.startswith("geobrix-") and n.endswith(".whl")]
        jars = [n for n in listed if n.endswith("-jar-with-dependencies.jar")]
        if wheels != [info["name"]] or listed[info["name"]] != info["size"]:
            raise StepFailed(f"the Volume folder holds {wheels} (sizes {[listed[n] for n in wheels]}), want {info['name']} with {info['size']} bytes")
        if len(jars) != 1 or listed[jars[0]] != info["jar_size"]:
            raise StepFailed(f"the Volume folder holds JAR {jars} (sizes {[listed[n] for n in jars]}), want one with {info['jar_size']} bytes")
        say(f"staged {wheels[0]} ({info['size']} bytes) and {jars[0]} ({info['jar_size']} bytes); sizes verified by listing")
        return f"{self.folder}/{info['name']}"

    def step_install(self, volume_wheel: str) -> None:
        say(f"step 4/6: install into {self.a.catalog}.{self.schema} from a Serverless env 6 job (procedures=True)")
        me = self.client().current_user.me().user_name
        self.ws_dir = f"{jobs_root(me)}/uc-it-{self.sha12}"
        nb_dir = self.run_dir / "nb"
        nb_dir.mkdir(parents=True, exist_ok=True)
        nb = nb_dir / "uc_it_install.ipynb"
        nb.write_text(json.dumps(install_notebook(self.a.catalog, self.schema, self.folder)))
        self.created["ws"] = True
        rc, out = run_cmd(
            ["bash", str(CMD / "gbx-test-notebooks-serverless.sh"), "--notebook", str(nb), "--env-version", "6", "--wheel", volume_wheel,
             "--profile", self.a.profile, "--ws-dir", self.ws_dir, "--timeout-min", str(self.a.install_timeout_min)],
            self.run_dir / "install.log", "install job",
        )
        self.run_ids += re.findall(r"run_id=(\d+)", out)
        need(rc, "the install job", out)
        self.created["schema"] = True

    def run_statements(self, name: str, pairs: list, batchable=frozenset()) -> Tuple[Path, Path, Path]:
        from databricks.labs.gbx.bench import oracles

        self.run_dir.mkdir(parents=True, exist_ok=True)
        sql = self.run_dir / f"{name}.sql"
        sql.write_text(oracles.uc_sql_text(pairs, batchable))
        return sql, self.run_dir / f"{name}.json", self.run_dir / f"{name}.log"

    def uc_sql_argv(self, sql: Path, out: Path, schema: Optional[str] = None, batch: bool = False) -> List[str]:
        argv = ["bash", str(CMD / "gbx-uc-sql.sh"), "--profile", self.a.profile, "--file", str(sql), "--out", str(out),
                "--catalog", self.a.catalog, "--timeout-s", "900"]
        if batch:
            argv += ["--batch", str(self.a.batch), "--concurrency", str(self.a.concurrency)]
        if schema:
            argv += ["--schema", schema]
        if self.warehouse:
            argv += ["--warehouse-id", self.warehouse]
        return argv

    def step_warehouse(self, tables, known: Dict[str, str], part: Tuple[int, int] = (1, 1)) -> Tuple[dict, Optional[dict]]:
        """(slice verdict, examples verdict) of this run's share: all of it, or part `part` (I, N) of a parted run.

        The examples verdict is None for a part that does not run the example files.
        """
        from databricks.labs.gbx.bench import probes

        i_part, n_parts = part
        label = "" if n_parts == 1 else f"part {i_part}/{n_parts}: "
        pairs = slice_pairs(self.a.catalog, self.schema, tables)
        if self.a.scope == "smoke":
            pairs = pairs[:SMOKE_STATEMENTS]
        all_cases = probes.uc_slice_cases(tables)
        in_pairs = {cid for cid, _ in pairs}
        self.slice_expected = sorted(c.cid for c in all_cases if "uc" in c.surfaces and c.cid in in_pairs)
        pairs, run_examples = part_work(pairs, i_part, n_parts)
        say(f"step 5/6: {label}" + ("the oracle slice and the example files on the warehouse, in parallel" if pairs and run_examples
                                     else "the oracle slice on the warehouse" if pairs else "the example files on the warehouse"))
        batchable = slice_batchable(tables)
        jobs: List[Tuple[str, subprocess.Popen, Path, Path, int]] = []
        if pairs:
            say(f"slice: {len(pairs)} cases as {warehouse_statements(pairs, batchable, self.a.batch)} warehouse statements (batch {self.a.batch}, concurrency {self.a.concurrency}, {self.a.shards} session(s))")
            prefix = "slice" if n_parts == 1 else f"slice_p{i_part}"
            for i, share in enumerate(shard(pairs, self.a.shards)):
                sql, out, log = self.run_statements(f"{prefix}_{i}", share, batchable)
                jobs.append((f"{prefix}_{i}", self._popen(self.uc_sql_argv(sql, out, self.schema, batch=True), log), out, log, len(share)))
        ex_dir = self.run_dir / "examples"
        ex_pairs: Dict[Path, list] = {}
        if run_examples:
            ex_log = self.run_dir / "examples.log"
            ex_argv = ["bash", str(CMD / "gbx-examples-uc-sql.sh"), "--profile", self.a.profile, "--catalog", self.a.catalog,
                       "--schema", self.schema, "--out-dir", str(ex_dir), "--timeout-s", "900",
                       "--parallel", str(self.a.example_files_parallel)]
            if self.warehouse:
                ex_argv += ["--warehouse-id", self.warehouse]
            if self.a.scope == "smoke":
                ex_argv += ["--files", SMOKE_EXAMPLES]
            ex_pairs = example_pairs(self.a.catalog, self.schema, SMOKE_EXAMPLES if self.a.scope == "smoke" else None)
            ex_total = sum(len(p) for p in ex_pairs.values())
            for old in ex_dir.glob("*.json") if ex_dir.exists() else []:
                old.unlink()  # a rerun of the part must not count the files of an earlier one
            jobs.append(("examples", self._popen(ex_argv, ex_log), ex_dir, ex_log, ex_total))
        start = time.time()
        while any(j[1].poll() is None for j in jobs):
            time.sleep(HEARTBEAT_S)
            slice_done = sum(count_results(o) for n, _, o, _, _ in jobs if n != "examples")
            ex_done = len(list(ex_dir.glob("*.json"))) if run_examples and ex_dir.exists() else 0
            running = sum(j[1].poll() is None for j in jobs)
            say(f"warehouse: {label}{int(time.time() - start)} s, slice {slice_done}/{len(pairs)} statements, examples {ex_done}/{len(ex_pairs)} files, {running} sessions running")
        for name, proc, _, log, _ in jobs:
            if proc.returncode not in (0, 1):  # 1 = the run finished and found failures: judged below
                raise StepFailed(f"{name} stopped (exit {proc.returncode}); see {log}")
        merged: dict = {}
        for name, _, out, _, _ in jobs:
            if name != "examples" and out.is_file():
                merged.update(oracles_load(out))
        ran = {cid for cid, _ in pairs}
        cases = [c for c in all_cases if c.cid in ran or "uc" not in c.surfaces]  # a smoke run or a part judges only what it ran
        self.part_cids = sorted(c.cid for c in cases if "uc" in c.surfaces)
        failures = judge_uc(cases, merged, self.a.scope)
        (self.run_dir / ("slice_failures.txt" if n_parts == 1 else f"slice_failures_p{i_part}.txt")).write_text("\n".join(failures))
        total = len([1 for c in cases if "uc" in c.surfaces])
        sl = judge_slice(failures, total, known)
        for line in failures[:30]:
            say("slice failure: " + " ".join(line.split())[:240])
        ex = self.judge_examples(ex_pairs, ex_dir) if run_examples else None
        return sl, ex

    def _popen(self, argv: List[str], log: Path) -> subprocess.Popen:
        log.parent.mkdir(parents=True, exist_ok=True)
        return subprocess.Popen(argv, cwd=REPO_ROOT, stdout=log.open("w"), stderr=subprocess.STDOUT, text=True)

    def judge_examples(self, ex_pairs: Dict[Path, list], ex_dir: Path) -> dict:
        import uc_examples_sql as ex

        ok = total = 0
        problems: List[str] = []
        for f, pairs in ex_pairs.items():
            total += len(pairs)
            out = ex_dir / f"{f.stem}.json"
            results = json.loads(out.read_text()) if out.is_file() else {}
            bad = ex.judge(pairs, results)
            ok += len(pairs) - len({b.split(":")[0] for b in bad})
            problems += [f"{f.name}: {b}" for b in bad]
        for line in problems[:30]:
            say("example failure: " + " ".join(line.split())[:240])
        return {"statements_ok": ok, "total": total, "failures": problems}

    def cleanup(self) -> str:
        if self.a.keep:
            return "kept"
        notes = []
        try:
            if self.created["schema"] or self.created["ws"]:
                sql = self.run_dir / "cleanup.sql"
                sql.write_text(f"-- cid: drop_schema\nDROP SCHEMA IF EXISTS `{self.a.catalog}`.`{self.schema}` CASCADE\n")
                out = self.run_dir / "cleanup.json"
                rc, text = run_cmd(self.uc_sql_argv(sql, out), self.run_dir / "cleanup.log", "drop schema", echo=False)
                res = json.loads(out.read_text()) if out.is_file() else {}
                if rc != 0 or "error" in res.get("drop_schema", {}):
                    notes.append(f"schema drop failed: {str(res.get('drop_schema', text))[:200]}")
            w = self.client()
            if self.created["folder"]:
                try:
                    for e in w.files.list_directory_contents(self.folder):
                        w.files.delete(e.path)
                    w.files.delete_directory(self.folder)
                except Exception as exc:  # noqa: BLE001 - report it in the evidence, do not hide the run's result
                    if "NOT_FOUND" not in str(exc).upper() and "does not exist" not in str(exc):
                        notes.append(f"Volume folder delete failed: {str(exc)[:200]}")
            # The workspace folder stays: the install run's page opens its notebook from it. The notebooks-serverless
            # run recorded that notebook, and gbx:workspace:sweep removes the folder after its window (60 min, 24 h
            # after a failed run).
            if self.ws_dir and self.created["ws"]:
                say(f"kept {self.ws_dir} for the install run's page; gbx:workspace:sweep removes it after its window")
        except Exception as exc:  # noqa: BLE001
            notes.append(f"cleanup error: {str(exc)[:200]}")
        return "dropped" if not notes else "; ".join(notes)

    # -- the run --
    def write_evidence(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        path = ADHOC_EVIDENCE if self.a.against else EVIDENCE
        path.write_text(json.dumps(self.ev, indent=2, sort_keys=True) + "\n")


# ---- a gated run in parts ---------------------------------------------------------------------------------------------


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def part_ok(rec: dict) -> bool:
    ex = rec.get("examples")
    return "error" not in rec and rec["slice"]["failed"] == 0 and (ex is None or (ex["total"] > 0 and ex["statements_ok"] == ex["total"]))


def run_parted(g: "Gate", a, tables, known: Dict[str, str]) -> int:
    """Part I of N of a gated run: install unless an earlier part did, run the part's share, merge after the last part."""
    part, n = a.run_part
    path = g.run_dir / f"parts-{a.scope}-of-{n}.json"
    src = uc_sources_hash.sources_hash(REPO_ROOT)
    def usable(st: dict) -> bool:
        return (st.get("sources_hash") == src and st.get("scope") == a.scope and st.get("parts") == n
                and not st.get("dropped") and not st.get("merged"))

    state = _load_json(path)
    if not usable(state):
        state = {}
        # Other commits may land between parts. A record of an earlier HEAD is resumed when no UC source and no wheel
        # input changed since: its install is still the code under test.
        for other in sorted(OUT_DIR.glob(f"*/{path.name}"), key=lambda q: q.stat().st_mtime, reverse=True):
            st = _load_json(other)
            if other == path or not usable(st):
                continue
            changed = inputs_changed(st.get("head_sha", ""))
            if changed:
                say(f"not resuming the parted run of {st.get('head_sha', '?')[:12]}: {len(changed)} UC source or wheel input(s) changed since, such as {changed[0]}")
                continue
            say(f"resuming the parted run of {st['head_sha'][:12]}: no UC source or wheel input changed since")
            path, state = other, st
            g.run_dir, g.sha12, g.schema, g.folder = other.parent, other.parent.name, st["schema"], st["volume_dir"]
            break
    if not state:
        state = {"head_sha": g.head, "sources_hash": src, "scope": a.scope, "parts": n, "catalog": a.catalog,
                 "schema": g.schema, "volume_dir": g.folder, "done": {}}
    say(f"part {part}/{n} of a gated {a.scope} run; its record is {path}")
    rec: dict = {"started": now(), "warehouse_id": g.warehouse, "head_sha": g.head}
    try:
        inst = state.get("install")
        if inst and not g.schema_exists():
            say(f"the schema {a.catalog}.{g.schema} of the recorded install is gone: installing again")
            inst = None
            state.pop("install", None)
        if inst:
            g.run_ids, g.ws_dir = list(inst["job_run_ids"]), inst.get("ws_dir")
            say(f"steps 2-4/6: reuse the install of an earlier part (job run {', '.join(g.run_ids) or '-'}, wheel {inst['wheel'].get('name')})")
        else:
            wheel = g.step_wheel()
            info = wheel_info(wheel)
            g.step_install(g.step_stage(wheel, info))
            state["install"] = {"wheel": info, "job_run_ids": list(g.run_ids), "ws_dir": g.ws_dir, "finished": now()}
            _save_json(path, state)
        sl, ex = g.step_warehouse(tables, known, part=(part, n))
        rec.update({"slice": sl, "examples": ex, "cids": g.part_cids})
        state["expected"] = g.slice_expected
    except StepFailed as exc:
        rec["error"] = str(exc)
        print(f"FAILED: part {part}/{n}: {exc}", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - any other error fails the part; its rerun resumes
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:400]}"
        print(f"FAILED: part {part}/{n}: {rec['error']}", file=sys.stderr)
    rec["finished"] = now()
    state["done"][str(part)] = rec
    _save_json(path, state)
    ok = part_ok(rec)
    if "error" not in rec:
        ex = rec["examples"]
        say(f"part {part}/{n}: slice {rec['slice']['passed']}/{rec['slice']['total']} passed, {rec['slice']['failed']} failed"
            + (f"; examples {ex['statements_ok']}/{ex['total']}" if ex else ""))
    todo = [i for i in range(1, n + 1) if str(i) not in state["done"] or "error" in state["done"][str(i)]]
    if todo:
        say(f"part {part}/{n} {'passed' if ok else 'FAILED'}; still to run: {', '.join(f'--run-part {i}/{n}' for i in todo)}. "
            f"The schema stays until then (drop an abandoned run with --drop-parts {g.sha12})")
        return 0 if ok else 1
    sl, ex = merge_parts(state["done"], state.get("expected", []), n)
    done = state["done"]
    g.ev = {
        "head_sha": g.head, "sources_hash": src, "started": min(r["started"] for r in done.values()), "profile": a.profile,
        "warehouse_id": g.warehouse, "catalog": a.catalog, "schema": g.schema, "volume_dir": g.folder, "scope": a.scope,
        "parts": n, "install_head_sha": state["head_sha"],
        "part_runs": {i: {k: done[i].get(k) for k in ("started", "finished", "warehouse_id", "head_sha")} for i in sorted(done, key=int)},
        "wheel": state["install"]["wheel"], "job_run_ids": list(state["install"]["job_run_ids"]),
        "slice": {**sl, "scope": a.scope, "tables": list(tables)},
        "examples": {"statements_ok": ex["statements_ok"], "total": ex["total"], "failed": ex["failures"][:40]},
        "result": decide(sl, ex), "finished": now(),
    }
    g.write_evidence()
    state["merged"] = g.ev["finished"]  # merged: a later part starts a new run
    _save_json(path, state)
    say("step 6/6: " + ("keeping the schema and the Volume folder" if a.keep else "dropping the schema and deleting the Volume folder"))
    g.ws_dir = state["install"].get("ws_dir")
    g.created = {"folder": True, "schema": True, "ws": True}
    g.ev["cleanup"] = g.cleanup()
    g.ev["finished"] = now()
    g.write_evidence()
    if not a.keep:
        state["dropped"] = g.ev["cleanup"]
        _save_json(path, state)
    for line in sl["failed_ids"][:30]:
        say(f"slice failure: {line}")
    say(gate.summary_line(g.ev) + f"; {n} parts; cleanup {g.ev['cleanup']}")
    say(f"evidence: {EVIDENCE}")
    return 0 if g.ev["result"] == "pass" else 1


def drop_parts(g: "Gate", sha12: str) -> int:
    """Clean up the unfinished parted runs of commit `sha12`: what a parted run keeps between its parts."""
    states = sorted((OUT_DIR / sha12).glob("parts-*.json"))
    if not states:
        print(f"no parted run recorded under {OUT_DIR / sha12}", file=sys.stderr)
        return 1
    rc = 0
    for path in states:
        state = _load_json(path)
        if state.get("dropped"):
            say(f"{path.name}: already cleaned up ({state['dropped']})")
            continue
        if state.get("schema") != f"geobrix_uc_it_{sha12}":  # only the gate's own scratch schema is ever dropped
            print(f"{path.name}: refused, it names schema {state.get('schema')!r}, not geobrix_uc_it_{sha12}", file=sys.stderr)
            rc = 1
            continue
        g.schema, g.folder, g.run_dir = state["schema"], state["volume_dir"], path.parent
        g.ws_dir = (state.get("install") or {}).get("ws_dir")
        g.created = {"folder": True, "schema": True, "ws": True}
        say(f"{path.name}: dropping {state.get('catalog')}.{g.schema} and deleting {g.folder}")
        state["dropped"] = g.cleanup()
        _save_json(path, state)
        say(f"{path.name}: cleanup {state['dropped']}")
        rc = rc or (0 if state["dropped"] == "dropped" else 1)
    return rc


def oracles_load(path: Path) -> dict:
    from databricks.labs.gbx.bench import oracles

    return oracles.load_uc_results(str(path))


def install_into(g: "Gate", a) -> int:
    """Install into a schema that stays: wheel, stage, install and probe. No warehouse step, no cleanup, no evidence."""
    clash = [f for f, v in (("--against", a.against), ("--run-part", a.run_part), ("--drop-parts", a.drop_parts)) if v]
    if clash:
        print(f"--install-into takes no {', '.join(clash)}", file=sys.stderr)
        return 2
    say(f"install into {a.catalog}.{g.schema} from the Volume folder {g.folder}; the schema and the folder stay")
    if a.dry_run:
        print(f"dry run: install into {a.catalog}.{g.schema}, staging folder {g.folder}; no workspace call")
        return 0
    try:
        wheel = g.step_wheel()
        info = wheel_info(wheel)
        g.step_install(g.step_stage(wheel, info))
    except StepFailed as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        say(f"install job run ids: {', '.join(g.run_ids) or '-'}")
    say(f"installed {a.catalog}.{g.schema} from {info['name']} (sha256 {info['sha256']}, commit {info['git_sha'][:12]})")
    return 0


def main(argv=None) -> int:
    a = parse_args(argv)
    if a.judge_uc:
        from databricks.labs.gbx.bench import oracles, probes

        results = oracles.load_uc_results(a.judge_uc)
        cases = [c for c in probes.uc_slice_cases(slice_tables(a.scope)) if c.cid in results]
        with tempfile.TemporaryDirectory(prefix="uc-it-judge-") as d:
            failures = oracles.check_uc_results(cases, results, d)
        Path(a.judge_out).write_text(json.dumps({"failures": failures}))
        say(f"judged {len(cases)} case(s): {len(failures)} failure line(s)")
        return 0
    if a.emit_slice:
        if not a.against:
            print("--emit-slice needs --against SCHEMA (the schema the statements are written for)", file=sys.stderr)
            return 2
        pairs = slice_pairs(a.catalog, a.against, slice_tables(a.scope))
        Path(a.emit_slice).write_text(json.dumps({"pairs": pairs}))
        say(f"wrote {len(pairs)} statements to {a.emit_slice}")
        return 0
    if not a.profile:
        print("--profile required (a profile is never chosen for you: see `databricks auth profiles`)", file=sys.stderr)
        return 2
    if a.run_part and a.scope == "smoke":
        print("--run-part cuts a core or full run into parts: it takes no smoke scope", file=sys.stderr)
        return 2
    g = Gate(a)
    if a.drop_parts:
        if not g.warehouse:
            print("--warehouse-id required to drop the schema", file=sys.stderr)
            return 2
        problem = warehouse_problem(g.client(), g.warehouse, CONFIG, a.profile)
        if problem:
            print(f"refused: {problem}", file=sys.stderr)
            return 1
        return drop_parts(g, a.drop_parts)
    if a.install_into:
        return install_into(g, a)
    say(f"HEAD {g.head}, schema {a.catalog}.{g.schema}, Volume folder {g.folder}")
    if a.scope == "smoke" and not a.against:
        print("--scope smoke needs --against SCHEMA: a smoke run is a wiring check on an existing install, never a gated run", file=sys.stderr)
        return 2
    # An ad hoc run (--against) judges whatever is installed, so uncommitted edits do not matter; a gated run must describe committed code.
    dirty = [] if a.against else uc_sources_hash.dirty_files(REPO_ROOT)
    if dirty:
        print("step 1/6: refused, uncommitted changes under the UC sources: " + ", ".join(dirty[:8]), file=sys.stderr)
        print("Fix: commit or stash them; the evidence must describe committed code.", file=sys.stderr)
        return 1
    say("step 1/6: UC sources are clean")
    try:
        known = parse_known_failures(KNOWN_FAILURES.read_text()) if KNOWN_FAILURES.is_file() else {}
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    tables = slice_tables(a.scope)
    if a.dry_run:
        try:
            pairs = slice_pairs(a.catalog, g.schema, tables)
        except StepFailed as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 1
        if a.scope == "smoke":
            pairs = pairs[:SMOKE_STATEMENTS]
        ex_pairs = example_pairs(a.catalog, g.schema, SMOKE_EXAMPLES if a.scope == "smoke" else None)
        wheels = sorted((REPO_ROOT / "dist" / "env6").glob("*.whl"))
        wheel_note = "none in dist/env6"
        if wheels:
            ok, why = wheel_reusable(wheel_info(wheels[-1]))
            wheel_note = f"{wheels[-1].name}: {'reusable' if ok else 'rebuild'}, {why}"
        print(
            f"dry run: sources hash {uc_sources_hash.sources_hash(REPO_ROOT)[:12]}; wheel {wheel_note}; "
            f"scope {a.scope}{' against ' + a.against if a.against else ''}: slice {len(pairs)} statements in {len(shard(pairs, a.shards))} shard(s) "
            f"({warehouse_statements(pairs, slice_batchable(tables), a.batch)} warehouse statements, batch {a.batch}, concurrency {a.concurrency}); "
            f"examples: {sum(len(p) for p in ex_pairs.values())} statements in {len(ex_pairs)} files; "
            f"warehouse {g.warehouse or '-'}; known failures {len(known)}; no workspace call"
        )
        if a.run_part:
            n = a.run_part[1]
            batchable = slice_batchable(tables)
            for i in range(1, n + 1):
                share, run_ex = part_work(pairs, i, n)
                if share:
                    print(f"  part {i}/{n}: slice {len(share)} statements ({warehouse_statements(share, batchable, a.batch)} warehouse statements)")
                if run_ex:
                    print(f"  part {i}/{n}: the example files, {sum(len(p) for p in ex_pairs.values())} statements in {len(ex_pairs)} files")
        return 0

    started = now()
    g.ev = {
        "head_sha": g.head, "sources_hash": uc_sources_hash.sources_hash(REPO_ROOT), "started": started, "profile": a.profile,
        "warehouse_id": g.warehouse, "catalog": a.catalog, "schema": g.schema, "volume_dir": g.folder, "result": "fail",
        "scope": a.scope,
    }
    if a.against:
        g.ev["adhoc"] = True  # not evidence: the installed code may not match HEAD
    elif EVIDENCE.exists():
        EVIDENCE.unlink()  # a run that stops early must not leave the last run's pass behind
    if not g.warehouse:
        print("--warehouse-id required (or GBX_UC_GATE_WAREHOUSE_ID / GBX_UC_WAREHOUSE_ID in examples/tests/databricks_cluster_config.env)", file=sys.stderr)
        return 2
    # Before any build, stage or install: a configured warehouse that was deleted would fail only after all three.
    problem = warehouse_problem(g.client(), g.warehouse, CONFIG, a.profile)
    if problem:
        print(f"refused: {problem}", file=sys.stderr)
        return 1
    if a.run_part and not a.against:
        return run_parted(g, a, tables, known)
    sl = ex = None
    try:
        if a.against:
            say(f"scope {a.scope}: warehouse step only, against the existing install in {a.catalog}.{a.against}")
        else:
            wheel = g.step_wheel()
            info = wheel_info(wheel)
            g.ev["wheel"] = info
            volume_wheel = g.step_stage(wheel, info)
            g.step_install(volume_wheel)
        # a part here is an --against one: a gated part goes through run_parted
        sl, ex = g.step_warehouse(tables, known, part=a.run_part) if a.run_part else g.step_warehouse(tables, known)
        g.ev["slice"] = {**sl, "scope": a.scope, "tables": list(tables)}
        if a.run_part:
            g.ev["part"] = "%d/%d" % a.run_part
        if ex is None:  # a part with no example files
            g.ev["examples"] = None
            g.ev["result"] = "pass" if sl["failed"] == 0 else "fail"
        else:
            g.ev["examples"] = {"statements_ok": ex["statements_ok"], "total": ex["total"], "failed": ex["failures"][:40]}
            g.ev["result"] = decide(sl, ex)
    except StepFailed as exc:
        g.ev["error"] = str(exc)
        print(f"FAILED: {exc}", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - any other error is a failed run with its message in the evidence
        g.ev["error"] = f"{type(exc).__name__}: {str(exc)[:400]}"
        print(f"FAILED: {g.ev['error']}", file=sys.stderr)
    finally:
        g.ev["job_run_ids"] = g.run_ids
        g.ev["finished"] = now()
        g.write_evidence()
        say("step 6/6: " + ("keeping the schema and the Volume folder" if a.keep or a.against else "dropping the schema and deleting the Volume folder"))
        g.ev["cleanup"] = "kept" if a.against else g.cleanup()
        g.ev["finished"] = now()
        g.write_evidence()
    say(gate.summary_line(g.ev) + f"; cleanup {g.ev['cleanup']}")
    say(f"evidence: {ADHOC_EVIDENCE if a.against else EVIDENCE}")
    return 0 if g.ev["result"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
