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
import uc_sources_hash  # noqa: E402

CMD = REPO_ROOT / "scripts" / "commands"
OUT_DIR = REPO_ROOT / "test-logs" / "uc-integration"
EVIDENCE = OUT_DIR / "evidence.json"
ADHOC_EVIDENCE = OUT_DIR / "evidence-adhoc.json"
SMOKE_STATEMENTS = 12
SMOKE_EXAMPLES = "00,01"
KNOWN_FAILURES = REPO_ROOT / "scripts" / "ci" / "uc_integration_known_failures.txt"
CONFIG = REPO_ROOT / "examples" / "tests" / "databricks_cluster_config.env"
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
    ids = re.findall(r"^GBX_UC_WAREHOUSE_ID=(.*)$", config.read_text(), flags=re.M)
    return ids[-1].strip().strip("\"'") if ids else None


def install_notebook(catalog: str, schema: str, folder: str) -> dict:
    """The Serverless notebook that installs the functions and the procedures from `folder` into the schema."""
    code = f'''import time
t0 = time.time()
print("[UC IT] install START", flush=True)
from databricks.labs.gbx import uc
res = uc.install({catalog!r}, {schema!r}, {folder!r}, procedures=True)
n = len(res) if hasattr(res, "__len__") else -1
print("[UC IT] install DONE %.1fs, %s statements" % (time.time() - t0, n), flush=True)
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


def slice_pairs(catalog: str, schema: str, tables) -> list:
    from databricks.labs.gbx.bench import t28

    notes: List[str] = []
    return t28._slice_pairs(t28.Params(catalog=catalog, schema=schema), schema, tables, notes)


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
    p.add_argument("--shards", type=int, default=8, help="parallel warehouse sessions for the slice")
    p.add_argument("--rebuild", action="store_true", help="build the wheel even when dist/env6 holds a reusable one")
    p.add_argument("--keep", action="store_true", help="keep the schema and the Volume folder")
    p.add_argument("--install-timeout-min", type=int, default=90)
    p.add_argument("--against", metavar="SCHEMA", default=None, help="run the scope against the existing install in SCHEMA: no build, stage, install or cleanup; writes evidence-adhoc.json, never evidence.json")
    p.add_argument("--dry-run", action="store_true", help="print the plan (the slice and example counts); no workspace call, no build")
    return p.parse_args(argv)


class Gate:
    def __init__(self, args):
        self.a = args
        self.head = uc_sources_hash.head_sha(REPO_ROOT)
        self.sha12 = self.head[:12]
        self.schema = args.against or f"geobrix_uc_it_{self.sha12}"
        self.folder = f"{args.volroot.rstrip('/')}/uc-it/{self.sha12}"
        self.ws_dir: Optional[str] = None
        self.run_dir = OUT_DIR / self.sha12
        self.warehouse = args.warehouse_id or warehouse_from_config()
        self.w = None
        self.run_ids: List[str] = []
        self.ev: dict = {}
        self.created = {"folder": False, "schema": False, "ws": False}

    # -- steps --
    def client(self):
        if self.w is None:
            from databricks.sdk import WorkspaceClient

            self.w = WorkspaceClient(profile=self.a.profile)
        return self.w

    def step_wheel(self) -> Path:
        say("step 2/6: env 6 wheel")
        dist = sorted((REPO_ROOT / "dist" / "env6").glob("geobrix-*+env6-py3-none-any.whl"))
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
        self.ws_dir = f"/Users/{me}/GeoBrix/uc-it-{self.sha12}"
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

    def run_statements(self, name: str, pairs: list) -> Tuple[Path, Path, Path]:
        from databricks.labs.gbx.bench import oracles

        self.run_dir.mkdir(parents=True, exist_ok=True)
        sql = self.run_dir / f"{name}.sql"
        sql.write_text(oracles.uc_sql_text(pairs))
        return sql, self.run_dir / f"{name}.json", self.run_dir / f"{name}.log"

    def uc_sql_argv(self, sql: Path, out: Path, schema: Optional[str] = None) -> List[str]:
        argv = ["bash", str(CMD / "gbx-uc-sql.sh"), "--profile", self.a.profile, "--file", str(sql), "--out", str(out),
                "--catalog", self.a.catalog, "--timeout-s", "900"]
        if schema:
            argv += ["--schema", schema]
        if self.warehouse:
            argv += ["--warehouse-id", self.warehouse]
        return argv

    def step_warehouse(self, tables, known: Dict[str, str]) -> Tuple[dict, dict]:
        from databricks.labs.gbx.bench import probes

        say("step 5/6: the oracle slice and the example files on the warehouse, in parallel")
        pairs = slice_pairs(self.a.catalog, self.schema, tables)
        if self.a.scope == "smoke":
            pairs = pairs[:SMOKE_STATEMENTS]
        shards = shard(pairs, self.a.shards)
        jobs: List[Tuple[str, subprocess.Popen, Path, Path, int]] = []
        for i, part in enumerate(shards):
            sql, out, log = self.run_statements(f"slice_{i}", part)
            jobs.append((f"slice_{i}", self._popen(self.uc_sql_argv(sql, out, self.schema), log), out, log, len(part)))
        ex_dir = self.run_dir / "examples"
        ex_log = self.run_dir / "examples.log"
        ex_argv = ["bash", str(CMD / "gbx-examples-uc-sql.sh"), "--profile", self.a.profile, "--catalog", self.a.catalog,
                   "--schema", self.schema, "--out-dir", str(ex_dir), "--timeout-s", "900"]
        if self.warehouse:
            ex_argv += ["--warehouse-id", self.warehouse]
        if self.a.scope == "smoke":
            ex_argv += ["--files", SMOKE_EXAMPLES]
        ex_pairs = example_pairs(self.a.catalog, self.schema, SMOKE_EXAMPLES if self.a.scope == "smoke" else None)
        ex_total = sum(len(p) for p in ex_pairs.values())
        jobs.append(("examples", self._popen(ex_argv, ex_log), ex_dir, ex_log, ex_total))
        start = time.time()
        while any(j[1].poll() is None for j in jobs):
            time.sleep(HEARTBEAT_S)
            slice_done = sum(count_results(o) for n, _, o, _, _ in jobs if n != "examples")
            ex_done = len(list(ex_dir.glob("*.json"))) if ex_dir.exists() else 0
            running = sum(j[1].poll() is None for j in jobs)
            say(f"warehouse: {int(time.time() - start)} s, slice {slice_done}/{len(pairs)} statements, examples {ex_done}/{len(ex_pairs)} files, {running} sessions running")
        for name, proc, _, log, _ in jobs:
            if proc.returncode not in (0, 1):  # 1 = the run finished and found failures: judged below
                raise StepFailed(f"{name} stopped (exit {proc.returncode}); see {log}")
        merged: dict = {}
        for name, _, out, _, _ in jobs:
            if name != "examples" and out.is_file():
                merged.update(oracles_load(out))
        cases = probes.uc_slice_cases(tables)
        ran = {cid for cid, _ in pairs}
        cases = [c for c in cases if c.cid in ran or "uc" not in c.surfaces]  # a smoke run judges only what it ran
        with tempfile.TemporaryDirectory(prefix="uc-it-judge-") as d:
            from databricks.labs.gbx.bench import oracles

            failures = oracles.check_uc_results(cases, merged, d)
        (self.run_dir / "slice_failures.txt").write_text("\n".join(failures))
        total = len([1 for c in cases if "uc" in c.surfaces])
        sl = judge_slice(failures, total, known)
        for line in failures[:30]:
            say("slice failure: " + " ".join(line.split())[:240])
        ex = self.judge_examples(ex_pairs, ex_dir)
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
            if self.ws_dir and self.created["ws"]:
                try:
                    w.workspace.delete(self.ws_dir, recursive=True)
                except Exception as exc:  # noqa: BLE001
                    notes.append(f"workspace folder delete failed: {str(exc)[:200]}")
        except Exception as exc:  # noqa: BLE001
            notes.append(f"cleanup error: {str(exc)[:200]}")
        return "dropped" if not notes else "; ".join(notes)

    # -- the run --
    def write_evidence(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        path = ADHOC_EVIDENCE if self.a.against else EVIDENCE
        path.write_text(json.dumps(self.ev, indent=2, sort_keys=True) + "\n")


def oracles_load(path: Path) -> dict:
    from databricks.labs.gbx.bench import oracles

    return oracles.load_uc_results(str(path))


def main(argv=None) -> int:
    a = parse_args(argv)
    if not a.profile:
        print("--profile required (a profile is never chosen for you: see `databricks auth profiles`)", file=sys.stderr)
        return 2
    g = Gate(a)
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
        pairs = slice_pairs(a.catalog, g.schema, tables)
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
            f"scope {a.scope}{' against ' + a.against if a.against else ''}: slice {len(pairs)} statements in {len(shard(pairs, a.shards))} shard(s); "
            f"examples: {sum(len(p) for p in ex_pairs.values())} statements in {len(ex_pairs)} files; "
            f"warehouse {g.warehouse or '-'}; known failures {len(known)}; no workspace call"
        )
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
        print("--warehouse-id required (or GBX_UC_WAREHOUSE_ID in examples/tests/databricks_cluster_config.env)", file=sys.stderr)
        return 2
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
        sl, ex = g.step_warehouse(tables, known)
        g.ev.update({"slice": {**sl, "scope": a.scope, "tables": list(tables)}, "examples": {k: ex[k] for k in ("statements_ok", "total")}})
        g.ev["examples"]["failed"] = ex["failures"][:40]
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
