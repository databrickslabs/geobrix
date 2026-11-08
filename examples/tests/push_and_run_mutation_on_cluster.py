"""gbx:test:mutation-databricks -- run EVERY mutant of some kernel modules on a large Databricks node (guidance, not a gate).

    push_and_run_mutation_on_cluster.py --family pyvx --modules pyvx/_validity,pyvx/_cleaning \\
        --tests "test/pyvx" --purpose "why this run" [--profile oauth-fe] [--node-type m5d.16xlarge] [--run-id ID] [--explain-only]

The local gate (gbx:test:mutation-gate) samples and decides a fold. This run tries all the mutants with `mutmut` on one
single-node job cluster (DBR 19, dedicated, on demand; it ends with the run), and records a row per mutant in the Delta table
`<catalog>.<schema>.mutation_results`, plus `test-logs/mutation-guidance/<run id>/summary.md` here. It never blocks a fold.

What it stages, to an isolated Volume folder `<volume>/mutation-runs/<run id>/` (removed at the end): the env6 wheel (the
dependencies and the native payload; sha256 checked by downloading it back) and a tarball of the COMMITTED tree at the
sha (`git archive`: python/geobrix/src, python/geobrix/test, the pyproject, scripts/mutation). The run mutates and tests
that tarball, not the wheel's copy of the package. Uncommitted edits are not in the run; the launcher says how many.

Options:
  --family <name>        names the run (a row column and the output folder)
  --modules <a,b>        kernel modules under src/databricks/labs/gbx, e.g. pyvx/_validity,pyvx/_cleaning
  --tests "<paths>"      pytest args relative to python/geobrix, one quoted string (Tier 0: Spark fixtures are skipped)
  --purpose "<why>"      REQUIRED on a launch (heads the notebook, rides on the run name; --explain-only may omit it)
  --profile <name>       Databricks profile (default DATABRICKS_CONFIG_PROFILE)
  --node-type <t>        default m5d.16xlarge (64 vCPU, local NVMe); the smoke uses m5d.8xlarge
  --run-id <id>          default mut-<date>-<time>-<4 hex>
  --sha <rev>            the commit to test (default HEAD)
  --wheel <path>         the env6 wheel (default: the one in dist/env6/ of the main checkout)
  --table <c.s.t>        default <GBX_BUNDLE_VOLUME_CATALOG>.<GBX_BUNDLE_VOLUME_SCHEMA>.mutation_results
  --oracle-tables <T,T>  GBX_ORACLE_TABLES for the family harness
  --wall-cap-min <n>     stop the mutants after this long (default 180); rows so far are kept
  --setup-cap-min <n>    stop when no mutant was tried this long after the node started the work (default 30)
  --stall-min <n>        stop when no mutant finished for this long (default 15)
  --start-cap-min <n>    cancel when the node has not beaten this long after the submit (default 20: a cold DBR 19 start)
  --keep-stage           leave the Volume folder and the runner notebook (default: remove them after the download)
  --explain-only         print the plan and exit; no network, nothing staged or started
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

from _cli import exit_if_help  # sibling module (script dir is on sys.path)
from _cluster_config import cluster_config_file  # sibling module
import _run_watchdog  # sibling module

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(os.environ.get("GBX_SOURCE_ROOT") or TESTS_DIR.parent.parent)
sys.path.insert(0, str(REPO_ROOT / "scripts" / "mutation"))

import guidance_rows  # noqa: E402  (pure: the rows, the summary, the table DDL)

DEFAULT_NODE = "m5d.16xlarge"
SPARK_VERSION = "19.x-scala2.13"
SUBSET = ("python/geobrix/src", "python/geobrix/test", "python/geobrix/pyproject.toml", "scripts/mutation")
MUTMUT_PIN = "mutmut==3.8.0"  # the pins the gate holds; scripts/mutation/gate.py is the source, test_gate_pins_match checks
PYTEST_PIN = "pytest==9.1.1"
EXIT_FAILED, EXIT_USAGE, EXIT_NO_START, EXIT_CAPPED = 1, 2, 4, 5
POLL_S = 10
STATUS_EVERY_S = 300


def load_config() -> None:
    f = cluster_config_file(TESTS_DIR)
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                if k.strip() and v.strip() and not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip()


def parse(argv):
    p = argparse.ArgumentParser(prog="push_and_run_mutation_on_cluster.py", add_help=False)
    p.add_argument("--family", required=False)
    p.add_argument("--modules")
    p.add_argument("--tests")
    p.add_argument("--purpose", default="")
    p.add_argument("--profile", default="")
    p.add_argument("--node-type", default=DEFAULT_NODE)
    p.add_argument("--run-id", default="")
    p.add_argument("--sha", default="HEAD")
    p.add_argument("--wheel", default="")
    p.add_argument("--table", default="")
    p.add_argument("--oracle-tables", default="")
    p.add_argument("--wall-cap-min", type=float, default=180.0)
    p.add_argument("--setup-cap-min", type=float, default=30.0)
    p.add_argument("--stall-min", type=float, default=15.0)
    p.add_argument("--start-cap-min", type=float, default=20.0)
    p.add_argument("--keep-stage", action="store_true")
    p.add_argument("--explain-only", action="store_true")
    return p.parse_args(argv)


def default_run_id() -> str:
    return f"mut-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def problems(a) -> list:
    out = []
    for flag in ("family", "modules", "tests"):
        if not getattr(a, flag):
            out.append(f"--{flag} is required")
    if not a.explain_only and not " ".join(a.purpose.split()):
        out.append("--purpose is required on a launch: say what the run tests and why (it heads the notebook and rides on the run name)")
    if a.run_id and not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", a.run_id):
        out.append("--run-id may hold letters, digits, '.', '_' and '-' only (it names a Volume folder)")
    for flag in ("wall_cap_min", "setup_cap_min", "stall_min", "start_cap_min"):
        if getattr(a, flag) <= 0:
            out.append(f"--{flag.replace('_', '-')} must be positive")
    return out


def sh(*cmd, cwd=None) -> str:
    return subprocess.run(cmd, cwd=cwd or REPO_ROOT, capture_output=True, text=True, check=True).stdout


def resolve_sha(rev: str) -> str:
    return sh("git", "rev-parse", "--verify", f"{rev}^{{commit}}").strip()


def uncommitted_in_subset() -> list:
    out = sh("git", "status", "--porcelain", "--", *SUBSET)
    return [ln[3:] for ln in out.splitlines() if ln.strip()]


def find_wheel(explicit: str) -> Path:
    if explicit:
        p = Path(explicit)
    else:
        found = sorted(REPO_ROOT.glob("dist/env6/geobrix-*+env6-py3-none-any.whl"))
        if len(found) != 1:
            sys.exit(
                f"PRECONDITION MISSING: expected one dist/env6/geobrix-*+env6-py3-none-any.whl, found {len(found)}; "
                "wheel stage not run. Fix: bash scripts/commands/gbx-build-wheel.sh --env 6, or pass --wheel <path>."
            )
        p = found[0]
    if not p.is_file():
        sys.exit(f"PRECONDITION MISSING: wheel {p} does not exist; run not started.")
    return p


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_tarball(sha: str, dest: Path) -> int:
    """git archive of the subset at `sha` (the committed tree: nothing uncommitted rides along). Returns the size."""
    have = sh("git", "ls-tree", "--name-only", sha, "scripts/mutation/guidance_runner.py").strip()
    if not have:
        sys.exit(f"{sha[:10]} has no scripts/mutation/guidance_runner.py: commit it first, or pass --sha of a commit that has it.")
    subprocess.run(["git", "archive", "--format=tar.gz", "-o", str(dest), sha, *SUBSET], cwd=REPO_ROOT, check=True)
    return dest.stat().st_size


def wheel_git_sha(wheel: Path) -> str:
    import zipfile

    try:
        with zipfile.ZipFile(wheel) as z:
            m = re.search(r'GIT_SHA = "([0-9a-f]+)"', z.read("databricks/labs/gbx/_build_info.py").decode())
            return m.group(1) if m else "?"
    except Exception:  # noqa: BLE001
        return "?"


def cores_of(node_type: str) -> int:
    m = re.search(r"\.(\d*)xlarge$", node_type)
    base = {"": 4, "2": 8, "4": 16, "8": 32, "9": 36, "12": 48, "16": 64, "18": 72, "24": 96, "32": 128}
    return base.get(m.group(1), 0) if m else 0


def make_plan(a, sha: str, wheel: Path, volroot: str, table: str) -> dict:
    run_id = a.run_id
    stage = f"{volroot}/mutation-runs/{run_id}"
    cores = cores_of(a.node_type)
    return {
        "run_id": run_id, "sha": sha, "family": a.family, "modules": [m for m in a.modules.split(",") if m.strip()],
        "tests": a.tests, "purpose": " ".join(a.purpose.split()), "node_type": a.node_type, "cores": cores or "?",
        "workers": max(cores - 2, 1) if cores else "cores - 2",
        "spark_version": SPARK_VERSION, "table": table, "stage": stage,
        "wheel": {"local": str(wheel), "name": wheel.name, "built_from": wheel_git_sha(wheel)},
        "tarball": f"{stage}/{run_id}-src.tar.gz", "out_dir": f"{stage}/out",
        "heartbeat": f"{stage}/heartbeat.json", "progress": f"{stage}/progress.json",
        "caps": {"wall_min": a.wall_cap_min, "setup_min": a.setup_cap_min, "stall_min": a.stall_min, "start_min": a.start_cap_min},
        "run_timeout_min": int(a.start_cap_min + a.wall_cap_min + 25),
        "local_out": f"test-logs/mutation-guidance/{run_id}",
        "pins": [MUTMUT_PIN, PYTEST_PIN],
    }


# ---------------------------------------------------------------------------------------------- the runner notebook


def header_md(plan: dict) -> str:
    c = plan["caps"]
    return (
        f"**Mutation guidance run `{plan['run_id']}`** (commit `{plan['sha'][:10]}`): {plan['purpose']}  \n"
        f"**Output:** a row per mutant in `{plan['table']}` (run_id = `{plan['run_id']}`: killed, survived, timeout, segfault, absent, "
        f"the killing test, a diff per survivor) and `rows.jsonl` / `run.json` in `{plan['out_dir']}`; the verdict per module is its kill rate.  \n"
        f"**Time:** the node starts in 5-15 min; then the stats pass and the mutants on {plan['cores']} vCPU. Upper bound {plan['run_timeout_min']} min "
        f"(mutants stop at {c['wall_min']:g} min).  \n"
        f"**Monitoring:** caps: wall {c['wall_min']:g} min, no mutant tried {c['setup_min']:g} min, none finished {c['stall_min']:g} min; "
        f"heartbeat `{plan['heartbeat']}` (the launcher cancels a stale one), progress `{plan['progress']}`. "
        f"Cancel: `databricks jobs cancel-run <run id> --profile <profile>` (the id is printed at submit; Ctrl-C of the launcher does not cancel)."
    )


def install_source(plan: dict) -> str:
    return (
        f"%pip install --quiet \"geobrix @ file://{plan['stage']}/{plan['wheel']['name']}\" {MUTMUT_PIN} {PYTEST_PIN}"
    )


def run_source(plan: dict, token: str, extra: dict) -> str:
    cfg = json.dumps(dict(plan, token=token, **extra), indent=1)
    return f'''import json, os, shutil, subprocess, sys, tarfile, time

CFG = json.loads({cfg!r})
t_start = time.time()
root = "/local_disk0/gbxmut"
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root + "/src")
shutil.copy(CFG["tarball"], root + "/src.tar.gz")  # one read of the Volume file, then local disk
with tarfile.open(root + "/src.tar.gz") as tf:
    tf.extractall(root + "/src", filter="data")
print("source unpacked:", sorted(os.listdir(root + "/src")), "cores:", os.cpu_count(), flush=True)
os.makedirs(CFG["stage"], exist_ok=True)

cmd = [sys.executable, root + "/src/scripts/mutation/guidance_runner.py",
       "--family", CFG["family"], "--modules", ",".join(CFG["modules"]), "--tests", CFG["tests"],
       "--run-id", CFG["run_id"], "--out-dir", root + "/out", "--max-workers", str(CFG["workers"]),
       "--python", sys.executable, "--git-sha", CFG["sha"], "--purpose", CFG["purpose"], "--node-type", CFG["node_type"],
       "--wall-cap-min", str(CFG["caps"]["wall_min"]), "--setup-cap-min", str(CFG["caps"]["setup_min"]),
       "--stall-min", str(CFG["caps"]["stall_min"]), "--heartbeat", CFG["heartbeat"], "--token", CFG["token"],
       "--progress-file", CFG["progress"]]
if CFG.get("oracle_tables"):
    cmd += ["--oracle-tables", CFG["oracle_tables"]]
env = dict(os.environ, PYTHONPATH="")  # the source tree is put on the path by the test config, not by the notebook
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=root + "/src")
for line in proc.stdout:  # progress lines between and within the modules, as the runner prints them
    print(line, end="", flush=True)
rc = proc.wait()
print("runner rc:", rc, "after", round(time.time() - t_start), "s", flush=True)
'''


def collect_source() -> str:
    return '''sys.path.insert(0, root + "/src/scripts/mutation")
import guidance_rows

out = root + "/out"
shutil.rmtree(CFG["out_dir"], ignore_errors=True)
shutil.copytree(out, CFG["out_dir"])  # rows.jsonl, run.json, killers.jsonl, K.json, mutmut.log ... to the Volume
result = {"rc": rc, "rows": 0}
if os.path.exists(out + "/rows.jsonl"):
    rows = guidance_rows.read_jsonl(out + "/rows.jsonl")
    trend = guidance_rows.previous_rates(spark, CFG["table"], CFG["family"], CFG["run_id"])
    json.dump(trend, open(CFG["out_dir"] + "/trend.json", "w"))
    result["rows"] = guidance_rows.write_delta(spark, rows, CFG["table"])
    result["kill_rate"] = json.load(open(out + "/run.json")).get("kill_rate")
    print("rows written to", CFG["table"], ":", result["rows"], flush=True)
else:
    print("no rows.jsonl: the runner did not finish its first pass (see mutmut.log in", CFG["out_dir"], ")", flush=True)
if rc not in (0, 3):  # 3: a cap stopped the mutants; the rows so far are written
    raise RuntimeError(f"guidance runner failed (rc {rc}); see preflight.txt and mutmut.log in " + CFG["out_dir"])
dbutils.notebook.exit(json.dumps(result))
'''


def cell(source: str, kind: str = "code") -> dict:
    c = {"cell_type": kind, "metadata": {}, "source": source.splitlines(keepends=True)}
    if kind == "code":
        c.update(outputs=[], execution_count=None)
    return c


def build_notebook(plan: dict, token: str) -> dict:
    extra = {"oracle_tables": plan.get("oracle_tables", "")}
    return {
        "cells": [
            cell(header_md(plan), "markdown"),
            cell(install_source(plan)),
            cell(run_source(plan, token, extra)),
            cell(collect_source()),
        ],
        "metadata": {"language_info": {"name": "python"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------------------------- the workspace side


def stage_file(w, local: Path, remote: str) -> None:
    """files.upload, then the remote size must equal the local one, and the downloaded bytes must hash the same."""
    from _artifact_volume import upload_verified

    if upload_verified(w, local, remote) != 0:
        sys.exit(f"staging {remote} failed (size check)")
    h = hashlib.sha256()
    body = w.files.download(remote).contents
    for chunk in iter(lambda: body.read(1 << 20), b""):
        h.update(chunk)
    if h.hexdigest() != sha256_file(local):
        sys.exit(f"staging {remote} failed: sha256 of the downloaded file differs from {local}")
    print(f"  verified sha256 {h.hexdigest()[:16]}... {remote}", flush=True)


def rm_volume_dir(w, path: str) -> None:
    try:
        for e in list(w.files.list_directory_contents(path)):
            if e.is_directory:
                rm_volume_dir(w, e.path)
            else:
                w.files.delete(e.path)
        w.files.delete_directory(path)
        print(f"Removed {path}")
    except Exception as exc:  # noqa: BLE001 - say what is left
        print(f"WARNING: could not remove {path} ({type(exc).__name__}: {exc}); delete it by hand.", file=sys.stderr)


def download_dir(w, remote: str, local: Path) -> list:
    local.mkdir(parents=True, exist_ok=True)
    got = []
    try:
        for e in w.files.list_directory_contents(remote):
            if e.is_directory:
                continue
            data = w.files.download(e.path).contents.read()
            (local / e.name).write_bytes(data)
            got.append(e.name)
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: download of {remote} stopped ({type(exc).__name__}: {exc})", file=sys.stderr)
    return got


def read_json(w, path: str):
    try:
        return json.loads(w.files.download(path).contents.read().decode())
    except Exception:  # noqa: BLE001 - absent until the node writes it
        return None


def status_line(prog: dict) -> str:
    per = "; ".join(
        f"{m}: {sum(v[s] for s in guidance_rows.STATUSES if s != 'unchecked')}/{sum(v.values())} killed {v['killed']} survived {v['survived']}"
        for m, v in (prog.get("modules") or {}).items()
    )
    return f"tried {prog.get('tried', 0)}/{prog.get('mutants', 0)} mutants after {prog.get('elapsed_s', 0)} s. {per}"


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
    sha = resolve_sha(a.sha)
    wheel = find_wheel(a.wheel)
    volroot = "/Volumes/%s/%s/%s" % tuple(
        (os.environ.get(k) or d).strip()
        for k, d in (("GBX_BUNDLE_VOLUME_CATALOG", "main"), ("GBX_BUNDLE_VOLUME_SCHEMA", "default"), ("GBX_BUNDLE_VOLUME_NAME", "geobrix_samples"))
    )
    table = a.table or f"{volroot.split('/')[2]}.{volroot.split('/')[3]}.mutation_results"
    plan = make_plan(a, sha, wheel, volroot, table)
    plan["oracle_tables"] = a.oracle_tables
    dirty = uncommitted_in_subset()
    if dirty:
        print(f"NOTE: {len(dirty)} uncommitted path(s) under the staged subset are NOT in this run (it tests {sha[:10]}): "
              + ", ".join(dirty[:6]) + (" ..." if len(dirty) > 6 else ""), file=sys.stderr)
    if a.explain_only:
        print(json.dumps(plan, indent=2))
        print("\nexplain-only: nothing staged or started.")
        return 0
    return launch(a, plan)


def launch(a, plan: dict) -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import compute, jobs
    from _job_submit import runner_dir, submit_once

    w = WorkspaceClient(profile=a.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or None)
    me = w.current_user.me().user_name
    run_id, stage = plan["run_id"], plan["stage"]
    token = uuid.uuid4().hex
    local_out = REPO_ROOT / plan["local_out"]
    local_out.mkdir(parents=True, exist_ok=True)

    tar_local = local_out / f"{run_id}-src.tar.gz"
    size = build_tarball(plan["sha"], tar_local)
    print(f"Source tarball of {plan['sha'][:10]}: {size / 1e6:.1f} MB; wheel {plan['wheel']['name']} (built from {plan['wheel']['built_from'][:10]})", flush=True)
    stage_file(w, Path(plan["wheel"]["local"]), f"{stage}/{plan['wheel']['name']}")
    stage_file(w, tar_local, plan["tarball"])
    tar_local.unlink()

    nb_path = f"{runner_dir(w)}/mutation_{run_id}.ipynb"
    nb_path = "/" + nb_path.strip().removeprefix("/Workspace").lstrip("/")
    from databricks.sdk.service.workspace import ImportFormat

    w.workspace.mkdirs(str(Path(nb_path).parent))
    w.workspace.upload(nb_path, io.BytesIO(json.dumps(build_notebook(plan, token), indent=1).encode()), format=ImportFormat.JUPYTER, overwrite=True)
    print(f"Uploaded runner notebook {nb_path}", flush=True)

    ok = False
    rc = EXIT_FAILED
    try:
        name = f"gbx-mutation-{run_id} | {plan['purpose'][:80]} | {plan['sha'][:10]}"
        timeout_s = plan["run_timeout_min"] * 60
        print(f"Submitting {name} on a new single-node job cluster ({plan['node_type']}, {SPARK_VERSION}), time limit {plan['run_timeout_min']} min...")
        spec = compute.ClusterSpec(
            spark_version=SPARK_VERSION, node_type_id=plan["node_type"], num_workers=0,
            data_security_mode=compute.DataSecurityMode.SINGLE_USER, single_user_name=me,
            spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*, 4]"},
            custom_tags={"ResourceClass": "SingleNode", "gbx_mutation": run_id[:64]},
            aws_attributes=compute.AwsAttributes(availability=compute.AwsAvailability.ON_DEMAND, first_on_demand=1, zone_id="auto"),
        )
        waiter = submit_once(
            w, run_name=name, timeout_seconds=timeout_s,
            tasks=[jobs.SubmitTask(
                task_key="run_mutation", new_cluster=spec, max_retries=0,
                description=f"{plan['purpose']} (commit {plan['sha'][:10]})"[:1000],
                notebook_task=jobs.NotebookTask(notebook_path=nb_path, source=jobs.Source.WORKSPACE),
            )],
        )
        job_run = waiter.run_id
        try:
            print(f"  run URL : {w.jobs.get_run(job_run).run_page_url}")
        except Exception:  # noqa: BLE001
            pass
        print(f"  results : {plan['table']}  (run_id = '{run_id}')")
        print(f"  heartbeat: {plan['heartbeat']}; cancel: databricks jobs cancel-run {job_run}")
        rc = watch(w, job_run, plan, token)
        ok = rc == 0
        got = download_dir(w, plan["out_dir"], local_out)
        for extra in ("progress.json", "heartbeat.json"):
            d = read_json(w, f"{stage}/{extra}")
            if d is not None:
                (local_out / extra).write_text(json.dumps(d))
        print(f"Downloaded {len(got)} file(s) to {local_out}: {', '.join(sorted(got))}")
        write_summary(local_out)
        if rc == 0:
            try:
                out = w.jobs.get_run_output(w.jobs.get_run(job_run).tasks[0].run_id)
                if out.notebook_output and out.notebook_output.result:
                    print("Notebook output:", out.notebook_output.result)
                    if json.loads(out.notebook_output.result).get("rc") == 3:
                        print("A cap stopped the mutants before all were tried: the rows are partial (status 'unchecked').", file=sys.stderr)
                        rc = EXIT_CAPPED
            except Exception:  # noqa: BLE001
                pass
    finally:
        if a.keep_stage:
            print(f"Kept {stage} and {nb_path} (--keep-stage).")
        else:
            rm_volume_dir(w, stage)
            if ok or rc in (_run_watchdog.EXIT_STALLED, EXIT_NO_START):
                try:
                    w.workspace.delete(nb_path)
                    print(f"Removed the runner notebook {nb_path}")
                except Exception as exc:  # noqa: BLE001
                    print(f"WARNING: could not remove {nb_path} ({exc}); delete it by hand.", file=sys.stderr)
            else:
                print(f"Kept the runner notebook {nb_path} (the run did not succeed; a repair run reads it).")
    return rc


def watch(w, job_run: int, plan: dict, token: str) -> int:
    c = plan["caps"]
    wd = _run_watchdog.Watchdog(form_cap_s=(c["wall_min"] + c["setup_min"]) * 60.0 + 600, stale_s=10 * 60.0, token=token)
    t0 = time.monotonic()
    last = {"say": 0.0, "beat": None, "err": None}

    def get_state():
        try:
            st = w.jobs.get_run(job_run).state
        except Exception as exc:  # noqa: BLE001 - transient: keep polling
            print(f"  (poll error: {exc})", file=sys.stderr)
            return ("RUNNING", "")

        def val(x):
            return x.value if x is not None and hasattr(x, "value") else (str(x) if x is not None else "")

        return (val(st.life_cycle_state), val(st.result_state))

    def beat():
        try:
            got = _run_watchdog.read_beat(w, plan["heartbeat"])
        except _run_watchdog.BeatReadError as exc:
            last["err"] = exc
            raise
        last["err"] = None
        if got is not None:
            last["beat"] = got
        return got

    def on_poll(lc):
        now = time.monotonic()
        if last["beat"] is None and now - t0 > c["start_min"] * 60:
            raise StartCap()
        if now - last["say"] >= STATUS_EVERY_S:
            last["say"] = now
            prog = read_json(w, plan["progress"])
            print(f"  [{lc}] {int(now - t0)} s: " + (status_line(prog) if prog else "no progress yet (node starting or in the stats pass)"), flush=True)

    try:
        kind, what = _run_watchdog.watch_run(
            get_state=get_state, read_beat_fn=beat, cancel=lambda: w.jobs.cancel_run(job_run), on_poll=on_poll,
            watchdog=wd, poll_secs=POLL_S, run_id=job_run,
        )
    except StartCap:
        print(f"No heartbeat {c['start_min']:g} min after the submit: the node did not start the work. Cancelling run {job_run}.", file=sys.stderr)
        try:
            w.jobs.cancel_run(job_run)
        except Exception as exc:  # noqa: BLE001
            print(f"cancel FAILED: {exc}; cancel run {job_run} by hand", file=sys.stderr)
        return EXIT_NO_START
    if kind == "stalled":
        print(f"Stalled run cancelled. Rows are not written when the notebook is cancelled; the files in {plan['out_dir']} may hold the partial run.", file=sys.stderr)
        return _run_watchdog.EXIT_STALLED
    run = w.jobs.get_run(job_run)
    result = getattr(run.state.result_state, "value", str(run.state.result_state))
    print(f"Run {job_run} finished: {result} after {int(time.monotonic() - t0)} s")
    return 0 if result == "SUCCESS" else EXIT_FAILED


class StartCap(Exception):
    pass


def write_summary(local_out: Path) -> None:
    rows_f, run_f = local_out / "rows.jsonl", local_out / "run.json"
    if not rows_f.exists() or not run_f.exists():
        print("No rows.jsonl / run.json came back: no summary.md written (see mutmut.log in the output folder).", file=sys.stderr)
        return
    rows = guidance_rows.read_jsonl(rows_f)
    run = json.loads(run_f.read_text())
    trend_f = local_out / "trend.json"
    trend = json.loads(trend_f.read_text()) if trend_f.exists() else None
    (local_out / "summary.md").write_text(guidance_rows.summary_md(rows, run, trend or None))
    print(f"Wrote {local_out / 'summary.md'}")


if __name__ == "__main__":
    sys.exit(main())
