#!/usr/bin/env python3
"""Run EVERY mutant of some kernel modules against one test selection and record what happened (guidance, not a gate).

The gate (gate.py) answers pass or fail for a test fold from a sample. This runner is the other half: it tries all the
mutants of the modules it is given, once, with the tests it is given, and writes a row per mutant (killed, survived,
timeout, segfault, absent), the first failing test of each killed mutant, and a diff for each survivor. It is meant for
a large node (gbx:test:mutation-databricks) and also runs on a host, with a small `--max-workers`, for a quick check.

It borrows the gate's helpers (module names, the throwaway copy, the mutmut pins) and adds three things the gate does
not need: a killing-test plugin, progress lines and a heartbeat while mutmut works, and caps that stop a run that has
stalled or has taken too long (the mutants tried so far are still written).

Outputs, in --out-dir: rows.jsonl, K.json, killers.jsonl, survivors.json, progress.json, run.json, mutmut.log.
"""

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import gate  # noqa: E402  (the helpers: norm_module, build_work_dir, collect_codes, the pins)
import guidance_rows as rows_mod  # noqa: E402

KILLERS_PLUGIN = "gbx_mut_killers"
PROGRESS_EVERY_S = 20.0
PRINT_EVERY_S = 60.0


def parse(argv):
    p = argparse.ArgumentParser(prog="guidance_runner", description=__doc__.split("\n\n")[0])
    p.add_argument("--family", required=True)
    p.add_argument("--modules", required=True, help="comma-separated kernel modules, e.g. pyvx/_validity,pyvx/_cleaning")
    p.add_argument("--tests", required=True, help="pytest args relative to python/geobrix, one quoted string")
    p.add_argument("--run-id", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--max-workers", type=int, default=max((os.cpu_count() or 4) - 2, 1), help="mutmut workers (default: cores - 2)")
    p.add_argument("--python", default=sys.executable, help="the python that has mutmut and pytest (its bin/mutmut runs)")
    p.add_argument("--git-sha", default="")
    p.add_argument("--purpose", default="")
    p.add_argument("--node-type", default="")
    p.add_argument("--oracle-tables", default="")
    p.add_argument("--timeout-multiplier", type=float, default=5.0, help="mutmut: a mutant may run this times the tests' own time (default 5)")
    p.add_argument("--timeout-constant", type=float, default=5.0, help="mutmut: plus this many seconds (default 5)")
    p.add_argument("--wall-cap-min", type=float, default=120.0, help="stop the run after this many minutes (rows so far are written)")
    p.add_argument("--setup-cap-min", type=float, default=30.0, help="stop when no mutant has been tried this long after the start")
    p.add_argument("--stall-min", type=float, default=15.0, help="stop when no mutant finished for this many minutes")
    p.add_argument("--diff-cap", type=int, default=400, help="at most this many survivor diffs (mutmut show)")
    p.add_argument("--heartbeat", default="", help="heartbeat file the launcher's watchdog reads (a Volume path)")
    p.add_argument("--progress-file", default="", help="also write progress.json here (a Volume path the launcher reads)")
    p.add_argument("--token", default="", help="the launch's token, written in each beat")
    p.add_argument("--plan", action="store_true", help="print the plan and stop")
    return p.parse_args(argv)


def guidance_section(modules, selection, args) -> str:
    """The gate's [tool.mutmut] block, with the killers plugin and the run's timeout settings."""
    sel = ["-p", gate.PLUGIN, "-p", KILLERS_PLUGIN, "-p", "no:cacheprovider", "-m", "not integration and not perf"]
    return (
        "\n[tool.mutmut]\n"
        f"source_paths = {json.dumps(modules)}\n"
        f"pytest_add_cli_args = {json.dumps(sel)}\n"
        f"pytest_add_cli_args_test_selection = {json.dumps(selection)}\n"
        'also_copy = ["src/", "test/"]\n'
        "mutate_only_covered_lines = true\n"
        f"timeout_multiplier = {args.timeout_multiplier}\n"
        f"timeout_constant = {args.timeout_constant}\n"
    )


def plan(args, modules, selection) -> dict:
    return {
        "family": args.family, "run_id": args.run_id, "modules": modules, "tests": selection,
        "max_workers": args.max_workers, "python": args.python, "out_dir": args.out_dir,
        "caps": {"wall_min": args.wall_cap_min, "setup_min": args.setup_cap_min, "stall_min": args.stall_min},
        "mutmut_section": guidance_section(modules, selection, args),
    }


def counts_of(codes: dict, modules: list) -> dict:
    out = {}
    for key, code in codes.items():
        m = rows_mod.module_of(key, modules)
        c = out.setdefault(m, {s: 0 for s in rows_mod.STATUSES})
        c[rows_mod.status_of(code)] += 1
    return out


def read_codes(work: Path) -> dict:
    try:
        return gate.collect_codes(work)
    except (ValueError, OSError):  # a meta file caught while mutmut rewrites it: the next poll reads it whole
        return {}


def read_durations(work: Path) -> dict:
    import glob

    out = {}
    for m in glob.glob(str(work / "mutants" / "**" / "*.py.meta"), recursive=True):
        try:
            out.update(json.load(open(m)).get("durations_by_key", {}))
        except (ValueError, OSError):
            pass
    return out


class Beat:
    """The heartbeat for the launcher's watchdog (the bench's Heartbeat class when the package is importable)."""

    def __init__(self, path, token, label, cap_s):
        self.hb = None
        if not path:
            return
        try:
            from databricks.labs.gbx.bench.liveness import Heartbeat

            self.hb = Heartbeat(path, interval=30.0, token=token, print_every=3600.0)
            self.hb.start()
            self.hb.begin(label, 1, 1, cap_s=cap_s)
        except Exception as exc:  # noqa: BLE001 - no heartbeat is not a reason to lose the run
            print(f"WARNING: no heartbeat ({type(exc).__name__}: {exc})", flush=True)

    def stop(self, state="done"):
        if self.hb is not None:
            self.hb.end()
            self.hb.stop(state)


def diffs_for(py: Path, work: Path, survivors: list, cap: int, env: dict) -> dict:
    """`mutmut show` for up to `cap` survivors, eight at a time (it forks a python each)."""
    out = {}
    exe = str(py.parent / "mutmut")

    def one(key):
        r = subprocess.run([exe, "show", key], cwd=work, env=env, capture_output=True, text=True, timeout=60)
        return key, r.stdout if r.returncode == 0 else None

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        for fut in [pool.submit(one, k) for k in survivors[:cap]]:
            try:
                k, d = fut.result()
                if d:
                    out[k] = d
            except Exception:  # noqa: BLE001 - a diff is a convenience
                pass
    return out


def main(argv=None) -> int:
    args = parse(argv)
    modules = [gate.norm_module(m) for m in args.modules.split(",") if m.strip()]
    missing = [m for m in modules if not (gate.PKG_ROOT / m).exists()]
    if missing:
        sys.exit(f"no such kernel module: {missing}")
    short = [m.removeprefix(gate.GBX_SRC + "/").removesuffix(".py") for m in modules]
    selection = gate.split_tests(args.tests)
    absent = [a for a in selection if not a.startswith("-") and not (gate.PKG_ROOT / a.split("::", 1)[0]).exists()]
    if absent:
        sys.exit(f"tests do not exist: {absent}")
    if args.plan:
        print(json.dumps(plan(args, modules, selection), indent=2))
        return 0

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    py = Path(args.python)
    mutmut_exe = py.parent / "mutmut"
    if not mutmut_exe.exists():
        sys.exit(f"{mutmut_exe} not found: install {gate.MUTMUT_PIN} and {gate.PYTEST_PIN} into the python at --python")

    work = Path(tempfile.mkdtemp(prefix=f"gbx-mutg-{args.family}-", dir=gate.tmp_root()))
    t0 = time.time()
    stopped = ""
    try:
        gate.build_work_dir(work)
        shutil.copy(HERE / "killers_plugin.py", work / "_gbx" / f"{KILLERS_PLUGIN}.py")
        (work / "pyproject.toml").write_text((gate.PKG_ROOT / "pyproject.toml").read_text() + guidance_section(modules, selection, args))
        env = dict(os.environ, PYTHONPATH=str(work / "_gbx"), PYTHONDONTWRITEBYTECODE="1", TZ="UTC",
                   PYSPARK_PYTHON=str(py), GBX_MUT_KILLERS=str(out / "killers.jsonl"))
        if args.oracle_tables:
            env["GBX_ORACLE_TABLES"] = args.oracle_tables
        (out / "killers.jsonl").write_text("")
        wall_cap_s = args.wall_cap_min * 60
        beat = Beat(args.heartbeat, args.token, f"mutants:{args.family}", wall_cap_s)
        print(f"[MUTATION {args.family}] {len(modules)} module(s), tests {selection}, {args.max_workers} workers, "
              f"caps: wall {args.wall_cap_min:g} min, setup {args.setup_cap_min:g} min, stall {args.stall_min:g} min", flush=True)

        # A collect pass first: a test file that cannot import (a missing test dependency, a conftest error) aborts
        # mutmut's stats pass with nothing on its log, and costs the cluster start for nothing. Say why, and stop.
        pre = subprocess.run(
            [str(py), "-m", "pytest", "--rootdir=.", "--collect-only", "-q", "-p", "no:randomly", *selection,
             "-p", gate.PLUGIN, "-p", KILLERS_PLUGIN, "-p", "no:cacheprovider", "-m", "not integration and not perf"],
            cwd=work, env=env, capture_output=True, text=True,
        )
        (out / "preflight.txt").write_text(pre.stdout + "\n--- stderr ---\n" + pre.stderr)
        tail = "\n".join((pre.stdout + pre.stderr).strip().splitlines()[-12:])
        if pre.returncode != 0:
            print(f"[MUTATION {args.family}] PRECONDITION MISSING: the selected tests do not collect in the copy "
                  f"(pytest rc {pre.returncode}); mutation not run. Last lines:\n{tail}", flush=True)
            beat.stop("failed")
            return 2
        print(f"[MUTATION {args.family}] collect pass ok: {tail.splitlines()[-1] if tail else ''}", flush=True)

        log = open(out / "mutmut.log", "w")
        proc = subprocess.Popen([str(mutmut_exe), "run", "--max-children", str(args.max_workers)],
                                cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
        state = {"tried": -1, "tried_at": time.time(), "last_print": 0.0}
        stop = threading.Event()

        def watch():
            nonlocal stopped
            while not stop.wait(PROGRESS_EVERY_S):
                now = time.time()
                codes = read_codes(work)
                tried = sum(1 for c in codes.values() if c is not None)
                if tried != state["tried"]:
                    state["tried"], state["tried_at"] = tried, now
                counts = counts_of(codes, short)
                prog = {"t": now, "elapsed_s": round(now - t0), "mutants": len(codes), "tried": tried, "modules": counts}
                (out / "progress.json").write_text(json.dumps(prog))
                if args.progress_file:
                    try:
                        Path(args.progress_file).write_text(json.dumps(prog))
                    except OSError:  # a Volume write that fails once is not worth the run
                        pass
                if now - state["last_print"] >= PRINT_EVERY_S:
                    state["last_print"] = now
                    per = "; ".join(f"{m} {sum(v[s] for s in rows_mod.STATUSES if s != 'unchecked')}/{sum(v.values())}"
                                    f" killed {v['killed']} survived {v['survived']}" for m, v in counts.items())
                    print(f"[MUTATION {args.family}] {round(now - t0)}s: tried {tried}/{len(codes)} mutants. {per}", flush=True)
                why = ""
                if now - t0 > wall_cap_s:
                    why = f"wall cap {args.wall_cap_min:g} min"
                elif tried <= 0 and now - t0 > args.setup_cap_min * 60:
                    why = f"no mutant tried {args.setup_cap_min:g} min after the start (setup cap)"
                elif tried > 0 and now - state["tried_at"] > args.stall_min * 60:
                    why = f"no mutant finished for {args.stall_min:g} min (stall cap)"
                if why:
                    stopped = why
                    print(f"[MUTATION {args.family}] STOPPING: {why}", flush=True)
                    proc.terminate()
                    return

        th = threading.Thread(target=watch, name="gbx-mutation-watch", daemon=True)
        th.start()
        try:
            rc = proc.wait()
        except BaseException:
            proc.kill()
            raise
        finally:
            stop.set()
            th.join(timeout=5)
            log.close()
        beat.stop("done" if not stopped else "failed")

        log_text = (out / "mutmut.log").read_text(errors="replace")
        reason = gate.aborted_reason(log_text)
        codes = read_codes(work)
        if reason and not any(c is not None for c in codes.values()):
            print(f"[MUTATION {args.family}] mutmut stopped before it ran the tests ({reason!r}); see mutmut.log", flush=True)
            print(log_text[-3000:], flush=True)
            return 2
        if not codes:
            print(f"[MUTATION {args.family}] mutmut produced no mutants (rc {rc}); see mutmut.log", flush=True)
            print(log_text[-3000:], flush=True)
            return 2

        durations = read_durations(work)
        killers = rows_mod.killers_from_jsonl(out / "killers.jsonl")
        survivors = sorted(k for k, c in codes.items() if rows_mod.status_of(c) == "survived")
        diffs = diffs_for(py, work, survivors, args.diff_cap, env) if survivors and not stopped else {}
        run = {"run_id": args.run_id, "run_ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t0)), "git_sha": args.git_sha,
               "family": args.family, "tests": args.tests, "node_type": args.node_type, "purpose": args.purpose}
        rows = rows_mod.rows_from_run(codes, modules=short, run=run, killers=killers, durations=durations, diffs=diffs)
        with open(out / "rows.jsonl", "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        (out / "K.json").write_text(json.dumps(codes, indent=0, sort_keys=True))
        (out / "survivors.json").write_text(json.dumps(survivors, indent=0))
        counts = counts_of(codes, short)
        meta = dict(run, modules=short, cores=os.cpu_count(), max_workers=args.max_workers, wall_s=round(time.time() - t0),
                    mutants=len(codes), counts=counts, stopped=stopped, mutmut_rc=rc,
                    kill_rate={m: rows_mod.kill_rate(c) for m, c in counts.items()}, pins=[gate.MUTMUT_PIN, gate.PYTEST_PIN])
        (out / "run.json").write_text(json.dumps(meta, indent=1))
        print(f"[MUTATION {args.family}] done in {meta['wall_s']}s: {len(codes)} mutants. "
              + "; ".join(f"{m} kill rate " + ("n/a" if k is None else f"{k:.1f}%") for m, k in meta["kill_rate"].items()), flush=True)
        return 3 if stopped else 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
