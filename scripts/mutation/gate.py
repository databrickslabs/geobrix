#!/usr/bin/env python3
"""The mutation gate for a test fold (gbx:test:mutation-gate).

Mutates a family's kernel modules in a throwaway copy of the tree and compares two test selections: the old tests
(about to be deleted) and the slim ones (the oracle-table rows and the few kept kernel files). The slim selection
passes when it kills at least 95% of the mutants the old one kills (judged on a 50-mutant sample, a 100-mutant
sample and all of them) and no module's kill rate falls by more than 5 points. A mutant the slim run does not
generate (mutmut mutates only covered lines) counts as NOT killed. Tier 0 only: tests that need a Spark session
are skipped (see tier0_plugin.py).

Standard library only; it drives mutmut 3.8.0 in a throwaway venv.
"""

import argparse
import ast
import collections
import glob
import json
import os
import random
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PKG_ROOT = REPO_ROOT / "python" / "geobrix"
GBX_SRC = "src/databricks/labs/gbx"
MUTMUT_PIN = "mutmut==3.8.0"
PYTEST_PIN = "pytest==9.1.1"  # mutmut 3.8.0 crashes under pytest 9.2.0 (no Function._nodeid)
THRESHOLD = 95.0  # percent of the old kills the slim run must keep
MODULE_DROP = 5.0  # points a module's kill rate may fall
SAMPLE_SIZES = (50, 100)
SAMPLE_SEED = 31
PLUGIN = "gbx_mut_tier0"


# ---------------------------------------------------------------------------------------------- arguments


def norm_module(spec: str) -> str:
    """'pygx/_bng', 'pygx._bng' or 'python/geobrix/src/databricks/labs/gbx/pygx/_bng.py' -> the path under src/."""
    s = spec.strip().replace("\\", "/")
    s = re.sub(r"^(python/geobrix/)?(src/databricks/labs/gbx/)?", "", s)
    if s.endswith(".py"):
        s = s[:-3]
    s = s.replace(".", "/")
    return f"{GBX_SRC}/{s}.py"


def norm_test_arg(arg: str) -> str:
    """Selection args are relative to python/geobrix (the pytest rootdir); accept repo-relative ones too."""
    prefix = "python/geobrix/"
    if arg.startswith("--") and "=" in arg:
        k, v = arg.split("=", 1)
        return f"{k}={v[len(prefix):] if v.startswith(prefix) else v}"
    return arg[len(prefix) :] if arg.startswith(prefix) else arg


def split_tests(spec: str) -> list:
    return [norm_test_arg(a) for a in shlex.split(spec)]


def selected_files(args: list, root: Path) -> list:
    """The .py files a selection names (node ids cut to their file; directories walked); options are skipped."""
    ignored = {a.split("=", 1)[1].rstrip("/") for a in args if a.startswith("--ignore=")}
    out = []
    for a in args:
        if a.startswith("-"):
            continue
        p = root / a.split("::", 1)[0]
        files = sorted(p.rglob("test_*.py")) if p.is_dir() else [p]
        for f in files:
            rel = f.relative_to(root).as_posix()
            if f.exists() and not any(rel == i or rel.startswith(i + "/") for i in ignored):
                out.append(f)
    return out


def pyspark_importers(files: list) -> list:
    """Files that import pyspark at module level (a Tier 0 warning: the fixtures are skipped, direct use is not)."""
    hits = []
    for f in files:
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError:
            continue
        for node in tree.body:
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                mods = [node.module]
            if any(m == "pyspark" or m.startswith("pyspark.") for m in mods):
                hits.append(f)
                break
    return hits


# ---------------------------------------------------------------------------------------------- the maths


def detected(code) -> bool:
    """mutmut's exit code: 1 a test failed, -24 timeout, -11 segfault count as killed; 0 survived; None unchecked."""
    return code not in (0, None)


def mutant_module(key: str) -> str:
    return key.rsplit(".", 1)[0].split(".")[-1]


def mutant_function(key: str) -> str:
    last = key.rsplit(".", 1)[1]
    last = re.sub(r"__mutmut_\d+$", "", last)
    return re.sub(r"^x(ǁ[^ǁ]*ǁ)?_?", "", last)


def sample(codes: dict, n: int, seed: int = SAMPLE_SEED) -> list:
    """n mutants, stratified by (module, function): shuffle the strata, take one mutant per stratum, round robin."""
    groups = collections.defaultdict(list)
    for k in sorted(codes):
        groups[(mutant_module(k), mutant_function(k))].append(k)
    rng = random.Random(seed)
    keys = sorted(groups)
    rng.shuffle(keys)
    for g in groups.values():
        rng.shuffle(g)
    pick = []
    while len(pick) < n:
        progressed = False
        for key in keys:
            if groups[key] and len(pick) < n:
                pick.append(groups[key].pop())
                progressed = True
        if not progressed:
            break
    return sorted(pick)


def kept(old: dict, new: dict, universe=None):
    """(old-killed in the universe, those the slim run kills too). A mutant absent from `new` is not killed."""
    keys = set(old) if universe is None else set(universe) & set(old)
    K = {k for k in keys if detected(old[k])}
    return K, {k for k in K if detected(new.get(k))}


def judge(old: dict, new: dict, samples: dict) -> dict:
    """The verdict. `samples` is {name: [mutant keys]}."""
    rows = {}
    for name, universe in (("sample50", samples.get("sample50")), ("sample100", samples.get("sample100")), ("all", None)):
        if universe is None and name != "all":
            continue
        K, ok = kept(old, new, universe)
        rows[name] = {
            "old_killed": len(K),
            "slim_kills_of_those": len(ok),
            "pct": (100.0 * len(ok) / len(K)) if K else 100.0,
            "lost": sorted(K - ok),
        }
    modules = {}
    by = collections.defaultdict(lambda: [0, 0, 0])
    for k in old:
        m = mutant_module(k)
        by[m][0] += 1
        by[m][1] += detected(old[k])
        by[m][2] += detected(new.get(k))
    for m, (n, o, s) in sorted(by.items()):
        modules[m] = {"mutants": n, "old_pct": 100.0 * o / n, "slim_pct": 100.0 * s / n, "delta": 100.0 * (s - o) / n}
    failures = [f"{n} keeps {r['pct']:.1f}% (< {THRESHOLD:.0f}%)" for n, r in rows.items() if r["pct"] < THRESHOLD]
    failures += [f"module {m} falls {-v['delta']:.1f} points (> {MODULE_DROP:.0f})" for m, v in modules.items() if v["delta"] < -MODULE_DROP]
    return {"pass": not failures, "failures": failures, "rows": rows, "modules": modules}


def format_report(family: str, v: dict, extra: dict) -> str:
    lines = [f"mutation gate: {family}: {'PASS' if v['pass'] else 'FAIL'}"]
    for name, r in v["rows"].items():
        lines.append(f"  {name:9s} |K_slim n K| / |K| = {r['slim_kills_of_those']}/{r['old_killed']} = {r['pct']:.1f}%  (lost {len(r['lost'])})")
    lines.append("  module           mutants  old%   slim%  delta")
    for m, x in v["modules"].items():
        lines.append(f"  {m:15s} {x['mutants']:7d} {x['old_pct']:6.1f} {x['slim_pct']:6.1f} {x['delta']:+6.1f}")
    for f in v["failures"]:
        lines.append(f"  FAIL: {f}")
    for k, val in extra.items():
        lines.append(f"  {k}: {val}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------- the work dir


def tmp_root() -> Path:
    return Path("/private/tmp") if Path("/private/tmp").is_dir() else Path("/tmp")


def host_venv() -> Path:
    return Path(os.environ.get("GBX_PYRX_VENV") or REPO_ROOT / ".venv-pyrx")


def mutmut_venv() -> Path:
    """The venv the gate runs mutmut from: per-user cache, never a shared temp path (it is executed)."""
    cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return Path(os.environ.get("GBX_MUTATION_VENV") or cache / "geobrix" / "mutation-venv")


def require_private(path: Path) -> None:
    """Refuse to run code from a path another user could have planted: it must be a real directory (not a
    link) owned by this user and not group- or world-writable."""
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022:
        sys.exit(
            f"{path} is not a private directory of this user (owner, mode or link). "
            f"Fix: remove it or point GBX_MUTATION_VENV at a directory you own with mode 0700, and run again."
        )


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def ensure_venv(venv: Path, host: Path) -> Path:
    """A throwaway venv with the pinned mutmut and pytest and a .pth onto the host venv's packages (shapely,
    pyproj, rasterio, pyspark are the repo's); nothing is installed into the host venv."""
    if venv.exists() or venv.is_symlink():
        require_private(venv)
    else:
        venv.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    py = venv / "bin" / "python"
    want = {"mutmut": MUTMUT_PIN.split("==")[1], "pytest": PYTEST_PIN.split("==")[1]}

    def versions():
        if not py.exists():
            return {}
        code = "import importlib.metadata as m,json;print(json.dumps({p:m.version(p) for p in %r}))" % list(want)
        r = subprocess.run([str(py), "-c", code], capture_output=True, text=True)
        try:
            return json.loads(r.stdout)
        except ValueError:
            return {}

    if versions() != want:
        if not py.exists():
            run(["uv", "venv", "--python", "3.12", str(venv)])
            venv.chmod(0o700)
        run(["uv", "pip", "install", "--python", str(py), MUTMUT_PIN, PYTEST_PIN])
        if versions() != want:
            sys.exit(f"mutation venv {venv} does not hold {MUTMUT_PIN} and {PYTEST_PIN}: {versions()}")
    host_sp = sorted(glob.glob(str(host / "lib" / "python3*" / "site-packages")))
    if not host_sp:
        sys.exit(f"host venv {host} has no site-packages (run gbx:venv:sync)")
    for sp in sorted(glob.glob(str(venv / "lib" / "python3*" / "site-packages"))):
        (Path(sp) / "gbx_host.pth").write_text(f"import site; site.addsitedir({host_sp[0]!r})\n")
    return py


IGNORE_COPY = shutil.ignore_patterns("__pycache__", "*.pyc", "coverage-report", ".pytest_cache")


def build_work_dir(work: Path) -> None:
    """src/ and test/ of python/geobrix, the pyproject (its pytest config), and the Tier 0 plugin."""
    shutil.copytree(PKG_ROOT / "src", work / "src", ignore=IGNORE_COPY)
    shutil.copytree(PKG_ROOT / "test", work / "test", ignore=IGNORE_COPY)
    shutil.copy(PKG_ROOT / "pyproject.toml", work / "pyproject.toml")
    (work / "_gbx").mkdir()
    shutil.copy(Path(__file__).with_name("tier0_plugin.py"), work / "_gbx" / f"{PLUGIN}.py")


def mutmut_section(modules: list, selection: list) -> str:
    sel = ["-p", PLUGIN, "-p", "no:cacheprovider", "-m", "not integration and not perf"]
    return (
        "\n[tool.mutmut]\n"
        f"source_paths = {json.dumps(modules)}\n"
        f"pytest_add_cli_args = {json.dumps(sel)}\n"
        f"pytest_add_cli_args_test_selection = {json.dumps(selection)}\n"
        'also_copy = ["src/", "test/"]\n'
        "mutate_only_covered_lines = true\n"
    )


def collect_codes(work: Path) -> dict:
    codes = {}
    for m in glob.glob(str(work / "mutants" / "**" / "*.py.meta"), recursive=True):
        codes.update(json.load(open(m)).get("exit_code_by_key", {}))
    return codes


def run_suite(label: str, work: Path, py: Path, modules: list, selection: list, args, log_dir: Path) -> dict:
    base = (PKG_ROOT / "pyproject.toml").read_text()
    (work / "pyproject.toml").write_text(base + mutmut_section(modules, selection))
    shutil.rmtree(work / "mutants", ignore_errors=True)
    env = dict(os.environ, PYTHONPATH=str(work / "_gbx"), PYTHONDONTWRITEBYTECODE="1", TZ="UTC")
    if args.oracle_tables:
        env["GBX_ORACLE_TABLES"] = args.oracle_tables
    env["PYSPARK_PYTHON"] = str(py)
    t0 = time.time()
    with open(log_dir / f"mutmut_{label}.log", "w") as log:
        proc = subprocess.run(
            [str(py.parent / "mutmut"), "run", "--max-children", str(args.max_children)],
            cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
    codes = collect_codes(work)
    print(f"[{label}] {len(codes)} mutants in {time.time() - t0:.0f}s (mutmut rc {proc.returncode}); log {log_dir}/mutmut_{label}.log", flush=True)
    if not codes:
        sys.exit(f"[{label}] mutmut produced no mutants; see {log_dir}/mutmut_{label}.log")
    return codes


# ---------------------------------------------------------------------------------------------- main


def parse(argv):
    p = argparse.ArgumentParser(prog="gbx:test:mutation-gate", description=__doc__.split("\n\n")[0])
    p.add_argument("--family", required=True, help="name of the family (names the work dir and the default output dir)")
    p.add_argument("--modules", required=True, help="comma-separated kernel modules, e.g. pygx/_bng,pygx/_quadbin")
    p.add_argument("--tests", required=True, help="the old tests: pytest args, relative to python/geobrix, one quoted string")
    p.add_argument("--slim-tests", required=True, help="the slim tests: pytest args, one quoted string")
    p.add_argument("--out", help="output dir (default test-logs/mutation-gate/<family>)")
    p.add_argument("--oracle-tables", default="", help="GBX_ORACLE_TABLES for the family harness, e.g. GRID_SCALAR,GRID_ARRAY")
    p.add_argument("--max-children", type=int, default=10)
    p.add_argument("--seed", type=int, default=SAMPLE_SEED)
    p.add_argument("--old-codes", help="reuse the K_old.json of an earlier run instead of running the old tests again "
                   "(for the next slim candidate of the same fold: the kernel modules and the old tests must be unchanged)")
    p.add_argument("--keep-work", action="store_true", help="keep the work dir (debugging)")
    p.add_argument("--plan", action="store_true", help="print the plan (work dir, config) and stop; runs nothing")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse(argv)
    modules = [norm_module(m) for m in args.modules.split(",") if m.strip()]
    missing = [m for m in modules if not (PKG_ROOT / m).exists()]
    if missing:
        sys.exit(f"no such kernel module: {missing}")
    old_sel, slim_sel = split_tests(args.tests), split_tests(args.slim_tests)
    out = Path(args.out) if args.out else REPO_ROOT / "test-logs" / "mutation-gate" / args.family
    if not out.is_absolute():
        out = REPO_ROOT / out
    prefix = f"gbx-mut-{args.family}-"
    work = tmp_root() / (prefix + "<random>")  # made by mkdtemp (mode 0700) when the run starts

    warn = []
    for label, sel in (("old", old_sel), ("slim", slim_sel)):
        files = selected_files(sel, PKG_ROOT)
        absent = [a for a in sel if not a.startswith("-") and not (PKG_ROOT / a.split("::", 1)[0]).exists()]
        if absent:
            sys.exit(f"{label} tests do not exist: {absent}")
        for f in pyspark_importers(files):
            warn.append(f"{label}: {f.relative_to(PKG_ROOT)} imports pyspark at module level (Tier 0 skips Spark fixtures only)")
    for w in warn:
        print("WARNING:", w, flush=True)
    if args.old_codes and not Path(args.old_codes).is_file():
        sys.exit(f"--old-codes {args.old_codes} is not a file. Fix: pass the K_old.json an earlier run wrote, or drop the option to run the old tests.")
    if args.plan:
        print(json.dumps({"work": str(work), "out": str(out), "modules": modules, "old": old_sel, "slim": slim_sel,
                          "venv": str(mutmut_venv()), "host_venv": str(host_venv()), "oracle_tables": args.oracle_tables,
                          "old_codes": args.old_codes, "mutmut": mutmut_section(modules, old_sel)}, indent=2))
        return 0

    out.mkdir(parents=True, exist_ok=True)
    py = ensure_venv(mutmut_venv(), host_venv())
    work = Path(tempfile.mkdtemp(prefix=prefix, dir=tmp_root()))
    try:
        build_work_dir(work)
        t0 = time.time()
        if args.old_codes:
            old = json.loads(Path(args.old_codes).read_text())
            print(f"[old] {len(old)} mutants reused from {args.old_codes}", flush=True)
        else:
            old = run_suite("old", work, py, modules, old_sel, args, out)
        slim = run_suite("slim", work, py, modules, slim_sel, args, out)
        samples = {f"sample{n}": sample(old, n, args.seed) for n in SAMPLE_SIZES}
        verdict = judge(old, slim, samples)
        survivors = sorted(k for k in old if not detected(old[k]))
        for name, data in (("K_old", old), ("K_slim", slim), ("sample50", samples["sample50"]), ("sample100", samples["sample100"]), ("survivors_old", survivors)):
            (out / f"{name}.json").write_text(json.dumps(data, indent=0, sort_keys=isinstance(data, dict)))
        (out / "lost.json").write_text(json.dumps({n: r["lost"] for n, r in verdict["rows"].items()}, indent=1))
        report = format_report(args.family, verdict, {"wall_s": f"{time.time() - t0:.0f}", "out": str(out), "seed": args.seed, "warnings": len(warn)})
        (out / "report.txt").write_text(report + "\n")
        (out / "verdict.json").write_text(json.dumps({k: v for k, v in verdict.items()}, indent=1))
        print(report)
        return 0 if verdict["pass"] else 1
    finally:
        if not args.keep_work:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
