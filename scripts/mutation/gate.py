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
import fcntl
import glob
import hashlib
import io
import json
import os
import random
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
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
TIMEOUT_SHARE = 0.20  # a pass in which more than this share of the judged mutants timed out proves nothing
TIMEOUT_MODULE_SHARE = 0.50  # nor does a module (of at least TIMEOUT_MODULE_MIN mutants) where more than this share did
TIMEOUT_MODULE_MIN = 5
SAMPLE_SIZES = (50, 100)
SAMPLE_FIRST, SAMPLE_EXTENDED = SAMPLE_SIZES  # sample-first mode: judge on 50 mutants, on 100 after any loss
SAMPLE_SEED = 31
GENERATE_ONLY = "gbx-generate-only"  # names no mutant: mutmut generates the mutants and collects stats, then stops
NO_TESTS = 33  # mutmut's exit code for a mutant no test reaches
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


NOT_KILLS = (0, None, 2, 5, 33, 34, 35, 37)  # survived, unchecked, interrupted, no tests (pytest 5, mutmut 33), skipped, suspicious, type-check
TIMEOUT_CODES = (-24, 24, 36, 152, 255)  # mutmut's "timeout" statuses (SIGXCPU, wall limit)


def detected(code) -> bool:
    """mutmut's exit code: 1 or 3 (a test failed), a timeout (-24), a segfault (-11) or any other crash is a kill.
    Survived (0), unchecked (None), interrupted, skipped and 'no tests ran' (5, 33) are not: no test saw the mutant."""
    return code not in NOT_KILLS


def timed_out(code) -> bool:
    return code in TIMEOUT_CODES


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


def invalid_timeouts(total: int, modules: dict) -> list:
    """A timeout counts as a kill (mutmut's convention), so a pass where most mutants 'died' by the clock proves
    nothing: the host was overloaded or the tests hang. Name each pass and module where that happened."""
    fix = "Fix: the host was overloaded or the tests hang; rerun with fewer workers (--max-children or GBX_MUTATION_MAX_WORKERS), ideally when the host is quiet."
    out = []
    for label, key in (("old", "timeouts_old"), ("slim", "timeouts_slim")):
        n = sum(m[key] for m in modules.values())
        if total and n > TIMEOUT_SHARE * total:
            out.append(f"INVALID run: {n} of {total} mutants timed out in the {label} pass (> {TIMEOUT_SHARE:.0%}). {fix}")
        for name, m in modules.items():
            if m["mutants"] >= TIMEOUT_MODULE_MIN and m[key] > TIMEOUT_MODULE_SHARE * m["mutants"]:
                out.append(f"INVALID run: {m[key]} of {m['mutants']} {name} mutants timed out in the {label} pass (> {TIMEOUT_MODULE_SHARE:.0%}). {fix}")
    return out


def judge(old: dict, new: dict, samples: dict, all_label: str = "all") -> dict:
    """The verdict. `samples` is {name: [mutant keys]}; the row over every mutant of `old` is named `all_label`."""
    rows = {}
    for name, universe in (("sample50", samples.get("sample50")), ("sample100", samples.get("sample100")), (all_label, None)):
        if universe is None and name != all_label:
            continue
        K, ok = kept(old, new, universe)
        rows[name] = {
            "old_killed": len(K),
            "slim_kills_of_those": len(ok),
            "pct": (100.0 * len(ok) / len(K)) if K else 100.0,
            "lost": sorted(K - ok),
        }
    modules = {}
    by = collections.defaultdict(lambda: [0, 0, 0, 0, 0])
    for k in old:
        m = mutant_module(k)
        by[m][0] += 1
        by[m][1] += detected(old[k])
        by[m][2] += detected(new.get(k))
        by[m][3] += timed_out(old[k])
        by[m][4] += timed_out(new.get(k))
    for m, (n, o, s, to, ts) in sorted(by.items()):
        modules[m] = {
            "mutants": n, "old_pct": 100.0 * o / n, "slim_pct": 100.0 * s / n, "delta": 100.0 * (s - o) / n,
            "timeouts_old": to, "timeouts_slim": ts,
        }
    failures = []
    if not rows[all_label]["old_killed"]:
        failures.append("the old run killed nothing: there is nothing to compare the slim run with")
    failures += [f"{n} keeps {r['pct']:.1f}% (< {THRESHOLD:.0f}%)" for n, r in rows.items() if r["pct"] < THRESHOLD]
    failures += [f"module {m} falls {-v['delta']:.1f} points (> {MODULE_DROP:.0f})" for m, v in modules.items() if v["delta"] < -MODULE_DROP]
    invalid = invalid_timeouts(len(old), modules)
    failures += invalid
    return {"pass": not failures, "invalid": bool(invalid), "failures": failures, "rows": rows, "modules": modules}


def format_report(family: str, v: dict, extra: dict) -> str:
    lines = [f"mutation gate: {family}: {'PASS' if v['pass'] else 'FAIL (invalid run: too many timeouts)' if v.get('invalid') else 'FAIL'}"]
    for name, r in v["rows"].items():
        lines.append(f"  {name:9s} |K_slim n K| / |K| = {r['slim_kills_of_those']}/{r['old_killed']} = {r['pct']:.1f}%  (lost {len(r['lost'])})")
    lines.append("  module           mutants  old%   slim%  delta  t/o old  t/o slim")
    for m, x in v["modules"].items():
        lines.append(f"  {m:15s} {x['mutants']:7d} {x['old_pct']:6.1f} {x['slim_pct']:6.1f} {x['delta']:+6.1f} {x['timeouts_old']:8d} {x['timeouts_slim']:9d}")
    for f in v["failures"]:
        lines.append(f"  FAIL: {f}")
    for k, val in extra.items():
        lines.append(f"  {k}: {val}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------- a run that proves nothing

ABORT_MARKERS = (
    "failed to collect stats",
    "Failed to run clean test",
    "Stopping early, because we could not find any test case",
)


def aborted_reason(log_text: str):
    """The mutmut message that says it stopped before running the tests against any mutant, or None."""
    return next((m for m in ABORT_MARKERS if m in log_text), None)


def require_old_kills(old: dict) -> None:
    """The old run is the yardstick. If it killed nothing, its tests did not run (a test file that fails to collect
    in the throwaway copy aborts mutmut's stats phase, and every mutant then stays unchecked): refuse, never pass."""
    if not any(detected(c) for c in old.values()):
        sys.exit(
            "old run killed nothing: the old selection did not run, so a pass would prove nothing. "
            "Fix: read mutmut_old.log in the output dir for the first failure (a test that cannot collect or "
            "that reads the repo tree breaks in the throwaway copy), drop that file from --tests, and run again."
        )


# ---------------------------------------------------------------------------------------------- the old tests at a ref


def resolve_ref(ref: str, repo: Path = REPO_ROOT) -> str:
    r = subprocess.run(["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"--old-ref {ref!r} is not a commit in this repository. Fix: pass a branch, tag or sha that holds the old tests (for example HEAD~1 or the commit before the fold).")
    return r.stdout.strip()


def exists_at_ref(arg: str, ref: str, repo: Path = REPO_ROOT) -> bool:
    """Does a selection arg (a file, a directory or a node id; relative to python/geobrix) exist at the ref?"""
    path = f"python/geobrix/{arg.split('::', 1)[0]}".rstrip("/")
    return subprocess.run(["git", "-C", str(repo), "cat-file", "-e", f"{ref}:{path}"], capture_output=True).returncode == 0


def export_ref_dir(ref: str, subdir: str, dest: Path, repo: Path = REPO_ROOT) -> None:
    """python/geobrix/<subdir> (test or src) as it was at the ref, written to dest (which must not exist)."""
    tar = subprocess.run(["git", "-C", str(repo), "archive", ref, f"python/geobrix/{subdir}"], capture_output=True, check=True).stdout
    with tempfile.TemporaryDirectory(prefix="gbx-mut-ref-") as t:
        with tarfile.open(fileobj=io.BytesIO(tar)) as tf:
            tf.extractall(t, filter="data")
        shutil.copytree(Path(t) / "python" / "geobrix" / subdir, dest, ignore=IGNORE_COPY)


def export_ref_tests(ref: str, dest: Path, repo: Path = REPO_ROOT) -> None:
    export_ref_dir(ref, "test", dest, repo)


def require_unchanged_since(ref: str, modules: list, repo: Path = REPO_ROOT) -> None:
    """The kernels are mutated from the current tree for both runs, so they must be what the old tests were
    written against: a kernel edited since the ref would make the old kills mean something else."""
    r = subprocess.run(["git", "-C", str(repo), "diff", "--name-only", ref, "--", *[f"python/geobrix/{m}" for m in modules]], capture_output=True, text=True, check=True)
    if r.stdout.strip():
        sys.exit(
            f"kernel modules changed since {ref}: {', '.join(r.stdout.split())}. "
            f"Fix: pass an --old-ref taken after the last kernel change (the fold itself must not edit the kernels), or gate without --old-ref."
        )


def old_fingerprint(module_files: list, test_files: list, ref_sha, old_sel: list) -> str:
    """What a saved kill set depends on: the kernel modules, the old selection and the old test files."""
    h = hashlib.sha1()
    for part in (ref_sha or "", *old_sel):
        h.update(part.encode() + b"\0")
    for f in sorted([*module_files, *([] if ref_sha else test_files)]):
        h.update(str(f).encode() + b"\0" + Path(f).read_bytes() + b"\0")
    return h.hexdigest()


def fingerprint_path(codes_file: Path) -> Path:
    return Path(codes_file).with_name("K_old.fingerprint")


def write_fingerprint(codes_file: Path, fp: str) -> None:
    fingerprint_path(codes_file).write_text(fp + "\n")


def check_fingerprint(codes_file: Path, fp: str) -> None:
    """Refuse a saved kill set whose kernels or old tests changed since it was written. A set from before
    fingerprints has nothing to compare and is accepted."""
    p = fingerprint_path(codes_file)
    if p.exists() and p.read_text().strip() != fp:
        sys.exit(
            f"{codes_file} was made from other kernel modules or old tests than these. "
            f"Fix: run again without --old-codes (it writes a fresh K_old.json and K_old.fingerprint), then reuse that."
        )


# ---------------------------------------------------------------------------------------------- the host's worker budget


def worker_capacity() -> int:
    """Workers all gates on this host may use together: GBX_MUTATION_MAX_WORKERS, else the cores less two."""
    env = os.environ.get("GBX_MUTATION_MAX_WORKERS", "").strip()
    if env:
        try:
            if int(env) >= 1:
                return int(env)
        except ValueError:
            pass
        sys.exit(f"GBX_MUTATION_MAX_WORKERS={env!r} is not a whole number of at least 1. Fix: unset it or set it to the workers all gates may use together.")
    return max(1, (os.cpu_count() or 4) - 2)


def slots_dir() -> Path:
    cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return Path(os.environ.get("GBX_MUTATION_SLOTS") or cache / "geobrix" / "mutation-slots")


class Lease:
    """Worker slots held by this process. Each is a file under an exclusive flock, which the kernel drops when the
    process ends, so a gate that crashed or was killed never leaves a slot behind."""

    def __init__(self, fds):
        self._fds = list(fds)

    @property
    def n(self) -> int:
        return len(self._fds)

    def release(self) -> None:
        for fd in self._fds:
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


class WorkerBudget:
    """A host-wide pool of worker slots, so that all concurrent gates together stay within `capacity` workers.

    A gate asks for its fair share (capacity / gates running, at most its own bound) and takes the free slots up
    to that; it always gets at least one, and waits for one if none is free. The pool lives in a directory of
    lock files: `slot-<i>` for each worker and `gate-<pid>` to say a gate is running.
    """

    def __init__(self, directory=None, capacity=None, wait_timeout=None, grace=20.0, progress=30.0,
                 clock=time.monotonic, sleep=time.sleep, say=None):
        self.dir = Path(directory) if directory else slots_dir()
        self.capacity = capacity or worker_capacity()
        env_wait = os.environ.get("GBX_MUTATION_SLOT_WAIT", "").strip()
        self.wait_timeout = wait_timeout if wait_timeout is not None else (float(env_wait) if env_wait else 3600.0)
        self.grace, self.progress, self.clock, self.sleep = grace, progress, clock, sleep
        self.say = say or (lambda msg: print(msg, flush=True))
        self._presence = None

    def _flock(self, path: Path, create: bool = True):
        """The fd of `path` with its lock taken, or None if another process holds it."""
        fd = os.open(path, os.O_RDWR | (os.O_CREAT if create else 0), 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return None
        return fd

    def register(self) -> None:
        """Say this gate is running (for the others' fair share); the lock goes with the process."""
        self.dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._presence = self._flock(self.dir / f"gate-{os.getpid()}")

    def unregister(self) -> None:
        if self._presence is not None:
            os.close(self._presence)
            (self.dir / f"gate-{os.getpid()}").unlink(missing_ok=True)
            self._presence = None

    def others(self) -> int:
        """Gates other than this one that are running. A presence file nobody locks is a dead gate's: removed."""
        n = 0
        for f in self.dir.glob("gate-*"):
            if f.name == f"gate-{os.getpid()}":
                continue
            fd = self._flock(f, create=False)
            if fd is None:
                n += 1
            else:
                os.close(fd)
                f.unlink(missing_ok=True)
        return n

    def _take(self, n: int):
        got = []
        for i in range(self.capacity):
            if len(got) >= n:
                break
            fd = self._flock(self.dir / f"slot-{i}")
            if fd is not None:
                os.ftruncate(fd, 0)
                os.write(fd, f"{os.getpid()}\n".encode())
                got.append(fd)
        return got

    def acquire(self, bound=None, label: str = "") -> Lease:
        """Take this gate's share of the free slots (at least one). `bound` is the most it wants (--max-children)."""
        self.dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        others = self.others()
        want = max(1, min(bound or self.capacity, self.capacity, self.capacity // (others + 1)))
        t0 = self.clock()
        fds, last = self._take(want), t0
        while not fds:
            if self.clock() - t0 >= self.wait_timeout:
                sys.exit(
                    f"no mutation worker slot became free in {self.wait_timeout:.0f}s ({self.capacity} slots, {self.others()} other gates running). "
                    f"Fix: wait for the other gates, or raise GBX_MUTATION_MAX_WORKERS (now {self.capacity}) or GBX_MUTATION_SLOT_WAIT."
                )
            if self.clock() - last >= self.progress:
                last = self.clock()
                self.say(f"[{label}] waiting for a free worker slot: all {self.capacity} are in use by {self.others()} other gates ({self.clock() - t0:.0f}s so far)")
            self.sleep(min(2.0, self.progress))
            fds = self._take(want)
        if len(fds) < want:  # others hold the rest: give them a moment to finish, then go with what we have
            deadline = self.clock() + self.grace
            while len(fds) < want and self.clock() < deadline:
                self.sleep(min(1.0, self.grace))
                fds += self._take(want - len(fds))
        self.say(f"[{label}] using {len(fds)} of {self.capacity} workers ({others} other gates running)")
        return Lease(fds)


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


def mangled_name(key: str) -> str:
    """mutmut's key for a function's stats: the mutant name without its __mutmut_<n> suffix."""
    return key.partition("__mutmut_")[0]


def judge_sample(old: dict, slim: dict, names: list) -> dict:
    """The verdict over the sampled mutants only. A sampled mutant the slim run did not generate is not killed."""
    return judge({k: old.get(k) for k in names}, {k: slim[k] for k in names if k in slim}, {}, all_label=f"sample{len(names)}")


def sampled_gate(universe, old_for, slim_for, seed: int = SAMPLE_SEED) -> dict:
    """Sample-first: judge on a stratified sample of 50 mutants of the old run, and on 100 after any loss.

    `universe` holds the mutants the old run generates. `old_for(names)` and `slim_for(names)` return {name: exit code}
    for the names they ran (a name the slim run did not generate is left out: not killed). Only sampled mutants are
    ever run. The 50-sample is the first half of the 100-sample, so the extension runs 50 more.
    """
    pool = dict.fromkeys(universe)
    first = sample(pool, SAMPLE_FIRST, seed)
    old = dict(old_for(first))
    require_old_kills(old)
    slim = dict(slim_for(first))
    names, samples = first, {f"sample{len(first)}": first}
    verdict = judge_sample(old, slim, names)
    extended = False
    if next(iter(verdict["rows"].values()))["lost"]:
        bigger = sample(pool, SAMPLE_EXTENDED, seed)
        if len(bigger) > len(first):
            extra = [k for k in bigger if k not in old]
            old.update(old_for(extra))
            require_old_kills(old)
            slim.update(slim_for(extra))
            names, extended = bigger, True
            samples[f"sample{len(bigger)}"] = bigger
            verdict = judge_sample(old, slim, names)
    return {
        "verdict": verdict, "names": names, "samples": samples, "extended": extended,
        "old": {k: old.get(k) for k in names}, "slim": {k: slim[k] for k in names if k in slim},
    }


class Side:
    """One test selection's mutmut passes, in a work dir of its own.

    `generate()` makes the mutants (mutmut also collects its stats then: one run of the selected tests) and stops;
    `run(names)` runs only those mutants; `run_all()` runs every one. The mutants keep their exit codes across calls.
    """

    def __init__(self, label, py, modules, selection, args, out, ref_sha=None, budget=None):
        self.label, self.py, self.modules, self.selection = label, py, modules, selection
        self.args, self.out, self.ref_sha, self.budget = args, out, ref_sha, budget
        self.work = None
        self.keys = None  # the mutants generated, sorted
        self.tested = set()  # mangled names mutmut's stats tie to at least one test
        self.codes = {}
        self.log = out / f"mutmut_{label}.log"
        self._logged = False

    def open(self) -> Path:
        """The throwaway copy. The old side of an in-place fold gets the tests and the source as they were at the ref."""
        if self.work is None:
            self.work = Path(tempfile.mkdtemp(prefix=f"gbx-mut-{self.args.family}-{self.label}-", dir=tmp_root()))
            build_work_dir(self.work)
            if self.ref_sha:
                for sub in ("test", "src"):
                    shutil.rmtree(self.work / sub)
                    export_ref_dir(self.ref_sha, sub, self.work / sub)
        return self.work

    def close(self) -> None:
        if self.work is not None and not self.args.keep_work:
            shutil.rmtree(self.work, ignore_errors=True)

    def _prepare(self) -> None:
        work = self.open()
        base = (PKG_ROOT / "pyproject.toml").read_text()
        (work / "pyproject.toml").write_text(base + mutmut_section(self.modules, self.selection))
        shutil.rmtree(work / "mutants", ignore_errors=True)

    def _invoke(self, names, what: str, bound=None) -> int:
        """One `mutmut run` (every mutant when `names` is empty). Its output is appended to the label's log.
        The workers come from the host's budget: at most `bound` (default --max-children)."""
        work = self.open()
        bound = bound or self.args.max_children
        if self.budget is None:
            return self._spawn(names, what, work, bound or 10)
        with self.budget.acquire(bound, self.label) as lease:
            return self._spawn(names, what, work, lease.n)

    def _spawn(self, names, what: str, work: Path, workers: int) -> int:
        env = dict(os.environ, PYTHONPATH=str(work / "_gbx"), PYTHONDONTWRITEBYTECODE="1", TZ="UTC")
        if self.args.oracle_tables:
            env["GBX_ORACLE_TABLES"] = self.args.oracle_tables
        env["PYSPARK_PYTHON"] = str(self.py)
        cmd = [str(self.py.parent / "mutmut"), "run", "--max-children", str(workers), *names]
        with open(self.log, "a" if self._logged else "w") as log:
            self._logged = True
            log.write(f"=== {what}\n")
            log.flush()
            proc = subprocess.run(cmd, cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
        reason = aborted_reason(self.log.read_text(errors="replace"))
        if reason:
            sys.exit(f"[{self.label}] mutmut stopped before it ran the tests ({reason!r}); the {self.label} selection did not run. Fix: read {self.log} for the first failure.")
        return proc.returncode

    def generate(self) -> list:
        """Make the mutants and collect stats; run none. mutmut stops on the one name that matches no mutant."""
        if self.keys is not None:
            return self.keys
        t0 = time.time()
        self._prepare()
        self._invoke([GENERATE_ONLY], "generate the mutants", bound=2)  # one stats run: it needs a worker or two only for libcst
        codes = collect_codes(self.work)
        if not codes:
            sys.exit(f"[{self.label}] mutmut produced no mutants; see {self.log}")
        self.keys = sorted(codes)
        stats = self.work / "mutants" / "mutmut-stats.json"
        self.tested = set(json.loads(stats.read_text()).get("tests_by_mangled_function_name", {})) if stats.exists() else set()
        print(f"[{self.label}] {len(self.keys)} mutants generated in {time.time() - t0:.0f}s; log {self.log}", flush=True)
        return self.keys

    def run(self, names) -> dict:
        """Run the named mutants and return {name: exit code} for those this side generated. A mutant no test
        reaches gets mutmut's own no-tests code, as in a run of everything (a named run would crash on it)."""
        keys = set(self.generate())
        todo = [n for n in dict.fromkeys(names) if n in keys and n not in self.codes]
        for n in todo:
            if mangled_name(n) not in self.tested:
                self.codes[n] = NO_TESTS
        todo = [n for n in todo if n not in self.codes]
        if todo:
            t0 = time.time()
            rc = self._invoke(todo, f"run {len(todo)} named mutants")
            got = collect_codes(self.work)
            self.codes.update({n: got.get(n) for n in todo})
            print(f"[{self.label}] {len(todo)} mutants in {time.time() - t0:.0f}s (mutmut rc {rc}); log {self.log}", flush=True)
        return {n: self.codes[n] for n in names if n in self.codes}

    def run_all(self) -> dict:
        """Run every mutant (--full)."""
        t0 = time.time()
        self._prepare()
        rc = self._invoke([], "run every mutant")
        codes = collect_codes(self.work)
        print(f"[{self.label}] {len(codes)} mutants in {time.time() - t0:.0f}s (mutmut rc {rc}); log {self.log}", flush=True)
        if not codes:
            sys.exit(f"[{self.label}] mutmut produced no mutants; see {self.log}")
        self.keys, self.codes = sorted(codes), codes
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
    p.add_argument("--max-children", type=int, default=None,
                   help="most mutmut workers this gate may use (default: its share of the host budget, GBX_MUTATION_MAX_WORKERS)")
    p.add_argument("--seed", type=int, default=SAMPLE_SEED)
    p.add_argument("--full", action="store_true",
                   help=f"run every mutant in the old and the slim pass (the default is a stratified sample of {SAMPLE_FIRST}, "
                        f"extended to {SAMPLE_EXTENDED} when the slim run loses a kill)")
    p.add_argument("--old-codes", help="reuse the K_old.json of an earlier run instead of running the old tests again "
                   "(for the next slim candidate of the same fold: the kernel modules and the old tests must be unchanged)")
    p.add_argument("--old-ref", help="git ref holding the old state: a fold that edits or deletes test files in place is gated against "
                   "their contents at that ref (the kernel modules must not have changed since). With --old-codes, the kill set must "
                   "have been made from that ref")
    p.add_argument("--keep-work", action="store_true", help="keep the work dirs (debugging)")
    p.add_argument("--plan", action="store_true", help="print the plan (work dir, config) and stop; runs nothing")
    return p.parse_args(argv)


def mode_file(codes_file: Path) -> Path:
    return Path(codes_file).with_name("K_old.mode")


def check_codes_mode(codes_file: Path, full: bool) -> None:
    """A kill set from a sampled run holds the sample only: a full run on it would judge the sample and call it everything."""
    p = mode_file(codes_file)
    if full and p.exists() and p.read_text().strip() != "full":
        sys.exit(
            f"{codes_file} holds a sample of the old run's kills ({p.read_text().strip()}), not all of them. "
            f"Fix: drop --full to gate on the sample, or run once with --full and no --old-codes to write a full kill set."
        )


def write_outputs(out: Path, family: str, verdict: dict, files: dict, extra: dict) -> str:
    for name, data in files.items():
        (out / f"{name}.json").write_text(json.dumps(data, indent=0, sort_keys=isinstance(data, dict)))
    (out / "lost.json").write_text(json.dumps({n: r["lost"] for n, r in verdict["rows"].items()}, indent=1))
    report = format_report(family, verdict, extra)
    (out / "report.txt").write_text(report + "\n")
    (out / "verdict.json").write_text(json.dumps(dict(verdict, mode=extra["mode"]), indent=1))
    return report


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
    work = tmp_root() / f"gbx-mut-{args.family}-<old|slim>-<random>"  # made by mkdtemp (mode 0700) when a pass starts

    ref_sha = resolve_ref(args.old_ref) if args.old_ref else None
    if ref_sha:
        require_unchanged_since(ref_sha, modules)

    warn = []
    for label, sel in (("old", old_sel), ("slim", slim_sel)):
        at_ref = label == "old" and ref_sha
        absent = [
            a for a in sel
            if not a.startswith("-") and not (exists_at_ref(a, ref_sha) if at_ref else (PKG_ROOT / a.split("::", 1)[0]).exists())
        ]
        if absent:
            sys.exit(f"{label} tests do not exist{f' at {args.old_ref}' if at_ref else ''}: {absent}")
        if at_ref:
            continue  # the files are read from the ref when the run starts
        for f in pyspark_importers(selected_files(sel, PKG_ROOT)):
            warn.append(f"{label}: {f.relative_to(PKG_ROOT)} imports pyspark at module level (Tier 0 skips Spark fixtures only)")
    for w in warn:
        print("WARNING:", w, flush=True)
    if args.old_codes and not Path(args.old_codes).is_file():
        sys.exit(f"--old-codes {args.old_codes} is not a file. Fix: pass the K_old.json an earlier run wrote, or drop the option to run the old tests.")
    fingerprint = old_fingerprint([PKG_ROOT / m for m in modules], selected_files(old_sel, PKG_ROOT) if not ref_sha else [], ref_sha, old_sel)
    if args.old_codes:
        check_fingerprint(Path(args.old_codes), fingerprint)
        check_codes_mode(Path(args.old_codes), args.full)
    mode = "full (every mutant)" if args.full else f"sample-first ({SAMPLE_FIRST} mutants, {SAMPLE_EXTENDED} after a loss)"
    if args.plan:
        print(json.dumps({"work": str(work), "out": str(out), "modules": modules, "old": old_sel, "slim": slim_sel,
                          "venv": str(mutmut_venv()), "host_venv": str(host_venv()), "oracle_tables": args.oracle_tables,
                          "old_codes": args.old_codes, "old_ref": ref_sha, "mode": mode,
                          "workers": {"host_capacity": worker_capacity(), "max_children": args.max_children, "slots": str(slots_dir())}, "mutmut": mutmut_section(modules, old_sel)}, indent=2))
        return 0

    out.mkdir(parents=True, exist_ok=True)
    py = ensure_venv(mutmut_venv(), host_venv())
    print(f"mode: {mode}", flush=True)
    budget = WorkerBudget()
    budget.register()
    old_side = Side("old", py, modules, old_sel, args, out, ref_sha, budget)
    slim_side = Side("slim", py, modules, slim_sel, args, out, budget=budget)
    t0 = time.time()
    try:
        old_known = json.loads(Path(args.old_codes).read_text()) if args.old_codes else {}
        if args.old_codes:
            print(f"[old] {len(old_known)} mutants reused from {args.old_codes}", flush=True)
        run = gate_full if args.full else gate_sample_first
        return run(args, out=out, old_side=old_side, slim_side=slim_side, old_known=old_known, fingerprint=fingerprint, warn=warn, t0=t0)
    finally:
        old_side.close()
        slim_side.close()
        budget.unregister()


def gate_full(args, *, out, old_side, slim_side, old_known, fingerprint, warn, t0) -> int:
    """Every mutant of the old run, in both passes."""
    old = old_known or old_side.run_all()
    require_old_kills(old)
    if not args.old_codes:  # keep the slow old run if the slim run cannot start
        (out / "K_old.json").write_text(json.dumps(old, indent=0, sort_keys=True))
        write_fingerprint(out / "K_old.json", fingerprint)
        mode_file(out / "K_old.json").write_text("full\n")
    slim = slim_side.run_all()
    samples = {f"sample{n}": sample(old, n, args.seed) for n in SAMPLE_SIZES}
    verdict = judge(old, slim, samples)
    survivors = sorted(k for k in old if not detected(old[k]))
    files = {"K_old": old, "K_slim": slim, "sample50": samples["sample50"], "sample100": samples["sample100"], "survivors_old": survivors}
    report = write_outputs(out, args.family, verdict, files, {"mode": "full (every mutant)", "wall_s": f"{time.time() - t0:.0f}", "out": str(out), "seed": args.seed, "warnings": len(warn)})
    write_fingerprint(out / "K_old.json", fingerprint)
    mode_file(out / "K_old.json").write_text("full\n")
    print(report)
    return 0 if verdict["pass"] else 1


def gate_sample_first(args, *, out, old_side, slim_side, old_known, fingerprint, warn, t0) -> int:
    """A stratified sample of the old run's mutants, in both passes; 100 when the slim run loses a kill."""
    universe = sorted(old_known) if args.old_codes else old_side.generate()

    def old_for(names):
        missing = [n for n in names if n not in old_known]
        if missing and not args.old_codes:
            old_known.update(old_side.run(missing))
        return {n: old_known[n] for n in names if n in old_known}

    res = sampled_gate(universe, old_for, slim_side.run, args.seed)
    verdict, names = res["verdict"], res["names"]
    mode = f"sample-first ({len(names)} of {len(universe)} mutants" + (f", extended from {SAMPLE_FIRST} after a loss)" if res["extended"] else ")")
    survivors = sorted(k for k in res["old"] if not detected(res["old"][k]))
    files = {"K_slim": res["slim"], **res["samples"], "survivors_old": survivors}
    if not args.old_codes:  # the sample's old kills: a later run can reuse them with --old-codes
        files["K_old"] = res["old"]
    report = write_outputs(out, args.family, verdict, files, {"mode": mode, "wall_s": f"{time.time() - t0:.0f}", "out": str(out), "seed": args.seed, "warnings": len(warn)})
    if not args.old_codes:
        write_fingerprint(out / "K_old.json", fingerprint)
        mode_file(out / "K_old.json").write_text(f"sample {len(names)}\n")
    print(report)
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
