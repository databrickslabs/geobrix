"""The shards of a test run on Databricks: read from the CI workflow, never listed here.

CI runs the Python tests as a matrix of shards (``.github/workflows/build_main.yml``, job ``light``), the tests of
``scripts/ci`` (job ``light-scripts``), the Python tests of the dirs the light shards leave out (``LIGHT_IGNORES`` in
``.github/actions/python_build/action.yml``; one heavy shard per dir here, so they run side by side) and the Scala
suites (job ``build``). ``load()`` turns those into ``Shard`` rows, so a cluster run and CI cannot drift: a shard added
to the matrix is a shard here.

Pure Python (PyYAML and git only): the tests of this module need no workspace.
"""

from __future__ import annotations

import ast
import re
import subprocess
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

WORKFLOW = ".github/workflows/build_main.yml"
HEAVY_ACTION = ".github/actions/python_build/action.yml"
LIGHT_ACTION = ".github/actions/pyrx_build/action.yml"
CONFTEST = "python/geobrix/test/conftest.py"
PKG = "python/geobrix"
SRC = "python/geobrix/src/databricks/labs/gbx"

POOL_LIGHT = "gbx-test-1n"  # single node, no JAR
POOL_JAR = "gbx-test-1n-jar"  # single node, the staged JAR
POOL_MN = "gbx-test-mn"  # 1 + 4 workers, the JAR: selectable, no shard maps to it by default

# What a shard is expected to take on the node, in minutes, from the CI times its workflow comments give (about 15 min
# at the longest). It heads the run and sizes the lanes; the caps judge the run, not this.
DEFAULT_EXPECTED_MIN = 12
FIXED_MIN = 0.7  # what one task costs before its first test: the cluster picks it up, installs the wheel, unpacks, imports
EXPECTED_MIN = {"pyrx": 17, "zarr": 9, "manager family": 9, "bench 2/3": 9, "bench 1/3": 13, "scripts/ci": 8}


@dataclass
class Shard:
    name: str  # as CI names it ("pyrx 1/2"), or "heavy <dir>", "scripts/ci", "scala"
    kind: str  # light | scripts | heavy | scala
    paths: List[str] = field(default_factory=list)  # relative to python/geobrix (relative to the repo root for scripts)
    ignore: List[str] = field(default_factory=list)
    part: str = ""  # "i/n": GBX_SHARD
    oracle_tables: str = ""  # GBX_ORACLE_TABLES
    delta: bool = False
    pool: str = POOL_LIGHT
    marker: str = "not integration"
    local_only: str = ""  # why it does not run on a cluster ("" when it does)
    est_min: float = 0.0  # measured/estimated minutes (from the timings of earlier runs); 0: use the CI-based table
    unsplittable: bool = False  # one test file the shard cannot be cut below (the family oracle tables)

    @property
    def group(self) -> str:
        """The name without its ``i/n`` suffix: ``--shards pyrx`` selects both halves."""
        return re.sub(r"\s+\d+/\d+$", "", self.name)

    @property
    def slug(self) -> str:
        return re.sub(r"[^a-z0-9]+", "-", self.name.lower()).strip("-")

    @property
    def expected_min(self) -> float:
        if self.est_min:
            return self.est_min
        for key in (self.name, self.group):
            if key in EXPECTED_MIN:
                return EXPECTED_MIN[key]
        return DEFAULT_EXPECTED_MIN


# ---------------------------------------------------------------------------------------------- reading CI


def _yaml(path: Path):
    import yaml

    return yaml.safe_load(path.read_text())


def light_test_dirs(root: Path) -> List[str]:
    """``_LIGHT_TEST_DIRS`` of the test conftest (read from its source: the conftest imports pytest)."""
    tree = ast.parse((root / CONFTEST).read_text())
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "_LIGHT_TEST_DIRS" for t in n.targets):
            return list(ast.literal_eval(n.value))
    raise ValueError(f"no _LIGHT_TEST_DIRS in {CONFTEST}")


def shard_of(root: Path):
    """The conftest's own ``shard_of(relpath, n)``: which part of a split run a test file belongs to."""
    tree = ast.parse((root / CONFTEST).read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "shard_of")
    ns = {"zlib": zlib}
    exec(compile(ast.Module([fn], []), CONFTEST, "exec"), ns)  # noqa: S102 - the repo's own function, as CI's guard does
    return ns["shard_of"]


def light_shards(root: Path) -> List[Shard]:
    """One shard per entry of the ``light`` job's matrix."""
    rows = _yaml(root / WORKFLOW)["jobs"]["light"]["strategy"]["matrix"]["shard"]
    out = []
    for r in rows:
        out.append(
            Shard(
                name=r["name"], kind="light", paths=str(r.get("paths", "")).split(), ignore=str(r.get("ignore", "")).split(),
                part=str(r.get("part", "")), oracle_tables=str(r.get("oracle_tables", "")), delta=bool(r.get("delta", False)),
                unsplittable=bool(r.get("oracle_tables", "")),
            )
        )
    return out


def scripts_shard(root: Path) -> Optional[Shard]:
    """The ``light-scripts`` job: pytest over ``scripts/ci`` from the repo root."""
    job = _yaml(root / WORKFLOW)["jobs"].get("light-scripts")
    if not job:
        return None
    for step in job.get("steps", []):
        m = re.search(r"python -m pytest (\S+)", str(step.get("run", "")))
        if m:
            return Shard(name=m.group(1), kind="scripts", paths=[m.group(1)], marker="")
    return None


def heavy_ignores(root: Path) -> List[str]:
    """The dirs the heavy build leaves to the light shards: ``LIGHT_IGNORES`` of the heavy action (``test/pyrx`` ...)."""
    text = (root / HEAVY_ACTION).read_text()
    m = re.search(r'LIGHT_IGNORES="([^"]*)"', text)
    if not m:
        raise ValueError(f"no LIGHT_IGNORES in {HEAVY_ACTION}")
    return [x.split("=", 1)[1] for x in m.group(1).split() if x.startswith("--ignore=")]


_NOT_TESTS = {"__pycache__", "coverage-report", "_plugins"}


def heavy_shards(root: Path) -> List[Shard]:
    """One shard per test dir the light tier does not cover, and one for the test files at the top of ``test/``."""
    ignored = {Path(p).name for p in heavy_ignores(root)}
    test_root = root / PKG / "test"
    out = []
    for d in sorted(p for p in test_root.iterdir() if p.is_dir() and p.name not in ignored | _NOT_TESTS):
        if any(d.rglob("test_*.py")) or any(d.rglob("*_test.py")):
            out.append(Shard(name=f"heavy {d.name}", kind="heavy", paths=[f"test/{d.name}"], pool=POOL_JAR))
    loose = sorted(p.name for p in test_root.glob("test_*.py"))
    if loose:
        out.append(Shard(name="heavy root files", kind="heavy", paths=[f"test/{n}" for n in loose], pool=POOL_JAR))
    return out


def scala_shard() -> Shard:
    return Shard(
        name="scala", kind="scala", pool="", local_only="needs Maven and the Scala sources built in the dev container: run gbx:test:scala",
    )


def load(root: Path) -> List[Shard]:
    root = Path(root)
    out = light_shards(root)
    s = scripts_shard(root)
    if s:
        out.append(s)
    out += heavy_shards(root)
    out.append(scala_shard())
    names = [x.name for x in out]
    dup = {n for n in names if names.count(n) > 1}
    if dup:
        raise ValueError(f"duplicate shard names: {sorted(dup)}")
    return out


# ---------------------------------------------------------------------------------------------- selecting


def select(shards: List[Shard], spec: str, changed: Optional[Iterable[str]] = None, root: Optional[Path] = None) -> List[Shard]:
    """The shards ``--shards`` names: ``all-light``, ``all-heavy``, ``affected``, or names (a name, or a group such as
    ``pyrx`` for both of its halves), comma separated. Unknown names raise ValueError (with the known ones)."""
    runnable = [s for s in shards if not s.local_only]
    out: List[Shard] = []

    def add(items):
        for s in items:
            if s not in out:
                out.append(s)

    for tok in [t.strip() for t in spec.split(",") if t.strip()]:
        if tok == "all-light":
            add(s for s in runnable if s.kind in ("light", "scripts"))
        elif tok == "all-heavy":
            add(s for s in runnable if s.kind == "heavy")
        elif tok == "affected":
            if changed is None or root is None:
                raise ValueError("affected needs the changed files")
            add(affected(runnable, changed, root))
        else:
            hit = [s for s in shards if tok in (s.name, s.group, s.slug)]
            if not hit:
                raise ValueError(f"unknown shard {tok!r}; known: {', '.join(sorted({s.group for s in shards}))}")
            local = [s for s in hit if s.local_only]
            if local:
                raise ValueError(f"shard {local[0].name!r} does not run on a cluster: {local[0].local_only}")
            add(hit)
    return out


def changed_files(root: Path, base: str, sha: str) -> List[str]:
    """Files ``sha`` changed since it left ``base`` (a three-dot diff, from their merge base): what a push of ``sha`` adds. A
    two-dot diff would also list what ``base`` gained meanwhile, files this branch never touched."""
    res = subprocess.run(["git", "diff", "--name-only", f"{base}...{sha}"], cwd=root, capture_output=True, text=True, check=True)
    return [ln for ln in res.stdout.splitlines() if ln.strip()]


def _covers(shard: Shard, rel: str, shard_fn) -> bool:
    """Whether ``shard`` runs the test file ``rel`` (a path from python/geobrix, ``test/...``)."""
    if not any(rel == p or rel.startswith(p.rstrip("/") + "/") for p in shard.paths):
        return False
    if any(rel == p or rel.startswith(p.rstrip("/") + "/") for p in shard.ignore):
        return False
    if shard.part:
        i, n = (int(x) for x in shard.part.split("/"))
        return shard_fn(rel, n) == i
    return True


# Source dirs that every light test can reach: a change there selects every light shard.
_SHARED_SRC = ("core", "_compat", "_native", "_rs", "manifest", "manager", "engine", "uc")
_HEAVY_SRC = ("rasterx", "gridx", "vectorx")


def affected(shards: List[Shard], changed: Iterable[str], root: Path) -> List[Shard]:
    """The shards that hold a test touched by ``changed`` (paths from the repo root).

    * a test file selects the shards that run it (a split dir: the part that holds it);
    * a source file under ``src/databricks/labs/gbx/<pkg>/`` selects the shards of ``test/<pkg>``; a shared package
      (``core``, ``manager``, ...) or a top-level module selects every light shard; ``rasterx``/``gridx``/``vectorx`` the heavy
      shard of the same name;
    * the test conftest, the package pyproject or a requirements file selects every light shard;
    * a file under ``scripts/ci`` or the workflow/action files select the ``scripts/ci`` shard.
    """
    fn = shard_of(root)
    pick: List[Shard] = []

    def add(items):
        for s in items:
            if s not in pick:
                pick.append(s)

    light = [s for s in shards if s.kind == "light"]
    for f in changed:
        if f.startswith(PKG + "/test/"):
            rel = f[len(PKG) + 1:]
            if rel.endswith("conftest.py") and rel.count("/") == 1:
                add(light)
                continue
            add(s for s in shards if s.kind in ("light", "heavy") and _covers(s, rel, fn))
            if rel.endswith("conftest.py"):  # a dir's conftest: every shard of the dir
                d = rel.rsplit("/", 1)[0]
                add(s for s in shards if s.kind in ("light", "heavy") and any(p == d or p.startswith(d + "/") for p in s.paths))
        elif f.startswith(SRC + "/"):
            rest = f[len(SRC) + 1:]
            top = rest.split("/", 1)[0]
            is_module = "/" not in rest
            if is_module or top in _SHARED_SRC:
                add(light)
            elif top in _HEAVY_SRC:
                add(s for s in shards if s.kind == "heavy" and s.name == f"heavy {top}")
            else:
                dirs = f"test/{top}"
                add(s for s in shards if s.kind in ("light", "heavy") and any(p == dirs or p.startswith(dirs + "/") for p in s.paths))
        elif f in (f"{PKG}/pyproject.toml",) or f.startswith(PKG + "/requirements"):
            add(light)
        elif f.startswith("scripts/ci/") or f in (WORKFLOW, LIGHT_ACTION, HEAVY_ACTION) or f.startswith(".github/actions/"):
            add(s for s in shards if s.kind == "scripts")
    order = {s.name: i for i, s in enumerate(shards)}
    return sorted(pick, key=lambda s: order[s.name])


# ---------------------------------------------------------------------------------------------- lanes


def lanes(shards: List[Shard], n: int) -> List[List[Shard]]:
    """Spread ``shards`` over at most ``n`` lanes, longest expected first onto the lightest lane (the tasks of a lane run
    one after the other, so ``n`` is how many shards run at once on a node). Returns the non-empty lanes."""
    n = max(1, n)
    out: List[List[Shard]] = [[] for _ in range(min(n, max(len(shards), 1)))]
    load = [0] * len(out)
    for s in sorted(shards, key=lambda s: (-s.expected_min, s.name)):
        i = load.index(min(load))
        out[i].append(s)
        load[i] += s.expected_min
    return [lane for lane in out if lane]


def pytest_args(shard: Shard) -> List[str]:
    """The arguments of the shard's pytest process: what CI's action passes, in the same order."""
    args = list(shard.paths)
    for p in shard.ignore:
        args.append(f"--ignore={p}")
    if shard.marker:
        args += ["-m", shard.marker]
    return args


def shard_env(shard: Shard) -> Dict[str, str]:
    env = {}
    if shard.part:
        env["GBX_SHARD"] = shard.part
    if shard.oracle_tables:
        env["GBX_ORACLE_TABLES"] = shard.oracle_tables
    return env


# ---------------------------------------------------------------------------------------------- finer shards, by file


def files_of(shard: Shard, root: Path) -> Optional[List[str]]:
    """The test files the shard runs, as paths from its working dir (``test/...``; ``scripts/ci/...`` for scripts), or None
    when it cannot be cut by file (an oracle-table shard is one file)."""
    if shard.unsplittable or shard.local_only:
        return None
    base = root if shard.kind == "scripts" else root / PKG
    fn = shard_of(root) if shard.part else None
    out = set()
    for p in shard.paths:
        path = base / p
        found = [path] if path.is_file() else [*path.rglob("test_*.py"), *path.rglob("*_test.py")]
        for f in found:
            rel = f.relative_to(base).as_posix()
            if any(rel == i or rel.startswith(i.rstrip("/") + "/") for i in shard.ignore):
                continue
            if shard.part:
                i, n = (int(x) for x in shard.part.split("/"))
                if fn(rel, n) != i:
                    continue
            out.add(rel)
    return sorted(out)


def file_minutes(files: List[str], file_seconds: Dict[str, float], default_s: float = 20.0) -> Dict[str, float]:
    return {f: file_seconds.get(f, default_s) / 60.0 for f in files}


def estimate(shard: Shard, root: Path, file_seconds: Dict[str, float]) -> float:
    """Minutes the shard takes: the measured seconds of its files plus the fixed cost of a task; the table when nothing is known.
    An oracle-table shard runs one file for some of its tables, so the file's total says nothing about it: the table counts."""
    if shard.oracle_tables:
        return float(shard.est_min or shard.expected_min)
    files = files_of(shard, root)
    known = [f for f in (files or []) if f in file_seconds]
    if files and len(known) >= max(1, len(files) // 2):
        return FIXED_MIN + sum(file_minutes(files, file_seconds).values())
    return float(shard.expected_min)


def split_tables(shard: Shard, k: int) -> List[Shard]:
    """Cut an oracle-table shard (one test file, run once per table) into ``k`` shards of whole tables, equal in number: a
    table takes about the same time as another, and the table list is CI's own."""
    tables = [t for t in shard.oracle_tables.split(",") if t]
    k = min(k, len(tables))
    if k <= 1:
        return [shard]
    per_table = max((shard.expected_min - FIXED_MIN), 0.5) / len(tables)
    parts = [tables[i::k] for i in range(k)]  # a round robin: neighbouring tables are often alike in size
    return [
        Shard(name=f"{shard.name} [{i}/{k}]", kind=shard.kind, paths=list(shard.paths), ignore=list(shard.ignore), part=shard.part,
              oracle_tables=",".join(ts_), delta=shard.delta, pool=shard.pool, marker=shard.marker,
              est_min=round(FIXED_MIN + per_table * len(ts_), 2), unsplittable=True)
        for i, ts_ in enumerate(parts, 1)
    ]


def split_shard(shard: Shard, k: int, root: Path, file_seconds: Dict[str, float]) -> List[Shard]:
    """Cut ``shard`` into ``k`` shards of whole files, balanced by the measured seconds of each file (longest first onto the
    lightest part), so that no part is much longer than another. A shard that cannot be cut by file comes back as it is."""
    if shard.oracle_tables:
        return split_tables(shard, k)
    files = files_of(shard, root)
    if not files or k <= 1:
        return [shard]
    k = min(k, len(files))
    per_file_s = max((shard.expected_min - FIXED_MIN) * 60.0 / len(files), 1.0)  # a file nobody timed shares the table's minutes
    mins = file_minutes(files, file_seconds, per_file_s)
    parts: List[List[str]] = [[] for _ in range(k)]
    load = [0.0] * k
    for f in sorted(files, key=lambda f: (-mins[f], f)):
        i = load.index(min(load))
        parts[i].append(f)
        load[i] += mins[f]
    out = []
    for i, (fs, m) in enumerate(zip(parts, load), 1):
        out.append(
            Shard(name=f"{shard.name} [{i}/{k}]", kind=shard.kind, paths=sorted(fs), ignore=[], part="", oracle_tables="",
                  delta=shard.delta, pool=shard.pool, marker=shard.marker, est_min=round(FIXED_MIN + m, 2))
        )
    return out


def auto_split(shards: List[Shard], root: Path, file_seconds: Dict[str, float], target_min: float) -> List[Shard]:
    """Cut every shard longer than ``target_min`` (by the timings, else the table) into parts of about that length. A task
    pays about FIXED_MIN before its first test, so ``target_min`` should stay several times that."""
    out: List[Shard] = []
    for s in shards:
        est = estimate(s, root, file_seconds)
        if (s.unsplittable and not (s.oracle_tables and not s.name.endswith("]"))) or est <= target_min * 1.25:
            out.append(Shard(**{**s.__dict__, "est_min": round(est, 2)}))
            continue
        k = max(2, round((est - FIXED_MIN) / (target_min - FIXED_MIN)))
        out += split_shard(s, k, root, file_seconds)
    return out


def pytest_file_args(shard: Shard) -> List[str]:
    return pytest_args(shard)


# Measured on 2026-11-08 (all-light pilot, 8 tasks at once on one 32 vCPU node): a chunk took 1.6 to 3.4x as long as the same
# kind of shard alone (2.4x on average), because the tasks share one Spark driver JVM and the node's cores. Slowdown grows about
# in step with the tasks beyond 3 or 4, so 8 cores per task (4 slots on 32 vCPU) keeps it near 1.2x; more wall-clock speed comes
# from more clusters, not more slots.
CORES_PER_TASK = 8


def slots_for(cores: int, cores_per_task: int = CORES_PER_TASK) -> int:
    """How many shards run side by side on a node: its cores over what one shard's pytest and its share of Spark use."""
    return max(1, cores // max(1, cores_per_task))


def auto_target_min(shards: List[Shard], root: Path, file_seconds: Dict[str, float], slots: int, chunks_per_slot: float = 3.5,
                    lo: float = 3.5, hi: float = 10.0) -> float:
    """The length to cut shards to so that there are about ``chunks_per_slot`` x ``slots`` chunks of the same length: a slot that
    finishes early takes another, so the last chunk to start is a small one and the tail stays short. Kept above ``lo`` so a
    task's fixed cost (FIXED_MIN) stays about a fifth of it at most."""
    total = sum(estimate(s, root, file_seconds) for s in shards)
    return round(min(hi, max(lo, total / max(1.0, slots * chunks_per_slot))), 2)


def slowdown(slots: int) -> float:
    """How much longer a chunk takes with ``slots`` chunks running on a node than alone (an estimate from the first pilot: 2.4x at
    8 slots; about 1x up to 3 slots). The next runs' timings refine the file seconds, not this curve."""
    return max(1.0, slots / 3.3)


def rescale_seconds(file_seconds: Dict[str, float], measured_at_slots: int, plan_for_slots: int) -> Dict[str, float]:
    """File seconds measured with ``measured_at_slots`` chunks per node, as they will be with ``plan_for_slots``."""
    k = slowdown(plan_for_slots) / slowdown(max(1, measured_at_slots))
    return {f: v * k for f, v in file_seconds.items()}
