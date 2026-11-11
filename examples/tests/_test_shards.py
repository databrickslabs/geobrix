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
import math
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

# Heavy chunks: they slow the chunks that share their node's one Spark driver by several times (2026-11-10, family 3/3 beside four
# other chunks: neighbours about 10x slower, and both parts of the shard stopped at the 45 min cap), so at most one runs on a cluster
# at a time. A chunk is heavy when it holds a table of this manifest (the GDAL spec engine's oracle tables: it runs gdal on every row,
# in the driver's process tree), or a file or table whose recorded time alone is HEAVY_UNIT_MIN or more.
HEAVY_TABLES = frozenset({"SPEC_ANALYSIS", "SPEC_CALC", "SPEC_DEM", "SPEC_GDAL", "SPEC_SE", "SPEC_WARP"})
HEAVY_UNIT_MIN = 15.0
# The stall cap (no test finished for n minutes) of a chunk is at least this share of its longest file or table's recorded time: a
# test can take a good part of its file, and the flat cap would stop a long one that is making progress.
STALL_SHARE = 0.5


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
    est_src: str = ""  # where est_min came from: history | partial (some keys timed) | lower bound | ci (CI's minutes)
    heavy: bool = False  # holds a table of HEAVY_TABLES or a unit recorded at HEAVY_UNIT_MIN+: one at a time per cluster
    unit_max_min: float = 0.0  # the longest file or table of the chunk by its recorded time (0: not known)
    subset: bool = False  # the selected files or tables of a CI shard (``affected``): collecting nothing is no failure
    solo_min: float = 0.0  # est_min without the contention factors of its units (what it takes beside light neighbours)
    test_part: str = ""  # "i/n": this chunk runs the tests of its one file or table whose node id hashes to part i of n (TEST_PART)
    select_k: str = ""  # a pytest -k of function names: only the family harness rows of those functions (``affected``, row-level)
    share: float = 1.0  # the part of its tables' recorded time a -k selection is expected to take

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


def select(shards: List[Shard], spec: str, changed: Optional[Iterable[str]] = None, root: Optional[Path] = None,
           affected_shards: Optional[List[Shard]] = None) -> List[Shard]:
    """The shards ``--shards`` names: ``all-light``, ``all-heavy``, ``affected``, or names (a name, or a group such as
    ``pyrx`` for both of its halves), comma separated; a test file (``test/pyrx/test_env.py``) adds one shard ``files`` of the files
    named. ``affected`` adds ``affected_shards``: the files and oracle tables the change reaches (``_affected.to_shards``); with no
    change (``changed`` empty) it adds nothing. Unknown names raise ValueError (with the known ones)."""
    runnable = [s for s in shards if not s.local_only]
    out: List[Shard] = []

    def add(items):
        for s in items:
            if s not in out:
                out.append(s)

    files: List[str] = []
    for tok in [t.strip() for t in spec.split(",") if t.strip()]:
        if tok.endswith(".py") and root is not None:
            rel = tok[len(PKG) + 1:] if tok.startswith(PKG + "/") else tok
            if not (Path(root) / PKG / rel).is_file():
                raise ValueError(f"no test file {tok!r} under {PKG}/ (a file is named from the repo root or from {PKG})")
            files.append(rel)
        elif tok == "all-light":
            add(s for s in runnable if s.kind in ("light", "scripts"))
        elif tok == "all-heavy":
            add(s for s in runnable if s.kind == "heavy")
        elif tok == "affected":
            if affected_shards is None and changed:
                raise ValueError("affected needs the selection of the changed files (_affected.to_shards)")
            add(affected_shards or [])
        else:
            hit = [s for s in shards if tok in (s.name, s.group, s.slug)]
            if not hit:
                raise ValueError(f"unknown shard {tok!r}; known: {', '.join(sorted({s.group for s in shards}))}")
            local = [s for s in hit if s.local_only]
            if local:
                raise ValueError(f"shard {local[0].name!r} does not run on a cluster: {local[0].local_only}")
            add(hit)
    if files:  # test files named one by one: one shard of just those files (a light shard, so every file runs where CI runs it)
        add([Shard(name="files", kind="light", paths=sorted(set(files)))])
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


def _is_test_file(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1]
    return (name.startswith("test_") or name.endswith("_test.py")) and name.endswith(".py")


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
    if shard.select_k:
        args += ["-k", shard.select_k]
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


def untimed_file_seconds(shard: Shard, n_files: int) -> float:
    """What a file nobody timed counts as, in the estimate and in the split alike: its share of the table's minutes for the shard."""
    return max((shard.expected_min - FIXED_MIN) * 60.0 / max(1, n_files), 1.0)


TABLE_REST = "table:*"  # the tests of the family file that run in every table chunk, a key of their own


def _items(shard: Shard, root: Path, file_seconds: Dict[str, float]):
    """The units the shard is made of and their minutes: its oracle tables (``table:<T>``) or its files. A unit with no
    recorded time gets its share of CI's minutes for the shard. Also returns how many units were timed, and the per-chunk
    minutes of the family file's other tests. None when the shard has no units to cut by."""
    if shard.oracle_tables:
        tables = [t for t in shard.oracle_tables.split(",") if t]
        keys = [f"table:{t}" for t in tables]
        share = max((shard.expected_min - FIXED_MIN), 0.5) * 60.0 / max(1, len(tables))
        rest = file_seconds.get(TABLE_REST, 0.0) / 60.0
    else:
        keys = files_of(shard, root)
        if not keys:
            return None
        share = untimed_file_seconds(shard, len(keys))
        rest = 0.0
    n = part_count(shard) / max(1e-6, min(1.0, shard.share or 1.0))  # a test part runs about 1/n of its unit, a -k selection its share
    mins = {k: file_seconds.get(k, share) / 60.0 / n for k in keys}
    return mins, sum(1 for k in keys if k in file_seconds), rest / n


def test_part_n(shard: Shard) -> str:
    return shard.test_part.split("/")[1] if shard.test_part else ""


def part_count(shard: Shard) -> int:
    """How many test parts the chunk's unit is cut into (1 when it is not)."""
    return int(shard.test_part.split("/")[1]) if shard.test_part else 1


def estimate_src(shard: Shard, root: Path, file_seconds: Dict[str, float], lower: Optional[set] = None):
    """(minutes, source) of the shard: the recorded times of its files or tables, plus the fixed cost of a task, once ANY of
    them has a recorded time (history wins over CI's minutes from its first sample; untimed units take their share of CI's
    minutes); CI's minutes when none has. Source: history | partial | lower bound | ci."""
    it = _items(shard, root, file_seconds)
    if it is None or not it[1]:
        return float(shard.est_min or shard.expected_min) if shard.oracle_tables else float(shard.expected_min), "ci"
    mins, timed, rest = it
    src = "history" if timed == len(mins) else "partial"
    if lower and any(k in lower for k in mins):
        src = "lower bound"
    return FIXED_MIN + rest + sum(mins.values()), src


def estimate(shard: Shard, root: Path, file_seconds: Dict[str, float]) -> float:
    """Minutes the shard takes (``estimate_src`` without the source)."""
    return estimate_src(shard, root, file_seconds)[0]


def is_heavy_key(key: str, minutes: float, contended: Iterable[str] = ()) -> bool:
    """Whether a unit (``table:<T>`` or a test file) is heavy: a table of the manifest, a unit recorded at HEAVY_UNIT_MIN or more,
    or a unit ``contended`` names (its contention factor in the history is CONTENTION_HEAVY or more: ``_timings.contended``)."""
    return (key.startswith("table:") and key.split(":", 1)[1] in HEAVY_TABLES) or minutes >= HEAVY_UNIT_MIN or key in set(contended)


def stall_min_for(unit_max_min: float, base: float, shard_cap: float) -> float:
    """The stall cap of a chunk: ``base``, or STALL_SHARE of its longest unit when that is more, never past the shard cap."""
    return round(min(max(base, STALL_SHARE * unit_max_min), max(base, shard_cap)), 1)


def annotate(shard: Shard, root: Path, file_seconds: Dict[str, float], contended: Iterable[str] = ()) -> Shard:
    """``shard`` with its ``heavy`` flag and ``unit_max_min`` from the recorded times (a shard that has no units to look at is
    heavy when its tables are, and has no longest unit)."""
    it = _items(shard, root, file_seconds)
    if it is None:
        return shard
    mins, _, _ = it
    contended = set(contended)
    heavy = any(is_heavy_key(k, m, contended) for k, m in mins.items())
    return Shard(**{**shard.__dict__, "heavy": heavy, "unit_max_min": round(max(mins.values(), default=0.0), 2)})


def pack(mins: Dict[str, float], target: float) -> List[List[str]]:
    """Cut units (minutes each) into chunks of about ``target`` minutes: a unit longer than the target is a chunk of its own,
    the rest go longest first onto the lightest of ``ceil(sum / target)`` chunks. So one long file or table never makes a
    lopsided chunk with others."""
    work = max(0.1, target - FIXED_MIN)
    big = [k for k in sorted(mins, key=lambda k: (-mins[k], k)) if mins[k] >= work]
    small = [k for k in sorted(mins, key=lambda k: (-mins[k], k)) if mins[k] < work]
    out = [[k] for k in big]
    if small:
        n = max(1, math.ceil(sum(mins[k] for k in small) / work - 1e-9))
        while True:  # the target is a hard cap: one chunk more until the longest-first fill leaves none of them over it
            parts: List[List[str]] = [[] for _ in range(min(n, len(small)))]
            load = [0.0] * len(parts)
            for k in small:
                i = load.index(min(load))
                parts[i].append(k)
                load[i] += mins[k]
            if max(load) <= work + 1e-9 or n >= len(small):
                break
            n += 1
        out += [p for p in parts if p]
    return out


def cut(shard: Shard, target: float, root: Path, file_seconds: Dict[str, float], lower: Optional[set] = None,
        contended: Iterable[str] = ()) -> List[Shard]:
    """Cut ``shard`` into chunks of about ``target`` minutes by its files, or by its oracle tables (the family file), from the
    recorded times (``pack``). A shard with nothing to cut by comes back whole."""
    it = _items(shard, root, file_seconds)
    if it is None:
        return [shard]
    mins, _, rest = it
    contended = set(contended)
    hot = {k for k, m in mins.items() if is_heavy_key(k, m, contended)}
    # the heavy units are packed apart from the others: a chunk is heavy as a whole, so a light table beside one would wait for the gate
    groups = pack({k: m for k, m in mins.items() if k in hot}, target) + pack({k: m for k, m in mins.items() if k not in hot}, target)
    if len(groups) <= 1 and not (len(mins) == 1 and FIXED_MIN + rest + max(mins.values()) > target):
        est, src = estimate_src(shard, root, file_seconds, lower)
        return [Shard(**{**shard.__dict__, "est_min": round(est, 2), "est_src": src, "heavy": bool(hot), "unit_max_min": round(max(mins.values()), 2)})]
    # the hard cap: a unit longer than the target alone is cut into test parts (by node id) of about the target each
    work = max(0.1, target - FIXED_MIN)
    pieces = []
    for g in groups:
        n = math.ceil(mins[g[0]] / work - 1e-9) if len(g) == 1 and FIXED_MIN + rest + mins[g[0]] > target else 1
        pieces += [(g, f"{j}/{n}" if n > 1 else "", n) for j in range(n)]
    out = []
    for i, (g, tpart, n) in enumerate(pieces, 1):
        timed = sum(1 for k in g if k in file_seconds)
        src = "lower bound" if lower and any(k in lower for k in g) else ("history" if timed == len(g) else "partial" if timed else "ci")
        common = dict(kind=shard.kind, delta=shard.delta, pool=shard.pool, marker=shard.marker, est_src=src, subset=shard.subset,
                      heavy=any(k in hot for k in g), unit_max_min=round(max(mins[k] for k in g) / n, 2), test_part=tpart,
                      select_k=shard.select_k, share=shard.share)
        if shard.oracle_tables:
            out.append(Shard(name=f"{shard.name} [{i}/{len(pieces)}]", paths=list(shard.paths), ignore=list(shard.ignore), part=shard.part,
                             oracle_tables=",".join(k.split(":", 1)[1] for k in g), unsplittable=True,
                             est_min=round(FIXED_MIN + (rest + sum(mins[k] for k in g)) / n, 2), **common))
        else:
            out.append(Shard(name=f"{shard.name} [{i}/{len(pieces)}]", paths=sorted(g), ignore=[], part="", oracle_tables="",
                             est_min=round(FIXED_MIN + sum(mins[k] for k in g) / n, 2), **common))
    return out


def split_tables(shard: Shard, k: int, file_seconds: Optional[Dict[str, float]] = None) -> List[Shard]:
    """Cut an oracle-table shard (one test file, run once per table) into ``k`` shards of whole tables, balanced by the recorded
    seconds of each table (``table:<T>``; an untimed table takes its share of CI's minutes)."""
    tables = [t for t in shard.oracle_tables.split(",") if t]
    k = min(k, len(tables))
    if k <= 1:
        return [shard]
    fsec = file_seconds or {}
    share = max((shard.expected_min - FIXED_MIN), 0.5) * 60.0 / len(tables)
    mins = {t: fsec.get(f"table:{t}", share) / 60.0 for t in tables}
    parts: List[List[str]] = [[] for _ in range(k)]
    load = [0.0] * k
    for t in sorted(tables, key=lambda t: (-mins[t], t)):
        i = load.index(min(load))
        parts[i].append(t)
        load[i] += mins[t]
    rest = fsec.get(TABLE_REST, 0.0) / 60.0
    return [
        Shard(name=f"{shard.name} [{i}/{k}]", kind=shard.kind, paths=list(shard.paths), ignore=list(shard.ignore), part=shard.part,
              oracle_tables=",".join(ts_), delta=shard.delta, pool=shard.pool, marker=shard.marker,
              est_min=round(FIXED_MIN + rest + sum(mins[t] for t in ts_), 2), unsplittable=True)
        for i, ts_ in enumerate(parts, 1) if ts_
    ]


def split_shard(shard: Shard, k: int, root: Path, file_seconds: Dict[str, float]) -> List[Shard]:
    """Cut ``shard`` into ``k`` shards of whole files, balanced by the measured seconds of each file (longest first onto the
    lightest part), so that no part is much longer than another. A shard that cannot be cut by file comes back as it is."""
    if shard.oracle_tables:
        return split_tables(shard, k, file_seconds)
    files = files_of(shard, root)
    if not files or k <= 1:
        return [shard]
    k = min(k, len(files))
    mins = file_minutes(files, file_seconds, untimed_file_seconds(shard, len(files)))
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


def auto_split(shards: List[Shard], root: Path, file_seconds: Dict[str, float], target_min: float, lower: Optional[set] = None,
               contended: Iterable[str] = ()) -> List[Shard]:
    """Cut every shard longer than ``target_min`` (by the recorded times, else CI's minutes) into chunks of about that length
    (``cut``: a file or table longer than the target gets a chunk of its own). A task pays about FIXED_MIN before its first
    test, so ``target_min`` should stay several times that. Every chunk carries its estimate and where it came from."""
    out: List[Shard] = []
    for s in shards:
        est, src = estimate_src(s, root, file_seconds, lower)
        whole = annotate(Shard(**{**s.__dict__, "est_min": round(est, 2), "est_src": src}), root, file_seconds, contended)
        # the target is a hard cap: a shard over it is cut (a unit over it alone into test parts), never kept whole
        if (s.unsplittable and not (s.oracle_tables and not s.name.endswith("]"))) or est <= target_min:
            out.append(whole)
            continue
        out += cut(s, target_min, root, file_seconds, lower, contended)
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
                    lo: float = 3.0, hi: float = 5.0) -> float:
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
