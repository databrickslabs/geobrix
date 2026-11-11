"""The timing history of gbx:test:databricks: what each test file (and each oracle table of the family file) took, over runs.

Every run merges its samples into one JSON file; nothing overwrites another run's samples. A sample is one key's seconds in one
run, normalized to a chunk running alone (``_test_shards.slowdown`` of the run's slots), so runs at different slot counts can be
pooled. The estimate of a key is the median of its last WINDOW samples, at the plan's slots. A chunk that did not finish (the
shard cap, the stall cap, a crash) leaves lower bounds for the files it did not finish; a lower bound newer than the key's last
full sample sets the estimate to at least that value, so the next plan gives the file a chunk of its own and puts it first.

Keys: a test file as the shard's working dir names it (``test/ds/test_paths.py``, ``scripts/ci/test_x.py``); an oracle table
of ``test/manager/test_family_cross_tier.py`` as ``table:<TABLE>``; the rest of that file (the tests that run in every table
chunk) as ``table:*``.

Contention: a key also keeps, per run, how its actual time compared with its solo-expected time (``x``, actual over expected,
the expected being the estimate the plan had for it without any contention factor) and how many chunks ran beside it on its cluster
(``nb``). Its contention factor is the median ``x`` of its last WINDOW runs that had neighbours. A factor of CONTENTION_HEAVY or
more marks the key contention-heavy: the planner treats it as a heavy unit (at most one heavy chunk per cluster at a time), and
plans it at its solo time times its factor. A sample taken beside neighbours is deflated by its run's ``x`` before it joins the
solo estimate, so the factor is never counted twice.

Serverless: a run there records its samples under ``sls:<key>`` (each runner task has compute of its own, so a sample is a chunk
alone, and no contention record is made). A Serverless plan reads ``view(h, serverless=True)``: a key's ``sls:`` samples where it has
them, else its pool samples without their contention records. A pool plan never sees the ``sls:`` keys.

A run that stopped early (fail fast) flags its lower bounds with the rule (``stop``) and its run entry with ``stopped_early``.

Writers take an exclusive ``fcntl`` lock on ``<file>.lock``, read, merge and replace the file atomically (``os.replace`` of a
temporary in the same folder), so two runs that finish together keep both their samples.
"""

import contextlib
import fcntl
import json
import os
import re
import statistics
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import _test_shards as ts

try:  # the junit files are our own, but a parser that refuses entities costs nothing
    from defusedxml import ElementTree as ET
except ImportError:  # pragma: no cover - the launcher venv may lack it
    import xml.etree.ElementTree as ET  # noqa: S405

VERSION = 2
WINDOW = 5  # the samples an estimate is the median of
CONTENTION_HEAVY = 2.0  # a key whose contention factor reaches this is contention-heavy: one heavy chunk per cluster at a time
CONTENTION_MIN_S = 60.0  # a key's time in a run must reach this for its ratio to count (short keys give noisy ratios)
OVERLAP_MIN_S = 30.0  # two chunks overlapping for less than this (a hand-off) are not neighbours
KEEP = 20  # the samples kept per key
FAMILY_FILE = "test/manager/test_family_cross_tier.py"
REST = "table:*"
FUNCTIONS_JSON = "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
SLS = "sls:"  # the prefix of a Serverless run's keys


# ---------------------------------------------------------------------------------------------- the file


def empty() -> dict:
    return {"version": VERSION, "keys": {}, "runs": []}


def load(path) -> dict:
    """The history in ``path``; an empty one when it is absent or unreadable. A version-1 file (one run's seconds per file,
    the last writer's) becomes one sample per file."""
    try:
        d = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return empty()
    if d.get("version") == VERSION:
        d.setdefault("keys", {})
        d.setdefault("runs", [])
        return d
    out = empty()
    slots = int(d.get("slots") or 1)
    for k, v in (d.get("files") or {}).items():
        out["keys"][k] = {"samples": [{"s": round(float(v) / ts.slowdown(slots), 2), "run": str(d.get("run") or "v1"), "slots": slots, "lb": False}]}
    return out


@contextlib.contextmanager
def locked(path):
    """An exclusive lock for a read-merge-write of ``path`` (``<path>.lock``, held while the block runs)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(str(p) + ".lock", "a+") as lk:
        fcntl.flock(lk.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lk.fileno(), fcntl.LOCK_UN)


def _write_atomic(path, data: dict) -> None:
    p = Path(path)
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", dir=str(p.parent))
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
    os.replace(tmp, p)


def merge(path, samples: Iterable[dict], run: dict, contention: Iterable[dict] = ()) -> dict:
    """Add one run's samples (``{"key", "s", "slots", "lb"}``, seconds as measured) and contention records (``{"key", "x", "nb",
    "peak"}``) to the history in ``path``, under the lock. A run's samples and records are added once: merging the same run again
    adds nothing. Returns what was added per kind."""
    run_id = str(run.get("run") or "")
    added = {"keys": 0, "lower_bounds": 0, "contention": 0}
    with locked(path):
        h = load(path)
        for c in contention:
            entry = h["keys"].setdefault(c["key"], {"samples": []})
            recs = entry.setdefault("contention", [])
            if any(x.get("run") == run_id for x in recs):
                continue
            recs.append({"run": run_id, "x": round(float(c["x"]), 3), "nb": int(c.get("nb") or 0), "peak": int(c.get("peak") or 0)})
            entry["contention"] = recs[-KEEP:]
            added["contention"] += 1
        for smp in samples:
            entry = h["keys"].setdefault(smp["key"], {"samples": []})
            if any(x.get("run") == run_id for x in entry["samples"]):
                continue
            slots = int(smp.get("slots") or 1)
            entry["samples"].append({"s": round(float(smp["s"]) / ts.slowdown(slots), 2), "run": run_id, "slots": slots,
                                     "lb": bool(smp.get("lb")), **({"stop": str(smp["stop"])} if smp.get("stop") else {})})
            entry["samples"] = entry["samples"][-KEEP:]
            added["lower_bounds" if smp.get("lb") else "keys"] += 1
        if run_id and not any(r.get("run") == run_id for r in h["runs"]):
            h["runs"] = (h["runs"] + [dict(run, merged=time.strftime("%Y-%m-%d %H:%M:%S"))])[-200:]
        _write_atomic(path, h)
    return added


# ---------------------------------------------------------------------------------------------- estimates


def view(h: dict, serverless: bool = False) -> dict:
    """The history a plan reads. A pool plan: every key but the ``sls:`` ones. A Serverless plan: per key, its ``sls:`` entry where
    there is one, else its pool entry, read with ``adjust=False`` and no contended keys (a Serverless runner has no neighbours on its
    compute): the pool's contention records then only deflate their samples to the solo time."""
    keys = h.get("keys", {})
    if not serverless:
        return dict(h, keys={k: v for k, v in keys.items() if not k.startswith(SLS)})
    out = {k: dict(v) for k, v in keys.items() if not k.startswith(SLS)}
    for k, v in keys.items():
        if k.startswith(SLS) and v.get("samples"):
            out[k[len(SLS):]] = {"samples": list(v["samples"])}
    return dict(h, keys=out)


def contention_of(entry: dict) -> Optional[float]:
    """The key's contention factor: the median actual/expected of its last WINDOW runs that had neighbours; None without one."""
    recs = [c for c in entry.get("contention") or [] if int(c.get("nb") or 0) >= 1 and c.get("x")]
    if not recs:
        return None
    return round(statistics.median(float(c["x"]) for c in recs[-WINDOW:]), 2)


def contention(h: dict) -> Dict[str, float]:
    """Contention factor per key (the keys that have one)."""
    return {k: f for k, f in ((k, contention_of(v)) for k, v in h.get("keys", {}).items()) if f is not None}


def contended(h: dict, threshold: float = CONTENTION_HEAVY) -> set:
    """The contention-heavy keys: factor ``threshold`` or more."""
    return {k for k, f in contention(h).items() if f >= threshold}


def estimate_of(entry: dict, slots: int, adjust: bool = True) -> Optional[dict]:
    """``{"s": seconds at ``slots``, "n": full samples, "lb": True when a lower bound sets it, "solo": seconds without the
    contention factor, "x": the factor (1.0 without one)}``; None without samples. ``adjust``: ``s`` is the solo time times the
    factor (when it is above 1), else the solo time. A sample from a run that recorded the key above its expected time beside
    neighbours is deflated by that ratio first (the factor carries it)."""
    smp = entry.get("samples") or []
    x_of = {c["run"]: float(c["x"]) for c in entry.get("contention") or [] if int(c.get("nb") or 0) >= 1 and c.get("x")}

    def solo(x):
        return x["s"] / max(1.0, x_of.get(x.get("run"), 1.0))

    full = [x for x in smp if not x.get("lb")]
    k = ts.slowdown(slots)
    est = statistics.median(solo(x) for x in full[-WINDOW:]) if full else 0.0
    last_full = max((i for i, x in enumerate(smp) if not x.get("lb")), default=-1)
    lbs = [solo(x) for x in smp[last_full + 1:] if x.get("lb")]  # bounds a later full run has not answered yet
    if not full and not lbs:
        return None
    lb = bool(lbs) and max(lbs) > est
    base = max([est] + lbs) * k
    f = contention_of(entry) or 1.0
    return {"s": round(base * max(1.0, f) if adjust else base, 2), "n": len(full), "lb": lb, "solo": round(base, 2), "x": f}


def estimates(h: dict, slots: int, adjust: bool = True) -> Dict[str, dict]:
    return {k: e for k, e in ((k, estimate_of(v, slots, adjust)) for k, v in h.get("keys", {}).items()) if e}


def seconds(h: dict, slots: int, adjust: bool = True) -> Dict[str, float]:
    """Seconds per key at ``slots``, the form the planner takes (``_test_shards.estimate`` / ``auto_split``): contention-adjusted
    (``adjust``), or the solo time."""
    return {k: e["s"] for k, e in estimates(h, slots, adjust).items()}


def lower_bounds(h: dict, slots: int) -> set:
    return {k for k, e in estimates(h, slots).items() if e["lb"]}


# ---------------------------------------------------------------------------------------------- neighbours


def neighbours(records: Dict[str, List[dict]], clusters: Dict[str, int]) -> tuple:
    """Who ran beside whom, from the runners' records (``{pool: [runner record]}``; runner ``r<i>`` runs on cluster ``i %
    clusters[pool]``). Returns ``({slug: {"nb", "peak", "cluster", "with"}}, {pool: {cluster: peak concurrency}})``: ``nb`` is
    how many other chunks overlapped it on its cluster (by OVERLAP_MIN_S or more), ``peak`` the most chunks that ran at once on
    the cluster while it ran (itself included)."""
    spans: Dict[tuple, List[tuple]] = {}
    for pool, recs in records.items():
        n = max(1, int(clusters.get(pool) or 1))
        for r in recs or []:
            m = re.fullmatch(r"r(\d+)", str(r.get("runner") or ""))
            c = int(m.group(1)) % n if m else 0
            for x in r.get("shards", []):
                if x.get("start") is not None and x.get("end") is not None:
                    spans.setdefault((pool, c), []).append((x["slug"], float(x["start"]), float(x["end"])))
    per: Dict[str, dict] = {}
    peaks: Dict[str, Dict[int, int]] = {}
    for (pool, c), items in spans.items():
        events = sorted([(a, 1) for _, a, _ in items] + [(b, -1) for _, _, b in items], key=lambda e: (e[0], e[1]))
        live, top = 0, 0
        for _, d in events:
            live += d
            top = max(top, live)
        peaks.setdefault(pool, {})[c] = top
        for slug, a, b in items:
            near = [o for o, oa, ob in items if o != slug and min(b, ob) - max(a, oa) >= OVERLAP_MIN_S]
            pk = max(sum(1 for _, oa, ob in items if oa <= t < ob) for t in [a] + [oa for _, oa, _ in items if a <= oa < b])
            per[slug] = {"nb": len(near), "peak": pk, "cluster": c, "with": sorted(near)}
    return per, peaks


def chunk_keys(chunk: dict) -> List[str]:
    tables = list(chunk.get("tables") or [])
    return [f"table:{t}" for t in tables] + [REST] if tables else list(chunk.get("files") or [])


def contention_records(chunk: dict, actual_s: Optional[float], per_key_s: Dict[str, float], nb: int, peak: int,
                       incomplete: bool = False) -> List[dict]:
    """The contention records one chunk leaves: per key, its actual seconds over its solo-expected seconds (the plan's
    ``unit_solo_s``); without those, the chunk's actual over its solo-expected time, given to the keys that took a fifth or more of
    its test time. Only keys whose actual time reaches CONTENTION_MIN_S, and never for a lower-bound estimate, nor for a part of a
    split CI shard estimated by its share of CI's minutes (a guess; CI's minutes for a whole shard are a measured solo time). A chunk
    that did not finish (a cap) gives its ratio, a lower bound, only when that already reaches CONTENTION_HEAVY."""
    if chunk.get("est_src") == "lower bound" or (chunk.get("est_src") == "ci" and str(chunk.get("name") or "").endswith("]")):
        return []
    out = []
    unit = chunk.get("unit_solo_s") or {}
    if unit and not incomplete:
        for k, act in per_key_s.items():
            exp = float(unit.get(k) or 0.0)
            if exp > 0 and act >= CONTENTION_MIN_S:
                out.append({"key": k, "x": act / exp, "nb": nb, "peak": peak})
        return out
    solo = float(chunk.get("solo_min") or chunk.get("expected_min") or 0.0) * 60
    total = sum(per_key_s.values())
    if not solo or not actual_s or not total or actual_s < 2 * CONTENTION_MIN_S:
        return []
    x = float(actual_s) / solo
    if incomplete and x < CONTENTION_HEAVY:
        return []
    return [{"key": k, "x": x, "nb": nb, "peak": peak} for k, v in per_key_s.items() if v >= 0.2 * total and v >= CONTENTION_MIN_S]


def key_seconds(junit: Path, root: Path, chunk: dict, fn_table: Dict[str, str]) -> Dict[str, float]:
    per: Dict[str, float] = {}
    for k, v in case_seconds(junit, root, list(chunk.get("tables") or []), fn_table):
        per[k] = per.get(k, 0.0) + v
    return per


# ---------------------------------------------------------------------------------------------- samples of a run


def table_of_function(root: Path) -> Dict[str, str]:
    """sql_name -> oracle table, from the manifest (the table a family row runs under)."""
    try:
        fns = json.loads((Path(root) / FUNCTIONS_JSON).read_text()).get("functions") or []
    except (OSError, ValueError):
        return {}
    return {f["sql_name"]: f.get("oracle", "") for f in fns if f.get("sql_name")}


def table_of_case(name: str, chunk_tables: List[str], fn_table: Dict[str, str]) -> str:
    """The key a test case of the family file counts under: its oracle table among the chunk's, else the per-chunk rest."""
    m = re.match(r"[\w]+\[([^\]]+)\]", name or "")
    if not m:
        return REST
    first = m.group(1).split("-")[0]
    if first in chunk_tables or (first.isupper() and "_" in first):
        return f"table:{first}"
    t = fn_table.get(first)
    if not t:
        return REST
    if not chunk_tables or t in chunk_tables:
        return f"table:{t}"
    if "SPEC_SE" in chunk_tables:  # a row with a gdal block runs on the spec engine too
        return "table:SPEC_SE"
    return f"table:{chunk_tables[0]}" if len(chunk_tables) == 1 else REST


def file_of_case(classname: str, root: Path) -> Optional[str]:
    """The test file of a junit test case: the longest prefix of its dotted classname that is a file (a class may follow)."""
    mod = (classname or "").split(".")
    for k in range(len(mod), 0, -1):
        cand = "/".join(mod[:k]) + ".py"
        if (Path(root) / ts.PKG / cand).is_file() or (Path(root) / cand).is_file():
            return cand
    return None


def case_seconds(junit: Path, root: Path, chunk_tables: List[str], fn_table: Dict[str, str]) -> List[tuple]:
    """``(key, seconds)`` per test case, in the order the cases ran; [] when the file is absent or unreadable."""
    try:
        cases = list(ET.parse(junit).getroot().iter("testcase"))
    except (OSError, ET.ParseError):
        return []
    out = []
    for c in cases:
        f = file_of_case(c.get("classname", ""), root)
        if f is None:
            continue
        key = table_of_case(c.get("name", ""), chunk_tables, fn_table) if f == FAMILY_FILE else f
        out.append((key, float(c.get("time") or 0)))
    return out


def run_samples(chunk: dict, verdict: str, busy_s: Optional[float], junit: Path, root: Path, slots: int,
                fn_table: Optional[Dict[str, str]] = None, stop: str = "", prefix: str = "") -> List[dict]:
    """The samples one chunk leaves. A finished chunk: the seconds of each key from its junit. A chunk that did not finish
    (INCOMPLETE: a cap, a stall, a crash, --maxfail, a fail-fast stop): its keys that finished are full samples; the key it was in
    when it stopped and the keys it never reached share what its elapsed time leaves over as lower bounds, so none of them is
    underestimated next time. ``stop`` (the fail-fast rule that stopped the run) flags those lower bounds; ``prefix`` (``sls:``)
    names the keys of a Serverless run."""
    out = _run_samples(chunk, verdict, busy_s, junit, root, slots, fn_table)
    for x in out:
        x["key"] = prefix + x["key"]
        if stop and x["lb"]:
            x["stop"] = stop
    return out


def _run_samples(chunk: dict, verdict: str, busy_s: Optional[float], junit: Path, root: Path, slots: int,
                 fn_table: Optional[Dict[str, str]] = None) -> List[dict]:
    if verdict in ("EMPTY", "ERRORS"):  # nothing ran, or pytest stopped before the tests: no times to learn from
        return []
    tables = list(chunk.get("tables") or [])
    if tables:
        keys = [f"table:{t}" for t in tables] + [REST]
    else:
        keys = list(chunk.get("files") or [])
    got = case_seconds(junit, root, tables, fn_table if fn_table is not None else table_of_function(root))
    per: Dict[str, float] = {}
    for k, s in got:
        per[k] = per.get(k, 0.0) + s
    if verdict != "INCOMPLETE":
        return [{"key": k, "s": s, "slots": slots, "lb": False} for k, s in per.items()]
    last = got[-1][0] if got else None
    done = {k: s for k, s in per.items() if k != last}
    open_keys = [k for k in keys if k not in done] or ([last] if last else [])
    if not open_keys:
        return [{"key": k, "s": s, "slots": slots, "lb": False} for k, s in done.items()]
    elapsed = float(busy_s or 0.0) - ts.FIXED_MIN * 60
    # what no key's cases took: the stopped key's own seconds are already in its share, so they are not left over too
    left = max(0.0, elapsed - sum(per.values()))
    out = [{"key": k, "s": s, "slots": slots, "lb": False} for k, s in done.items()]
    bounds = [(k, per.get(k, 0.0) + left / len(open_keys)) for k in open_keys]
    # a bound of 0 s (a chunk that never ran) says nothing, and would plan the key at 0 minutes in place of CI's share
    out += [{"key": k, "s": s, "slots": slots, "lb": True} for k, s in bounds if s > 0]
    return out


# ---------------------------------------------------------------------------------------------- backfill


def backfill(path, runs_dir, root) -> dict:
    """Merge the junit files of earlier runs (``<runs_dir>/tst-*/<chunk>/junit.xml``) into the history in ``path``: one run at a
    time, the finished chunks only (an old run kept no plan, so a stopped chunk's files are unknown). The slots of a run come
    from its summary.md (``N slots per cluster``), else 4. Safe to repeat: a run is merged once."""
    fn_table = table_of_function(root)
    added = {"runs": 0, "keys": 0}
    for run_dir in sorted(Path(runs_dir).glob("tst-*")):
        if not run_dir.is_dir():
            continue
        m = re.search(r"\((\d+) slots per cluster\)", (run_dir / "summary.md").read_text()) if (run_dir / "summary.md").exists() else None
        slots = int(m.group(1)) if m else 4
        samples = []
        for chunk in sorted(p for p in run_dir.iterdir() if (p / "junit.xml").exists()):
            try:
                res = json.loads((chunk / "result.json").read_text())
            except (OSError, ValueError):
                continue
            if res.get("reason") or res.get("rc") not in (0, 1):
                continue
            samples += run_samples({}, "ok", None, chunk / "junit.xml", root, slots, fn_table)
        if samples:
            got = merge(path, samples, {"run": run_dir.name, "backfill": True, "slots": slots})
            added["runs"] += 1
            added["keys"] += got["keys"]
    return added


def _summary_rows(text: str) -> Dict[str, dict]:
    """name -> {"predicted_s", "actual_s", "est"} from a summary.md's table."""
    out = {}
    head = None
    for ln in text.splitlines():
        if not ln.startswith("|"):
            continue
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        if "predicted s" in cells or "CI ~min" in cells:  # the first runs' summaries gave CI's minutes
            head = cells
            continue
        if head and len(cells) == len(head) and cells[0] and not cells[0].startswith(("**", "---")):
            r = dict(zip(head, cells))
            try:
                if "predicted s" in r:
                    out[r["shard"]] = {"predicted_s": float(r["predicted s"]), "est": r.get("estimate", ""), "name": r["shard"]}
                else:
                    out[r["shard"]] = {"predicted_s": float(r["CI ~min"]) * 60, "est": "ci", "name": r["shard"]}
            except (KeyError, ValueError):
                continue
    return out


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def backfill_contention(path, runs_dir, root, expected: Optional[Dict[str, Dict[str, float]]] = None,
                        clusters: Optional[Dict[str, Dict[str, int]]] = None) -> dict:
    """Merge the contention records of earlier runs into the history in ``path``: per run under ``runs_dir`` that kept its runners'
    records (``_runners/<pool>/r<i>.json``), the chunk's actual time (its runner's record), its expected time (``predicted s`` of
    the run's summary.md, or ``expected[run][slug]`` in minutes: a queue table's ``expected_min``), its neighbours on its cluster
    (``clusters[run][pool]``, else the clusters the summary names, else 1) and its keys' seconds from its junit. Safe to repeat."""
    fn_table = table_of_function(root)
    expected, clusters = expected or {}, clusters or {}
    added = {"runs": 0, "records": 0}
    for run_dir in sorted(Path(runs_dir).glob("tst-*")):
        rdir = run_dir / "_runners"
        if not rdir.is_dir():
            continue
        summ = (run_dir / "summary.md").read_text() if (run_dir / "summary.md").exists() else ""
        srows = {_slug(k): v for k, v in _summary_rows(summ).items()}
        exp = {k: v * 60 for k, v in (expected.get(run_dir.name) or {}).items()}
        records = {}
        for pool_dir in sorted(p for p in rdir.iterdir() if p.is_dir()):
            recs = []
            for f in sorted(pool_dir.glob("r*.json")):
                try:
                    recs.append(json.loads(f.read_text()))
                except (OSError, ValueError):
                    continue
            records[pool_dir.name] = recs
        ncl = dict(clusters.get(run_dir.name) or {})
        for pool in records:
            if pool not in ncl:
                m = re.search(rf"- {re.escape(pool)}: cluster\(s\) ([^ ]+(?:, [^ ]+)*) \(", summ)
                ncl[pool] = len(m.group(1).split(", ")) if m else 1
        per, _ = neighbours(records, ncl)
        recs_out = []
        for pool, recs in records.items():
            for r in recs:
                for x in r.get("shards", []):
                    slug = x.get("slug")
                    incomplete = bool(x.get("reason")) or x.get("rc") not in (0, 1)
                    if not slug or x.get("end") is None or (incomplete and "watchdog" not in str(x.get("reason") or "")):
                        continue
                    srow = srows.get(slug) or {}
                    want = exp.get(slug) or srow.get("predicted_s")
                    if not want:
                        continue
                    junit = run_dir / slug / "junit.xml"
                    tables = []
                    try:
                        tables = [t for t in json.loads((run_dir / slug / "result.json").read_text()).get("tables") or []]
                    except (OSError, ValueError, AttributeError):
                        pass
                    chunk = {"tables": tables, "solo_min": want / 60.0, "est_src": srow.get("est", ""), "name": srow.get("name", "")}
                    ks = key_seconds(junit, root, chunk, fn_table)
                    nb = per.get(slug, {}).get("nb", 0)
                    recs_out += contention_records(chunk, x["end"] - x["start"], ks, nb, per.get(slug, {}).get("peak", 0), incomplete)
        if recs_out:
            got = merge(path, [], {"run": run_dir.name, "backfill": True}, recs_out)
            added["runs"] += 1
            added["records"] += got["contention"]
    return added


if __name__ == "__main__":  # python examples/tests/_timings.py <history.json> <runs dir> [contention [expected.json]]: one-off backfills
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _cli import exit_if_help  # sibling module (script dir is on sys.path)

    exit_if_help(__doc__)
    root = Path(__file__).resolve().parents[2]
    if len(sys.argv) > 3 and sys.argv[3] == "contention":
        extra = json.loads(Path(sys.argv[4]).read_text()) if len(sys.argv) > 4 else {}
        print(backfill_contention(sys.argv[1], sys.argv[2], root, extra.get("expected"), extra.get("clusters")))
    else:
        print(backfill(sys.argv[1], sys.argv[2], root))
