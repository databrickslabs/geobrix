"""The timing history of gbx:test:databricks: what each test file (and each oracle table of the family file) took, over runs.

Every run merges its samples into one JSON file; nothing overwrites another run's samples. A sample is one key's seconds in one
run, normalized to a chunk running alone (``_test_shards.slowdown`` of the run's slots), so runs at different slot counts can be
pooled. The estimate of a key is the median of its last WINDOW samples, at the plan's slots. A chunk that did not finish (the
shard cap, the stall cap, a crash) leaves lower bounds for the files it did not finish; a lower bound newer than the key's last
full sample sets the estimate to at least that value, so the next plan gives the file a chunk of its own and puts it first.

Keys: a test file as the shard's working dir names it (``test/ds/test_paths.py``, ``scripts/ci/test_x.py``); an oracle table
of ``test/manager/test_family_cross_tier.py`` as ``table:<TABLE>``; the rest of that file (the tests that run in every table
chunk) as ``table:*``.

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
KEEP = 20  # the samples kept per key
FAMILY_FILE = "test/manager/test_family_cross_tier.py"
REST = "table:*"
FUNCTIONS_JSON = "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"


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


def merge(path, samples: Iterable[dict], run: dict) -> dict:
    """Add one run's samples (``{"key", "s", "slots", "lb"}``, seconds as measured) to the history in ``path``, under the
    lock. A run's samples are added once: merging the same run again adds nothing. Returns what was added per kind."""
    run_id = str(run.get("run") or "")
    added = {"keys": 0, "lower_bounds": 0}
    with locked(path):
        h = load(path)
        for smp in samples:
            entry = h["keys"].setdefault(smp["key"], {"samples": []})
            if any(x.get("run") == run_id for x in entry["samples"]):
                continue
            slots = int(smp.get("slots") or 1)
            entry["samples"].append({"s": round(float(smp["s"]) / ts.slowdown(slots), 2), "run": run_id, "slots": slots,
                                     "lb": bool(smp.get("lb"))})
            entry["samples"] = entry["samples"][-KEEP:]
            added["lower_bounds" if smp.get("lb") else "keys"] += 1
        if run_id and not any(r.get("run") == run_id for r in h["runs"]):
            h["runs"] = (h["runs"] + [dict(run, merged=time.strftime("%Y-%m-%d %H:%M:%S"))])[-200:]
        _write_atomic(path, h)
    return added


# ---------------------------------------------------------------------------------------------- estimates


def estimate_of(entry: dict, slots: int) -> Optional[dict]:
    """``{"s": seconds at ``slots``, "n": full samples, "lb": True when a lower bound sets it}``; None without samples."""
    smp = entry.get("samples") or []
    full = [x for x in smp if not x.get("lb")]
    k = ts.slowdown(slots)
    est = statistics.median(x["s"] for x in full[-WINDOW:]) if full else 0.0
    last_full = max((i for i, x in enumerate(smp) if not x.get("lb")), default=-1)
    lbs = [x["s"] for x in smp[last_full + 1:] if x.get("lb")]  # bounds a later full run has not answered yet
    if not full and not lbs:
        return None
    lb = bool(lbs) and max(lbs) > est
    return {"s": round(max([est] + lbs) * k, 2), "n": len(full), "lb": lb}


def estimates(h: dict, slots: int) -> Dict[str, dict]:
    return {k: e for k, e in ((k, estimate_of(v, slots)) for k, v in h.get("keys", {}).items()) if e}


def seconds(h: dict, slots: int) -> Dict[str, float]:
    """Seconds per key at ``slots``, the form the planner takes (``_test_shards.estimate`` / ``auto_split``)."""
    return {k: e["s"] for k, e in estimates(h, slots).items()}


def lower_bounds(h: dict, slots: int) -> set:
    return {k for k, e in estimates(h, slots).items() if e["lb"]}


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
                fn_table: Optional[Dict[str, str]] = None) -> List[dict]:
    """The samples one chunk leaves. A finished chunk: the seconds of each key from its junit. A chunk that did not finish
    (INCOMPLETE: a cap, a stall, a crash): its keys that finished are full samples; the key it was in when it stopped and the keys
    it never reached share what its elapsed time leaves over as lower bounds, so none of them is underestimated next time."""
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
    left = max(0.0, elapsed - sum(done.values()))
    out = [{"key": k, "s": s, "slots": slots, "lb": False} for k, s in done.items()]
    out += [{"key": k, "s": per.get(k, 0.0) + left / len(open_keys), "slots": slots, "lb": True} for k in open_keys]
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


if __name__ == "__main__":  # python examples/tests/_timings.py <history.json> <runs dir>: the one-off backfill
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    root = Path(__file__).resolve().parents[2]
    print(backfill(sys.argv[1], sys.argv[2], root))
