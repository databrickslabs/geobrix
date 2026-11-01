"""Local timing pass: does each form of a levels set scale with n on one machine (`gbx:bench:local-timing`)?

The cluster ladder takes hours; most of what is wrong with a set of forms shows locally in minutes. The pass times the
forms of a set at three sizes on ``local[8]`` (a dropped warmup pass, then ``--repeats`` measured runs each; their median
is what is judged) and flags what no
healthy form does:

* more than K times the row UDF (the reference decode) at the largest n;
* a time that does not scale with n (the smallest n takes over 60 % of the largest: the form works on more than n tiles);
* a time that does not grow like n: the largest over the middle n is under MIN_GROWTH (flat) or over 1.5 times the ratio of
  the two ns (worse than linear). Growth below that ratio is normal: a fixed cost (the scan, the worker start-up) and 8
  cores keep the ratio of an 8-core pass between 1.3 and 3 for a decode that is linear in n;
* a cost per tile above K times the median of its peers;
* a form that has no time (it failed, was skipped or timed 0 s).

Those rules compare a form with the reference and its peers, so they miss a form whose own cost per tile doubled
(they need 3x). ``--baseline-write`` stores each form's cost per tile (the slope between the two larger ns, minus the
floor's, the median over the runs; one run of a form differed by 1.6x from the next) with the machine it was taken on; ``--baseline`` flags a form above ``--baseline-k`` (1.5) times its stored cost.

Exit codes: 0 no flags, 1 the pass flagged a form, 2 a precondition is missing (``PRECONDITION MISSING``, or a usage
error), 3 the pass itself failed.

Only the IO set is implemented; ``SETS`` is where another set's forms and sizes go. The flagging is plain Python over a
``{(form, n): seconds}`` dict, so it is unit-tested without Spark (``test/bench/test_local_timing.py``). The runner
(``main``) is what ``scripts/commands/gbx-bench-local-timing.sh`` runs in the dev container.

The default pass times a 96-tile copy in `/tmp`: a folder of 96 files. The cluster reads a folder of thousands, where
the listing a worker makes of the folder (and the sidecar look-up it chooses) decides the cost of every open. Run it
once with ``--corpus-dir <big folder>/rows`` (the container's ``/Volumes/main/default/bench-corpus/rows``) before a
cluster round: it reads that folder in place and prints the ``readdir_mode`` its tiles get.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
import traceback
from typing import Dict, List, Optional, Sequence, Tuple

K_DEFAULT = 3.0  # the recipe's multiple (Task 17): a form above K x the reference or its peers is flagged
MIN_GROWTH = 1.1  # t(largest n) / t(middle n) under this: the time is flat across the two larger sizes
N_INDEPENDENT = 0.6  # t(smallest n) above this share of t(largest n): the form does the same work whatever n is
BASELINE_K_DEFAULT = (
    1.5  # a form above this multiple of its stored cost per tile is flagged
)
BASELINE_REPEATS = 3  # measured runs per n for a baseline write or comparison: one run of three is not the cost
MIN_BASELINE_MS = (
    1.0  # a stored cost under this (noise around zero) is compared as this much
)


class PreconditionMissing(Exception):
    """An artifact the pass needs is absent (no JAR, a short corpus, no baseline): not a defect, exit 2."""


@dataclasses.dataclass(frozen=True)
class SetSpec:
    mvp: str  # the levels set (`levels_forms.FORMS` key)
    forms: Tuple[
        str, ...
    ]  # variants timed; the last is the floor (its shell cost, no decode)
    reference: str  # the form the others are compared with
    ns: Tuple[int, ...]  # sizes, smallest first; the corpus holds the largest
    corpus_src: str  # where the real files are copied from (once)
    corpus_dir: (
        str  # the copy (a container /tmp path); `<corpus_dir>/rows` holds the files
    )


SETS: Dict[str, SetSpec] = {
    "IO": SetSpec(
        mvp="IO",
        forms=("b", "c", "udf", "d-py", "d-rs", "floor"),
        reference="udf",
        ns=(6, 24, 96),
        corpus_src="/Volumes/main/default/bench-corpus/rows",
        corpus_dir="/tmp/gbx-local-timing/IO",
    )
}


def _ok(t) -> bool:
    """A usable time: present, not NaN (failed or skipped) and above zero (a ratio of it would divide by zero)."""
    return t is not None and not math.isnan(t) and t > 0


def _ratio(a, b) -> float:
    return a / b if _ok(a) and _ok(b) else float("nan")


def flags(
    res: Dict[Tuple[str, int], float],
    *,
    forms: Sequence[str],
    ns: Sequence[int],
    k: float = K_DEFAULT,
    reference: str = "udf",
) -> List[str]:
    """What is wrong with the timings `res` (seconds by `(form, n)`); an empty list is a healthy pass."""
    lo, mid, hi = ns[0], ns[len(ns) // 2], ns[-1]
    floor = forms[-1]
    out: List[str] = []
    timed: List[str] = []
    for f in forms:
        missing = [n for n in ns if not _ok(res.get((f, n)))]
        if missing:
            out.append(
                f"{f}: no time at n={missing[0]} (the form failed, was skipped or timed 0 s)"
            )
        elif f != floor:
            timed.append(f)
    ref = res.get((reference, hi))
    per_tile = {
        f: (res[(f, hi)] - res[(floor, hi)]) / hi * 1000
        for f in timed
        if _ok(res.get((floor, hi)))
    }
    peers = statistics.median(per_tile.values()) if per_tile else None
    for f in timed:
        t_lo, t_mid, t_hi = res[(f, lo)], res[(f, mid)], res[(f, hi)]
        if f != reference and _ok(ref) and t_hi / ref > k:
            out.append(f"{f}: {t_hi / ref:.1f}x the row UDF at n={hi}")
        if t_lo > N_INDEPENDENT * t_hi:
            out.append(
                f"{f}: n-independent (t{lo}={t_lo:.1f}s ~ t{hi}={t_hi:.1f}s): decodes more than n tiles"
            )
        else:
            want = hi / mid
            if not MIN_GROWTH <= t_hi / t_mid <= want * 1.5:
                out.append(
                    f"{f}: non-linear t{hi}/t{mid}={t_hi / t_mid:.2f} (expect {MIN_GROWTH:g} to {want * 1.5:g})"
                )
        if peers is not None and per_tile[f] > k * peers:
            out.append(
                f"{f}: per-tile {per_tile[f]:.0f} ms > {k:g}x its peers' median {peers:.0f} ms"
            )
    return out


def table(
    res: Dict[Tuple[str, int], float],
    *,
    forms: Sequence[str],
    ns: Sequence[int],
    reference: str = "udf",
) -> str:
    """Seconds per form and n, the ratio to the floor and to the reference at the largest n, and largest/middle n."""
    mid, hi = ns[len(ns) // 2], ns[-1]
    floor = forms[-1]
    lines = [
        "form    "
        + "".join(f"{n:>8}" for n in ns)
        + "  x floor  x udf  t%d/t%d" % (hi, mid)
    ]
    for f in forms:
        t = [res.get((f, n), float("nan")) for n in ns]
        cells = "".join(f"{x:8.1f}" for x in t)
        lines.append(
            f"{f:7s} {cells}  {_ratio(t[-1], res.get((floor, hi))):7.1f}  {_ratio(t[-1], res.get((reference, hi))):5.1f}"
            f"  {_ratio(t[-1], t[ns.index(mid)]):7.2f}"
        )
    return "\n".join(lines)


def per_tile_ms(
    res: Dict[Tuple[str, int], float], *, forms: Sequence[str], ns: Sequence[int]
) -> Dict[str, float]:
    """Each timed form's cost per tile in ms: the slope between the middle and the largest n, less the floor's slope.

    The slope leaves a fixed cost (the scan, the worker start-up) out, which is what a regression in the decode alone
    moves. A form without both times, or a pass without the floor's, is left out.
    """
    mid, hi = ns[len(ns) // 2], ns[-1]

    def slope(f):
        a, b = res.get((f, mid)), res.get((f, hi))
        return (b - a) / (hi - mid) * 1000 if _ok(a) and _ok(b) else None

    base = slope(forms[-1])
    if base is None:
        return {}
    return {f: s - base for f in forms[:-1] if (s := slope(f)) is not None}


def median_res(
    runs: Sequence[Dict[Tuple[str, int], float]],
) -> Dict[Tuple[str, int], float]:
    """The median seconds of each `(form, n)` over the measured runs.

    A cell with no usable time in any run (failed, skipped, 0 s) stays NaN: a failure is a flag, not noise to out-vote.
    """
    out: Dict[Tuple[str, int], float] = {}
    for key in {k for r in runs for k in r}:
        ts = [r.get(key) for r in runs]
        out[key] = statistics.median(ts) if all(_ok(t) for t in ts) else float("nan")
    return out


def median_per_tile(
    runs: Sequence[Dict[Tuple[str, int], float]],
    *,
    forms: Sequence[str],
    ns: Sequence[int],
) -> Dict[str, float]:
    """Each form's median cost per tile (ms) over the runs: the median of the runs' slopes (`per_tile_ms`).

    A form without a slope in some run takes the median of the runs that have one."""
    each = [per_tile_ms(r, forms=forms, ns=ns) for r in runs]
    return {
        f: statistics.median(v) for f in forms if (v := [p[f] for p in each if f in p])
    }


def per_run_table(
    runs: Sequence[Dict[Tuple[str, int], float]],
    *,
    forms: Sequence[str],
    ns: Sequence[int],
) -> str:
    """Cost per tile (ms) of each form in each run and the spread (max/min): how noisy this machine was."""
    each = [per_tile_ms(r, forms=forms, ns=ns) for r in runs]
    lines = [
        "per-tile ms per run"
        + "".join(f"{'run ' + str(i + 1):>9}" for i in range(len(runs)))
        + "   max/min"
    ]
    for f in forms[:-1]:
        v = [p.get(f) for p in each]
        ok = [x for x in v if x is not None and x > 0]
        spread = max(ok) / min(ok) if len(ok) > 1 else float("nan")
        lines.append(
            f"  {f:17s}"
            + "".join(f"{'-' if x is None else format(x, '.1f'):>9}" for x in v)
            + f"{spread:10.2f}"
        )
    return "\n".join(lines)


def baseline_flags(
    per_tile: Dict[str, float],
    baseline: Dict[str, float],
    k: float = BASELINE_K_DEFAULT,
) -> List[str]:
    """What is above `k` times its stored cost per tile (and the forms the two do not share)."""
    out: List[str] = []
    for f in sorted(per_tile):
        if f not in baseline:
            out.append(f"{f}: not in the baseline (write it again)")
            continue
        was = max(baseline[f], MIN_BASELINE_MS)
        if per_tile[f] > k * was:
            out.append(
                f"{f}: per-tile {per_tile[f]:.0f} ms is {per_tile[f] / was:.1f}x its baseline {baseline[f]:.0f} ms"
                f" (flags above {k:g}x)"
            )
    out += [
        f"{f}: in the baseline but not timed in this pass"
        for f in sorted(baseline)
        if f not in per_tile
    ]
    return out


def baseline_table(per_tile: Dict[str, float], baseline: Dict[str, float]) -> str:
    lines = [
        "per-tile ms (slope of the two larger ns, less the floor's)     now  baseline       x"
    ]
    for f in sorted(set(per_tile) | set(baseline)):
        now, was = per_tile.get(f), baseline.get(f)
        x = (
            now / max(was, MIN_BASELINE_MS)
            if now is not None and was is not None
            else float("nan")
        )
        cell = lambda v: "       -" if v is None else f"{v:8.1f}"  # noqa: E731
        lines.append(f"  {f:7s}{' ' * 49}{cell(now)}{cell(was)}{x:8.2f}")
    return "\n".join(lines)


def default_baseline_path(set_name: str) -> str:
    """Per machine and untracked (`test-logs/` is gitignored); outside `test-logs/bench/`, which `gbx:bench:clean` prunes."""
    return os.path.join(_root(), "test-logs", f"local-timing-baseline-{set_name}.json")


def _environment() -> Dict[str, object]:
    try:
        sha = subprocess.run(
            ["git", "-C", _root(), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        sha = ""
    return {
        "cores": os.cpu_count(),
        "arch": platform.machine(),
        "git_sha": sha or "unknown",
    }


def write_baseline(
    path: str,
    set_name: str,
    ns: Sequence[int],
    per_tile: Dict[str, float],
    env: Dict[str, object],
    repeats: int = 1,
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(
            {
                "set": set_name,
                "ns": list(ns),
                "per_tile_ms": {f: round(v, 3) for f, v in sorted(per_tile.items())},
                "repeats": repeats,
                "env": env,
                "written": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            fh,
            indent=2,
        )
        fh.write("\n")


def read_baseline(path: str, set_name: str, ns: Optional[Sequence[int]] = None) -> dict:
    if not os.path.isfile(path):
        raise PreconditionMissing(
            f"no baseline at {path}; write one first with --baseline-write"
        )
    with open(path) as fh:
        got = json.load(fh)
    if got.get("set") != set_name or (ns is not None and got.get("ns") != list(ns)):
        raise PreconditionMissing(
            f"the baseline {path} is for set {got.get('set')!r} at n {got.get('ns')}, not set {set_name!r} at n {list(ns or [])};"
            " write it again with --baseline-write"
        )
    return got


def ensure_corpus(src: str, dst: str, n: int) -> int:
    """Make `dst` hold `n` real copies (not links: the reader skips symlinks) of the first files of `src`; once.

    A `dst` that already holds `n` tiles (`.tif`; a sidecar does not count) is left alone. Returns the tile count of `dst`.
    """
    have = (
        sorted(f for f in os.listdir(dst) if f.endswith(".tif"))
        if os.path.isdir(dst)
        else []
    )
    if len(have) >= n:
        return len(have)
    files = (
        sorted(f for f in os.listdir(src) if f.endswith(".tif"))
        if os.path.isdir(src)
        else []
    )
    if len(files) < n:
        raise PreconditionMissing(
            f"{src} holds only {len(files)} tiles, the pass needs {n}; corpus not made"
        )
    os.makedirs(dst, exist_ok=True)
    for f in files[:n]:
        shutil.copyfile(os.path.join(src, f), os.path.join(dst, f))
    return n


def count_tiles(rows: str) -> int:
    """The `.tif` tiles `rows` holds (0 for a folder that is absent)."""
    return (
        sum(1 for f in os.listdir(rows) if f.endswith(".tif"))
        if os.path.isdir(rows)
        else 0
    )


_PROBE = """
import json, os, sys, time
from databricks.labs.gbx.pyrx import _env
_env.configure_gdal_env()
rows = sys.argv[1]
tiles = sorted(f for f in os.listdir(rows) if f.endswith(".tif"))[:2]
t0 = time.monotonic()
modes = [_env.readdir_mode(os.path.join(rows, t)) for t in tiles]
print(json.dumps({"files": len(os.listdir(rows)), "cap": _env.listing_cap(),
    "bound": _env.listing_ceiling(), "tiles": tiles, "modes": modes,
    "listings": _env._LISTINGS.listings, "seconds": time.monotonic() - t0}))
"""


def probe_readdir(rows: str) -> str:
    """One line: the GDAL_DISABLE_READDIR_ON_OPEN mode the lightweight tier gives the first tiles of `rows`.

    It runs in a fresh process (`GDAL_DISABLE_READDIR_ON_OPEN` unset, as on a worker), where the second distinct
    file opened lists the folder, so the second tile's mode is the one the listing decides and the elapsed time is that
    listing. A folder past GDAL's listing limit that still gives None is the shape the gate exists to catch.
    """
    env = {k: v for k, v in os.environ.items() if k != "GDAL_DISABLE_READDIR_ON_OPEN"}
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    try:
        out = subprocess.run(
            [sys.executable, "-c", _PROBE, rows],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
        got = json.loads(out.stdout.strip().splitlines()[-1])
    except (OSError, subprocess.SubprocessError, ValueError, IndexError) as e:
        return f"readdir_mode probe of {rows} failed: {type(e).__name__}: {e}"
    mode = got["modes"][-1] if got["modes"] else None
    past = (
        " (past GDAL's listing limit of %s)" % got["cap"]
        if got["cap"] and got["files"] > got["cap"]
        else ""
    )
    verdict = (
        f"readdir_mode = {mode}"
        if mode is not None
        else "readdir_mode = None (the open keeps the process value TRUE: every sidecar name is stat'ed)"
    )
    return (
        f"folder shape: {rows} holds {got['files']} files{past}; {verdict} for {got['tiles'][-1] if got['tiles'] else '-'}"
        f" (modes of the first two tiles {got['modes']}, {got['listings']} listing, {got['seconds']:.2f} s)"
    )


def _root() -> str:
    here = os.path.abspath(__file__)
    return (
        here[: here.index("/python/geobrix/")]
        if "/python/geobrix/" in here
        else "/root/geobrix"
    )


def _jar(root: Optional[str] = None) -> str:
    """The JAR: `GBX_JAR` first, then `python/geobrix/lib`, then `target/` (the last of a sorted glob in each)."""
    import glob

    root = root or _root()
    for pattern in (
        os.environ.get("GBX_JAR", ""),
        f"{root}/python/geobrix/lib/geobrix-*-jar-with-dependencies.jar",
        f"{root}/target/geobrix-*-jar-with-dependencies.jar",
    ):
        found = sorted(glob.glob(pattern)) if pattern else []
        if found:
            return found[-1]
    raise PreconditionMissing(
        "no geobrix JAR (python/geobrix/lib or target/); c needs it"
    )


@contextlib.contextmanager
def _launch_env_without_pyrx_readdir():
    """Run the block without the GDAL_DISABLE_READDIR_ON_OPEN value pyrx put in this process's environment.

    Importing pyrx sets it to TRUE (a worker's default). A JVM started after that hands it to the Python workers,
    where a value present at start reads as the user's own and turns the per-file choice of sidecar mode off: the timed
    forms would then never get what a cluster gives them. A value the user exported is kept.
    """
    from databricks.labs.gbx.pyrx import _env

    key = "GDAL_DISABLE_READDIR_ON_OPEN"
    held = os.environ.get(key)
    drop = _env._READDIR_SET_HERE and held is not None
    if drop:
        del os.environ[key]
    try:
        yield
    finally:
        if drop:
            os.environ[key] = held


def _spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    with _launch_env_without_pyrx_readdir():
        spark = (
            SparkSession.builder.master("local[8]").appName("gbx-local-timing").config("spark.jars", _jar())
            .config("spark.driver.memory", "6g").config("spark.ui.enabled", "false").getOrCreate()
        )  # fmt: skip
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def measure_runs(
    spec: SetSpec, corpus: str, repeats: int = 1, spark=None
) -> List[Dict[Tuple[str, int], float]]:
    """Run the set's forms at each n on local[8]: one dropped warmup pass at the smallest n (it only warms the JVM and
    the workers), then `repeats` measured passes over every n. One `{(form, n): seconds}` dict per measured pass.
    """
    from unittest import mock

    from databricks.labs.gbx.bench import levels
    from databricks.labs.gbx.bench import levels_forms as lf

    spark = spark or _spark()
    chosen = sorted(
        (f for f in lf.FORMS[spec.mvp] if f.variant in spec.forms),
        key=lambda f: spec.forms.index(f.variant),
    )
    runs: List[Dict[Tuple[str, int], float]] = []
    passes = [(None, spec.ns[0])] + [
        (i, n) for i in range(repeats) for n in spec.ns
    ]  # (run index | warmup, n)
    for i, n in passes:
        with mock.patch.dict(lf.FORMS, {spec.mvp: chosen}):
            rows = levels.run_mvp(
                spark,
                spec.mvp,
                "local-timing",
                0,
                1,
                corpus=corpus,
                schema=None,
                scales=(1,),
                where="docker",
                base_rows=n,
            )
        if i is not None and i == len(runs):
            runs.append({})
        for r in rows:
            if i is not None:
                runs[i][(r.form, n)] = (
                    r.iter_median_s if r.status == "ok" else float("nan")
                )
            if r.status != "ok":
                print(
                    f"  {r.form} n={n}{'' if i is None else f' run {i + 1}'}: {r.status} {r.failure_mode or r.note}",
                    flush=True,
                )
    return runs


def measure(spec: SetSpec, corpus: str, spark=None) -> Dict[Tuple[str, int], float]:
    """One measured pass (see `measure_runs`)."""
    return measure_runs(spec, corpus, 1, spark)[0]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Local timing pass of a levels set. Exit 0: no flags; 1: a form was flagged; 2: a precondition is "
        "missing or the usage is wrong; 3: the pass failed."
    )
    ap.add_argument("--set", default="IO", help="levels set (only IO is implemented)")
    ap.add_argument(
        "--k",
        type=float,
        default=K_DEFAULT,
        help=f"the multiple that flags a form (default {K_DEFAULT:g})",
    )
    ap.add_argument(
        "--corpus-src", help="where the tiles are copied from (default: the set's)"
    )
    ap.add_argument(
        "--corpus-dir",
        help="the corpus to time. A folder named `rows` is read in place (nothing is copied; it must hold the "
        "tiles): use the big corpus folder to time the shape of a cluster run. Any other path is the parent of the "
        "96-tile copy (default: the set's, under /tmp)",
    )
    ap.add_argument(
        "--baseline",
        nargs="?",
        const="",
        default=None,
        metavar="PATH",
        help="flag a form whose cost per tile is above --baseline-k times the stored one "
        "(PATH default: test-logs/local-timing-baseline-<set>.json)",
    )
    ap.add_argument(
        "--baseline-write",
        action="store_true",
        help="store this pass' cost per tile as the baseline (at --baseline PATH or the default) instead of comparing; "
        "a pass that flags a form is not stored",
    )
    ap.add_argument(
        "--baseline-k",
        type=float,
        default=BASELINE_K_DEFAULT,
        help=f"the multiple of the baseline that flags a form (default {BASELINE_K_DEFAULT:g})",
    )
    ap.add_argument(
        "--repeats",
        type=int,
        default=None,
        help="measured runs per n; the table and the baseline use their median (default 1 for the plain pass, "
        f"{BASELINE_REPEATS} with --baseline or --baseline-write; --baseline-write needs at least {BASELINE_REPEATS})",
    )
    a = ap.parse_args(argv)
    if a.repeats is None:
        a.repeats = (
            BASELINE_REPEATS if (a.baseline is not None or a.baseline_write) else 1
        )
    if a.repeats < 1:
        ap.error("--repeats must be at least 1")
    if a.baseline_write and a.repeats < BASELINE_REPEATS:
        ap.error(
            f"--baseline-write needs at least {BASELINE_REPEATS} runs per n (one run is too noisy to store); got --repeats {a.repeats}"
        )
    if a.set not in SETS:
        ap.error(f"set {a.set!r} is not implemented (have: {', '.join(sorted(SETS))})")
    spec = SETS[a.set]
    try:
        return _run(a, spec)
    except PreconditionMissing as e:
        print(f"PRECONDITION MISSING: {e}; local timing not run", file=sys.stderr)
        return 2
    except Exception:  # noqa: BLE001 - the pass itself failed: not a flag, so not 1
        traceback.print_exc()
        print("local timing failed (see the traceback above)", file=sys.stderr)
        return 3


def _run(a, spec: SetSpec) -> int:
    path = a.baseline or default_baseline_path(spec.mvp)
    stored = None
    if a.baseline is not None and not a.baseline_write:
        stored = read_baseline(
            path, spec.mvp, spec.ns
        )  # before the minutes of the pass
    in_place = (
        bool(a.corpus_dir) and os.path.basename(a.corpus_dir.rstrip("/")) == "rows"
    )
    if in_place:
        # a folder of tiles named `rows`: read where it is (a big folder, the shape that shows a listing cost a
        # 96-tile copy hides); never copy into it
        rows = a.corpus_dir.rstrip("/")
        corpus = os.path.dirname(rows)
        held = count_tiles(rows)
        if held < spec.ns[-1]:
            raise PreconditionMissing(
                f"{rows} holds only {held} tiles, the pass needs {spec.ns[-1]}; nothing was copied into it"
            )
    else:
        corpus = a.corpus_dir or spec.corpus_dir
        rows = os.path.join(corpus, "rows")
        held = ensure_corpus(a.corpus_src or spec.corpus_src, rows, spec.ns[-1])
    print(
        f"[local-timing {spec.mvp}] corpus {rows}: {held} tiles{' (read in place)' if in_place else ''}; "
        f"forms {', '.join(spec.forms)}; n {spec.ns}; K={a.k:g}",
        flush=True,
    )
    if in_place:
        print(f"[local-timing {spec.mvp}] {probe_readdir(rows)}", flush=True)
    runs = measure_runs(spec, corpus, a.repeats)
    res = median_res(runs)
    print(
        f"\nseconds per form at n tiles (median of {len(runs)} measured run{'s' if len(runs) != 1 else ''})\n"
        + table(res, forms=spec.forms, ns=spec.ns, reference=spec.reference)
    )
    found = flags(res, forms=spec.forms, ns=spec.ns, k=a.k, reference=spec.reference)
    per_tile = median_per_tile(runs, forms=spec.forms, ns=spec.ns)
    if len(runs) > 1:
        print("\n" + per_run_table(runs, forms=spec.forms, ns=spec.ns))
    env = _environment()
    if stored is not None:
        print(
            f"\nbaseline {path} (K={a.baseline_k:g}), taken at {stored['env']}; now {env}\n"
            + baseline_table(per_tile, stored["per_tile_ms"])
        )
        if {stored["env"].get("cores"), stored["env"].get("arch")} != {
            env["cores"],
            env["arch"],
        }:
            print("NOTE: the baseline was taken on another machine; compare with care")
        found += baseline_flags(per_tile, stored["per_tile_ms"], a.baseline_k)
    print("FLAGS:\n  " + ("\n  ".join(found) if found else "none"))
    if a.baseline_write:
        if found:
            print(f"baseline not written: the pass flagged a form ({path} unchanged)")
        else:
            write_baseline(path, spec.mvp, spec.ns, per_tile, env, a.repeats)
            print(
                f"baseline written: {path} ({len(per_tile)} forms, median of {a.repeats} runs per n, {env})"
            )
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
