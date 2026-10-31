"""Local timing pass: does each form of a levels set scale with n on one machine (`gbx:bench:local-timing`)?

The cluster ladder takes hours; most of what is wrong with a set of forms shows locally in minutes. The pass times the
forms of a set at three sizes on ``local[8]`` (a dropped warmup pass, then one measured run each) and flags what no
healthy form does:

* more than K times the row UDF (the reference decode) at the largest n;
* a time that does not scale with n (the smallest n takes over 60 % of the largest: the form works on more than n tiles);
* a time that does not grow like n: the largest over the middle n is under MIN_GROWTH (flat) or over 1.5 times the ratio of
  the two ns (worse than linear). Growth below that ratio is normal: a fixed cost (the scan, the worker start-up) and 8
  cores keep the ratio of an 8-core pass between 1.3 and 3 for a decode that is linear in n;
* a cost per tile above K times the median of its peers;
* a form that has no time (it failed or was skipped).

Only the IO set is implemented; ``SETS`` is where another set's forms and sizes go. The flagging is plain Python over a
``{(form, n): seconds}`` dict, so it is unit-tested without Spark (``test/bench/test_local_timing.py``). The runner
(``main``) is what ``scripts/commands/gbx-bench-local-timing.sh`` runs in the dev container.
"""

from __future__ import annotations

import argparse
import dataclasses
import math
import os
import shutil
import statistics
import sys
from typing import Dict, List, Sequence, Tuple

K_DEFAULT = 3.0  # the recipe's multiple (Task 17): a form above K x the reference or its peers is flagged
MIN_GROWTH = 1.1  # t(largest n) / t(middle n) under this: the time is flat across the two larger sizes
N_INDEPENDENT = 0.6  # t(smallest n) above this share of t(largest n): the form does the same work whatever n is


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
    return t is not None and not math.isnan(t)


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
                f"{f}: no time at n={missing[0]} (the form failed or was skipped)"
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
            f"{f:7s} {cells}  {t[-1] / res[(floor, hi)]:7.1f}  {t[-1] / res[(reference, hi)]:5.1f}  {t[-1] / t[ns.index(mid)]:7.2f}"
        )
    return "\n".join(lines)


def ensure_corpus(src: str, dst: str, n: int) -> int:
    """Make `dst` hold `n` real copies (not links: the reader skips symlinks) of the first files of `src`; once.

    A `dst` that already holds `n` files is left alone. Returns the file count of `dst`.
    """
    have = sorted(os.listdir(dst)) if os.path.isdir(dst) else []
    if len(have) >= n:
        return len(have)
    files = (
        sorted(f for f in os.listdir(src) if f.endswith(".tif"))
        if os.path.isdir(src)
        else []
    )
    if len(files) < n:
        raise SystemExit(
            f"PRECONDITION MISSING: {src} holds only {len(files)} tiles, the pass needs {n}; corpus not made"
        )
    os.makedirs(dst, exist_ok=True)
    for f in files[:n]:
        shutil.copyfile(os.path.join(src, f), os.path.join(dst, f))
    return n


def _jar() -> str:
    import glob

    here = os.path.abspath(__file__)
    root = (
        here[: here.index("/python/geobrix/")]
        if "/python/geobrix/" in here
        else "/root/geobrix"
    )
    for pattern in (
        os.environ.get("GBX_JAR", ""),
        f"{root}/python/geobrix/lib/geobrix-*-jar-with-dependencies.jar",
        f"{root}/target/geobrix-*-jar-with-dependencies.jar",
    ):
        found = sorted(glob.glob(pattern)) if pattern else []
        if found:
            return found[-1]
    raise SystemExit(
        "PRECONDITION MISSING: no geobrix JAR (python/geobrix/lib or target/); c needs it"
    )


def measure(spec: SetSpec, corpus: str) -> Dict[Tuple[str, int], float]:
    """Run the set's forms at each n on local[8]: a first pass at the smallest n only warms the JVM and the workers."""
    from unittest import mock

    from pyspark.sql import SparkSession

    from databricks.labs.gbx.bench import levels
    from databricks.labs.gbx.bench import levels_forms as lf

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[8]").appName("gbx-local-timing").config("spark.jars", _jar())
        .config("spark.driver.memory", "6g").config("spark.ui.enabled", "false").getOrCreate()
    )  # fmt: skip
    spark.sparkContext.setLogLevel("ERROR")
    chosen = sorted(
        (f for f in lf.FORMS[spec.mvp] if f.variant in spec.forms),
        key=lambda f: spec.forms.index(f.variant),
    )
    res: Dict[Tuple[str, int], float] = {}
    for n in (spec.ns[0],) + tuple(spec.ns):
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
        for r in rows:
            res[(r.form, n)] = r.iter_median_s if r.status == "ok" else float("nan")
            if r.status != "ok":
                print(
                    f"  {r.form} n={n}: {r.status} {r.failure_mode or r.note}",
                    flush=True,
                )
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Local timing pass of a levels set; exits 1 when it flags anything."
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
        "--corpus-dir", help="the copy to time (default: the set's, under /tmp)"
    )
    a = ap.parse_args(argv)
    if a.set not in SETS:
        raise SystemExit(
            f"set {a.set!r} is not implemented (have: {', '.join(sorted(SETS))})"
        )
    spec = SETS[a.set]
    corpus = a.corpus_dir or spec.corpus_dir
    held = ensure_corpus(
        a.corpus_src or spec.corpus_src, os.path.join(corpus, "rows"), spec.ns[-1]
    )
    print(
        f"[local-timing {spec.mvp}] corpus {corpus}/rows: {held} tiles; forms {', '.join(spec.forms)}; n {spec.ns}; K={a.k:g}",
        flush=True,
    )
    res = measure(spec, corpus)
    print(
        "\nseconds per form at n tiles\n"
        + table(res, forms=spec.forms, ns=spec.ns, reference=spec.reference)
    )
    found = flags(res, forms=spec.forms, ns=spec.ns, k=a.k, reference=spec.reference)
    print("FLAGS:\n  " + ("\n  ".join(found) if found else "none"))
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
