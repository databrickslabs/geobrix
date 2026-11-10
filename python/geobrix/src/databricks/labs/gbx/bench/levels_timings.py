"""Size the run parts of a levels pass from the times of the runs already made.

A timings file holds, per function and per scale, the seconds its forms took on the cluster (setup, warmup and the
measured iterations of each form, from `bench_results`), and per part the wall minutes of the run. `plan_parts` cuts a
list of functions into parts near a target wall time from that history: the functions of one input stay together where
they fit, a function longer than the target gets its own part, and the longest parts come first. A function with no
history at the scale asked is projected from the nearest scale it has (linear in the rows); one with none at all gets the
`default_fn_min`.

Timings file (JSON)::

    {"fixed_min": 6.0,                       # a part's start-up: lease, cluster start, install, smoke
     "functions": {"<fn>": {"<scale>": {"min": <minutes of its forms>, "run": "<run id>"}}},
     "parts": {"<run id>": {"scale": "<scale>", "fns": [...], "predicted_min": x, "actual_min": y}}}

`python -m databricks.labs.gbx.bench.levels_timings collect --rows <q.json> --scale-of <run>=<scale>,... --out <file>`
adds the per-function minutes of the rows a `gbx:uc:sql` query of `bench_results` returned (the `|`-joined lines of
`test-logs/t25-q/q-times.sql`). `... plan --timings <file> --scale 4 --target-min 25 --fns a,b` prints the parts (JSON:
name, functions, the `--levels-forms` value and the predicted minutes).
"""

from __future__ import annotations

import json
import math
from typing import Dict, Iterable, List, Optional, Tuple

# a form's value fingerprint and NULL report each read the output once more after the measured iterations
_PASSES = 2


def form_minutes(
    median_s: float,
    setup_s: float,
    warmup_s: float,
    measured: int,
    scale_ratio: float = 1.0,
    passes: int = _PASSES,
) -> float:
    """The minutes one form takes at `scale_ratio` times the rows of the row it was measured on."""
    per_iter = median_s * scale_ratio
    warm = warmup_s * scale_ratio
    return (setup_s + warm + per_iter * (max(measured, 1) + passes)) / 60.0


def parse_rows(lines: Iterable[str]) -> List[dict]:
    """The `|`-joined rows of the timing query: run_id|fn|level|form|rows|status|median|total|warmup|setup|measured|..."""
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) < 11 or p[5] != "ok":
            continue
        try:
            out.append(
                {
                    "run": p[0],
                    "fn": p[1],
                    "form": p[3],
                    "rows": int(p[4]),
                    "median": float(p[6]),
                    "warmup": float(p[8] or 0),
                    "setup": float(p[9] or 0),
                    "measured": int(p[10] or 1),
                }
            )
        except ValueError:
            continue
    return out


def collect(
    timings: dict, rows: List[dict], scale_of: Dict[str, str], measured: int = 3
) -> dict:
    """Add each function's minutes at the run's scale (projected to `measured` iterations) to `timings`."""
    fns = timings.setdefault("functions", {})
    per: Dict[Tuple[str, str], Tuple[float, str]] = {}
    for r in rows:
        scale = scale_of.get(r["run"])
        if scale is None:
            continue
        m = form_minutes(r["median"], r["setup"], r["warmup"], measured)
        key = (r["fn"], scale)
        cur = per.get(key, (0.0, r["run"]))
        per[key] = (cur[0] + m, r["run"])
    for (fn, scale), (m, run) in per.items():
        fns.setdefault(fn, {})[scale] = {"min": round(m, 2), "run": run}
    return timings


def fn_minutes(
    timings: dict, fn: str, scale: float, default_fn_min: float = 5.0
) -> float:
    """`fn`'s minutes at `scale`: its record there, else the nearest recorded scale times the row ratio."""
    hist = timings.get("functions", {}).get(fn, {})
    if not hist:
        return default_fn_min
    if str(scale) in hist or str(int(scale)) in hist:
        return float(hist.get(str(scale), hist.get(str(int(scale))))["min"])
    near = min(hist, key=lambda s: abs(math.log(float(s)) - math.log(scale)))
    return float(hist[near]["min"]) * scale / float(near)


def plan_parts(
    fns: List[str],
    timings: dict,
    scale: float,
    target_min: float = 25.0,
    input_of=None,
    default_fn_min: float = 5.0,
) -> List[dict]:
    """Cut `fns` into parts near `target_min` (fixed start-up included), longest first.

    Functions are packed first-fit-decreasing within each input (`input_of(fn)`), so a part reads one input where it can;
    a function at or over the target is a part on its own. Then parts too small to pay their start-up are merged
    across inputs (largest first) while the sum fits, so no part is mostly start-up."""
    fixed = float(timings.get("fixed_min", 6.0))
    room = max(target_min - fixed, 1.0)
    by_input: Dict[str, List[Tuple[float, str]]] = {}
    for fn in fns:
        inp = input_of(fn) if input_of else "-"
        by_input.setdefault(inp, []).append(
            (fn_minutes(timings, fn, scale, default_fn_min), fn)
        )
    bins: List[List] = []  # [minutes, [fns], input]
    for inp, items in by_input.items():
        for m, fn in sorted(items, reverse=True):
            for b in bins:
                if b[2] == inp and b[0] + m <= room:
                    b[0] += m
                    b[1].append(fn)
                    break
            else:
                bins.append([m, [fn], inp])
    merged: List[List] = []  # [minutes, [fns], [inputs]]
    for m, fs, inp in sorted(bins, key=lambda b: -b[0]):
        for b in merged:
            if b[0] + m <= room:
                b[0] += m
                b[1].extend(fs)
                b[2].append(inp)
                break
        else:
            merged.append([m, list(fs), [inp]])
    merged.sort(key=lambda b: -b[0])
    return [
        {"fns": b[1], "inputs": b[2], "predicted_min": round(fixed + b[0], 1)}
        for b in merged
    ]


def _input_of(fn: str) -> Optional[str]:
    from databricks.labs.gbx.bench import levels_jvm

    s = levels_jvm.spec_of(fn)
    return s.inp if s is not None else None


def main(argv=None) -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="size levels run parts from past run times"
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect", help="add bench_results minutes to a timings file")
    c.add_argument(
        "--rows", required=True, help="the gbx:uc:sql --out JSON of the timing query"
    )
    c.add_argument("--cid", default="times")
    c.add_argument(
        "--scale-of",
        required=True,
        help="run_id=scale,... (the scale each run's rows are)",
    )
    c.add_argument("--out", required=True)
    p = sub.add_parser("plan", help="print the parts of a list of functions")
    p.add_argument("--timings", required=True)
    p.add_argument("--fns", required=True, help="comma-separated SQL names")
    p.add_argument("--scale", type=float, required=True)
    p.add_argument("--target-min", type=float, default=25.0)
    p.add_argument("--no-floor", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "collect":
        try:
            with open(a.out) as f:
                timings = json.load(f)
        except FileNotFoundError:
            timings = {}
        with open(a.rows) as f:
            value = json.load(f)[a.cid]["value"]
        scale_of = dict(kv.split("=", 1) for kv in a.scale_of.split(",") if kv)
        collect(timings, parse_rows(str(value).split("\n")), scale_of)
        with open(a.out, "w") as f:
            json.dump(timings, f, indent=1, sort_keys=True)
        return 0
    from databricks.labs.gbx.bench import levels_jvm

    with open(a.timings) as f:
        timings = json.load(f)
    fns = [x.strip() for x in a.fns.split(",") if x.strip()]
    parts = plan_parts(fns, timings, a.scale, a.target_min, input_of=_input_of)
    for p_ in parts:
        p_["forms"] = ",".join(levels_jvm.part_forms(p_["fns"], floors=not a.no_floor))
    print(json.dumps(parts, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
