"""Read legs of the Zarr size ladder (``ZL-R1`` .. ``ZL-R4``) and the local runner (``gbx:bench:local-timing --set ZL``).

The stores come from ``bench.zarr_ladder`` (two families, eight rungs; the ``zarr-weather-features`` notebook's cube at
other sizes). Each leg reads ONE store per call, as ``gbx_zarr_read`` / ``gbx_zarr_sample`` do, and mirrors a step of
that notebook:

* ``ZL-R1`` the whole store to a table, with a count (the scaling baseline).
* ``ZL-R2`` a box over all time, to a table (the notebook's ``sel`` box): the middle 0.6 x 0.6 degrees of a county,
  5 x 5 degrees of the globe.
* ``ZL-R3`` ``gbx_zarr_sample`` at 100 seeded sites (WKT points), every hour, all four variables.
* ``ZL-R4`` wind speed ``sqrt(u10^2 + v10^2)``, then the hourly mean over the area (the whole county; the 5 x 5 degree
  box of the globe).
* ``ZL-R5`` one hour of ``u10`` as a single whole-grid tile (``output="tiles"``, ``virtual_tiles=False``). The work does
  not grow with the store: it checks the cost of reaching one hour in a store that does.

Every leg is timed ``reps`` times (3; the first is cold) and records, per run, the planning time (the call that
returns the lazy DataFrame: it opens the store, plans the units and builds the work frame, all on the driver) and
the execution time (a ``noop`` write), the number of tasks (the units the read planned) against the chunks and shards
the store holds and the read touches, the rows, the decoded bytes touched per second, and, where the Spark UI is
reachable (the local runner), the executors' Python-worker seconds as a share of ``cores x execution time``.
Nothing here calls ``spark.conf.set``, ``sparkContext``, ``.rdd`` or ``_jvm`` (the attribution helper, which does,
is used only when ``LadderConfig.attribution`` is set).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from databricks.labs.gbx.bench import zarr_ladder as zl
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.zarr import _spans as zspans

__all__ = [
    "LADDER_CASES",
    "LadderConfig",
    "ladder_case",
    "run_ladder",
    "verdicts",
    "summary_table",
]

# case -> (workload name, what it reads, families it applies to)
LADDER_CASES: Dict[str, Tuple[str, str, Tuple[str, ...]]] = {
    "ZL-R1": ("full_table", "full read to a table, with a count", tuple(zl.FAMILIES)),
    "ZL-R2": ("box_all_time", "a box over all time, to a table", tuple(zl.FAMILIES)),
    "ZL-R3": (
        "sample_100pts",
        "gbx_zarr_sample: 100 points, every hour",
        tuple(zl.FAMILIES),
    ),
    "ZL-R4": (
        "wind_speed_hourly_mean",
        "sqrt(u10^2 + v10^2), hourly mean over the area",
        tuple(zl.FAMILIES),
    ),
    "ZL-R5": (
        "one_hour_tile",
        "one hour of u10 as one whole-grid tile",
        tuple(zl.FAMILIES),
    ),
}
# the write legs (bench.zarr_ladder_write): a table of cells to a Zarr store, three ways, and an append
from databricks.labs.gbx.bench.zarr_ladder_write import WRITE_CASES  # noqa: E402

for _c, (_w, _d) in WRITE_CASES.items():
    LADDER_CASES[_c] = (_w, _d, tuple(zl.FAMILIES))
GATE_PER_GB = 1.5  # time per GB within this multiple of the reference rung's
GATE_PLAN_SHARE = 0.10
MIN_PLAN_S = (
    0.25  # under this much planning, a share of a sub-second wall is only the job start
)
VERIFY_MAX_BYTES = 2e9  # larger stores are not re-summed in numpy
_N_POINTS = 100
# the box of ZL-R2 and the point box of ZL-R3, per family: (lat_hi, lat_lo, lon_west, lon_east)
_BOX = {
    "county_wind": (39.8, 39.2, 255.2, 255.8),
    "global_11km": (2.5, -2.5, 177.5, 182.5),
}
_POINT_BOX = {
    "county_wind": (39.99, 39.01, 255.01, 255.99),
    "global_11km": (4.9, -4.9, 175.1, 184.9),
}


def ladder_case(case: str) -> Tuple[str, str, Tuple[str, ...]]:
    if case not in LADDER_CASES:
        raise KeyError(
            f"no Zarr ladder case {case!r}; cases: {', '.join(LADDER_CASES)}"
        )
    return LADDER_CASES[case]


@dataclass
class LadderConfig:
    """Where the stores are and what to run. ``root``: the ladder directory (``<family>/<rung>.zarr`` under it)."""

    root: str
    rungs: Tuple[str, ...] = ("10MB", "50MB", "100MB")
    families: Tuple[str, ...] = tuple(zl.FAMILIES)
    cases: Tuple[str, ...] = tuple(LADDER_CASES)
    reps: int = 3
    run_id: str = "zarr-ladder"
    where: str = "local"
    serverless: bool = False
    cores: Optional[int] = (
        None  # task slots, for the parallelism gate and the busy share
    )
    verify: bool = True
    attribution: bool = (
        False  # ask the Spark UI for worker seconds (not Serverless-safe)
    )
    budget_mib: Optional[int] = (
        None  # unit budget of the read planner, MiB (None: the runtime's own)
    )
    scratch: Optional[str] = (
        None  # where the write legs put their stores (they empty it)
    )
    schema: Optional[str] = (
        None  # catalog.schema of the write legs' manifest and staging tables
    )


@contextlib.contextmanager
def _unit_budget(mib: Optional[int]):
    """Plan reads with a decoded unit budget of ``mib`` MiB: the planner packs chunks into tasks up to this size
    (``zarr._read._budget_bytes``: 64 MiB on Serverless and locally, 1536 MiB on a classic cluster).
    """
    from databricks.labs.gbx.zarr import _read

    if mib is None:
        yield
        return
    real = _read._budget_bytes
    _read._budget_bytes = lambda: int(mib) << 20
    try:
        yield
    finally:
        _read._budget_bytes = real


@dataclass
class _Tap:
    """What one run of a read planned, seen from the module-level hooks."""

    units: int = 0
    chunks: int = 0
    shards: int = 0
    route: str = "spark"  # "driver" when a sample was answered by the driver
    cps: Tuple[int, ...] = ()
    seen_shards: set = field(default_factory=set)


@contextlib.contextmanager
def _tapped(cps: Tuple[int, ...]):
    """Hook ``_read._work_frame`` and ``_sample._through_spark`` / ``_sample_on_driver``: they are where the
    driver hands the planned units to Spark, so the tap counts exactly the tasks and chunks a run reads.
    """
    from databricks.labs.gbx.zarr import _plan, _read, _sample

    tap = _Tap(cps=cps)

    def note(units: Sequence[_plan.Unit]):
        tap.units += len(units)
        for u in units:
            tap.chunks += len(u.chunks)
            for c in u.chunks:
                tap.seen_shards.add(tuple(i // n for i, n in zip(c, cps)))
        tap.shards = len(tap.seen_shards)

    real_work, real_through, real_driver, real_tiles = (
        _read._work_frame,
        _sample._through_spark,
        _sample._sample_on_driver,
        _read._tiles_frame,
    )

    def tiles(spark, plans, virtual, access=None):
        total = sum(p.n_tiles for p in plans)
        per = _read._TILES_PER_TASK if virtual else _read._MATERIALIZED_PER_TASK
        tap.units += -(-total // per) if total else 0
        return real_tiles(spark, plans, virtual, access)

    def work(spark, units):
        note(units)
        return real_work(spark, units)

    def through(df, cfg, units, schema):
        note([_plan.Unit.from_json(t) for t in units.values()])
        return real_through(df, cfg, units, schema)

    def driver(spark, rows, cfg, units, mapped, schema):
        tap.route = "driver"
        note([_plan.Unit.from_json(t) for t in units.values()])
        return real_driver(spark, rows, cfg, units, mapped, schema)

    _read._work_frame, _sample._through_spark, _sample._sample_on_driver = (
        work,
        through,
        driver,
    )
    _read._tiles_frame = tiles
    try:
        yield tap
    finally:
        _read._work_frame, _sample._through_spark, _sample._sample_on_driver = (
            real_work,
            real_through,
            real_driver,
        )
        _read._tiles_frame = real_tiles


def _array_doc(store: str, var: str) -> dict:
    with open(os.path.join(store, var, "zarr.json")) as fh:
        return json.load(fh)


def store_layout(store: str, var: str) -> dict:
    """Shape, inner chunk shape, shard shape and the counts of a stored array, read from its ``zarr.json``."""
    doc = _array_doc(store, var)
    shape = tuple(doc["shape"])
    shard = tuple(doc["chunk_grid"]["configuration"]["chunk_shape"])
    inner = shard
    for c in doc["codecs"]:
        if c["name"] == "sharding_indexed":
            inner = tuple(c["configuration"]["chunk_shape"])
    cps = tuple(s // i for s, i in zip(shard, inner))
    n = lambda a, b: math.prod(-(-x // y) for x, y in zip(a, b))  # noqa: E731
    return {
        "shape": shape,
        "chunks": inner,
        "shards": shard,
        "cps": cps,
        "n_chunks": n(shape, inner),
        "n_shards": n(shape, shard),
    }


# --- the legs --------------------------------------------------------------------------------


def _noop(df) -> None:
    df.write.format("noop").mode("overwrite").save()


def _sum(df, col: str) -> float:
    from pyspark.sql import functions as F

    return float(df.agg(F.sum(F.col(col).cast("double"))).first()[0])


def _close(a: float, b: float, rel: float = 1e-9) -> bool:
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def _box_idx(g: zl.Geometry, box) -> Tuple[np.ndarray, np.ndarray]:
    lat_hi, lat_lo, west, east = box
    lat, lon = g.axes()
    return (
        np.nonzero((lat <= lat_hi + 1e-9) & (lat >= lat_lo - 1e-9))[0],
        np.nonzero((lon >= west - 1e-9) & (lon <= east + 1e-9))[0],
    )


def _block(g: zl.Geometry, variables, t0: int, t1: int, rows=None, cols=None):
    """Hours ``[t0, t1)`` of ``variables`` as the generator wrote them, over the rows and columns given."""
    from databricks.labs.gbx.sample.zarr_era5 import era5_block

    lat, lon = g.axes()
    return era5_block(
        variables,
        t0,
        t1,
        lat if rows is None else lat[rows],
        lon if cols is None else lon[cols],
        noise=g.spec.noise,
        precision=g.spec.precision,
    )


def _expected_sum(g: zl.Geometry, var: str, rows=None, cols=None) -> float:
    """Sum of ``var`` over all hours and the given latitude rows / longitude columns."""
    total = 0.0
    for t0 in range(0, g.hours, 168):
        a = _block(g, (var,), t0, min(t0 + 168, g.hours), rows, cols)[var]
        total += float(a.astype(np.float64).sum())
    return total


def _sites(g: zl.Geometry):
    """100 seeded sites in the family's point box: WKT points as the notebook writes them, and the cell each lies in."""
    lat_hi, lat_lo, west, east = _POINT_BOX[g.family]
    rng = np.random.default_rng([7, g.ny, g.nx])
    lat = rng.uniform(lat_lo, lat_hi, _N_POINTS).round(2)
    lon = rng.uniform(west, east, _N_POINTS).round(2)
    la, lo = g.axes()
    r = np.abs(la[None, :] - lat[:, None]).argmin(axis=1)
    c = np.abs(lo[None, :] - lon[:, None]).argmin(axis=1)
    wkt = [f"POINT({x} {y})" for x, y in zip(lon, lat)]
    return wkt, r, c


@dataclass
class _Leg:
    """One workload on one store: how to build its DataFrame, and what it should hold."""

    case: str
    build: Callable[[], object]
    variables: int  # variables decoded
    cells: int  # cells of the result (rows of a table)
    check: Optional[Callable[[object], str]] = None  # run once, on a DataFrame; "" = ok
    bytes_touched: Optional[int] = (
        None  # decoded bytes read, when not counted from the planned chunks
    )


def _legs(  # noqa: C901
    spark, g: zl.Geometry, store: str, cfg: LadderConfig
) -> List[_Leg]:
    from pyspark.sql import functions as F

    from databricks.labs.gbx.zarr import gbx_zarr_read, gbx_zarr_sample

    nv = len(zl.VARIABLES)
    verify = cfg.verify and g.logical_bytes <= VERIFY_MAX_BYTES
    legs: List[_Leg] = []
    first = zl.VARIABLES[0]

    # ZL-R1: everything, as a table
    def check_full(df) -> str:
        n = df.count()
        if n != g.hours * g.ny * g.nx:
            return f"count {n} != {g.hours * g.ny * g.nx}"
        if verify:
            got, want = _sum(df, first), _expected_sum(g, first)
            if not _close(got, want):
                return f"sum({first}) {got!r} != expected {want!r}"
        return ""

    legs.append(
        _Leg(
            "ZL-R1",
            lambda: gbx_zarr_read(spark, store, output="table"),
            nv,
            g.hours * g.ny * g.nx,
            check_full,
        )
    )

    # ZL-R2: a box over all time
    rows, cols = _box_idx(g, _BOX[g.family])
    lat_hi, lat_lo, west, east = _BOX[g.family]
    sel = {"latitude": slice(lat_hi, lat_lo), "longitude": slice(west, east)}

    def check_box(df) -> str:
        n = df.count()
        want_n = g.hours * len(rows) * len(cols)
        if n != want_n:
            return f"count {n} != {want_n}"
        if verify:
            got, want = _sum(df, first), _expected_sum(g, first, rows, cols)
            if not _close(got, want):
                return f"sum({first}) {got!r} != expected {want!r}"
        return ""

    legs.append(
        _Leg(
            "ZL-R2",
            lambda: gbx_zarr_read(spark, store, sel=sel, output="table"),
            nv,
            g.hours * len(rows) * len(cols),
            check_box,
        )
    )

    # ZL-R3: 100 sites, every hour
    wkt, pr, pc = _sites(g)
    sites = spark.createDataFrame(
        [(f"site-{i:03d}", w) for i, w in enumerate(wkt)], ["site", "geom"]
    )

    def check_sample(df) -> str:
        n = df.count()
        if n != _N_POINTS * g.hours:
            return f"count {n} != {_N_POINTS * g.hours}"
        if verify and n <= 2_000_000:
            want = 0.0
            for i in range(_N_POINTS):
                a = _block(g, (first,), 0, g.hours, [pr[i]], [pc[i]])[first]
                want += float(a.astype(np.float64).sum())
            got = _sum(df, first)
            if not _close(got, want):
                return f"sum({first}) {got!r} != expected {want!r}"
        return ""

    legs.append(
        _Leg(
            "ZL-R3",
            lambda: gbx_zarr_sample(spark, sites, store, variables=list(zl.VARIABLES)),
            nv,
            _N_POINTS * g.hours,
            check_sample,
        )
    )

    # ZL-R4: wind speed, then the hourly mean over the area
    area = {} if g.family == "county_wind" else sel
    a_rows, a_cols = (None, None) if not area else (rows, cols)

    def build_speed():
        df = gbx_zarr_read(
            spark, store, variables=["u10", "v10"], sel=area or None, output="table"
        )
        u, v = F.col("u10").cast("double"), F.col("v10").cast("double")
        return df.groupBy("time").agg(F.avg(F.sqrt(u * u + v * v)).alias("speed"))

    def check_speed(df) -> str:
        n = df.count()
        if n != g.hours:
            return f"hours {n} != {g.hours}"
        if verify:
            got = _sum(df, "speed")
            want = 0.0
            for t0 in range(0, g.hours, 168):
                b = _block(
                    g, ("u10", "v10"), t0, min(t0 + 168, g.hours), a_rows, a_cols
                )
                u, v = b["u10"].astype(np.float64), b["v10"].astype(np.float64)
                want += float(np.sqrt(u * u + v * v).mean(axis=(1, 2)).sum())
            if not _close(got, want, 1e-8):
                return f"sum(hourly mean) {got!r} != expected {want!r}"
        return ""

    n_area = g.hours * (g.ny * g.nx if not area else len(rows) * len(cols))
    legs.append(_Leg("ZL-R4", build_speed, 2, n_area, check_speed))

    # ZL-R5: one hour as one whole-grid tile
    hour = g.hours // 2
    stamp = str(np.datetime64("2023-01-01T00:00") + np.timedelta64(hour, "h"))[:16]

    def check_tile(df) -> str:
        n = df.count()
        return "" if n == 1 else f"{n} tiles != 1"

    legs.append(
        _Leg(
            "ZL-R5",
            lambda: gbx_zarr_read(
                spark,
                store,
                variables=["u10"],
                sel={"time": stamp},
                output="tiles",
                tile_size=(g.nx, g.ny),
                virtual_tiles=False,
            ),
            1,
            g.ny * g.nx,
            check_tile,
            bytes_touched=g.ny * g.nx * zl._ITEM,
        )
    )
    return [leg for leg in legs if leg.case in cfg.cases]


def _attribution(df, t0_epoch: float) -> dict:
    from databricks.labs.gbx.bench import attribution

    try:
        return attribution.from_plan(df, t0_epoch)
    except Exception:  # noqa: BLE001 - best effort: no UI, no numbers
        return {}


def _median_spans(runs: List[Dict[str, float]]) -> Dict[str, float]:
    """The median seconds of each planning span over the runs of a leg (a run without the span counts 0)."""
    names = sorted({n for r in runs for n in r})
    return {n: round(statistics.median(r.get(n, 0.0) for r in runs), 3) for n in names}


def _spans_text(spans: Dict[str, float]) -> str:
    """The planning spans of a leg as ``name 0.123 s`` pairs, the longest first."""
    ranked = sorted(spans.items(), key=lambda kv: -kv[1])
    return ", ".join(f"{n} {v}" for n, v in ranked) or "no spans"


def _run_leg(
    leg: _Leg, g: zl.Geometry, store: str, layout: dict, cfg: LadderConfig
) -> dict:
    """Time ``cfg.reps`` runs of ``leg``; the metrics of the leg, as a flat dict."""
    plans, execs, taps, busy, peaks, span_runs = [], [], [], [], [], []
    for _ in range(cfg.reps):
        with _tapped(layout["cps"]) as tap:
            t0 = time.perf_counter()
            wall0 = time.time()
            with zspans.record() as spans:
                df = leg.build()
            plan = time.perf_counter() - t0
            t1 = time.perf_counter()
            _noop(df)
            ex = time.perf_counter() - t1
        span_runs.append(spans)
        plans.append(plan)
        execs.append(ex)
        taps.append(tap)
        if cfg.attribution:
            m = _attribution(df, wall0)
            if m.get("py_eval_s") is not None and cfg.cores:
                busy.append(m["py_eval_s"] / (ex * cfg.cores))
            if m.get("peak_exec_mem_bytes") is not None:
                peaks.append(m["peak_exec_mem_bytes"])
    walls = [round(p + e, 3) for p, e in zip(plans, execs)]
    tap = taps[-1]
    # an inner chunk longer than the array (a 24-hour chunk of a one-hour regional cut) holds only the array's cells
    chunk_bytes = (
        math.prod(min(c, n) for c, n in zip(layout["chunks"], layout["shape"]))
        * zl._ITEM
    )
    touched = (
        leg.bytes_touched
        if leg.bytes_touched is not None
        else tap.chunks * chunk_bytes * leg.variables
    )
    # a sample answered by the driver has no unit counted by a task: its "tasks" is 0
    tasks = 0 if tap.route == "driver" else tap.units
    med = statistics.median(walls)
    verdict = leg.check(leg.build()) if leg.check else ""
    out = {
        "case": leg.case,
        "workload": LADDER_CASES[leg.case][0],
        "family": g.family,
        "rung": g.rung,
        "shape": list(g.shape),
        "regional": g.regional,
        "store_bytes": g.logical_bytes,
        "walls_s": walls,
        "cold_s": walls[0],
        "wall_s": med,
        "plan_s": round(statistics.median(plans), 3),
        "exec_s": round(statistics.median(execs), 3),
        "plan_share": round(statistics.median(plans) / med, 3) if med else 0.0,
        "tasks": tasks,
        "route": tap.route,
        "units": tap.units,
        "chunks_touched": tap.chunks * leg.variables,
        "chunks_touched_ref": tap.chunks,  # of the variable the units are cut by: what the tasks divide
        "chunks_total": layout["n_chunks"] * len(zl.VARIABLES),
        "shards_touched": tap.shards * leg.variables,
        "shards_total": layout["n_shards"] * len(zl.VARIABLES),
        "rows": leg.cells,
        "bytes_touched": touched,
        "mb_s": round(touched / 1e6 / med, 1) if med and touched else 0.0,
        "s_per_gb": round(med / (touched / 1e9), 2) if touched else None,
        "busy_share": round(statistics.median(busy), 3) if busy else None,
        "peak_exec_mem_mb": round(max(peaks) / 1e6, 1) if peaks else None,
        "cores": cfg.cores,
        "check": verdict or "ok",
        "plan_spans": _median_spans(span_runs),
    }
    return out


def _row(cfg: LadderConfig, m: dict):
    """The ``ResultRow`` of a leg's metrics (the walls and the structured metrics are in its note)."""
    from databricks.labs.gbx.bench import zarr_bench as zb

    zcfg = zb.ZarrBenchConfig(
        store="",
        cog_dir="",
        scratch="",
        schema="",
        run_id=cfg.run_id,
        serverless=cfg.serverless,
        reps=cfg.reps,
        where=cfg.where,
    )
    fn = f"gbx_zarr_{'sample' if m['case'] == 'ZL-R3' else 'read'}[{m['case']} {m['family']} {m['rung']}]"
    keep = {k: m[k] for k in m if k not in ("walls_s", "cold_s")}
    status = "ok" if m["check"] == "ok" else "error"
    return zb._row(
        zcfg,
        fn,
        m["walls_s"],
        m["rows"],
        f"{m['workload']}; metrics={json.dumps(keep, separators=(',', ':'))}",
        status=status,
        note_cap=2500,
    )


def run_ladder(
    spark,
    cfg: LadderConfig,
    sink: Optional[Callable[[List[ResultRow]], None]] = None,
    collect: Optional[List[dict]] = None,
) -> List[ResultRow]:
    """Run the selected cases on every (family, rung) store under ``cfg.root``; ``sink(rows)`` gets each store's
    rows as it finishes, ``collect`` each leg's metrics dict."""
    for c in cfg.cases:
        ladder_case(c)
    with _unit_budget(cfg.budget_mib):
        return _run_all(spark, cfg, sink, collect)


def _run_all(spark, cfg, sink, collect) -> List[ResultRow]:
    rows: List[ResultRow] = []
    for family in cfg.families:
        for rung in cfg.rungs:
            g = zl.geometry(family, rung)
            store = zl.store_path(cfg.root, family, rung)
            got: List[ResultRow] = []
            if not os.path.exists(os.path.join(store, zl.VARIABLES[0], "zarr.json")):
                got.append(_missing_row(cfg, family, rung, store))
            else:
                layout = store_layout(store, zl.VARIABLES[0])
                got += _run_write_cases(spark, cfg, g, store, collect)
                for leg in _legs(spark, g, store, cfg):
                    print(
                        f"ZARR-LADDER {family} {rung} {leg.case}: starting", flush=True
                    )
                    try:
                        m = _run_leg(leg, g, store, layout, cfg)
                        got.append(_row(cfg, m))
                        if collect is not None:
                            collect.append(m)
                        print(
                            f"ZARR-LADDER {family} {rung} {leg.case}: wall {m['wall_s']} s "
                            f"(plan {m['plan_s']}: {_spans_text(m['plan_spans'])}), "
                            f"tasks {m['tasks']}, check {m['check']}",
                            flush=True,
                        )
                    except Exception as e:  # noqa: BLE001
                        got.append(_error(cfg, leg.case, family, rung, e))
            rows += got
            if sink is not None:
                sink(got)
    return rows


def _run_write_cases(spark, cfg, g, store, collect) -> List[ResultRow]:
    """The ``ZL-W*`` cases of one store: their rows (an error row per case when the writes failed)."""
    from databricks.labs.gbx.bench import zarr_bench as zb
    from databricks.labs.gbx.bench import zarr_ladder_write as zw

    if not any(c in WRITE_CASES for c in cfg.cases):
        return []
    print(f"ZARR-LADDER {g.family} {g.rung} writes: starting", flush=True)
    try:
        ms = zw.run_writes(spark, cfg, g, store)
    except Exception as e:  # noqa: BLE001
        return [
            _error(cfg, c, g.family, g.rung, e) for c in cfg.cases if c in WRITE_CASES
        ]
    zcfg = zb.ZarrBenchConfig(
        "", "", "", "", cfg.run_id, cfg.serverless, reps=cfg.reps, where=cfg.where
    )
    rows = []
    for m in ms:
        if collect is not None:
            collect.append(m)
        print(
            f"ZARR-LADDER {g.family} {g.rung} {m['case']}: wall {m['wall_s']} s, check {m['check']}",
            flush=True,
        )
        fn = f"gbx_zarr_write[{m['case']} {m['family']} {m['rung']}]"
        ok = m["check"] == "ok"
        rows.append(
            zb._row(
                zcfg,
                fn,
                m["walls_s"],
                int(np.prod(m["shape"])),
                (
                    f"{m['workload']}; metrics={zw.row_json(m)}"
                    if m["walls_s"]
                    else f"{m['workload']}: {m['check']}"
                ),
                status="ok" if ok else "error",
                note_cap=2500,
            )
        )
    return rows


def _missing_row(cfg: LadderConfig, family: str, rung: str, store: str) -> ResultRow:
    return _error(
        cfg,
        "store",
        family,
        rung,
        FileNotFoundError(
            f"{store} is missing. Fix: gbx:bench:gen-data --zarr-ladder {rung} --zarr-ladder-family {family}"
        ),
    )


def _error(cfg, case, family, rung, e) -> ResultRow:
    from databricks.labs.gbx.bench import zarr_bench as zb

    zcfg = zb.ZarrBenchConfig(
        "", "", "", "", cfg.run_id, cfg.serverless, where=cfg.where
    )
    return zb._row(
        zcfg,
        f"gbx_zarr_read[{case} {family} {rung}]",
        [],
        0,
        f"{type(e).__name__}: {e}",
        status="error",
    )


# --- the local table and its verdicts ---------------------------------------------------------


def _fit(points: List[Tuple[float, float]]) -> Tuple[float, float]:
    """Least squares ``wall = fixed + slope * GB`` over ``(GB, wall)``; ``(nan, nan)`` under two points."""
    if len(points) < 2:
        return float("nan"), float("nan")
    x = np.array([p[0] for p in points])
    y = np.array([p[1] for p in points])
    if np.ptp(x) == 0:
        return float("nan"), float("nan")
    slope, fixed = np.polyfit(x, y, 1)
    return float(fixed), float(slope)


def summary_table(metrics: List[dict]) -> str:
    metrics = [m for m in metrics if not m.get("case", "").startswith("ZL-W")]
    head = (
        "| family | rung | workload | wall s | cold s | s/GB | tasks | chunks (touched/total) | "
        "shards (touched/total) | plan s | plan share | busy | MB/s | check |\n"
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    )
    lines = []
    for m in metrics:
        lines.append(
            f"| {m['family']} | {m['rung']} | {m['workload']} | {m['wall_s']:.2f} | {m['cold_s']:.2f} | "
            f"{m['s_per_gb'] if m['s_per_gb'] is not None else '-'} | {m['tasks']}{'*' if m['route'] == 'driver' else ''} | "
            f"{m['chunks_touched']}/{m['chunks_total']} | {m['shards_touched']}/{m['shards_total']} | "
            f"{m['plan_s']:.2f} | {'driver' if m['route'] == 'driver' else format(m['plan_share'], '.0%')} | "
            f"{m['busy_share'] if m['busy_share'] is not None else '-'} | {m['mb_s']} | {m['check']} |"
        )
    return head + "\n".join(lines) + "\n(* answered on the driver, no task)"


CONSTANT_WORK = "one_hour_tile"  # work that does not depend on the size of the store
MIN_TASK_BYTES = (
    8 << 20
)  # a task is not worth its overhead under this much decoded data


def verdicts(metrics: List[dict], cores: int, reference: str = "100MB") -> List[str]:
    """The ladder's gates, per (family, workload), as one line each (``FLAG`` or ``ok``):

    * time per GB of the rungs at or above ``reference`` within ``GATE_PER_GB`` of the reference rung's;
    * planning under ``GATE_PLAN_SHARE`` of the wall once it takes ``MIN_PLAN_S`` (not for a sample the driver answered:
      its planning IS the read);
    * tasks at least ``min(cores, chunks touched, touched bytes / 8 MiB)``;
    * a clean read-back check.

    Below the reference rung a fixed cost (job start, planning) dominates, so those rungs are not gated on time per
    GB. The line carries a fit ``wall = fixed + slope x GB`` and the slope between neighbouring rungs: a slope that
    grows by more than ``GATE_PER_GB`` from one segment to the next is superlinear and is flagged.
    """
    out: List[str] = []
    by: Dict[Tuple[str, str], List[dict]] = {}
    for m in metrics:
        if not m.get("case", "").startswith("ZL-W"):
            by.setdefault((m["family"], m["workload"]), []).append(m)
    for (family, workload), ms in sorted(by.items()):
        ms = sorted(ms, key=lambda m: m["store_bytes"])
        ref = next((m for m in ms if m["rung"] == reference), ms[-1])
        flags = []
        if workload == CONSTANT_WORK:
            # one hour of a store that grows: the cost must not grow with it
            for m in ms:
                if m["check"] != "ok":
                    flags.append(f"{m['rung']}: check failed: {m['check']}")
                elif m["store_bytes"] >= ref["store_bytes"] and m[
                    "wall_s"
                ] > GATE_PER_GB * max(ref["wall_s"], 0.2):
                    flags.append(
                        f"{m['rung']}: {m['wall_s']} s for one hour, {ref['rung']} took {ref['wall_s']} s"
                    )
            walls = ", ".join(f"{m['rung']} {m['wall_s']:.2f}" for m in ms)
            out.append(
                f"{'FLAG' if flags else 'ok  '} {family} {workload} [constant work; walls {walls}]"
                + "".join(f"\n       - {f}" for f in flags)
            )
            continue
        for m in ms:
            if m["check"] != "ok":
                flags.append(f"{m['rung']}: check failed: {m['check']}")
            if (
                m["store_bytes"] >= ref["store_bytes"]
                and m["s_per_gb"]
                and ref["s_per_gb"]
                and m["s_per_gb"] > GATE_PER_GB * ref["s_per_gb"]
            ):
                flags.append(
                    f"{m['rung']}: {m['s_per_gb']} s/GB is {m['s_per_gb'] / ref['s_per_gb']:.1f}x the "
                    f"{ref['rung']} rung's ({ref['s_per_gb']})"
                )
            if (
                m["route"] != "driver"
                and m["plan_share"] > GATE_PLAN_SHARE
                and m["plan_s"] > MIN_PLAN_S
            ):
                flags.append(
                    f"{m['rung']}: planning is {m['plan_share']:.0%} of the wall"
                )
            want = min(
                cores,
                max(1, m["chunks_touched_ref"]),
                max(1, int(m["bytes_touched"] // MIN_TASK_BYTES)),
            )
            if m["route"] != "driver" and m["tasks"] < want:
                flags.append(
                    f"{m['rung']}: {m['tasks']} tasks for {m['chunks_touched_ref']} chunks "
                    f"({m['bytes_touched'] / 1e6:.0f} MB) on {cores} cores (want {want})"
                )
        slopes = []
        for lo, hi in zip(ms, ms[1:]):
            d = (hi["bytes_touched"] - lo["bytes_touched"]) / 1e9
            slopes.append((hi["wall_s"] - lo["wall_s"]) / d if d > 0 else float("nan"))
        for (i, s0), s1 in zip(enumerate(slopes), slopes[1:]):
            if s0 > 0 and s1 > GATE_PER_GB * s0:
                flags.append(
                    f"superlinear: {ms[i + 1]['rung']}->{ms[i + 2]['rung']} costs {s1:.2f} s/GB, "
                    f"{ms[i]['rung']}->{ms[i + 1]['rung']} cost {s0:.2f}"
                )
        fixed, slope = _fit([(m["bytes_touched"] / 1e9, m["wall_s"]) for m in ms])
        fit = (
            f" [fit: fixed {fixed:.2f} s + {slope:.2f} s/GB; segment slopes "
            + ", ".join(f"{x:.2f}" for x in slopes)
            + "]"
            if fixed == fixed
            else ""
        )
        out.append(
            f"{'FLAG' if flags else 'ok  '} {family} {workload}{fit}"
            + ("".join(f"\n       - {f}" for f in flags))
        )
    return out


def plan_projection(
    root: str, families, rungs, budgets_mib=(64, 1536), cores: int = 2
) -> List[dict]:
    """What the driver plans for the whole ladder, with no data written: a store holding only its metadata
    (``zarr_ladder.write_skeleton``) is planned by ``gbx_zarr_read(output="table")`` exactly as a full one is (a
    plain store has no manifest to read). Records the planning seconds and the tasks (units) at each unit budget:
    ``budgets_mib`` are the Serverless / local value and the classic-cluster value of the planner; ``cores`` the
    task slots of the session that plans (the planner lowers a unit so each slot gets one).
    """
    import shutil
    import tempfile

    from databricks.labs.gbx.zarr import gbx_zarr_read

    spark = _spark(cores)
    out: List[dict] = []
    scratch = tempfile.mkdtemp(
        prefix="gbx_zl_plan_", dir=root if os.path.isdir(root) else None
    )
    try:
        for family in families:
            for rung in rungs:
                g = zl.geometry(family, rung)
                path = zl.store_path(scratch, family, rung)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                zl.write_skeleton(path, g)
                layout = store_layout(path, zl.VARIABLES[0])
                rec = {"family": family, "rung": rung, "store_bytes": g.logical_bytes,
                       "chunks_ref": layout["n_chunks"], "shards_ref": layout["n_shards"]}  # fmt: skip
                for mib in budgets_mib:
                    with _unit_budget(mib), _tapped(layout["cps"]) as tap:
                        t = time.perf_counter()
                        gbx_zarr_read(spark, path, output="table")
                        rec[f"plan_s@{mib}"] = round(time.perf_counter() - t, 3)
                        rec[f"tasks@{mib}"] = tap.units
                out.append(rec)
                shutil.rmtree(
                    os.path.dirname(path) + "/" + f"{rung}.zarr", ignore_errors=True
                )
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return out


# --- the local runner -----------------------------------------------------------------------------


def _delta_jars() -> List[str]:
    """The delta-spark and delta-storage jars of the Maven cache (``GBX_M2_REPO``, the checkout's, the user's)."""
    from pathlib import Path

    v = "4.0.1"
    repos = [
        os.environ.get("GBX_M2_REPO"),
        Path(__file__).resolve().parents[7] / "scripts" / "docker" / "m2",
        Path.home() / ".m2" / "repository",
    ]
    for repo in filter(None, repos):
        jars = [
            Path(repo) / "io/delta/delta-spark_2.13" / v / f"delta-spark_2.13-{v}.jar",
            Path(repo) / "io/delta/delta-storage" / v / f"delta-storage-{v}.jar",
        ]
        if all(j.is_file() for j in jars):
            return [str(j) for j in jars]
    return []


def _spark(cores: int, delta: bool = False):
    """A ``local[cores]`` session; with ``delta`` the OSS Delta Lake (the write legs' manifest and staging tables)."""
    import shlex
    import tempfile

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if delta and "PYSPARK_SUBMIT_ARGS" not in os.environ:
        jars = _delta_jars()
        if not jars:
            raise RuntimeError(
                "PRECONDITION MISSING: the delta-spark 4.0.1 jars are not in the Maven cache. "
                "Fix: run scripts/ci/fetch-delta-test-jars.sh (gbx:test:python does)."
            )
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-class-path {shlex.quote(os.pathsep.join(jars))} pyspark-shell"
        )
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName("gbx-zarr-ladder")
        .config("spark.driver.memory", "6g")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4050")
    )
    if delta:
        b = (
            b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="gbx_zl_wh_"))
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        )
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Local run of the Zarr ladder read legs. Exit 0: no flags; 1: a gate flagged; 3: failed."
    )
    ap.add_argument("--root", required=True, help="ladder directory")
    ap.add_argument("--rungs", default="10MB,50MB,100MB")
    ap.add_argument("--families", default=",".join(zl.FAMILIES))
    ap.add_argument("--cases", default=",".join(LADDER_CASES))
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--cores", type=int, default=8)
    ap.add_argument(
        "--scratch", default=None, help="where the write legs put their stores"
    )
    ap.add_argument(
        "--generate", action="store_true", help="write the missing stores first"
    )
    ap.add_argument("--json-out", help="write the metrics of every leg here")
    ap.add_argument(
        "--budget-mib",
        type=int,
        default=None,
        help="plan reads with this unit budget (MiB)",
    )
    ap.add_argument(
        "--plan-only",
        action="store_true",
        help="no reads: plan the whole ladder (all rungs) from metadata-only stores and print the tasks at "
        "the Serverless/local and classic unit budgets",
    )
    a = ap.parse_args(argv)
    if a.plan_only:
        rows = plan_projection(
            a.root, [f for f in a.families.split(",") if f], zl.RUNGS, cores=a.cores
        )
        print(
            "| family | rung | store MB | chunks/var | shards/var | plan s @64MiB | tasks @64MiB | plan s @1536MiB | tasks @1536MiB |\n|---|---|---|---|---|---|---|---|---|"
        )
        for r in rows:
            print(f"| {r['family']} | {r['rung']} | {r['store_bytes'] / 1e6:.0f} | {r['chunks_ref']} | {r['shards_ref']} | "
                  f"{r['plan_s@64']} | {r['tasks@64']} | {r['plan_s@1536']} | {r['tasks@1536']} |")  # fmt: skip
        if a.json_out:
            with open(a.json_out, "w") as fh:
                json.dump(rows, fh, indent=1)
        return 0
    rungs = tuple(r for r in a.rungs.split(",") if r)
    families = tuple(f for f in a.families.split(",") if f)
    if a.generate:
        for fam in families:
            for rung in rungs:
                r = zl.generate(a.root, fam, rung)
                print(
                    f"[zarr-ladder] {fam} {rung}: logical {r['logical_bytes'] / 1e6:.1f} MB, "
                    f"disk {r['disk_bytes'] / 1e6:.1f} MB, {r['n_chunks']} chunks / {r['n_shards']} shards, "
                    f"{'kept' if r['kept'] else str(r['gen_s']) + ' s'}",
                    flush=True,
                )
    cfg = LadderConfig(
        root=a.root,
        rungs=rungs,
        families=families,
        cases=tuple(c for c in a.cases.split(",") if c),
        reps=a.reps,
        cores=a.cores,
        attribution=True,
        budget_mib=a.budget_mib,
        scratch=a.scratch,
    )
    spark = _spark(a.cores, delta=any(c in WRITE_CASES for c in cfg.cases))
    metrics: List[dict] = []
    rows = run_ladder(spark, cfg, collect=metrics)
    if a.json_out:
        with open(a.json_out, "w") as fh:
            json.dump(metrics, fh, indent=1)
    from databricks.labs.gbx.bench import zarr_ladder_write as zw

    if any(m["case"].startswith("ZL-R") for m in metrics):
        print("\n" + summary_table(metrics))
    wm = [m for m in metrics if m["case"] in WRITE_CASES]
    if wm:
        print("\nWRITES (table to Zarr):\n" + zw.write_table(wm))
    errors = [r for r in rows if r.status != "ok"]
    for r in errors:
        print(f"ERROR {r.fn}: {r.note}", file=sys.stderr)
    print(
        "\nGATES (s/GB within 1.5x of the 100MB rung, plan < 10%, tasks >= min(cores, chunks)):"
    )
    lines = verdicts(metrics, a.cores) + zw.write_verdicts(wm)
    print("\n".join(lines))
    return 1 if (errors or any(line.startswith("FLAG") for line in lines)) else 0


if __name__ == "__main__":
    sys.exit(main())
