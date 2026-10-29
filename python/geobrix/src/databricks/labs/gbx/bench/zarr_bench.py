"""Zarr benchmark legs (light tier only): the cases of ``spec.ZARR_CASES``.

- **R1-R3** read the ERA5 January 2023 store (a plain v3 sharded store, 744 h x 721 x
  1440) with ``gbx_zarr_read`` / ``gbx_zarr_sample``, and the same ``t2m`` values as one
  tiled GeoTIFF per hour (``readers.stage_zarr_cog_corpus``) with ``raster_gbx`` plus a
  pixel function (``rst_avg`` / ``rst_sample``), so both sides decode the values they read.
- **W1** creates the prototype's 1 GB throughput cube with ``gbx_zarr_write`` and with plain
  ``xarray.to_zarr`` region writes to the same kind of path, both from data generated in
  the tasks (the prototype's recipe, ``field``), and checks the gate.
- **W2** appends 8 days to a one-day store, all at once from 8 driver threads and one
  after another.
- Two probes for the write and sample paths: ``shard_index_probe`` (the cost of a shard's
  index against its inner-chunk count) and ``sample_density_probe`` (vectorized point reads
  against a box read and gather, by points per chunk).

Every leg times ``reps`` runs (3; the first is cold: a fresh query on data no run of this
leg has read yet) and returns ``ResultRow``s whose ``note`` carries the walls, so the
cold/warm split survives in the results table. Nothing here calls ``spark.conf.set``,
``sparkContext``, ``.rdd`` or ``_jvm``: the legs run on Serverless too.
"""

from __future__ import annotations

import dataclasses
import functools
import hashlib
import json
import os
import shutil
import statistics
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, List, Optional

import numpy as np

from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, peak_rss_mb
from databricks.labs.gbx.bench.spec import zarr_case

__all__ = [
    "ZarrBenchConfig",
    "field",
    "run_zarr_cases",
    "sample_density_probe",
    "shard_index_probe",
    "update_window_probe",
    "w1_fixed",
    "w1_fixed_verdict",
    "w1_verdict",
]

_MB = 1e6  # throughput in decimal MB/s, as the prototype reported it
_R2_COG_HOURS = 4  # hourly files the rst_sample baseline reads (one task each)
_DIMS = ("time", "latitude", "longitude")


@dataclass
class ZarrBenchConfig:
    """Where the legs read and write.

    ``store``: the plain ERA5 store; ``cog_dir``: its GeoTIFF-per-hour copy (with
    ``cog_hourly_manifest.json``); ``scratch``: a directory the write legs may fill and
    empty (on a Volume on Databricks); ``schema``: a catalog.schema for the manifest
    tables (dropped after each run); ``serverless``: picks the W1 gate.
    """

    store: str
    cog_dir: str
    scratch: str
    schema: str
    run_id: str
    serverless: bool = False
    variable: str = "t2m"
    reps: int = 3
    where: str = "cluster"
    w1_shape: tuple = (240, 721, 1440)
    w1_access: tuple = ("fuse", "direct")
    r2_budget_s: float = 600.0  # the baseline's three runs must fit, else fewer points
    cases: tuple = ("R1", "R2", "R3", "W1", "W2")


# --- timing and rows ----------------------------------------------------------------------


def _walls(fn: Callable[[], object], reps: int) -> List[float]:
    out = []
    for _ in range(reps):
        t = time.perf_counter()
        fn()
        out.append(round(time.perf_counter() - t, 3))
    return out


def _noop(df) -> None:
    df.write.format("noop").mode("overwrite").save()


def _row(
    cfg: ZarrBenchConfig,
    fn: str,
    walls: List[float],
    rows: int,
    note: str,
    status: str = "ok",
    tput_rows: Optional[float] = None,
    note_cap: int = 2000,
    cold: Optional[float] = None,
) -> ResultRow:
    """A result row. ``walls`` are the timed runs; with ``cold`` (the leg's first run, on
    compute that had not run it before) they are the warm runs after it, and the medians are
    theirs. Every note starts with the installed wheel's fingerprint (``wheel_record``).
    """
    med = statistics.median(walls) if walls else 0.0
    if cold is not None:
        walls_txt = f"cold_s={cold}; warm_s={walls}"
    else:
        walls_txt = f"walls_s={walls} (run 1 cold)" if walls else "walls_s=[]"
    walls_txt = f"wheel_record={wheel_fingerprint()}; {walls_txt}"
    return ResultRow(
        run_id=cfg.run_id,
        api="lightweight",
        fn=fn,
        category="zarr",
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="float32",
        srid=4326,
        rows=int(rows),
        nodata_frac=0.0,
        warmup_iters=0,
        measured_iters=len(walls),
        iter_median_s=med,
        iter_min_s=min(walls) if walls else 0.0,
        iter_p90_s=max(walls) if walls else 0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=(
            tput_rows if tput_rows is not None else (rows / med if med else 0.0)
        ),
        peak_rss_mb=peak_rss_mb(),
        status=status,
        note=f"{walls_txt}; {note}"[:note_cap],
        iter_total_wall_clock_s=sum(walls),
        avg_wall_clock_s=(sum(walls) / len(walls)) if walls else 0.0,
        **capture_env(cfg.where),
    )


@functools.lru_cache(maxsize=1)
def wheel_fingerprint() -> str:
    """The first 16 hex digits of the sha256 of the installed geobrix ``RECORD`` (the list of
    every installed file with its own sha256), ``"unknown"`` when there is none (a source
    tree on the path). Two runs with one fingerprint ran the same wheel contents; the
    staging side computes it from the wheel file the same way (``record_fingerprint``).
    """
    from importlib.metadata import PackageNotFoundError, distribution

    try:
        text = distribution("geobrix").read_text("RECORD")
    except PackageNotFoundError:
        return "unknown"
    return record_fingerprint(text) if text else "unknown"


def record_fingerprint(record_text: str) -> str:
    """``wheel_fingerprint`` of a ``RECORD`` text: its lines that name a package file (the
    installed one also lists the ``.pyc`` files and installer lines a wheel lacks), sorted.
    """
    lines = sorted(
        line
        for line in record_text.splitlines()
        if line.startswith("databricks/") and not line.split(",")[0].endswith(".pyc")
    )
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


def _error_row(cfg: ZarrBenchConfig, fn: str, e: BaseException) -> ResultRow:
    return _row(cfg, fn, [], 0, f"{type(e).__name__}: {e}", status="error")


# --- the store and its GeoTIFF copy --------------------------------------------------------


def _coords(store: str):
    """``(times, latitudes, longitudes)`` of the plain store (decoded, numpy)."""
    import xarray as xr

    ds = xr.open_zarr(store, consolidated=None, decode_times=True)
    return ds["time"].values, ds["latitude"].values, ds["longitude"].values


def _cog_manifest(cog_dir: str) -> dict:
    with open(os.path.join(cog_dir, "cog_hourly_manifest.json")) as fh:
        return json.load(fh)


def _cog_frame(spark, files: List[str], **options):
    """``raster_gbx`` over exactly ``files`` (one directory): the reader takes one path, so
    the files are picked by a full-path ``filterRegex``."""
    import re

    folder = os.path.dirname(files[0])
    names = "|".join(re.escape(os.path.basename(f)) for f in files)
    reader = spark.read.format("raster_gbx").option("filterRegex", f".*/(?:{names})$")
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load(folder)


def _register_light(spark) -> None:
    from databricks.labs.gbx.ds.register import register

    register(spark)


# --- R1: one time step, full globe --------------------------------------------------------


def run_r1(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.zarr import gbx_zarr_read

    case = zarr_case("R1")
    times, lat, lon = _coords(cfg.store)
    t = min(int(case.args["time_index"]), len(times) - 1)
    cells = len(lat) * len(lon)
    out = []
    fn = f"{case.name}[R1]"
    try:
        df = lambda: gbx_zarr_read(  # noqa: E731
            spark, cfg.store, variables=cfg.variable, isel={"time": t}
        )
        walls = _walls(lambda: _noop(df()), cfg.reps)
        blocks = df().count()
        out.append(
            _row(
                cfg,
                fn,
                walls,
                cells,
                f"gbx_zarr_read blocks, time index {t} ({times[t]}), {blocks} blocks, "
                f"{cells} cells decoded",
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    fn = "raster_gbx+rst_avg[R1]"
    try:
        _register_light(spark)
        path = _cog_manifest(cfg.cog_dir)["files"][t]
        df = lambda: (  # noqa: E731
            spark.read.format("raster_gbx")
            .load(path)
            .select(prx.rst_avg(F.col("tile")).alias("avg"))
        )
        walls = _walls(lambda: _noop(df()), cfg.reps)
        out.append(
            _row(
                cfg, fn, walls, cells, f"one GeoTIFF ({os.path.basename(path)}) decoded"
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    return out


# --- R2: points x every hour ---------------------------------------------------------------


def _points(spark, n: int, seed: int):
    import shapely

    rng = np.random.default_rng(seed)
    lat = rng.uniform(-89.5, 89.5, n)
    lon = rng.uniform(-180.0, 180.0, n)
    rows = [
        (i, shapely.to_wkb(shapely.Point(float(x), float(y))))
        for i, (x, y) in enumerate(zip(lon, lat))
    ]
    return spark.createDataFrame(rows, "pid INT, geom BINARY")


def run_r2(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.zarr import gbx_zarr_sample

    case = zarr_case("R2")
    times, _, _ = _coords(cfg.store)
    n_pts = int(case.args["points"])
    points = _points(spark, n_pts, int(case.args["seed"]))
    out = []
    fn = f"{case.name}[R2]"
    try:
        df = lambda: gbx_zarr_sample(  # noqa: E731
            spark, points, cfg.store, variables=cfg.variable
        )
        walls = _walls(lambda: _noop(df()), cfg.reps)
        rows = n_pts * len(times)
        out.append(
            _row(
                cfg,
                fn,
                walls,
                rows,
                f"gbx_zarr_sample, {n_pts} points x {len(times)} hours",
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))

    fn = "raster_gbx+rst_sample[R2]"
    try:
        _register_light(spark)
        files = _cog_manifest(cfg.cog_dir)["files"]

        # The baseline is one Python call per point and hour, so it is measured on a few
        # hourly files (one task each, side by side) and as many points as the budget
        # allows; the full workload is reported as an extrapolation from that.
        hours = min(_R2_COG_HOURS, len(files))

        def job(n):
            tiles = _cog_frame(spark, files[:hours])
            _noop(
                tiles.crossJoin(F.broadcast(points.limit(n))).select(
                    prx.rst_sample(F.col("tile"), F.col("geom")).alias("v")
                )
            )

        probe = _walls(lambda: job(10), 1)[0]  # cold, sizes the measured run
        n = _r2_points(probe / 10, cfg.reps, cfg.r2_budget_s, n_pts)
        walls = _walls(lambda: job(n), cfg.reps)
        med = statistics.median(walls)
        per_sample = med / n  # seconds per point in one task (one hourly file)
        full = per_sample * n_pts * len(files)
        out.append(
            _row(
                cfg,
                fn,
                walls,
                n * hours,
                f"rst_sample over {hours} hourly GeoTIFFs (one task each) x {n} of "
                f"{n_pts} points (cross join); probe {probe:.1f} s for 10 points; "
                f"{per_sample:.3f} task-seconds per point-hour; the {n_pts} points x "
                f"{len(files)} h workload is EXTRAPOLATED: {full:.0f} task-seconds "
                f"({full / 4:.0f} s on 4 task slots)",
                tput_rows=(n * hours / med) if med else 0.0,
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    return out


def _r2_points(per_point: float, reps: int, budget_s: float, n_pts: int) -> int:
    """Points the GeoTIFF baseline of R2 measures: the most of ``n_pts``, 300, 100 and 30
    whose ``reps`` runs fit ``budget_s`` at ``per_point`` seconds a point, else 10."""
    for p in (n_pts, 300, 100, 30):
        if p * per_point * reps <= budget_s:
            return p
    return 10


# --- R3: a 30-day box ------------------------------------------------------------------------


def _box_window(lat, lon, lat_hi, lat_lo, lon_west, lon_east, roll: int):
    """The GeoTIFF pixel window ``(col, row, width, height)`` of the box, for a copy whose
    columns were rolled by ``roll`` (longitudes moved to -180..180)."""
    rows = np.nonzero((lat <= lat_hi) & (lat >= lat_lo))[0]
    lon180 = np.roll(np.where(lon >= 180.0, lon - 360.0, lon), roll)
    west = lon_west - 360.0 if lon_west >= 180.0 else lon_west
    east = lon_east - 360.0 if lon_east >= 180.0 else lon_east
    cols = np.nonzero((lon180 >= west) & (lon180 <= east))[0]
    return (
        int(cols[0]),
        int(rows[0]),
        int(cols[-1] - cols[0] + 1),
        int(rows[-1] - rows[0] + 1),
    )


def run_r3(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.zarr import gbx_zarr_read

    case = zarr_case("R3")
    times, lat, lon = _coords(cfg.store)
    hours = min(int(case.args["hours"]), len(times))
    (lat_hi, lat_lo), (west, east) = case.args["latitude"], case.args["longitude"]
    sel = {
        "time": slice(str(times[0]), str(times[hours - 1])),
        "latitude": slice(lat_hi, lat_lo),
        "longitude": slice(west, east),
    }
    out = []
    fn = f"{case.name}[R3]"
    try:
        df = lambda: gbx_zarr_read(  # noqa: E731
            spark, cfg.store, variables=cfg.variable, sel=sel
        )
        walls = _walls(lambda: _noop(df()), cfg.reps)
        rows = df().selectExpr("aggregate(shape, 1L, (a, x) -> a * x) AS n")
        cells = rows.agg(F.sum("n")).first()[0]
        out.append(
            _row(cfg, fn, walls, int(cells), f"gbx_zarr_read blocks, sel={sel!r}")
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    fn = "raster_gbx+rst_avg[R3]"
    try:
        _register_light(spark)
        man = _cog_manifest(cfg.cog_dir)
        win = _box_window(lat, lon, lat_hi, lat_lo, west, east, int(man["lon_roll"]))
        files = man["files"][:hours]
        df = lambda: _cog_frame(  # noqa: E731
            spark, files, windows=json.dumps(list(win))
        ).select(prx.rst_avg(F.col("tile")).alias("avg"))
        walls = _walls(lambda: _noop(df()), cfg.reps)
        out.append(
            _row(
                cfg,
                fn,
                walls,
                hours * win[2] * win[3],
                f"window {list(win)} of {hours} GeoTIFFs decoded",
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    return out


# --- W1: the 1 GB throughput cube ------------------------------------------------------------


def field(t0: int, t1: int, ny: int, nx: int, seed: int = 0) -> np.ndarray:
    """The prototype's throughput data: float32 ``(t1 - t0, ny, nx)``, a smooth field plus
    seeded noise per time step (the spike's ``txnlib.field``)."""
    out = np.empty((t1 - t0, ny, nx), np.float32)
    yy = np.linspace(-90, 90, ny, dtype=np.float32)[:, None]
    xx = np.linspace(-180, 180, nx, dtype=np.float32)[None, :]
    base = 15 * np.cos(np.deg2rad(yy))
    for i, t in enumerate(range(t0, t1)):
        rng = np.random.default_rng(t * 1000 + seed)
        out[i] = (
            base
            + 5 * np.sin(np.deg2rad(xx * 2 + t))
            + rng.standard_normal((ny, nx), dtype=np.float32) * 0.5
            + seed
        )
    return out


def _labels(nt: int, ny: int, nx: int, t_start: int = 0):
    import pandas as pd

    time_ = pd.date_range("2023-01-01", periods=t_start + nt, freq="h").values[t_start:]
    return time_, np.linspace(90.0, -90.0, ny), np.arange(nx) * (360.0 / nx)


def _coord_block(name, values, start=0):
    values = np.ascontiguousarray(values)
    return (
        name,
        [name],
        [start],
        [start + len(values)],
        None,
        None,
        values.dtype.str,
        [len(values)],
        values.tobytes(),
        None,
        None,
    )


def _one_per_task(spark, n: int):
    """``n`` ids, one in each of ``n`` partitions, so each block is made by a task of its
    own. A range split ``n`` ways does that with no shuffle; ``repartition(n, "id")`` hashes
    the ids, and ten of them land in seven partitions (three tasks make two blocks each).
    """
    return spark.range(0, n, 1, n)


def _cube_blocks(spark, nt, ny, nx, block, t_start=0, coords=("time", "lat", "lon")):
    """A blocks DataFrame of the throughput cube's steps ``[t_start, t_start + nt)``: one
    ``t2m`` block per ``block`` steps, made in its own task, plus coordinate blocks."""
    import pyarrow as pa

    from databricks.labs.gbx.zarr._read import BLOCKS_SCHEMA

    n = -(-nt // block)

    def gen(batches):
        for b in batches:
            for i in b.column("id").to_pylist():
                lo, hi = t_start + i * block, min(
                    t_start + (i + 1) * block, t_start + nt
                )
                data = field(lo, hi, ny, nx)
                yield pa.RecordBatch.from_pylist(
                    [
                        {
                            "variable": "t2m",
                            "dims": list(_DIMS),
                            "start": [lo, 0, 0],
                            "stop": [hi, ny, nx],
                            "coord_start": None,
                            "coord_stop": None,
                            "dtype": data.dtype.str,
                            "shape": list(data.shape),
                            "data": data.tobytes(),
                            "fill_value": None,
                            "attrs": None,
                        }
                    ],
                    schema=_blocks_arrow(),
                )

    data = _one_per_task(spark, n).mapInArrow(gen, BLOCKS_SCHEMA)
    t, la, lo = _labels(nt, ny, nx, t_start)
    want = {"time": ("time", t, t_start), "lat": ("latitude", la, 0)}
    want["lon"] = ("longitude", lo, 0)
    rows = [_coord_block(n_, v, s) for k, (n_, v, s) in want.items() if k in coords]
    return data.unionByName(spark.createDataFrame(rows, BLOCKS_SCHEMA))


def _blocks_arrow():
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    return _BLOCKS_ARROW


class _Phases:
    """Driver-side timers around the write's steps (bench only); ``restore`` undoes them."""

    def __init__(self):
        self.t: dict = {}
        self.traces: list = []  # per-task traces of the pack stage (_write._TRACE)
        self._lock = threading.Lock()
        self._reals: list = []

    def wrap(self, mod, name, label):
        real = getattr(mod, name)

        def timed(*a, **k):
            t = time.perf_counter()
            try:
                return real(*a, **k)
            finally:
                with self._lock:
                    self.t[label] = self.t.get(label, 0.0) + time.perf_counter() - t

        setattr(mod, name, timed)
        self._reals.append((mod, name, real))
        return real

    def flag(self, mod, name, label):
        """Record ``label`` as 1.0 when ``mod.name`` last returned true, else 0.0."""
        real = getattr(mod, name)

        def recorded(*a, **k):
            out = real(*a, **k)
            with self._lock:
                self.t[label] = 1.0 if out else 0.0
            return out

        setattr(mod, name, recorded)
        self._reals.append((mod, name, real))

    def restore(self):
        for mod, name, real in reversed(self._reals):
            setattr(mod, name, real)
        self._reals = []


def _w1_phases():
    """The create's steps, timed: setup (table check, root check, file mode, access, the
    ownership claim and its parts, CREATE TABLE), the position pass, the files phase (and
    whether its blocks were packed where made: ``files: packed`` is 1), and the commit
    with its parts. A part's time is inside its step's."""
    from databricks.labs.gbx.zarr import _manifest, _register, _write

    ph = _Phases()
    for mod, name, label in (
        (_register, "_table_exists", "setup: table check"),
        (_write, "_check_new_root", "setup: root check"),
        (_write, "resolve_file_mode", "setup: file mode"),
        (_write, "_write_access", "setup: access"),
        (_register, "claim_root", "setup: ownership claim"),
        (_register, "_found_ledger", "claim: found ledger"),
        (_register, "_ledger_state", "claim: ledger dir check"),
        (_register, "_create_ledger", "claim: create ledger"),
        (_register, "_held", "claim: read ledger"),
        (_register, "_claim_marker", "claim: marker"),
        (_manifest, "create_manifest", "setup: create table"),
        (_write, "_planned_blocks", "position pass"),
        (_write, "_write_files", "files"),
        (_write, "_commit", "commit"),
        (_write, "_coordinate_rows", "commit: coordinates"),
        (_write, "_current_user", "commit: user"),
        (_write, "_require_packs", "commit: pack check"),
        (_manifest, "commit", "commit: manifest"),
        (_manifest, "find_commit_version", "commit: version lookups"),
        (_manifest, "_with_view", "commit: merge"),
    ):
        ph.wrap(mod, name, label)
    ph.flag(_write, "_aligned", "files: packed")
    _count_tasks(ph, _write)
    return ph


def _count_tasks(ph: "_Phases", _write) -> None:
    """Record, from the digest rows the files phase returns (one per task), how many tasks
    made data blocks (``files: data tasks``) and the most blocks one of them made
    (``files: most blocks in a task``, coordinate blocks included)."""
    import pyarrow.compute as pc

    real = _write._side_info

    def counted(rows):
        infos = [
            json.loads(t)
            for t in rows.filter(pc.equal(rows.column("rtype"), "digest"))
            .column("info")
            .to_pylist()
        ]
        data = [i["count"] for i in infos if i.get("variables")]
        with ph._lock:
            ph.t["files: data tasks"] = float(len(data))
            ph.t["files: most blocks in a task"] = float(max(data, default=0))
            ph.traces = [
                i["trace"] for i in infos if "trace" in i and i.get("variables")
            ]
        return real(rows)

    _write._side_info = counted
    ph._reals.append((_write, "_side_info", real))


def _drop(spark, table: str, root: str) -> None:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def w1_verdict(mb_s: float, gate_key: str) -> tuple[str, float]:
    """``("PASS" | "MISS", floor)``: part 1 of the W1 gate for ``gate_key``
    (``dbr19_fuse``, ``dbr19_direct`` or ``serverless``): the files phase's MB/s within
    the case's tolerance of the prototype.
    """
    case = zarr_case("W1")
    floor = case.args["gate_mb_s"][gate_key] * (1.0 - case.args["gate_tolerance"])
    return ("PASS" if mb_s >= floor else "MISS"), round(floor, 1)


_FIXED_STEPS = ("setup: ownership claim", "setup: create table", "commit")


def w1_fixed(phases: dict) -> float:
    """A create's fixed cost (part 2 of the W1 gate): its ownership claim, CREATE TABLE and
    commit, from one run's phase timers. As the gate words it: the checks before them (table
    check, root check, file mode, access: 0.1-0.6 s in measured runs) and the position pass
    are not in it."""
    return round(sum(phases.get(k, 0.0) for k in _FIXED_STEPS), 2)


def w1_fixed_verdict(seconds: float) -> tuple[str, float]:
    """``("PASS" | "MISS", limit)``: part 2 of the W1 gate."""
    limit = float(zarr_case("W1").args["fixed_cost_s"])
    return ("PASS" if seconds <= limit else "MISS"), limit


def run_w1(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    from databricks.labs.gbx.zarr import gbx_zarr_write

    case = zarr_case("W1")
    nt, ny, nx = cfg.w1_shape
    block = int(case.args["chunks"]["time"])
    nbytes = nt * ny * nx * 4
    chunks = dict(case.args["chunks"])
    shards = dict(case.args["shards"])
    if (ny, nx) != (721, 1440):  # a smaller cube (local runs): one shard per block
        chunks = {"time": block, "latitude": -(-ny // 4), "longitude": -(-nx // 4)}
        shards = {
            "time": block,
            "latitude": 4 * chunks["latitude"],
            "longitude": 4 * chunks["longitude"],
        }
    out = []
    accesses = ("auto",) if cfg.serverless else tuple(cfg.w1_access)
    legs = {
        a: {"walls": [], "phases": [], "at": [], "check": "", "error": None}
        for a in accesses
    }
    made = 0  # creates run on this compute so far
    for rep in range(cfg.reps):
        for access in _leg_order(accesses, rep):
            leg = legs[access]
            if leg["error"] is not None:
                continue
            tag = f"{cfg.run_id}_{access}_{rep}_{uuid.uuid4().hex[:6]}"
            table = f"{cfg.schema}.t22_w1_{tag}".replace("-", "_")
            root = os.path.join(cfg.scratch, "w1", tag)
            ph = _w1_phases()
            try:
                data = _cube_blocks(spark, nt, ny, nx, block)
                t = time.perf_counter()
                gbx_zarr_write(
                    data,
                    table,
                    mode="create",
                    root=root,
                    chunks=chunks,
                    shards=shards,
                    access=access,
                    spark=spark,
                )
                leg["walls"].append(round(time.perf_counter() - t, 3))
                made += 1
                leg["at"].append(made)
                leg["phases"].append({k: round(v, 2) for k, v in ph.t.items()})
                if rep == 0:
                    leg["check"] = _w1_check(spark, table, ny, nx)
            except Exception as e:  # noqa: BLE001
                leg["error"] = e
            finally:
                ph.restore()
                _drop(spark, table, root)
    for access in accesses:
        leg = legs[access]
        fn = f"{case.name}[W1,{access}]"
        if leg["error"] is not None:
            out.append(_error_row(cfg, fn, leg["error"]))
            continue
        gate_key = "serverless" if cfg.serverless else f"dbr19_{access}"
        out.append(_w1_row(cfg, fn, nt, nbytes, gate_key, leg))
    fn = "xarray.to_zarr[W1]"
    try:
        walls = []
        for rep in range(cfg.reps):
            path = os.path.join(cfg.scratch, "w1", f"plain_{uuid.uuid4().hex[:8]}.zarr")
            try:
                walls.append(_plain_w1(spark, path, nt, ny, nx, block, chunks, shards))
            finally:
                shutil.rmtree(path, ignore_errors=True)
        warm = walls[1:] or walls
        lo, hi = min(warm), max(warm)
        mb_s = nbytes / _MB / statistics.median(warm)
        out.append(
            _row(
                cfg,
                fn,
                warm,
                nt,
                f"WARM median {statistics.median(warm):.2f} s (min {lo:.2f}, max {hi:.2f}) "
                f"= {mb_s:.1f} MB/s; xarray.to_zarr region writes, one task per {block} "
                "steps, sharded like the GeoBrix store",
                tput_rows=mb_s,
                cold=walls[0] if len(walls) > 1 else None,
            )
        )
    except Exception as e:  # noqa: BLE001
        out.append(_error_row(cfg, fn, e))
    return out


def _leg_order(accesses, rep: int) -> tuple:
    """The legs of repetition ``rep`` (from 0) in the order they run: as given in even
    repetitions, reversed in odd ones, so that no leg always runs first."""
    return tuple(accesses) if rep % 2 == 0 else tuple(reversed(accesses))


def _rotated(seq, i: int) -> tuple:
    """``seq`` rotated by ``i``: the order a probe runs its variants in at step ``i``, so that
    no variant always runs first."""
    seq = tuple(seq)
    k = i % len(seq) if seq else 0
    return seq[k:] + seq[:k]


def _spread(values) -> tuple:
    return statistics.median(values), min(values), max(values)


def w1_summary(nbytes: float, gate_key: str, walls, phases) -> dict:
    """Both gate parts of one W1 leg. ``verdict``: over its WARM runs (every run after its
    first, the cold one), the median with min and max (``statistics.median``). ``first3``:
    the earlier protocol, the median of its first three runs (the cold one included), kept
    for comparison."""
    files = [p.get("files", 0.0) for p in phases]
    fixed = [w1_fixed(p) for p in phases]
    warm = slice(1, None) if len(files) > 1 else slice(None)

    def parts(f, x):
        f_med, f_min, f_max = _spread(f)
        mb = nbytes / _MB / f_med if f_med else 0.0
        x_med, x_min, x_max = _spread(x)
        v1, floor = w1_verdict(mb, gate_key)
        v2, limit = w1_fixed_verdict(x_med)
        return {
            "runs": len(f),
            "files_s": (f_med, f_min, f_max),
            "files_mb_s": mb,
            "part1": v1,
            "floor": floor,
            "fixed_s": (x_med, x_min, x_max),
            "part2": v2,
            "limit": limit,
        }

    return {
        "cold": {"wall": walls[0], "files": files[0], "fixed": fixed[0]},
        "warm": parts(files[warm], fixed[warm]),
        "first3": parts(files[:3], fixed[:3]),
        "files": files,
        "fixed": fixed,
    }


def _w1_row(cfg, fn, nt, nbytes, gate_key, leg) -> ResultRow:
    walls, phases = leg["walls"], leg["phases"]
    sm = w1_summary(nbytes, gate_key, walls, phases)
    w, f3 = sm["warm"], sm["first3"]

    def triple(t):
        return f"{t[0]:.2f} (min {t[1]:.2f}, max {t[2]:.2f})"

    note = (
        f"{gate_key}; WARM ({w['runs']} runs after the cold one): files phase median "
        f"{triple(w['files_s'])} s = {w['files_mb_s']:.1f} MB/s, part 1 >= {w['floor']} "
        f"MB/s: {w['part1']}; fixed cost median {triple(w['fixed_s'])} s, part 2 <= "
        f"{w['limit']:.0f} s: {w['part2']}. COLD (this leg's first create, create "
        f"#{leg['at'][0]} on this compute): wall {sm['cold']['wall']} s, files "
        f"{sm['cold']['files']} s, fixed {sm['cold']['fixed']} s. FIRST THREE (cold "
        f"included): files {f3['files_mb_s']:.1f} MB/s {f3['part1']}, fixed "
        f"{f3['fixed_s'][0]:.2f} s {f3['part2']}. files per run {sm['files']}; fixed per "
        f"run {sm['fixed']}; create # on this compute per run {leg['at']}; phases "
        f"{phases}; {leg['check']}"
    )
    return _row(
        cfg,
        fn,
        walls[1:] or walls,
        nt,
        note,
        tput_rows=w["files_mb_s"],
        note_cap=1 << 16,  # every run's phases
        cold=walls[0] if len(walls) > 1 else None,
    )


# --- W1D: the direct leg of W1 against plain object-store puts on the same nodes ---------------


_W1D_PACK_BYTES = (
    87_000_000  # a W1 block's pack (16 zstd chunks of a 24 x 721 x 1440 float32 shard)
)


def _plain_puts(
    spark, cfg: ZarrBenchConfig, access, n: int, ny: int, nx: int, size: int, tag: str
):
    """The same-node baseline of the direct leg: ``n`` tasks, one per partition as the cube's
    blocks are, each computing its block of the field (as the GeoBrix input does), staging
    ``size`` bytes of it on local disk, and putting them with ONE plain ``obstore.put``
    (obstore's defaults: multipart for a file this size) to the scratch Volume, with the
    store and credentials direct access vends. Returns ``(wall_s, records)``; a record is one
    task: host, process, partition, attempt, wall-clock start and end, and the seconds of
    the generate, stage and put steps."""
    from databricks.labs.gbx.zarr._access import bounded_closure

    dest = os.path.join(cfg.scratch, "w1d", tag)

    def build(a):
        def task(batches):
            import socket

            import obstore
            import pyarrow as pa
            from pyspark import TaskContext

            from databricks.labs.gbx.zarr._access import _make_store

            ctx = TaskContext.get()
            out = []
            for b in batches:
                for i in b.column("id").to_pylist():
                    start = time.time()
                    data = field(i * 24, (i + 1) * 24, ny, nx).tobytes()[:size]
                    gen = time.time()
                    fd, local = tempfile.mkstemp(suffix=".bin")
                    with os.fdopen(fd, "wb") as fh:
                        fh.write(data)
                    staged = time.time()
                    with open(local, "rb") as fh:
                        obstore.put(
                            _make_store(a),
                            a.key_of(os.path.join(dest, f"part-{i}.bin")),
                            fh,
                        )
                    end = time.time()
                    os.remove(local)
                    out.append(
                        json.dumps(
                            {
                                "host": socket.gethostname(),
                                "pid": os.getpid(),
                                "partition": ctx.partitionId() if ctx else None,
                                "attempt": ctx.attemptNumber() if ctx else None,
                                "start": start,
                                "end": end,
                                "gen_s": round(gen - start, 3),
                                "stage_s": round(staged - gen, 3),
                                "put_s": round(end - staged, 3),
                                "bytes": len(data),
                            }
                        )
                    )
            yield pa.RecordBatch.from_pylist(
                [{"rec": r} for r in out], schema=pa.schema([("rec", pa.string())])
            )

        return task

    task, _ = bounded_closure(spark, access, build)
    t = time.perf_counter()
    rows = _one_per_task(spark, n).mapInArrow(task, "rec STRING").collect()
    wall = round(time.perf_counter() - t, 3)
    shutil.rmtree(dest, ignore_errors=True)
    return wall, [json.loads(r["rec"]) for r in rows]


def _spread_txt(values) -> str:
    if not values:
        return "[]"
    med, lo, hi = _spread(values)
    return f"median {med:.2f} (min {lo:.2f}, max {hi:.2f})"


def run_w1d(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    """W1D: the DBR 19 direct leg of W1 (traced: every pack-stage task's host, times and puts)
    in alternation with ``_plain_puts`` on the same nodes, ``cfg.reps`` runs each (the first
    cold), so the two spreads can be told apart. Also times an owner's claim of a root that
    already has a ledger (the ledger-state check asks Delta)."""
    from databricks.labs.gbx.zarr import _register, _write, gbx_zarr_write
    from databricks.labs.gbx.zarr._access import resolve_access

    case = zarr_case("W1")
    nt, ny, nx = cfg.w1_shape
    block = int(case.args["chunks"]["time"])
    nbytes = nt * ny * nx * 4
    chunks, shards = dict(case.args["chunks"]), dict(case.args["shards"])
    if (ny, nx) != (721, 1440):
        chunks = {"time": block, "latitude": -(-ny // 4), "longitude": -(-nx // 4)}
        shards = {"time": block, "latitude": 4 * chunks["latitude"]}
        shards["longitude"] = 4 * chunks["longitude"]
    n = -(-nt // block)
    leg = {"walls": [], "phases": [], "at": [], "check": "", "traces": []}
    puts = {"walls": [], "records": []}
    reclaims: list = []
    made, err = 0, None
    access = None
    real_trace = _write._TRACE
    _write._TRACE = True
    try:
        for rep in range(cfg.reps):
            for which in _leg_order(("gbx", "puts"), rep):
                tag = f"{cfg.run_id}_w1d_{which}_{rep}_{uuid.uuid4().hex[:6]}"
                if which == "puts":
                    if access is None:
                        os.makedirs(os.path.join(cfg.scratch, "w1d"), exist_ok=True)
                        access = resolve_access(
                            spark,
                            os.path.join(cfg.scratch, "w1d"),
                            access="direct",
                            operation="WRITE_VOLUME",
                        )
                    wall, recs = _plain_puts(
                        spark, cfg, access, n, ny, nx, _W1D_PACK_BYTES, tag
                    )
                    puts["walls"].append(wall)
                    puts["records"].append(recs)
                    made += 1
                    continue
                table = f"{cfg.schema}.t22c_{tag}".replace("-", "_")
                root = os.path.join(cfg.scratch, "w1d", tag)
                ph = _w1_phases()
                try:
                    data = _cube_blocks(spark, nt, ny, nx, block)
                    t = time.perf_counter()
                    gbx_zarr_write(
                        data,
                        table,
                        mode="create",
                        root=root,
                        chunks=chunks,
                        shards=shards,
                        access="direct",
                        spark=spark,
                    )
                    leg["walls"].append(round(time.perf_counter() - t, 3))
                    made += 1
                    leg["at"].append(made)
                    leg["phases"].append({k: round(v, 2) for k, v in ph.t.items()})
                    leg["traces"].append(ph.traces)
                    if rep == 0:
                        leg["check"] = _w1_check(spark, table, ny, nx)
                    owner = _register.qualified_table_name(spark, table)
                    t = time.perf_counter()
                    _register.claim_root(
                        spark, root, owner, "managed"
                    )  # its own: no claim
                    reclaims.append(round(time.perf_counter() - t, 3))
                finally:
                    ph.restore()
                    _drop(spark, table, root)
    except Exception as e:  # noqa: BLE001
        err = e
    finally:
        _write._TRACE = real_trace
    out = []
    if err is not None or not leg["walls"]:
        out.append(
            _error_row(cfg, "gbx_zarr_write[W1D,direct]", err or ValueError("no run"))
        )
        return out
    row = _w1_row(cfg, "gbx_zarr_write[W1D,direct]", nt, nbytes, "dbr19_direct", leg)
    gbx_puts = [p["s"] for run in leg["traces"][1:] for t in run for p in t["packs"]]
    gbx_task = [
        round(t["end"] - t["start"], 3) for run in leg["traces"][1:] for t in run
    ]
    out.append(
        dataclasses.replace(
            row,
            note=row.note
            + f"; warm pack puts {_spread_txt(gbx_puts)} s over {len(gbx_puts)} puts; warm "
            f"task wall {_spread_txt(gbx_task)} s; owner re-claim of an existing root "
            f"{reclaims} s; TRACES per run (host, partition, start, end, packs) "
            f"{json.dumps(leg['traces'])}",
        )
    )
    warm = puts["walls"][1:] or puts["walls"]
    put_s = [r["put_s"] for run in puts["records"][1:] for r in run]
    task_s = [
        round(r["end"] - r["start"], 3) for run in puts["records"][1:] for r in run
    ]
    out.append(
        _row(
            cfg,
            "obstore.put[W1D,baseline]",
            warm,
            n,
            f"plain obstore.put of {_W1D_PACK_BYTES} bytes a task, {n} tasks one per partition, "
            f"field computed in-task, direct-access store and credentials; stage wall per run "
            f"{puts['walls']} (run 1 cold), warm {_spread_txt(warm)} s; warm puts "
            f"{_spread_txt(put_s)} s over {len(put_s)}; warm task wall {_spread_txt(task_s)} s; "
            f"RECORDS per run {json.dumps(puts['records'])}",
            note_cap=1 << 18,
            cold=puts["walls"][0] if len(puts["walls"]) > 1 else None,
        )
    )
    return out


def _w1_check(spark, table, ny, nx, t=7) -> str:
    """One cell of the written cube at each step of ``t`` (an int or a list) against the
    generator (run 1 only)."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    steps = sorted([t] if isinstance(t, int) else list(t))
    y, x = ny // 3, nx // 5
    got = sorted(  # a table's rows come in no set order: compare them step by step
        gbx_zarr_read(
            spark,
            table,
            variables="t2m",
            isel={"time": steps, "latitude": y, "longitude": x},
            output="table",
        ).collect(),
        key=lambda r: r["time"],
    )
    want = [float(field(s, s + 1, ny, nx)[0, y, x]) for s in steps]
    ok = len(got) == len(steps) and all(
        np.isclose(r["t2m"], w) for r, w in zip(got, want)
    )
    where = steps[0] if len(steps) == 1 else steps
    return f"verify cell ({where},{y},{x}): {'OK' if ok else 'DIFF'}"


def _plain_w1(spark, path, nt, ny, nx, block, chunks, shards) -> float:
    """Plain ``xarray.to_zarr``: the driver writes the coordinates and an empty sharded
    ``t2m``; each task writes its steps with ``to_zarr(region=...)``. Timed: the tasks.
    """
    import pandas as pd
    import xarray as xr
    import zarr

    t, la, lo = _labels(nt, ny, nx)
    xr.Dataset(coords={"time": ("time", t), "latitude": la, "longitude": lo}).to_zarr(
        path, mode="w", zarr_format=3, consolidated=False
    )
    zarr.open_group(path, mode="a").create_array(
        "t2m",
        shape=(nt, ny, nx),
        chunks=tuple(chunks[d] for d in _DIMS),
        shards=tuple(shards[d] for d in _DIMS),
        dtype="float32",
        fill_value=np.nan,
        dimension_names=_DIMS,
    )
    n = -(-nt // block)

    def task(it):
        for pdf in it:
            for i in pdf["id"]:
                lo_, hi = int(i) * block, min((int(i) + 1) * block, nt)
                xr.Dataset({"t2m": (_DIMS, field(lo_, hi, ny, nx))}).to_zarr(
                    path,
                    region={
                        "time": slice(lo_, hi),
                        "latitude": slice(0, ny),
                        "longitude": slice(0, nx),
                    },
                    mode="r+",
                    consolidated=False,
                )
            yield pd.DataFrame({"id": pdf["id"]})

    t0 = time.perf_counter()
    _one_per_task(spark, n).mapInPandas(task, "id long").collect()
    return round(time.perf_counter() - t0, 3)


# --- W2: eight appenders ----------------------------------------------------------------------


def run_w2(spark, cfg: ZarrBenchConfig) -> List[ResultRow]:
    from databricks.labs.gbx.zarr import _manifest, gbx_zarr_write

    case = zarr_case("W2")
    writers = int(case.args["writers"])
    _, ny, nx = cfg.w1_shape
    day = 24
    out = []
    attempts: dict = {}
    lock = threading.Lock()
    real_commit = _manifest.commit

    def counting_commit(*a, **k):
        with lock:
            attempts[k.get("write_id")] = attempts.get(k.get("write_id"), 0) + 1
        return real_commit(*a, **k)

    def append(table, d):
        gbx_zarr_write(
            _cube_blocks(spark, day, ny, nx, day, t_start=d * day, coords=("time",)),
            table,
            mode="append",
            append_dim="time",
            spark=spark,
        )

    _manifest.commit = counting_commit
    try:
        for how in ("concurrent", "serial"):
            fn = f"{case.name}[W2,{how}]"
            walls, tries, check = [], [], ""
            try:
                for rep in range(cfg.reps):
                    tag = f"{cfg.run_id}_{how}_{rep}_{uuid.uuid4().hex[:6]}"
                    table = f"{cfg.schema}.t22_w2_{tag}".replace("-", "_")
                    root = os.path.join(cfg.scratch, "w2", tag)
                    try:
                        gbx_zarr_write(
                            _cube_blocks(spark, day, ny, nx, day),
                            table,
                            mode="create",
                            root=root,
                            chunks=(
                                {"time": day, "latitude": 181, "longitude": 360}
                                if (ny, nx) == (721, 1440)
                                else {"time": day}
                            ),
                            spark=spark,
                        )
                        attempts.clear()
                        t = time.perf_counter()
                        if how == "serial":
                            for d in range(1, writers + 1):
                                append(table, d)
                        else:
                            with ThreadPoolExecutor(writers) as pool:
                                for f in [
                                    pool.submit(append, table, d)
                                    for d in range(1, writers + 1)
                                ]:
                                    f.result()
                        walls.append(round(time.perf_counter() - t, 3))
                        tries.append(sorted(attempts.values()))
                        if rep == 0:  # the last step of every appended day
                            last = [(d + 1) * day - 1 for d in range(1, writers + 1)]
                            check = _w1_check(spark, table, ny, nx, t=last)
                    finally:
                        _drop(spark, table, root)
                out.append(
                    _row(
                        cfg,
                        fn,
                        walls,
                        writers,
                        f"{writers} appends of {day} steps ({day * ny * nx * 4 / 1e6:.0f} "
                        f"MB each) to a one-day store; commit attempts per run {tries}; "
                        f"{check}",
                    )
                )
            except Exception as e:  # noqa: BLE001
                out.append(_error_row(cfg, fn, e))
    finally:
        _manifest.commit = real_commit
    return out


# --- probes: shard index size, sample density ------------------------------------------------


def shard_index_probe(directory: str, counts=(256, 1024, 4096, 16384, 93312), reps=3):
    """Read one inner chunk of a shard of ``n`` tiny inner chunks (float32 (12, 5, 6),
    1.4 kB decoded), for each ``n`` in ``counts``: through zarr-python from the shard
    object (it fetches the index, 16 bytes a chunk), and through GeoBrix from chunk rows
    (it builds a virtual index of the same size). Returns one dict per ``n``."""
    import zarr

    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box
    from databricks.labs.gbx.zarr._shard import decode_index, index_nbytes
    from databricks.labs.gbx.zarr._store import ManifestStore
    from databricks.labs.gbx.zarr._testing import plain_store_rows

    out = []
    for n in counts:
        path = os.path.join(directory, f"shard_{n}_{uuid.uuid4().hex[:6]}.zarr")
        try:
            shape = (12 * n, 5, 6)
            arr = zarr.open_group(path, mode="w").create_array(
                "v",
                shape=shape,
                chunks=(12, 5, 6),
                shards=shape,
                dtype="float32",
                dimension_names=_DIMS,
            )
            arr[:] = np.arange(np.prod(shape), dtype="float32").reshape(shape)
            plain = _walls(
                lambda: np.asarray(zarr.open_array(f"{path}/v", mode="r")[0:12]), reps
            )
            metas, _, arrays = plain_store_rows(path)
            name = "v"
            shard_file = os.path.join(path, name, "c", "0", "0", "0")
            codecs = json.loads(metas[f"{name}/zarr.json"])["codecs"][0][
                "configuration"
            ]["index_codecs"]
            size = os.path.getsize(shard_file)
            ib = index_nbytes(n, codecs)
            with open(shard_file, "rb") as fh:
                fh.seek(size - ib)
                entries = decode_index(fh.read(ib), n, codecs)
            rel = os.path.relpath(shard_file, path)
            chunk_rows = [
                {
                    "kind": "chunk",
                    "array": name,
                    "key": f"c/{i}/0/0",
                    "shard_key": "c/0/0/0",
                    "file": rel,
                    "offset": int(off),
                    "length": int(ln),
                    "origin": "gbx",
                    "identity": None,
                    "write_id": "0" * 32,
                }
                for i, (off, ln) in enumerate(entries)
            ]

            def virtual():
                store = ManifestStore(metas, chunk_rows, FuseRangeReader(path), arrays)
                try:
                    a = open_manifest_array(store, name)
                    return read_box(a, (slice(0, 12),))
                finally:
                    store.close()

            got = virtual()
            assert np.array_equal(
                got, np.arange(360, dtype="float32").reshape(12, 5, 6)
            )
            built = _walls(virtual, reps)
            out.append(
                {
                    "inner_chunks": n,
                    "index_bytes": ib,
                    "shard_bytes": size,
                    "plain_read_s": plain,
                    "virtual_read_s": built,
                }
            )
        finally:
            shutil.rmtree(path, ignore_errors=True)
    return out


def sample_density_probe(
    store, variable="t2m", counts=(10, 100, 1000, 2000, 3000, 4050, 8100), reps=3
):
    """``gbx_zarr_sample``'s own read of one spatial chunk of ``store`` with every time step,
    for each number of points in the chunk: the shipped ``_sample_points`` path (the
    driver's plan, a task's unit read, its batches), with the dense switch as shipped, with
    it turned off (every unit read point by point) and with it always on (every unit whose
    box fits the budget read as one box), on the zarr engine (Spark tasks) and tensorstore
    (the driver route)."""
    import shapely

    from databricks.labs.gbx.zarr import _read, _sample

    src = _read._open_source(None, store, "path", None, None)
    names = _read._choose_variables(variable, src)
    cfg = _sample._plan_sample(src, names, None, None, None)
    _, lat, lon = _coords(store)
    cy, cx = _chunk_shape(store, variable)[1:]
    y0, x0 = (len(lat) // 3 // cy) * cy, (len(lon) // 2 // cx) * cx
    hy, hx = min(cy, len(lat) - y0), min(cx, len(lon) - x0)
    rng = np.random.default_rng(7)
    lon180 = np.where(lon >= 180.0, lon - 360.0, lon)
    out = []
    for n in counts:
        n = min(n, hy * hx)
        flat = rng.choice(hy * hx, size=n, replace=False)
        ys, xs = y0 + flat // hx, x0 + flat % hx
        points = [
            shapely.to_wkb(shapely.Point(float(lon180[x]), float(lat[y])))
            for y, x in zip(ys, xs)
        ]
        uid, row, col = cfg.grid.map(points, None)
        units = _sample._units(src, cfg, np.unique(uid[uid >= 0]))

        def run(engine):
            for _ in _sample._sample_points(cfg, units, uid, row, col, engine):
                pass

        for engine in ("zarr", "auto"):
            real = _sample._DENSE
            switch = {"shipped": real, "points_only": float("inf"), "box_only": 0.0}
            took, order = {}, _rotated(switch, len(out))
            try:
                for variant in order:  # no variant always first
                    _sample._DENSE = switch[variant]
                    took[variant] = _walls(lambda: run(engine), reps)
            finally:
                _sample._DENSE = real
            shipped, off, box = took["shipped"], took["points_only"], took["box_only"]
            out.append(
                {
                    "engine": "zarr" if engine == "zarr" else "tensorstore",
                    "points": int(n),
                    "density": round(n / (hy * hx), 3),
                    "units": int(len(units)),
                    "shipped_s": shipped,
                    "points_only_s": off,
                    "box_only_s": box,
                    "order": list(order),
                    "batch_cells": int(cfg.batch_cells),
                    "box_bytes": int(cfg.box_bytes),
                }
            )
    return out


def update_window_probe(spark, cfg: ZarrBenchConfig, nt: int = 96, reps: int = 3):
    """What the narrowing window of a one-pass update costs: a store of ``nt`` steps of the
    W1 cube (one shard per 24 steps), updated whole from blocks (a one-pass write, every
    shard touched), its files phase timed with the window (``_touched_groups``, as shipped)
    and without it, the two alternated ``reps`` times. Each group the window buffers is a
    shard: its base rows and its encoded chunks."""
    from databricks.labs.gbx.zarr import _write, gbx_zarr_write

    case = zarr_case("W1")
    _, ny, nx = cfg.w1_shape
    chunks, shards = dict(case.args["chunks"]), dict(case.args["shards"])
    if (ny, nx) != (721, 1440):  # a smaller cube (local runs): one shard per block
        chunks = {"time": 24, "latitude": -(-ny // 4), "longitude": -(-nx // 4)}
        shards = {"time": 24, "latitude": 4 * chunks["latitude"]}
        shards["longitude"] = 4 * chunks["longitude"]
    tag = f"{cfg.run_id}_upd_{uuid.uuid4().hex[:6]}"
    table = f"{cfg.schema}.t22b_{tag}".replace("-", "_")
    root = os.path.join(cfg.scratch, "upd", tag)
    times: dict = {"window": [], "no window": []}
    real = _write._touched_groups
    try:
        gbx_zarr_write(
            _cube_blocks(spark, nt, ny, nx, 24),
            table,
            mode="create",
            root=root,
            chunks=chunks,
            shards=shards,
            spark=spark,
        )
        for i in range(reps):
            for variant in _rotated(("window", "no window"), i):
                ph = _Phases()
                ph.wrap(_write, "_write_files", "files")
                if variant == "no window":
                    _write._touched_groups = lambda grouped: grouped
                try:
                    gbx_zarr_write(
                        _cube_blocks(spark, nt, ny, nx, 24, coords=()),
                        table,
                        mode="update",
                        spark=spark,
                    )
                finally:
                    _write._touched_groups = real
                    ph.restore()
                times[variant].append(round(ph.t["files"], 2))
    finally:
        _write._touched_groups = real
        _drop(spark, table, root)
    groups = -(-nt // 24)
    return [
        {
            "variant": v,
            "files_s": t,
            "median_s": statistics.median(t),
            "groups": groups,
            "group_mb": round(24 * ny * nx * 4 / _MB, 1),
        }
        for v, t in times.items()
    ]


def _chunk_shape(store, variable):
    with open(os.path.join(store, variable, "zarr.json")) as fh:
        doc = json.load(fh)
    codecs = doc.get("codecs", [])
    if codecs and codecs[0]["name"] == "sharding_indexed":
        return tuple(codecs[0]["configuration"]["chunk_shape"])
    return tuple(doc["chunk_grid"]["configuration"]["chunk_shape"])


def _probe_row(cfg, fn, payload, wall) -> ResultRow:
    # the whole payload: a probe's figures are its result (a 2,000-character cap cut one)
    return _row(
        cfg, fn, [round(wall, 3)], len(payload), json.dumps(payload), note_cap=1 << 20
    )


# --- all of it ----------------------------------------------------------------------------------


def run_zarr_cases(spark, cfg: ZarrBenchConfig, sink=None) -> List[ResultRow]:
    """Run the selected cases, then the two probes; ``sink(rows)`` gets each case's rows as
    it finishes (the notebook streams them to the results table)."""
    legs = {
        "R1": run_r1,
        "R2": run_r2,
        "R3": run_r3,
        "W1": run_w1,
        "W2": run_w2,
        "W1D": run_w1d,  # the direct leg beside plain object-store puts (investigation)
    }
    rows: List[ResultRow] = []
    for case in cfg.cases:
        print(f"ZARR {case}: starting", flush=True)
        t = time.perf_counter()
        got = legs[case](spark, cfg)
        print(
            f"ZARR {case}: {time.perf_counter() - t:.0f} s; "
            + "; ".join(
                f"{r.fn} {r.status} median {r.iter_median_s:.2f} s" for r in got
            ),
            flush=True,
        )
        rows += got
        if sink is not None:
            sink(got)
    for fn, probe in (
        (
            "probe_shard_index",
            lambda: shard_index_probe(os.path.join(cfg.scratch, "b")),
        ),
        ("probe_sample_density", lambda: sample_density_probe(cfg.store, cfg.variable)),
        ("probe_update_window", lambda: update_window_probe(spark, cfg)),
    ):
        try:
            os.makedirs(os.path.join(cfg.scratch, "b"), exist_ok=True)
            t = time.perf_counter()
            payload = probe()
            got = [_probe_row(cfg, fn, payload, time.perf_counter() - t)]
        except Exception as e:  # noqa: BLE001
            got = [_error_row(cfg, fn, e)]
        print(f"ZARR {fn}: {got[0].status} {got[0].note[:1500]}", flush=True)
        rows += got
        if sink is not None:
            sink(got)
    return rows


def local_scratch() -> str:
    """A temporary directory for a local run of the legs."""
    return tempfile.mkdtemp(prefix="gbx_zarr_bench_")
