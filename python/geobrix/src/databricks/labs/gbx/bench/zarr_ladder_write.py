"""Write legs of the Zarr size ladder (``ZL-W1`` .. ``ZL-W5``): a table of cells to a Zarr v3 store.

The AccuWeather path, per store of the ladder (``bench.zarr_ladder``):

1. read the store as a table of cells, ``gbx_zarr_read(..., output="table")`` (``t2m``, ``u10``, ``v10``);
2. add ``wind_speed = sqrt(u10^2 + v10^2)`` in Spark;
3. hand the table of cells straight to ``gbx_zarr_write`` (``dim_cols``): ``ZL-W1`` into a manifest table, ``ZL-W2`` the
   plain ``transactional=False`` store, then ``ZL-W4`` / ``ZL-W5`` append one more day to each. ``ZL-W3`` is the
   baseline (the driver makes the store, each task writes the shards of staged blocks with zarr-python, which is what a
   hand-written Spark job does);
4. the same four writes by the path that has no table input, as comparison cases (suffix ``s``): **pack** the cells into
   blocks on the store's shard grid and **stage** them in a Delta table (the writer plans a create from the block
   positions and then writes, so a block input that is a query would be evaluated twice), then write the staged blocks
   (``ZL-W1s``, ``ZL-W2s``, ``ZL-W4s``, ``ZL-W5s``);
5. verify each output by a read-back: the cell count and the sums of the four variables equal the table's.

Per write the leg records the wall, the steps the writer exposes (the survey of the dimension columns of a table of cells,
position pass, ownership claim, manifest table, data files, commit; for a plain store the survey, position pass, data
files, coordinates and documents), the tasks that made
data blocks, MB/s and the share of the time that was NOT spent writing data files. That share is reported for the
write call alone and for the whole table-to-Zarr path (pack and stage included, for the ``s`` cases); ``path s`` is the
wall plus the pack and stage time.

The staged blocks cover whole shards (the shard grid of the source store), so every staged create takes the writer's
no-shuffle path; a create from cells makes the same blocks itself, in one shuffle. Nothing here calls ``spark.conf.set``, ``sparkContext``, ``.rdd`` or ``_jvm``.
"""

from __future__ import annotations

import json
import os
import shutil
import statistics
import time
import uuid
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from databricks.labs.gbx.bench import zarr_ladder as zl

__all__ = [
    "WRITE_CASES",
    "make_packer",
    "run_writes",
    "write_table",
    "write_verdicts",
]

# case -> (workload name, what it does)
WRITE_CASES: Dict[str, Tuple[str, str]] = {
    "ZL-W1": ("table_to_zarr_managed", "table to a manifest store (gbx_zarr_write)"),
    "ZL-W2": ("table_to_zarr_plain", "table to a plain store (transactional=False)"),
    "ZL-W3": (
        "table_to_zarr_tasks",
        "baseline: tasks write the shards with zarr-python",
    ),
    "ZL-W4": ("append_day_managed", "append one more day to the manifest store"),
    "ZL-W5": ("append_day_plain", "append one more day to the plain store"),
    # the same four writes without handing the table of cells to the writer: pack the cells into shard-grid blocks,
    # stage them in a Delta table, write the blocks (what a job does when the writer takes blocks, xarray or tiles)
    "ZL-W1s": (
        "table_to_zarr_managed_staged",
        "pack + stage, then a manifest store from the blocks",
    ),
    "ZL-W2s": (
        "table_to_zarr_plain_staged",
        "pack + stage, then a plain store from the blocks",
    ),
    "ZL-W4s": (
        "append_day_managed_staged",
        "pack + stage a day, append it to the manifest store",
    ),
    "ZL-W5s": (
        "append_day_plain_staged",
        "pack + stage a day, append it to the plain store",
    ),
}
GATE_PER_GB = 1.5
VARS_OUT = ("t2m", "u10", "v10", "wind_speed")
_DIMS = ("time", "latitude", "longitude")
_T0 = np.datetime64("2023-01-01T00:00", "s").astype("int64")  # epoch seconds of hour 0
_SUMS = ("t2m", "u10", "v10", "wind_speed")


def _close(a: float, b: float, rel: float = 1e-9) -> bool:
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


# --- the pack step ------------------------------------------------------------------------


def make_packer(names, shard, extent):
    """A ``mapInArrow`` function that turns cells into blocks on the shard grid.

    Its input is ``__k`` (the shard key ``(ts << 20) | (ys << 10) | xs``, sorted within the partition), the cell's
    absolute indices ``__ti``, ``__yi``, ``__xi`` and one float32 column per name. It fills one dense array per
    variable and shard (NaN where no cell came), and emits one block per variable, cropped to the cells that came.
    Only one shard is held at a time. ``extent`` is the store's size after the write.
    """
    import pyarrow as pa

    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    names = tuple(names)

    def run(batches):
        key = None
        arrs: dict = {}
        lo = (0, 0, 0)
        bb = None

        def flush():
            (t0, y0, x0), (t1, y1, x1) = bb
            rows = []
            for n in names:
                a = np.ascontiguousarray(
                    arrs[n][
                        t0 - lo[0] : t1 + 1 - lo[0],
                        y0 - lo[1] : y1 + 1 - lo[1],
                        x0 - lo[2] : x1 + 1 - lo[2],
                    ]
                )
                rows.append(
                    {
                        "variable": n,
                        "dims": list(_DIMS),
                        "start": [t0, y0, x0],
                        "stop": [t1 + 1, y1 + 1, x1 + 1],
                        "coord_start": None,
                        "coord_stop": None,
                        "dtype": a.dtype.str,
                        "shape": list(a.shape),
                        "data": a.tobytes(),
                        "fill_value": None,
                        "attrs": None,
                    }
                )
            return pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)

        for b in batches:
            if b.num_rows == 0:
                continue
            k = b.column("__k").to_numpy(zero_copy_only=False)
            ti = b.column("__ti").to_numpy(zero_copy_only=False)
            yi = b.column("__yi").to_numpy(zero_copy_only=False)
            xi = b.column("__xi").to_numpy(zero_copy_only=False)
            vals = {n: b.column(n).to_numpy(zero_copy_only=False) for n in names}
            cuts = np.flatnonzero(k[1:] != k[:-1]) + 1
            for s, e in zip([0, *cuts.tolist()], [*cuts.tolist(), len(k)]):
                kk = int(k[s])
                if kk != key:
                    if key is not None:
                        yield flush()
                    key = kk
                    lo = (
                        (kk >> 20) * shard[0],
                        ((kk >> 10) & 1023) * shard[1],
                        (kk & 1023) * shard[2],
                    )
                    dims = tuple(
                        min(sz, ext - l) for sz, ext, l in zip(shard, extent, lo)
                    )
                    arrs = {n: np.full(dims, np.nan, np.float32) for n in names}
                    bb = None
                t, y, x = ti[s:e], yi[s:e], xi[s:e]
                for n in names:
                    arrs[n][t - lo[0], y - lo[1], x - lo[2]] = vals[n][s:e]
                cur = (
                    (int(t.min()), int(y.min()), int(x.min())),
                    (int(t.max()), int(y.max()), int(x.max())),
                )
                bb = (
                    cur
                    if bb is None
                    else (
                        tuple(min(a, c) for a, c in zip(bb[0], cur[0])),
                        tuple(max(a, c) for a, c in zip(bb[1], cur[1])),
                    )
                )
        if key is not None:
            yield flush()

    return run


def _n_keys(g: zl.Geometry, t_lo: int, t_hi: int) -> int:
    ts, ys, xs = g.fam.shards
    return (-(-t_hi // ts) - t_lo // ts) * (-(-g.ny // ys)) * (-(-g.nx // xs))


def _coord_rows(g: zl.Geometry, t_lo: int, t_hi: int, with_space: bool):
    """Coordinate blocks of hours ``[t_lo, t_hi)`` (and, for a create, the latitudes and longitudes)."""
    import pandas as pd

    from databricks.labs.gbx.bench.zarr_bench import _coord_block

    times = pd.date_range("2023-01-01", periods=t_hi, freq="h").values[t_lo:]
    rows = [_coord_block("time", times, t_lo)]
    if with_space:
        lat, lon = g.axes()
        rows += [_coord_block("latitude", lat), _coord_block("longitude", lon)]
    return rows


def source_table(
    spark, store: str, g: zl.Geometry, *, hours: Optional[int] = None, shift: int = 0
):
    """The table of step 1 and 2: the store's cells with ``wind_speed`` added. ``hours`` keeps the first hours only
    and ``shift`` moves the times on by that many hours (the next day's table)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.zarr import gbx_zarr_read

    kw = {} if hours is None else {"isel": {"time": slice(0, hours)}}
    df = gbx_zarr_read(
        spark, store, variables=["t2m", "u10", "v10"], output="table", **kw
    )
    u, v = F.col("u10").cast("double"), F.col("v10").cast("double")
    df = df.withColumn("wind_speed", F.sqrt(u * u + v * v).cast("float"))
    if shift:
        df = df.withColumn("time", F.col("time") + F.expr(f"INTERVAL {shift} HOURS"))
    return df


def _pack_stage(
    spark, df, g: zl.Geometry, t_lo: int, t_hi: int, table: str, with_space: bool
) -> None:
    """Pack ``df`` (hours ``[t_lo, t_hi)`` of the store) into shard-grid blocks plus coordinate blocks and stage them
    in the Delta table ``table``."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.zarr._read import BLOCKS_SCHEMA

    ts, ys, xs = g.fam.shards
    lat, lon = g.axes()
    ti = ((F.col("time").cast("long") - F.lit(int(_T0))) / 3600).cast("long")
    yi = F.round((F.lit(float(lat[0])) - F.col("latitude")) / g.fam.step).cast("long")
    xi = F.round((F.col("longitude") - F.lit(float(lon[0]))) / g.fam.step).cast("long")
    cells = df.select(
        ti.alias("__ti"), yi.alias("__yi"), xi.alias("__xi"), *VARS_OUT
    ).select(
        (
            (F.floor(F.col("__ti") / ts) * 1048576)
            + (F.floor(F.col("__yi") / ys) * 1024)
            + F.floor(F.col("__xi") / xs)
        )
        .cast("long")
        .alias("__k"),
        "__ti",
        "__yi",
        "__xi",
        *VARS_OUT,
    )
    n = max(1, _n_keys(g, t_lo, t_hi))
    packer = make_packer(VARS_OUT, g.fam.shards, (t_hi, g.ny, g.nx))
    blocks = (
        cells.repartition(n, "__k")
        .sortWithinPartitions("__k")
        .mapInArrow(packer, BLOCKS_SCHEMA)
    )
    coords = spark.createDataFrame(
        _coord_rows(g, t_lo, t_hi, with_space), BLOCKS_SCHEMA
    )
    blocks.unionByName(coords).write.format("delta").mode("overwrite").saveAsTable(
        table
    )


# --- the baseline ---------------------------------------------------------------------------


def _baseline_write(spark, stage: str, path: str, g: zl.Geometry) -> int:
    """The hand-written Spark job: the driver makes the store (coordinates, empty sharded arrays) with xarray and
    zarr-python, each task writes the shards of its blocks. Returns the number of blocks written.
    """
    import pandas as pd
    import xarray as xr
    import zarr
    from pyspark.sql import functions as F

    lat, lon = g.axes()
    xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2023-01-01", periods=g.hours, freq="h").values,
            ),
            "latitude": lat,
            "longitude": lon,
        }
    ).to_zarr(path, mode="w", zarr_format=3, consolidated=False)
    grp = zarr.open_group(path, mode="a")
    for v in VARS_OUT:
        grp.create_array(
            v,
            shape=g.shape,
            chunks=g.fam.chunks,
            shards=g.fam.shards,
            dtype="float32",
            fill_value=np.nan,
            dimension_names=_DIMS,
        )

    def task(batches):
        import pyarrow as pa

        n = 0
        for b in batches:
            for r in b.to_pylist():
                a = np.frombuffer(r["data"], r["dtype"]).reshape(r["shape"])
                sl = tuple(slice(s, e) for s, e in zip(r["start"], r["stop"]))
                zarr.open_array(f"{path}/{r['variable']}", mode="r+")[sl] = a
                n += 1
        yield pa.RecordBatch.from_pylist(
            [{"n": n}], schema=pa.schema([("n", pa.int64())])
        )

    blocks = spark.read.table(stage).where(F.col("variable").isin(*VARS_OUT))
    return int(sum(r["n"] for r in blocks.mapInArrow(task, "n long").collect()))


# --- one rep ----------------------------------------------------------------------------------


def _phases(plain: bool):
    from databricks.labs.gbx.bench import zarr_bench as zb
    from databricks.labs.gbx.zarr import _write, _write_plain

    ph = zb._w1_phases()
    from databricks.labs.gbx.zarr import _write_cells

    for name, label in (
        ("_survey_distinct", "cells: label survey"),
        ("_survey_units", "cells: unit survey"),
    ):
        ph.wrap(_write_cells, name, label)
    if plain:
        for mod, name, label in (
            (_write, "_plain_coordinates", "plain: coordinates"),
            (_write_plain, "write_documents", "plain: documents"),
        ):
            ph.wrap(mod, name, label)
    return ph


def _store_coords(store: str) -> dict:
    """The labels of the source store's dimensions, as a user who knows their grid would pass them."""
    import xarray as xr

    ds = xr.open_zarr(store, chunks=None)
    return {d: ds[d].values for d in _DIMS}


def _write_call(
    spark,
    kind: str,
    source,
    target: str,
    root: Optional[str],
    g: zl.Geometry,
    mode: str,
    cells: bool = False,
    coords: Optional[dict] = None,
):
    """One ``gbx_zarr_write`` of ``kind`` (``managed`` or ``plain``) with its steps timed; returns ``(wall, steps)``.
    ``cells``: the source is the table of cells (``dim_cols``), not blocks."""
    from databricks.labs.gbx.zarr import gbx_zarr_write

    ph = _phases(kind == "plain")
    kw: dict = {}
    if mode == "create":
        kw["chunks"] = dict(zip(_DIMS, g.fam.chunks))
        kw["shards"] = dict(zip(_DIMS, g.fam.shards))
    else:
        kw["append_dim"] = "time"
    if cells:
        kw["dim_cols"] = {d: d for d in _DIMS}
        if coords is not None and mode == "create":
            # every label is known and every storage unit has cells: the writer plans without a survey scan
            kw["coords"], kw["complete"] = coords, True
    if kind == "plain":
        kw["transactional"] = False
    elif mode == "create":
        kw["root"] = root
    try:
        t = time.perf_counter()
        gbx_zarr_write(source, target, mode=mode, spark=spark, **kw)
        wall = time.perf_counter() - t
    finally:
        ph.restore()
    return wall, {k: round(v, 3) for k, v in ph.t.items()}


def _agg(spark, target: str):
    from pyspark.sql import functions as F

    from databricks.labs.gbx.zarr import gbx_zarr_read

    df = gbx_zarr_read(spark, target, output="table")
    r = df.agg(
        F.count(F.lit(1)).alias("n"),
        *[F.sum(F.col(v).cast("double")).alias(v) for v in _SUMS],
    ).first()
    return {"n": int(r["n"]), **{v: float(r[v]) for v in _SUMS}}


def _expect(spark, df) -> dict:
    from pyspark.sql import functions as F

    r = df.agg(
        F.count(F.lit(1)).alias("n"),
        *[F.sum(F.col(v).cast("double")).alias(v) for v in _SUMS],
    ).first()
    return {"n": int(r["n"]), **{v: float(r[v]) for v in _SUMS}}


def _compare(got: dict, want: dict) -> str:
    if got["n"] != want["n"]:
        return f"cells {got['n']} != {want['n']}"
    for v in _SUMS:
        if not _close(got[v], want[v], 1e-7):
            return f"sum({v}) {got[v]!r} != expected {want[v]!r}"
    return ""


def _steps(kind: str, wall: float, ph: dict) -> dict:
    """The writer's steps of one write, in seconds; ``other`` is what the timers do not cover."""
    g_ = lambda k: round(ph.get(k, 0.0), 3)  # noqa: E731
    survey = g_("cells: label survey") + g_("cells: unit survey")
    if kind == "plain":
        s = {
            "survey": round(survey, 3),
            "position": g_("position pass"),
            "files": g_("files"),
            "coordinates": g_("plain: coordinates"),
            "documents": g_("plain: documents"),
        }
    else:
        s = {
            "survey": round(survey, 3),
            "position": g_("position pass"),
            "claim": g_("setup: ownership claim"),
            "manifest": g_("setup: create table"),
            "files": g_("files"),
            "commit": g_("commit"),
        }
    if not survey:
        del s["survey"]  # the blocks paths have none
    s["other"] = round(max(0.0, wall - sum(s.values())), 3)
    return s


def run_writes(  # noqa: C901
    spark,
    cfg,
    g: zl.Geometry,
    store: str,
    emit: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    """Run the selected ``ZL-W*`` cases on one store, ``cfg.reps`` times each (the first cold). Returns one metrics
    dict per case, in the shape of ``zarr_ladder_bench._run_leg``'s plus the steps of a write.

    A case with no suffix hands the table of cells straight to ``gbx_zarr_write`` (``dim_cols``); the ``s`` cases
    (``ZL-W1s`` ...) are the path without that input: pack the cells into blocks, stage them in Delta, write the blocks.
    """
    emit = emit or (lambda s: print(s, flush=True))
    want = [c for c in cfg.cases if c in WRITE_CASES]
    if g.hours < 2 and any(c[4] in "45" for c in want):
        # a create of one time step chooses 'days since' for the time array (xarray's choice from a single midnight
        # value), and an hourly append is then refused: a one-step store has no day to append
        emit(
            f"ZARR-LADDER {g.family} {g.rung}: no append legs (a one-step store takes no hourly append)"
        )
        want = [c for c in want if c[4] not in "45"]
    if not want:
        return []
    schema = cfg.schema or "default"
    scratch = cfg.scratch or os.path.join(
        os.environ.get("TMPDIR", "/tmp"), "gbx_zl_write"
    )
    os.makedirs(scratch, exist_ok=True)
    if "." in schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    day = min(24, g.hours)
    base = source_table(spark, store, g)
    labels = _store_coords(store)
    expect = _expect(spark, base)
    expect_day = _expect(spark, source_table(spark, store, g, hours=day, shift=g.hours))
    expect_after = {k: expect[k] + expect_day[k] for k in expect}
    staged_cases = [c for c in want if c.endswith("s") or c == "ZL-W3"]
    runs: Dict[str, dict] = {
        c: {
            "walls": [],
            "steps": [],
            "tasks": [],
            "packed": [],
            "check": "",
            "extra": [],
            "pre": [],
        }
        for c in want
    }
    prep: List[dict] = []
    error: Dict[str, str] = {}
    for rep in range(cfg.reps):
        tag = f"{cfg.run_id}_{g.family}_{g.rung}_{rep}_{uuid.uuid4().hex[:6]}".replace(
            "-", "_"
        ).lower()
        stage = f"{schema}.zl_stage_{tag}"
        stage_day = f"{schema}.zl_day_{tag}"
        made_tables = [stage, stage_day]
        paths = []
        verify = rep == 0

        def note(case, text):
            emit(
                f"ZARR-LADDER {g.family} {g.rung} {case} rep {rep + 1}/{cfg.reps}: {text}"
            )

        try:
            from databricks.labs.gbx.bench.zarr_ladder_bench import _noop

            t = time.perf_counter()
            _noop(base.select("time", "latitude", "longitude", "wind_speed"))
            read_s = time.perf_counter() - t
            stage_s = 0.0
            if staged_cases:
                t = time.perf_counter()
                _pack_stage(spark, base, g, 0, g.hours, stage, True)
                stage_s = time.perf_counter() - t
                note(
                    "stage",
                    f"read+transform {read_s:.2f} s, pack+stage {stage_s:.2f} s (incl. a read)",
                )
            prep.append({"read_s": read_s, "stage_s": stage_s})

            def record(
                case, kind, wall, ph, out_path, pre=0.0, tasks_key="files: data tasks"
            ):
                r = runs[case]
                r["pre"].append(pre)
                r["walls"].append(wall)
                r["steps"].append(_steps(kind, wall, ph))
                r["tasks"].append(int(ph.get(tasks_key, 0)))
                r["packed"].append(ph.get("files: packed"))
                r["extra"].append(
                    zl.disk_bytes(out_path) if os.path.exists(out_path) else 0
                )

            for sfx in ("", "s"):
                c1, c2, c4, c5 = (f"ZL-W{i}{sfx}" for i in (1, 2, 4, 5))
                mine = {c1, c2, c4, c5} & set(want)
                if not mine:
                    continue
                direct = sfx == ""
                tbl = f"{schema}.zl_t{sfx}_{tag}"
                root = os.path.join(scratch, f"t{sfx}_{tag}")
                plain = os.path.join(scratch, f"p{sfx}_{tag}.zarr")
                made_tables.append(tbl)
                paths += [root, plain]
                src = (
                    base.select(*_DIMS, *VARS_OUT)
                    if direct
                    else spark.read.table(stage)
                )
                if {c1, c4} & mine:
                    wall, ph = _write_call(
                        spark, "managed", src, tbl, root, g, "create", direct, labels
                    )
                    if c1 in want:
                        record(c1, "managed", wall, ph, root, stage_s * (not direct))
                        if verify:
                            runs[c1]["check"] = (
                                _compare(_agg(spark, tbl), expect) or "ok"
                            )
                    note(
                        c1,
                        f"managed create {wall:.2f} s, files {ph.get('files', 0):.2f} s",
                    )
                if {c2, c5} & mine:
                    wall, ph = _write_call(
                        spark, "plain", src, plain, None, g, "create", direct, labels
                    )
                    if c2 in want:
                        record(c2, "plain", wall, ph, plain, stage_s * (not direct))
                        if verify:
                            runs[c2]["check"] = (
                                _compare(_agg(spark, plain), expect) or "ok"
                            )
                    note(
                        c2,
                        f"plain create {wall:.2f} s, files {ph.get('files', 0):.2f} s",
                    )
                if {c4, c5} & mine and g.hours >= 2:
                    dayf = source_table(spark, store, g, hours=day, shift=g.hours)
                    day_s = 0.0
                    if direct:
                        dsrc = dayf.select(*_DIMS, *VARS_OUT)
                    else:
                        t = time.perf_counter()
                        _pack_stage(
                            spark, dayf, g, g.hours, g.hours + day, stage_day, False
                        )
                        day_s = time.perf_counter() - t
                        dsrc = spark.read.table(stage_day)
                    if c4 in want:
                        wall, ph = _write_call(
                            spark, "managed", dsrc, tbl, None, g, "append", direct
                        )
                        record(c4, "managed", wall, ph, root, day_s)
                        if verify:
                            runs[c4]["check"] = (
                                _compare(_agg(spark, tbl), expect_after) or "ok"
                            )
                        note(c4, f"managed append {wall:.2f} s")
                    if c5 in want:
                        wall, ph = _write_call(
                            spark, "plain", dsrc, plain, None, g, "append", direct
                        )
                        record(c5, "plain", wall, ph, plain, day_s)
                        if verify:
                            runs[c5]["check"] = (
                                _compare(_agg(spark, plain), expect_after) or "ok"
                            )
                        note(c5, f"plain append {wall:.2f} s")
            if "ZL-W3" in want:
                basel = os.path.join(scratch, f"b_{tag}.zarr")
                paths.append(basel)
                t = time.perf_counter()
                nb = _baseline_write(spark, stage, basel, g)
                wall = time.perf_counter() - t
                runs["ZL-W3"]["walls"].append(wall)
                runs["ZL-W3"]["steps"].append({"files": round(wall, 3)})
                runs["ZL-W3"]["tasks"].append(nb)
                runs["ZL-W3"]["packed"].append(None)
                runs["ZL-W3"]["extra"].append(zl.disk_bytes(basel))
                runs["ZL-W3"]["pre"].append(stage_s)
                if verify:
                    runs["ZL-W3"]["check"] = (
                        _compare(_agg(spark, basel), expect) or "ok"
                    )
                note("ZL-W3", f"baseline {wall:.2f} s ({nb} blocks)")
        except Exception as e:  # noqa: BLE001
            for c in want:
                error.setdefault(c, f"{type(e).__name__}: {e}"[:600])
            emit(
                f"ZARR-LADDER {g.family} {g.rung}: write rep {rep + 1} failed: {type(e).__name__}: {str(e)[:300]}"
            )
            break
        finally:
            for tb in made_tables:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {tb}")
                except Exception:  # noqa: BLE001
                    pass
            for p in paths:
                shutil.rmtree(p, ignore_errors=True)
    out = []
    for case in want:
        r = runs[case]
        if case in error or not r["walls"]:
            out.append(
                _metrics(case, g, cfg, r, prep, error.get(case, "no run"), failed=True)
            )
        else:
            out.append(_metrics(case, g, cfg, r, prep, "", failed=False))
    return out


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 3) if xs else None


def _metrics(case, g, cfg, r, prep, err, failed) -> dict:
    name = WRITE_CASES[case][0]
    base = {
        "case": case,
        "workload": name,
        "family": g.family,
        "rung": g.rung,
        "shape": list(g.shape),
        "regional": g.regional,
        "store_bytes": g.logical_bytes,
        "cores": cfg.cores,
    }
    if failed:
        return {
            **base,
            "walls_s": [],
            "cold_s": 0.0,
            "wall_s": 0.0,
            "check": err or "failed",
            "s_per_gb": None,
        }
    walls = [round(w, 3) for w in r["walls"]]
    wall = statistics.median(walls)
    steps = {k: _med([s.get(k) for s in r["steps"]]) for k in r["steps"][0]}
    files = steps.get("files") or 0.0
    is_append = case[4] in "45"
    nbytes = (
        g.logical_bytes
        if not is_append
        else (g.logical_bytes // g.hours * min(24, g.hours))
    )
    pre = _med(r["pre"]) or 0.0  # pack + stage, for the paths that stage
    return {
        **base,
        "walls_s": walls,
        "cold_s": walls[0],
        "wall_s": round(wall, 3),
        "steps_s": steps,
        "tasks": int(statistics.median(r["tasks"])) if r["tasks"] else 0,
        "packed_no_shuffle": r["packed"][0],
        "read_transform_s": (
            _med([p["read_s"] for p in prep]) if not is_append else None
        ),
        "pack_stage_s": pre or None,
        "path_s": round(wall + pre, 3),
        "bytes_written": nbytes,
        "out_disk_bytes": int(statistics.median(r["extra"])) if r["extra"] else 0,
        "mb_s": round(nbytes / 1e6 / wall, 1) if wall else 0.0,
        "files_mb_s": round(nbytes / 1e6 / files, 1) if files else None,
        "overhead_write_share": round(1 - files / wall, 3) if wall and files else None,
        "overhead_total_share": (
            round(1 - files / (wall + pre), 3) if wall and files else None
        ),
        "s_per_gb": round(wall / (nbytes / 1e9), 2) if nbytes else None,
        "check": r["check"] or "ok",
    }


# --- the table and the gates --------------------------------------------------------------------


def write_table(metrics: List[dict]) -> str:
    head = (
        "| family | rung | case | wall s | cold s | s/GB | MB/s | tasks | pack+stage s | path s | steps (s) | "
        "files MB/s | overhead (write) | overhead (path) | check |\n"
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    )
    pct = lambda x: "-" if x is None else format(x, ".0%")  # noqa: E731
    lines = []
    for m in metrics:
        steps = ", ".join(f"{k} {v}" for k, v in (m.get("steps_s") or {}).items())
        lines.append(
            f"| {m['family']} | {m['rung']} | {m['case']} {m['workload']} | {m['wall_s']:.2f} | {m['cold_s']:.2f} | "
            f"{m['s_per_gb'] if m['s_per_gb'] is not None else '-'} | {m.get('mb_s', '-')} | {m.get('tasks', '-')} | "
            f"{m.get('pack_stage_s') if m.get('pack_stage_s') is not None else '-'} | {m.get('path_s', '-')} | {steps or '-'} | "
            f"{m.get('files_mb_s') if m.get('files_mb_s') is not None else '-'} | {pct(m.get('overhead_write_share'))} | "
            f"{pct(m.get('overhead_total_share'))} | {m['check']} |"
        )
    return head + "\n".join(lines)


def write_verdicts(metrics: List[dict], reference: str = "100MB") -> List[str]:
    """Per (family, workload): a failed or mismatching write is a flag; time per GB at or above the reference rung
    within ``GATE_PER_GB`` of the reference rung's."""
    out: List[str] = []
    by: Dict[Tuple[str, str], List[dict]] = {}
    for m in metrics:
        by.setdefault((m["family"], m["workload"]), []).append(m)
    for (family, workload), ms in sorted(by.items()):
        ms = sorted(ms, key=lambda m: m["store_bytes"])
        ref = next((m for m in ms if m["rung"] == reference), ms[-1])
        flags = []
        for m in ms:
            if m["check"] != "ok":
                flags.append(f"{m['rung']}: check failed: {m['check']}")
            elif (
                m["store_bytes"] >= ref["store_bytes"]
                and m["s_per_gb"]
                and ref["s_per_gb"]
                and m["s_per_gb"] > GATE_PER_GB * ref["s_per_gb"]
            ):
                flags.append(
                    f"{m['rung']}: {m['s_per_gb']} s/GB is {m['s_per_gb'] / ref['s_per_gb']:.1f}x the {ref['rung']} rung's ({ref['s_per_gb']})"
                )
        out.append(
            f"{'FLAG' if flags else 'ok  '} {family} {workload}"
            + "".join(f"\n       - {f}" for f in flags)
        )
    return out


def row_json(m: dict) -> str:
    keep = {k: m[k] for k in m if k not in ("walls_s", "cold_s")}
    return json.dumps(keep, separators=(",", ":"))
