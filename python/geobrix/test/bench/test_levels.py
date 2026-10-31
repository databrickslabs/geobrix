"""The levels leg: forms timed per level, with the attribution split beside the timing."""

import contextlib
import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
import types
import uuid

import pytest

from databricks.labs.gbx import _rs
from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import results

B = "gbx_bng_eastnorthasbng"
RST = "gbx_rst_transformcrs"
LINUX = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="reads /proc/self/clear_refs"
)


def run_s(spark, tmp_path, scales, base_rows=2000, mvp="S", **kw):
    kw = {"only_levels": ("L3",), **kw}
    return levels.run_mvp(
        spark, mvp, "t1", 1, 1, corpus=str(tmp_path), schema=None, scales=scales,
        where="venv", base_rows=base_rows, **kw,
    )  # fmt: skip


def test_jvm_forms_without_the_jar_are_na_by_design(spark_session_local, tmp_path):
    """A host with no geobrix JAR cannot time the JVM levels: a row that says so, not an error."""
    from databricks.labs.gbx.manager import capabilities

    spark = spark_session_local
    if capabilities.detect(spark, need_jvm=True).jar is not None:
        pytest.skip("this session carries the geobrix JAR")
    rows = run_s(spark, tmp_path, scales=(1,), base_rows=100, only_levels=("L1", "L2"))
    bng = [r for r in rows if r.fn == B]
    assert {r.level for r in bng} == {"L1", "L2"}
    assert {r.status for r in bng} == {"na_by_design"}
    assert all("no geobrix JAR" in r.note for r in bng)


def test_levels_leg_times_the_towers_at_l3_and_not_at_the_jvm_levels_without_a_jar(
    spark_session_local, tmp_path, monkeypatch
):
    """The towers input is a DSM of tiles under the corpus with the towers beside it. L3 runs it and
    fingerprints one row per tower; L1-steps and JVM-DF are not applicable where there is no JAR.
    """
    from databricks.labs.gbx.manager import capabilities

    spark = spark_session_local
    if capabilities.detect(spark, need_jvm=True).jar is not None:
        pytest.skip("this session carries the geobrix JAR")
    monkeypatch.setattr(lf, "_DSM_GRID", 2)
    monkeypatch.setattr(lf, "_DSM_TILE_PX", 64)
    monkeypatch.setattr(lf, "_TOWER_RADIUS", 20.0)
    monkeypatch.setitem(
        lf.FORMS, "H", [f for f in lf.FORMS["H"] if f.fn == "rst_viewshed_towers"]
    )
    rows = run_s(
        spark, tmp_path, scales=(1,), base_rows=6, mvp="H",
        only_levels=("L3", "L1-steps", "JVM-DF"),
    )  # fmt: skip
    by = {r.level: r for r in rows}
    assert set(by) == {"L3", "L1-steps", "JVM-DF"}
    assert (
        by["L3"].status == "ok" and by["L3"].rows == 6 and by["L3"].output_fingerprint
    )
    for level in ("L1-steps", "JVM-DF"):
        assert by[level].status == "na_by_design", by[level]
        assert "no geobrix JAR" in by[level].note


def test_a_heavy_towers_form_is_an_error_row_when_its_level_would_fall_back_to_light(
    spark_session_local, tmp_path, monkeypatch
):
    """A session that has the JAR but cannot load GDAL runs rst_viewshed_towers at L3 whatever level is asked for:
    the heavy forms must be error rows saying so, never an ok row timed under a heavy name.
    """
    import weakref

    from databricks.labs.gbx.manager import capabilities
    from databricks.labs.gbx.manager import levels as manager_levels

    jar = capabilities.JarIdentity("0.6.0", "0" * 64)
    no_gdal = capabilities.Capabilities(
        False, False, jar, "", "GDAL natives not loadable (test)", True
    )
    monkeypatch.setattr(capabilities, "detect", lambda spark, need_jvm: no_gdal)
    # as a session that has set nothing up (an earlier test may have set up a heavy level on this one)
    monkeypatch.setattr(manager_levels, "_READY", weakref.WeakKeyDictionary())
    monkeypatch.setattr(lf, "_DSM_GRID", 2)
    monkeypatch.setattr(lf, "_DSM_TILE_PX", 64)
    monkeypatch.setattr(lf, "_TOWER_RADIUS", 20.0)
    monkeypatch.setitem(
        lf.FORMS, "H", [f for f in lf.FORMS["H"] if f.fn == "rst_viewshed_towers"]
    )
    rows = run_s(
        spark_session_local, tmp_path, scales=(1,), base_rows=6, mvp="H",
        only_levels=("L1-steps", "JVM-DF"),
    )  # fmt: skip
    assert {r.level: r.status for r in rows} == {"L1-steps": "error", "JVM-DF": "error"}
    for r in rows:
        assert f"cannot run rst_viewshed_towers at {r.level}" in r.failure_mode


def test_the_dsm_tiles_are_copied_into_place_and_a_half_written_one_is_rewritten(
    tmp_path, monkeypatch
):
    """A UC Volume does not reliably rename (.claude/rules/uc-volumes.md): a tile is written locally and copied to
    its path, never renamed there, and a tile that an interrupted run left half-written is not reused.
    """
    import os

    import rasterio

    from databricks.labs.gbx.bench import oracles

    def no_rename(*args, **kwargs):
        raise OSError("this file system has no rename")

    monkeypatch.setattr(os, "replace", no_rename)
    monkeypatch.setattr(os, "rename", no_rename)
    rows = oracles.dsm_tile_rows(str(tmp_path), grid=2, size=40)
    assert sorted(os.listdir(tmp_path)) == [
        f"dsm_{x}_{y}.tif" for x in (0, 1) for y in (0, 1)
    ]
    path = rows[0][2]
    with open(path, "rb") as fh:
        whole = fh.read()
    with open(path, "wb") as fh:
        fh.write(whole[: len(whole) // 2])
    oracles.dsm_tile_rows(str(tmp_path), grid=2, size=40)
    with rasterio.open(path) as ds:
        assert ds.read().shape == (1, 40, 40)
    assert sorted(os.listdir(tmp_path)) == [
        f"dsm_{x}_{y}.tif" for x in (0, 1) for y in (0, 1)
    ]


def test_levels_leg_runs_mvp_s_light_forms(spark_session_local, tmp_path):
    spark = spark_session_local
    # 20k rows and up: Spark's sent-bytes counter stops growing past one 10k-row Arrow batch
    rows = run_s(spark, tmp_path, scales=(1, 4), base_rows=20000)
    assert rows and {r.level for r in rows} == {"L3"}
    assert all(r.status in ("ok", "na_by_design") for r in rows)
    ok = [r for r in rows if r.status == "ok"]
    assert all(r.iter_median_s > 0 and r.rows > 0 and r.output_fingerprint for r in ok)
    c0 = [r for r in ok if r.fn == B and r.form == "c0"]
    # at each scale c0, c1 and d compute the same cells (`floor` is a constant, not a variant);
    # `d` is a row that says "Rust kernels unavailable" when this host has no build
    no_rust = _rs.unavailable_reason() is not None
    cells = {}
    for r in ok:
        if r.fn == B and r.form != "floor":
            cells.setdefault(r.rows, {})[r.form] = r.output_fingerprint
    assert len(cells) == 2 and all(
        set(forms) == ({"c0", "c1"} if no_rust else {"c0", "c1", "d"})
        and len(set(forms.values())) == 1
        for forms in cells.values()
    )
    # No tile corpus here: the raster forms are skipped with a reason, not dropped.
    assert {r.fn for r in rows if r.status == "na_by_design"} == (
        {RST, B} if no_rust else {RST}
    )
    for r in ok:
        assert "attr=" in r.note and r.compute == "dedicated" and r.native_s is None
        if "attr=rest" in r.note:  # classic reads the Spark REST API
            assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
    # Spark counts no more bytes sent for 4x the DOUBLEs: NULL, and says so. WKT scales.
    for r in c0:
        assert r.ser_sent_bytes is None and "Spark under-counts numeric" in r.note
    assert all(
        r.ser_sent_bytes > 0
        for r in ok
        if r.fn == "gbx_st_makevalid" and "attr=rest" in r.note
    )
    # The floor form is the same Arrow shell with a constant body: its fingerprint is the
    # constant's, and its median sits beside each variant's in the table.
    h = spark.sql("SELECT pmod(xxhash64('TQ3080'), 2147483647)").first()[0]
    floor = {r.rows: r for r in ok if r.form == "floor"}
    assert len(floor) == 2 and all(
        r.output_fingerprint == str(n * h) for n, r in floor.items()
    )
    table = levels.summarize(rows)
    assert "input unavailable" in table  # a skipped form says why
    for r in c0:
        line = next(
            x for x in table.splitlines() if f"| c0 | dedicated | {r.rows} |" in x
        )
        assert f"| {floor[r.rows].iter_median_s:.3f} |" in line
    # a cached input is named by its recipe version
    assert spark.catalog.tableExists(f"mvp_in_bng_points_v{lf.RECIPE_VERSION}_20000")


def test_levels_leg_times_every_light_shape_of_the_generators(
    spark_session_local, tmp_path
):
    """The shapes of one generator are the same rows asked for in different ways: one fingerprint each."""
    from databricks.labs.gbx.bench import oracles

    (tmp_path / "rows").mkdir()
    for i in range(2):  # 300 x 200 px: six 128-px windows each
        (tmp_path / "rows" / f"r{i}.tif").write_bytes(
            oracles._tile_bytes("EPSG:4326", 300, 200)
        )
    rows = run_s(spark_session_local, tmp_path, scales=(1,), base_rows=20, mvp="G")
    bad = [r for r in rows if r.status == "error"]
    assert bad == [], [(r.fn, r.form, r.failure_mode) for r in bad]
    ok = [r for r in rows if r.status == "ok"]
    retile = {r.form: r.output_fingerprint for r in ok if r.fn == "gbx_rst_retile"}
    assert set(retile) == {
        "udtf",
        "udtf-arrow",
        "array_inline",
        "count_part",
        "session-tvf",
    }
    # the tessellate generator and the array function give the same cells, so their shapes share one answer
    cells = {
        (r.fn, r.form): r.output_fingerprint
        for r in ok
        if r.fn in ("gbx_bng_tessellateexplode", "gbx_bng_tessellate")
    }
    assert len(cells) == 3 and len(set(cells.values())) == 1, cells
    assert len(set(retile.values())) == 1, retile
    # the big-tile shapes have no tiles here, and say so
    big = [r for r in rows if r.form.endswith("-big")]
    assert big and {r.status for r in big} == {"na_by_design"}


def test_levels_leg_times_every_light_shape_of_the_aggregates(
    spark_session_local, tmp_path, monkeypatch
):
    """The light shapes of one aggregate ask for the same groups in different ways: one fingerprint each (the
    tree step re-encodes its canvases, so it agrees on the groups only), and each is a form of the Python
    worker, which is how the runner comes to record its worker memory."""
    from databricks.labs.gbx.bench import oracles

    (tmp_path / "rows").mkdir()
    for i in range(2):
        (tmp_path / "rows" / f"r{i}.tif").write_bytes(
            oracles._tile_bytes("EPSG:4326", 32, 32)
        )
    small = (
        530000.0,
        180000.0,
        532000.0,
        182000.0,
        2,
        2,
    )  # 2 x 2 pixels: 200 groups stay cheap
    monkeypatch.setitem(
        lf.FORMS,
        "A",
        lf._a_light(
            lf._MA,
            lf._ma_shapes(),
            (("tiles_grouped", ""), ("tiles_grouped_skew", "-skew")),
        )
        + lf._a_light(
            lf._BR,
            lf._br_shapes(small),
            (("bng_cells", ""), ("bng_cells_skew", "-skew")),
        ),
    )
    rows = run_s(spark_session_local, tmp_path, scales=(1,), base_rows=400, mvp="A")
    bad = [r for r in rows if r.status != "ok"]
    assert bad == [], [(r.fn, r.form, r.status, r.failure_mode, r.note) for r in bad]
    assert len(rows) == 14 and {r.level for r in rows} == {"L3"}
    for fn in (lf._MA, lf._BR):
        for sfx in ("", "-skew"):
            mine = {
                r.form: r
                for r in rows
                if r.fn == fn and r.form.endswith("-skew") == bool(sfx)
            }
            prints = {
                f: r.output_fingerprint
                for f, r in mine.items()
                if not f.startswith("tree")
            }
            assert len(set(prints.values())) == 1, (fn, sfx, prints)
            assert len({r.rows for r in mine.values()}) == 1


def test_the_merge_input_groups_hold_tiles_of_one_crs_sized_from_the_rows_there_are(
    spark_session_local, tmp_path
):
    """A group of tiles in several CRSs is a mosaic of several places (terabytes, for the corpus' tiles), the
    count a group is sized from is the rows the input holds, not the rows a scale asks for, and the skew input
    is a skew at its base size: its hot key is at least 10 times the typical other group.
    """
    import statistics

    from rasterio.io import MemoryFile

    from databricks.labs.gbx.bench import oracles

    (tmp_path / "rows").mkdir()
    crs = (
        ["EPSG:4326"] * 80
        + ["EPSG:3857"] * 14
        + ["EPSG:27700"] * 14
        + ["EPSG:32618"] * 14
    )
    for i, c in enumerate(crs):  # 122 tiles; the even input asks for 5000
        (tmp_path / "rows" / f"r{i}.tif").write_bytes(oracles._tile_bytes(c, 32, 32))

    def groups(name, n):
        table = lf.INPUTS[name](spark_session_local, n, str(tmp_path), None)
        out = {}
        for key, raster in (
            spark_session_local.table(table).select("key", "tile.raster").collect()
        ):
            with MemoryFile(bytes(raster)) as mf, mf.open() as ds:
                out.setdefault(key, []).append(ds.crs.to_epsg())
        return out

    even = groups("tiles_grouped", 5000)
    assert sum(map(len, even.values())) == 122
    assert all(len(set(v)) == 1 for v in even.values()), even  # one CRS a group
    assert len(even) >= 4 and max(map(len, even.values())) <= lf._GROUP_SIZE
    skew = groups("tiles_grouped_skew", lf.BASE_ROWS["tiles_grouped_skew"])
    assert sum(map(len, skew.values())) == 112
    assert all(len(set(v)) == 1 for v in skew.values()), skew
    rest = [len(v) for k, v in skew.items() if k != "hot"]
    assert len(skew["hot"]) == 56  # half the rows in one key
    assert max(rest) <= lf._REST_GROUP_SIZE
    assert len(skew["hot"]) / statistics.median(rest) >= 10


class _Forbidden:
    """A Connect-like session. Whatever attribute the code reads that is not set here (a Connect
    session has no sparkContext, _jvm, _jsc, rdd, or settable conf) is recorded: the code under
    test may swallow the error, so the test reads the record."""

    __module__ = "pyspark.sql.connect.session"
    touched: list = []

    def __getattr__(self, name):
        _Forbidden.touched.append(name)
        raise AttributeError(name)


class _Node:
    def __init__(self, name, metrics, children=()):
        self.name, self.children = name, list(children)
        self.metrics = [
            type("M", (), {"name": k, "value": v}) for k, v in metrics.items()
        ]


def _connect_df(root, graph):
    df = type(
        "DataFrame", (_Forbidden,), {"__module__": "pyspark.sql.connect.dataframe"}
    )()
    df.sparkSession = _Forbidden()
    walk = type("C", (), {"extract_graph": lambda s: (root, graph)})
    df.executionInfo = type("I", (), {"metrics": walk()})() if graph else None
    return df


def test_connect_session_records_unavailable_metrics_instead_of_failing():
    _Forbidden.touched.clear()
    none = attribution.from_plan(_connect_df(0, None), 0.0)  # before the plan ran
    assert none["source"] == "none" and all(
        none[k] is None for k in attribution.METRICS
    )
    peak = attribution.WorkerPeak(
        _Forbidden()
    )  # nothing reaches every worker from Connect
    peak.start()
    assert peak.read(None) is None and "Connect" in peak.reason
    # Two stages (an exchange between them); the Python stage's pipeline holds a nested one.
    graph = {
        0: _Node("AdaptiveSparkPlan", {}, [1]),
        1: _Node("WholeStageCodegen (3)", {"pipelineTime": 10}, [2]),
        2: _Node("Exchange", {"shuffleBytesWritten": 64}, [3]),
        3: _Node("WholeStageCodegen (2)", {"pipelineTime": 2000}, [4]),
        4: _Node("HashAggregate", {"peakMemory": 7, "spillSize": 5}, [5]),
        5: _Node("ArrowEvalPython", {"pythonDataSent": 2048, "pythonTotalTime": 1500}, [6]),
        6: _Node("WholeStageCodegen (1)", {"pipelineTime": 1900}),
    }  # fmt: skip
    got = attribution.from_plan(_connect_df(0, graph), 0.0)
    assert got["source"] == "executionInfo"
    assert (got["ser_sent_bytes"], got["py_eval_s"], got["peak_exec_mem_bytes"]) == (
        2048,
        1.5,
        7,
    )
    assert (got["spill_bytes"], got["shuffle_bytes"]) == (5, 64)
    assert (
        got["ser_recv_bytes"] is None and got["gc_s"] is None
    )  # nobody reported: NULL, not 0
    assert got["py_eval_frac"] == pytest.approx(1.5 / 2.01)  # 2.0 + 0.01, not + 1.9
    assert _Forbidden.touched == []  # nothing a Connect session refuses was even tried


@contextlib.contextmanager
def _own_worker_pool(spark):
    """Python UDFs built inside run in workers of their own: Spark keeps a pool per worker
    environment, so what a test reads does not depend on what earlier tests left in the session's
    pool. Yields the pool's marker."""
    marker = uuid.uuid4().hex
    env = spark.sparkContext.environment
    env["GBX_TEST_POOL"] = marker
    try:
        yield marker
    finally:
        env.pop("GBX_TEST_POOL", None)


def _pool_workers(marker):
    """(pid, RSS in MB) of the Python workers a test pool holds: the `pyspark.daemon` processes
    that were started with its marker and whose parent is a `pyspark.daemon` too (the forked
    children)."""
    ppid, argv, rss, pool = {}, {}, {}, set()
    for d in (d for d in os.listdir("/proc") if d.isdigit()):
        try:
            with open(f"/proc/{d}/cmdline", "rb") as fh:
                argv[int(d)] = fh.read()
            with open(f"/proc/{d}/stat") as fh:
                ppid[int(d)] = int(fh.read().rsplit(")", 1)[1].split()[1])
            with open(f"/proc/{d}/environ", "rb") as fh:
                if f"GBX_TEST_POOL={marker}".encode() in fh.read().split(b"\0"):
                    pool.add(int(d))
            with open(f"/proc/{d}/status") as fh:
                rss[int(d)] = max(
                    (int(x.split()[1]) // 1024 for x in fh if x.startswith("VmRSS:")),
                    default=0,
                )
        except (OSError, ValueError, IndexError):
            continue  # exited while we looked, or not ours to read
    daemon = {p for p, a in argv.items() if b"pyspark.daemon" in a and p in pool}
    return [(p, rss.get(p, 0)) for p in daemon if ppid[p] in daemon]


_SWEEP_MARK = (
    b"GBX_PEAK_SWEEP=1"  # the worker sweeps' environment entry (bench/attribution.py)
)


def _forked_workers():
    """{pid: environ} of the Python workers below this process that are not the worker sweeps' own:
    the processes a `pyspark.daemon` forked. The JVM's daemons (whose parent is no daemon) are left out,
    they last as long as the session. Reads /proc only."""
    procs = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat") as fh:
                ppid = int(fh.read().rsplit(")", 1)[1].split()[1])
            with open(f"/proc/{name}/cmdline", "rb") as fh:
                argv = fh.read()
            with open(f"/proc/{name}/environ", "rb") as fh:
                environ = fh.read().split(b"\0")
        except (OSError, ValueError, IndexError):
            continue  # exited while we looked, or not ours to read
        procs[int(name)] = (ppid, argv, environ)

    def daemon(pid):
        return pid in procs and b"pyspark.daemon" in procs[pid][1]

    children = {}
    for pid, (ppid, _, _) in procs.items():
        children.setdefault(ppid, []).append(pid)
    below, todo = set(), [os.getpid()]
    while todo:
        for child in children.get(todo.pop(), []):
            if child not in below:
                below.add(child)
                todo.append(child)
    return {
        pid: procs[pid][2]
        for pid in below
        if daemon(pid) and daemon(procs[pid][0]) and _SWEEP_MARK not in procs[pid][2]
    }


def _settle(timeout=100.0):
    """Ends the Python workers that earlier tests left, before this test's reset.

    The reset notes every live worker, and a worker of any pool that is stopped after the reset voids
    the reading (the judge cannot tell that it did not run). Spark stops an idle pool's workers 60 to 70 s
    after the pool's last release. A test pool (GBX_TEST_POOL) is never used again, so its workers are
    killed at once. The default pool's are waited for: Spark still holds them, and a killed one would be
    handed to the next user of that pool.
    """
    end = time.time() + timeout
    while True:
        workers = _forked_workers()
        if not workers:
            return
        for pid, environ in workers.items():
            if any(e.startswith(b"GBX_TEST_POOL=") for e in environ):
                try:
                    os.kill(pid, 9)
                except OSError:
                    pass  # gone meanwhile
        assert (
            time.time() < end
        ), f"workers of an earlier test did not stop: {sorted(workers)}"
        time.sleep(0.5)


class _Shapes:
    """The UDFs and readings of the worker-peak tests. Build the queries inside
    `_own_worker_pool`: a UDF takes the pool of the environment it is first used in."""

    def __init__(self, spark):
        import pandas as pd
        from pyspark.sql import functions as F

        self.spark, self.F = spark, F
        self.slots = spark.sparkContext.defaultParallelism

        @F.udf("long")
        def heavy(x):
            b = bytearray(400 * 1024 * 1024)
            for i in range(0, len(b), 4096):
                b[i] = 1  # touch every page
            return len(b)

        @F.udf("long")
        def idle_fat(x):
            import builtins

            # holds 250 MB for the life of this worker, which then sits idle in the pool
            builtins._gbx_hold = bytearray(250 * 1024 * 1024)
            for i in range(0, len(builtins._gbx_hold), 4096):
                builtins._gbx_hold[i] = 1
            return 1

        @F.udf("long")
        def light(x):
            return x

        @F.pandas_udf("long")
        def plus1(s: pd.Series) -> pd.Series:
            return s + 1

        self.heavy, self.idle_fat, self.light, self.plus1 = (
            heavy,
            idle_fat,
            light,
            plus1,
        )

    def heavy_shape(self):
        return self.spark.range(self.slots, numPartitions=self.slots).select(
            self.heavy("id")
        )

    def trivial_shape(self, partitions=4):
        rows = self.spark.range(1000, numPartitions=partitions)
        return rows.select(self.F.sum(self.light("id")))

    def two_workers_per_task(self):
        """A pandas UDF over a Python UDF takes two workers per task: the pool is left with more
        idle workers than slots."""
        rows = self.spark.range(4 * self.slots, numPartitions=2 * self.slots)
        return rows.select(self.plus1(self.light("id")))

    def fat_idle_workers(self):
        """One 250 MB worker per slot, then idle."""
        return self.spark.range(self.slots, numPartitions=self.slots).select(
            self.idle_fat("id")
        )

    def peak_of(self, df):
        peak = attribution.WorkerPeak(self.spark)
        assert peak.reason is None, peak.reason
        peak.start()
        df.collect()
        return peak.read(df)


@LINUX
def test_worker_peak_is_the_shapes_not_the_workers_lifetime(spark_session_local):
    _settle()
    with _own_worker_pool(spark_session_local):
        s = _Shapes(spark_session_local)
        heavy, trivial = s.heavy_shape(), s.trivial_shape()

    assert s.peak_of(heavy) >= 400
    # a trivial shape after it does not inherit the 400 MB
    assert s.peak_of(trivial) < 150


@LINUX
def test_worker_peak_reaches_every_python_worker_not_one_per_slot(spark_session_local):
    # A query with two Python evaluators in each task takes two workers per task, so the pool is
    # left with more idle workers than the session has slots. Spark hands idle workers out
    # first-in-first-out: a sweep that only reads and resets the process it runs in lands on other
    # workers than the shape did, and a shape that held 400 MB read as the pool's baseline
    # (125 MB here, 245 MB in the full file, 298 MB on the CI runner).
    _settle()
    with _own_worker_pool(spark_session_local) as pool:
        s = _Shapes(spark_session_local)
        warm, heavy, trivial = (
            s.two_workers_per_task(),
            s.heavy_shape(),
            s.trivial_shape(),
        )
    warm.collect()
    assert (
        len(_pool_workers(pool)) > s.slots
    ), "the warm-up left no more workers than slots"

    assert s.peak_of(heavy) >= 400
    assert s.peak_of(trivial) < 150


@LINUX
def test_worker_peak_leaves_out_idle_workers_that_hold_memory(spark_session_local):
    # Idle pooled workers hold about 250 MB each, left by earlier shapes. A trivial shape runs on
    # one lean worker (the pool hands out first-in-first-out, and the fat ones went to the back),
    # so its reading is that worker's, not the 250 MB the pool holds.
    _settle()
    with _own_worker_pool(spark_session_local) as pool:
        s = _Shapes(spark_session_local)
        warm, fat, trivial = (
            s.two_workers_per_task(),
            s.fat_idle_workers(),
            s.trivial_shape(1),
        )
    warm.collect()
    fat.collect()
    held = [mb for _, mb in _pool_workers(pool) if mb >= 200]
    assert len(held) >= s.slots, "the pool holds no idle workers of 250 MB"

    assert s.peak_of(trivial) < 150


@LINUX
def test_worker_peak_is_none_when_no_python_worker_ran(spark_session_local):
    _settle()
    spark = spark_session_local
    peak = attribution.WorkerPeak(spark)
    assert peak.reason is None, peak.reason
    peak.start()
    df = spark.range(1000, numPartitions=4).selectExpr(
        "sum(id)"
    )  # no Python in the plan
    df.collect()
    assert peak.read(df) is None and "no Python worker ran" in peak.reason


def _until(cond, what, timeout=90.0):
    end = time.time() + timeout
    while not cond():
        assert time.time() < end, f"timed out waiting for {what}"
        time.sleep(0.1)


def _in_background(df):
    """df.collect() on a thread of its own; (thread, dict that holds an error, if any)."""
    box = {}

    def run():
        try:
            df.collect()
        except Exception as exc:  # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    return thread, box


def _gone(pid):
    return not os.path.exists(f"/proc/{pid}")


class _Held:
    """A shape whose tasks write their worker's pid to `where` and wait for the file ``go``.
    With `early` the first task holds 400 MB and returns at once and the others hold nothing;
    without, every task holds 400 MB until ``go``."""

    def __init__(self, spark, where, wait_s=120):
        from pyspark.sql import functions as F

        self.spark, self.where, self.F = spark, str(where), F
        self.slots = spark.sparkContext.defaultParallelism
        where = self.where

        @F.udf("long")
        def held(x, early):
            import os
            import time as t

            first = early and x == 0
            if first or not early:
                b = bytearray(400 * 1024 * 1024)
                for i in range(0, len(b), 4096):
                    b[i] = 1  # touch every page
            with open(f"{where}/pid{x}", "w") as fh:
                fh.write(str(os.getpid()))
            end = t.time() + wait_s
            while not first and not os.path.exists(f"{where}/go") and t.time() < end:
                t.sleep(0.1)
            return 0

        self.held = held

    def shape(self, early):
        rows = self.spark.range(self.slots, numPartitions=self.slots)
        return rows.select(self.held("id", self.F.lit(early)))

    def pids(self):
        out = {}
        for name in os.listdir(self.where):
            if name.startswith("pid"):
                with open(f"{self.where}/{name}") as fh:
                    text = fh.read()
                if text:
                    out[name] = int(text)
        return out

    def go(self):
        open(f"{self.where}/go", "w").close()


@LINUX
@pytest.mark.parametrize("victim", ["the worker that ran", "an idle worker"])
def test_worker_peak_is_none_when_a_worker_that_was_there_exits_before_the_read(
    spark_session_local, tmp_path, victim
):
    # One task holds 400 MB and returns; the others run on. The finished worker is idle, and
    # Spark stops an idle worker after 60 s (killed here: it is gone just the same), so its peak
    # is gone by the read. The reading used to be the live workers' (54 MB). Nothing the kernel
    # keeps says whether a worker that has gone had run, so an idle one voids the reading too.
    spark = spark_session_local
    _settle()
    with _own_worker_pool(spark) as pool:
        s = _Shapes(spark)
        held = _Held(spark, tmp_path)
        warm = (
            s.two_workers_per_task()
            if victim == "an idle worker"
            else s.trivial_shape(s.slots)
        )
        shape = held.shape(early=victim == "the worker that ran")
    warm.collect()
    peak = attribution.WorkerPeak(spark)
    assert peak.reason is None, peak.reason
    peak.start()
    thread, box = _in_background(shape)
    try:
        if victim == "the worker that ran":
            _until(lambda: "pid0" in held.pids(), "the first task to finish")
            _until(lambda: _tasks_done(spark) >= 1, "its result to reach the driver")
            doomed = [held.pids()["pid0"]]
        else:
            _until(lambda: len(held.pids()) == s.slots, "every task to be running")
            busy = set(held.pids().values())
            doomed = [pid for pid, _ in _pool_workers(pool) if pid not in busy]
            assert doomed, "the pool holds no idle worker"
        for pid in doomed:
            os.kill(pid, 9)
        _until(lambda: all(_gone(pid) for pid in doomed), "the daemon to reap them")
    finally:
        held.go()
        thread.join()
    assert "error" not in box, box
    assert peak.read(shape) is None and "exited before the read" in peak.reason


def _tasks_done(spark):
    tracker = spark.sparkContext.statusTracker()
    infos = [tracker.getStageInfo(i) for i in tracker.getActiveStageIds()]
    return max([i.numCompletedTasks for i in infos if i] or [0])


@LINUX
@pytest.mark.perf
@pytest.mark.parametrize("pool", ["warm", "cold"])
def test_worker_peak_is_none_after_spark_stops_a_finished_worker(
    spark_session_local, pool
):
    # The real thing, with nothing killed by hand: one task holds 400 MB and returns, the other
    # sleeps past Spark's 60 s idle timeout, and the finished worker is stopped. It read 54 MB.
    # With a warm pool the worker was there at the reset; with a cold one it was created by the
    # shape, and only the task end times show that a worker went idle long enough.
    from pyspark.sql import functions as F

    spark = spark_session_local
    # Settled first: the other perf test's idle worker, left in its own pool, would be stopped by Spark
    # during this shape and read as lost (it was, before the judge looked at every pool).
    _settle()
    with _own_worker_pool(spark):
        s = _Shapes(spark)

        @F.udf("long")
        def uneven(x):
            import time as t

            if x == 0:
                b = bytearray(400 * 1024 * 1024)
                for i in range(0, len(b), 4096):
                    b[i] = 1
                return len(b)
            t.sleep(85)
            return 0

        warm = s.trivial_shape(s.slots)
        shape = spark.range(s.slots, numPartitions=s.slots).select(uneven("id"))
    if pool == "warm":
        warm.collect()
    peak = attribution.WorkerPeak(spark)
    peak.start()
    shape.collect()
    assert peak.read(shape) is None
    assert (
        "exited before the read" if pool == "warm" else "may have exited"
    ) in peak.reason


def _iso(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dGMT" % (
        t % 1 * 1000
    )


def _fake_rest(
    ends=None,
    failed=0,
    killed_stage=0,
    attempt=0,
    jobs=1,
    no_attempts=False,
    group=None,
):
    """A stand-in for the Spark REST API: `jobs` jobs of one stage each (one attempt), whose
    tasks ended at `ends` ({executor: [times]}). `failed` is the job's failed-task count.
    """
    ends = ends or {"0": [time.time()]}

    def get(url):
        if url.endswith("/jobs"):
            return [
                {"jobId": i, "jobGroup": group, "stageIds": [0], "skippedStages": [],
                 "numFailedTasks": failed, "numKilledTasks": 0, "numFailedStages": 0,
                 "submissionTime": _iso(0), "completionTime": _iso(time.time())}
                for i in range(jobs)
            ]  # fmt: skip
        if "taskList" in url:
            return [
                {"launchTime": _iso(t - 0.001), "duration": 1, "executorId": ex}
                for ex, times in ends.items()
                for t in times
            ]
        if "/stages/" in url:
            if no_attempts:
                return []
            return [
                {
                    "attemptId": attempt,
                    "numFailedTasks": 0,
                    "numKilledTasks": killed_stage,
                }
            ]
        raise AssertionError(url)

    return get


@pytest.mark.parametrize(
    "case, rest, executors, long, want",
    [
        ("a quick shape, nothing wrong", {}, 1, False, ""),
        ("no job at all", {"jobs": 0}, 1, False, attribution._NO_TASKS_REASON),
        ("a stage the API will not give", {"no_attempts": True}, 1, False, attribution._NO_TASKS_REASON),
        ("a failed task", {"failed": 1}, 1, False, attribution._RETRIED_REASON),
        ("a killed task", {"killed_stage": 1}, 1, False, attribution._RETRIED_REASON),
        ("a second stage attempt", {"attempt": 1}, 1, False, attribution._RETRIED_REASON),
        ("the sweeps' own job is left out", {"failed": 1, "group": attribution._PEAK_JOB_GROUP + "x"}, 1, False, attribution._NO_TASKS_REASON),
        ("a long shape, a task end every 10 s", {"ends": {"0": [100 + 10 * i for i in range(30)]}}, 1, True, ""),
        ("a long shape, a task end every 70 s", {"ends": {"0": [100, 170, 240, 310]}}, 1, True, attribution._MAYBE_LOST_REASON),
        ("a long shape, no task end at all", {"ends": {"0": []}}, 1, True, attribution._NO_TASKS_REASON),
        ("a long shape, one executor of two", {"ends": {"0": [100 + 10 * i for i in range(30)]}}, 2, True, attribution._NO_TASKS_REASON),
    ],
)  # fmt: skip
def test_the_rest_api_must_show_no_lost_task_and_answer(
    monkeypatch, case, rest, executors, long, want
):
    """Closed by default: only a clean, complete answer reads."""
    monkeypatch.setattr(attribution, "_get", _fake_rest(**rest))
    got = attribution._rest_trouble("http://x/api", 99.0, 400.0, executors, long)
    assert got == want, case


def test_a_rest_api_that_does_not_answer_voids_every_shape(monkeypatch):
    def down(url):
        raise OSError("connection refused")

    monkeypatch.setattr(attribution, "_get", down)
    peak = attribution.WorkerPeak.__new__(attribution.WorkerPeak)
    peak.spark = types.SimpleNamespace(
        sparkContext=types.SimpleNamespace(uiWebUrl="http://x", applicationId="a")
    )
    peak._t0 = time.time() - 5  # a quick shape
    assert peak._rest_trouble(time.time(), 1) == attribution._NO_REST_REASON


@LINUX
@pytest.mark.parametrize("gap, reads", [(10.0, True), (70.0, False)])
def test_a_long_shape_reads_only_if_the_pool_never_idled_for_a_minute(
    spark_session_local, monkeypatch, gap, reads
):
    # The shape was quick; only its task end times (here made up) say how it looked to the pool.
    _settle()
    with _own_worker_pool(spark_session_local):
        s = _Shapes(spark_session_local)
        warm, shape = s.trivial_shape(s.slots), s.heavy_shape()
    warm.collect()
    peak = attribution.WorkerPeak(spark_session_local)
    peak.start()
    shape.collect()
    peak._t0 -= 300  # as if the shape had run for five minutes
    now = time.time()
    # the made-up end times come from the executor the shape really ran on, one every `gap` s
    ex = [e["id"] for e in attribution._get(
        f"{spark_session_local.sparkContext.uiWebUrl}/api/v1/applications/"
        f"{spark_session_local.sparkContext.applicationId}/executors")][0]  # fmt: skip
    ends = {ex: [now - 300 + i * gap for i in range(int(300 / gap) + 1)]}
    monkeypatch.setattr(attribution, "_get", _fake_rest(ends))
    got = peak.read(shape)
    assert (got is not None and got >= 400) is reads, (got, peak.reason)
    assert reads or "may have exited" in peak.reason


_RETRY = """
import json, os, sys, tempfile, threading, time, uuid
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[2, 3]")  # 3 attempts per task
         .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
from databricks.labs.gbx.bench import attribution
where = tempfile.mkdtemp()
env = spark.sparkContext.environment
env["GBX_TEST_POOL"] = uuid.uuid4().hex

@F.udf("long")
def victim(x):
    import os, time
    from pyspark import TaskContext
    if x == 0 and TaskContext.get().attemptNumber() == 0:
        b = bytearray(400 * 1024 * 1024)
        for i in range(0, len(b), 4096):
            b[i] = 1
        open(where + "/victim", "w").write(str(os.getpid()))
        time.sleep(60)  # killed from outside long before this
    elif x == 1:
        time.sleep(4)
    return 0

shape = spark.range(2, numPartitions=2).select(victim("id"))
env.pop("GBX_TEST_POOL")
peak = attribution.WorkerPeak(spark)
peak.start()

def killer():
    end = time.time() + 60
    while time.time() < end:
        try:
            os.kill(int(open(where + "/victim").read()), 9)
            return
        except (OSError, ValueError):
            time.sleep(0.05)

thread = threading.Thread(target=killer)
thread.start()
shape.collect()  # the killed task is retried on a new worker that holds nothing
thread.join()
print(json.dumps({"peak": peak.read(shape), "reason": peak.reason}))
"""


@LINUX
def test_worker_peak_is_none_when_a_task_is_retried_after_its_worker_was_killed():
    # A worker the shape created holds 400 MB and is killed (a crash, an OOM kill); its task runs
    # again on a new worker that holds nothing. The killed worker leaves no trace in /proc, and the
    # reading used to be the new worker's 52 MB. The REST API shows the lost attempt.
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
        "PYSPARK_PYTHON": sys.executable,
    }
    out = subprocess.run(
        [sys.executable, "-c", _RETRY],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    got = json.loads(out.stdout.strip().splitlines()[-1])
    assert got["peak"] is None, (got, out.stderr[-500:])
    assert got["reason"] == attribution._RETRIED_REASON


@LINUX
@pytest.mark.parametrize("pool", ["warm", "cold"])
def test_worker_peak_is_none_when_a_read_that_stops_early_destroys_the_workers(
    spark_session_local, pool
):
    # `limit` makes the JVM stop reading: Spark destroys the heavy worker without failing a task, and
    # another action of the session runs on a worker that stays. The reading was that worker's (51 MB).
    # The physical plan of the shape holds the early stop, and the plan decides it.
    spark = spark_session_local
    _settle()
    from pyspark.sql import functions as F

    @F.udf("long")
    def first_is_heavy(x):
        if x == 0:
            b = bytearray(400 * 1024 * 1024)
            for i in range(0, len(b), 4096):
                b[i] = 1
        return x

    with _own_worker_pool(spark):
        s = _Shapes(spark)
        warm = s.trivial_shape(s.slots) if pool == "warm" else None
        shape = spark.range(1000, numPartitions=2).select(first_is_heavy("id")).limit(1)
        other = s.trivial_shape(s.slots)  # the second action, on the same pool
    if warm is not None:
        warm.collect()
    peak = attribution.WorkerPeak(spark)
    assert peak.reason is None, peak.reason
    peak.start()
    shape.collect()  # the limit ends the read: the heavy worker goes, and no task fails
    other.collect()  # another action of the session, on a worker that stays
    assert peak.read(shape) is None
    assert "an early-stop plan can end a worker before the read" in peak.reason


@LINUX
def test_worker_peak_is_none_when_another_pools_idle_worker_exits(
    spark_session_local, tmp_path
):
    # The pool of an earlier test still holds idle workers, and Spark stops them (killed here). They did
    # not run this shape, but the reading cannot tell that: any worker that was there at the reset voids it.
    spark = spark_session_local
    _settle()
    with _own_worker_pool(spark) as other:
        warm = _Shapes(spark).two_workers_per_task()
    with _own_worker_pool(spark):
        s = _Shapes(spark)
        held = _Held(spark, tmp_path)
        shape = held.shape(early=False)
    warm.collect()
    strays = [pid for pid, _ in _pool_workers(other)]
    assert strays, "the earlier pool holds no worker"
    peak = attribution.WorkerPeak(spark)
    peak.start()
    thread, box = _in_background(shape)
    try:
        _until(lambda: len(held.pids()) == s.slots, "every task to be running")
        for pid in strays:
            os.kill(pid, 9)
        _until(lambda: all(_gone(pid) for pid in strays), "the daemon to reap them")
    finally:
        held.go()
        thread.join()
    assert "error" not in box, box
    assert peak.read(shape) is None and peak.reason == attribution._LOST_REASON


@LINUX
def test_worker_peak_is_none_when_one_of_two_pools_runners_exits(
    spark_session_local, tmp_path
):
    # A shape on two worker environments. Pool A's tasks each hold 400 MB and return, so every runner of
    # pool A is idle afterwards; Spark's idle stop takes them (killed here) while pool B's work runs on.
    # The reading was pool B's small mark (51.6 MB) with no reason: the judge skipped pool A, since none
    # of its runners was left to show it had run. Any worker that was there at the reset voids the reading.
    from pyspark.sql import functions as F

    spark = spark_session_local
    _settle()
    where = tmp_path / "a"
    where.mkdir()
    w = str(where)
    with _own_worker_pool(spark):
        s = _Shapes(spark)
        warm_a = s.two_workers_per_task()  # leaves idle workers of pool A at the reset

        @F.udf("long")
        def hog(x):
            import os

            b = bytearray(400 * 1024 * 1024)
            for i in range(0, len(b), 4096):
                b[i] = 1  # touch every page
            with open(f"{w}/pid{x}", "w") as fh:
                fh.write(str(os.getpid()))
            return x

        shape_a = spark.range(s.slots, numPartitions=s.slots).select(hog("id"))

    @F.udf("long")
    def busy(x):
        return sum(range(3_000_000))  # CPU time, so that pool B's worker shows it ran

    with _own_worker_pool(spark):
        warm_b = spark.range(1, numPartitions=1).select(busy("id"))
        shape_b = spark.range(1, numPartitions=1).select(busy("id"))
    warm_a.collect()
    warm_b.collect()  # pool B's worker is there at the reset too
    peak = attribution.WorkerPeak(spark)
    assert peak.reason is None, peak.reason
    peak.start()
    shape_a.collect()  # every task of pool A ran, and its worker is now idle
    shape_b.collect()  # pool B's worker ran too
    doomed = [int((where / f"pid{x}").read_text()) for x in range(s.slots)]
    for pid in doomed:
        os.kill(pid, 9)
    _until(lambda: all(_gone(pid) for pid in doomed), "the daemons to reap them")
    assert peak.read(shape_a) is None and peak.reason == attribution._LOST_REASON


@LINUX
@pytest.mark.parametrize(
    "how", ["killed", pytest.param("idle", marks=pytest.mark.perf)]
)
def test_worker_peak_is_none_when_two_environments_start_cold_and_one_goes_idle(
    spark_session_local, tmp_path, how
):
    # A shape on two worker environments, both cold, so neither pool has a baseline. Pool A's two
    # tasks hold 400 MB and return; then pool A's workers go: killed here, or stopped by Spark's idle stop
    # while pool B keeps releasing (the idle variant, as the probe ran it). The reading was pool B's small
    # mark (55.07 MB, no reason). Two pools that ran on one executor are two environments: NULL.
    from pyspark.sql import functions as F

    spark = spark_session_local
    _settle()
    w = str(tmp_path)

    @F.udf("long")
    def hog(x):
        import os

        b = bytearray(400 * 1024 * 1024)
        for i in range(0, len(b), 4096):
            b[i] = 1  # touch every page
        with open(f"{w}/pid{x}", "w") as fh:
            fh.write(str(os.getpid()))
        return x

    @F.udf("long")
    def busy(x):
        import time as t

        end = t.process_time() + 0.4  # CPU time past the CPU-tick filter
        while t.process_time() < end:
            pass
        return x

    with _own_worker_pool(spark):
        shape_a = spark.range(2, numPartitions=2).select(hog("id"))
    with _own_worker_pool(spark):
        shape_b = spark.range(1, numPartitions=1).select(busy("id"))
    peak = attribution.WorkerPeak(spark)
    assert peak.reason is None, peak.reason
    peak.start()  # both pools are cold: neither exists at the reset
    shape_a.collect()
    pids = [int((tmp_path / f"pid{x}").read_text()) for x in (0, 1)]
    if how == "killed":
        for pid in pids:
            os.kill(pid, 9)
        _until(lambda: all(_gone(pid) for pid in pids), "the daemons to reap them")
        shape_b.collect()
    else:
        for _ in range(8):  # pool B releases every 10 s for 80 s; pool A idles out
            shape_b.collect()
            time.sleep(10)
        _until(lambda: all(_gone(pid) for pid in pids), "pool A's idle stop", 60.0)
    assert peak.read(shape_a) is None and peak.reason == attribution._MULTI_ENV_REASON


@LINUX
def test_the_reading_is_none_when_the_plan_is_not_given_or_stops_early(
    spark_session_local,
):
    spark = spark_session_local
    peak = attribution.WorkerPeak(spark)
    assert peak.read(None) is None and peak.reason == attribution._NO_PLAN_REASON
    peak = attribution.WorkerPeak(spark)
    limited = spark.range(100, numPartitions=2).limit(3)
    assert peak.read(limited) is None
    assert peak.reason == attribution._EARLY_STOP_REASON.format(node="CollectLimit")


def _udf_over_a_limit(spark):
    from pyspark.sql import functions as F

    ident = F.udf(lambda x: x, "long")
    return spark.range(100, numPartitions=2).limit(5).agg(F.sum(ident("id")))


@pytest.mark.parametrize(
    "build, names",
    [
        (lambda s: s.range(100, numPartitions=2).select("id"), ("",)),
        (lambda s: s.range(100, numPartitions=2).limit(3), ("CollectLimit",)),
        (
            lambda s: s.range(100, numPartitions=2).orderBy("id").limit(3),
            ("TakeOrderedAndProject",),
        ),
        (
            lambda s: s.range(100, numPartitions=2).limit(3).agg({"id": "count"}),
            ("GlobalLimit", "LocalLimit"),
        ),
        (lambda s: s.sql("SELECT id FROM range(100) LIMIT 2"), ("CollectLimit",)),
        # a limit that builds the input, below the Python evaluation, ends no worker early
        (lambda s: _udf_over_a_limit(s), ("",)),
    ],
)
def test_an_early_stop_is_found_in_the_physical_plan(spark_session_local, build, names):
    assert attribution._early_stop_node(build(spark_session_local)) in names
    assert attribution._early_stop_node(None) is None  # a plan that cannot be read


@pytest.mark.parametrize(
    "case, members, want",
    [
        ("nothing moved", {"w1": (5, 0), "w2": (7, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (0, 0, 0, 0)),
        ("a worker ran and is alive", {"w1": (9, 400), "w2": (7, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (400, 1, 0, 1)),
        ("a worker of the pool that ran has gone", {"w1": (9, 400), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (400, 1, 1, 1)),
        ("a worker has gone and nothing ran", {"w1": (5, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (0, 0, 1, 0)),
        ("another pool's straggler has gone", {"w1": (9, 400), "w2": (7, 0), "d": (100, 0), "e": (50, 0)}, (400, 1, 1, 1)),
        ("another pool's fat idle worker is not counted", {"w1": (9, 90), "w2": (7, 0), "x1": (3, 250), "d": (100, 0), "e": (50, 0)}, (90, 1, 0, 1)),
        ("a new worker ran", {"w1": (5, 0), "w2": (7, 0), "w3": (2, 80), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (80, 1, 0, 1)),
        ("a zombie of a pool that ran", {"w1": (9, 0, "z"), "w2": (7, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (0, 0, 1, 1)),
        ("an idle zombie beside a worker that ran", {"w1": (5, 0, "z"), "w2": (9, 60), "x1": (3, 0), "d": (100, 0), "e": (50, 0)}, (60, 1, 1, 1)),
        ("a new daemon is not a worker", {"w1": (5, 0), "w2": (7, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0), "f": (7, 51)}, (0, 0, 0, 1)),
        ("only the daemons have gone", {"w1": (5, 0), "w2": (7, 0), "x1": (3, 0)}, (0, 0, 0, 0)),
        ("two environments ran, both warm", {"w1": (9, 400), "w2": (7, 0), "x1": (7, 60), "d": (100, 0), "e": (50, 0)}, (400, 2, 0, 2)),
        ("a second environment's new daemon", {"w1": (9, 400), "w2": (7, 0), "x1": (3, 0), "d": (100, 0), "e": (50, 0), "f": (7, 51)}, (400, 1, 0, 2)),
    ],
)  # fmt: skip
def test_the_read_counts_the_workers_that_ran_and_voids_any_that_have_gone(
    case, members, want
):
    """The judgement on synthetic /proc numbers. The baseline: workers w1 (5 ticks) and w2 (7) of
    daemon d (100), and x1 (3) of daemon e (50). Any worker that was there at the reset and has gone
    voids the reading, whichever pool it is in; a daemon never counts. ``envs`` counts the pools that
    ran: a daemon new since the reset, or a pool with a worker that is new or whose ticks moved.
    """
    scope = "h:1"
    key = lambda k: f"{scope}:{k}"  # noqa: E731
    pid_of = {
        "d": 100,
        "e": 200,
        "f": 300,
    }  # the daemons' pids; a worker's ppid is its pool's
    baseline = {
        "cpu": {key("w1"): 5, key("w2"): 7, key("x1"): 3, key("d"): 100, key("e"): 50},
        "daemons": {key("d"), key("e")},
    }
    live = {}
    for k, v in members.items():
        daemon = k in "def"
        live[key(k)] = {
            "cpu": v[0],
            "mb": v[1],
            "zombie": len(v) > 2,
            "daemon": daemon,
            "pid": pid_of[k] if daemon else 0,
            "ppid": 1 if daemon else pid_of[{"w": "d", "x": "e"}[k[0]]],
        }
    assert attribution._read_judge()(scope, live, baseline) == want, case


def test_the_longest_wait_between_task_ends_is_per_executor_and_runs_to_the_read():
    gap = attribution._longest_release_gap
    assert gap({"0": [10.0, 12.0, 80.0]}, 90.0) == 68.0  # 12 -> 80
    assert gap({"1": [11.0, 12.5]}, 100.0) == 87.5  # 12.5 -> the read
    assert (
        gap({"0": [10.0, 12.0, 80.0], "1": [11.0, 12.5]}, 90.0) == 77.5
    )  # the worst of both
    assert gap({}, 100.0) == 0.0  # no task ended: no worker was released


_N1 = """
import json, sys
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]").config("spark.default.parallelism", "2")
         .getOrCreate())
from databricks.labs.gbx.bench import attribution
sys.platform = "linux"  # the slot check runs before any worker is touched
p = attribution.WorkerPeak(spark)
p.start()
print(json.dumps({"peak": p.read(spark.range(1)), "reason": p.reason}))
"""


def test_a_stale_default_parallelism_reads_null_not_a_partial_reset():
    # 4 real slots but a default parallelism of 2: a 2-task sweep would reset only some workers
    # and still read a number (a trivial shape after a heavy one read 531 MB, with no note).
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
    }  # same code
    out = subprocess.run(
        [sys.executable, "-c", _N1],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    got = json.loads(out.stdout.strip().splitlines()[-1])
    assert (
        got["peak"] is None
        and "default parallelism (2) != executor slots (4)" in got["reason"]
    )


@pytest.mark.parametrize(
    "master, parallelism, executors, slots",
    [
        # a cluster's driver is no executor, whatever cores it reports; local mode's IS
        ("spark://h:7077", 16, {"driver": 8, "0": 8, "1": 8}, 16),
        ("local[4]", 4, {"driver": 4}, 4),
    ],
)
def test_the_slot_count_leaves_out_a_cluster_drivers_cores(
    monkeypatch, master, parallelism, executors, slots
):
    sc = types.SimpleNamespace(
        master=master, uiWebUrl="http://x", applicationId="a", getConf=dict,
        defaultParallelism=parallelism,
    )  # fmt: skip
    rest = [{"id": i, "totalCores": c, "isActive": True} for i, c in executors.items()]
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(attribution, "_get", lambda url: rest)
    peak = attribution.WorkerPeak(types.SimpleNamespace(sparkContext=sc))
    assert (peak._slots, peak.reason) == (slots, None)


def test_the_floor_udf_is_registered_once_outside_the_timed_region(
    spark_session_local, tmp_path, monkeypatch
):
    from pyspark.sql.udf import UDFRegistration

    calls, real = [], UDFRegistration.register
    monkeypatch.setattr(
        UDFRegistration, "register",
        lambda self, name, *a, **k: calls.append(name) or real(self, name, *a, **k),
    )  # fmt: skip
    only_floor = [f for f in lf.FORMS["S"] if f.variant == "floor"]
    monkeypatch.setitem(lf.FORMS, "S", only_floor)
    rows = run_s(spark_session_local, tmp_path, scales=(1, 2))
    assert [r.status for r in rows] == ["ok", "ok"]  # four timed iterations in all
    assert calls.count("gbx_bng_floor") == 1


def test_a_busy_cluster_cannot_stall_the_worker_reset(spark_session_local, monkeypatch):
    from pyspark.sql import functions as F

    spark = spark_session_local
    slots = spark.sparkContext.defaultParallelism
    monkeypatch.setattr(sys, "platform", "linux")  # only the wait is under test
    monkeypatch.setattr(attribution.WorkerPeak, "TIMEOUT_S", 2.0)
    monkeypatch.setattr(attribution.WorkerPeak, "_failed", {})
    nap = F.udf(lambda x: time.sleep(7) or x, "long")
    busy = threading.Thread(
        target=lambda: spark.range(slots, numPartitions=slots)
        .select(nap("id"))
        .collect()
    )
    busy.start()
    time.sleep(1.5)  # every slot is taken
    peak, t0 = attribution.WorkerPeak(spark), time.time()
    peak.start()
    assert (
        time.time() - t0 < 6
        and peak.read(spark.range(1)) is None
        and "timed out" in peak.reason
    )
    busy.join()


@pytest.mark.parametrize(
    "text, expect",
    [
        ("total (min, med, max (stageId: taskId))\n398.5 KiB (199.2 KiB, 1 B)", 398.5 * 1024),
        ("total (min, med, max (stageId: taskId))\n1.8 s (893 ms, 894 ms)", 1.8),
        ("\n893 ms (3 ms, 4 ms)", 0.893),
        ("20,000", 20000.0),
        ("128.0 B", 128.0),
        ("2.5 m", 150.0),
        (7, 7.0),
        ("n/a", 0.0),
    ],
)  # fmt: skip
def test_rest_metric_text_is_read_past_its_header(text, expect):
    assert attribution._num(text) == pytest.approx(expect)


@pytest.mark.parametrize(
    "t0, expect",
    [
        (1000.0, 7),
        (1000.2, 8),
        (1000.4, 9),
        (1001.0, None),
    ],  # first at or after t0, not newest
)
def test_the_rest_execution_is_the_one_the_action_started(t0, expect):
    stamp = (
        lambda s: f"1970-01-01T00:{int(s) // 60 % 60:02d}:{s % 60:06.3f}GMT"
    )  # noqa: E731
    times = ((6, 995.0), (7, 1000.1), (8, 1000.3), (9, 1000.5))
    execs = [{"id": i, "submissionTime": stamp(t)} for i, t in times]
    got = attribution._pick_execution(execs, t0)
    assert (got["id"] if got else None) == expect


def test_old_rows_load_and_unavailable_is_null_in_the_delta_columns(
    tmp_path, spark_session_local
):
    fields = dataclasses.fields(results.ResultRow)
    old = {
        f.name: "x" if f.type in (str, "str") else 0
        for f in fields
        if f.default is dataclasses.MISSING
    }
    f = tmp_path / "old.jsonl"
    f.write_text(json.dumps(old) + "\n")
    (row,) = results.read_jsonl(f)
    assert (row.level, row.form, row.compute, row.failure_mode) == (
        "na",
        "na",
        "na",
        "",
    )
    new = (
        "ser_sent_bytes",
        "py_eval_s",
        "py_eval_frac",
        "native_s",
        "py_worker_rss_mb",
    )
    assert all(getattr(row, k) is None for k in new)  # NULL, not a made-up 0
    assert {"level", "form", "compute", "failure_mode", "gc_s", "py_eval_frac"} <= set(
        cl.ORDER
    )
    # A batch where a metric is None in every row still has the column's real type, so it
    # appends to a table that holds numbers (an all-None pandas column would be `void`).
    df = cl.rows_to_dataframe([row], spark_session_local)
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert types["ser_sent_bytes"] == "bigint" and types["gc_s"] == "double"
    assert "void" not in types.values() and df.collect()[0]["ser_sent_bytes"] is None


def test_a_form_with_null_outputs_is_an_error_unless_it_allows_them(
    spark_session_local, monkeypatch, tmp_path
):
    sql = "SELECT count(*) AS n, sum(x) AS fp, count(x) AS n_out FROM (SELECT nullif(id, 3) AS x FROM {src})"

    def view(spark, n, corpus, schema):
        spark.range(n).createOrReplaceTempView("t_null_in")
        return "t_null_in"

    monkeypatch.setitem(lf.INPUTS, "t_in", view)
    out = {}
    for allows in (False, True):
        form = levels.Form(
            "T", "f", "L3", "-", "t_in", sql, register=False, allows_null=allows
        )
        monkeypatch.setitem(lf.FORMS, "T", [form])
        (out[allows],) = run_s(
            spark_session_local, tmp_path, (1,), 10, mvp="T", only_levels=None
        )
    assert (
        out[False].status == "error"
        and "1 of 10 outputs NULL" in out[False].failure_mode
    )
    assert out[True].status == "ok" and "1 of 10 outputs NULL" in out[True].note


def test_levels_leg_on_a_connect_session_reports_what_executioninfo_carries(
    spark_connect_local, tmp_path
):
    # Runs alone (the fixture skips when a classic session is already up in the process).
    ok = [
        r
        for r in run_s(spark_connect_local, tmp_path, scales=(1, 4))
        if r.status == "ok"
    ]
    assert ok and all(r.compute == "standard" for r in ok)
    for r in ok:
        assert "attr=executionInfo" in r.note and "unavailable:" in r.note
        assert r.gc_s is None and r.py_worker_rss_mb is None  # not measurable here
        assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
        assert "Connect" in r.note  # the reason the worker peak is NULL


def test_notebook_runs_the_levels_leg_alone_when_asked():
    cfg = dict(
        wheel="/Volumes/c/s/v/w.whl", corpus="/Volumes/c/s/v/corpus", out_dir="/Volumes/c/s/v/out",
        table="c.s.bench_results", run_id="r1", functions="", modes="spark-path", row_counts="10",
        warmup=0, measured=1, lightweight=True, levels_mvp="S", levels_uc="c.s",
    )  # fmt: skip

    def sources(**kw):
        nb = cl.build_bench_notebook({**cfg, **kw})
        return "\n".join("".join(c["source"]) for c in nb["cells"])

    both, only = sources(), sources(levels_only=True)
    assert "LEVELS_MVP = 'S'" in both and "LEVELS_UC = 'c.s'" in both
    assert "_lv.run_mvp(" in both and "_lv.run_mvp(" in only
    assert "LEVELS_ONLY = False" in both and "LEVELS_ONLY = True" in only
    # the function-benchmark sections run beside the leg, and not at all with --levels-only
    assert "# (c) Lightweight spark-path" in both
    assert "# (c) Lightweight spark-path" not in only
    assert "_lv.run_mvp(" not in sources(levels_mvp="")  # off unless asked
    assert "LEVELS_SCALES = (1, 4, 16)" in both and "scales=LEVELS_SCALES" in both
    assert "LEVELS_SCALES = (1, 4)" in sources(levels_scales=(1, 4))
    # --levels-forms limits the forms the notebook times, in the run and in the smoke
    assert "LEVELS_FORMS = ()" in both
    assert "LEVELS_FORMS = ('h_combine_avg',)" in sources(
        levels_forms=("h_combine_avg",)
    )
    assert "_f.variant in LEVELS_FORMS" in both


def test_a_failed_form_keeps_the_python_workers_cause():
    """A Python worker's error ends with the cause, after a traceback longer than any prefix the row keeps."""
    from databricks.labs.gbx.bench.levels import failure_text

    text = (
        "\n  An exception was thrown from the Python worker. Please see the stack trace below.\n"
        "[PYTHON_EXCEPTION] An exception was thrown from the Python worker: Traceback (most recent call last):\n"
        + '  File "/databricks/spark/python/worker.py", line 5393, in invoke_udf\n    fun()\n'
        * 20
        + "ImportError: libgdal.so.39: cannot open shared object file\n SQLSTATE: 38000"
    )
    got = failure_text(RuntimeError(text))
    assert got.startswith(
        "RuntimeError: An exception was thrown from the Python worker"
    )
    assert got.endswith("ImportError: libgdal.so.39: cannot open shared object file")
    assert failure_text(ValueError("bad window")) == "ValueError: bad window"


def test_a_failed_form_keeps_the_executors_exception_class_and_reason():
    """Spark folds the executor's exception into one long "Lost task" line; a head cut at 200 characters left
    "(executor 4): java.l" and nothing of what failed."""
    from databricks.labs.gbx.bench.levels import failure_text

    text = (
        "An error occurred while calling o21387.collectToPython.\n"
        ": org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 761.0 failed 4 times, "
        "most recent failure: Lost task 3.3 in stage 761.0 (TID 11675) (10.0.0.1 executor 4): "
        "java.lang.IllegalStateException: geobrix: this JVM already runs the GDAL loaded from /a/lib/x.so, and a "
        "later configuration names /b/lib/x.so\n"
        "\tat com.databricks.labs.gbx.rasterx.gdal.GDALManager$.ensureJniLoaded(GDALManager.scala:437)\n"
        "Driver stacktrace:\n"
    )
    got = failure_text(RuntimeError(text))
    assert (
        "java.lang.IllegalStateException: geobrix: this JVM already runs the GDAL"
        in got
    )
    assert "/b/lib/x.so" in got
    wrapped = text.replace(
        "Driver stacktrace:",
        "Caused by: java.io.FileNotFoundException: /Volumes/x/y.tif\nDriver stacktrace:",
    )
    assert "Caused by: java.io.FileNotFoundException: /Volumes/x/y.tif" in failure_text(
        RuntimeError(wrapped)
    )


def test_a_multiline_pip_failure_keeps_its_first_lines_not_just_the_first():
    """A UC function environment that cannot install the wheel fails with pip output, no exception-class line:
    the first actionable pip lines are the finding."""
    from databricks.labs.gbx.bench.levels import failure_text

    pip = [
        "[ISOLATED_COMMAND_EXEC_ERROR] Failed to create the environment for the function; pip install returned 1.",
        "Looking in links: /Volumes/x/y",
        "ERROR: Could not find a version that satisfies the requirement geobrix==0.6.0+env6",
        "ERROR: No matching distribution found for geobrix==0.6.0+env6",
    ] + [f"extra pip line {i}" for i in range(30)]
    got = failure_text(RuntimeError("\n".join(pip)))
    assert "Could not find a version that satisfies" in got
    assert "No matching distribution" in got
    assert "extra pip line 20" not in got  # bounded: about the first ten lines
    assert len(got) < 1000


def test_the_smoke_keeps_the_head_of_a_failure_not_its_tail():
    from databricks.labs.gbx.bench import cluster as cl

    src = "\n".join(
        "".join(c["source"])
        for c in cl.build_bench_notebook(
            dict(corpus="/c", out_dir="/o", table="a.b.c", run_id="r", functions="", set="core",
                 modes="spark-path", row_counts="1", warmup=0, measured=1, lightweight=True,
                 heavyweight=False, levels_mvp="S", levels_only=True, wheel="/w.whl", smoke=True)
        )["cells"]
    )  # fmt: skip
    assert "failure=_r.failure_mode[:700]" in src and "failure_mode[-400:]" not in src
