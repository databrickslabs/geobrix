"""The levels leg: forms timed per level, with the attribution split beside the timing."""

import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
import types

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
    assert peak.read() is None and "Connect" in peak.reason
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


def _peak_shapes(spark):
    """(peak_of, heavy, light): a UDF that holds 400 MB, one that holds nothing, and the worker
    peak WorkerPeak reads for a query run between its reset and its read."""
    from pyspark.sql import functions as F

    @F.udf("long")
    def heavy(x):
        b = bytearray(400 * 1024 * 1024)
        for i in range(0, len(b), 4096):
            b[i] = 1  # touch every page
        return len(b)

    @F.udf("long")
    def light(x):
        return x

    def peak_of(df):
        peak = attribution.WorkerPeak(spark)
        assert peak.reason is None, peak.reason
        peak.start()
        df.collect()
        return peak.read()

    return peak_of, heavy, light


def _python_workers():
    """The Python worker processes of this machine's Spark sessions: the `pyspark.daemon`
    processes whose parent is a `pyspark.daemon` too (the daemon's forked children)."""
    ppid, argv = {}, {}
    for d in (d for d in os.listdir("/proc") if d.isdigit()):
        try:
            with open(f"/proc/{d}/cmdline", "rb") as fh:
                argv[int(d)] = fh.read()
            with open(f"/proc/{d}/stat") as fh:
                ppid[int(d)] = int(fh.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue  # exited while we looked
    daemon = {p for p, a in argv.items() if b"pyspark.daemon" in a}
    return [p for p in daemon if ppid[p] in daemon]


def _assert_the_peak_is_the_shapes(spark, heavy_partitions, warm=None):
    """A shape that holds 400 MB reads at least that; a trivial shape after it does not inherit it.

    The second reading is held against a trivial shape's own, taken first on the same workers:
    a pool that earlier tests left holding 250 MB reads that, whatever runs, but never the
    400 MB more that a heavy shape adds. `warm` runs before any reading."""
    from pyspark.sql import functions as F

    peak_of, heavy, light = _peak_shapes(spark)
    if warm:
        warm(light)
    trivial = spark.range(1000, numPartitions=4).select(F.sum(light("id")))
    floor = peak_of(trivial)

    assert (
        peak_of(
            spark.range(heavy_partitions, numPartitions=heavy_partitions).select(
                heavy("id")
            )
        )
        >= 400
    )
    assert peak_of(trivial) < floor + 100


@LINUX
def test_worker_peak_is_the_shapes_not_the_workers_lifetime(spark_session_local):
    _assert_the_peak_is_the_shapes(spark_session_local, heavy_partitions=4)


@LINUX
def test_worker_peak_reaches_every_python_worker_not_one_per_slot(spark_session_local):
    # A query with two Python evaluators in each task (a pandas UDF over a Python UDF) takes two
    # workers per task, so the pool is left with more idle workers than the session has slots.
    # Spark hands idle workers out first-in-first-out: a sweep that only reads and resets the
    # process it runs in lands on other workers than the shape did, and a shape that held 400 MB
    # read as the pool's baseline (125 MB here, 245 MB in the full file, 298 MB on the CI runner).
    import pandas as pd
    from pyspark.sql import functions as F

    spark = spark_session_local
    slots = spark.sparkContext.defaultParallelism

    @F.pandas_udf("long")
    def plus1(s: pd.Series) -> pd.Series:
        return s + 1

    def two_workers_per_task(light):
        spark.range(4 * slots, numPartitions=2 * slots).select(
            plus1(light("id"))
        ).collect()
        assert (
            len(_python_workers()) > slots
        ), "the warm-up left no more workers than slots"

    _assert_the_peak_is_the_shapes(
        spark, heavy_partitions=slots, warm=two_workers_per_task
    )


_N1 = """
import json, sys
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]").config("spark.default.parallelism", "2")
         .getOrCreate())
from databricks.labs.gbx.bench import attribution
sys.platform = "linux"  # the slot check runs before any worker is touched
p = attribution.WorkerPeak(spark)
p.start()
print(json.dumps({"peak": p.read(), "reason": p.reason}))
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
    assert time.time() - t0 < 6 and peak.read() is None and "timed out" in peak.reason
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
