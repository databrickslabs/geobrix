"""The engine leaves nothing behind: 1,000-tile passes on local[8] leave /vsimem/gbx-spec empty in every worker,
and once the workers are warm more passes do not grow their memory. Needs the payload's osgeo (the container with
GBX_NATIVE_ROOT).
"""

import os
from test._provided_spark import wanted as _provided_session

import pytest

from databricks.labs.gbx import register
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.manifest import TILE_SQL

N, PARTS = 1000, 8


def test_reading_an_engine_input_does_not_import_the_light_function_module():
    """A worker's first engine row must not import pyrx.functions (about 10,000 lines: over 0.3 s on every new
    worker, paid even for a NULL row the batch wrapper routes to the row-wise UDF). `_inputs` on a materialized
    tile reads no pixels and needs no osgeo, so this runs on the host."""
    import subprocess
    import sys

    code = (
        "import sys\n"
        "from databricks.labs.gbx import manifest\n"
        "from databricks.labs.gbx.engine import l3\n"
        "fn = manifest.load().functions['gbx_rst_slope']\n"
        "tile = {'cellid': 7, 'raster': b'II*\\x00' + bytes(60), 'path': None, 'metadata': {}}\n"
        "rasters, passthrough, cellid = l3._inputs(fn, tile)\n"
        "assert rasters and cellid == 7\n"
        "assert l3._inputs(fn, None) == (None, None, None)\n"
        "print('databricks.labs.gbx.pyrx.functions' in sys.modules)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=300
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip() == "False", out.stdout + out.stderr[-2000:]


@pytest.fixture(scope="module")
def tiles(spark8):
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    from databricks.labs.gbx.bench import levels_forms

    tile = levels_forms._se_dem_tile(
        256, 1
    )  # 256 KiB decoded, noisy: a leaked input or dataset shows
    return (
        spark8.createDataFrame([(tile,)] * N, f"tile {TILE_SQL}")
        .repartition(PARTS)
        .cache()
    )


def _worker_state(spark):
    """{pid: (files under /vsimem/gbx-spec, RSS in kB)} of the Python workers, by a probe on many partitions."""
    import pandas as pd
    from pyspark.sql import functions as F

    def probe(it):
        from databricks.labs.gbx.engine import run as engine_run

        left = engine_run._gdal().ReadDirRecursive(engine_run.PREFIX) or []
        with open("/proc/self/status") as fh:
            rss = next(int(ln.split()[1]) for ln in fh if ln.startswith("VmRSS:"))
        for _ in it:
            pass
        yield pd.DataFrame({"pid": [os.getpid()], "left": [len(left)], "rss_kb": [rss]})

    rows = (
        spark.range(64 * PARTS)
        .repartition(64 * PARTS)
        .mapInPandas(probe, "pid long, left long, rss_kb long")
        .groupBy("pid")
        .agg(F.max("left").alias("left"), F.max("rss_kb").alias("rss_kb"))
        .collect()
    )
    return {r["pid"]: (r["left"], r["rss_kb"]) for r in rows}


def _count(spark, df, level, variant):
    register(spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": (level, variant)})
    df.createOrReplaceTempView("_gbx_leak")
    (r,) = spark.sql(
        "SELECT count(*) AS n, count(s.raster) AS ok FROM "
        "(SELECT gbx_rst_slope(tile) AS s FROM _gbx_leak)"
    ).collect()
    return r["n"], r["ok"]


def test_a_thousand_tiles_through_c1_leave_no_vsimem_file_and_flat_workers(
    spark8, tiles
):
    # A worker's first real engine batch loads GDAL's drivers and the light tier (some 65 to 100 MiB, once), and its
    # first full batch holds its rows' buffers (about 55 MiB more, once). Local[8] keeps more than eight workers and a
    # pass reaches only some of them, so under load a worker can take one of these in the measured window. So one row on
    # each of 512 partitions loads every worker, and three passes bring most to their batch peak. A leak of an input
    # (150 KiB a call here, some 330 MiB over four passes) grows the workers' total in every window, whichever workers a
    # pass reaches; a first batch is a one-time step, so three windows of four passes cannot all hold the two or more
    # that would carry the total past 100 MiB. The total must not grow by more than that in all three.
    warm_rows = tiles.limit(64 * PARTS).repartition(64 * PARTS)
    assert _count(spark8, warm_rows, "L3", "engine-c1") == (64 * PARTS, 64 * PARTS)
    for _ in range(3):
        assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
    states = [_worker_state(spark8)]
    for _ in range(3):
        for _ in range(4):
            assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
        states.append(_worker_state(spark8))
    assert states and all(left == 0 for st in states for left, _ in st.values())
    if _provided_session():
        pytest.skip(
            "the RSS half sums a local[8] session's Python workers; on a Databricks node the provided session is "
            "shared by the node's shards, so other work grows and replaces the workers it reads (no /vsimem file left)"
        )
    steady = set.intersection(*(set(st) for st in states))
    assert steady, "no worker is in every snapshot"
    grown = [
        sum(after[pid][1] - before[pid][1] for pid in steady) // 1024
        for before, after in zip(states, states[1:])
    ]
    assert (
        min(grown) <= 100
    ), f"the workers' total RSS grew {grown} MiB over three windows of 4,000 tiles"
