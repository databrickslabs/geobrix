"""The engine leaves nothing behind: 1,000-tile passes on local[8] leave /vsimem/gbx-spec empty in every worker,
and once the workers are warm more passes do not grow their memory; the JVM engine on eight threads leaves its /vsimem empty and does not
crash. Needs the payload's osgeo (the container with GBX_NATIVE_ROOT); the JVM row also the staged JAR on the
payload's GDAL.
"""

import os

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.manifest import TILE_SQL

N, PARTS = 1000, 8


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
    # A worker's first real engine batch loads GDAL's drivers and the light tier (about 90 MiB, once), and a batch
    # holds its rows' buffers while it runs (up to about 30 MiB): three passes warm every worker up; five more must
    # not raise the workers' peak beyond that. A leak of an input (about 256 KiB a call here) would add some
    # 150 MiB over them.
    for _ in range(3):
        assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
    warm = _worker_state(spark8)
    for _ in range(5):
        assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
    later = _worker_state(spark8)
    states = list(warm.values()) + list(later.values())
    assert states and all(left == 0 for left, _ in states)
    grown = (
        max(r for _, r in later.values()) - max(r for _, r in warm.values())
    ) // 1024
    assert grown <= 40, f"the workers' peak RSS grew {grown} MiB over 5,000 more tiles"


@pytest.mark.integration
def test_the_jvm_engine_on_eight_threads_leaves_no_vsimem_file(spark8, tiles):
    from databricks.labs.gbx.manager import capabilities

    caps = capabilities.detect(spark8, need_jvm=True)
    if caps.jvm_gdal != manifest.load().gdal.pin:
        pytest.skip(
            f"PRECONDITION MISSING: the JVM runs GDAL {caps.jvm_gdal or caps.jar_reason}, not "
            f"{manifest.load().gdal.pin}"
        )
    for _ in range(2):
        assert _count(spark8, tiles, "L1", "engine") == (N, N)
    left = spark8._jvm.org.gdal.gdal.gdal.ReadDirRecursive("/vsimem/gbx-spec")
    assert left is None or left.size() == 0
