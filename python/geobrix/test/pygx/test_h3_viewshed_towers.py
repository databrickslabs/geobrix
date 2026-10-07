"""Unit tests for pygx ``h3_viewshed_towers`` — distributed per-tower H3 viewshed.

Pins that the DataFrame wrapper reproduces the shipped ``h3_los_visible``
line-of-sight exactly, groups one row per (tower, visible cell), and runs on a
plain ``local[2]`` Spark session (the ``mapInPandas`` fan-out is the single code
path — no ``.rdd`` / ``sparkContext``, so the same path runs on Serverless).

H3 cell ids flow as ``BIGINT`` (the integer H3 representation) in every column,
matching the Databricks product H3 convention; ``_i`` converts the ``h3`` string
cells used to build the fixtures to that integer form.

Run:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_viewshed_towers.py
"""

import pytest

h3 = pytest.importorskip("h3")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.pygx import h3_los_visible, h3_viewshed_towers  # noqa: E402

VRES = 12


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("vst").getOrCreate()


def _i(cell):
    """H3 string cell -> integer (BIGINT) id, matching the long-typed columns."""
    return int(cell, 16)


def _ring(center, k):
    return list(h3.grid_disk(center, k))


def test_matches_h3_los_visible_per_tower(spark):
    tower = h3.latlng_to_cell(37.77, -122.45, VRES)
    cells = _ring(tower, 6)
    # flat ground at 0; surface flat at 10 except one tall blocker near the tower
    blocker = h3.grid_disk(tower, 1)[1]
    surface = {c: (100.0 if c == blocker else 10.0) for c in cells}
    ground = {c: 0.0 for c in cells}
    observer_z = 20.0  # tower-top

    expected = h3_los_visible(
        tower, observer_z, cells, surface, ground, target_height=1.6
    )

    towers_df = spark.createDataFrame(
        [(_i(tower), observer_z)], "tower_cellid long, observer_z double"
    )
    surface_df = spark.createDataFrame(
        [(_i(c), surface[c]) for c in cells], "cellid long, z double"
    )
    ground_df = spark.createDataFrame(
        [(_i(c), ground[c]) for c in cells], "cellid long, z double"
    )

    out = h3_viewshed_towers(
        towers_df,
        surface_df,
        ground_df,
        radius_m=2500,
        viewshed_res=VRES,
        target_height=1.6,
        num_partitions=1,
    )
    got = {r["cellid"] for r in out.where(f"tower_cellid = {_i(tower)}").collect()}
    # The wrapper reproduces the exact h3_los_visible result, in BIGINT space.
    assert got == {_i(c) for c in expected}
    # The 100 m blocker occludes the cells behind it, so not every cell is
    # visible — but the blocker cell itself is adjacent to the tower and visible.
    assert got != {_i(c) for c in cells}
    assert _i(blocker) in got


def test_two_towers_grouped(spark):
    t1 = h3.latlng_to_cell(37.77, -122.45, VRES)
    t2 = h3.latlng_to_cell(37.78, -122.44, VRES)
    cells = list(set(_ring(t1, 4) + _ring(t2, 4)))
    towers_df = spark.createDataFrame(
        [(_i(t1), 20.0), (_i(t2), 20.0)], "tower_cellid long, observer_z double"
    )
    sdf = spark.createDataFrame([(_i(c), 10.0) for c in cells], "cellid long, z double")
    gdf = spark.createDataFrame([(_i(c), 0.0) for c in cells], "cellid long, z double")
    out = h3_viewshed_towers(
        towers_df, sdf, gdf, radius_m=2500, viewshed_res=VRES, num_partitions=2
    )
    towers = {
        r["tower_cellid"] for r in out.select("tower_cellid").distinct().collect()
    }
    assert towers == {_i(t1), _i(t2)}


def test_empty_towers_returns_empty(spark):
    cells = _ring(h3.latlng_to_cell(37.77, -122.45, VRES), 2)
    sdf = spark.createDataFrame([(_i(c), 10.0) for c in cells], "cellid long, z double")
    gdf = spark.createDataFrame([(_i(c), 0.0) for c in cells], "cellid long, z double")
    empty = spark.createDataFrame([], "tower_cellid long, observer_z double")
    out = h3_viewshed_towers(
        empty, sdf, gdf, radius_m=500, viewshed_res=VRES, num_partitions=1
    )
    assert out.count() == 0
