"""Fan-outs that must name a key: a bare ``repartition(N)`` is coalesced by AQE (Serverless in particular),
so the work lands on a few tasks. Each fan-out here hashes on its unique columns and keeps its count.
"""

import numpy as np
import pytest
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile


class _Stop(Exception):
    """Raised by a spy to end a function once its fan-out call has been recorded."""


def _plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def _spy_fan_outs(spark, monkeypatch, stop):
    """Record every ``repartition`` / ``repartitionByRange`` call as ``(name, args)``."""
    calls = []
    cls = type(
        spark.range(1)
    )  # the session's concrete DataFrame class (classic or Connect)
    for name in ("repartition", "repartitionByRange"):
        real = getattr(cls, name)

        def spy(self, *a, _name=name, _real=real, **k):
            calls.append((_name, a))
            if stop:
                raise _Stop()
            return _real(self, *a, **k)

        monkeypatch.setattr(cls, name, spy)
    return calls


def test_lidar_node_download_is_hashed_on_the_node_and_honours_the_count(
    spark, monkeypatch
):
    from databricks.labs.gbx.sample import lidar

    monkeypatch.setattr(lidar, "_make_fetch_nodes", lambda **kw: (lambda it: iter(())))
    rows = [("proj", f"0-0-0-{i}") for i in range(40)]
    out = lidar.LidarDownloader()._download_spark(
        rows, (0.0, 0.0, 1.0, 1.0), None, spark, 6, False, False
    )
    plan = _plan(out)
    assert "hashpartitioning(project#" in plan and "node_key#" in plan
    assert ", 6)" in plan


def test_dense_laz_write_is_hashed_on_group_and_cluster(spark, monkeypatch):
    from databricks.labs.gbx.rasterx import ortho

    pts = np.arange(3, dtype=float)
    rgb = np.zeros(3, dtype=int)
    clusters = [("g", c, pts, pts, pts, rgb, rgb, rgb) for c in range(3)]
    calls = _spy_fan_outs(spark, monkeypatch, stop=True)
    with pytest.raises(_Stop):
        ortho._write_dense_laz_dataset(spark, clusters, "/tmp/unused-dense-laz")
    assert calls == [("repartition", (3, "group", "cluster"))]


def test_combine_by_key_without_a_count_uses_the_merge_width(spark, monkeypatch):
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setattr(helpers, "_merge_partitions", lambda s: 7)
    rows = [
        (f"k{i % 3}", VirtualTile.from_v1(cellid=i, raster=b"x").to_row())
        for i in range(6)
    ]
    df = spark.createDataFrame(
        rows,
        StructType(
            [
                StructField("k", StringType()),
                StructField("tile", V2_TILE_SCHEMA),
            ]
        ),
    )
    plan = _plan(helpers.combine_by_key(df, ["k"], "tile", "avg"))
    assert "hashpartitioning(k#" in plan and ", 7)" in plan
    explicit = _plan(helpers.combine_by_key(df, ["k"], "tile", "avg", num_partitions=5))
    assert ", 5)" in explicit


def test_selfcheck_install_paths_fans_out_over_a_keyed_repartition(spark, monkeypatch):
    from databricks.labs.gbx._native import _selfcheck

    calls = _spy_fan_outs(spark, monkeypatch, stop=False)
    _selfcheck.install_paths(spark)
    assert ("repartition", (4, "id")) in calls
