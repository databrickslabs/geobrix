"""Tests for run_raster_decode_read_sweep (the raster decode-read throughput leg).

read_file_table, rst_avg, measure_parallelism and time_iters are patched, so the leg's own row logic is what runs:
which mode rows a (file_mode, skip_ordering) pair emits, the na_by_design rows when the FILE tier is unavailable, and
per_tile_avg_s dividing by the task count.
"""

from __future__ import annotations

import pyspark.sql.functions as _F
import pytest


def _tile_df(spark, n: int):
    """Return an n-row DataFrame with a nullable binary 'tile' column.

    Used by the mocked read_file_table; the rst_avg mock replaces the UDF with
    F.lit(1.0) so the actual column value is irrelevant.
    """
    return spark.range(n).select(
        _F.lit(None).cast("binary").alias("tile"),
    )


def _mock_stats(w, m, ms=200.0):
    return {
        "iter_median_ms": ms,
        "iter_min_ms": ms * 0.9,
        "iter_p90_ms": ms * 1.1,
        "iter_total_wall_clock_ms": ms * 2,
        "avg_wall_clock_ms": ms,
        "warmup_iters": w,
        "measured_iters": m,
    }


def _tile_df(spark, n: int):
    """Return an n-row DataFrame with a nullable binary 'tile' column.

    Used by the mocked read_file_table; the rst_avg mock replaces the UDF with
    F.lit(1.0) so the actual column value is irrelevant.
    """
    return spark.range(n).select(
        _F.lit(None).cast("binary").alias("tile"),
    )


def _mock_stats(w, m, ms=200.0):
    return {
        "iter_median_ms": ms,
        "iter_min_ms": ms * 0.9,
        "iter_p90_ms": ms * 1.1,
        "iter_total_wall_clock_ms": ms * 2,
        "avg_wall_clock_ms": ms,
        "warmup_iters": w,
        "measured_iters": m,
    }


def _patch_common(monkeypatch, spark, n, ms=200.0, parts=2):
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.measure_parallelism",
        lambda _s, _df: (parts, 4),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.time_iters",
        lambda fn, w, m: _mock_stats(w, m, ms),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.rasterx.file_table.read_file_table",
        lambda _spark, _table, *, skip_ordering=False: _tile_df(spark, n),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.rasterx.functions.rst_avg",
        lambda col: _F.lit(1.0),
    )


def _sweep(spark, file_mode, **kw):
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    return run_raster_decode_read_sweep(
        spark,
        f"cat.sch.bench_layout_gtiff_{file_mode}_order",
        "t",
        0,
        1,
        file_mode=file_mode,
        where="venv",
        **kw,
    )


_BOTH = ["spark-path", "spark-path-skip-order"]


@pytest.mark.parametrize(
    "file_mode,skip_ordering,modes",
    [
        ("managed", None, _BOTH),
        ("external", False, ["spark-path"]),
        ("external", True, ["spark-path-skip-order"]),
    ],
)
def test_mode_rows_per_skip_ordering(spark, monkeypatch, file_mode, skip_ordering, modes):
    """skip_ordering=None emits the auto and skip-order rows; True/False emit the one."""
    _patch_common(monkeypatch, spark, n=4)
    rows = _sweep(spark, file_mode, skip_ordering=skip_ordering)
    assert sorted(r.mode for r in rows) == sorted(modes)
    for r in rows:
        assert (r.fn, r.category, r.file_mode, r.status, r.api) == (
            "gtiff_decode_read",
            "reader",
            file_mode,
            "ok",
            "lightweight",
        )


@pytest.mark.parametrize(
    "file_mode,skip_ordering,n_rows",
    [("managed", None, 2), ("external", False, 1), ("external", None, 2)],
)
def test_file_tier_unavailable_is_na_by_design(
    spark, monkeypatch, file_mode, skip_ordering, n_rows
):
    """A ValueError from read_file_table (FILE tier unavailable) is na_by_design, one row per variant."""
    monkeypatch.setattr(
        "databricks.labs.gbx.rasterx.file_table.read_file_table",
        lambda *_a, **_kw: (_ for _ in ()).throw(ValueError("FILE tier unavailable")),
    )
    rows = _sweep(spark, file_mode, skip_ordering=skip_ordering)
    assert len(rows) == n_rows
    assert {(r.status, r.file_mode) for r in rows} == {("na_by_design", file_mode)}


def test_per_tile_avg_uses_n_tasks_not_n_tiles(spark, monkeypatch):
    """per_tile_avg_s = iter_median_s / n_tasks, not / tile count: 100 tiles, 4 tasks, 400 ms -> 0.1 s."""
    _patch_common(monkeypatch, spark, n=100, ms=400.0, parts=4)
    (r,) = _sweep(spark, "external", skip_ordering=False)
    assert abs(r.per_tile_avg_s - 0.1) < 1e-9, r.per_tile_avg_s
