"""Tests for run_raster_decode_read_sweep (Task C: raster decode-read throughput leg).

Covers:
  - managed/external: monkeypatched read_file_table returns tiles → "ok"
  - managed/external: ValueError from read_file_table → na_by_design rows
  - skip_ordering=None (default): two rows emitted for managed/external
    (spark-path + spark-path-skip-order)
  - skip_ordering=False: single row with mode="spark-path"
  - skip_ordering=True: single row with mode="spark-path-skip-order"
  - fuse mode: function gracefully returns 1-element list (error or ok)
  - per_tile_avg_s = iter_median_s / n_tasks (not / tile count)
  - all result rows are ResultRow instances with fn="gtiff_decode_read"

Monkeypatch strategy:
  - read_file_table: patch the SOURCE module (file_table.read_file_table) so
    the deferred ``from ... import read_file_table`` inside the leg picks it up.
  - rst_avg: patch functions.rst_avg so the UDF is replaced by F.lit(1.0),
    making .select(rst_avg(col("tile"))).count() work on any DataFrame.
  - measure_parallelism / time_iters: patch the bench.readers module attributes.
"""

from __future__ import annotations

import pyspark.sql.functions as _F


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


def _patch_common(monkeypatch, spark, n, ms=200.0):
    """Apply the three monkeypatches shared by all managed/external tests."""
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.measure_parallelism",
        lambda _s, _df: (2, 4),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.time_iters",
        lambda fn, w, m: _mock_stats(w, m, ms),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda _spark, _table, *, skip_ordering=False: _tile_df(spark, n),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.functions.rst_avg",
        lambda col: _F.lit(1.0),
    )


# ---------------------------------------------------------------------------
# managed / external -- row counts and mode labels
# ---------------------------------------------------------------------------


def test_managed_default_emits_two_rows(spark, monkeypatch):
    """managed mode with skip_ordering=None: emits 2 rows (auto-order + skip-order)."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    _patch_common(monkeypatch, spark, n=6)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_managed_order",
        "t",
        0,
        1,
        file_mode="managed",
        skip_ordering=None,
        where="venv",
    )
    assert len(rows) == 2, f"Expected 2 rows (auto + skip), got {len(rows)}"
    modes = {r.mode for r in rows}
    assert "spark-path" in modes, "Missing auto-order row (mode='spark-path')"
    assert (
        "spark-path-skip-order" in modes
    ), "Missing skip-order row (mode='spark-path-skip-order')"
    for r in rows:
        assert r.fn == "gtiff_decode_read"
        assert r.category == "reader"
        assert r.file_mode == "managed"
        assert r.status == "ok"


def test_external_skip_ordering_false_emits_one_row(spark, monkeypatch):
    """external mode with skip_ordering=False: exactly 1 row, mode='spark-path'."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    _patch_common(monkeypatch, spark, n=4)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        skip_ordering=False,
        where="venv",
    )
    assert len(rows) == 1
    assert rows[0].mode == "spark-path"
    assert rows[0].file_mode == "external"
    assert rows[0].status == "ok"


def test_external_skip_ordering_true_emits_one_row(spark, monkeypatch):
    """external mode with skip_ordering=True: exactly 1 row, mode='spark-path-skip-order'."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    _patch_common(monkeypatch, spark, n=4)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        skip_ordering=True,
        where="venv",
    )
    assert len(rows) == 1
    assert rows[0].mode == "spark-path-skip-order"
    assert rows[0].file_mode == "external"
    assert rows[0].status == "ok"


def test_managed_default_emits_auto_and_skip_ordering_variants(spark, monkeypatch):
    """skip_ordering=None (default) emits exactly one row per mode label."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    _patch_common(monkeypatch, spark, n=3)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_managed_order",
        "t",
        0,
        1,
        file_mode="managed",
        where="venv",
    )
    mode_labels = [r.mode for r in rows]
    assert mode_labels.count("spark-path") == 1
    assert mode_labels.count("spark-path-skip-order") == 1


# ---------------------------------------------------------------------------
# na_by_design paths
# ---------------------------------------------------------------------------


def test_managed_value_error_yields_na_by_design_rows(spark, monkeypatch):
    """ValueError from read_file_table (FILE tier unavailable) → na_by_design rows."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            ValueError("FILE tier unavailable on FUSE-only tier")
        ),
    )

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_managed_order",
        "t",
        0,
        1,
        file_mode="managed",
        skip_ordering=None,  # two variants requested → both should be na_by_design
        where="venv",
    )
    # skip_ordering=None → 2 variants; both should be na_by_design.
    assert len(rows) == 2
    for r in rows:
        assert r.status == "na_by_design", f"Expected na_by_design, got {r.status!r}"
        assert r.file_mode == "managed"


def test_external_value_error_single_variant_na(spark, monkeypatch):
    """ValueError with skip_ordering=False (one variant) → exactly 1 na_by_design row."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            ValueError("FILE tier unavailable on FUSE-only tier")
        ),
    )

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        skip_ordering=False,
        where="venv",
    )
    assert len(rows) == 1
    assert rows[0].status == "na_by_design"
    assert rows[0].file_mode == "external"


def test_external_value_error_two_variants_both_na(spark, monkeypatch):
    """ValueError on probe with skip_ordering=None → 2 na_by_design rows."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            ValueError("FILE tier unavailable on FUSE-only tier")
        ),
    )

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        skip_ordering=None,
        where="venv",
    )
    assert len(rows) == 2
    for r in rows:
        assert r.status == "na_by_design"


# ---------------------------------------------------------------------------
# per_tile_avg_s computation
# ---------------------------------------------------------------------------


def test_per_tile_avg_uses_n_tasks_not_n_tiles(spark, monkeypatch):
    """per_tile_avg_s = iter_median_s / n_tasks (input_partitions), not / tile count.

    With n_tiles=100 tiles, n_tasks=4 input partitions, and iter_median_ms=400:
    expected per_tile_avg_s = 0.4 / 4 = 0.1s, NOT 0.4 / 100 = 0.004s.
    """
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    n_tiles = 100
    parts = 4
    ms = 400.0

    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.measure_parallelism",
        lambda _s, _df: (parts, 8),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.readers.time_iters",
        lambda fn, w, m: _mock_stats(w, m, ms),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda _spark, _table, *, skip_ordering=False: _tile_df(spark, n_tiles),
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.functions.rst_avg",
        lambda col: _F.lit(1.0),
    )

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        skip_ordering=False,
        where="venv",
    )
    assert len(rows) == 1
    r = rows[0]
    expected = ms / parts / 1000.0
    assert (
        abs(r.per_tile_avg_s - expected) < 1e-9
    ), f"per_tile_avg_s={r.per_tile_avg_s!r} != expected {expected!r}"


# ---------------------------------------------------------------------------
# ResultRow shape
# ---------------------------------------------------------------------------


def test_result_row_has_correct_shape(spark, monkeypatch):
    """All returned objects are ResultRow dataclass instances with expected fields."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep
    from databricks.labs.gbx.bench.results import ResultRow

    _patch_common(monkeypatch, spark, n=2)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_managed_order",
        "t",
        0,
        1,
        file_mode="managed",
        skip_ordering=False,
        where="venv",
    )
    assert len(rows) == 1
    r = rows[0]
    assert isinstance(r, ResultRow)
    assert r.run_id == "t"
    assert r.api == "lightweight"
    assert r.fn == "gtiff_decode_read"
    assert r.category == "reader"
    assert r.file_mode == "managed"
    assert r.mode == "spark-path"


def test_all_rows_are_result_rows(spark, monkeypatch):
    """The two-row ordering comparison: both rows are valid ResultRow instances."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep
    from databricks.labs.gbx.bench.results import ResultRow

    _patch_common(monkeypatch, spark, n=5)

    rows = run_raster_decode_read_sweep(
        spark,
        "cat.sch.bench_layout_gtiff_external_order",
        "t",
        0,
        1,
        file_mode="external",
        where="venv",
    )
    assert len(rows) == 2
    for r in rows:
        assert isinstance(r, ResultRow)
        assert r.fn == "gtiff_decode_read"


# ---------------------------------------------------------------------------
# fuse mode -- returns a list (error row expected on local[2] w/o raster corpus)
# ---------------------------------------------------------------------------


def test_fuse_mode_returns_list_of_one(spark):
    """fuse mode returns a 1-element list (raster_gbx not registered → error row)."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    rows = run_raster_decode_read_sweep(
        spark,
        "/tmp/nonexistent_corpus",
        "t",
        0,
        1,
        file_mode="fuse",
        where="venv",
    )
    # Must always return a List (never raise).
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].fn == "gtiff_decode_read"
    assert rows[0].file_mode == "fuse"
    # On local[2] without raster_gbx registered, we expect an error row.
    assert rows[0].status in ("ok", "error", "empty")


def test_fuse_mode_skip_ordering_param_accepted(spark):
    """fuse mode accepts skip_ordering param in all forms without raising."""
    from databricks.labs.gbx.bench.readers import run_raster_decode_read_sweep

    for skip_val in (True, False, None):
        rows = run_raster_decode_read_sweep(
            spark,
            "/tmp/nonexistent_corpus",
            "t",
            0,
            1,
            file_mode="fuse",
            skip_ordering=skip_val,
            where="venv",
        )
        assert isinstance(rows, list)
        assert (
            len(rows) == 1
        ), f"fuse mode must return 1 row for skip_ordering={skip_val!r}"
