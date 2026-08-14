# Reader Listing Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the ~28-minute `partitions()` rake on large tile counts by (1) adding `manifest`/`tilesTable` options that bypass `os.walk` + per-file header opens entirely, and (2) deferring the per-file `rasterio.open` header read to the executor for the common passthrough-virtual-tile case.

**Architecture:** Two independent fixes in `ds/raster.py::RasterGbxReader`. Approach 1: new `.option("manifest", <path>)` and `.option("tilesTable", <name>)` read pre-computed tile rows from a JSON/Parquet file or a Spark table on the driver (a single `spark.table(...).collect()` call), then build `_TilePartition`s directly without `os.walk` or per-file `rasterio.open` when dims/window are present. Approach 3: for the common `virtualTiles=true`, no-split, no-AOI case, remove the `rasterio.open` call from `_plan_partitions_for_file` (emit `window=None`) and resolve the window lazily in `read()` when the executor opens the file. All three readers (`raster_gbx`/`gtiff_gbx`/`cog_gbx`) share `partitions()` via the base class and benefit from both fixes. The tile struct and emitted schema are unchanged.

**Tech Stack:** Python 3.12, PySpark DataSource V2, rasterio, numpy, pytest + monkeypatch. No shapely in tests — build rasters via `rasterio.open(..., "w", ...)`.

**Spec:** `docs/superpowers/specs/2026-08-14-reader-listing-performance-design.md`

## Global Constraints

- Connect/Serverless-safe: `partitions()` uses only `spark.table(...)` (driver-side metadata query) + file reads; no `sparkContext`/`.rdd`/`_jvm` anywhere.
- Back-compat: `.load(dir)` with no `manifest`/`tilesTable` unchanged; the lazy-planning change is transparent for passthrough and preserves output; split/AOI paths unchanged where they need the header.
- All three readers (`raster_gbx`, `gtiff_gbx`, `cog_gbx`) benefit via shared `partitions()` in the base class.
- Emitted v2 tile struct (`V2_TILE_SCHEMA`) and `(source, tile)` schema unchanged.
- Tests run from `python/geobrix/` via `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/ -q`.
- `flake8` clean. No shapely in tests.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/ds/raster.py` | Modify | Add `_read_manifest_rows`, `_partitions_from_tile_rows`, `_resolved_budget`; wire `manifest`/`tilesTable` into `__init__`/`partitions()`; lazy window in `_plan_partitions_for_file` + `read()` |
| `python/geobrix/test/ds/test_raster_manifest.py` | Create | Unit + integration tests for Approach 1 (manifest + tilesTable paths) |
| `python/geobrix/test/ds/test_raster_lazy_planning.py` | Create | Unit + integration tests for Approach 3 (lazy planning) |
| `docs/docs/readers/raster.mdx` | Modify | Add many-small-files performance note |
| `docs/docs/api/virtual-tiles.mdx` | Modify | Add many-small-files performance note |
| `python/geobrix/src/databricks/labs/gbx/bench/results.py` | Modify | Add `plan_s: float = 0.0` field to `ResultRow` |
| `python/geobrix/src/databricks/labs/gbx/bench/readers.py` | Modify | Add `split_plan_read` param to `run_spark_path_reader`; add `run_virtual_tile_pixel_read` |

---

## Phase 0 — Bench instrumentation + baseline

Add the bench instrumentation first so the Phase 0 baseline measurement can be taken on the **current** reader (before any remedy is applied). All Phase 2 alternative benchmarks run against this baseline.

### Task 1 — Benchmark instrumentation: plan/read split + virtual-tile pixel-read leg

Add `plan_s: float = 0.0` to `ResultRow` (backward-compatible default), a `split_plan_read=False` parameter to `run_spark_path_reader` that times planning separately, and a new `run_virtual_tile_pixel_read` function for the FILE-on vs FILE-off comparative.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/results.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py`

**Interfaces:**
- Produces: `ResultRow.plan_s: float = 0.0` — planning time in seconds (0.0 for rows where not measured)
- Produces: `run_spark_path_reader(..., split_plan_read=False)` — when `True`, populates `plan_s` in emitted `ResultRow`
- Produces: `run_virtual_tile_pixel_read(spark, path, run_id, warmup, measured, *, where, disable_file) -> List[ResultRow]`

- [ ] **Step 1: Write the failing test**

Append to `python/geobrix/test/bench/test_large_raster_bench.py` (or create `python/geobrix/test/ds/test_bench_reader_instrumentation.py`):

```python
"""Tests for plan/read split instrumentation in run_spark_path_reader."""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=64, height=64, epsg=4326):
    data = np.zeros((height, width), dtype="float32")
    profile = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="float32",
        crs=f"EPSG:{epsg}", transform=from_origin(10.0, 50.0, 0.01, 0.01), nodata=-9999.0,
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data, 1)


def test_run_spark_path_reader_plan_s_populated(tmp_path, spark):
    """split_plan_read=True → emitted ResultRow has plan_s > 0."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource
    from databricks.labs.gbx.bench.readers import run_spark_path_reader

    for i in range(3):
        _write_sample(tmp_path / f"r{i}.tif")

    spark.dataSource.register(RasterGbxDataSource)

    rows = run_spark_path_reader(
        spark,
        path=str(tmp_path),
        run_id="test",
        warmup=0,
        measured=1,
        size_mib=-1,
        where="venv",
        split_plan_read=True,
    )

    assert len(rows) >= 1
    ok_rows = [r for r in rows if r.status == "ok"]
    assert ok_rows, f"Expected at least one ok row; got: {rows}"
    assert ok_rows[0].plan_s >= 0.0, "plan_s must be non-negative"
    # plan_s should be populated when split_plan_read=True
    assert ok_rows[0].plan_s > 0.0, (
        "plan_s should be > 0 when split_plan_read=True (planning is not instant)"
    )


def test_run_spark_path_reader_plan_s_default_zero(tmp_path, spark):
    """split_plan_read=False (default) → plan_s is 0.0."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource
    from databricks.labs.gbx.bench.readers import run_spark_path_reader

    _write_sample(tmp_path / "r.tif")
    spark.dataSource.register(RasterGbxDataSource)

    rows = run_spark_path_reader(
        spark,
        path=str(tmp_path),
        run_id="test",
        warmup=0,
        measured=1,
        size_mib=-1,
        where="venv",
    )

    ok_rows = [r for r in rows if r.status == "ok"]
    assert ok_rows
    assert ok_rows[0].plan_s == 0.0


def test_result_row_plan_s_default():
    """ResultRow can be constructed without plan_s (backward compat)."""
    from databricks.labs.gbx.bench.results import ResultRow

    r = ResultRow(
        run_id="x", api="lightweight", fn="f", category="c", mode="spark-path",
        tile_px=0, bands=0, dtype="", srid=0, rows=1, nodata_frac=0.0,
        warmup_iters=0, measured_iters=1, iter_median_s=1.0, iter_min_s=1.0,
        iter_p90_s=1.0, throughput_mpix_s=0.0, throughput_rows_s=1.0,
        peak_rss_mb=0.0, status="ok", note="",
        env_arch="x86_64", env_cpu_model="test", env_cpu_count=1,
        env_os="linux", env_gbx_version="0.5.0", env_gdal_version="3.9",
        env_runtime_version="3.12", env_where="venv",
    )
    assert r.plan_s == 0.0  # default
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_bench_reader_instrumentation.py -q 2>&1 | tail -10
```

Expected: `test_result_row_plan_s_default` fails (field not yet on `ResultRow`); `run_spark_path_reader` tests fail (no `split_plan_read` param).

- [ ] **Step 3: Add `plan_s` to `ResultRow` in `results.py`**

In `python/geobrix/src/databricks/labs/gbx/bench/results.py`, add after `output_disposition: str = "na"` (the last field, around line 72):

```python
    # Planning time in seconds from a split plan/read measurement.
    # 0.0 when not measured (the default; all pre-existing rows stay valid).
    plan_s: float = 0.0
```

- [ ] **Step 4: Add `split_plan_read` param to `run_spark_path_reader` in `readers.py`**

Modify the `run_spark_path_reader` signature (around line 120):

```python
def run_spark_path_reader(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    size_mib: int = 16,
    where: str = "venv",
    split_plan_read: bool = False,
) -> List[ResultRow]:
    """Time the raster_gbx Spark data source over a corpus directory.

    When ``split_plan_read=True``, times ``RasterGbxReader.partitions()``
    separately (driver-side planning) and records the result as ``plan_s`` in the
    emitted ``ResultRow``. The total ``.count()`` iteration time is unchanged.
    This isolates the listing/header-open planning cost from the executor read cost.
    """
```

Inside `run_spark_path_reader`, before the `try: stats = time_iters(...)` block, add:

```python
    # Optional: time planning separately to isolate the listing/header-open cost.
    _plan_s = 0.0
    if split_plan_read:
        import time as _time
        from databricks.labs.gbx.ds.raster import RasterGbxReader

        _plan_start = _time.monotonic()
        try:
            RasterGbxReader({"path": path, "sizeInMB": str(size_mib)}).partitions()
        except Exception:  # noqa: BLE001
            pass  # planning failure handled by the timed _job below
        _plan_s = _time.monotonic() - _plan_start
```

In the success `ResultRow(...)` construction, add `plan_s=_plan_s` before the `**env` expansion:

```python
        out = [
            ResultRow(
                ...
                plan_s=_plan_s,
                **env,
            )
        ]
```

In the error `ResultRow(...)` construction, add `plan_s=0.0` (already the default, but explicit for clarity):

```python
    except Exception as e:  # noqa: BLE001
        out = [
            ResultRow(
                ...
                plan_s=0.0,
                **env,
            )
        ]
```

- [ ] **Step 5: Add `run_virtual_tile_pixel_read` function to `readers.py`**

Add after `run_spark_path_reader` (before `run_format_read`):

```python
def run_virtual_tile_pixel_read(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    where: str = "cluster",
    disable_file: bool = False,
) -> List[ResultRow]:
    """Time a pixel-reading operation over virtual tiles from the light raster reader.

    Loads a directory as virtual tiles (``virtualTiles=true``), then applies a Python
    UDF that calls ``rasterio.open`` + ``ds.read(1)`` on each executor to force actual
    pixel reads. This measures the per-tile I/O cost (FUSE open + band read) separately
    from planning cost.

    ``disable_file=True`` sets ``GBX_DISABLE_FILE=1`` before the run, simulating the
    no-FILE fallback (FUSE reads on every executor open). Compare FILE-on vs FILE-off
    to measure the FILE byte-range read win in the virtual-tile reader path.

    **Cluster-only:** intended for manual at-scale runs on a dedicated cluster (not CI).
    Run the listing comparative first (``run_spark_path_reader`` with ``split_plan_read``),
    then this leg to measure executor-side read cost separately.

    Runbook:
    1. Confirm ``bench-corpus-reader-10k`` exists at
       ``/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k``
       (10,000 tiny 256px / 1-band / float32 tiles; generated separately).
    2. Stage the wheel: ``gbx:data:push-wheel``.
    3. Run FILE-on (no env override)::

         run_virtual_tile_pixel_read(spark, corpus_dir, "file-on", warmup=1, measured=3)

    4. Run FILE-off::

         run_virtual_tile_pixel_read(spark, corpus_dir, "file-off", warmup=1, measured=3,
                                     disable_file=True)

    5. Compare ``iter_median_s`` and ``throughput_rows_s`` between the two result rows.
    """
    import os
    import time as _time

    if disable_file:
        os.environ["GBX_DISABLE_FILE"] = "1"
    else:
        os.environ.pop("GBX_DISABLE_FILE", None)

    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    import pyspark.sql.functions as _F
    from pyspark.sql.types import DoubleType

    @_F.udf(DoubleType())
    def _pixel_mean(path_col, window_col):
        """Force rasterio.open + band1 read on the executor."""
        import rasterio
        from rasterio.windows import Window as _W

        if path_col is None:
            return None
        try:
            win = None
            if window_col is not None:
                win = _W(
                    window_col["col_off"],
                    window_col["row_off"],
                    window_col["width"],
                    window_col["height"],
                )
            with rasterio.open(path_col) as ds:
                data = ds.read(1, window=win) if win else ds.read(1)
            return float(data.mean())
        except Exception:  # noqa: BLE001
            return None

    def _job():
        return (
            spark.read.format("raster_gbx")
            .option("virtualTiles", "true")
            .load(path)
            .select(
                _pixel_mean(_F.col("tile.path"), _F.col("tile.window")).alias("mean")
            )
            .count()
        )

    mode_label = "virtual-pixel-read-no-file" if disable_file else "virtual-pixel-read"
    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        return [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read_pixels",
                category="reader",
                mode=mode_label,
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=int(actual_rows),
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0,
                per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0,
                peak_rss_mb=peak_rss_mb(),
                status="ok",
                note=f"virtual-tile pixel read ({mode_label})",
                output_fingerprint="",
                **env,
            )
        ]
    except Exception as e:  # noqa: BLE001
        return [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read_pixels",
                category="reader",
                mode=mode_label,
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                iter_total_wall_clock_s=0.0,
                avg_wall_clock_s=0.0,
                per_tile_avg_s=0.0,
                per_tile_avg_ms=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=peak_rss_mb(),
                status="error",
                note=str(e)[:500],
                output_fingerprint="",
                **env,
            )
        ]
```

- [ ] **Step 6: Run the instrumentation tests**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_bench_reader_instrumentation.py -v
```

Expected: all three tests pass.

- [ ] **Step 7: Verify existing JSONL round-trip handles new `plan_s` field (backward compat)**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -c "
from databricks.labs.gbx.bench.results import ResultRow, write_jsonl, read_jsonl
import tempfile, os
r = ResultRow(
    run_id='x', api='lightweight', fn='f', category='c', mode='spark-path',
    tile_px=0, bands=0, dtype='', srid=0, rows=1, nodata_frac=0.0,
    warmup_iters=0, measured_iters=1, iter_median_s=1.0, iter_min_s=1.0,
    iter_p90_s=1.0, throughput_mpix_s=0.0, throughput_rows_s=1.0,
    peak_rss_mb=0.0, status='ok', note='',
    env_arch='x86_64', env_cpu_model='test', env_cpu_count=1,
    env_os='linux', env_gbx_version='0.5.0', env_gdal_version='3.9',
    env_runtime_version='3.12', env_where='venv', plan_s=0.042,
)
with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False, mode='w') as f:
    fname = f.name
write_jsonl([r], fname)
rows = read_jsonl(fname)
os.unlink(fname)
assert rows[0].plan_s == 0.042, f'plan_s round-trip failed: {rows[0].plan_s}'
print('OK — plan_s round-trips correctly')
"
```

Expected: `OK — plan_s round-trips correctly`.

- [ ] **Step 8: Flake8 clean**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m flake8 \
    src/databricks/labs/gbx/bench/results.py \
    src/databricks/labs/gbx/bench/readers.py \
    --max-line-length=100
```

Expected: no output.

- [ ] **Step 9: Run all affected test suites for final regression check**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest \
    test/ds/test_raster_manifest.py \
    test/ds/test_raster_lazy_planning.py \
    test/ds/test_bench_reader_instrumentation.py \
    test/ds/test_raster_datasource.py \
    test/ds/test_raster_virtual.py \
    test/ds/test_raster_options.py \
    -q
```

Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/results.py \
        python/geobrix/src/databricks/labs/gbx/bench/readers.py \
        python/geobrix/test/ds/test_bench_reader_instrumentation.py
git commit -m "feat(bench): plan/read timing split + virtual-tile pixel-read leg

ResultRow.plan_s (default 0.0) captures planning cost separately.
run_spark_path_reader(split_plan_read=True) populates it by timing
RasterGbxReader.partitions() before the counted job.
run_virtual_tile_pixel_read times executor-side pixel reads via a UDF,
supports FILE-on vs FILE-off (GBX_DISABLE_FILE) comparison."
```

---

**Phase 0 gate — BASELINE measurement (manual, orchestrator-run)**

Before implementing any listing remedy, record the current planning cost on the 10K-file corpus. This is the baseline all Phase 2 alternatives must beat:

```python
# On the bench cluster, with the current wheel staged (no remedy applied):
results_baseline = run_spark_path_reader(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
    run_id="phase0-baseline",
    warmup=1,
    measured=3,
    size_mib=-1,
    where="cluster",
    split_plan_read=True,
)
# Record plan_s and iter_median_s. All Phase 2 remedies are benchmarked against these values.
```

---

## Phase 1 — FILE-on/off comparison

FILE accelerates the executor-side pixel READ via the existing `_open(file_ref=...)` path. It does **NOT** fix the driver-side listing/planning rake. This phase measures the read-path win in isolation before tackling the rake remedy in Phase 2.

### Task 2 — FILE-on vs FILE-off pixel-read comparison (manual, orchestrator-run)

**Purpose:** Quantify the executor-side FILE benefit on virtual tiles before implementing any listing remedy. FILE-on vs FILE-off isolates whether executor I/O is a bottleneck independent of the planning rake.

> **LATERAL VIEW note:** The `run_virtual_tile_pixel_read` UDF returns a DOUBLE scalar, so `.count()` forces it without LATERAL VIEW. If you extend this leg to any **array-returning function** (e.g. `rst_histogram`, aggregator outputs), use `LATERAL VIEW explode(...)` (SQL) or `df.select(F.explode(...)).count()` (PySpark) to force the Generate op — a bare `.count()` on an unexploded array column can be pruned and will not measure actual pixel computation.

**Steps**

- [ ] **Run FILE-on (default):**

  ```python
  results_on = run_virtual_tile_pixel_read(
      spark,
      path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
      run_id="phase1-file-on",
      warmup=1,
      measured=3,
      where="cluster",
      disable_file=False,
  )
  ```

- [ ] **Run FILE-off (`GBX_DISABLE_FILE=1`):**

  ```python
  results_off = run_virtual_tile_pixel_read(
      spark,
      path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
      run_id="phase1-file-off",
      warmup=1,
      measured=3,
      where="cluster",
      disable_file=True,
  )
  ```

- [ ] **Compare `iter_median_s` and `throughput_rows_s`** between `results_on` and `results_off`. FILE-on should show lower `iter_median_s` (executor-side range-read instead of full FUSE open). The `plan_s` field will be similar in both runs — FILE does not affect the driver-side listing/planning rake.

**Phase 1 gate:** If FILE-on is markedly SLOWER than FILE-off on pixel-reading ops, stop and investigate before Phase 2. A wash or moderate FILE win is acceptable — the primary bottleneck (listing rake) is addressed in Phase 2. Proceed once FILE-on vs FILE-off is documented.

---

### Task 2b — COG multi-window corpus generator (local, TDD)

**Purpose:** Extend `datagen.py` with a `generate_cog_multiwindow_corpus` function and a `--cog-multiwindow` CLI mode. This produces K large COGs plus a `cog_multiwindow_manifest.json` listing M `{"path", "window"}` rows per COG — the same JSON format that `_read_manifest_rows` (Task 3) already parses, so Task 2c can feed it to `run_virtual_tile_pixel_read` via the `manifest` reader option without additional plumbing.

**COG generation rationale:** A narrow `window` out of a large COG lets FILE issue a byte-range request against the COG's internal tile grid; only the IFD header and the tile blocks covering the window are transferred. The FUSE fallback opens and seeks the full file for each window. On whole-file small-tile corpora (Task 2) these costs are a wash; on large-COG-multi-window corpora the byte-range win is expected to be measurable. This is the canonical virtual-tile-by-reference case: one COG, many `(path, window)` references.

**Datagen state today:** `make_tile_bytes()` uses `driver="GTiff"` (plain GeoTIFF, not COG). `generate_corpus()` writes one file per `TileEntry` with no multi-window concept. `Corpus`/`TileEntry` in `manifest.py` do not model multiple windows into one file. There are no `--cog-multiwindow`, `--cog-count`, `--windows-per-cog`, or `--cog-px` CLI flags. **This task adds all of that** rather than calling a pre-existing function.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/datagen.py` — add `generate_cog_multiwindow_corpus` after `validity_gate`; add `--cog-multiwindow` branch in `main()`
- Create: `python/geobrix/test/bench/test_datagen_cog_multiwindow.py`

**Interfaces:**
- Produces: `generate_cog_multiwindow_corpus(out_dir, seed, cog_count, windows_per_cog, cog_px, bands, dtype, srid) -> Path` — writes `out_dir/cogs/cog_{i}.tif` (driver=COG, blockxsize=blockysize=256) + `out_dir/cog_multiwindow_manifest.json`; returns the manifest path
- CLI: `python -m databricks.labs.gbx.bench.datagen --out <dir> --cog-multiwindow [--cog-count K] [--windows-per-cog M] [--cog-px N] [--bands 1] [--dtypes float32] [--srids 4326] [--seed S]`

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/bench/test_datagen_cog_multiwindow.py`:

```python
"""Tests for generate_cog_multiwindow_corpus in bench/datagen.py."""

import json
from collections import defaultdict

import pytest
import rasterio


def test_cog_multiwindow_manifest_shape(tmp_path):
    """Two-COG, three-windows corpus: manifest row count and path grouping are correct."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=42,
        cog_count=2,
        windows_per_cog=3,
        cog_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
    )

    rows = json.loads(manifest_path.read_text())
    assert len(rows) == 6  # 2 COGs × 3 windows

    by_path = defaultdict(list)
    for r in rows:
        by_path[r["path"]].append(tuple(r["window"]))
    assert len(by_path) == 2  # two distinct COG files

    # Windows within each COG are distinct
    for path, windows in by_path.items():
        assert len(windows) == len(set(windows)), f"{path}: duplicate windows"


def test_cog_multiwindow_file_is_valid_cog(tmp_path):
    """Generated COG is internally tiled (blockxsize in profile)."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=7,
        cog_count=1,
        windows_per_cog=2,
        cog_px=256,
        bands=1,
        dtype="float32",
        srid=32618,
    )

    rows = json.loads(manifest_path.read_text())
    cog_path = tmp_path / rows[0]["path"]
    with rasterio.open(cog_path) as ds:
        assert ds.width == 256
        assert ds.count == 1
        # driver="COG" writes internally tiled blocks
        assert ds.profile.get("blockxsize") is not None, "expected tiled COG (blockxsize missing)"


def test_cog_multiwindow_windows_are_within_bounds(tmp_path):
    """Every manifest window fits within [0, 0, cog_px, cog_px]."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    cog_px = 256
    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=99,
        cog_count=1,
        windows_per_cog=5,
        cog_px=cog_px,
        bands=1,
        dtype="uint8",
        srid=4326,
    )

    rows = json.loads(manifest_path.read_text())
    for r in rows:
        off_x, off_y, win_w, win_h = r["window"]
        assert off_x >= 0 and off_y >= 0
        assert off_x + win_w <= cog_px
        assert off_y + win_h <= cog_px
        assert win_w > 0 and win_h > 0
```

Run (expect `ImportError: cannot import name 'generate_cog_multiwindow_corpus'`):

```bash
cd /Users/mjohns/IdeaProjects/geobrix && .venv-pyrx/bin/python -m pytest python/geobrix/test/bench/test_datagen_cog_multiwindow.py -q 2>&1 | head -20
```

- [ ] **Step 2: Implement `generate_cog_multiwindow_corpus`**

Add after `validity_gate` in `python/geobrix/src/databricks/labs/gbx/bench/datagen.py`:

```python
def generate_cog_multiwindow_corpus(
    out_dir,
    seed: int,
    cog_count: int,
    windows_per_cog: int,
    cog_px: int,
    bands: int,
    dtype: str,
    srid: int,
) -> Path:
    """Write K large COGs + a JSON manifest of M (path, window) rows per COG.

    Each COG uses ``driver='COG'`` (internally tiled at 256 px) so Databricks
    FILE can issue byte-range requests against the COG tile grid.  The manifest
    is a JSON array of ``{"path": "cogs/cog_N.tif", "window": [off_x, off_y,
    win_w, win_h]}`` rows — same format as ``_read_manifest_rows`` in ds/raster.py.
    Returns the ``Path`` to ``<out_dir>/cog_multiwindow_manifest.json``.
    """
    import json as _json

    out_dir = Path(out_dir)
    cogs_dir = out_dir / "cogs"
    cogs_dir.mkdir(parents=True, exist_ok=True)

    tile_size = 256  # COG internal block size
    rng = np.random.default_rng(seed)
    manifest_rows = []

    for i in range(cog_count):
        cog_seed = int(rng.integers(0, 2 ** 31))
        tile_bytes = make_tile_bytes(
            tile_px=cog_px,
            bands=bands,
            dtype=dtype,
            srid=srid,
            nodata_frac=0.0,
            seed=cog_seed,
        )
        rel_path = f"cogs/cog_{i}.tif"
        dest = out_dir / rel_path
        # Re-encode plain GTiff → COG (driver="COG" = internally tiled,
        # overview-capable). Memory note: driver=COG uses ~2.8× peak RAM vs
        # rio-cogeo's ~10× — acceptable for the tile sizes used here.
        with rasterio.io.MemoryFile(tile_bytes) as mf:
            with mf.open() as src:
                cog_profile = src.profile.copy()
                cog_profile.update(
                    driver="COG",
                    blockxsize=tile_size,
                    blockysize=tile_size,
                )
                with rasterio.open(str(dest), "w", **cog_profile) as dst:
                    dst.write(src.read())

        # Partition cog_px columns into windows_per_cog equal strips (full height).
        win_w = max(1, cog_px // windows_per_cog)
        for j in range(windows_per_cog):
            off_x = j * win_w
            if off_x >= cog_px:
                break
            actual_w = min(win_w, cog_px - off_x)
            manifest_rows.append(
                {"path": rel_path, "window": [off_x, 0, actual_w, cog_px]}
            )

    manifest_path = out_dir / "cog_multiwindow_manifest.json"
    manifest_path.write_text(_json.dumps(manifest_rows, indent=2))
    return manifest_path
```

- [ ] **Step 3: Add CLI branch in `main()`**

In `datagen.py:main()`, add to the `argparse` block (after existing `ap.add_argument` calls):

```python
ap.add_argument("--cog-multiwindow", action="store_true", default=False)
ap.add_argument("--cog-count", type=int, default=3)
ap.add_argument("--windows-per-cog", type=int, default=10)
ap.add_argument("--cog-px", type=int, default=1024)
```

Then add a branch **before** the `generate_corpus(...)` call:

```python
if a.cog_multiwindow:
    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=a.out,
        seed=a.seed,
        cog_count=a.cog_count,
        windows_per_cog=a.windows_per_cog,
        cog_px=a.cog_px,
        bands=_parse_int_list(a.bands)[0],
        dtype=a.dtypes.split(",")[0],
        srid=_parse_int_list(a.srids)[0],
    )
    print(json.dumps(
        {"manifest": str(manifest_path), "rows": a.cog_count * a.windows_per_cog},
        indent=2,
    ))
    return
```

- [ ] **Step 4: Run tests green**

```bash
cd /Users/mjohns/IdeaProjects/geobrix && .venv-pyrx/bin/python -m pytest python/geobrix/test/bench/test_datagen_cog_multiwindow.py -q
```

All three tests should pass. If `blockxsize` is absent from the COG profile (rasterio version quirk), check `ds.is_tiled` as an alternative assertion.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/datagen.py \
        python/geobrix/test/bench/test_datagen_cog_multiwindow.py
git commit -m "feat(bench): cog-multiwindow corpus generator for FILE byte-range bench

Adds generate_cog_multiwindow_corpus() + --cog-multiwindow CLI mode.
Writes K large COGs (driver=COG, internally tiled) + a manifest JSON of
M (path,window) rows per COG. Manifest format matches _read_manifest_rows
so the corpus feeds run_virtual_tile_pixel_read in the FILE-capability bench."
```

---

### Task 2c — COG multi-window FILE-capability bench (cluster, manual)

**Purpose:** Measure the FILE byte-range win on the corpus that best exposes it: large COGs + many narrow window references per file. FILE-on vs FILE-off here tests whether byte-range requests against COG tile grids reduce `iter_median_s` relative to full-file FUSE opens. This is the canonical virtual-tile-by-reference scenario.

**Prerequisite:** Task 2b green and committed; bench cluster available.

**Deferred until:** Task 2b is green AND a cluster session is open. Do not attempt on a workstation — the FILE path is a Databricks cluster capability (absent from `.venv-pyrx`).

- [ ] **Generate the corpus on dogfood (oauth-fe, e2-demo-field-eng)**

  ```python
  # Notebook cell on the bench cluster:
  import subprocess, json
  result = subprocess.run([
      "python", "-m", "databricks.labs.gbx.bench.datagen",
      "--out", "/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow",
      "--cog-multiwindow",
      "--cog-count", "5",
      "--windows-per-cog", "200",
      "--cog-px", "4096",
      "--bands", "4",
      "--dtypes", "float32",
      "--srids", "4326",
      "--seed", "2025",
  ], capture_output=True, text=True, check=True)
  print(result.stdout)
  # Expected: {"manifest": ".../cog_multiwindow_manifest.json", "rows": 1000}
  # 5 COGs × 4096×4096 float32 4-band ≈ 256 MB each; 200 windows per COG = 1000 rows.
  # Narrow strips (4096 px tall × ~20 px wide) — FILE byte-ranges ~20-40 KB per window.
  ```

- [ ] **Run FILE-on (default):**

  ```python
  from databricks.labs.gbx.bench.readers import run_virtual_tile_pixel_read

  results_cog_on = run_virtual_tile_pixel_read(
      spark,
      path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow/cog_multiwindow_manifest.json",
      run_id="phase1-cog-file-on",
      warmup=1,
      measured=3,
      where="cluster",
      disable_file=False,
  )
  ```

  > **LATERAL VIEW note (inherited from Task 2):** `run_virtual_tile_pixel_read` returns a DOUBLE scalar; `.count()` forces the UDF without LATERAL VIEW. If you extend this to array-returning ops, use `df.select(F.explode(...)).count()` to force the Generate op.

- [ ] **Run FILE-off:**

  ```python
  results_cog_off = run_virtual_tile_pixel_read(
      spark,
      path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow/cog_multiwindow_manifest.json",
      run_id="phase1-cog-file-off",
      warmup=1,
      measured=3,
      where="cluster",
      disable_file=True,
  )
  ```

- [ ] **Compare `iter_median_s` and `throughput_rows_s`**

  **Expected:** A sharper FILE win than on the Task 2 whole-file corpus. FILE byte-ranges only the COG tile blocks covering each narrow window; FUSE opens and seeks the full ~256 MB file per window. Record the ratio `results_cog_off.iter_median_s / results_cog_on.iter_median_s` alongside the Task 2 ratio.

  **If the win does NOT materialize:** That is a documented finding, not a failure. It may indicate FILE does not yet issue byte-range requests against COG tile grids on this cluster/environment, or that per-window scheduling overhead dominates at these strip widths. Record the numbers and disposition in the Phase 1 comparison table (alongside Task 2 results).

---

## Phase 2 — Listing-rake remedies

Implement each remedy and benchmark it against the Phase 0 baseline. Explore and refine best approaches based on alternative benchmarking.

### Task 3 — Helper functions: `_read_manifest_rows`, `_partitions_from_tile_rows`, `_resolved_budget`

Add three module-level helpers to `ds/raster.py`. No wiring to `__init__`/`partitions()` yet — just the functions with tests. These are the core logic that Task 4 will call.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (add after `_as_window_list`, before `RasterGbxReader`)
- Create: `python/geobrix/test/ds/test_raster_manifest.py`

**Interfaces:**
- Produces: `_read_manifest_rows(path: str) -> list` — returns list of `dict` (JSON) or `Row` (Parquet, via Spark)
- Produces: `_partitions_from_tile_rows(rows, *, emit_virtual, budget_bytes, clip_polygons, clip_crs, windows, tile_size, overlap_percent) -> list[_TilePartition]`
- Produces: `_resolved_budget(size_mib: int, strategy) -> int`

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/ds/test_raster_manifest.py`:

```python
"""Tests for manifest/tilesTable tile-row input helpers (Approach 1)."""

import json

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=4, height=3, epsg=4326):
    """Write a minimal single-band float32 GeoTIFF to *path*."""
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data, 1)


# ---------------------------------------------------------------------------
# _resolved_budget
# ---------------------------------------------------------------------------

def test_resolved_budget_size_mib_wins():
    from databricks.labs.gbx.ds.raster import _resolved_budget
    assert _resolved_budget(size_mib=2, strategy="none") == 2 * 1024 * 1024


def test_resolved_budget_strategy_used_when_no_size_mib():
    from databricks.labs.gbx.ds.raster import _resolved_budget
    # strategy="none" → decoded_budget_bytes returns 0 (no split)
    result = _resolved_budget(size_mib=-1, strategy="none")
    assert result == 0


# ---------------------------------------------------------------------------
# _read_manifest_rows (JSON path only — Parquet needs Spark; tested in Task 4)
# ---------------------------------------------------------------------------

def test_read_manifest_rows_json(tmp_path):
    from databricks.labs.gbx.ds.raster import _read_manifest_rows

    manifest = [
        {"path": "/Volumes/x/a.tif", "window": [0, 0, 256, 256]},
        {"path": "/Volumes/x/b.tif"},
    ]
    manifest_file = str(tmp_path / "tiles.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    rows = _read_manifest_rows(manifest_file)
    assert len(rows) == 2
    assert rows[0]["path"] == "/Volumes/x/a.tif"
    assert rows[0]["window"] == [0, 0, 256, 256]
    assert rows[1].get("window") is None


def test_read_manifest_rows_json_not_found():
    from databricks.labs.gbx.ds.raster import _read_manifest_rows
    with pytest.raises(FileNotFoundError):
        _read_manifest_rows("/nonexistent/manifest.json")


# ---------------------------------------------------------------------------
# _partitions_from_tile_rows
# ---------------------------------------------------------------------------

def test_partitions_from_tile_rows_window_and_dims_no_rasterio_open(tmp_path, monkeypatch):
    """Row with path + window + dims → _TilePartition built without rasterio.open."""
    _write_sample(tmp_path / "a.tif", width=4, height=3)
    _write_sample(tmp_path / "b.tif", width=8, height=6)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(p)
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [
        {"path": str(tmp_path / "a.tif"), "window": [0, 0, 4, 3], "width": 4, "height": 3},
        {"path": str(tmp_path / "b.tif"), "window": [0, 0, 8, 6], "width": 8, "height": 6},
    ]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    assert len(open_calls) == 0, f"rasterio.open called {len(open_calls)} time(s) during planning"
    assert len(parts) == 2
    assert parts[0].file_path == str(tmp_path / "a.tif")
    assert parts[0].window == (0, 0, 4, 3)
    assert parts[0].emit_virtual is True
    assert parts[1].file_path == str(tmp_path / "b.tif")
    assert parts[1].window == (0, 0, 8, 6)


def test_partitions_from_tile_rows_window_only_no_rasterio_open(tmp_path, monkeypatch):
    """Row with path + window but no dims → still no rasterio.open (window is enough)."""
    _write_sample(tmp_path / "a.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(p)
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "a.tif"), "window": [0, 0, 4, 3]}]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    assert len(open_calls) == 0
    assert parts[0].window == (0, 0, 4, 3)


def test_partitions_from_tile_rows_path_only_opens_header(tmp_path, monkeypatch):
    """Row with only path (no window, no dims) → header read for that file only."""
    _write_sample(tmp_path / "a.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "a.tif")}]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    # Header was opened for the listed file only
    assert any(str(tmp_path / "a.tif") in c for c in open_calls), (
        f"Expected rasterio.open for a.tif; calls={open_calls}"
    )
    assert len(parts) >= 1


def test_partitions_from_tile_rows_unlisted_file_never_opened(tmp_path, monkeypatch):
    """A file on disk that is NOT in the manifest is never opened."""
    _write_sample(tmp_path / "in_manifest.tif", width=4, height=3)
    _write_sample(tmp_path / "not_in_manifest.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "in_manifest.tif"), "window": [0, 0, 4, 3]}]
    _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    for c in open_calls:
        assert "not_in_manifest" not in c, (
            f"Unlisted file was opened: {c}"
        )
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_manifest.py -q 2>&1 | head -30
```

Expected: `ImportError` or `AttributeError` — `_read_manifest_rows`, `_partitions_from_tile_rows`, `_resolved_budget` not yet defined.

- [ ] **Step 3: Implement the three helpers in `ds/raster.py`**

Add **immediately before `class RasterGbxReader`** (after `_as_window_list`, around line 682):

```python
# ---------------------------------------------------------------------------
# Budget resolution helper (shared by partitions() and _partitions_from_tile_rows)
# ---------------------------------------------------------------------------

def _resolved_budget(size_mib: int, strategy) -> int:
    """Return the decoded-memory budget in bytes.

    ``size_mib > 0`` is a power-user override that wins over ``strategy``.
    ``size_mib <= 0`` defers to the strategy-derived budget (0 = no split).
    """
    if size_mib > 0:
        return size_mib * 1024 * 1024
    return budget.decoded_budget_bytes(strategy)


# ---------------------------------------------------------------------------
# Manifest / tile-table helpers (Approach 1 — pre-computed tile input)
# ---------------------------------------------------------------------------

def _read_manifest_rows(manifest_path: str) -> list:
    """Read tile rows from a JSON or Parquet manifest file.

    JSON: a list of dicts; each must have ``path`` (str) and optionally
    ``window`` ([col_off, row_off, width, height]), ``width``, ``height``,
    ``bands``, ``dtype``, ``srid``.

    Parquet: read via ``spark.table()`` / ``spark.read.parquet(...)`` on the
    driver (Connect-safe); returns a list of ``pyspark.sql.Row`` objects.
    """
    import os

    local = _listing.to_local_path(manifest_path)
    if not os.path.exists(local):
        raise FileNotFoundError(
            f"raster_gbx: manifest not found: {manifest_path!r}"
        )
    if local.endswith(".parquet"):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "raster_gbx: reading a Parquet manifest requires an active SparkSession."
            )
        return spark.read.parquet(local).collect()
    # JSON (default)
    with open(local) as fh:
        return json.load(fh)


def _row_to_window(w) -> Optional[Tuple[int, int, int, int]]:
    """Normalise a manifest row's ``window`` value to a 4-int tuple or None.

    Accepts: None, a 4-element list/tuple [c, r, w, h], or a Spark Row /
    namedtuple with fields ``col_off``, ``row_off``, ``width``, ``height``.
    """
    if w is None:
        return None
    if isinstance(w, (list, tuple)):
        return tuple(int(v) for v in w)  # type: ignore[return-value]
    # Spark Row / namedtuple
    return (int(w.col_off), int(w.row_off), int(w.width), int(w.height))


def _partitions_from_tile_rows(
    rows,
    *,
    emit_virtual: bool,
    budget_bytes: int,
    clip_polygons: Sequence = (),
    clip_crs: Optional[str] = None,
    windows: Sequence = (),
    tile_size=None,
    overlap_percent: int = 0,
) -> list:
    """Build ``_TilePartition`` objects from pre-computed tile rows.

    Each row is a dict (JSON manifest) or a ``pyspark.sql.Row`` (Parquet /
    table). Required field: ``path``. Optional fields: ``window`` (4-element),
    ``width``, ``height`` (whole-file dims when window absent).

    Decision tree per row:
    - Row has ``window``  → build ``_TilePartition`` directly; NO rasterio.open.
    - Row has ``width`` + ``height`` (no window) → use ``(0, 0, w, h)``; NO open.
    - Row has only ``path`` → call ``_plan_partitions_for_file``; header read
      for that file only (still skips ``os.walk`` over the full directory).
    """
    result: list = []
    for row in rows:
        r: dict = row.asDict() if hasattr(row, "asDict") else dict(row)
        path = str(r.get("path") or "")
        if not path:
            raise ValueError(
                f"raster_gbx manifest/table: row missing 'path' field: {r!r}"
            )

        win = _row_to_window(r.get("window"))
        if win is None:
            # Try whole-file dims as fallback
            w_val = r.get("width")
            h_val = r.get("height")
            if w_val is not None and h_val is not None:
                win = (0, 0, int(w_val), int(h_val))

        if win is not None:
            # Window is known → build partition directly, no header open
            result.append(
                _TilePartition(
                    file_path=path,
                    window=win,
                    is_passthrough=False,
                    is_whole=True,
                    emit_fmt="gtiff",
                    emit_virtual=emit_virtual,
                )
            )
        else:
            # Path only → open header for this specific file
            result.extend(
                _plan_partitions_for_file(
                    file_path=path,
                    budget_bytes=budget_bytes,
                    clip_polygons=clip_polygons,
                    clip_crs=clip_crs,
                    windows=list(windows),
                    tile_size=tile_size,
                    overlap_percent=overlap_percent,
                    emit_virtual=emit_virtual,
                )
            )
    return result
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_manifest.py -q
```

Expected: all 8 tests pass.

- [ ] **Step 5: Flake8 clean**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m flake8 src/databricks/labs/gbx/ds/raster.py --max-line-length=100
```

Expected: no output (clean).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_manifest.py
git commit -m "feat(ds): add _read_manifest_rows/_partitions_from_tile_rows/_resolved_budget helpers

Approach 1 foundations: helpers that will let partitions() skip os.walk
+ per-file rasterio.open when a manifest or tilesTable is provided."
```

---

### Task 4 — Wire `manifest` and `tilesTable` into `RasterGbxReader`

Add `manifest` / `tilesTable` option parsing to `__init__`, update `partitions()` to route through the helpers from Task 3, and add end-to-end integration tests (including tilesTable with Spark).

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`__init__` + `partitions()`)
- Modify: `python/geobrix/test/ds/test_raster_manifest.py` (add integration tests)

**Interfaces:**
- Consumes: `_read_manifest_rows`, `_partitions_from_tile_rows`, `_resolved_budget` (from Task 3)
- Produces: `RasterGbxReader` accepts `.option("manifest", <path>)` and `.option("tilesTable", <name>)`

- [ ] **Step 1: Write failing integration tests** (append to `test_raster_manifest.py`)

```python
# ---------------------------------------------------------------------------
# Integration tests — full partitions() routing (append to test_raster_manifest.py)
# ---------------------------------------------------------------------------

def test_partitions_manifest_json_end_to_end(tmp_path, monkeypatch):
    """RasterGbxReader with manifest= option → correct partitions, 0 rasterio.open."""
    _write_sample(tmp_path / "r0.tif", width=4, height=3)
    _write_sample(tmp_path / "r1.tif", width=8, height=6)

    manifest = [
        {"path": str(tmp_path / "r0.tif"), "window": [0, 0, 4, 3]},
        {"path": str(tmp_path / "r1.tif"), "window": [0, 0, 8, 6]},
    ]
    manifest_file = str(tmp_path / "tiles.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader({
        "path": str(tmp_path),
        "manifest": manifest_file,
        "virtualTiles": "true",
    })
    parts = reader.partitions()

    assert len(open_calls) == 0, f"Expected 0 rasterio.open during partitions(); got {open_calls}"
    assert len(parts) == 2
    assert parts[0].file_path == str(tmp_path / "r0.tif")
    assert parts[0].window == (0, 0, 4, 3)
    assert parts[1].file_path == str(tmp_path / "r1.tif")
    assert parts[1].window == (0, 0, 8, 6)


def test_partitions_manifest_mutual_exclusion_error():
    """Supplying both manifest and tilesTable raises ValueError."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    with pytest.raises(ValueError, match="mutually exclusive"):
        RasterGbxReader({
            "path": "/tmp/x",
            "manifest": "/tmp/m.json",
            "tilesTable": "catalog.schema.tiles",
        })


def test_partitions_tiles_table_end_to_end(tmp_path, spark):
    """RasterGbxReader with tilesTable= option reads tile rows from a Spark temp view."""
    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("path", StringType(), False),
        StructField("col_off", IntegerType(), True),
        StructField("row_off", IntegerType(), True),
        StructField("width", IntegerType(), True),
        StructField("height", IntegerType(), True),
    ])
    # Build a tile-rows table: one row, whole-file window supplied as cols
    tile_data = [(str(tmp_path / "raster.tif"), 0, 0, 4, 3)]
    spark.createDataFrame(tile_data, schema=schema).createOrReplaceTempView(
        "_test_tile_rows_gbx"
    )

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    # tilesTable path: rows have col_off/row_off/width/height as separate columns,
    # not a nested window struct — test that path-only + dims also works.
    reader = RasterGbxReader({
        "path": str(tmp_path),
        "tilesTable": "_test_tile_rows_gbx",
        "virtualTiles": "true",
    })
    parts = reader.partitions()

    assert len(parts) == 1
    assert parts[0].file_path == str(tmp_path / "raster.tif")
    # Dims supplied as cols → whole-file window (0,0,4,3)
    assert parts[0].window == (0, 0, 4, 3)


def test_partitions_manifest_full_spark_read(tmp_path, spark):
    """End-to-end Spark read with manifest option emits correct virtual tiles."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    _write_sample(tmp_path / "raster.tif", width=4, height=3)
    manifest = [{"path": str(tmp_path / "raster.tif"), "window": [0, 0, 4, 3]}]
    manifest_file = str(tmp_path / "manifest.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("manifest", manifest_file)
        .option("virtualTiles", "true")
        .load(str(tmp_path))
        .collect()
    )

    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is None  # virtual: no bytes
    assert tile["path"] == str(tmp_path / "raster.tif")
    assert tile["window"]["width"] == 4
    assert tile["window"]["height"] == 3
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_manifest.py::test_partitions_manifest_json_end_to_end test/ds/test_raster_manifest.py::test_partitions_manifest_mutual_exclusion_error test/ds/test_raster_manifest.py::test_partitions_tiles_table_end_to_end test/ds/test_raster_manifest.py::test_partitions_manifest_full_spark_read -v 2>&1 | tail -20
```

Expected: FAILED — `RasterGbxReader.__init__` does not yet know `manifest`/`tilesTable` options.

- [ ] **Step 3: Add `manifest`/`tilesTable` parsing to `RasterGbxReader.__init__`**

In `RasterGbxReader.__init__`, after the existing `self.emit_virtual` line (line ~723), add:

```python
        # Approach 1 — pre-computed tile input.
        # When manifest or tilesTable is present, partitions() reads tile rows
        # from those sources instead of walking self.path.
        self.manifest = options.get("manifest")
        self.tiles_table = options.get("tilesTable")
        if self.manifest and self.tiles_table:
            raise ValueError(
                "raster_gbx: 'manifest' and 'tilesTable' are mutually exclusive; "
                "supply at most one."
            )
```

- [ ] **Step 4: Extract `_resolved_budget` call and add manifest routing to `partitions()`**

Replace the existing `partitions()` method body with:

```python
    def partitions(self) -> Sequence[InputPartition]:
        resolved_budget = _resolved_budget(self.size_mib, self.strategy)

        # Approach 1: pre-computed tile input bypasses os.walk + per-file header opens.
        if self.manifest or self.tiles_table:
            if self.manifest:
                tile_rows = _read_manifest_rows(self.manifest)
            else:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise RuntimeError(
                        "raster_gbx: 'tilesTable' requires an active SparkSession."
                    )
                tile_rows = spark.table(self.tiles_table).collect()
            return _partitions_from_tile_rows(
                tile_rows,
                emit_virtual=self.emit_virtual,
                budget_bytes=resolved_budget,
                clip_polygons=self.clip_polygons,
                clip_crs=self.clip_crs,
                windows=self.windows,
                tile_size=self.tile_size,
                overlap_percent=self.overlap_percent,
            )

        # Default path: walk self.path and plan partitions per file.
        files = _listing.list_files(self.path, self.filter_regex)
        result: list = []
        for f in files:
            result.extend(
                _plan_partitions_for_file(
                    file_path=f,
                    budget_bytes=resolved_budget,
                    clip_polygons=self.clip_polygons,
                    clip_crs=self.clip_crs,
                    windows=self.windows,
                    tile_size=self.tile_size,
                    overlap_percent=self.overlap_percent,
                    emit_virtual=self.emit_virtual,
                )
            )
        return result
```

Note: the original `partitions()` had the budget resolution inline. This replaces it cleanly.

For the `tilesTable` case with flat columns (like the test above that has `col_off`/`row_off`/`width`/`height` as separate columns), update `_partitions_from_tile_rows` to also handle the flat-column layout. In `_partitions_from_tile_rows`, add between `win = _row_to_window(r.get("window"))` and the `if win is None:` fallback:

```python
        # Also handle flat column layout: col_off/row_off/width/height at top level
        # (common for tilesTable results that store window fields as separate columns).
        if win is None and all(k in r for k in ("col_off", "row_off", "width", "height")):
            win = (int(r["col_off"]), int(r["row_off"]), int(r["width"]), int(r["height"]))
```

- [ ] **Step 5: Run the full test_raster_manifest.py suite**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_manifest.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run existing reader tests to confirm no regression**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_datasource.py test/ds/test_raster_virtual.py test/ds/test_raster_options.py -q
```

Expected: all pass.

- [ ] **Step 7: Flake8 clean**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m flake8 src/databricks/labs/gbx/ds/raster.py --max-line-length=100
```

Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_manifest.py
git commit -m "feat(ds): wire manifest/tilesTable options into RasterGbxReader

Approach 1: .option(\"manifest\", path) reads pre-computed tile rows from
JSON/Parquet; .option(\"tilesTable\", name) reads from a Spark table.
Both bypass os.walk + per-file rasterio.open when window/dims are supplied.
Mutual exclusion validated. All three readers benefit via shared base."
```

---

**Phase 2A gate — manifest/tilesTable benchmark vs Phase 0 baseline (manual, orchestrator-run)**

```python
# Stage the wheel with Tasks 3+4 applied, then on the bench cluster:
results_manifest = run_spark_path_reader(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
    run_id="phase2a-manifest",
    warmup=1,
    measured=3,
    size_mib=-1,
    where="cluster",
    split_plan_read=True,
    # Pass manifest option — path to a pre-generated tile-index JSON for the corpus.
)
```

Compare `plan_s` vs Phase 0 baseline. Manifest/tilesTable should reduce `plan_s` to near-zero (no header opens). Record the win factor before implementing the lazy-planning alternative in Task 5.

---

### Task 5 — Lazy planning for `.load(dir)` passthrough (Approach 3)

Remove the `rasterio.open` call from `_plan_partitions_for_file` for the `emit_virtual=True, no-split, no-AOI` case (the common default). Emit `window=None` and resolve the window lazily in `read()` when the executor opens the file.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`_plan_partitions_for_file` + `read()`)
- Create: `python/geobrix/test/ds/test_raster_lazy_planning.py`

**Interfaces:**
- Consumes: `_TilePartition(window=None, emit_virtual=True)` — reuses the existing null-window slot (already used by the materialized passthrough path with `is_passthrough=True`); the lazy-virtual case is gated by `emit_virtual` dispatch in `read()`, so there is no runtime collision between the two uses of `window=None`.
- Produces: `read()` correctly fills `window` for lazy partitions; `_plan_partitions_for_file` returns `window=None` for the passthrough virtual case

- [ ] **Step 1: Write failing tests**

Create `python/geobrix/test/ds/test_raster_lazy_planning.py`:

```python
"""Tests for Approach 3: lazy planning (no rasterio.open at partitions() for virtual passthrough)."""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=4, height=3, epsg=4326):
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data, 1)


# ---------------------------------------------------------------------------
# _plan_partitions_for_file: virtual passthrough case should NOT open header
# ---------------------------------------------------------------------------

def test_plan_partitions_virtual_passthrough_no_rasterio_open(tmp_path, monkeypatch):
    """virtualTiles=True, no split, no AOI → rasterio.open NOT called at plan time."""
    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _plan_partitions_for_file

    parts = _plan_partitions_for_file(
        file_path=str(tmp_path / "raster.tif"),
        budget_bytes=0,
        emit_virtual=True,
    )

    assert len(open_calls) == 0, (
        f"rasterio.open called {len(open_calls)} time(s) during planning: {open_calls}"
    )
    assert len(parts) == 1
    assert parts[0].window is None  # lazy: filled in by read()
    assert parts[0].emit_virtual is True
    assert parts[0].is_passthrough is False


def test_plan_partitions_virtual_three_files_zero_opens(tmp_path, monkeypatch):
    """Three files in a directory → zero header opens during partitions()."""
    for i in range(3):
        _write_sample(tmp_path / f"raster_{i}.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader({"path": str(tmp_path), "virtualTiles": "true"})
    parts = reader.partitions()

    assert len(open_calls) == 0, (
        f"Expected 0 rasterio.open during partitions(); got {len(open_calls)}: {open_calls}"
    )
    assert len(parts) == 3
    for p in parts:
        assert p.window is None
        assert p.emit_virtual is True


# ---------------------------------------------------------------------------
# tileSize case must STILL read header at plan (correctness regression guard)
# ---------------------------------------------------------------------------

def test_plan_partitions_tilesize_still_reads_header_at_plan(tmp_path, monkeypatch):
    """tileSize requires dims at plan time → rasterio.open IS called."""
    _write_sample(tmp_path / "raster.tif", width=8, height=6)

    open_calls = []
    real_open = rasterio.open
    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)
    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader({
        "path": str(tmp_path),
        "virtualTiles": "true",
        "tileSize": "4",
    })
    parts = reader.partitions()

    # Header opened for grid-window planning
    assert len(open_calls) >= 1, "Expected rasterio.open for tileSize grid planning"
    # 8×6 with 4×4 tiles: ceil(8/4) × ceil(6/4) = 2 × 2 = 4 partitions
    assert len(parts) == 4
    for p in parts:
        assert p.window is not None  # tileSize fills window at plan


# ---------------------------------------------------------------------------
# End-to-end: read() fills the lazy window; emitted tile has correct dims
# ---------------------------------------------------------------------------

def test_read_fills_lazy_window_via_spark(tmp_path, spark):
    """Virtual passthrough read via Spark → tile.window matches actual raster dims."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .load(str(tmp_path / "raster.tif"))
        .collect()
    )

    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is None, "Virtual tile must have no raster bytes"
    assert tile["window"] is not None, "read() must fill the lazy window"
    assert tile["window"]["col_off"] == 0
    assert tile["window"]["row_off"] == 0
    assert tile["window"]["width"] == 4
    assert tile["window"]["height"] == 3


def test_read_lazy_window_three_files(tmp_path, spark):
    """Three files: each emitted virtual tile has the correct per-file window."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    sizes = [(4, 3), (8, 6), (2, 2)]
    for i, (w, h) in enumerate(sizes):
        _write_sample(tmp_path / f"raster_{i}.tif", width=w, height=h)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .load(str(tmp_path))
        .collect()
    )

    assert len(rows) == 3
    by_path = {r["tile"]["path"]: r["tile"] for r in rows}
    for i, (w, h) in enumerate(sizes):
        key = str(tmp_path / f"raster_{i}.tif")
        tile = by_path[key]
        assert tile["window"]["width"] == w, f"width mismatch for raster_{i}.tif"
        assert tile["window"]["height"] == h, f"height mismatch for raster_{i}.tif"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_lazy_planning.py -q 2>&1 | tail -20
```

Expected: `test_plan_partitions_virtual_passthrough_no_rasterio_open` FAILS (open_calls > 0) — the header is currently opened at plan. The tileSize test PASSES (opens > 0). Spark read tests PASS (window already filled, just from the plan rather than lazily).

- [ ] **Step 3: Modify `_plan_partitions_for_file` — remove header open for virtual passthrough**

In `raster.py`, find the virtual passthrough block (currently lines ~440-457):

```python
    if emit_virtual and not tile_size:
        if clip_polygons:
            return _clip_partitions(
                file_path, clip_polygons, clip_crs, emit_virtual=True
            )

        with rasterio.open(file_path) as ds:
            width, height = ds.width, ds.height
        return [
            _TilePartition(
                file_path=file_path,
                window=(0, 0, width, height),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=True,
            )
        ]
```

Replace with:

```python
    if emit_virtual and not tile_size:
        if clip_polygons:
            return _clip_partitions(
                file_path, clip_polygons, clip_crs, emit_virtual=True
            )

        # Approach 3 — lazy planning: skip the header open at plan time.
        # Reuses the existing null-window slot (window=None already means
        # "passthrough GTiff fast path" for materialized tiles), gated by
        # emit_virtual=True. read() dispatches emit_virtual first, so there
        # is no collision with the materialized passthrough case (is_passthrough=True).
        # This avoids N rasterio.open calls when loading a directory of N files.
        return [
            _TilePartition(
                file_path=file_path,
                window=None,  # filled lazily by read() on the executor
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=True,
            )
        ]
```

- [ ] **Step 4: Modify `read()` — compute lazy window when `window is None` and `emit_virtual`**

In `read()`, find the virtual tile branch (lines ~781-803):

```python
        if getattr(partition, "emit_virtual", False):
            with rasterio.open(partition.file_path) as ds:
                meta = {
                    "sourcePath": partition.file_path,
                    "driver": ds.driver,
                    "format": ("cog" if ds.driver == "COG" else "gtiff"),
                    "width": str(ds.width),
                    "height": str(ds.height),
                    "count": str(ds.count),
                }
            yield (
                source,
                _v2_tile_row(
                    _encode.CELLID_FRESH,
                    None,
                    path=partition.file_path,
                    window=partition.window,
                    clip_polygon=partition.clip_polygon,
                    clip_crs=partition.clip_crs,
                ),
            )
            return
```

Replace with:

```python
        if getattr(partition, "emit_virtual", False):
            with rasterio.open(partition.file_path) as ds:
                meta = {
                    "sourcePath": partition.file_path,
                    "driver": ds.driver,
                    "format": ("cog" if ds.driver == "COG" else "gtiff"),
                    "width": str(ds.width),
                    "height": str(ds.height),
                    "count": str(ds.count),
                }
                # Lazy window (Approach 3): window=None at plan → resolve here
                # from the actual raster dims. Pre-planned windows are used as-is.
                win = (
                    (0, 0, ds.width, ds.height)
                    if partition.window is None
                    else partition.window
                )
            yield (
                source,
                _v2_tile_row(
                    _encode.CELLID_FRESH,
                    None,
                    path=partition.file_path,
                    window=win,
                    metadata=meta,
                    clip_polygon=partition.clip_polygon,
                    clip_crs=partition.clip_crs,
                ),
            )
            return
```

- [ ] **Step 5: Run the lazy planning test suite**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_lazy_planning.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run the full ds test suite for regression**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest test/ds/test_raster_datasource.py test/ds/test_raster_virtual.py test/ds/test_raster_options.py test/ds/test_raster_manifest.py test/ds/test_raster_clip.py test/ds/test_raster_tilesize.py -q
```

Expected: all pass.

- [ ] **Step 7: Flake8 clean**

```bash
cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m flake8 src/databricks/labs/gbx/ds/raster.py --max-line-length=100
```

Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_lazy_planning.py
git commit -m "feat(ds): lazy window planning for virtual-tile passthrough (Approach 3)

Skip rasterio.open at partitions() for the common emit_virtual=True,
no-split, no-AOI case. window=None signals read() to resolve dims lazily
from the executor-side rasterio.open that was already required there.
Over N files this drops planning from N header opens to 0. tileSize/AOI
paths are unchanged (header read at plan preserved for correctness)."
```

---

**Phase 2B gate — lazy-planning benchmark vs Phase 0 baseline (manual, orchestrator-run)**

```python
# Stage the wheel with Task 5 applied, then on the bench cluster:
results_lazy = run_spark_path_reader(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
    run_id="phase2b-lazy",
    warmup=1,
    measured=3,
    size_mib=-1,
    where="cluster",
    split_plan_read=True,
)
```

Compare `plan_s` vs Phase 0 baseline. Lazy planning should reduce `plan_s` to near-zero for the no-tileSize virtual path. Compare also against the Phase 2A manifest result to pick the better approach for the Final gate.

---

## Phase 3 — Docs note

### Task 6 — Docs: many-small-files note + COG multi-window FILE-capability callout

Add a many-small-files performance note to the raster readers page and the Virtual Tiles page. Add a COG multi-window FILE-capability callout — distinct from the many-small-files note — to three pages: `large-rasters.mdx`, `virtual-tiles.mdx`, and `benchmarking.mdx`. Also add a **stock-Spark-reader underscore-file note** to `raster.mdx` (Step 1b): stock readers (`binaryFile` etc.) silently skip `_`-prefixed files via Spark's hidden-file filter — `pathGlobFilter` does NOT bypass it — and some GeoBrix heavy-writer tile names start with `_`; document the driver-side-listing workaround. DOC-ONLY: do not change any reader default (per user — flipping to include `_*` risks ingesting `_SUCCESS`/`_committed_*` metadata). User-facing voice; no internal vocabulary.

**Files:**
- Modify: `docs/docs/readers/raster.mdx`
- Modify: `docs/docs/api/virtual-tiles.mdx`
- Modify: `docs/docs/api/large-rasters.mdx`
- Modify: `docs/docs/api/benchmarking.mdx`

**Interfaces:**
- Consumes: nothing from code; doc-only change.
- Produces: user-facing guidance; grep check passes (`grep -rniE "wave [0-9]|wave-[0-9]" docs/docs/` emits nothing).

- [ ] **Step 1: Add performance note to `docs/docs/readers/raster.mdx`**

After the options table and before the "Split strategy" section, add:

```mdx
:::tip Loading many small files
Reading a directory that contains a large number of small files can be slow:
the reader must visit every file header at planning time to compute tile windows.
For large tile counts, prefer one of:
- **`manifest` option** — supply a JSON or Parquet file listing `path` + `window` for each tile; the reader skips the directory walk and header opens entirely.
  ```python
  spark.read.format("raster_gbx") \
      .option("manifest", "/Volumes/catalog/schema/vol/tiles.json") \
      .option("virtualTiles", "true") \
      .load("/Volumes/catalog/schema/vol/rasters")
  ```
- **`tilesTable` option** — point to a Delta table (e.g. one built by your ingest pipeline) that has a `path` column and optionally `window` / dimension columns.
  ```python
  spark.read.format("raster_gbx") \
      .option("tilesTable", "geospatial.myschema.tile_index") \
      .option("virtualTiles", "true") \
      .load("/")
  ```
- **Fewer, larger COGs** — consolidate small files into Cloud-Optimized GeoTIFFs using the [`cog_gbx` writer](./cog); the reader then opens a small number of large files whose headers are comparatively cheap.
:::
```

- [ ] **Step 1b: Add the stock-Spark-reader underscore-file note to `docs/docs/readers/raster.mdx`**

After the "Loading many small files" tip (Step 1), add a second tip. This documents a
real interop gotcha: stock Spark file readers skip `_`-prefixed files, and some GeoBrix
heavy-writer tiles are named with a leading `_`. This is DOC-ONLY — no reader-default
change. Add:

```mdx
:::tip Reading GeoBrix output with a stock Spark reader
Stock Spark file readers (`spark.read.format("binaryFile")`, `text`, `parquet`, …)
silently skip any file whose name starts with `_` or `.` — Spark's hidden-file filter,
applied during directory listing. Some GeoBrix raster tiles are written with a leading
`_` in their (hashed) name, so a stock reader can miss a large fraction of a directory.
Note that **`.option("pathGlobFilter", "*.tif")` does not override this filter** — the
filter runs before the glob, so `_`-prefixed files stay excluded (an explicit `_*` glob,
explicit `_`-file paths, and `recursiveFileLookup` do not help either).

To read every tile, enumerate the paths on the driver (which does not apply the filter)
and build the DataFrame explicitly — ideal when you only need the file list, e.g. to feed
a VRT builder:

```python
from pathlib import Path
paths = [str(p) for p in Path(output_dir).rglob("*.tif")]
files_df = spark.createDataFrame([(p,) for p in paths], ["path"])
```

GeoBrix's own raster readers (`raster_gbx` / `gtiff_gbx` / `cog_gbx`) list files directly
and are **not** affected — this applies only to stock Spark file readers.
:::
```

- [ ] **Step 2: Add performance note to `docs/docs/api/virtual-tiles.mdx`**

Find the section on loading directories (likely the "Using the light reader" or equivalent section) and add:

```mdx
:::tip Many-file directories
When a directory contains thousands of small tiles, loading it with `.load(dir)`
incurs planning overhead even with virtual tiles (the reader still walks the directory
and opens each header to compute `window` dimensions). Use the `manifest` or
`tilesTable` reader option to supply pre-computed tile paths and windows, reducing
planning to a single file or table read regardless of tile count.
See [Raster Reader performance](../readers/raster#loading-many-small-files).
:::
```

- [ ] **Step 3: Add FILE-capability callout to `docs/docs/api/large-rasters.mdx`**

Find the section discussing large COG files (COG standardization / `cog_gbx` writer) and add, in user-facing voice:

```mdx
:::tip Virtual tiles + large COGs: byte-range reads
When virtual tiles reference **windows** of a large Cloud-Optimized GeoTIFF,
Databricks FILE can fetch only the bytes covering each window's COG tile blocks —
rather than opening and seeking the full file. For large COGs (hundreds of MB),
this is where the virtual-tile model pays off most: many narrow window references
into one large COG are substantially cheaper with FILE than with the FUSE fallback.
See [FILE-on vs FILE-off](../api/benchmarking#file-capability-cog-multiwindow) for measured results.
:::
```

- [ ] **Step 4: Add FILE-capability callout to `docs/docs/api/benchmarking.mdx`**

Add a subsection (or note within the virtual-tile bench section) covering the COG multi-window corpus and how to reproduce the FILE comparison. User-facing voice; GBX_DISABLE_FILE is a public env var:

```mdx
#### FILE byte-range vs FUSE: large-COG multi-window corpus {#file-capability-cog-multiwindow}

The standard bench corpus uses many small whole-file tiles. To measure the
Databricks FILE byte-range benefit directly, use the COG multi-window corpus:
K large Cloud-Optimized GeoTIFFs, each referenced by M narrow window rows.
FILE fetches only the COG tile blocks covering each window; the FUSE fallback
opens and seeks the full file per window.

**Generate the corpus:**

```python
import subprocess
subprocess.run([
    "python", "-m", "databricks.labs.gbx.bench.datagen",
    "--out", "/Volumes/<catalog>/<schema>/<vol>/bench-corpus-cog-multiwindow",
    "--cog-multiwindow", "--cog-count", "5", "--windows-per-cog", "200",
    "--cog-px", "4096",
], check=True)
```

**Run FILE-on vs FILE-off:**

```python
from databricks.labs.gbx.bench.readers import run_virtual_tile_pixel_read

results_on  = run_virtual_tile_pixel_read(spark, path="<manifest_path>",
                run_id="cog-file-on",  disable_file=False, where="cluster", warmup=1, measured=3)
results_off = run_virtual_tile_pixel_read(spark, path="<manifest_path>",
                run_id="cog-file-off", disable_file=True,  where="cluster", warmup=1, measured=3)
```

Set `GBX_DISABLE_FILE=1` in the cluster environment to disable FILE globally
instead of using the `disable_file` parameter.
```

- [ ] **Step 5: Verify no internal vocabulary leaked**

```bash
grep -rniE "wave [0-9]+|wave-[0-9]+" /Users/mjohns/IdeaProjects/geobrix/docs/docs/
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add docs/docs/readers/raster.mdx docs/docs/api/virtual-tiles.mdx \
        docs/docs/api/large-rasters.mdx docs/docs/api/benchmarking.mdx
git commit -m "docs(readers): many-small-files note + COG multi-window FILE callout

Add performance guidance for large tile counts (manifest/tilesTable/fewer
larger COGs). Add FILE byte-range callout to large-rasters, virtual-tiles,
and benchmarking pages for the large-COG multi-window scenario."
```

---

## Final gate — 10K corpus + COG multi-window corpus

Two confirmation runs are required before merging.

**Gate A — listing-rake fix (10K whole-file corpus)**

Re-run the winning Phase 2 remedy on `bench-corpus-reader-10k` to confirm the listing rake is solved at scale.

**Corpus:** `bench-corpus-reader-10k` — 10,000 tiny 256px / 1-band / float32 tiles at `/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k` (generated separately). The 1K corpus is too small to confirm a listing-rake fix; 10K is the minimum gate.

```python
# On the bench cluster, with the winning Phase 2 remedy staged:
results_final = run_spark_path_reader(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k",
    run_id="final-gate-10k",
    warmup=1,
    measured=3,
    size_mib=-1,
    where="cluster",
    split_plan_read=True,
)
# plan_s on 10K should be ≤ 5% of the Phase 0 baseline for the remedy to be confirmed.
# iter_median_s must not regress vs Phase 0 (executor read time unchanged).
```

Gate A is confirmed when `plan_s` on `bench-corpus-reader-10k` shows the expected reduction vs Phase 0.

**Gate B — FILE-capability distinction (COG multi-window corpus)**

Re-run the Task 2c comparison (FILE-on vs FILE-off) on `bench-corpus-cog-multiwindow` after the Phase 2 remedy is staged, to confirm that the listing-rake fix does not affect the executor-side FILE byte-range win.

```python
# After Phase 2 remedy is staged; corpus generated in Task 2c:
results_cog_final_on = run_virtual_tile_pixel_read(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow/cog_multiwindow_manifest.json",
    run_id="final-gate-cog-file-on",
    warmup=1,
    measured=3,
    where="cluster",
    disable_file=False,
)
results_cog_final_off = run_virtual_tile_pixel_read(
    spark,
    path="/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow/cog_multiwindow_manifest.json",
    run_id="final-gate-cog-file-off",
    warmup=1,
    measured=3,
    where="cluster",
    disable_file=True,
)
# iter_median_s(FILE-off) / iter_median_s(FILE-on) should match or exceed the
# Task 2c ratio. A regression here means the Phase 2 change inadvertently altered
# the executor-side read path — investigate before merging.
```

Gate B is confirmed when the FILE-on/off ratio on the COG multi-window corpus is consistent with (or better than) the Task 2c pre-remedy measurement. Only after both Gate A and Gate B are confirmed should the branch be merged and documented.

---

## Self-Review

**1. Spec coverage:**

| Spec requirement | Covered by |
|---|---|
| §3 `.option("manifest", ...)` JSON/Parquet input | Task 3 (`_read_manifest_rows`) + Task 4 (wiring) |
| §3 `.option("tilesTable", ...)` Spark table input | Task 4 (`tilesTable` branch in `partitions()`) |
| §3 dims present → zero header reads at plan | Task 3 (`_partitions_from_tile_rows` with `win is not None`) |
| §3 path-only → header for those files only | Task 3 (`_partitions_from_tile_rows` path-only branch) |
| §3 mutual exclusion validation | Task 4 (`__init__` guard) |
| §4 passthrough virtual → defer header to executor | Task 5 (`_plan_partitions_for_file` lazy branch) |
| §4 tileSize/AOI still read header at plan | Task 5 (regression test confirms unchanged) |
| §5 docs note on raster readers page | Task 6 (raster.mdx) |
| §5 docs note on virtual-tiles page | Task 6 (virtual-tiles.mdx) |
| §6 Connect-safe: no sparkContext/rdd/jvm | Task 4 uses `spark.table()` (driver-side query only) |
| §6 back-compat: `.load(dir)` unchanged | Task 4 default path preserved; Task 5 output identical |
| §7b plan/read timing split in bench | Task 1 (`split_plan_read` param + `plan_s` field) |
| §7b virtual-tile pixel-read leg | Task 1 (`run_virtual_tile_pixel_read`) + Task 2 (execution) |
| §7b FILE-on vs FILE-off (`GBX_DISABLE_FILE`) | Task 2 (`disable_file` param + comparison steps) |
| §7b runbook steps documented | Task 1 (docstring in `run_virtual_tile_pixel_read`) + Task 2 steps |
| COG multi-window corpus generator | Task 2b (`generate_cog_multiwindow_corpus` + `--cog-multiwindow` CLI) |
| COG multi-window FILE bench (cluster) | Task 2c (FILE-on vs FILE-off on `bench-corpus-cog-multiwindow`) |
| FILE-capability callout — large-rasters page | Task 6 Step 3 (`docs/docs/api/large-rasters.mdx`) |
| FILE-capability callout — virtual-tiles page | Task 6 Step 2 + callout addition (`docs/docs/api/virtual-tiles.mdx`) |
| FILE-capability callout — benchmarking page | Task 6 Step 4 (`docs/docs/api/benchmarking.mdx`) |
| Final gate: COG multi-window corpus confirmed | Gate B (`final-gate-cog-file-on/off` ratio consistent with Task 2c) |

No gaps found.

**2. Placeholder scan:**

No "TBD", "TODO", or "similar to Task N" patterns. Every code block contains real implementation. The `run_virtual_tile_pixel_read` function (Task 1 Step 5) includes `iter_total_wall_clock_s` and `avg_wall_clock_s` in both the success and error return paths — verify these are present in the implementation copy.

Task 2b: `generate_cog_multiwindow_corpus` uses real rasterio `MemoryFile` + `driver="COG"` round-trip. The window-strip arithmetic is exact (no placeholder). COG block size is hardcoded at `tile_size = 256` to match typical COG internal tile grids. The test file `test_datagen_cog_multiwindow.py` has three distinct assertion sets — shape, COG profile, window bounds — none delegated to a "similar to" helper. Task 2c bench paths reference the real Volume path produced by Task 2b's CLI. Task 6 Steps 3 and 4 include real MDX blocks (admonition + subsection) with real Python snippets; the `#file-capability-cog-multiwindow` anchor in `large-rasters.mdx` matches the target fragment in `benchmarking.mdx`'s cross-link.

**3. Type consistency:**

- `_resolved_budget(size_mib: int, strategy) -> int` — used as `_resolved_budget(self.size_mib, self.strategy)` in Task 4; `size_mib` is `int` (set in `__init__` as `int(options.get("sizeInMB", "-1"))`). ✓
- `_row_to_window(w) -> Optional[Tuple[int, int, int, int]]` — called in `_partitions_from_tile_rows` with `r.get("window")`. Return type matches `_TilePartition.window: Optional[Tuple[int, int, int, int]]`. ✓
- `_partitions_from_tile_rows(rows, *, emit_virtual, budget_bytes, ...)` — `budget_bytes: int` (from `_resolved_budget`). ✓
- `_TilePartition(window=None, emit_virtual=True)` — `window: Optional[Tuple[int, int, int, int]]` accepts `None`; `window=None` reuses the existing null-window slot, gated by `emit_virtual` in `read()`. ✓
- `read()` lazy window: `win: Tuple[int,int,int,int]` — `(0, 0, ds.width, ds.height)` always 4 ints. ✓
- `_v2_tile_row(..., window=win, metadata=meta, ...)` — `metadata` is required positional arg 5; the replacement code in Task 5 Step 4 computes `meta` inside the `with rasterio.open(...)` block and passes it explicitly. ✓
- `ResultRow.plan_s: float = 0.0` — populated with `_time.monotonic()` difference (float). ✓
- `run_virtual_tile_pixel_read` returns `List[ResultRow]` — matches caller expectation. ✓
