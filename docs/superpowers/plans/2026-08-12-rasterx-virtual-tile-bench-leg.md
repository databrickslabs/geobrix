# RasterX Lightweight Virtual-Tile Benchmark Leg — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a virtual-tile dimension to the lightweight raster benchmark — time each tile-consuming function on a virtual input tile and measure `rst_fromfile` virtual-tile creation, on the fixed 20-worker cluster — published as a new `lw_virtual_per_tile_s` + `disposition` column, while using the run as a QA pass that root-causes (never papers over) any virtual-tile defect.

**Architecture:** A run-level `--input-tile virtual|materialized` flag threads `cluster.py` → `runner.py`. In `run_spark_path`, the `_to_tile` UDF gains a virtual branch (emits `raster=NULL` + `path`+`window`); timing stays a `noop` write. A separate untimed one-row `.collect()` records output disposition. A creation micro-leg times `rst_fromfile` over the `binaryFile` `path` column. Correctness parity (virtual == materialized) runs in **pure-core** (which already fingerprints) and as Spark-free unit tests. `ResultRow` gains `input_tile` + `output_disposition`.

**Tech Stack:** Python 3.12, PySpark (Spark 4.0.0), rasterio, pytest; Databricks CLI/SDK for the cluster run; the `gbx:*` command palette; Docker dev container for all local test/bench runs.

## Global Constraints

- **Light tier only ⇒ no JAR.** pyrx is pure Python; nothing here needs the Scala JAR. State this in every dispatch.
- **Timing is spark-path only; correctness is pure-core only.** The published `lw_virtual_per_tile_s` comes from spark-path; the virtual==materialized fingerprint gate runs in pure-core (spark-path does not fingerprint scalar/tile ops).
- **Accept defaults — no forced strategy.** Never pass `materialize=`/`virtualize_*` to coerce a function; each function's natural disposition is the signal.
- **No papering over (QA discipline).** Any fingerprint divergence, error, zero-row, or unexpected disposition is a blocking finding: root-cause via systematic-debugging, fix in the feature/pyrx code (not the harness), add a regression test. The virtual `_to_tile` branch and lazy open must **never** silently fall back to materialized-on-error.
- **Key-aligned rows for speedup.** Virtual `ResultRow`s must share the materialized key `(fn, tile_px, bands, dtype, srid, nodata_frac, mode=spark-path, rows)` so a later `speedup = materialized_per_tile_s / virtual_per_tile_s` join is trivial. Do not precompute a ratio in the row.
- **Corpus/scale must match the published materialized baseline** (tile shapes + row-count sweep, per `bench-1000-scale-only-now`) or there are no matching keys to join.
- **User-facing docs voice.** `benchmarking.mdx` must contain no internal vocabulary (no `wave N`, no subagent/dispatch language); passes the QC `internals-leak` check.
- **Fixed 20-worker cluster** `0519-143423-0jwqt79u`, profile `oauth-fe`, autoscale OFF. Verify before any cluster run.
- **All local runs happen in the `geobrix-dev` Docker container** via `gbx:*` commands. Never invoke `docker`/`pytest`/`mvn` ad hoc — fix the `gbx:*` command if it's wrong.
- **Hold pushes for the user's explicit go.** Commit locally per task; do not push.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/bench/results.py` | `ResultRow` schema, JSONL I/O, `summarize` | Add `input_tile` + `output_disposition` fields; surface them in `summarize`; add QA anomalies section |
| `python/geobrix/src/databricks/labs/gbx/bench/spec.py` | `FnSpec` registry | Add `virtual_disposition` field; classify accessors |
| `python/geobrix/src/databricks/labs/gbx/bench/runner.py` | pure-core + spark-path timing | Virtual `_to_tile` branch + virtual `df_all`; `input_tile` param on `run_spark_path` and `run_pure_core`; untimed disposition collect; creation micro-leg; pure-core virtual-input parity |
| `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` | notebook generation / job launch | Thread `input_tile` into `_PREAMBLE` + `run_light` calls |
| `scripts/commands/gbx-bench-cluster.*` (+ the push_and_run script) | `gbx:bench:cluster` entry | Expose `--input-tile` (verify exact path first) |
| `python/geobrix/test/bench/test_virtual_tile_bench.py` (new) | correctness parity unit tests | Spark-free virtual==materialized fingerprint tests |
| `python/geobrix/test/bench/test_results.py` | results tests | New-field roundtrip + backward-compat |
| `python/geobrix/test/bench/test_spec.py` | spec tests | Accessor disposition classification |
| `python/geobrix/test/bench/test_runner.py` | runner tests | Virtual `_to_tile`, disposition, creation micro-leg, pure-core parity |
| `python/geobrix/test/bench/test_cluster.py` | cluster tests | `input_tile` threading into the notebook |
| `docs/docs/api/benchmarking.mdx` | published Raster tab | New `lw_virtual_per_tile_s` + `disposition` columns + framing (post-run) |

**Verified source anchors (from extraction; re-confirm exact lines before editing):**
- `ResultRow` — `results.py:12-66` (frozen dataclass; defaults at the tail: `output_fingerprint`, `per_tile_avg_*`, `run_event_num`, `split_strategy`).
- `run_spark_path(spark, corpus_root, corpus, fnspecs, run_id, row_counts, warmup, measured, where, sink=None, partition_size=0, explain_only=False, explain_dir="")` — `runner.py:1345-1359`; `df_all`/`_to_tile` built once at `runner.py:1395-1435`.
- `_run_sp_scalar_fn(...)` timed `noop` loop — `runner.py:1273-1342`; `_input_col(fn, kind, df=None)` — `runner.py:1466-1475`.
- Aggregator's untimed fingerprint collect (the model for disposition capture) — `runner.py:791-799` / `794-860`.
- `FnSpec` frozen dataclass — `spec.py:196-289`; entries e.g. `rst_width` `spec.py:483-493`, `rst_slope` `spec.py:505-515`, `rst_fromfile` `spec.py:1594-1615` (`modes=("pure-core",)`, `input_kind="path"`).
- Lazy open — `pyrx/core/open_tile.py`: `open_header(tile)` (header-only, virtual→`rasterio.open`+`_WindowHeaderView`, no pixel I/O) `~492-576`; `_open(tile)`/`open_tile(tile)` (materializing/window pixel read); `_to_virtual_tile`, `_stage_local_if_needed`, `_is_full_extent`.
- Column fns — `pyrx/functions.py`: `rst_width` `4356-4357` → `_u_width=_header_accessor_udf(accessors.width, IntegerType())` `355` (built `240-255`, uses `ot.open_header`); `rst_slope` `3984-4033` → `_slope_udf` `3874-3891` (uses `ot._open`, returns `_serde.build_tile(...)` = materialized).
- `rst_fromfile` — `pyrx/functions.py`: wrapper `643-673`, `_fromfile_impl(path, driver, materialize)` `550-617` (virtual branch builds `VirtualTile(cellid=0, raster=None, path=local, window=(0,0,W,H), metadata=…).to_row()`; `to_local_path` normalizes the URI), UDFs `620-640`.
- `VirtualTile` — `pyrx/core/virtual_tile.py:48-112` (`to_row`, `from_row`, `is_virtual`); `V2_TILE_SCHEMA` `34-45`.
- `build_tile(raster_bytes, driver, cellid=0)` / `open_tile(raster_bytes)` — `pyrx/_serde.py:69-87` / `48-53`.
- Bench tests live in `python/geobrix/test/bench/`; run with `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/ --log bench.log` (in Docker).

---

### Task 1: Foundational QA — virtual==materialized correctness parity (Spark-free)

This is the first QA gate: prove the virtual-tile feature produces the same result as a materialized tile, single-tile, no Spark. If it fails, that is a **feature bug** to root-cause and fix (systematic-debugging) before anything else — not a test to weaken.

**Files:**
- Create: `python/geobrix/test/bench/test_virtual_tile_bench.py`
- Reads (no change): `pyrx/core/open_tile.py`, `pyrx/core/terrain.py`, `pyrx/core/accessors.py`, `pyrx/core/virtual_tile.py`, `pyrx/_serde.py`, `bench/datagen.py`, `bench/fingerprint.py`

**Interfaces:**
- Consumes: `datagen.generate_corpus(...)` → `Corpus` with `.size_sweep: list[TileEntry]` (each has `.path`, `.tile_px`, `.role`); `open_tile._open(tile)` (pixel-capable open, accepts bytes or a v2 tile dict/VirtualTile); `open_tile.open_header(tile)` (header-only, no pixel I/O); `_serde.open_tile(raster_bytes)`; `terrain.slope(ds, unit, xscale, yscale)`; `accessors.width(ds)`; `VirtualTile(cellid, raster, path, window, metadata).to_row()`; `fingerprint.fingerprint_output(out)`.
- Produces: nothing consumed by later tasks (pure test), but confirms the exact open entry points the runner tasks reuse.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/bench/test_virtual_tile_bench.py
"""QA: a virtual input tile must produce the SAME result as a materialized one."""
from pathlib import Path

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench.fingerprint import fingerprint_output
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors, open_tile as ot, terrain
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _one_tile(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path, seed=9, tile_px=[64], bands=[1], dtypes=["float32"],
        srids=[4326], nodata_fracs=[0.0], row_rows=1, row_tile_px=64,
        row_bands=1, row_dtype="float32",
    )
    te = next(t for t in corpus.size_sweep if t.role != "bng_gb")
    return Path(tmp_path) / te.path, te


def test_slope_virtual_equals_materialized(tmp_path):
    p, te = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    vt = VirtualTile(cellid=0, raster=None, path=str(p),
                     window=(0, 0, te.tile_px, te.tile_px))
    with ot._open(vt.to_row()) as ds:
        virt = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    assert fingerprint_output(mat) == fingerprint_output(virt)


def test_width_virtual_is_header_only_and_matches(tmp_path):
    p, _ = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat_w = accessors.width(ds)
    vt = VirtualTile(cellid=0, raster=None, path=str(p), window=None)
    with ot.open_header(vt.to_row()) as ds:
        virt_w = accessors.width(ds)
    assert mat_w == virt_w
```

- [ ] **Step 2: Run — verify it fails or passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_virtual_tile_bench.py --log vt-parity.log`
Expected: PASS if the feature is correct. If it FAILS (e.g. `ot._open`/`open_header` name wrong, or fingerprints diverge), that is a real finding: (a) if the open entry name differs, correct it to the actual pixel/header entry in `open_tile.py` and re-run; (b) if fingerprints diverge, STOP and root-cause the virtual-tile read bug via systematic-debugging before continuing the plan.

- [ ] **Step 3: If a divergence was a feature bug, fix at root cause + add the failing tile as a regression case** (in `pyrx` tests), then re-run until green. If it passed first time, note "virtual parity holds for slope+width" and continue.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/bench/test_virtual_tile_bench.py
git commit -m "test(bench): QA gate — virtual tile equals materialized (slope, width)"
```

---

### Task 2: ResultRow gains `input_tile` + `output_disposition`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/results.py` (append two fields to `ResultRow`, tail of the dataclass ~line 66)
- Test: `python/geobrix/test/bench/test_results.py`

**Interfaces:**
- Produces: `ResultRow.input_tile: str = "materialized"`, `ResultRow.output_disposition: str = "na"` — consumed by Tasks 4, 5, 7, 10 and the docs task.

- [ ] **Step 1: Write the failing test** (append to `test_results.py`)

```python
import dataclasses as _dc
import json
from pathlib import Path
from databricks.labs.gbx.bench.results import ResultRow, write_jsonl, read_jsonl


def _row(**over):
    """A minimal valid ResultRow; override any field via kwargs."""
    base = dict(
        run_id="t", api="lightweight", fn="rst_slope", category="terrain",
        mode="spark-path", tile_px=64, bands=1, dtype="float32", srid=4326,
        rows=10, nodata_frac=0.0, warmup_iters=1, measured_iters=2,
        iter_median_s=0.1, iter_min_s=0.09, iter_p90_s=0.11,
        throughput_mpix_s=1.0, throughput_rows_s=1.0, peak_rss_mb=1.0,
        status="ok", note="", env_arch="x86_64", env_cpu_model="m",
        env_cpu_count=4, env_os="linux", env_gbx_version="0.5.0",
        env_gdal_version="3.11", env_runtime_version="18", env_where="venv",
    )
    base.update(over)
    return ResultRow(**base)


def test_resultrow_new_fields_default_and_roundtrip(tmp_path):
    r = _row()
    assert r.input_tile == "materialized"
    assert r.output_disposition == "na"
    r2 = _dc.replace(r, input_tile="virtual", output_disposition="deferred")
    p = tmp_path / "x.jsonl"
    write_jsonl([r2], p)
    back = read_jsonl(p)[0]
    assert back.input_tile == "virtual"
    assert back.output_disposition == "deferred"


def test_resultrow_loads_legacy_row_without_new_fields(tmp_path):
    d = _dc.asdict(_row())
    d.pop("input_tile"); d.pop("output_disposition")
    p = tmp_path / "legacy.jsonl"
    p.write_text(json.dumps(d) + "\n")
    r = read_jsonl(p)[0]
    assert r.input_tile == "materialized"
    assert r.output_disposition == "na"
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_results.py -k "new_fields or legacy" --log res.log`
Expected: FAIL — `ResultRow.__init__` has no `input_tile`/`output_disposition`.

- [ ] **Step 3: Add the fields** (after `split_strategy: Optional[str] = None`, ~results.py:66)

```python
    # Which input tile the row measured: "materialized" (bytes) or "virtual"
    # (path+window, bytes-free). Default preserves every pre-existing row.
    input_tile: str = "materialized"
    # Did the fn materialize pixels on a virtual input: "deferred" (stayed
    # virtual / header-only), "materialized" (read/generated pixels), or "na"
    # (not applicable / not captured). Default "na".
    output_disposition: str = "na"
```

(`read_jsonl` already drops unknown keys and lets dataclass defaults fill missing ones via `_normalize_row`, so legacy rows load unchanged.)

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_results.py -k "new_fields or legacy" --log res.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/results.py python/geobrix/test/bench/test_results.py
git commit -m "feat(bench): ResultRow carries input_tile + output_disposition"
```

---

### Task 3: FnSpec accessor disposition classification

Accessors return scalars (no output tile to sample), so disposition is classified: pixel-reading accessors ⇒ `materialized`, all other accessors (header/metadata) ⇒ `deferred`. The Task-5 run cross-checks this — a "deferred" accessor that unexpectedly reads pixels is a finding.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/spec.py` (add `virtual_disposition` override field to `FnSpec` ~line 289; add module-level classifier)
- Test: `python/geobrix/test/bench/test_spec.py`

**Interfaces:**
- Produces: `FnSpec.virtual_disposition: Optional[str] = None` (per-fn override); `spec.accessor_disposition(name: str, fs: FnSpec | None = None) -> str` returning `"materialized"`/`"deferred"` — consumed by Task 5.

- [ ] **Step 1: Write the failing test** (append to `test_spec.py`)

```python
from databricks.labs.gbx.bench import spec as s


def test_pixel_reading_accessors_are_materialized():
    for fn in ("rst_avg", "rst_min", "rst_max", "rst_median",
               "rst_pixelcount", "rst_summary", "rst_histogram"):
        assert s.accessor_disposition(fn) == "materialized", fn


def test_header_accessors_are_deferred():
    for fn in ("rst_width", "rst_height", "rst_numbands", "rst_srid",
               "rst_pixelwidth", "rst_georeference", "rst_boundingbox"):
        assert s.accessor_disposition(fn) == "deferred", fn


def test_virtual_disposition_override_wins():
    fs = s.REGISTRY["rst_width"]
    fs2 = __import__("dataclasses").replace(fs, virtual_disposition="materialized")
    assert s.accessor_disposition("rst_width", fs2) == "materialized"
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_spec.py -k "disposition" --log spec.log`
Expected: FAIL — no `accessor_disposition` / no `virtual_disposition` field.

- [ ] **Step 3: Add the field + classifier**

Add to `FnSpec` (after `udtf: bool = False`, ~spec.py:289):

```python
    # Optional explicit disposition for a scalar accessor on a virtual input:
    # "deferred" (header-only) or "materialized" (reads pixels). None => derive
    # from the pixel-reading allowlist below. Ignored for tile-returning ops
    # (their disposition is sampled from the output tile at run time).
    virtual_disposition: Optional[str] = None
```

Add at module scope (near the registry):

```python
# Accessors that MUST read pixels (statistics over pixel values); everything
# else in the accessor category reads only the header/metadata via open_header.
# Verified against pyrx: these route through pixel-reading UDFs, the rest through
# _header_accessor_udf (open_header, no pixel I/O).
_PIXEL_READING_ACCESSORS = frozenset({
    "rst_avg", "rst_min", "rst_max", "rst_median",
    "rst_pixelcount", "rst_summary", "rst_histogram",
})


def accessor_disposition(name: str, fs: "FnSpec | None" = None) -> str:
    """Disposition of a scalar accessor on a virtual input tile."""
    if fs is not None and fs.virtual_disposition is not None:
        return fs.virtual_disposition
    return "materialized" if name in _PIXEL_READING_ACCESSORS else "deferred"
```

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_spec.py -k "disposition" --log spec.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/spec.py python/geobrix/test/bench/test_spec.py
git commit -m "feat(bench): classify accessor virtual-tile disposition"
```

---

### Task 4: Virtual `_to_tile` branch + `input_tile` param on `run_spark_path`

Factor the tile-building out of the nested `_to_tile` UDF into a module-level, unit-testable helper, add the virtual branch (dogfooding the real `_fromfile_impl` creation path), and thread `input_tile` into `run_spark_path`. **No silent fallback:** a failed virtual build raises.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/runner.py` (new `_build_input_tile`; `_to_tile` calls it ~1415; `input_tile` param on `run_spark_path` ~1345)
- Test: `python/geobrix/test/bench/test_runner.py`

**Interfaces:**
- Produces: `runner._build_input_tile(path: str, content, cellid: int, input_tile: str) -> dict` (V2 tile row); `run_spark_path(..., input_tile: str = "materialized")` — consumed by Tasks 5, 7, 8.

- [ ] **Step 1: Write the failing tests** (append to `test_runner.py`)

```python
def test_build_input_tile_virtual_is_bytes_free(tmp_path):
    from databricks.labs.gbx.bench import runner as rn, datagen as dg
    corpus = dg.generate_corpus(out_dir=tmp_path, seed=9, tile_px=[64], bands=[1],
        dtypes=["float32"], srids=[4326], nodata_fracs=[0.0], row_rows=1,
        row_tile_px=64, row_bands=1, row_dtype="float32")
    te = next(t for t in corpus.size_sweep if t.role != "bng_gb")
    p = tmp_path / te.path
    content = p.read_bytes()
    mat = rn._build_input_tile(str(p), content, 7, "materialized")
    assert mat["raster"] is not None and mat["cellid"] == 7
    virt = rn._build_input_tile(str(p), content, 7, "virtual")
    assert virt["raster"] is None
    assert virt["path"] and virt["window"] is not None
    assert virt["cellid"] == 7  # corpus cellid preserved (not _fromfile_impl's 0)


def test_build_input_tile_virtual_no_silent_fallback():
    import pytest
    from databricks.labs.gbx.bench import runner as rn
    with pytest.raises(Exception):
        rn._build_input_tile("/nonexistent/path.tif", b"", 0, "virtual")


def test_run_spark_path_accepts_input_tile_kwarg():
    import inspect
    from databricks.labs.gbx.bench import runner as rn
    assert "input_tile" in inspect.signature(rn.run_spark_path).parameters
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "build_input_tile or input_tile_kwarg" --log runner.log`
Expected: FAIL — `_build_input_tile` undefined; `run_spark_path` has no `input_tile`.

- [ ] **Step 3: Add the helper + branch + param**

Module scope in `runner.py` (near `_serde` import):

```python
def _build_input_tile(path, content, cellid, input_tile):
    """Build a V2 tile dict for the spark-path leg.

    materialized -> bytes tile (build_tile). virtual -> bytes-free tile pointing
    at the path over its whole-file window, built via the real rst_fromfile
    creation path (_fromfile_impl), so the leg dogfoods the feature. Raises on a
    virtual build failure -- NEVER silently falls back to materialized.
    """
    if input_tile == "virtual":
        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        row = _fromfile_impl(path, "GTiff", False)
        if row is None:
            raise ValueError(f"virtual tile build returned null for {path!r}")
        row["cellid"] = int(cellid)  # preserve corpus cellid for key alignment
        return row
    return _serde.build_tile(bytes(content), "GTiff", int(cellid))
```

Replace the `_to_tile` body (~runner.py:1416-1421) with:

```python
    @F.udf(returnType=V2_TILE_SCHEMA)
    def _to_tile(path, content):
        import os

        cid = _cellid_by_base.get(os.path.basename(path), 0)
        return _build_input_tile(path, content, cid, input_tile)
```

Add `input_tile: str = "materialized"` to the `run_spark_path` signature (after `explain_dir: str = ""`, ~runner.py:1358).

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "build_input_tile or input_tile_kwarg" --log runner.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/runner.py python/geobrix/test/bench/test_runner.py
git commit -m "feat(bench): virtual input-tile branch + input_tile param"
```

---

### Task 5: Disposition capture + row tagging (untimed)

Classify each fn's disposition and stamp `input_tile`/`output_disposition` on its rows. Accessors classify by name (Task 3); tile-returning ops sample one output tile via a **separate untimed** collect (never inside the timed `noop` loop).

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/runner.py` (add `_disposition_of`; untimed sample + `dataclasses.replace` tagging in the `run_spark_path` fn loop, at the `_mark = len(out)` seam ~1548)
- Test: `python/geobrix/test/bench/test_runner.py`

**Interfaces:**
- Consumes: `spec.accessor_disposition` (Task 3), `ResultRow.input_tile/output_disposition` (Task 2).
- Produces: `runner._disposition_of(fs, sample_out_tile) -> str` — used inside `run_spark_path`.

- [ ] **Step 1: Write the failing test** (append to `test_runner.py`)

```python
def test_disposition_accessor_uses_classifier():
    from databricks.labs.gbx.bench import runner as rn, spec as s
    assert rn._disposition_of(s.REGISTRY["rst_avg"], None) == "materialized"
    assert rn._disposition_of(s.REGISTRY["rst_width"], None) == "deferred"


def test_disposition_tile_returning_from_output_tile():
    from databricks.labs.gbx.bench import runner as rn, spec as s
    assert rn._disposition_of(s.REGISTRY["rst_slope"], {"raster": b"xx"}) == "materialized"
    assert rn._disposition_of(s.REGISTRY["rst_slope"], {"raster": None}) == "deferred"
    # no sample available -> "na" (not a crash)
    assert rn._disposition_of(s.REGISTRY["rst_slope"], None) == "na"
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "disposition" --log runner.log`
Expected: FAIL — `_disposition_of` undefined.

- [ ] **Step 3: Add `_disposition_of` + wire untimed tagging**

Module scope in `runner.py`:

```python
def _disposition_of(fs, sample_out_tile):
    """Disposition for a fn on a virtual input. Accessors: classify by name.
    Tile-returning: inspect the sampled output tile (raster None => deferred).
    None sample => "na" (uncaptured), never a crash."""
    from databricks.labs.gbx.bench.spec import accessor_disposition

    if fs.category == "accessor":
        return accessor_disposition(fs.name, fs)
    if sample_out_tile is None:
        return "na"
    try:
        raster = sample_out_tile["raster"]
    except (KeyError, TypeError, IndexError):
        raster = None
    return "deferred" if raster is None else "materialized"
```

In `run_spark_path`, at the per-fn seam (after `_mark = len(out)` and the fn's rows are appended, ~runner.py:1548+), when `input_tile == "virtual"` tag the new rows. For a tile-returning fn, take ONE untimed sample; guard it so a sampling failure is recorded as a finding, not a crash:

```python
        if input_tile == "virtual":
            _sample = None
            if fs.category != "accessor" and getattr(fs, "input_kind", "tile") == "tile":
                try:
                    _row1 = (
                        df_all.limit(1)
                        .select(fs.col_fn(_input_col(fs.name, "tile", df_all), fs.args).alias("out"))
                        .collect()
                    )
                    _sample = _row1[0]["out"] if _row1 else None
                except Exception as _e:  # noqa: BLE001 — finding, not a silent skip
                    print(f"[bench][QA] disposition sample failed for {fs.name}: {_e}")
            _disp = _disposition_of(fs, _sample)
            for _i in range(_mark, len(out)):
                out[_i] = _dc.replace(out[_i], input_tile="virtual", output_disposition=_disp)
```

(Ensure `import dataclasses as _dc` is present in `runner.py`.)

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "disposition" --log runner.log`
Expected: PASS. (The Spark sampling path is exercised in the Task 11 Docker smoke.)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/runner.py python/geobrix/test/bench/test_runner.py
git commit -m "feat(bench): capture + tag virtual-tile output disposition"
```

---

### Task 6: Pure-core virtual==materialized parity sweep (the QA gate, all tile-input fns)

A Spark-free sweep that, per `input_kind=="tile"` function, runs the core fn on a materialized open and a virtual open of the same source tile and compares fingerprints. This is the QA correctness gate across the whole leg (Task 1 covered two fns by hand).

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/runner.py` (add `run_pure_core_parity`)
- Test: `python/geobrix/test/bench/test_runner.py`

**Interfaces:**
- Consumes: `fingerprint.*` (verbatim: `fingerprint_output`, `fingerprint_dggs_grid`, `fingerprint_dggs_grid_str`, `fingerprint_collection`, `fingerprint_vector`); `open_tile._open`; `_serde.open_tile`; `VirtualTile`.
- Produces: `runner.run_pure_core_parity(corpus_root, corpus, fnspecs) -> list[tuple[str,int,bool,str,str]]` (fn, tile_px, matched, mat_fp, virt_fp) — consumed by the Task 11 smoke and the QA anomalies log.

- [ ] **Step 1: Write the failing test** (append to `test_runner.py`)

```python
def test_pure_core_parity_slope_width(tmp_path):
    from databricks.labs.gbx.bench import runner as rn, spec as s, datagen as dg
    corpus = dg.generate_corpus(out_dir=tmp_path, seed=9, tile_px=[64], bands=[1],
        dtypes=["float32"], srids=[4326], nodata_fracs=[0.0], row_rows=1,
        row_tile_px=64, row_bands=1, row_dtype="float32")
    fns = s.select(functions=["rst_slope", "rst_width"])
    res = rn.run_pure_core_parity(tmp_path, corpus, fns)
    assert res, "expected parity rows"
    for name, px, ok, mat, virt in res:
        assert ok, f"{name}@{px}: virtual != materialized ({mat} vs {virt})"
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "pure_core_parity" --log runner.log`
Expected: FAIL — `run_pure_core_parity` undefined.

- [ ] **Step 3: Implement**

```python
def _parity_fp(fs, out):
    from databricks.labs.gbx.bench import fingerprint as fp

    k = getattr(fs, "fingerprint_kind", "auto")
    if k == "dggs_grid":
        return fp.fingerprint_dggs_grid(out)
    if k == "dggs_grid_str":
        return fp.fingerprint_dggs_grid_str(out)
    if k == "vector":
        return fp.fingerprint_vector(out)
    if k == "collection":
        return fp.fingerprint_collection(out)
    return fp.fingerprint_output(out)


def run_pure_core_parity(corpus_root, corpus, fnspecs):
    """Per tile-input fn, compare materialized-open vs virtual-open output
    fingerprints. Returns (fn, tile_px, matched, mat_fp, virt_fp). Exceptions are
    recorded as matched=False findings, never swallowed."""
    from databricks.labs.gbx.pyrx.core import open_tile as _ot
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    root = Path(corpus_root)
    sweep = [t for t in corpus.size_sweep if t.role != "bng_gb"]
    out = []
    for fs in fnspecs:
        if getattr(fs, "input_kind", "tile") != "tile":
            continue
        for te in sweep:
            p = root / te.path
            try:
                with _serde.open_tile(p.read_bytes()) as ds:
                    mat_fp = _parity_fp(fs, fs.core_fn(ds, fs.args))
                vt = VirtualTile(cellid=int(te.cellid), raster=None, path=str(p),
                                 window=(0, 0, te.tile_px, te.tile_px)).to_row()
                with _ot._open(vt) as ds:
                    virt_fp = _parity_fp(fs, fs.core_fn(ds, fs.args))
                out.append((fs.name, te.tile_px, mat_fp == virt_fp, mat_fp, virt_fp))
            except Exception as e:  # noqa: BLE001 — finding, not silent skip
                out.append((fs.name, te.tile_px, False, "", f"ERROR: {e}"))
    return out
```

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "pure_core_parity" --log runner.log`
Expected: PASS. If any fn diverges here or in the Task 11 full sweep, STOP: root-cause the virtual-tile bug (systematic-debugging), fix in `pyrx`, add the failing tile as a regression case; do not proceed to publish.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/runner.py python/geobrix/test/bench/test_runner.py
git commit -m "feat(bench): pure-core virtual==materialized parity sweep"
```

---

### Task 7: `rst_fromfile` virtual-tile creation micro-leg (spark-path)

`rst_fromfile` has no ordinary spark-path form (no `path` column in the materialized tile DataFrame). The virtual run's `binaryFile` load DOES carry a `path` column, so a small dedicated leg times creating a virtual tile from a path at scale — the distributed creation cost you asked for. Runs only when `input_tile == "virtual"` and `rst_fromfile` is in the selected fns.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/runner.py` (add `_run_sp_creation`; keep `raw` and call it from `run_spark_path`)
- Test: `python/geobrix/test/bench/test_runner.py`

**Interfaces:**
- Consumes: `raw` (the `binaryFile` DataFrame with a `path` column), `pyrx.functions.rst_fromfile`, existing `_sp_scalar_ok_row`/`_sp_scalar_error_row`/`time_iters`, `spec.REGISTRY["rst_fromfile"]`.
- Produces: rows with `fn="rst_fromfile"`, `input_tile="virtual"`, `output_disposition="deferred"`.

- [ ] **Step 1: Write the failing tests** (append to `test_runner.py`)

```python
def test_creation_microleg_defined():
    from databricks.labs.gbx.bench import runner as rn
    assert hasattr(rn, "_run_sp_creation")


def test_rst_fromfile_column_builds():
    from pyspark.sql import functions as F
    from databricks.labs.gbx.pyrx import functions as prx
    c = prx.rst_fromfile(F.col("path"), "GTiff")
    assert c is not None
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "creation_microleg or fromfile_column" --log runner.log`
Expected: FAIL — `_run_sp_creation` undefined.

- [ ] **Step 3: Implement**

Add `_run_sp_creation` at module scope in `runner.py`:

```python
def _run_sp_creation(spark, run_id, pool, env, row_counts, warmup, measured,
                     raw, nparts, partition_size, F):
    """Time rst_fromfile virtual-tile CREATION over the binaryFile `path` column."""
    import math as _math

    from databricks.labs.gbx.bench.spec import REGISTRY
    from databricks.labs.gbx.pyrx import functions as prx

    fs = REGISTRY["rst_fromfile"]
    path_df = raw.select(F.col("path"))
    rows = []
    for n in sorted(row_counts):
        _psize = partition_size if partition_size and partition_size > 0 else max(1, n // (nparts * 4))
        _parts = max(1, _math.ceil(n / _psize))
        df = path_df.limit(n).repartition(_parts, F.rand())
        try:
            def job(_df=df):
                c = prx.rst_fromfile(F.col("path"), "GTiff")
                _df.select(c.alias("out")).write.format("noop").mode("overwrite").save()

            stats = time_iters(job, warmup, measured)
            r = _sp_scalar_ok_row(fs, run_id, pool, n, env, stats)
        except Exception as e:  # noqa: BLE001
            r = _sp_scalar_error_row(fs, run_id, pool, n, env, warmup, e)
        rows.append(_dc.replace(r, input_tile="virtual", output_disposition="deferred"))
    return rows
```

In `run_spark_path`: keep the `raw` DataFrame reachable after `df_all` is built, and after the main fn loop, when `input_tile == "virtual"` and `rst_fromfile` is among the selected fn names, append `_run_sp_creation(spark, run_id, pool, env, row_counts, SPARK_WARMUP-equivalent warmup, measured, raw, _nparts, partition_size, F)`.

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "creation_microleg or fromfile_column" --log runner.log`
Expected: PASS. (End-to-end creation timing is exercised in Task 11.)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/runner.py python/geobrix/test/bench/test_runner.py
git commit -m "feat(bench): rst_fromfile virtual-tile creation micro-leg"
```

---

### Task 8: Thread `--input-tile` through `cluster.py`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (the notebook builder — `build_bench_notebook` per recon; add param + `_PREAMBLE` var + pass to `run_spark_path` inside `run_light`)
- Test: `python/geobrix/test/bench/test_cluster.py`

**Interfaces:**
- Consumes: `run_spark_path(..., input_tile=...)` (Task 4).
- Produces: the generated notebook defines `INPUT_TILE` and passes `input_tile=INPUT_TILE` to the spark-path runner.

- [ ] **Step 1: Write the failing test** (append to `test_cluster.py`)

```python
def test_notebook_threads_input_tile():
    from databricks.labs.gbx.bench import cluster as cl
    # Reuse the argument set an existing test_cluster.py test passes to the
    # builder; add input_tile="virtual".
    src = _build_min_notebook(cl, input_tile="virtual")  # local helper mirroring existing test
    assert "INPUT_TILE" in src
    assert "input_tile=INPUT_TILE" in src
    assert "'virtual'" in src or '"virtual"' in src
```

(If `test_cluster.py` has no existing notebook-build helper, model `_build_min_notebook` on the arguments an existing test already passes to `build_bench_notebook`.)

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_cluster.py -k "input_tile" --log cluster.log`
Expected: FAIL — no `INPUT_TILE` in the rendered notebook.

- [ ] **Step 3: Implement**

- Add `input_tile: str = "materialized"` to `build_bench_notebook`'s signature.
- Add to the `_PREAMBLE` template (near the other flag vars, ~cluster.py:157-262): `INPUT_TILE = {input_tile!r}`.
- Pass `input_tile=input_tile` when `.format(...)`-ing the preamble.
- In the notebook's `run_light` helper, pass `input_tile=INPUT_TILE` to the `runner.run_spark_path(...)` call(s) (both the resume/`_todo` path and any full path).

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_cluster.py -k "input_tile" --log cluster.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/cluster.py python/geobrix/test/bench/test_cluster.py
git commit -m "feat(bench): thread --input-tile into the cluster notebook"
```

---

### Task 9: Expose `--input-tile` on the `gbx:bench:cluster` command

**Files:**
- Modify: `scripts/commands/gbx-bench-cluster.sh` (+ `.md`) and the push/run entry it calls (per `cluster-bench-setup`: `notebooks/tests/push_and_run_bench_on_cluster.py`) — **verify exact paths first**.

**Interfaces:**
- Produces: `gbx:bench:cluster --input-tile virtual|materialized` reaching `build_bench_notebook(input_tile=...)`.

- [ ] **Step 1: Locate the real entry**

Run: `grep -rn "input-tile\|argparse\|build_bench_notebook\|run_spark_path" scripts/commands/gbx-bench-cluster.sh notebooks/tests/push_and_run_bench_on_cluster.py`
Confirm which script parses CLI args and calls the notebook builder.

- [ ] **Step 2: Add the argument (argparse)**

In the push/run Python entry:

```python
parser.add_argument(
    "--input-tile", choices=["materialized", "virtual"], default="materialized",
    help="Input tile for the light spark-path leg: materialized (bytes) or virtual (path+window).",
)
```

Thread `args.input_tile` into `build_bench_notebook(..., input_tile=args.input_tile)`.

- [ ] **Step 3: Pass it through the `.sh`**

In `scripts/commands/gbx-bench-cluster.sh`, forward an `--input-tile <mode>` option to the Python entry (default `materialized`), and document it in `gbx-bench-cluster.md` (options list + one example: `gbx:bench:cluster --input-tile virtual`).

- [ ] **Step 4: Verify the flag is wired (help + dry parse)**

Run: `bash scripts/commands/gbx-bench-cluster.sh --help` and confirm `--input-tile` appears.
Expected: help shows the option; no traceback.

- [ ] **Step 5: Commit**

```bash
git add scripts/commands/gbx-bench-cluster.sh scripts/commands/gbx-bench-cluster.md notebooks/tests/push_and_run_bench_on_cluster.py
git commit -m "feat(bench): gbx:bench:cluster --input-tile flag"
```

---

### Task 10: `summarize` surfaces disposition + a QA anomalies section

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/results.py` (`summarize`, ~245-343)
- Test: `python/geobrix/test/bench/test_results.py`

**Interfaces:**
- Consumes: `ResultRow.input_tile/output_disposition` (Task 2).
- Produces: markdown containing a virtual-tile disposition breakdown and an anomalies list.

- [ ] **Step 1: Write the failing test** (append to `test_results.py`; reuses `_row` from Task 2)

```python
def test_summarize_surfaces_disposition_and_anomalies():
    from databricks.labs.gbx.bench.results import summarize
    rows = [
        _row(fn="rst_setsrid", input_tile="virtual", output_disposition="deferred"),
        _row(fn="rst_slope", input_tile="virtual", output_disposition="materialized"),
        _row(fn="rst_clip", input_tile="virtual", output_disposition="na",
             status="error", note="boom"),
    ]
    md = summarize(rows)
    assert "disposition" in md.lower()
    assert "deferred" in md and "materialized" in md
    assert "rst_clip" in md  # anomaly surfaced
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_results.py -k "disposition_and_anomalies" --log res.log`
Expected: FAIL — summary has no disposition/anomalies content.

- [ ] **Step 3: Implement** — in `summarize`, after the existing status/tables, append:

```python
    virt = [r for r in rows if r.input_tile == "virtual"]
    if virt:
        _def = sum(1 for r in virt if r.output_disposition == "deferred")
        _mat = sum(1 for r in virt if r.output_disposition == "materialized")
        lines += [
            "",
            "## Virtual-tile disposition",
            f"- deferred (stayed virtual / header-only): {_def}",
            f"- materialized (read/generated pixels): {_mat}",
            "",
            "| fn | disposition |",
            "|---|---|",
        ]
        for r in sorted(virt, key=lambda x: x.fn):
            lines.append(f"| {r.fn} | {r.output_disposition} |")
    _anom = [r for r in rows if r.status == "error"]
    if _anom:
        lines += ["", "## QA anomalies (must be triaged, not published around)"]
        for r in _anom:
            lines.append(f"- {r.fn} ({r.input_tile}): {r.note}")
    return "\n".join(lines)
```

(If `summarize` already ends by joining `lines`, fold these in before that return rather than adding a second return.)

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_results.py -k "disposition_and_anomalies" --log res.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/results.py python/geobrix/test/bench/test_results.py
git commit -m "feat(bench): summary shows disposition + QA anomalies"
```

---

### Task 11: Local Docker smoke — full bench suite + end-to-end virtual leg

Gate before any cluster time. All in the `geobrix-dev` container via `gbx:*`.

**Files:**
- Add: `python/geobrix/test/bench/test_runner.py` — one Spark-backed integration test (mark it the way existing spark-path bench tests are marked; Docker-only).

**Interfaces:** consumes everything from Tasks 2–7.

- [ ] **Step 1: Run the full bench unit suite**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/ --log bench-suite.log`
Expected: all green (Tasks 1–10 tests + no regressions).

- [ ] **Step 2: Write the end-to-end virtual smoke** (append to `test_runner.py`, using the existing spark fixture/marker in that file)

```python
def test_spark_virtual_leg_smoke(spark, tmp_path):  # use the file's existing spark fixture
    from databricks.labs.gbx.bench import runner as rn, spec as s, datagen as dg
    corpus = dg.generate_corpus(out_dir=tmp_path, seed=9, tile_px=[64], bands=[1],
        dtypes=["float32"], srids=[4326], nodata_fracs=[0.0], row_rows=4,
        row_tile_px=64, row_bands=1, row_dtype="float32")
    fns = s.select(functions=["rst_slope", "rst_setsrid", "rst_avg", "rst_fromfile"])
    rows = rn.run_spark_path(spark, tmp_path, corpus, fns, "smoke", [4], 1, 1,
                             "venv", input_tile="virtual")
    ok = [r for r in rows if r.status == "ok"]
    assert ok, "expected ok rows"
    assert all(r.input_tile == "virtual" for r in ok)
    disp = {r.fn: r.output_disposition for r in ok}
    assert disp.get("rst_slope") == "materialized"
    assert disp.get("rst_setsrid") == "deferred"
    assert disp.get("rst_avg") == "materialized"
    assert disp.get("rst_fromfile") == "deferred"  # creation micro-leg
    assert all(r.per_tile_avg_s >= 0 for r in ok)
```

- [ ] **Step 3: Run the smoke + parity sweep**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/bench/test_runner.py -k "spark_virtual_leg_smoke or pure_core_parity" --log smoke.log`
Expected: PASS. **If disposition is wrong or parity diverges → STOP, systematic-debugging, fix in `pyrx`, regression-test.** Do not adjust the test to pass.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/bench/test_runner.py
git commit -m "test(bench): end-to-end virtual spark-path leg smoke"
```

---

### Task 12: Cluster run on the fixed 20-worker cluster (light tier only)

Execution task — orchestrate via a subagent carrying the QA discipline + orientation (light tier ⇒ no JAR). Load `databricks-core` (+ the matching product skill) before touching the workspace.

- [ ] **Step 1: Auth + preflight (lead does this, not the subagent)**
  - `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch`; confirm `oauth-fe` VALID. If stale, cue the user the `databricks auth login --host <ws> --profile oauth-fe` line and wait.
  - `databricks clusters get 0519-143423-0jwqt79u --profile oauth-fe`; confirm `num_workers: 20`, no `autoscale`. If wrong: terminate → edit → start (edit spec from the `spec` sub-object, drop `autoscale`, keep `cluster_id`).

- [ ] **Step 2: Stage artifacts (match the published baseline scale)**
  - Build + stage the 0.5.0 wheel to the bundle-volume path; stage the spark-path corpus. Confirm the corpus tile-shapes + row-count sweep match the published materialized snapshot (`bench-1000-scale-only-now`), else the speedup join has no matching keys.
  - Confirm where the harness changes execute (host-generated notebook vs wheel); rebuild the wheel only if the harness logic ships in it.

- [ ] **Step 3: Launch the virtual leg**

Run (light only): `bash scripts/commands/gbx-bench-cluster.sh --input-tile virtual --lightweight-only --log bench-cluster-virtual.log` (confirm the exact lightweight-only flag name in the command).
  - Give the run/job URL **early** (query `list_runs` by exact `run_name` → `run_page_url`).
  - Post a one-line progress update ~every 30s.

- [ ] **Step 4: Verify + QA before reporting**
  - Verify **non-zero rows** against the expected spark-path count (only fns with a spark-path variant + the creation micro-leg row).
  - Confirm every row carries `input_tile="virtual"` and a disposition; run the pure-core parity sweep result — **zero divergences**, or each divergence root-caused + fixed.
  - Emit the summary (with disposition + anomalies) and give the **summary link**.
  - Any error / zero-row / unexpected disposition → triage per B.5; do not publish around it.

- [ ] **Step 5: Cluster lifecycle** — per the user's call (standing infra); do not auto-terminate without asking.

(No code commit here; this produces `bench_results` rows + a summary artifact.)

---

### Task 13: Publish the column in `benchmarking.mdx` (post-run)

**Files:**
- Modify: `docs/docs/api/benchmarking.mdx` (Raster tab)

- [ ] **Step 1:** Add two columns to the Raster tab per-function table: `lw_virtual_per_tile_s` (beside the existing materialized `lw_per_tile_s`) and `disposition`, populated from the run's results.

- [ ] **Step 2:** Add framing prose (user-facing voice): the virtual column measures default behavior on a virtual input tile (path + window, bytes-free); `deferred` rows are cheap because pixel I/O is **postponed to a later terminal operation, not eliminated**; the materialized column is the prior baseline; `rst_fromfile` is the virtual-tile **creation** cost from a path. No speedup column yet (data supports adding one later; cross-run caveat until a same-run `materialized=True` pass exists).

- [ ] **Step 3: Guardrail check**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/benchmarking.mdx` → must print nothing. Confirm no subagent/dispatch vocabulary. (The QC `internals-leak` check enforces this on push.)

- [ ] **Step 4: Commit**

```bash
git add docs/docs/api/benchmarking.mdx
git commit -m "docs(bench): publish lightweight virtual-tile column (Raster tab)"
```

---

## Self-Review (plan vs spec)

**Spec coverage:** A (measurement model) → Tasks 4,6,7; disposition marker → Tasks 3,5; scope incl. `rst_fromfile` creation → Task 7; B (code changes) → Tasks 2–8; B.5 (QA/no-papering-over) → Tasks 1,6,11 + the "STOP/root-cause" steps + Task 10 anomalies; C (docs + capture-for-speedup) → Tasks 2 (key-aligned rows),13; D (cluster) → Task 12; E (testing) → Tasks 1–11. Follow-ups (materialized=True re-run, pure-core-virtual beyond parity, reader/writer leg) are out of scope by design.

**Placeholder scan:** no TBD/TODO; each code step has real code. Two tasks intentionally point at an existing in-file pattern rather than reprinting unseen bodies (Task 8's notebook-builder arg set; Task 12's exact flag names) — flagged with a "confirm/verify" step, not a blind placeholder.

**Type consistency:** `input_tile`/`output_disposition` names are identical across Tasks 2,5,7,10,11; `_build_input_tile`, `_disposition_of`, `run_pure_core_parity`, `_run_sp_creation` names match their call sites; `accessor_disposition` signature matches Tasks 3↔5.

**Known verification points for the implementer (not gaps in intent):** exact line numbers in `runner.py`/`spec.py`/`results.py`/`cluster.py` (re-confirm before editing); the pixel-open entry name (`ot._open` vs `ot.open_tile`) — Task 1 Step 2 pins it; the notebook builder's real name/signature (`build_bench_notebook`); the lightweight-only flag name on `gbx:bench:cluster`.

---
