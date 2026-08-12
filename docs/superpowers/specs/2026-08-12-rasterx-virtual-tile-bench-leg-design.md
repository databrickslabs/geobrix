# RasterX Lightweight Virtual-Tile Benchmark Leg — Design

- **Date:** 2026-08-12
- **Status:** Approved (brainstorming) → ready for implementation plan
- **Branch:** `beta/0.5.0`
- **Owner:** mjohns
- **Related memories:** `bench-virtual-tile-leg`, `cluster-bench-setup`, `bench-cluster-fixed-20-workers`, `perf-benchmark-program-04`, `bench-run-give-summary-link`, `bench-verify-nonzero-before-reporting`, `rst-fromfile-virtual-default`, `light-through-finalize-spec`, `pyrx-udf-boundary-tax`, `light-tier-no-jar`, `bench-changes-update-docs`

## Context

The GeoBrix benchmark today measures the lightweight (pyrx) raster tier on **materialized**
tiles only — it has no notion of virtual-vs-materialized input, because virtual tiles did not
exist at the last run. Virtual tiles (bytes-free: `path` + `window`, lazy range-read; the 0.5.0
`rst_fromfile` virtual default and the through-op virtual passthrough) are the 0.5.0 headline, and
their per-function performance is currently uncharacterized and unpublished.

This work adds a **virtual-tile dimension** to the light raster leg, publishes it as a new column
in `docs/docs/api/benchmarking.mdx` (Raster tab), and — critically — uses the run as a **second QA
pass on the virtual-tile feature**: any glitch is root-caused and fixed, never papered over.

## Goal

For each light-raster function that consumes or produces a tile, measure its **default** behavior
when fed (or asked to create) a **virtual** tile, on the fixed 20-worker bench cluster, recorded as
a distinct series and published as a new column with a disposition marker — while robustly
surfacing and fixing any virtual-tile correctness or behavior defects the run exposes.

## Non-goals / deferred

- **No materialized re-run this cycle.** The existing published materialized numbers stand as the
  comparison baseline (they were effectively `materialized=True`, since virtual tiles didn't
  exist). A **future** run will make the materialized leg explicit with `materialized=True` on the
  same fixed cluster, yielding same-run pairs; this is captured as follow-up, not done here.
- **Pure-core virtual is out of scope.** The published light column is the distributed per-tile
  number (`lw_per_tile_s`), and virtual-tile handling is inherently a distributed/executor concern,
  so the virtual leg runs **spark-path only**. Pure-core stays as-is.
- No heavy-tier work. No new sidebar page (a column on an existing page).

## Section A — Measurement model & scope

**What we measure on virtual tiles (two faces of the same feature):**

1. **Virtual input** — tile-*consuming* functions fed a virtual input tile
   (`raster=NULL`, `path`+`window` set), timed at default behavior.
2. **Virtual output / creation** — tile-*producing* functions, above all `rst_fromfile` creating a
   virtual tile from a path (the create-a-virtual-tile-from-path cost, already the default on this
   branch). Also `rst_fromcontent` (note: from bytes you already hold, so its virtual disposition
   differs — a per-function nuance, not excluded).

We impose **no** materialization strategy. Whether the output comes back virtual or materialized is
the function's own decision: pixel-generating ops (slope, hillshade, mapalgebra, warp/resample,
clip) materialize as part of doing their work; pending-instruction/passthrough ops (setnodata,
setsrid, band-select, metadata edits — the `light-through-finalize-spec` class) stay virtual and
return lazily. The column reflects what the library naturally does.

**Scope:** every function in the light raster leg that consumes **or** produces a tile — accessors,
tile-in/tile-out ops, and the tile producers (`rst_fromfile`, `rst_fromcontent`). Nothing is carved
out for "no tile input" anymore: `rst_fromfile` is explicitly included to measure virtual-tile
*creation*.

**Disposition marker (the published explanatory variable):** *did the function trigger pixel
materialization?*
- `deferred` — tile stayed virtual / only the header/window metadata was read.
- `materialized` — pixels were read or generated.
- Detection: tile-returning/producing ops → sample the result tile's `raster` field
  (`raster is None` ⇒ `deferred`). Scalar accessors have no output tile → a small **verified**
  per-function classification (metadata-only accessors like width/srid/georeference/bbox ⇒
  `deferred`; pixel-reading accessors like avg/min/max/median/histogram/summary/pixelcount ⇒
  `materialized`), validated against measured timing, never guessed.

**Grain:** one virtual row per `(fn, tile-shape, row-count)`, keyed identically to the existing
materialized rows, published as a new virtual column beside the materialized one, with disposition.

## Section B — Bench code changes

Grounded in reconnaissance of the bench package (file:line references below reflect the current
tree at design time; **verify against current code during implementation** — some cited internals
may have shifted).

**Bench package (`python/geobrix/src/databricks/labs/gbx/bench/`):**
- `spec.py` — `REGISTRY: dict[str, FnSpec]` of benchmarked functions with per-function metadata
  (`sql_name`, `category`, `modes`, `core_fn`, `col_fn`, `input_kind`, `fingerprint_kind`, …).
- `runner.py` — orchestrates pure-core and spark-path timing; the `_to_tile` UDF (~line 1415)
  builds materialized `V2_TILE_SCHEMA` tiles via `_serde.build_tile(bytes(content), "GTiff", cid)`.
- `results.py` — `ResultRow` schema, JSONL I/O, markdown summary.
- `cluster.py` — Databricks job scheduling / notebook generation / tier+mode selection.
- `V2_TILE_SCHEMA` lives in `pyrx/core/virtual_tile.py` (`raster=NULL` + `path`/`window` ⇒ virtual;
  `VirtualTile.is_virtual()` ⇔ `raster is None`; `open_tile()` materializes on-demand from
  `path`+`window`).

**Changes:**

1. **Run-level virtual dimension (not a per-function knob).** A flag threaded
   `cluster.py` → `runner.py` (e.g. `--input-tile virtual|materialized`, default `materialized`).
   The materialized path is untouched.

2. **Branch in `_to_tile` (runner.py ~1415).** Add a virtual branch emitting a virtual
   `V2_TILE_SCHEMA` row (`raster=NULL`, `path=<corpus path>`, `window=<whole-tile>`, `cellid` set)
   instead of `build_tile(...)`. `col_fn`s already receive a `V2_TILE_SCHEMA` struct and open
   on-demand, so **no `col_fn` rewrites**.

3. **Spark-path only.** The virtual leg runs spark-path across the row-count sweep. Pure-core
   unchanged.

4. **Tile producers (`rst_fromfile`, `rst_fromcontent`).** Take a `path`/`bytes`
   (`input_kind="path"`/`"bytes"`), measured for **creation** cost — time the op producing its
   default tile; disposition follows from the produced tile. Ensure they're included in the leg; no
   special-casing beyond inclusion.

5. **Disposition capture.** After `col_fn`, record per function: tile-returning/producing → sample
   output tile `raster` null-ness; scalar accessors → the verified `FnSpec` classification (new
   field in `spec.py`).

6. **`results.py` — `ResultRow` gains two fields:** `input_tile` (`materialized`|`virtual`) and
   `output_disposition` (`deferred`|`materialized`|`na`), with backward-compatible defaults so
   existing JSONL still parses.

Net footprint: `runner.py` (`_to_tile` branch + disposition sampling + flag threading),
`results.py` (2 fields + markdown), `spec.py` (accessor classification field),
`cluster.py` (expose the flag in the generated job/notebook). No `col_fn` rewrites.

## Section B.5 — QA & correctness discipline (no papering over)

This run is a **second QA pass** on the virtual-tile feature. The bar:

1. **Correctness gate — virtual must equal materialized.** Every function's virtual-input output
   must **fingerprint-match** its materialized output (reuse `fingerprint.py`). Any divergence is a
   correctness bug in the virtual-tile code path (`pyrx/core/virtual_tile.py`, `_serde.open_tile`),
   not a benchmark curiosity → stop, systematic-debugging, fix upstream, add a regression test. A
   diverging number is **not** published.
2. **No silent fallback.** The virtual `_to_tile` branch and lazy `open_tile(path, window)` must
   **never** degrade to materialized-on-error. A failure surfaces as a hard error/finding, not a
   quiet fallback that hides the bug behind a plausible number.
3. **Disposition must be truthful and expected.** Where disposition is known a priori
   (passthrough ⇒ `deferred`; pixel-generating ⇒ `materialized`), assert it. A mismatch is a
   **flagged regression to investigate**, not just a recorded value.
4. **Errors and zero-rows are blocking findings.** Empty/zero-row or error results block reporting
   until explained; each is triaged — genuine bug → fix at root cause; true precondition → one
   explicit documented line. Never silently substituted or skipped.
5. **Every glitch → systematic-debugging → root-cause fix + regression test.** Fixes land in the
   feature/pyrx code, not in the harness to make a number look clean. Harness changes are TDD'd.
6. **QA findings log in the run summary.** An explicit anomalies section — every error, fingerprint
   divergence, and unexpected disposition, each with an outcome (fixed / root-caused / precondition).
   Nothing swept under the timing table.

**Dispatch rule:** every subagent (implementer or cluster-runner) gets this discipline verbatim
plus the standing orientation (CONFIRMED vs SUSPECTED; quote real source; `gbx:*` commands only,
fix don't work around; light tier ⇒ no JAR). The lead re-verifies subagent findings.

## Section C — Docs (benchmarking.mdx Raster tab) + capture-for-speedup

The Raster tab tables are hand-authored snapshots from the bench results.

**New columns on the existing per-function raster table:**
- `lw_virtual_per_tile_s` — virtual-input light per-tile timing, beside the existing materialized
  `lw_per_tile_s`.
- `disposition` — compact `deferred` / `materialized` marker.

**Speedup:** not published as a baked column by default, **but the data must fully support adding
one**:
- **Key alignment** — virtual rows keyed identically to materialized
  `(fn, tile-shape [tile_px, bands, dtype, srid, nodata_frac], mode=spark-path, row_count)` so
  `speedup = materialized_per_tile_s / virtual_per_tile_s` is a trivial publish-time derivation.
- **Raw timings stored, speedup computed on publish** (never a precomputed ratio in the row).
- **Disposition travels with each row**, so a multiplier is framed correctly: `deferred` rows → the
  multiplier *is* the lazy-defer win (a positive — "computes only when new pixels are forced");
  `materialized` rows → a true like-for-like compute comparison.
- **Full environment stamp** per row (cluster id, worker count, DBR, wheel version) via existing
  `results.py` metadata, so any speedup can assert configs match.
- **Corpus/scale must match the published materialized baseline.** Key alignment is only real if the
  virtual run uses the **same corpus tile-shapes and row-count sweep** as the existing published
  materialized snapshot — otherwise there are no matching keys to join. Confirm the baseline's
  corpus config (tile_px/bands/dtype/srid/nodata_frac + row-count sweep, per `bench-1000-scale-only-now`)
  **before staging the corpus**, and reproduce it for the virtual run.

**Rigor caveat (on record):** a fully defensible speedup wants materialized + virtual in the **same
run on the same fixed 20-worker cluster**. This run produces virtual only vs the existing
materialized snapshot — same cluster shape, different run — so a speedup column published now must
be labeled **cross-run** until the future `materialized=True` re-run yields same-run pairs. The
key-aligned, environment-stamped capture means that upgrade needs no virtual re-run.

**Framing prose (user-facing voice — no internal vocabulary, passes QC `internals-leak`):** the
virtual column measures default behavior on a virtual input tile (path + window, bytes-free);
`deferred` rows are cheap because pixel I/O is **postponed to a later terminal operation, not
eliminated**; the materialized column is the prior baseline; `rst_fromfile` is the virtual-tile
*creation* cost from a path.

## Section D — Cluster run plan

- **Auth:** `oauth-fe`. Re-verify VALID immediately before dispatch; if stale, cue the user the
  `databricks auth login` line and wait — never auto-login (hook-blocked).
- **Cluster:** fixed bench cluster `0519-143423-0jwqt79u`. **Preflight (mandatory):**
  `databricks clusters get 0519-143423-0jwqt79u --profile oauth-fe`; confirm `num_workers: 20` and
  **no** `autoscale`. If wrong: **terminate → edit → start** (edit spec built from the `spec`
  sub-object, drop `autoscale`, set `num_workers: 20`, keep `cluster_id`; a running cluster rejects
  `edit`).
- **Tier & artifacts:** **light tier only ⇒ no JAR** (stated in every dispatch). Stage the **0.5.0
  wheel** (carries `pyrx/core/virtual_tile.py`) to the bundle-volume path + the spark-path corpus.
  Verify where the harness changes execute (host-generated notebook vs wheel) rather than assume;
  cluster-bench memory suggests notebook-side (no wheel rebuild for harness logic), but confirm.
- **Run protocol:** give the run/job URL **early** (`list_runs` by exact `run_name` → `run_page_url`);
  **summary link at the end** (upload summary to the run out_dir). **Verify non-zero rows before
  reporting** against the expected spark-path count (only fns with a spark-path variant).
- **QA at run time:** fingerprint parity, disposition assertions, anomalies log (per B.5).
- **Orchestration:** long run via subagent with QA discipline + orientation; lead posts a one-line
  progress update ~every 30s. Cluster lifecycle after the run per the user's call (standing infra).

## Section E — Testing (TDD in Docker first)

Local Docker, **before any cluster time**:
1. Unit-test the virtual `_to_tile` branch emits a correct virtual `V2_TILE_SCHEMA`
   (`raster=None`, `path`/`window`/`cellid` set).
2. Test disposition detection: passthrough op (setnodata) ⇒ `deferred`; compute op (slope) ⇒
   `materialized`.
3. **Correctness gate test:** virtual-input output fingerprint-equals materialized-input output on
   a sample (the QA promise, as a test).
4. `ResultRow` new fields serialize/parse with backward-compat defaults.
5. Small local spark-path smoke (a few fns, low row-count) in Docker exercising the virtual leg
   end-to-end.

Run via `gbx:test:pyrx` on the affected bench tests (package-source change ⇒ unit suite, not just a
dry run). Only after local green do we go to the cluster. All harness edits are TDD'd; feature
fixes (if the QA pass finds virtual-tile bugs) land in `pyrx` with their own regression tests.

## Follow-up work (not this cycle)

- Explicit `materialized=True` materialized re-run on the same fixed cluster → same-run pairs →
  fully rigorous (caveat-free) speedup column.
- Pure-core virtual measurement, if ever wanted.
- Virtual-tile leg for the reader/writer benchmarks (`readers.py`), if in scope later.

## Verification checklist (definition of done)

- [ ] Harness changes TDD'd green in Docker (`gbx:test:pyrx` on affected bench tests).
- [ ] Local spark-path smoke of the virtual leg passes.
- [ ] Fixed cluster confirmed `num_workers: 20`, no autoscale.
- [ ] 0.5.0 wheel + spark-path corpus staged and verified.
- [ ] Cluster run completed; run URL given early, summary link at end.
- [ ] Non-zero rows verified against expected spark-path count.
- [ ] Fingerprint parity (virtual == materialized) holds for all fns, or divergences root-caused +
      fixed with regression tests.
- [ ] Disposition assertions hold, or mismatches investigated.
- [ ] Anomalies log emitted; every finding has an outcome (fixed / root-caused / precondition).
- [ ] `benchmarking.mdx` Raster tab gains `lw_virtual_per_tile_s` + `disposition`, fair framing,
      passes QC `internals-leak`.
- [ ] Speedup derivation validated as computable from captured data (even if not published yet).
