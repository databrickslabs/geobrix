# FILE-for-Raster (Light Tier) — Increment 2 Design

**Status:** Draft for review · 2026-08-18 · branch `beta/0.5.0`
**Builds on:** Increment 1 — spec `docs/superpowers/specs/2026-08-17-file-for-raster-design.md`,
plan `docs/superpowers/plans/2026-08-17-file-for-raster-light.md` (complete + on-cluster-validated, held).
**Runtime target:** DBR 19 (classic dogfood `19.x-snapshot`). FILE is absent on Serverless-GC until
environment v6; the reader **capability-gates** so nothing regresses where FILE is unavailable.

---

## 1. Context and goal

Increment 1 delivered the light-tier MANAGED-FILE bridge: the `path_mode` 9th tile field,
the FILE-table reader/writer (`file_table.py`), the per-partition grouped executor
(`grouped_exec.py`) with a byte-budget `OpenResourceLRU`, and one proof op —
`rst_memsize_grouped` — wired through the FILE-stream fast path with graceful fallback.

That proof op is **header-only**: it reads tile metadata, never pixels. The executor's FILE
fast path therefore hands `core_fn` a header-only `_WindowHeaderView` whose `read()` raises.
Real raster work reads pixels. **Increment 2 extends the FILE fast path to the ops that
actually read pixels** — `rst_clip` and the other windowed pixel ops, the `rst_*_agg`
aggregators, and the 1:n tile generators — while keeping attempt-FILE-with-graceful-fallback
and the per-partition open-amortization that makes FILE fast.

Doing that forces four design questions this document answers:

1. **Heavy/light tile-schema parity** — increment 1 widened the *light* v2 tile to 9 fields
   (`path_mode`); the *heavy* Scala tile is still 8. Mixing tiers on a tile column, or reading
   a cross-tier table, is a latent Catalyst schema mismatch. This must close **first**.
2. **Where MANAGED resolves** — the reader is the single place that turns a governed FILE into a
   path an op can open (reader-2b). Ops downstream read `path_mode` to choose MANAGED-vs-EXTERNAL
   handling.
3. **Header-view vs pixel-view (M2)** — the executor's opener/`core_fn` contract must serve both
   header ops (no pixel read) and pixel ops (a real windowed read) without the header view's
   `read()`-raises behavior leaking into pixel paths.
4. **Aggregators and 1:n generators** — neither fits the scalar 1:1 `mapInPandas` shape
   `rst_memsize_grouped` used, and aggregators additionally cannot inject `try_to_file`.

**Grounding facts (measured on dogfood, 2026-08):**

- A `.open()` stream opened via `rasterio.open(stream)` loads the **whole file into `/vsimem`
  memory** (~file-size RSS) — fast windows, but RAM ≈ file size. A 10 GB striped stream-open
  crashes the worker. FUSE (`.as_local_file()`) is lazy + low-RAM but slower cold.
- A `FileRef`'s entire public surface is `{as_local_file, checksum, content_type, from_bytes,
  from_local_file, offset, open, size, uri}`. Its only URL-ish accessor, `.uri`, returns a
  **`dbfs:` scheme path** (`dbfs:/Volumes/…`) — **not** an HTTP URL. There is no signed/presigned
  URL. GDAL `/vsicurl` therefore **cannot** address a FileRef; there is no lazy+low-RAM+fast
  third path. This is architectural, not a gap: a governed MANAGED FILE deliberately does not
  emit a raw signed URL. **The executor's large-file strategy is size-adaptive stream/FUSE — final.**

---

## 2. Global constraints (carry into the plan)

- **Light tier is pure Python, JAR-less.** No `spark.conf.set` / `_jvm` / `.rdd` in `pyrx`
  (bench-only `repartition` excepted). Serverless parallelism only via `repartition(N, column)`.
- **SQL binds positionally**; an extra wrapper arg beyond `builder()` arity is silently dropped.
  Any signature change moves all surfaces together.
- **FILE is DBR-19-only** → absent from `local[2]` test Spark. Each FILE-dependent seam is
  TDD-tested at its pure boundary locally (`gbx:test:pyrx`); the FILE round-trip is validated
  once on the dogfood cluster (`0813-214720-wo95qznu`, kept warm through this phase).
- **No hard dates and no internal planning vocabulary in user-facing docs** (`docs/docs/**`).
- **Package-source changes need the unit suite**, not just doc-tests: run `gbx:test:pyrx` on the
  affected `python/geobrix/test/**` files.
- **Graceful fallback is mandatory**: every FILE fast path degrades to the existing
  materialized/FUSE path on any failure; a FILE miss is a perf regression, never a correctness one.

---

## 3. Task 1 (FIRST) — Heavy-tier `path_mode`

**Problem.** Increment 1 made the light `V2_TILE_SCHEMA` 9 fields (adding `path_mode`); the heavy
Scala v2 tile `StructType` is still 8. Both tiers register the same `gbx_*` SQL names and share
one tile contract, so a light-produced tile (9) and a heavy-produced tile (8) do not share a
Catalyst schema — mixing them on a tile column, or reading a table one tier wrote and the other
reads, mismatches. `check-binding-parity.py` compares names, not schema, so CI would not catch it.

**Fix.** Add `path_mode` as the 9th field of the heavy Scala v2 tile `StructType` (in
`rasterx/util/RST_ExpressionUtil.scala` / `RasterSerializationUtil.scala` — the exact home is a
plan-time lookup), and **set it NULL for materialized tiles** (heavy does not yet emit virtual
tiles, so NULL/`None` = materialized is the correct default and matches light's
`effective_path_mode()`).

**Deliverable / tests.** A cross-tier round-trip: a tile written by one tier reads back with an
identical 9-field schema in the other; `path_mode` is NULL on heavy materialized output. This is a
Scala change → Docker/Maven/`gbx:test:scala` env plus a light-side schema-equality assertion.

**Why first.** Everything else in this increment builds on tiles flowing through the executor and
readers; closing the schema mismatch before that removes a latent cross-tier defect from under the
new code.

---

## 4. Reader-2b: where MANAGED resolves

**Principle (user, 2026-08-18):** *the reader is the one place that handles MANAGED FILE.* It reads
the FILE column and lifts the file's location into `tile.path`, tagging `path_mode`. Downstream ops
never re-derive governance; they read `path_mode` and decide how to open.

Two resolution options, and the choice:

- **2a — path = source path.** The tile carries the *original* source path (`/Volumes/…/x.tif`),
  `path_mode = external`. Simple, but **live-source-coupled**: if the source is moved or deleted,
  the tile dangles.
- **2b — path = the FILE's own location (chosen).** The reader reads the FILE column, takes
  `file.uri` (`dbfs:/Volumes/<governed-path>`), **strips the `dbfs:` scheme** to `/Volumes/<governed-path>`
  (a FUSE-openable path — confirmed: `FileRef.uri` is exactly `dbfs:/Volumes/…`), and sets
  `tile.path` to it with `path_mode = managed`. **Lifecycle-proof**: the path points at the
  governed copy the FILE owns, not a mutable external source.

**Capability-gating.** 2b runs only where the FILE column is accessible (classic DBR-19 now;
Serverless once environment v6 ships FILE). Where FILE is unavailable, the reader **falls back to
2a** (source path, `external`). The gate reuses the increment-1 capability signal (presence of the
reconstructable `_file_ref` column / `file_supported`), never a runtime version string.

**Call chains (concrete):**

```
2a (external / no-FILE fallback):
  read_file_table(df) ->
    tile.path      = "/Volumes/<catalog>/<schema>/<vol>/src/x.tif"   # the source path
    tile.path_mode = external
  op(tile): open tile.path via FUSE (rasterio.open(path))            # treated as external

2b (managed, FILE available):
  read_file_table(df) reads the FILE column ->
    file.uri       = "dbfs:/Volumes/<catalog>/<schema>/<vol>/__managed__/<id>.tif"
    tile.path      = strip("dbfs:", file.uri)  # -> "/Volumes/.../__managed__/<id>.tif" (FUSE path)
    tile.path_mode = managed
  op(tile): read tile.path_mode ->
              may open tile.path via FUSE (as external),  OR
              reconstruct a FileRef (try_to_file(tile.path)) for the .open() stream fast path
```

The distinction the caller asked to preserve: 2b tiles are still ordinary v2 tiles whose `path`
happens to be the governed FILE location. An op **may treat a managed path as external** (open it
via FUSE) — see §5. So `path_mode` is *information the op may exploit*, not a hard fork.

**Tier-general factoring.** The FILE primitives touched here — `file_supported` (capability gate),
`file_ref_arg` (FileRef reconstruction), the reader-2b managed-uri resolution, and the FILE-table
read path — are factored as **tier-general helpers**, not raster-only, so light-tier *vector*
(`pyvx`) readers can reuse them later. Caveat: `path_mode` is a *tile* field and does not transfer
to vector geometry rows; the shared base is the capability / FileRef / FILE-table / listing layer,
and the tile-specific bits stay raster-side.

---

## 5. Aggregators handle FILE tiles as EXTERNAL

**Constraint (measured, Spark 4):** `try_to_file(...)` is **nondeterministic**, and Spark 4 rejects
a nondeterministic expression as an argument to an aggregate function
(`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`). So the increment-1 FILE fast path — which
injects `file_ref_arg` = `try_to_file(tile["path"])` to reconstruct a FileRef — **cannot** be used
inside `rst_*_agg` (`rst_merge_agg`, `rst_combineavg_agg`).

**Design.** Aggregators open their input tiles **as EXTERNAL**: via the FUSE path
(`rasterio.open(tile.path)`), with **no `try_to_file` injection**. A managed-origin path (from
reader-2b) still works — it is a `/Volumes/…` FUSE path — it is simply *treated as external* rather
than reconstructed into a FileRef stream.

**Exact framing (user, 2026-08-18):** the docs say aggregators **"do not treat paths as managed"**
— i.e. they read managed-origin paths perfectly well, just as external via FUSE. They do **not** say
aggregators "cannot handle managed paths." This is a handling choice forced by the Spark-4
aggregate-arg rule, not a capability gap. The in-group open-amortization ROI for aggregators is ~0
(a group's tiles rarely share one source), so nothing is lost by skipping the stream path here.

---

## 6. Pixel-ops executor, M2 resolution, and memory-aware stability

### 6.1 Routing pixel ops and 1:n generators through the executor

Route the pixel-reading scalar ops (`rst_clip` first, then its family) and the 1:n tile generators
through `grouped_tile_map` (partition-scoped `mapInPandas` + per-partition `OpenResourceLRU`), the
same executor `rst_memsize_grouped` uses. This extends open-amortization (open a source once per
partition, read many windows) to pixel work, where it matters most.

- **Scalar 1:1 pixel ops** emit one output tile per input tile — the shape the executor already has.
- **1:n generators** (one input tile → many output tiles) do not fit 1:1. `mapInPandas` already
  permits 1:n: the per-partition function *yields multiple output rows per input row*. The plan
  defines the generator contract on top of `grouped_tile_map` (a `core_fn` that returns an iterable
  of tiles rather than a single tile), preserving the LRU and fallback.

### 6.2 M2 — header-view vs pixel-view

Increment 1's FILE fast path hands `core_fn` a header-only `_WindowHeaderView` whose `read()`
raises; the fallback path hands a real windowed `DatasetReader`. A pixel op needs a real windowed
pixel read on *both* paths. Resolution:

- The opener/`core_fn` contract gains an explicit **view kind**: `header` (metadata only) vs
  `pixels` (a real windowed read).
- **Header ops** (e.g. `rst_memsize`) keep the cheap `_WindowHeaderView` — unchanged.
- **Pixel ops** get a windowed materialized dataset: the cached open source (per-partition LRU) is
  read for the requested `Window` via `_window_dataset_bytes(cached_src, Window)` and handed to
  `core_fn` as a real readable dataset. This removes the header-view asymmetry for pixel paths while
  preserving the single cached open per source.

The op declares which view it needs; the executor supplies the matching view. A pixel op never
receives a `read()`-raises view.

### 6.3 Memory-aware stability (from the cache-largefile + /vsicurl probes)

The increment-1 LRU/staging was tuned for header ops and is unsafe for large-file pixel reads.
Increment 2 corrects it:

1. **Weigh the LRU by actual file size**, not the 16 MiB nominal — otherwise the `GBX_LRU_MAX_BYTES`
   (4 GiB) byte-budget never fires (it counts handles, not bytes).
2. **Low, size-adaptive `max_count`** — 1–4 concurrent open large rasters, not 64. Large sources
   dominate RAM; a high handle cap is the OOM lever.
3. **Size-adaptive open:**
   - **small / moderate** → `.open()` stream (fast windowed reads; RAM ≈ file size, acceptable
     when small);
   - **large** → FUSE `.as_local_file()` (lazy, low-RAM, slower cold);
   - **never stream huge-striped** (a striped multi-GB stream-open crashes the worker).
   The cutover is a configurable byte threshold (`GBX_STREAM_MAX_BYTES`, default in the low-hundreds
   of MB) plus a striped-layout guard; both are validated on-cluster during implementation.
4. **`GBX_STAGE_MAX_BYTES` guard on `_stage_local_if_needed`** — today it has no size guard and will
   copy an arbitrarily large file to `/tmp`; add a cap (huge files never stage) **and fix the
   temp-file leak** (`is_temp` is discarded in `_make_opener`, so evicted temp copies are never
   deleted).
5. **`/vsicurl` is NOT pursued** — the probe proved a FileRef exposes no HTTP-range URL, so there is
   no lazy+low-RAM+fast third path. Size-adaptive stream/FUSE is the whole strategy.

**Framing for the perf docs:** stream = fast-windowed but memory-bounded (RAM ≈ file size); FUSE =
lazy + memory-safe + slower cold; the executor picks by size.

---

## 7. Documentation and re-benchmark

Written **after** the code lands (docs describe shipped, validated behavior):

- **Performance page** (`docs/docs/**/performance.mdx`) — a new section on **how GeoBrix drives
  performance via virtual tiles + the FILE type**: open-cost-dominated reads, per-partition
  open-amortization via the grouped executor, MANAGED ≈ EXTERNAL for reads, path-aligned write
  layout (ORDER BY / CLUSTER BY path), and the size-adaptive stream-vs-FUSE trade. Classify by
  SHAPE/family per existing conventions. **No internal vocabulary; no hard dates** ("DBR 19 is
  coming soon to Serverless with FILE support"). Grounding exists in
  `input/file_type/2026-08-17-read-perf-matrix-and-handling-rules.md`.
- **Tile Structure page diagram** — update to show the 9th field, `path_mode` (and its
  materialized/external/managed values). Backtick any bare `STRUCT<…>` in prose to avoid the MDX
  JSX crash.
- **Re-benchmark virtual tiles with and without FILE enabled** (given the perf improvements), using
  the existing bench harness (spark-path = 0 warmup / 1 measured). The numbers feed the Performance
  section.
- Wire any *new* doc page into `sidebars.js` in the same stroke (a section inside `performance.mdx`
  likely needs no sidebar change).

---

## 8. Scope, non-goals, follow-ups

**In scope (increment 2):** Task 1 heavy `path_mode`; §1 reader-2b (capability-gated,
tier-general helper factoring); §2 aggregators-as-external; §3 pixel-ops executor + 1:n generators +
M2 + memory-aware stability; §4 docs + re-bench.

**Non-goals (this increment):**

- **Existing-table `ALTER`** to add a FILE column (increment-1 scope: new-table CREATE only).
- **MANAGED ↔ EXTERNAL convert helpers.**
- **Heavy-tier virtual tiling / heavy FILE ops** beyond Task 1's `path_mode` schema field.
- **Serverless-GC FILE** — gated on environment v6; a dedicated probe runs when v6 is selectable.

**Follow-ups (tracked, not this increment):**

- **F1 — file-lister + underscore option.** Use the FILE column / `read_files` as the native index
  instead of a manual FUSE directory walk (kills the listing tax), generalized across **all**
  light-tier readers including vector; this is also where the `_`-prefixed include/skip option
  rides in. Deferred follow-up.
- **F2 — VectorX FILE.** FILE-backed vector readers + writers (shp/geojson/gpkg via pyogrio),
  leveraging stream reads / governed storage for performance. Its own subsystem (formats,
  geometry-not-tiles) — needs its own brainstorm → spec → plan; the tier-general FILE helpers from
  §1 are the reuse surface.

---

## 9. Task ordering (for the plan)

1. **Heavy `path_mode`** (§3) — close the cross-tier schema mismatch first.
2. **Reader-2b** (§4) — capability-gated managed-uri resolution + tier-general helper factoring.
3. **Executor view contract + M2** (§6.2) — header-vs-pixel views on `grouped_tile_map`.
4. **Memory-aware LRU/staging/size-adaptive open** (§6.3) — the stability corrections.
5. **Route `rst_clip` (pixel scalar) through the executor** — the first pixel op, end-to-end.
6. **Route the remaining pixel ops + 1:n generators.**
7. **Aggregators-as-external** (§5).
8. **On-cluster validation** — FILE round-trip across pixel ops / aggregators / generators on the
   warm dogfood cluster; opportunistically run the deferred perf 3-way and the `/vsicurl` confirm if
   a ≥512 MB COG is available (already answered N/A, so informational only).
9. **Docs + re-bench** (§7).

---

## 10. Testing strategy

- **Per-seam TDD locally** (`gbx:test:pyrx`) at each pure boundary: schema equality, `path_mode`
  derivation, reader-2b uri-stripping, view-kind selection, LRU weigher/eviction, stage guard.
- **Heavy `path_mode`** via `gbx:test:scala` in Docker + a cross-tier schema round-trip.
- **FILE round-trip once on dogfood** (classic DBR-19): write → read (2b) → pixel op / aggregator /
  generator over real MANAGED + EXTERNAL tables, plus the size-adaptive open and graceful-fallback
  paths.
- **Package-source changes run the affected unit suites**, not only doc-tests.
- **Graceful fallback** is asserted for every FILE path (FILE-unavailable → existing path, identical
  result).
