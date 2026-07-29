# Large-Raster Reader — Design Spec

**Date:** 2026-07-29
**Branch:** `feature/large-raster-reader`
**Status:** Design approved (pending written-spec review)
**Release framing:** This feature is significant enough to *be* the GeoBrix **0.4.4** release (current is 0.4.3), not an incremental patch.

## Problem

A user tried to load VIIRS nightlights data for the UK (a large, **striped** GeoTIFF) into the lightweight (pyrx) raster reader on a Databricks Serverless cluster and it **OOM'd**. The reader could not ingest the file.

Two root causes:

1. **Default is "never split."** The light reader's `sizeInMB` defaults to `-1` → one file becomes one row in a single Python UDF task. On Serverless the UDF is capped near **~1 GB**, so a large raster materializes past the cap (or trips the ~1.8 GiB Spark BinaryType cell guard) before any operation runs.
2. **Striped-layout strip inflation.** Even with splitting enabled, the current split uses a power-of-4 rectangular grid (2^k × 2^k tiles). On a **striped** GeoTIFF (data stored in full-width horizontal strips), reading any rectangular sub-window forces GDAL to inflate whole strips (full raster width × strip height) regardless of the window's width. The in-code claim that "per-task RAM stays ~tile-sized regardless of source size" is **only true for internally-tiled sources**. Striped sources defeat naive rectangular tiling — the exact amplifier the user suspected.

A secondary latent bug: the current tile-count math (`_get_tile_size`) keys on **encoded** byte size. A highly compressed raster (small encoded, large decoded) can be under-split into tiles that decode past the memory cap.

## Goals

1. **Robust large-raster ingest (objective 1).** "Just point the reader at VIIRS" works out of the box on Serverless — no manual `sizeInMB` tuning required.
2. **Performance via COG standardization (objective 2).** Emit Cloud-Optimized GeoTIFF (COG) tiles where it pays off, so downstream windowed / multi-resolution operations get faster. Establish a documented POV that COG is GeoBrix's interchange format for processed/large rasters.

## Non-goals (this spec)

- **Full consumer audit (C).** Rewiring *every* COG-benefiting function to exploit overviews, plus overview-based "approx" stat/terrain variants. This spec does **one** representative consumer rewrite (`rst_resample*`) to prove the payoff; the full audit is the committed follow-on.
- **Heavy-tier implementation.** Ported as an immediate fast-follow (see Roadmap), not built here.
- **Transparent COG-ify-then-read normalization** (convert pathological source to a staged COG before reading). Layout-aware chunking handles one-shot ingest without it; captured as a future escape hatch only.

## Approach summary

- **Layout-aware chunking, no source rewrite.** Detect striped vs tiled at open; chunk along the axis the physical layout makes cheap (row-bands for striped, block-snapped grid for tiled). Never rewrite the source file.
- **Auto-chunk by default, keyed on a decoded-memory budget** — not encoded bytes, not the cell guard.
- **Two orthogonal option axes:** `splitStrategy` (robustness / runtime-aware budget) × `tileFormat` (gtiff | cog interchange format). They compose; they are not one conflated enum.
- **COG default is `auto`:** COG when the reader splits (marginal cost on a re-encode we already pay; real downstream upside), source-format passthrough when it doesn't (preserve the ~80× fast path for the common small-file case).
- **COG detection contract:** internals route COG-aware code paths without decoding the payload, via a reader-stamped metadata flag (fast path) plus a decode-free header sniff (fallback for old/foreign tiles).
- **Complete producer round-trip:** the reader *produces* COGs; the `gbx_gtiff` writer can *force-convert* existing tiles to COG.

---

## Section 1 — Option surface (two orthogonal axes)

The reader (`RasterGbxReader` and all named subclasses: `gtiff_gbx`, `netcdf_gbx`, …) gains two independent options.

### Axis 1 — `splitStrategy` (robustness / runtime-aware memory budgeting)

| Value | Meaning |
|---|---|
| `auto` *(default)* | Resolves to `serverless` when running on Serverless, else `classic`. |
| `serverless` | Decoded-memory budget ~**512 MB/tile** (headroom under the ~1 GB UDF cap). More, smaller tiles. |
| `classic` | Larger budget (~**1.5–2 GB/tile**, still under the ~1.8 GiB cell guard). Fewer, larger tiles. |
| `none` | Never split — one file → one row (today's `-1` behavior; explicit opt-out). |

### Axis 2 — `tileFormat` (performance / interchange format)

| Value | Meaning |
|---|---|
| `auto` *(default)* | **COG when the reader splits; source-format passthrough when it doesn't.** |
| `cog` | Always emit COG (transcode even unsplit tiles). |
| `gtiff` | Always plain GeoTIFF (today's behavior; disables COG emission). |

### `sizeInMB` demotion

`sizeInMB` becomes a **power-user override** of the auto-budget. When set > 0 it pins the per-tile target directly, bypassing the strategy's computed budget. `splitStrategy=none` with `sizeInMB` unset reproduces today's exact one-row-per-file behavior — the explicit escape hatch to old semantics.

### Behavior-change callout

The default flips from "never split" to "split large rasters automatically." This is the fix for "just point it at VIIRS." Documented in `beta-release-notes.mdx` as a behavior change.

### Why two axes, not one enum

All four `splitStrategy × tileFormat` combinations are meaningful (`none+cog` = "don't split this modest file but transcode to COG for fast later ops"; `classic+gtiff`; etc.). Folding format into a single `splitStrategy` enum (`"cog"` as a strategy value) forecloses those combos and forces a combinatorial enum later. `cog-custom` COG-creation controls belong on the *format* axis / writer options, not as strategy values.

---

## Section 2 — Layout-aware chunking (the strip-inflation fix)

The core robustness engine — makes the memory budget actually hold on striped rasters.

At `rasterio.open()` (header only, already happening today), read `profile["tiled"]` and block shape (`ds.block_shapes`):

- **Tiled source** (internal blocks, e.g. 256×256 / 512×512): use the existing **power-of-4 grid**, but **snap tile boundaries to block boundaries** so each read pulls whole blocks with no cross-block waste.
- **Striped source** (full-width strips): chunk **by row-bands only** — never column-split. Each tile is `full_width × N_strip_rows`, where `N` is chosen so `full_width × N × bytes_per_pixel × bands` fits the decoded-memory budget. Reads whole strips in order, so per-task RAM is bounded by the budget as intended.

### Decoded-memory budget is the driver

Budget is computed on **decoded** size (`width × height × bands × dtype_bytes`), not encoded bytes — because the ~1 GB UDF cap governs the in-memory NumPy array, and DEFLATE-encoded size wildly understates decoded footprint. This corrects the latent under-split bug where highly compressed rasters produced tiles that decoded past the cap.

### Split-count cap

The existing **≤ 512 split** guard is preserved to bound row explosion. If the budget would demand more than the cap, tiles grow past budget and the reader **logs a warning** (documented degradation, not a crash).

### Design decisions flagged (not glossed)

- **Row-band tiles are wide, not square.** A striped UK VIIRS raster becomes a stack of full-width horizontal bands. Correct for ingest and per-tile ops, but tile aspect ratio now depends on source layout — documented so users aren't surprised.
- **Reprojection/warp interaction:** non-square tiles remain correct (each carries its own geotransform); no special handling.

---

## Section 3 — COG detection contract (`detect_cog`)

How internals route COG-aware code paths **without decoding the payload**.

### Storage — in the tile `metadata` map (not the struct)

Format signal lives in the tile's `metadata: map<string,string>` alongside `driver`. This avoids a struct schema change (no binding-parity churn), survives serde, and keeps `(cellid, raster, metadata)` stable. Namespaced keys avoid colliding with GDAL-native metadata:

- `gbx_format` = `"cog"` | `"gtiff"`
- `gbx_blocksize` = e.g. `"512"`
- `gbx_overview_levels` = e.g. `"5"`

### Resolution — one shared helper `detect_cog(tile) -> CogInfo`

Every COG-aware function calls this; none rolls its own detection.

1. **Fast path:** `metadata["gbx_format"]` present → trust it, zero bytes touched. (New data — our reader always stamps for free since it just encoded the tile.)
2. **Fallback path:** absent → **header-sniff** the bytes: parse TIFF header + first IFD only (a few hundred bytes, no pixel decode), check for internal tiling + overview IFDs. (Old/foreign data processed before this feature.)

### Two standing rules (written into the spec, enforced by tests)

- **R1 — uniform detection.** All COG-aware functions call `detect_cog`; none reimplement sniffing. Divergent detection is the bug this prevents.
- **R2 — metadata-carry discipline.** Every op that emits a tile must propagate/refresh `metadata` (especially the `gbx_*` keys). Non-COG-aware functions must not drop the flag. Kin to the repo's WKB-bridge and binding-parity rules. Backed by a test asserting flag survival across a representative op chain (COG-aware → non-COG-aware → COG-aware).

### Metadata healing — `stamp_format_metadata`

There is no existing healing function today; metadata is whatever the encoder set. Introduce a single shared primitive `stamp_format_metadata(raster_bytes, existing_metadata) -> metadata` that:

- sniffs the bytes once (reusing the **same header-peek core** as `detect_cog` — one parser, not two),
- writes `gbx_format` / `gbx_blocksize` / `gbx_overview_levels` to match the **actual current bytes**,
- returns the merged map.

**Key invariant:** `detect_cog` (read/route) and `stamp_format_metadata` (write/heal) share one header-sniff core, so they can never disagree — this is what makes R1 enforceable rather than aspirational.

Healing falls out in two places:

- **Heal-on-write (the guarantee):** any op emitting a tile calls `stamp_format_metadata` on its output. Because it re-derives from actual bytes, an op that accidentally transcoded (or dropped the flag) is *corrected*, not merely preserved.
- **Heal-on-read (opportunistic):** when `detect_cog` hits the sniff fallback (old data, no flag), it returns the derived info so a caller *may* persist it. We do **not** force a tile rewrite mid-pipeline.

---

## Section 4 — Components & data flow

### New shared core — `pyrx/core/cog.py`

- `_sniff_tiff_header(raster_bytes) -> HeaderInfo` — decode-free TIFF header + first-IFD parse (tiled?, blocksize, overview IFDs). The single source of format truth.
- `detect_cog(tile) -> CogInfo` — R1 resolver: metadata fast-path → sniff fallback.
- `stamp_format_metadata(raster_bytes, existing_metadata) -> metadata` — R2 writer/healer; re-derives `gbx_*` from actual bytes via the same sniff core.
- `decoded_budget_for(strategy, runtime) -> int` — resolves `auto → serverless|classic` and returns the per-tile decoded-byte budget.

### Changed — reader (`ds/raster.py` + `ds/_encode.py` + `pyrx/core/tiling.py`)

- `partitions()` resolves `splitStrategy=auto` at the **driver** (Serverless probe → bake concrete budget into `_FilePartition`, same flow as `size_mib` today — honors the no-spark-config rule: no `spark.conf.set` / `_jvm` / `.rdd` in the reader).
- `read()` opens header, reads layout (`tiled`, `block_shapes`), computes chunking via the **decoded** budget: tiled → block-snapped power-of-4 grid; striped → row-bands.
- `_encode.py` `encode_tile` gains `tileFormat` awareness: emit COG when splitting under `auto` (or always under `cog`); each emitted tile passes through `stamp_format_metadata`. `passthrough_tile` stamps source format (source may already be a COG).

### Changed — writer (`gbx_gtiff` writer)

New options for force-converting existing tiles to COG on write:

```python
df.write.format("gbx_gtiff") \
  .option("cog", "true") \
  .option("cogBlockSize", "512") \
  .option("cogOverviews", "auto") \    # auto | none | <levels>
  .option("cogCompression", "deflate") \
  .option("cogPredictor", "2") \
  .save(path)
```

Per tile: already-COG (`detect_cog`) → passthrough (don't re-encode); else run the **same rio-cogeo conversion `rst_cog_convert` uses** (shared core — not a third COG-writing implementation) → `stamp_format_metadata`. This is the earlier `cog-custom` idea, correctly located on the writer's format axis.

### Changed — the one consumer rewrite (`rst_resample*` → `pyrx/core/resample.py`)

Replace `ds.read(out_shape=...)` (full-res read then in-memory decimation) with an overview-aware read: when `detect_cog` reports overviews, read from the nearest overview level ≥ target, then final decimation. Non-COG input keeps today's path (correctness parity). This is the single clear latent win from the function inventory and the proof that the COG default pays off beyond the already-free rio-tiler path (`rst_tilexyz` / `rst_xyzpyramid`).

### Data flow — large striped VIIRS, default options, Serverless

```
file → partitions(): auto→serverless, budget=512MB decoded, baked into _FilePartition
     → read(): open header → striped detected → row-band chunking to budget
     → per band: rasterio window read (whole strips, bounded) → encode_tile → COG
                 → stamp_format_metadata → metadata[gbx_format]=cog
     → rows: (source, tile{cellid=-1, raster=<cog bytes>, metadata{...,gbx_format:cog}})
downstream rst_tilexyz / rst_resample → detect_cog → overview-aware fast path
```

---

## Section 5 — Testing, error handling, edge cases

### Testing (TDD; light doc-tests execute real code on real sample data — no mocking Spark/GeoBrix/IO)

- **Layout chunking:** synthetic striped GeoTIFF + synthetic tiled GeoTIFF → striped yields full-width row-bands (never column-split); tiled yields block-snapped grid. Per-tile decoded size ≤ budget.
- **Decoded-budget correction:** highly-compressed raster (small encoded, large decoded) → splits on *decoded* size, not encoded (latent-bug regression test).
- **Strategy resolution:** `auto` → `serverless`/`classic` budget; `none` reproduces today's one-row behavior exactly; `sizeInMB` override pins the target.
- **Detection contract:** COG bytes with metadata → fast path (assert no decode); COG bytes without metadata → sniff detects; plain GTiff → both paths report `gtiff`. `detect_cog` and `stamp_format_metadata` agree (shared core).
- **R2 flag survival:** op chain COG-aware → non-COG-aware → COG-aware; assert `gbx_format` survives/heals across it.
- **tileFormat:** `auto` → COG when split, source passthrough when not; `cog` always COG; `gtiff` never COG. Passthrough fast-path preserved for small unsplit files.
- **Writer:** `.option("cog","true")` converts non-COG tiles, passes through already-COG tiles; COG-creation options honored (blocksize/overviews).
- **Resample rewrite:** COG input reads from overview (fewer bytes decoded vs full-res); non-COG parity with today's output.
- **End-to-end:** the actual user scenario — large striped raster, default options, stays under budget, emits valid COG tiles.

### Error handling / edge cases

- Budget demands > 512-split cap → grow tiles past budget + **warn** (documented degradation, not crash). Existing ≤512 guard preserved.
- Single strip already > budget (pathological) → warn, emit it; COG-ify normalization noted as the real future fix.
- Multi-band dtype in budget math (`width × height × bands × dtype_bytes`).
- `passthrough_tile` must still stamp `gbx_format` (source may already be a COG).
- Corrupt/truncated header in sniff → treat as non-COG (safe default), don't raise.
- NetCDF reader transcodes to GeoTIFF tiles already → inherits COG emission + stamping.

### Perf-parity gate (repo rule)

Resample-on-COG must be ≥ today's speed; the split path must not regress small-file passthrough. Benched before the consumer rewrite lands.

### Docs deliverables

- `readers/raster.mdx` — two axes, default-flip, striped-raster guidance.
- `beta-release-notes.mdx` — behavior change (default flips to auto-split).
- COG POV statement — where it pays off *now* (XYZ/tile serving via rio-tiler, resample) vs. follow-on; honest, not aspirational.
- Writer COG options page.
- Sidebar wiring (`sidebars.js`) for any new page.

---

## Section 6 — Large-raster benchmarking mandate

This feature's entire reason to exist is large striped rasters, so benchmarking is a **first-class, gating requirement** — distinct from the existing small-corpus bench profile (pure-core 1 tile / spark-path 1000 tiles). Applies to the light tier now and, as a gate, to the heavy fast-follow.

- **Corpus = the actual problem.** Real large striped GeoTIFFs at VIIRS / UK-extent scale (multi-GB decoded), plus a tiled COG counterpart of the same data, plus a pathological single-giant-strip case. Not the small-corpus defaults.
- **Push to failure, not just timing.** Establish the OOM envelope: what source size × strategy budget survives vs. crashes on Serverless (~1 GB) and on a sized heavy executor. The deliverable includes the *envelope*, not just latencies.
- **Light-vs-heavy head-to-head.** Same corpus, same operations, both tiers, compared directly: ingest throughput, per-tile memory high-water, and downstream resample/XYZ ops on COG vs plain. Tells us whether heavy parity is worth its cost and where each tier wins.
- **Striped-vs-tiled delta.** Quantify the strip-inflation amplifier: same raster striped vs COG-tiled, showing the memory/throughput gap that motivated layout-aware chunking.
- Honors existing bench discipline (verify rows > 0 before reporting, stamp corpus size, per-function progress output, summary.md link) but extends the harness with this feature's large-corpus profile — it must not reuse the small-corpus defaults.
- Any benchmarking change is reflected in `docs/docs/api/benchmarking.mdx` in the same stroke (repo rule).

---

## Roadmap / sequencing

1. **This spec (light tier, scope B):** robust large-raster ingest + two axes + COG detection contract + `stamp_format_metadata` + writer COG options + the one `rst_resample*` consumer rewrite + large-raster benchmarking. **= GeoBrix 0.4.4.**
2. **Heavy-tier parity (immediate fast-follow):** port layout-aware chunking + decoded-budget + `tileFormat` COG axis to the Scala reader. `splitStrategy` values become `auto | classic | cluster | none` (no `serverless`; heavy doesn't run on Serverless). **Gated on the light-vs-heavy head-to-head large-raster benchmark.**
3. **Full consumer audit (scope C):** rewire every STRONG-benefit COG-aware function to exploit overviews; add optional overview-based "approx" stat/terrain variants. Builds on `detect_cog` + `stamp_format_metadata` + R1/R2 from this spec. Nothing ships to users until C is reached (explicit user constraint).

## Open risks

- **Serverless-detection probe mechanism** (for `splitStrategy=auto`) must resolve at the driver without violating the no-spark-config rule. Mechanism to be nailed in the implementation plan; flagged as a known risk, not a blocker.
- **Header-sniff correctness** — the decode-free TIFF/IFD parser must correctly distinguish COG (tiled + overviews) from plain tiled and from striped. Corrupt-header inputs default to non-COG.
- **rio-cogeo determinism** — the writer and `rst_cog_convert` must share exactly one conversion path so outputs are identical regardless of entry point.
