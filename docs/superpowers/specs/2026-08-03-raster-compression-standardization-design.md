# Raster Compression Standardization — Design

**Status:** Approved design (2026-08-03). Feeds an implementation plan. Precedes the CRS effort (Spec B).

**One-liner:** Give GeoBrix one consistent, efficient materialize story — ZSTD + dtype-predictor as the baseline for every raster-bytes production site across both tiers, an `auto` size-adaptive level as the default on all row-data writes (grounded by a real read/write benchmark to 1 GB), and a clean `compress` option surface on the raster writers.

---

## Problem

An inventory of every raster materialize/encode site (`.superpowers/sdd/compression-inventory.md`) found the materialize story is inconsistent and inefficient:

- **Light tier: ~20 of 28 write sites set NO compression** — they `profile.copy()` + `driver="GTiff"` and inherit the source's compression (or GDAL's uncompressed default for fresh dicts). The dominant virtual-tile path (`_window_dataset_bytes`, `materialize_to_bytes`) and the reader hot path (`_encode.encode_tile`) apply no compression policy. **Predictor is almost entirely absent** light-side (only `ds/_write.py` sets it, COG path only).
- **Default is DEFLATE**, not ZSTD, wherever compression *is* set (`cogCompression="DEFLATE"`, `_encode`, `cog_convert_file`, `prepare_cogs`, heavy `OperatorOptions` fallback). ZSTD is only used if a caller explicitly asks.
- **rasterio's own GTiff default is no compression** (LZW only via `DefaultGTiffProfile`), which is why the light no-op sites produce uncompressed or source-inherited output.
- **Heavy tier is consistent** — `OperatorOptions.appendOptions` applies DEFLATE + dtype-adaptive predictor (2 int / 3 float) + ZLEVEL=6 uniformly at the serialization gateway — but on DEFLATE, not ZSTD, and not size-adaptive. Two heavy sites bypass it: `RST_CogConvert` and `GDALRasterize` (no predictor).
- **Nothing is size-adaptive**, though tile payloads range from ~KB to the ~1.8 GiB Spark cell cap and COG masters to 10 GiB.

### Benchmark evidence (local, rasterio 1.5.0 / GDAL 3.12.1)

Real GeoBrix tiles (`.superpowers/sdd/compression-inventory.md` → benchmark table):

- **ZSTD-9 + predictor beats DEFLATE-6 on ratio in every dtype AND is ~2–3× faster to write and faster to read.** (int16: 11.30× vs 10.63×, write 29ms vs 81ms; float32 read 4ms vs 16ms.)
- **Predictor is decisive and free** — float32 without it is +12% larger (3629 vs 3231 KB); no write-time cost.
- **ZSTD-22 is not a default** — <1% ratio gain over 9 for 30–65× the write time (int16: 1887ms vs 29ms for 11 MB), and its RAM/thread cost is exactly the OOM risk the guidance and the Serverless limits warn about.
- **`none`/LZW are strictly dominated** — LZW even *expands* float32/uint8 (0.97×).

### The two hard limits this design must respect

Documented at `docs/docs/writers/cog.mdx#drivermode` and `docs/docs/api/large-rasters.mdx#writing-cogs-and-why`:

1. **Serverless worker per-task memory ceiling (~1 GB, fixed, unraisable).** A large single-source conversion must run on the driver (`driverMode`/`prepare_cogs`), streaming block-by-block at flat ~2 GiB peak. **A high compression level on a large payload spikes RAM** (ZSTD dictionary × parallel threads) → worker OOM. This makes size-adaptive level a **correctness requirement**, not just an optimization.
2. **`driverMode` long-write connection cancellation (~1 GB/min → `CancelledKeyException`).** Higher level = slower write = longer blocking `.save()` = more cancellation risk. `auto` must keep large-payload levels low so throughput stays high.

---

## Design

### 1. Baseline codec: ZSTD + dtype-predictor

Every site that produces raster bytes uses **ZSTD + predictor** by default. Predictor is chosen from the band dtype:

- **3** — float32/float64 (DEM, temperature, band math)
- **2** — int16/uint16/int32/uint32 (optical, SAR, NIR)
- **1** — uint8/int8 (categorical / small-range; no horizontal correlation to exploit)

This replaces the DEFLATE default and the ~20 light no-compression sites.

### 2. `auto` — the default level, size-adaptive

`auto` (the default `compress` value on row-data writes) selects the ZSTD level from a **decoded-size estimate** computed *before* encoding: `count × width × height × itemsize` (the resolver already computes this for `rst_memsize`; no trial encode). Always pairs with the dtype-predictor.

**Provisional thresholds (MUST be re-grounded by the benchmark task below — do not ship the guesses):**

| Decoded size | ZSTD level | Rationale |
|---|---|---|
| ≤ 64 MiB | 19 | cheap at this size; squeeze for shuffle/storage (Serverless per-tile budget is 64 MiB) |
| ≤ 256 MiB | 12 | balanced |
| ≤ 1 GiB | 6 | write-time + RAM start to matter |
| > 1 GiB | 3 | avoid worker OOM / driverMode cancellation |

The exact breakpoints and levels are set by the benchmark, not asserted here.

### 3. `auto` grounding benchmark (blocking design input — per user directive)

Before the thresholds are fixed, run a **read/write performance sweep across payload sizes in powers of two up to 1 GB** (e.g. 1, 2, 4, …, 1024 MiB decoded), for each representative dtype (float32, int16/uint16, uint8), measuring: encoded size (ratio), write time, **peak RSS during write** (the OOM signal), and read time. The sweep covers **both the popular comparison codecs and ZSTD at multiple settings**, so the results serve double duty — grounding `auto` AND becoming the published evidence table on the Materialized Compression doc page (§5b):

- **Comparison codecs:** `none`, `LZW` (+predictor), `DEFLATE` (+predictor, level 6) — the alternatives users know and may choose via `compress=`.
- **ZSTD at various settings:** levels spanning the fast/balanced/ultra bands (e.g. 3, 6, 9, 12, 16, 19, 22) with the matched dtype-predictor, plus at least one ZSTD-without-predictor point per dtype to show the predictor's effect.

Output a table (size × dtype × codec/level → ratio, write ms, peak RSS, read ms) + the chosen breakpoints, committed alongside the inventory/benchmark artifact AND rendered (or excerpted) on the doc page. The `auto` ladder is derived from where write-time and peak-RSS start to climb non-linearly for each size — grounded, not from the guidance table alone. This benchmark runs on realistic synthetic tiles (the bench harness can generate them) and, for the ≥256 MiB sizes that matter for the memory limits, is confirmed on Serverless if local RSS measurement is unrepresentative.

### 4. Central authority (no drift)

- **Light:** a new `pyrx.core.compression` module — `creation_opts(dtype, decoded_bytes=None, compress="auto", level=None, predictor=None) -> dict` — returns the rasterio creation-options (`compress`, `zstd_level`/`zlevel`, `predictor`) to merge into a profile. **Every** light write site routes its profile-building through it: `edit._write` (and its callers clip/init_nodata/set_srid/band/threshold/update_type), `open_tile._window_dataset_bytes`/`_warp_window_bytes`/`_empty_dataset_bytes`/`materialize_to_bytes`, `agg` (merge/combineavg/frombands/rasterize/derivedband/_reproject), `cellraster.cells_to_raster`, `tiling._write`/`_encoded_size_bytes`, `resample._write_resampled`, `warp.reproject_to_srid`, `analysis.proximity`/`viewshed`/`cog_convert`/`cog_convert_file`, `ops.build_overviews`, `_encode.encode_tile`, `_write.tile_to_bytes`. Ancillary sites (`stac/_download`, `_xyz_mosaic`, `vizx/_simplify`, `bench/*`) route through it too or are explicitly exempted with a reason.
- **Heavy:** extend `OperatorOptions.appendOptions` to make ZSTD+predictor / `auto` the standard (it already has a ZSTD branch at `OperatorOptions.scala:45`), and route the two bypass sites — `RST_CogConvert` (`.scala:113`) and `GDALRasterize` (`.scala:44`) — through it so they gain the predictor and standard.

`decoded_bytes=None` means "not size-known here" → the authority falls back to a fixed default level for that context (e.g. the reader tile path passes the window's decoded size; a context with no size uses the balanced default).

**Exemption — measurement-only encodes.** `tiling._encoded_size_bytes` re-encodes a dataset solely to *measure* its byte size for split-planning; that output is discarded. Compressing it is wasted CPU and — worse — would make the split budget reflect compressed size while the actual concern is decoded memory. It is explicitly **exempt** from the authority (keeps its current cheap encode, or better, computes decoded size directly without a full encode). Any other measurement-only or throwaway encode is likewise exempt with a stated reason.

### 5. Writer option surface

New unified option on the raster writers (`gtiff_gbx`, `cog_gbx`) and `prepare_cogs`/`prepare_cog`:

- **`compress`** = `"auto"` (default) | `"zstd"` | `"deflate"` | `"lzw"` | `"none"`.
- When `compress != "auto"`: optional **`compressLevel`** (int; zstd_level or zlevel by codec) and **`predictor`** (`1`/`2`/`3`; default = dtype-derived) refine it. When `compress == "auto"`, an explicit `compressLevel`/`predictor` is ignored with a **warning** (auto owns them).
- **`cogCompression`** is retained as a documented **alias → `compress`** for back-compat (beta, but a silent break of existing notebooks is worse than one aliased name; the alias is documented as deprecated-in-favor-of-`compress`). `cogCompression="DEFLATE"` still works and maps to `compress="deflate"`.

Python API force-output params on `rst_*` (`materialize`, `virtualize_dir`) gain no new compression args in this spec — they use `auto`; explicit control is via the writers. (A follow-up could thread `compress` through `virtualize_dir` if needed.)

### 5b. Docs: a "Materialized Compression" page (RasterX API)

A small dedicated page under the RasterX API section (e.g. `docs/docs/api/materialized-compression.mdx`, wired into `sidebars.js` near tile-structure / large-rasters) explaining, in end-user voice:

- **What "materialize" means** and why compression matters at that boundary (row-data / tile bytes and written files) — link [Virtual Tiles](./virtual-tiles), [Tile Structure](./tile-structure).
- **GeoBrix's approach:** ZSTD + a dtype-matched predictor is the baseline; `auto` (the default) adapts the level to tile size — small tiles squeeze hard, large tiles stay light to protect memory (link the [Large Rasters memory model](./large-rasters#memory-footprint-and-staying-on-standard-serverless) and the `driverMode` note). Explain the predictor plainly (why float vs int differ) without the internal vocabulary.
- **Where users have control:** the `compress` option (`auto` | `zstd`/`deflate`/`lzw`/`none`) and, when not `auto`, `compressLevel` + `predictor`; when to reach for each (e.g. `deflate` for files handed off to non-ZSTD tools; a fixed high level for a write-once catalog). The portability note (ZSTD needs a ZSTD-enabled GDAL off-cluster).
- **The evidence table:** the powers-of-2 benchmark (§3) rendered (or excerpted) on this page — popular codecs (none/LZW/DEFLATE) vs ZSTD at various levels, across sizes/dtypes, showing ratio + write + read + peak memory — so users see *why* `auto` chooses what it does and can judge a manual `compress` override against real numbers.
- Kept **small and practical** — approach + the option table + the evidence table + 2–3 examples, not an exhaustive codec treatise.

### 6. Cross-tier parity

The raster cross-tier gate is a **bit-parity gate on a small gridded fixture** (`bench/cluster.py:704`). Moving **both** tiers to the same ZSTD+predictor/`auto` standard keeps byte-equality valid (both encode identically). Requirements:

- **Verify the heavy GDAL JNI build has ZSTD early** (the code already emits a `COMPRESS=ZSTD` branch and `RST_CogConvert` lists ZSTD, so support is very likely — but confirm at runtime before relying on it). If absent, this design is blocked and falls back to DEFLATE+predictor as the shared baseline (still a big win over today).
- **Re-baseline the parity fixture** to the new bytes; confirm heavy and light produce identical ZSTD+predictor output for the fixture (same level via the same `auto` size class, same predictor via same dtype). If exact byte-equality across two independent GDAL builds proves brittle, fall back to a **decoded-pixel parity** assertion (open both, compare arrays + georeference + nodata) — a more robust long-term gate; decide based on what the re-baselined fixture actually shows.

---

## Non-Goals

- **CRS handling** — the separate Spec B, next.
- **No tile-struct schema change.**
- **Off-cluster portability** beyond: a docs note that ZSTD GeoTIFFs need a ZSTD-enabled GDAL to read, and the explicit `compress="deflate"` escape for files handed off outside the cluster. (In-cluster GDAL — light rasterio 3.12 + heavy JNI — has ZSTD.)
- **No new codecs** beyond zstd/deflate/lzw/none (no LERC/JPEG/WEBP in this spec, though `compress="<alg>"` doesn't forbid GDAL-supported names).

---

## Testing

1. **`creation_opts` unit tests:** dtype→predictor (1/2/3); `auto` level from decoded_bytes at each threshold boundary; explicit `compress`+`level`+`predictor` honored; `auto`+explicit-level warns; unknown codec handled; `none` produces uncompressed.
2. **Every routed light site** produces ZSTD+predictor output by default (assert `rasterio.open(bytes).compression == Compression.zstd` and the predictor tag) on real rasters — a representative test per site-family (edit, open_tile window/materialize, agg, tiling, warp, resample, analysis, _encode).
3. **Writer option tests:** `gtiff_gbx`/`cog_gbx` with `compress="auto"` (adaptive), `compress="deflate"` (+ level/predictor), `compress="none"`; `cogCompression` alias still works; `auto`+explicit-level warns.
4. **Size-adaptive correctness:** a small tile and a large tile through `auto` get different levels (assert the level via the decoded-size→level mapping); the large path does not OOM (the benchmark + a bounded Serverless check).
5. **The grounding benchmark** (§3) — its output table + chosen thresholds committed; `auto` ladder matches it.
6. **Cross-tier parity:** the re-baselined raster fixture passes (byte or decoded-pixel per §6); a light tile and a heavy tile of the same fixture agree.
7. **Heavy:** `RST_CogConvert` and `GDALRasterize` outputs now carry the predictor; `OperatorOptions` ZSTD path exercised.
8. **Docs:** a dedicated **Materialized Compression** page under the RasterX API section (see below), plus the `compress` option wired into the writer pages' option tables + a cross-link from large-rasters; voice clean; docs build green; new page added to `sidebars.js`.

---

## Files (anticipated; finalized in the plan)

- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/compression.py` (`creation_opts` + the dtype→predictor + size→level tables).
- Modify (route through the authority): `pyrx/core/edit.py`, `open_tile.py`, `agg.py`, `cellraster.py`, `tiling.py`, `resample.py`, `warp.py`, `analysis.py`, `ops.py`; `ds/_encode.py`, `ds/_write.py`, `ds/writer.py`, `ds/cog_writer.py`, `pyrx/core/preparer.py`; ancillary `stac/_download.py`, `ds/_xyz_mosaic.py`, `vizx/_simplify.py`.
- Modify (writer option surface): `ds/raster.py`, `ds/cog_writer.py`, `ds/writer.py`, `preparer.py` (parse `compress`/`compressLevel`/`predictor`, alias `cogCompression`).
- Modify (heavy): `src/main/scala/com/databricks/labs/gbx/rasterx/operator/OperatorOptions.scala`, `.../expressions/analysis/RST_CogConvert.scala`, `.../operations/GDALRasterize.scala`.
- Benchmark: extend `bench/` (readers/synth) with the powers-of-2 compression sweep; commit the results + thresholds.
- Tests: `python/geobrix/test/pyrx/test_compression.py` (authority) + per-site-family assertions in existing test dirs; heavy Scala tests for the two bypass sites; parity fixture re-baseline.
- Docs: **create** `docs/docs/api/materialized-compression.mdx` (the dedicated RasterX page, §5b) + wire into `docs/sidebars.js`; **modify** `docs/docs/writers/cog.mdx`, `docs/docs/writers/geotiff*.mdx`, `docs/docs/api/large-rasters.mdx` (option tables + cross-link), `docs/docs/api/benchmarking.mdx` (if the bench gate changes).

---

## Open items for the plan

- The exact `auto` breakpoints + levels (output of §3).
- Whether the parity gate stays byte-identical (re-baselined) or moves to decoded-pixel (§6) — decided by what the re-baseline shows.
- Whether `_encoded_size_bytes` (split-planning) should use a cheap/no-compression encode (it only measures size for planning — compressing it is wasted work; likely exempt with a reason).
- Whether ancillary sites (`vizx/_simplify` LZW-for-overviews, `bench` synth) opt in or are exempted.
