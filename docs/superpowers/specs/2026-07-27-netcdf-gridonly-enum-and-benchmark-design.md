# NetCDF grid-only enumeration + at-scale benchmarking — Design

**Date:** 2026-07-27
**Status:** Design (pending plan).
**Branch:** `feature/netcdf-phase2`
**Relates:** `2026-07-27-netcdf-heavy-readers-design.md` (the reader this fixes), `netcdf-heavy-reader-light-writer` (memory), `heavy-bench-uc-volume-io`, `bench-changes-update-docs`, `benchmarking-preflight-discipline`.

## 1. Problem

Post-merge cluster validation of the heavy `netcdf_gdal` reader (steps 3–4 of the operational
follow-up) surfaced two real defects and one data-fit miss:

1. **FIXED (committed `7ba0d2dd`):** `NetCDF_Batch`'s subdataset-enumeration UDF ran
   `GDALManager.init` but not `NodeFileManager.init(exprConfig.hConf)`. `NodeFileManager.hconf`
   is a JVM-static set only on the driver, so on executor JVMs `readRemote` hit a null hconf,
   threw, and was swallowed by the UDF's blanket `catch(Throwable)` → enumeration returned zero
   rows on any multi-executor cluster reading a `/Volumes` path. Verified fixed on a 20-worker
   cluster.

2. **THIS SPEC — enumeration fan-out.** `NetCDF_Batch` plans **one partition per (file ×
   subdataset)** and keeps every subdataset GDAL opens as `>1×1` (minus `_bnds`/`_bounds`). A
   real Sentinel-5P L2 CH4 granule exposes **85 subdatasets** (verified via `gdalinfo`), the
   vast majority 1-D/support arrays under `/PRODUCT/SUPPORT_DATA/...`. So 409 files × 85 ≈
   **~35k partitions** — a benchmark that never converges (~3.5 h). The light `netcdf_gbx`
   reader does not have this problem: it enumerates via `classify()` and keeps only true grid
   (or, in vector mode, DSG/curvilinear) variables.

3. **Data-fit miss.** S5P L2 CH4 is a **swath**: verified via `gdalinfo` that
   `/PRODUCT/methane_mixing_ratio` is 215×3736 with **no geotransform, no CRS, a GEOLOCATION
   array** (per-pixel lat/lon). It is not a regular raster grid. The light tier already treats
   S5P as `CURVILINEAR` and routes it to vector mode (per-pixel points); heavy raster correctly
   should enumerate ~nothing for it. So **S5P can never produce a heavy-vs-light raster
   throughput number** — it is a *vector* corpus. The only regular-grid NetCDF we have staged
   is toy-scale (coral 6.9 MB; CMIP5/ECMWF single files).

**Goal:** (a) fix the enumeration fan-out so `netcdf_gdal` enumerates only true georeferenced
grid variables (fast + cross-tier consistent), and (b) stand up an at-scale benchmark that is
honest about the raster/vector split — a real regular-grid raster corpus for heavy-vs-light
raster, and the existing S5P swath corpus for the light-tier vector path.

## 2. Grounding facts (verified 2026-07-27 on-cluster / via PC REST)

- **S5P granule:** 57 MB, **85 subdatasets**; `/PRODUCT/methane_mixing_ratio` = 215×3736,
  `has_geotransform=False`, `has_gcps=False`, `geolocation_array=True`, no coordinate system.
- **coral `bleaching_alert_area`:** 7200×3600, `has_geotransform=True` — a real regular grid.
- **`NodeFilePathUtil`** caches remote→local by path with refcounting: repeated
  `readRemote(sameFile)` on one executor JVM reuses the copy (no re-download). So the fan-out
  cost is **partition/task count + per-partition HDF5 open**, not re-staging bytes.
- **Planetary Computer (anonymous, reachable):** `nasa-nex-gddp-cmip6` serves
  `application/netcdf` assets — one asset per climate variable (`pr`, `tas`, `tasmax`, …),
  global **regular 0.25° lat/lon grid** (720×1440, `cube:dimensions` present), one file per
  (model, scenario, year). GB-scale is trivial (thousands of items). This is the raster corpus.
- **`TropomiDownloader`** is the template: `StacClient(catalog=PC, sign="planetary_computer")`,
  `.download(bbox, out_dir, temporal=, spark=)`, download-and-stop, distributed, Serverless-safe.
- **Alternate raster corpus (noted, not chosen):** the EUPP ESSD-benchmark-datasets
  (EUMETNET Postprocessing Benchmark) are curated, citable regular-grid NWP NetCDF over Europe.
  Higher benchmark pedigree, but distributed via Zenodo/climetlab — a new fetch path. NASA-NEX
  is chosen for fit (reuses the existing PC/StacClient machinery, zero new download plumbing).

## 3. Design

### 3.1 `NetCDF_Batch` grid-only enumeration (the core fix)

Change the enumeration filter in `NetCDF_Batch.planInputPartitions`'s UDF so a subdataset is
kept only when it is a **true georeferenced 2-D grid**, not merely `>1×1`.

**Keep a subdataset iff**, after `gdal.Open` of its selector:
- `GetRasterXSize > 1 && GetRasterYSize > 1 && GetRasterCount >= 1` (existing), **AND**
- it has a real affine georeference: **a non-identity geotransform OR a projection/CRS**.
  Concretely: `ds.GetProjectionRef` is non-empty, **or** `ds.GetGeoTransform` differs from the
  GDAL default identity `[0,1,0,0,0,1]`. (A swath subdataset like S5P methane returns the
  identity transform and an empty projection — so it is dropped, matching the light
  `classify()` which returns `CURVILINEAR` and excludes it from raster mode.)

Also drop the `_bnds`/`_bounds` name filter's siblings that are pure coordinate arrays if they
slip through (they won't pass the geotransform test anyway; the name filter stays as a cheap
pre-filter).

**Effect:**
- coral / CMIP / NASA-NEX regular grids → kept (they have geotransform+CRS). Fast: one
  partition per real grid variable, matching light.
- S5P swaths → **0 grid variables kept** (no geotransform) — heavy raster enumerates nothing,
  exactly matching the light raster contract. No 85-way fan-out.
- The optional `variable`/`variables` filter still applies on top (intersect with kept set).

**Edge case — deliberate swath raster read:** out of scope. If a user wants S5P as raster they
use light vector mode (points) or `netcdf_ogr` for DSG; heavy raster is regular-grid-only, and
this is already the documented cross-tier asymmetry (from the prior spec + `benchmarking.mdx`).

This also means the "log the swallowed cause" hardening from `7ba0d2dd` stays — a genuine
per-file failure still prints rather than vanishing.

### 3.2 CMIP6 gridded-raster downloader

New `sample/nasanex.py` (mirrors `sample/tropomi.py`), class `NasaNexDownloader`:
- `catalog = PLANETARY_COMPUTER`, `collection = "nasa-nex-gddp-cmip6"`, `sign =
  "planetary_computer"`.
- `download(bbox, out_dir, temporal=, variables=("tas",), models=(...), spark=)` — discovers
  items (driver-side, metadata-only), filters assets to the requested climate variables, and
  fans out `StacClient.download(...)` saving each as `{item_id}_{asset}.nc` on the Volume.
  Download-and-stop; Serverless-safe (no `spark.conf.set`/`_jvm`/`.rdd`).
- Anonymous — no token (verified). Scale dialed by `temporal` window × `variables` × `models`.
- A convenience `download_nasanex_aoi(spark, bbox, out_dir, **kw)` wrapper, matching the
  `download_tropomi_aoi` pattern.

Rationale for a downloader (vs. ad-hoc staging): consistency with the existing sample family,
reuse of `StacClient` credential/sign/retry handling, and it becomes the documented recipe the
bench references — same shape as `stage_netcdf_corpus` already expects.

### 3.3 Bench harness — honest raster/vector split

The reader-bench cell (`bench/cluster.py::_CELL_NETCDF`) currently benches **both** tiers over
`{CORPUS}/netcdf` in raster mode. Split it to reflect what each corpus supports:

- **Raster leg** (`{CORPUS}/netcdf` = **NASA-NEX gridded** granules): heavy `netcdf_gdal` vs
  light `netcdf_gbx` **raster mode**, same corpus — the real heavy-vs-light raster number.
- **Vector leg** (`{CORPUS}/netcdf-swath` = **S5P** granules): light `netcdf_gbx` **vector
  mode** only (swath→points), with an explicit note that heavy has no swath path (so no
  heavy-vs-light here — it is a light-tier throughput number, not a comparison).

Corpus staging: `stage_netcdf_corpus` gains a `collection`/`downloader` parameter (or a sibling
`stage_nasanex_corpus`) so the raster pool is NASA-NEX and the swath pool is S5P. Both live
under the **default** bench corpus root (`{CORPUS}/netcdf`, `{CORPUS}/netcdf-swath`) so the
top-level `corpus.json` scaffold the notebook reads at cell 257 is present (the coupling that
broke the first full run — staging under the standard corpus root avoids repurposing
`GBX_BENCH_CORPUS`).

**`sizeInMB` for a like-granularity comparison:** `run_format_read` applies `sizeInMB` only for
`raster_gbx` today. Extend it to pass `sizeInMB` to `netcdf_gdal` too, so heavy can be told
"one tile per subdataset" (`sizeInMB <= 0`) to match light's one-tile-per-variable granularity
— otherwise heavy subdivides each grid via `BalancedSubdivision` and the throughput compares
different tile counts. Default the netcdf bench to `sizeInMB=-1` (no split) for the fair
comparison; a separate tiled run is a follow-up if desired.

**Scale discipline** (`benchmarking-preflight-discipline`): stage a bounded, non-empty corpus
(e.g. 20–50 NASA-NEX granules for the raster leg, a bounded S5P subset for the vector leg),
log the granule counts (no silent truncation), stamp the actual worker count, and give the
run's `summary.md` link at the end (`bench-run-give-summary-link`).

### 3.4 Latent harness coupling (fix in this cycle)

`bench/cluster.py:257` reads `{CORPUS}/corpus.json` unconditionally at notebook top-level, so a
`--netcdf-only` run still hard-requires the full function-bench corpus scaffold. Make that read
lazy / guarded so a reader-only run does not require the function-corpus `corpus.json`. Small,
removes a foot-gun that already cost one failed run.

## 4. Testing

- **Scala unit (`NetCDF_DataSourceTest`, `PlanTest with SilentSparkSession`):** add a case that
  a synthetic swath-like `.nc` (2-D var, no geotransform) enumerates **0** grid partitions, and
  a regular-grid fixture enumerates its grid variable(s). The coral fixture (has geotransform)
  should still enumerate its 2 grid vars.
- **Cross-tier parity (existing `test_netcdf_cross_tier.py`):** unchanged contract — on the
  gridded coral fixture heavy and light enumerate the same variable set. Add an assertion that
  a swath fixture yields the same (empty) raster set on both tiers.
- **Downloader:** unit-test `NasaNexDownloader` discovery/asset-filter with an injected STAC
  client seam (mirror `tropomi` tests); the live download is exercised only in the staging step
  (guarded/skipped when offline).
- **Bench:** the harness path is smoke-tested (imports, skip-clean when corpus empty) as in the
  prior cycle; the actual at-scale run is human-gated on the cluster.

## 5. Surfaces to update

- `src/main/scala/.../rasterx/ds/netcdf/NetCDF_Batch.scala` — geotransform/CRS grid filter.
- `src/test/scala/.../rasterx/ds/NetCDF_DataSourceTest.scala` — swath-enumerates-0 test.
- `python/.../sample/nasanex.py` (new) + `sample/__init__.py` export; `sample` tests.
- `python/.../bench/readers.py` — `run_format_read` passes `sizeInMB` to `netcdf_gdal`;
  `stage_*` for NASA-NEX; possibly a `stage_netcdf_swath_corpus` for S5P.
- `python/.../bench/cluster.py` — split `_CELL_NETCDF` into raster (NASA-NEX) + vector (S5P)
  legs; make the top-level `corpus.json` read lazy.
- `docs/docs/api/benchmarking.mdx` — document the two corpora (NASA-NEX raster recipe + S5P
  swath vector recipe), the raster/vector split, and the swath-is-vector-only note
  (`bench-changes-update-docs`).
- `docs/docs/readers/netcdf.mdx` — reinforce: heavy `netcdf_gdal` = regular grids only; swaths
  → light vector mode (already partly documented; make sure the grid-only enumeration is
  explicit).

## 6. Risks

- **Geotransform test correctness.** The identity-transform check must not drop a legitimately
  north-up grid whose transform happens to be identity-like (rare, and such a grid has no real
  georeference anyway). Mitigation: keep if projection is non-empty OR transform is non-identity
  — a grid with a CRS but identity transform is still kept. The cross-tier parity test (same set
  as light `classify()`) is the gate.
- **NASA-NEX file size / download time.** Global 0.25° daily files are large; bound the staged
  corpus and log counts. Download is anonymous but network-dependent — guard the bench to skip
  cleanly if the pool is empty (already the pattern).
- **`sizeInMB` semantics.** Passing `sizeInMB=-1` to heavy = one tile per grid subdataset; this
  is the fair comparison but means very large grids emit one big tile. That is the intended
  reader default (no split); a tiled sweep is a separate, opt-in bench.
- **Cross-tier value parity on scaled variables** (carried from prior spec): heavy = raw stored
  values, light = decoded physical. NASA-NEX variables may carry scale/offset — the raster bench
  is a *throughput* measure, not a bit-parity gate; parity stays scoped to unscaled fixtures.
