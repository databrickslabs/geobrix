# Serverless COG Preparer — Design Spec

**Date:** 2026-07-31
**Branch:** `feature/large-raster-reader`
**Status:** design approved; exploratory (non-wired) implementation to follow
**Related:** `docs/superpowers/specs/2026-07-30-cog-lane-and-file-preparer-design.md`
(the `cog_gbx` DS-V2 writer, whose Serverless memory ceiling motivates this work)

## Problem

The `cog_gbx` DataSource-V2 writer cannot COG-convert a single multi-GiB source
**inside a Databricks Serverless write task**. Every in-worker read/copy
mechanism was exhausted proving this: Python `copyfileobj` → OOM; GDAL-direct
read over FUSE → OOM; `WorkspaceClient`/`dbutils` cannot even be constructed in
a serverless worker. The failure is the ~1 GB per-PySpark-UDF memory cap
(`UDF_PYSPARK_ERROR.OOM`) exhausted by GDAL's overview-build transient on a
19968×20165 striped source — inherent to the DS-V2 write sandbox, not a
patchable code layer.

**Goal:** find *any* mechanism that prepares a large raster as a COG **on
Serverless**, so users are not forced onto a classic cluster. This is an
**exploratory** effort — the primary deliverable is a Serverless experiment
that answers the ceiling question, not a shipped feature.

## Objective (the core experiment)

Test the ceiling **directly**: does a **scalar UDF** — which runs in the
generic PySpark Python-worker sandbox, a *different* execution context than the
DS-V2 write sandbox — give GDAL enough headroom to convert the real ~1.5 GiB
striped VIIRS-UK GeoTIFF, where the DS-V2 writer (running the *same* core)
OOM'd?

- **PASS** → we have distributed Serverless COG prep; no classic needed.
- **FAIL** → the generic-UDF sandbox shares the DS-V2 ceiling; pivot to the
  documented fallbacks (§Fallbacks), all Serverless-preserving.

**Honest hypothesis:** the killer is GDAL's overview-build transient under the
1 GB cap. A scalar UDF faces the same core, so it is *not guaranteed* to clear
the ceiling — but it runs as its own generic-worker task (not nested inside a
writer task), which is the only variable under test. The probes that read
1.5 GiB "fine" were GDAL *native* reads off the Python heap; a sustained COG
convert with overview build is the heavy transient that actually blew up.

## Architecture

One shared core, two thin front-doors, exploration-first.

### Shared core (already built — no change)

`cog_convert_file(src_path, dst_path, compression="DEFLATE", blocksize=512,
overview_resampling="AVERAGE")` in
`python/geobrix/src/databricks/labs/gbx/pyrx/core/analysis.py`. Streams
block-by-block via `rasterio.shutil.copy(src, dst, driver="COG")` inside
`rasterio.Env(GDAL_CACHEMAX=200)`. Reused verbatim by both front-doors.

### Front-door A — scalar UDF (the experiment's subject)

```
gbx_rst_preparecog(
    path,             # source raster path — GeoTIFF in the test; any GDAL-readable single dataset
    out_dir,          # target directory for the prepared COG
    blocksize=512,
    resampling="AVERAGE",
    compression="DEFLATE",
    subdataset=None,  # optional; NetCDF/HDF multi-subdataset sources
    skip_if_exists=True,
) -> struct<output_path: string, peak_rss_mib: double, status: string>
```

- Runs in the generic PySpark Python-worker (via
  `df.withColumn("cog_path", prepare_cog_udf(col("path"), lit(out_dir)))`),
  **not** the DS-V2 write sandbox.
- Pixels never ride a Spark column — only the source path goes in, the output
  path string (plus RSS/status) comes out.
- **Exact same core** as the DS-V2 writer; the only variable under test is the
  sandbox's memory headroom.

### Front-door B — driver-side helper (documented fallback, not built in this pass)

`prepare_cog(src, dst, **opts)` — plain Python callable from the driver /
notebook, looping over `file_gbx` rows. Runs in the roomy driver context
(GB of RAM, `dbutils`/`WorkspaceClient` available) — **outside** the 1 GB UDF
cap. Highest-confidence Serverless answer; built only if front-door A OOMs.

### Relationship to the `cog_gbx` DS-V2 writer

The scalar UDF **cannot be invoked from inside** the DS-V2 writer to gain
headroom: a UDF's memory envelope comes from *where Spark schedules it*, not
the function body. Calling the UDF body inline in `write()` runs it in the
writer's sandbox — same ceiling. The boundary is the process/sandbox, not the
call (same reason `WorkspaceClient()` works on the driver but crashes
in-writer).

So if the experiment succeeds, the outcome is **two lanes chosen by size**, not
nesting: the `cog_gbx` DS-V2 writer stays as the moderate-file convenience
path; the scalar UDF (`df.withColumn(...)` transformation) becomes the
large-file path. Same core, different invocation context.

## Source-format scope

- **Accepted:** anything GDAL opens as a single dataset — GeoTIFF (the common
  case and the test corpus), NetCDF/HDF via subdataset URI, JP2, VRT, PNG.
- **`subdataset` param:** for multi-subdataset formats (NetCDF/HDF/GRIB), builds
  the subdataset URI (e.g. `NETCDF:"file.nc":var`). Unused/ignored for plain
  GeoTIFF sources.
- **Excluded:** non-georeferenced swath data (e.g. Sentinel-5P swaths) that
  requires warping/reprojection first — a heavier, out-of-scope operation.
  Documented as "reproject first, then prepare."
- **Test input:** a GeoTIFF path column (from `file_gbx` / the `gtiff_gbx`
  lane over the VIIRS corpus). The signature is format-agnostic; GeoTIFF is
  the test and common case.

## Naming & idempotency

- **Output naming:** full source basename **+ `.cog`** appended (do not strip
  the source extension): `myfile1.tiff` → `myfile1.tiff.cog`. Self-describing,
  greppable, collision-free when a dir already holds the source `.tiff`. GDAL
  reads by header, not extension, so `.cog` opens normally. (This differs from
  the `cog_gbx` DS-V2 writer's `<stem>.tif` — a distinct, deliberate
  convention for this function.)
- **`skip_if_exists=True` (default):** if `out_dir/<name>.cog` already exists,
  skip the convert and return the existing path with `status="skipped"`.
  Idempotent re-runs; cheap resume after a partial/transient failure (composes
  with the FUSE eventual-consistency retry story).
- **"Force write over all data" = `skip_if_exists=False`:** re-prepares every
  row regardless of existing outputs. Documented explicitly as the "rebuild
  everything" mode.
- **Existence check is name-only** (does `<name>.cog` exist), not a validity
  check — cheap, matches "resume", avoids re-opening every existing file over
  FUSE. A validity-based check is out of scope for the exploratory pass.

## Return contract & error handling

- **Return struct:** `(output_path: string|null, peak_rss_mib: double|null,
  status: string)`. `status` ∈ `"ok"`, `"skipped"`, `"error:<short reason>"`.
  The whole outcome rides the driver-collected return value — no worker
  markers, no failures lost at sandbox teardown.
- **Per-row isolation:** a convert failure sets `status="error:…"`,
  `output_path=null`, and does **not** abort the job. The DataFrame surfaces
  which files failed → re-run just those (composes with `skip_if_exists`).
- **OOM is not catchable in-worker** (it kills the task) — it surfaces as a
  missing/failed task in the Spark run state, not an `"error"` status. The
  experiment reads OOM from run state (the known DS-V2-writer failure
  signature).

## The Serverless experiment

- **Corpus:** the real ~1.5 GiB striped VIIRS-UK GeoTIFF on a UC Volume, listed
  via `file_gbx`.
- **Harness:** a throwaway notebook run through
  `notebooks/tests/run_notebooks_serverless.py` (env v5, `max_retries=0`, wheel
  hash-verified staged==local). **Not** wired into any `gbx:test:*` suite.
- **The run:** `df = file_gbx(corpus)` →
  `df.withColumn("r", prepare_cog_udf(col("path"), lit(out_dir))).collect()`.
  The `.collect()` forces distributed UDF execution and returns outputs **to
  the driver** (reliable capture; no worker markers).
- **RSS capture:** the UDF samples peak RSS via `resource.getrusage` inside the
  task and surfaces it in the returned struct, collected on the driver.
- **Pass/fail:**
  - **PASS** = the 1.5 GiB source converts to a valid COG
    (`rio_cogeo.cogeo.cog_validate`) without `UDF_PYSPARK_ERROR.OOM`, with
    driver-collected peak RSS recorded.
  - **FAIL** = OOM → generic-UDF sandbox shares the DS-V2 ceiling → pivot to
    fallbacks.

## Fallbacks (only if the UDF OOMs — documented, not built yet)

Ranked, all Serverless-preserving (no classic):

1. **Driver-side `prepare_cog`** (front-door B) — roomy driver context, outside
   the 1 GB UDF cap. Highest-confidence Serverless answer.
2. **Bounded/shallow overviews** — the overview pyramid build is GDAL's heavy
   transient; a size threshold → fewer/no overview levels yields a valid tiled
   COG that may fit the UDF cap.
3. **Decimated master** — coarser master COG when full-res won't fit.

Pursued in priority order, each only if the prior is insufficient.

## Testing strategy (local-first)

- **Local (fast; gating but NOT sufficient), Docker via `gbx:test:python`:**
  - naming: `x.tiff` → `x.tiff.cog`
  - `skip_if_exists` true (returns existing, `status="skipped"`) / false
    (rebuilds)
  - subdataset URI construction for a NetCDF fixture
  - per-row error isolation (`status="error:…"`, `output_path=null`, job
    survives)
  - valid-COG output (`sniff_header` / `cog_validate`)
  - RSS-bounded subprocess probe (reuse the `test_cog_writer.py`
    `_run_memory_probe` pattern)
- **Serverless (final gate):** the experiment above — the only thing that
  answers the ceiling question. Local green is necessary but not sufficient.
- **Non-wired throughout:** exploratory code lives **outside** the registered
  `rst_*` catalog — **no** `registered_functions.txt`, `function-info.json`, or
  Python/Scala binding entries — until a future promotion plan. Keeps
  binding-parity and QC green. Nothing ships to users from this pass.

## Non-goals

- No catalog registration / SQL surface in this pass (promotion is a separate
  plan, gated on a PASS).
- No swath/warp support.
- No heavy-tier (Scala/JVM) parity — heavy cannot lazily read Volumes anyway.
- No change to the existing `cog_gbx` DS-V2 writer (stays the moderate-file
  path).
