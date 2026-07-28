# NetCDF writer `singleFile` mode (symmetric raster + vector) — Design

**Date:** 2026-07-28
**Status:** Design (pending plan).
**Branch:** `feature/netcdf-phase2`
**Relates:** `2026-07-28-light-netcdf-writer-design.md` (the writer this extends), the `VectorGbxWriter`
two-phase single-file pattern (`ds/vector.py`) it mirrors, `_scratch`, `single-file-writer-serverless-memory-ceiling`,
`volumes-cleanpath-bare-not-file`.

## 1. Problem & goal

Both light `netcdf_gbx` writers currently emit **sharded "parts"** output — the raster writer writes
one `.nc` per `(source, tile)` row, the vector writer one `.nc` per Spark partition (`part-<uuid>.nc`).
This is the scalable, Serverless-safe default (parallel, no driver bottleneck), but users sometimes
need **one self-contained `.nc`** (a downstream CF tool opens one file at a time; a single logical
dataset should be one file).

**Goal:** add a **symmetric opt-in `singleFile` option to BOTH writers** (default `false` = current
parts behavior, non-breaking). Vector: concatenate all points into one CF-DSG `.nc`. Raster: merge
tiles that share a grid into one CF grid `.nc` (multiple data variables sharing lat/lon), erroring
clearly on incompatible grids. Mosaicking many spatial-window tiles of ONE variable back into a single
grid is **explicitly out of scope** — that is already served upstream by `gbx_rst_merge_agg` /
`gbx_rst_merge`; the docs must point users there.

## 2. Grounding facts (verified 2026-07-28)

- **Current writers** (`ds/_write_netcdf.py`): `NetcdfRasterGbxWriter.write` loops rows, writes one
  `.nc` per row (temp → `shutil.copyfile`); `NetcdfVectorGbxWriter.write` buffers a partition's points
  → one `.nc` per partition. Both `commit` no-op, `abort` deletes written paths.
- **Two-phase single-file pattern to mirror** (`ds/vector.py` `VectorGbxWriter`): `write(iterator)`
  writes each partition to an Arrow-IPC (feather) **fragment** in a shared `_scratch` dir and returns
  `_VectorCommitMessage(frag_path=...)`; `commit(messages)` (driver) reads all fragment paths, merges
  to ONE output written to a **driver-local temp** (random-access), then FUSE-safe copies to the
  target. `_should_stream` streams fragments one at a time to bound driver memory; `abort` cleans
  fragments. `_scratch.new_scratch_dir(parent)` makes a dot-prefixed `<parent>/.gbx_scratch/<uuid>/`
  (invisible to the recursive reader), age-GC'd (`DEFAULT_STALE_TTL_SECONDS = 3600`).
- **Mosaic already exists:** `gbx_rst_merge_agg` (GROUP BY aggregator: many same-variable tiles → one
  merged tile) and `gbx_rst_merge`. The idiomatic tile→one-grid path is
  `tiles → rst_merge_agg → write.format("netcdf_gbx")`. The writer must NOT duplicate this.
- **CF multi-variable file:** multiple data variables (`tas`, `pr`) sharing `lat`/`lon` dims + coord
  vars is standard CF; that is the well-defined raster single-file case.

## 3. Design

### 3.1 Option

`.option("singleFile", "true")` on both writer modes. Default `false` → current parts behavior,
byte-for-byte unchanged (non-breaking). Read in `NetcdfGbxDataSource.writer` / the writer
constructors from `self.options`.

### 3.2 Vector `singleFile` (concat points → one CF-DSG `.nc`)

Two-phase, mirroring `VectorGbxWriter`:
- **`write(iterator)` (executor):** buffer the partition's `(lon, lat, attrs)` as today, but instead
  of writing a `.nc`, write an **Arrow-IPC fragment** (columns: `longitude`, `latitude`, one per
  attribute; carry `geom_0_srid`) into `_scratch.new_scratch_dir(self.path)`. Return
  `NetcdfCommitMessage(frag_path=...)` (extend the message to carry a fragment path, or add a
  `_NcVectorCommitMessage`). Empty partition → empty frag path (skipped).
- **`commit(messages)` (driver):** collect fragment paths; if none, write nothing. Else write ONE
  CF-DSG `.nc` to a driver-local temp: create the `obs` dimension as **unlimited/appendable** and
  **stream** fragments one at a time — read each feather fragment, append its rows to `obs` (grow the
  coord + data vars) — so the driver never holds all points at once (bounded memory, matching
  `_should_stream`). SRID: reconcile across fragments (all-agree non-4326 → `crs` var). Then
  `shutil.copyfile` temp → the single target path (`{path}` treated as the file, or `{path}/{name}.nc`
  — match `_resolve_single_file_output` semantics from `ds/vector.py`). `abort`: delete fragments +
  partial output.

### 3.3 Raster `singleFile` (merge same-grid variables → one CF `.nc`)

Two-phase:
- **`write(iterator)` (executor):** for each `(source, tile)` row, decode the tile (array + transform +
  CRS + nodata + varname, as today) and write a fragment capturing `{varname, array, transform, crs,
  nodata}` (Arrow-IPC or a small `.npz`/pickle-free container in `_scratch`). Return the frag path.
- **`commit(messages)` (driver):** read all fragments. **Grid-compatibility gate:** all fragments must
  share the SAME grid — identical `(width, height, transform, crs)` (within a float tolerance on the
  transform). If they do: write ONE CF grid `.nc` with the shared `lat`/`lon` dims + coord vars and
  **one data variable per distinct varname** (each with its `_FillValue`; a shared `crs` grid_mapping
  var for non-4326). If fragments have **incompatible grids/CRS**: raise a clear `ValueError` naming the
  conflict and pointing at `gbx_rst_merge_agg` for mosaicking window-tiles of one variable first — do
  NOT silently mosaic or pick one. Duplicate varnames across fragments with the same grid (e.g. the
  same variable tiled by window) is the mosaic case → also error with the `rst_merge_agg` pointer
  (writer merges DISTINCT variables, not window-tiles). `shutil.copyfile` temp → single target.
  `abort`: clean fragments + partial output.

**Scope guard (explicit):** raster `singleFile` merges **distinct variables on one shared grid**. It
does NOT mosaic multiple spatial-window tiles of one variable — that is `gbx_rst_merge_agg`'s job,
applied UPSTREAM (`tiles → rst_merge_agg → write`). The error message and docs state this.

### 3.4 Docs — call out the mosaic pattern

`docs/docs/readers/netcdf.mdx` writer section: document `singleFile` on both modes (default parts,
opt-in single), the vector concat, the raster same-grid multi-variable merge, and — prominently — that
to combine many **spatial-window tiles of one variable** into a single grid, use `gbx_rst_merge_agg`
(or `gbx_rst_merge`) BEFORE the writer:
`SELECT rst_merge_agg(tile) ... GROUP BY ... ` → then `write.format("netcdf_gbx").option("singleFile","true")`.
Also note the single-file memory tradeoff (driver-funneled; scatter is safer at very large scale).

## 4. Testing (round-trip is the gate)

- **Vector singleFile round-trip:** points DataFrame (multi-partition, `coalesce`>1) → write
  `singleFile=true` → assert EXACTLY ONE `.nc` in the output → re-read → all points/attrs match.
  Default (no option) still yields `part-*.nc` (unchanged).
- **Raster singleFile round-trip:** two DISTINCT variables on the SAME grid (`tas`,`pr` from a shared
  lat/lon) → write `singleFile=true` → assert ONE `.nc` with both data vars sharing lat/lon → re-read
  each → values/CRS/nodata match.
- **Raster incompatible-grid error:** two tiles with different grids/CRS + `singleFile=true` → raises a
  clear `ValueError` naming the conflict and the `rst_merge_agg` pointer (not a silent pick/mosaic).
- **Raster duplicate-variable (window-tile) case:** two tiles, same varname + same grid dims but
  different windows + `singleFile=true` → errors with the `rst_merge_agg` mosaic pointer.
- **Memory/streaming:** vector commit streams fragments (assert it doesn't require all points resident
  — a structural check / large-ish fixture).
- **Serverless-safety:** no `spark.conf.set`/`_jvm`/`.rdd` in the new paths.
- Tests extend `python/geobrix/test/ds/test_netcdf_writer.py`.

## 5. Surfaces to update

- `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` — `singleFile` branch in both writers
  (two-phase: fragment on `write`, merge on `commit`); a fragment commit-message; reuse `_scratch`.
- `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` — pass `singleFile` through `writer()`.
- `python/geobrix/test/ds/test_netcdf_writer.py` — the tests above.
- `docs/docs/readers/netcdf.mdx` — `singleFile` on both modes + the `rst_merge_agg` mosaic pointer +
  memory tradeoff; `docs/docs/beta-release-notes.mdx` — the new option.
- Bench: add a `singleFile` variant to the writer bench. The existing `_CELL_NETCDF_WRITER` legs
  (raster + vector) run parts-mode (default); add a parallel measured leg per mode with
  `options={... "singleFile": "true"}` so the results table carries BOTH shapes (parts vs single) for
  raster and vector — a real parts-vs-single throughput comparison. Reuse `run_format_write` (the
  single `options` dict flows `singleFile` to the writer). Distinguish the rows via `fn`/`note`
  (e.g. append a `-single` suffix or a note tag) so parts and single don't collide in the store.
  Guard/skip-clean + row-count>0 as the other legs. Reflect in `benchmarking.mdx`
  (`bench-changes-update-docs`).

## 6. Risks

- **Grid-compatibility tolerance:** the raster merge's "same grid" test needs a float tolerance on the
  geotransform (exact-equality would reject grids that are numerically identical but for float noise).
  Mitigation: compare transform elements within a small rtol; require identical `(width,height)` and
  CRS EPSG exactly. The incompatible-grid test is the gate.
- **Driver memory (single-file):** both modes funnel the merge through the driver. Vector streams to
  bound it; raster holds one array per distinct variable (bounded by variable count × grid size, not
  tile count) — fine for multi-var, and the mosaic case (many tiles) is explicitly redirected to
  `rst_merge_agg`. Document the tradeoff (`single-file-writer-serverless-memory-ceiling`).
- **FUSE:** driver-local temp + `shutil.copyfile` (not `copy`/rename); `netCDF4.Dataset` needs
  random-access so temp-first is mandatory. Same as the parts writer.
- **Non-breaking:** `singleFile` defaults false; the existing parts tests + behavior must stay green
  (regression-gate them).
