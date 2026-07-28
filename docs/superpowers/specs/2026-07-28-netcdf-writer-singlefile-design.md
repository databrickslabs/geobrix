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

### 3.1 Options

Four writer options (all read from `self.options` in the writer constructors; all default to the
current behavior → non-breaking):

- **`singleFile`** (default `false`) — write ONE `.nc` instead of sharded parts, via the two-phase
  scratch/merge pattern (§3.2 vector, §3.3 raster).
- **`merge`** (default `false`) — **post-hoc directory merge.** `.write.format("netcdf_gbx")
  .option("merge","true").save("<dir>")` merges the `.nc` files ALREADY IN `<dir>` into one file,
  WITHOUT re-running the source DataFrame. Mechanism: DataSource V2 `write(iterator)` is pull-based —
  when `merge=true`, `write()` returns immediately WITHOUT consuming the iterator, so upstream
  read/transcode never advances (no re-work; Spark still schedules cheap no-op tasks per partition,
  but no file I/O). `commit()` on the driver then reads `<dir>/*.nc` and merges them using the SAME
  merge core as `singleFile`'s commit (vector concat on `obs`; raster distinct-same-grid-var merge,
  error→`rst_merge_agg`). This serves the "I already ran parts-mode, it took minutes, just combine the
  output" case. `merge` implies single output; if BOTH `merge` and `singleFile` are set, `merge` wins
  (directory-merge, no fresh write).
  - **Overwrite interaction (REQUIRED):** the writer's `overwrite` mode globs+deletes `*.nc` in the dir
    at construct time. In `merge` mode this MUST be suppressed (read-then-replace, never
    clear-then-read) or it deletes the very parts to merge. Enforce + test.
  - **`keepParts`** (default `false`) — after a SUCCESSFUL merge, the source `part-*.nc` files are
    deleted (consolidate-in-place). `keepParts=true` retains them alongside the merged file. This is
    ORTHOGONAL to Spark's `mode` (which governs the merged output vs. a pre-existing target, not
    intermediate-part retention) — hence a dedicated option, not `append`/`overwrite`.
  - **DATA-SAFETY ORDERING (REQUIRED — never lose the parts to a failed merge):** the part files are
    the expensive-to-produce inputs; they must NOT be deleted until the merged output is proven
    durable. Strict sequence when `keepParts=false`: (1) merge into a driver-local temp `.nc`;
    (2) VALIDATE the temp — reopens cleanly via `netCDF4.Dataset` AND its `obs`/data-var element count
    equals the summed input count (catch a truncated/corrupt merge); (3) `shutil.copyfile` temp →
    target; (4) VERIFY the target exists and its byte size equals the temp's (catch a partial FUSE
    copy); (5) ONLY THEN delete the part files. If ANY step (1–4) fails → raise and leave EVERY part
    intact; the delete never runs on an error path. `abort` never deletes source parts either. So a
    failed/partial merge always leaves the user's parts recoverable.
- **`fileName`** (default derived) — the output file name (stem) for `singleFile`/`merge` output.
  Resolved via `_resolve_single_file_output(path, fileName, "nc")` (its 3-case contract). Falls back to
  `nameCol`'s value, then the directory name, as today.
- **`partPrefix`** (default `"part"`) — the filename stem for parts-mode files: `<partPrefix>-<uuid>.nc`
  (currently hardcoded `part-`). Lets users label shards (e.g. `partPrefix="s5p"` → `s5p-<uuid>.nc`).

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

### 3.3a `merge` — post-hoc directory merge (no re-run) + filename options

- **`write(iterator)`:** when `merge=true`, return IMMEDIATELY without consuming `iterator` (so the
  source DataFrame's read/transcode never advances). Return an empty commit message.
- **`commit(messages)`:** on the driver, glob `<path>/*.nc` (the existing parts, EXCLUDING the
  `_scratch` container and any prior merged output at the resolved `fileName`), and merge them with the
  SAME core as `singleFile`'s commit — vector: concat on `obs` (open each `.nc`, read its points,
  append); raster: decode each `.nc`'s grid var(s), apply the grid-compat gate + distinct-var merge
  (same error→`rst_merge_agg` on incompatible/window-tile duplicate). Mode dispatch (raster vs vector)
  follows the writer's `mode` option as usual. Write to driver-local temp → validate → copy → verify →
  (delete parts unless `keepParts`), per the DATA-SAFETY ORDERING above.
- **Overwrite suppression:** in `merge` mode the constructor must NOT run the `overwrite` glob-delete
  (that would delete the inputs). Skip it when `merge=true`.
- **Empty dir:** no `.nc` files under `path` → raise a clear `ValueError` (nothing to merge), do not
  write an empty output.
- **`fileName` / `partPrefix`:** `fileName` sets the single/merge output stem (via
  `_resolve_single_file_output`); `partPrefix` replaces the hardcoded `part-` stem in parts-mode
  (`<partPrefix>-<uuid>.nc`). Both apply to raster and vector.
- **DRY:** factor the vector-concat and raster-grid-merge cores into helpers shared by BOTH the
  `singleFile` commit (merging `_scratch` fragments) and the `merge` commit (merging existing `.nc`
  files). The only difference is the INPUT source (feather fragments vs `.nc` files on disk) — the
  merge/validate/copy/verify/delete tail is identical.

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
- **`merge` round-trip (vector + raster):** write parts-mode to a dir (multiple `.nc`), THEN a second
  `.write.format("netcdf_gbx").option("merge","true").save(<same dir>)` → assert ONE merged `.nc`,
  parts deleted (default), re-read matches the original data. Assert the merge did NOT re-run the source
  (e.g. point `merge` at a dir but pass a DIFFERENT/empty DataFrame — the output must reflect the DIR's
  files, not the DataFrame).
- **`merge` incompatible + empty:** raster merge of incompatible-grid `.nc` files → `ValueError` →
  `rst_merge_agg` pointer; merge of an empty dir → clear `ValueError` (nothing to merge).
- **`keepParts` + DATA SAFETY (critical):** `merge` with `keepParts=true` → merged file AND parts both
  present. And a **failure-path test:** force the merge to fail (e.g. incompatible grids, or a
  monkeypatched validate) with `keepParts=false` → assert the parts are STILL present (never deleted on
  error) and no partial merged output is left as if valid.
- **`fileName` / `partPrefix`:** `fileName` sets the single/merge output name; `partPrefix` changes the
  parts stem (`<partPrefix>-<uuid>.nc`). Assert both.
- **Serverless-safety:** no `spark.conf.set`/`_jvm`/`.rdd` in the new paths.
- Tests extend `python/geobrix/test/ds/test_netcdf_writer.py`.

## 5. Surfaces to update

- `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` — `singleFile` + `merge` branches in
  both writers (two-phase: fragment on `write`, merge on `commit`; `merge` skips the iterator + globs
  the dir); shared vector-concat / raster-grid-merge core helpers; `keepParts` data-safe delete
  (validate→copy→verify→delete); `fileName`/`partPrefix` naming; suppress overwrite-clear in `merge`.
- `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` — options flow through `writer()` (they ride
  `self.options`; confirm no signature change needed).
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
