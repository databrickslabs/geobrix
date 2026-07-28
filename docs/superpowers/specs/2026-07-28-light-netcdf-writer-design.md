# Light `netcdf_gbx` writer — Design

**Date:** 2026-07-28
**Status:** Design (pending plan).
**Branch:** `feature/netcdf-phase2`
**Relates:** `netcdf-heavy-reader-light-writer` (the writer is the remaining half of this memory item),
the light `netcdf_gbx` READER (`ds/netcdf.py` + `_netcdf.py` + `_netcdf_vector.py`) this inverts,
the `gtiff_gbx`/`RasterGbxWriter` pattern it mirrors, `single-file-writer-serverless-memory-ceiling`,
`volumes-cleanpath-bare-not-file`, `host-vs-docker-black-mismatch`.

## 1. Problem & goal

The light tier has a `netcdf_gbx` READER (raster + vector modes) but no WRITER. Goal: add a
Serverless-safe light `netcdf_gbx` WRITER that is the **symmetric inverse of both reader modes** —
raster grid tiles → CF-compliant grid NetCDF, and vector points → CF Discrete Sampling Geometry
(DSG) NetCDF — in the **most performant manner consistent with the existing light writers** (the
DataSource V2 `write(iterator)` per-partition path, no scalar-UDF ser/de tax).

## 2. Grounding facts (verified in code, 2026-07-28)

- **Writer pattern to mirror** (`ds/writer.py` `RasterGbxWriter`): a `DataSourceWriter` with
  `write(iterator)` (per-partition), `commit` (no-op), `abort` (delete written paths). Scatter:
  one output file per row. Constructor calls `_listing.to_local_path(path)` to strip `dbfs:`/`file:`
  schemes so `os.*` hits the bare FUSE mount; `overwrite` globs+removes stale `*.<ext>` under the
  scheme-stripped path. Schema enforced by `assert_write_schema` (exact `(source, tile)`).
- **DataSource wiring** (`ds/netcdf.py`): `NetcdfGbxDataSource` already registered; `reader()`/`schema()`
  dispatch on `self.options.get("mode","raster")`. The writer adds a `writer(schema, overwrite)` method
  dispatching on the same `mode`.
- **Reader decode to invert** (`_netcdf.py`): `grid_transform_crs(ds, var)` builds a north-up Affine
  (`ulx = min(lons) - px/2`, `uly = max(lats) + py/2`) + CRS string (EPSG from a `grid_mapping`
  var's `epsg_code`/`spatial_epsg`, else `EPSG:4326`); `array_2d` returns a north-up 2-D array;
  `nodata_of` reads `_FillValue`/`missing_value`. The raster reader emits `source =
  NETCDF:"{path}":{var}` and one `(source, tile)` row per grid variable.
- **Vector reader schema** (`_netcdf_vector.py`): dynamic — one typed column per attribute variable,
  then `geom_0` (point WKB), `geom_0_srid`, `geom_0_srid_proj`. Points recovered via shapely.
- **Tile decode** (`_write.tile_to_bytes` inverse): a tile's `raster` bytes open via
  `rasterio.MemoryFile` → `read()`, `transform`, `crs`, `nodata`, `dtypes[0]`.
- **Deps/plumbing already present:** `netcdf4` in `requirements-pyrx-ci.in` + `pyproject.toml [light]`;
  test dir `test/ds/` in `_LIGHT_TEST_DIRS` and both CI action lists. **No CI plumbing change needed.**
- **Serverless rule:** no `spark.conf.set`/`_jvm`/`.rdd`/`cache`/`persist`. FUSE: `shutil.copyfile`
  (NOT `copy`/`copy2` — they `chmod` and FUSE rejects it); write to worker-local temp then copy (NOT
  `os.rename`); `netCDF4.Dataset` needs random-access construction so it MUST write to local temp first.

## 3. Design

### 3.1 Dispatch (`NetcdfGbxDataSource.writer`)

```
def writer(self, schema, overwrite):
    mode = self.options.get("mode", "raster").lower()
    if mode == "raster": return NetcdfRasterGbxWriter(self.options, schema, overwrite)
    if mode == "vector": return NetcdfVectorGbxWriter(self.options, schema, overwrite)
    raise ValueError(...)   # same message shape as reader()
```

New module `ds/_write_netcdf.py` holds both writer classes + a `tile_to_nc_bytes` / `points_to_nc`
helper. Both writers strip the path scheme via `to_local_path` and honor `overwrite` (glob+remove
stale `*.nc`).

### 3.2 Raster mode — `NetcdfRasterGbxWriter` (scatter, one `.nc` per row)

- **Schema:** exact `(source, tile)` via `assert_write_schema` (reuse).
- **Per row:** open `tile.raster` via `rasterio.MemoryFile` → `arr = read(1)`, `transform`, `crs`,
  `nodata`, `dtype`. Invert the reader's affine: derive 1-D coordinate arrays at pixel centers —
  `lon[i] = transform.c + transform.a*(i+0.5)` (i in 0..W-1); `lat[j] = transform.f + transform.e*(j+0.5)`
  (j in 0..H-1). Since `array_2d` guarantees north-up, `transform.e < 0` and `lat` is descending
  (correct CF orientation). Write a CF grid `.nc` with `netCDF4.Dataset` (local temp): dims `lat`(H),
  `lon`(W); coord vars `lat`(`standard_name="latitude"`, `units="degrees_north"`) and
  `lon`(`standard_name="longitude"`, `units="degrees_east"`); one data var (dtype from the array,
  `_FillValue`=nodata if present) with `arr` written; when CRS ≠ EPSG:4326, a scalar `crs`
  grid_mapping var carrying `spatial_epsg` (+ `grid_mapping_name`) and the data var's `grid_mapping="crs"`.
- **Variable name (decision):** parse from `source` when it is a `NETCDF:"…":var` selector → `var`;
  else fall back to `"data"`. Optional `varNameCol` option overrides per-row (basename-sanitized).
  This makes read→write→read an identity round-trip with zero config.
- **Filename:** `nameCol` (basename-sanitized) if set and non-empty; else the variable name; else the
  raster/gtiff-style content-hash+uuid fallback (reuse the `_safe_name` idea). `ext="nc"`.
- **copyfile** temp → `{path}/{name}.nc`; append to the commit message's path list.

### 3.3 Vector mode — `NetcdfVectorGbxWriter` (one `.nc` per partition)

- **Schema:** the dynamic vector schema — attribute columns + `geom_0` + `geom_0_srid` +
  `geom_0_srid_proj`. Validate that `geom_0`/`geom_0_srid` are present; treat every other non-geom
  column as an attribute variable (preserve its Spark type → numpy dtype).
- **Per partition (`write(iterator)`):** collect the partition's rows into aligned Python lists:
  `lon`, `lat` (from `shapely.from_wkb(row["geom_0"])` → `.x`/`.y`), and one list per attribute
  column. If the partition is empty, write nothing (return an empty commit message). Write ONE
  CF-DSG `.nc` (local temp): global attr `featureType="point"`; dim `obs`(N); coord vars
  `latitude`(`degrees_north`)/`longitude`(`degrees_east`) over `obs`; one data var per attribute over
  `obs` with `coordinates="latitude longitude"`. SRID: if all rows agree on `geom_0_srid` and it is
  non-4326, record it (a `crs` scalar var); default WGS84.
- **Filename:** `nameCol` if set (per-partition, take first row's value) else a uuid shard name
  (`part-<uuid8>.nc`). One file open per partition — performant.
- **Memory note** (`single-file-writer-serverless-memory-ceiling`): a partition's points are buffered
  in Python lists before the single `netCDF4` write. This is bounded by partition size (the user
  controls it via `repartition`), matching the existing per-partition writer memory profile; document
  that very large single partitions should be repartitioned. No whole-dataset driver collection.

### 3.4 Performance approach

DataSource V2 `write(iterator)` per partition — data crosses JVM↔Python once per partition, then a
Python loop. No scalar `pandas_udf` (which would buffer Arrow batches + pay per-batch ser/de). Raster
per-row cost is dominated by the `rasterio` decode + `netCDF4` encode, not Python overhead; vector is
one encode per partition. This matches every existing light writer and is the performant choice
(confirmed by the writer-family recon).

## 4. Testing (TDD; round-trip is the primary gate)

- **Raster round-trip:** build a known grid (via `netCDF4` or the reader's test fixtures) → read with
  `netcdf_gbx` raster → write with `netcdf_gbx` raster → re-read → assert array within tolerance, CRS
  EPSG, transform, and nodata match. Include a scaled-variable case and a non-4326 CRS case.
- **Vector round-trip:** points DataFrame (attrs + `geom_0` WKB) → write vector → re-read vector →
  assert lon/lat and attribute values match; assert output `featureType="point"` and the `obs`
  dimension via plain `netCDF4`/xarray.
- **CF-structure checks:** open a written `.nc` with plain `netCDF4.Dataset`/xarray and assert dims,
  coordinate vars + `standard_name`/`units`, `_FillValue`, and (non-4326) the `crs` grid_mapping var.
- **Writer mechanics:** `overwrite` clears stale `*.nc`; `nameCol`/`varNameCol` honored; empty
  partition writes nothing; `abort` removes written files; schema mismatch raises.
- **Serverless-safety guard:** grep the new module for `spark.conf.set`/`_jvm`/`.rdd` (must be absent).
- Tests live in `test/ds/test_netcdf_writer.py` (existing light test dir — no CI plumbing change).

## 5. Surfaces to update

- Create `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` (both writers + helpers).
- Modify `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` (`writer()` dispatch on `mode`).
- Create `python/geobrix/test/ds/test_netcdf_writer.py`.
- Docs: `docs/docs/readers/netcdf.mdx` (or a writers page) — document the `netcdf_gbx` writer, both
  modes, options (`mode`, `nameCol`, `varNameCol`), the round-trip, and the vector one-file-per-
  partition shape. Release-note the new writer in `docs/docs/beta-release-notes.mdx`.
- Wheel: rebuild + restage after the light package change (`whl-change-rebuild-and-stage`).

## 6. Risks

- **CRS fidelity:** encoding a non-EPSG or WKT-only CRS as a CF `grid_mapping` is lossy for exotic
  projections. Mitigation: store `spatial_epsg` when an EPSG is resolvable (the reader only round-trips
  EPSG anyway); for non-EPSG CRS, store the WKT in a `crs_wkt` attr and document the limitation.
- **Scale/offset:** the light reader decodes to physical values (mask_and_scale). The writer writes
  decoded physical values (no re-packing) — a lossless round-trip of values, but it does not
  re-compress to packed integers. Acceptable (documented); a `scale_factor`/`add_offset` packing
  option is a possible follow-up, not this cycle.
- **Vector partition memory:** buffering a partition's points in Python lists — bounded by partition
  size; document the repartition guidance. Not a whole-dataset collection.
- **FUSE write correctness:** the temp-then-`copyfile` pattern and scheme-strip are the known-good
  path; the tests must exercise a `/Volumes`-style path (or the local FUSE-sim used by other writer
  tests) to catch a regression.
- **`netCDF4.Dataset` local-temp requirement:** writing directly to a Volume FUSE path can corrupt;
  always write to `tempfile` then copy (enforced in the helper, covered by a test that asserts the
  output opens cleanly).
