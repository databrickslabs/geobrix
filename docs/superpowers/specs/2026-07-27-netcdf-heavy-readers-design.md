# NetCDF heavy readers + light auto-enumeration (Phase 2) — Design

**Date:** 2026-07-27
**Status:** Design (approved direction); pending plan.
**Branch:** `feature/netcdf-phase2` (targets a future beta release)
**Relates:** `netcdf-heavy-reader-light-writer` (this is the reader half; the light-tier NetCDF
*writer* is the second cycle on this branch), `vapor-eyes-methane-example` (the light swath→points path),
`05x-roadmap-backlog` (NetCDF was a queued item), `geobrix-onramp-to-databricks-native`.

## 1. Problem & goal

Phase 1 shipped a **light-tier** NetCDF reader (`netcdf_gbx`, v0.4.1). There is **no heavy-tier** NetCDF
reader (verified greenfield — no `NetCDF_DataSource` on the JVM; only the `rst_subdatasets` /
`rst_getsubdataset` expressions and a `FormatLookup` `"netCDF" -> "nc"` entry exist).

**Goal:** add heavy-tier NetCDF readers following the existing named-reader pattern, and unify the
variable-selection contract across tiers so light and heavy behave identically. NetCDF is dual-engine in
GDAL (raster driver `netCDF` + OGR driver `netCDF`), so — mirroring the existing `gtiff_gdal` (raster) vs
`geojson_ogr` (vector) family split — this adds **two** heavy readers:

- **`netcdf_gdal`** — CF grid variables → the shared `(source, tile)` raster struct (raster family).
- **`netcdf_ogr`** — CF Discrete Sampling Geometry (DSG) point features → the shared vector schema
  (vector family).

And it **improves the shipped light `netcdf_gbx` reader** so its variable option becomes an optional
filter (matching the new heavy default), not a mandatory selector.

**Explicitly a non-goal:** heavy swath→points. The light reader coerces any 2-D/curvilinear field (e.g. a
TROPOMI swath) to one point per pixel; GDAL's **OGR** netCDF driver does NOT do this (it surfaces only
native CF-DSG features), and reimplementing the flatten on the JVM is out of scope. Swath→points stays
light-only — a documented, intentional cross-tier asymmetry.

## 2. The unified variable-selection contract (all three readers)

| Reader | Tier | Engine | Reads | This cycle |
|---|---|---|---|---|
| `netcdf_gbx` | light (existing) | xarray | CF grid → raster tiles; DSG/swath → points | **modified** (auto-enumerate) |
| `netcdf_gdal` | heavy (new) | GDAL `netCDF` | CF grid variables → `(source, tile)` raster | new |
| `netcdf_ogr` | heavy (new) | OGR `netCDF` | CF-DSG features → vector schema | new |

**Contract (identical across all three):**
- **Default = process ALL readable variables.** A bare `load` returns every readable variable (raster:
  every georeferenced grid variable; vector: every DSG layer), one row-group per variable.
- **`variable` / `variables` is an OPTIONAL FILTER**, not a required selector. Absent → all; `variable=X`
  → only X; `variables=X,Y` → those. An empty/absent filter keeps all.
- The heavy raster reader and the light raster mode emit the **same `(source, tile)` struct**; `source`
  names the variable/subdataset so multiple variables in one result are disambiguable.
- Heavy vector (`netcdf_ogr`) and light vector mode emit the **same vector schema** (attributes + `geom_0`
  WKB + `geom_0_srid` / `geom_0_srid_proj` columns).

## 3. Grounding facts (verified 2026-07-27, recon)

- **Light `netcdf_gbx`** (`python/.../ds/netcdf.py` + `_netcdf.py` + `_netcdf_vector.py`): PySpark
  DataSource V2, registered via `spark.dataSource.register` (NOT META-INF/services). Dual-mode
  (`mode` option: `"raster"` default / `"vector"`). Uses **xarray** (netcdf4 engine). Already iterates
  `ds.variables` (in `_find_lat_lon`, `_crs_string`) and already has `classify(ds, variable)` →
  regular-grid / DSG / curvilinear-swath / raw-sensor. The mandatory-variable rule is a single function
  `_requested_variables` (netcdf.py:24) that `raise`s when the option is absent. Raster mode currently
  reads only `variables[0]` (netcdf.py:48).
- **Heavy named-reader pattern:** a named reader extends the generic engine DataSource with
  `DataSourceExtras` and presets the driver via `dsExtraMap`. Raster: `GTiff_DataSource extends
  GDAL_DataSource with DataSourceExtras`, `dsExtraMap = Map("driver" -> "GTiff")`, `shortName =
  "gtiff_gdal"`. Vector: `GeoJSON_DataSource extends OGR_DataSource with DataSourceExtras`,
  `dsExtraMap = Map("driverName" -> "GeoJSON"...)`, `shortName = "geojson_ogr"`. Both register in
  `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.
- **`GDAL_DataSource`** schema = `struct<source: string, tile: struct<cellid: bigint, raster: binary,
  metadata: map<string,string>>>`. Its `GDAL_Batch.planInputPartitions()` assumes one raster per file and
  tiles via `BalancedSubdivision`.
- **NetCDF subdataset plumbing already exists:** GDAL's netCDF driver exposes each variable as a
  subdataset `NETCDF:"/path/file.nc":varname`. `RasterAccessors.subdatasetsMap(ds)` =
  `ds.GetMetadata_Dict("SUBDATASETS")` returns `SUBDATASET_N_NAME` → selector. `RST_GetSubdataset` builds
  `s"$driver:$path:$name"` and `gdal.Open`s it. `RasterDriver` handles subdataset paths (`isSubdataset`
  option; `NETCDF:/vsimem/...:var` passes through). `RasterAccessors.isSubdatasetSelector` guards on
  `desc.contains(":/") && desc.split(":").length >= 3`.
- **Test data:** NetCDF fixtures live in `src/test/resources/binary/netcdf-CMIP5/`,
  `netcdf-coral/` (10 files), `netcdf-ECMWF/`. **No `.nc` in `sample-data/`.** Heavy readers are tested
  `PlanTest with SilentSparkSession` (e.g. `GTiff_DataSourceTest` unit — shortName/dsExtraMap/inferSchema;
  `GDAL_DataSourceTest` integration — `spark.read.format(...).load(...)`).

## 4. Design

### 4.1 `netcdf_gdal` (raster; the novel piece)

`class NetCDF_DataSource extends GDAL_DataSource with DataSourceExtras`, in
`src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/`:
- `override def shortName() = "netcdf_gdal"`
- `override def dsExtraMap(...) = Map("driver" -> "netCDF")` (matches the `FormatLookup` key)
- register in META-INF/services.

The one departure from `gtiff_gdal`: a NetCDF file has **no top-level bands, only subdatasets**, so
partition planning must enumerate subdatasets, not assume one raster per file.

- **Plan time:** for each input `.nc` file, open it, call `RasterAccessors.subdatasetsMap` to list
  `NETCDF:"file":var` selectors, **filter to georeferenced 2-D+ grid variables** (skip 1-D
  coordinate/bounds variables — `lat`, `lon`, `time_bnds`, etc. — that carry no geotransform / aren't real
  raster fields), then **apply the optional `variable`/`variables` filter** (empty → keep all). Emit
  **one partition per (file, subdataset)**.
- **Read time:** each partition opens its subdataset selector through the existing `RasterDriver` path
  (`isSubdataset=true` — already wired), then tiles via the same `BalancedSubdivision` path as
  `gdal`/`gtiff_gdal`. Output = the standard `(source, tile)` struct; `source` = the subdataset selector,
  so the variable is recoverable from the result.
- This reuses `GDAL_DataSource`'s read/tile machinery wholesale; the new code is concentrated in
  subdataset enumeration + the grid/coordinate-variable filter at plan time.

**Edge cases:** a file whose only subdatasets are non-grid (pure coordinate arrays) yields no rows (empty,
non-erroring). A `variable` filter naming a variable absent from a file → that file contributes no rows
for it (no error; consistent with a filter, not a selector).

### 4.2 `netcdf_ogr` (DSG vector; thin OGR reader)

`class NetCDF_OGR_DataSource extends OGR_DataSource with DataSourceExtras`, in
`src/main/scala/com/databricks/labs/gbx/vectorx/ds/netcdf/` (mirrors `geojson_ogr`):
- `override def shortName() = "netcdf_ogr"`
- `override def dsExtraMap(...) = Map("driverName" -> "netCDF")`
- register in META-INF/services.

Surfaces CF-DSG features via OGR into the shared vector schema (attributes + `geom_0` WKB +
`geom_0_srid` / `geom_0_srid_proj`), exactly as the other `*_ogr` readers. The optional
`variable`/`variables` filter restricts which DSG layers/variables are read (default all). A `.nc` with no
OGR-readable DSG features yields no rows (empty, non-erroring — matches the other OGR readers on
featureless sources). Does **not** do swath→points (light-only, §1).

### 4.3 Light `netcdf_gbx` change (implemented FIRST — establishes the shared contract)

Change `_requested_variables` (netcdf.py:24) from mandatory-selector to **optional-filter**:
- **Absent option:** enumerate all readable variables via the existing `classify()` — for `raster` mode
  keep the regular-grid (and projected-grid) variables; for `vector` mode keep DSG/curvilinear variables —
  instead of raising.
- **Present option:** filter the enumerated set to the named variables.
- **Raster mode** currently emits one tile from `variables[0]`; generalize to **one tile row per kept grid
  variable** (so a bare load returns all grid variables, matching `netcdf_gdal`).

This is a **behavior change to the shipped v0.4.1 reader**: a bare load that previously raised now
succeeds and returns all readable variables. Strictly more permissive — existing explicit-`variable` calls
are unaffected (the option now filters to exactly what it selected before). Release-noted.

## 5. Tiers, parity & sequencing

**Implementation order on this branch (cycle 1 of 2):**
1. **Light `netcdf_gbx` auto-enumerate** (§4.3) — small; establishes the shared contract + its tests
   first, so the heavy readers are built against a settled, tested contract.
2. **`netcdf_gdal`** (§4.1) — the raster reader.
3. **`netcdf_ogr`** (§4.2) — the DSG vector reader.

(The light-tier NetCDF **writer** is a separate, later cycle on this branch — not in this spec.)

**Parity bar:** on a shared `.nc` fixture, `netcdf_gdal` and light `netcdf_gbx` raster mode enumerate the
**same variable set** and produce the **same per-variable tile** — same CRS + geotransform, cell values
within tolerance (the light path is xarray/rasterio, heavy is GDAL; exact-byte parity is not expected, same
as the other cross-tier raster comparisons).

## 6. Testing & benchmarking

- **Cross-tier raster parity test:** `netcdf_gdal` vs light `netcdf_gbx` raster mode on a shared fixture
  from `src/test/resources/binary/netcdf-*` — same enumerated variables, same per-variable tile
  (CRS/transform equal, cell values within tolerance).
- **Heavy unit tests** (`PlanTest with SilentSparkSession`, `GTiff_DataSourceTest` pattern): `shortName`,
  `dsExtraMap` (`netcdf_gdal` → `driver=netCDF`; `netcdf_ogr` → `driverName=netCDF`), `inferSchema` shape,
  `TableProvider`/`DataSourceRegister` is-a.
- **Heavy integration tests** (`GDAL_DataSourceTest` pattern): `netcdf_gdal` bare load enumerates all grid
  variables (one row per variable); `variable`/`variables` filter restricts; non-grid coordinate variables
  are skipped; `netcdf_ogr` reads DSG features from a DSG fixture (stage one if the existing fixtures lack
  DSG — CMIP5/coral/ECMWF are grids; a small DSG `.nc` may need adding to `src/test/resources/binary/`).
- **Light tests:** auto-enumerate default returns all grid variables; `variable` filter restricts;
  back-compat — an explicit `variable` still returns exactly that variable's tile.
- **Benchmarking (both tiers):** add `netcdf_gdal` and light `netcdf_gbx` to the reader-bench harness
  (`gbx:bench:readers` family) so the two tiers are timed on the **same NetCDF corpus**. Stage a
  representative `.nc` into the bench corpus (the reader-bench corpus, not `sample-data/`); measure
  read/tile throughput. Per the `bench-changes-update-docs` rule, reflect the readers-bench addition in
  `benchmarking.mdx`.

## 7. Surfaces to update

- Heavy raster: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_DataSource.scala` (+
  subdataset-enumeration plan logic — likely a small `NetCDF_Batch`/planner override or a shared helper on
  `GDAL_Batch`).
- Heavy vector: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/netcdf/NetCDF_OGR_DataSource.scala`.
- `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` (+2 entries).
- Light: `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` (`_requested_variables` + raster
  multi-variable emission), possibly `_netcdf.py` (enumerate-readable-variables helper).
- Bench: reader-bench spec/harness + a staged NetCDF corpus file; `docs/docs/api/benchmarking.mdx`.
- Docs: `docs/docs/readers/netcdf.mdx` (document `netcdf_gdal` + `netcdf_ogr` alongside `netcdf_gbx`; the
  optional-filter contract; the light behavior change; the swath→points light-only asymmetry);
  `docs/docs/beta-release-notes.mdx` (new heavy readers + light behavior change).
- Reader lists / `CLAUDE.md` Readers section (add `netcdf_gdal`, `netcdf_ogr` to the named-reader tables).

## 8. Risks

- **Subdataset enumeration at plan time** is the one genuinely new mechanism vs the one-raster-per-file
  assumption in `GDAL_Batch`. Mitigation: reuse `RasterAccessors.subdatasetsMap` + the existing
  `RST_GetSubdataset` selector format and `RasterDriver` subdataset handling; the grid-vs-coordinate
  filter is the delicate bit (a too-loose filter emits junk coordinate "tiles"; too-tight drops real
  variables) — the cross-tier parity test (same variable set as the light `classify()`) is the gate.
- **DSG test fixture:** the existing NetCDF fixtures are grids; `netcdf_ogr` needs a CF-DSG `.nc` to test
  against — may need to stage a small one.
- **Light behavior change** to a shipped reader — mitigated: strictly more permissive, back-compat test
  for explicit `variable`, release-noted.
- **Bench corpus:** NetCDF read cost is subdataset-open-heavy; ensure the bench corpus file has multiple
  variables so enumeration cost is represented.
