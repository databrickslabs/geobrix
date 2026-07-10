# Design — `netcdf_gbx` lightweight-tier reader

- **Date:** 2026-07-10
- **Branch:** `examples/vapor-eyes`
- **Status:** Approved (design); implementation plan pending
- **Sub-project:** Spec A of two. Spec B is the `vapor-eyes` methane-detection example series, which consumes this reader (`docs/superpowers/specs/…-vapor-eyes-series-design.md`, to be written after this ships).

## 1. Context & motivation

The `vapor-eyes` example series (Permian Basin methane detection) needs to read
satellite/reanalysis data delivered as **NetCDF**, a format GeoBrix cannot read
today. The light-tier raster readers (`raster_gbx`, `gtiff_gbx`, `pmtiles_gbx`)
cover COG/GeoTIFF; the docs explicitly call out NetCDF as a heavy-GDAL-only
driver. This spec adds an **initial NetCDF reader to the lightweight (`pyrx`)
API**. A writer and a heavyweight reader are explicit later fast-follows (§13),
not part of this cut.

The design posture is "GeoBrix-maxxing": everything stays inside the GeoBrix
engine (RasterX / VectorX + Databricks-native ST), no Apache Sedona / RasterFrames.

### Which `vapor-eyes` data actually touches this reader

| Tier | Source | Res / cadence | Format | GeoBrix path |
|---|---|---|---|---|
| Wide-area screen | Sentinel-5P TROPOMI CH4 | 7 km, daily | **NetCDF-4, swath** | `netcdf_gbx` **vector** → points → H3 |
| Targeted detect | Sentinel-2 B11/B12 SWIR | 20 m | COG | `gtiff_gbx` raster (existing) |
| High-res attribute | EMIT L2B CH4 enhancement | 60 m | **COG** (not NetCDF — verified) | `gtiff_gbx`/`raster_gbx` raster (existing) |
| Facility attribution | ERA5 wind (u/v) | ~0.25° | **NetCDF-4, regular grid** | `netcdf_gbx` **raster** |

Two facts pinned by research on 2026-07-10:

- **EMIT L2B CH4 enhancement ships as Cloud-Optimized GeoTIFF**, not NetCDF
  (V2 = three COGs at 60 m; V1 decommissioned 2026-03-26). It is already
  orthorectified, so it rides the existing raster readers — **no NetCDF, no GLT
  orthorectification anywhere in `vapor-eyes`.**
- **Sentinel-5P L2 CH4 is netCDF-4 with irregular (swath) footprint geometry**
  and a `qa_value` quality field — a 2-D-coordinate ("curvilinear") layout.

Net: the reader's two real customers are **S5P (vector mode)** and **ERA5
(raster mode)**.

## 2. Goals / non-goals

**Goals**

1. A pure-Python `pyrx` DSv2 DataSource named `netcdf_gbx` that reads CF-convention
   NetCDF into GeoBrix's **existing** schemas — no new downstream contracts.
2. Two output modes from one reader: **raster** (→ tile struct) and **vector**
   (→ geometry + attributes).
3. A `TropomiDownloader` (`gbx.sample`) that stages real S5P granules for a
   vector-mode integration test — the reader's real-data proof-of-life.
4. GDAL-free light-tier install: exactly **one** new dependency (`netcdf4`);
   `xarray` is already present transitively via `xarray-spatial`.

**Non-goals (this cut)**

- A NetCDF **writer** (fast-follow).
- A **heavyweight** (Scala/JVM) NetCDF reader (fast-follow).
- In-reader **regridding/resampling** of swath data to a raster grid (swath is
  handled honestly via vector points instead — §5).
- In-reader **orthorectification** (GLT application) — not needed by `vapor-eyes`.
- **Multi-time-step fan-out** — the initial reader takes a single 2-D slice per
  variable; time fan-out is documented as a later increment.
- **Quality filtering** — the reader passes `qa_value` (and all requested
  variables) through as columns; the consumer filters.

## 3. Core insight — the reader is a transcoder

The light tile contract is defined once, in
`python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py:21` (`TILE_SCHEMA`):

```
struct<cellid: bigint, raster: binary, metadata: map<string,string>>
```

`raster` is **GeoTIFF bytes** (CRS/transform/nodata embedded), decoded by
rasterio. So `netcdf_gbx` is not a new raster type — in raster mode it **reads a
NetCDF variable into a georeferenced numpy array, then re-encodes to GeoTIFF
bytes** into the exact `(source, tile)` struct every light raster reader emits
(`reader_schema()` at `ds/raster.py:57`). Everything downstream
(`rst_h3_tessellate`, band math, tiling, PMTiles) works unchanged.

In vector mode it emits WKB point geometry + attribute columns, matching the
light vector reader (pyogrio) convention, so `gbx_st_*` / native ST / H3 binning
work unchanged.

## 4. Geometry classes

NetCDF encodes geography in several layouts; only some are "already a map."

| Class | Storage | Example | Reader handling |
|---|---|---|---|
| 1. Regular lat/lon grid | 1-D even `lat[]`,`lon[]` → one affine | ERA5 wind | **raster** (default) |
| 2. Projected regular grid | 1-D `x[]`,`y[]` + CF `grid_mapping` | gridded L3 | **raster** (CRS from grid_mapping) |
| 3. Curvilinear / swath | 2-D `lat[y,x]`,`lon[y,x]` | S5P TROPOMI L2 | **vector** (per-cell points) |
| 4. Sensor geometry + GLT | array in acquisition geometry | EMIT raw L1B/L2A | **rejected** (not needed) |

The key design move: **swath data (class 3) is read as vector points, not
regridded.** Emitting one point per cell (cell-center lon/lat + values) is
lossless and free of buried resampling/target-grid choices. Class 4 (raw
sensor + GLT) is rejected with a clear error; `vapor-eyes` never needs it
because EMIT's methane product is already-orthorectified COG.

## 5. Two modes

One reader, one `mode` option (default `raster`). Branching a reader on an
option has precedent: `pmtiles_gbx` branches its reader on a `source` option
(`ds/pmtiles.py:83`).

### `mode=raster` (default)
- Accepts **class 1 & 2** only. Class 3 → `ValueError` steering to `mode=vector`.
  Class 4 → `ValueError` **rejected in both modes** (see below).
- CF metadata → affine transform + CRS (default EPSG:4326 when lat/lon coords
  present; CRS from `grid_mapping` for projected grids).
- Re-encodes the variable's 2-D slice to GeoTIFF via rasterio `MemoryFile`,
  reusing `ds/_encode.py` / `pyrx/_serde.py:build_tile`.
- Output: `struct<source: string, tile: TILE_SCHEMA>` (identical to `gtiff_gbx`).
- `sizeInMB` sub-tiling and `bbox`/`bboxCrs` window-on-read inherited unchanged.

### `mode=vector`
- **DSG point data** (CF discrete sampling geometries: `lat[obs]`, `lon[obs]`,
  `var[obs]`) → point features natively.
- **Any 2-D field** (class 1/2/3) → coerced to **one point per cell**
  (cell-center lon/lat + variable values). This is how swath (class 3) is read —
  class 3 has per-pixel `lat[y,x]`/`lon[y,x]`, so each cell already has a known
  coordinate.
- **Class 4** (raw sensor geometry + GLT) → `ValueError`, **rejected here too**.
  Sensor geometry has no per-pixel lon/lat; georeferencing *is* the GLT, which
  must be applied (orthorectified) before points are meaningful. That ortho is a
  deferred fast-follow (§13), and `vapor-eyes` never needs class-4 input.
- Output: geometry WKB column + one attribute column per requested variable,
  matching the light vector reader (pyogrio) schema so downstream `gbx_st_*` /
  native ST / `h3_*` compose directly. (Exact schema mirrored from the existing
  `geojson_gbx`/`shapefile_gbx` reader output — to be confirmed against
  `ds/` vector reader source during implementation.)
- `bbox`/`bboxCrs` filter emitted points; `qa_value` and all requested
  variables travel as attribute columns for consumer-side filtering.

## 6. Architecture

Mirror the `gtiff_gbx` template (`ds/gtiff.py`):

- `NetcdfGbxReader(RasterGbxReader)` — parse `variable(s)`, `group`, `mode` in
  `__init__` (in addition to inherited `path`/`filterRegex`/`bbox`/`bboxCrs`/
  `sizeInMB`); branch `read()` on `mode`. Raster branch reuses the existing
  encode path; vector branch builds WKB points + attribute rows.
- `NetcdfGbxDataSource(RasterGbxDataSource)` — `name()` → `"netcdf_gbx"`;
  `reader()` returns `NetcdfGbxReader`; `schema()` returns the tile schema for
  raster mode and the vector schema for vector mode (schema is mode-dependent —
  see risk R1).
- Register: import the new DataSource in `ds/register.py` and add to the
  `_SOURCES` tuple (`ds/register.py:27`).

Decode backend (approach #1 of three considered; #2 relying on rasterio's
bundled-GDAL netCDF driver rejected as fragile, #3 rioxarray rejected as
redundant): **`xarray` + `netcdf4` → numpy + CF metadata**, then rasterio
`MemoryFile` for the GeoTIFF encode (raster) or shapely/WKB for points (vector).

## 7. Options

| Option | New? | Meaning |
|---|---|---|
| `path` | inherited | file/dir/glob |
| `filterRegex` | inherited | file filter (default `.*`) |
| `bbox`, `bboxCrs` | inherited | AOI window (raster) / point filter (vector) |
| `sizeInMB` | inherited | raster sub-tiling |
| `variable` (`variables`) | **new** | which NetCDF variable(s) to materialize |
| `group` | **new** | HDF5 group path (e.g. S5P `/PRODUCT/`) |
| `mode` | **new** | `raster` (default) or `vector` |

Single 2-D slice per variable in this cut (an optional index selector for
higher-dim variables may be added; multi-time fan-out is a later increment).

## 8. Dependencies

- **`xarray`** — already present (transitive via `xarray-spatial>=0.4,<1`,
  `pyproject.toml:115`). Used for CF decoding / coordinate + `grid_mapping`
  handling.
- **`scipy`** — already present (`pyproject.toml:66`). xarray's NetCDF-3 engine.
- **`netcdf4`** — **the one new dep**, folded into the `[light]` extra
  (`pyproject.toml` `light = [...]`). Single package bundling netcdf-c + HDF5;
  reads both NetCDF-3 and NetCDF-4/HDF5 (S5P, EMIT-core, modern CDS ERA5).
  Rationale for folding into `[light]` (vs. a dedicated `[netcdf]` extra):
  weight is a single wheel, and it honors the "fewer optional installs"
  preference; `geobrix[light]` reads NetCDF out of the box.
- Requirements lock: add to `requirements-pyrx-ci.in` + recompile the hashed
  `.txt` per the light-CI lock procedure.
- **Serverless pin verification (implementation task):** `[light]` is heavily
  env-v5-pin-sensitive. Verify `netcdf4` installs cleanly on Serverless env v5
  (Python 3.12) without shadowing immutable base packages; pin a floor/ceiling
  if the resolver floats it (same discipline as the rio-tiler / mapbox-vector-tile
  pins already documented in `pyproject.toml`).

## 9. `TropomiDownloader` (`gbx.sample`)

Mirror `DemDownloader` (`sample/dem.py`) — the `discover` / `download` / `read`
shape, Serverless-safe (no `spark.conf.set`/`_jvm`/`.rdd`/cache/persist):

- Source: Planetary Computer STAC collection `sentinel-5p-l2-netcdf`
  (same `StacClient` path the eo-series/helios downloaders use).
- `discover(bbox, …)` — metadata-only STAC search for S5P L2 CH4 granules.
- `download(bbox, out_dir, …)` — distributed asset fetch via `StacClient.download`.
- `read(out_dir, spark=None)` — loads via
  `spark.read.format("netcdf_gbx").option("mode","vector")
   .option("group","/PRODUCT/").option("variables","methane_mixing_ratio_bias_corrected,qa_value")…`
- Register in `sample/__init__.py` `__all__` alongside `NaipDownloader` /
  `DemDownloader` / `OvertureClient`; add a `download_tropomi_aoi` one-shot wrapper.

`EmitDownloader` (COG, rides `raster_gbx`) and an ERA5 `WindDownloader` (CDS-auth
friction) are **Spec B**.

## 10. Testing

Light-DS unit tests in `python/geobrix/test/ds/test_netcdf_datasource.py`,
mirroring `test_raster_datasource.py`. Synthesize fixtures inline with `netcdf4`
(no network):

1. **Raster round-trip** — write a small CF regular-grid NetCDF; read via
   `netcdf_gbx` raster mode; assert `(source, tile)` schema, `cellid == -1`, the
   11 metadata keys, and pixel-exact equality after decoding the tile GeoTIFF.
2. **Projected grid** — CF `grid_mapping` → assert CRS carried into the tile.
3. **Vector DSG** — write CF point/observation NetCDF; read vector mode; assert
   point count, geometry WKB validity, attribute columns.
4. **Swath → points** — write a 2-D-coordinate (curvilinear) NetCDF; read vector
   mode; assert one point per cell with correct lon/lat + value.
5. **Rejection** — swath/class-3 in `mode=raster` raises a clear `ValueError`.

Integration (marked `integration`, needs sample data): read a **staged S5P
granule** (vector mode) via `TropomiDownloader.read`; read a **staged ERA5
sample** (raster mode). No binding-parity / function-info tests (DataSource, not
a SQL function).

## 11. Docs deliverables

- `docs/docs/readers/netcdf.mdx` — front-matter, `!!raw-loader!` import, Options
  tables (incl. `mode`/`variable`/`group`), raster + vector examples.
- `docs/tests/python/readers/netcdf_gbx_read_examples.py` — example code as
  named string constants + runnable verifier functions.
- `docs/tests/python/readers/test_netcdf_gbx_read_examples.py` — wrapper running
  the verifiers against staged sample data.
- Sidebar entry under "Readers & Writers → Readers → Named" (`docs/sidebars.js`).

## 12. Data flow

```
NetCDF file (.nc)                    netcdf_gbx reader
      │                          ┌── mode=raster ─→ class 1/2 grid
      │  xarray + netcdf4        │      → CF → affine + CRS
      ▼  (CF decode)             │      → GeoTIFF bytes (rasterio MemoryFile)
 variable(s) + coords ───────────┤      → (source, tile) struct  ── rst_* / H3 / tiling
                                 │
                                 └── mode=vector ─→ DSG points, or
                                        any 2-D field → per-cell points
                                        → WKB geometry + attr cols  ── gbx_st_* / H3
```

## 13. Fast-follows (explicitly out of scope)

- NetCDF **writer** (light tier), then heavyweight reader + writer.
- **Multi-time-step** fan-out (one row/tile per time step).
- Optional **in-reader regrid / GLT ortho** as an explicit opt-in
  (`orthorectify=` / `regrid=`) if a future example needs class-3→raster or
  class-4 directly.

## 14. Risks / open items

- **R1 — mode-dependent schema.** PySpark `DataSource.schema()` is called before
  `read()`; the output schema differs by `mode`. Confirm the Python DataSource
  API lets `schema()` read `self.options["mode"]` (it does receive options) and
  return the right struct; `pmtiles_gbx`'s `source`-branching reader is the
  precedent to follow. Validate early in implementation.
- **R2 — vector output schema exact shape.** Mirror the existing
  `geojson_gbx`/`shapefile_gbx` reader output exactly (geometry column name,
  WKB vs EWKB, attribute typing). Read that reader's source before coding.
- **R3 — Serverless `netcdf4` pin.** See §8 — verify on env v5.
- **R4 — S5P group/variable names.** Confirm the exact `/PRODUCT/` group and
  `methane_mixing_ratio_bias_corrected` / `qa_value` variable names against a
  real granule before finalizing `TropomiDownloader.read` defaults.
