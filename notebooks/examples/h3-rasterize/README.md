# H3 Cell Rasterize + Band Stacking — Part 1: DEM

> **Part 1 of 3** in the DEM → LiDAR → CHM series.
> Part 2 covers LiDAR point-cloud processing with `rst_binpoints`;
> Part 3 assembles a Canopy Height Model with `rst_chm`.

A self-contained notebook that runs the full DEM-to-H3-raster pipeline:
download a **USGS 3DEP LiDAR-DTM (≈2 m)** via `DemDownloader.lidar_dtm()` and an
**Overture Maps** land mask via `OvertureClient`, tile the DEM into distributed
virtual tiles with the `raster_gbx` reader, clip each tile to land with `rst_clip`,
extract distributed isobands with `rst_isoband`, index each band using the
Databricks built-in `h3_try_coverash3` at H3 res 10, burn onto a shared aligned
canvas, and assemble a multi-band GeoTIFF stack. `cells_as_gdf` dissolves
per-group polygons on Spark via the Databricks product `st_union_agg` before
bringing merged footprints to the driver. Visualized throughout with the
`gbx.vizx` helpers.

![H3 Rasterize — DEM isobands to a multi-band H3 raster stack](../../../resources/images/diagrams/h3-rasterize/h3-rasterize.png)

> **Lightweight tier (Serverless) by default.** The notebook uses the lightweight
> tier — `geobrix[light_env5,stac,vizx,overture]` — pure Python/PySpark bindings with no
> JAR or GDAL init script required. It targets **Serverless environment 5**. See
> [Execution Tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

> **Data source: USGS 3DEP LiDAR-DTM (≈2 m), San Francisco.** `DemDownloader.lidar_dtm()`
> fetches 3DEP LiDAR-DTM tiles for `SF_BBOX = (-122.52, 37.70, -122.35, 37.83)` from
> Planetary Computer STAC and stages them to the sample-data Unity Catalog Volume on first
> run (idempotent — skipped if files already exist). `OvertureClient` fetches Overture Maps
> `base/water` features to build the land mask. No manual download is required.

---

## Notebooks at a glance

### h3\_rasterize\_isobands.ipynb

Full pipeline — LiDAR-DTM download and virtual tiling, Overture land mask, land-only clipping
with `rst_clip`, distributed isoband extraction with `rst_isoband`, product-H3 indexing,
shared-canvas computation, per-band rasterize, and multi-band stacking — producing a
multi-band GeoTIFF that stacks twelve 25 m elevation bands (0–300 m) over San Francisco.
Visualization appears after each major step: the raw DEM, per-cell H3 footprints on the
shared canvas, cumulative tier overlays, and a final coverage-depth composite.

---

## Files

| File | Purpose |
|---|---|
| `h3_rasterize_isobands.ipynb` | Complete pipeline notebook: DEM download → isoband extraction → product-H3 indexing → shared grid spec → per-band rasterize → band stack → visualization. |

---

## Prerequisites

- **Databricks Runtime 17.3 LTS / 18 LTS, or Serverless** (Python 3.12). The
  lightweight default runs on Serverless. The `CREATE TEMP TABLE` materialization
  used in Step 4 requires Serverless or DBR 18.1+ — it is **not** supported on
  dedicated/single-user clusters.
- **GeoBrix 0.5.2.** Update the `%pip install` cell to point at your staged
  `geobrix-0.5.2-py3-none-any.whl`. The `[light_env5,stac,vizx,overture]` extras install
  rasterio, geopandas, matplotlib, mapclassify, the planetary-computer STAC client, and
  Overture Maps helpers — no other dependencies assumed pre-staged.
- **Unity Catalog Volumes.** `DemDownloader.lidar_dtm()` stages 3DEP LiDAR-DTM GeoTIFFs to
  `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/elevation-3dep-lidar/dem_2m`;
  `OvertureClient` stages Overture water features to `.../sf/overture-water`.
  The Volume root must already exist; sub-directories are created automatically.
- **Databricks product H3.** `h3_try_coverash3` is a Databricks built-in function
  available on **DBR 16.3+ / Serverless** (tested on Serverless env 5), accessed via the product Python bindings
  (`from pyspark.databricks.sql import functions as DBF`) — no additional install is needed.

---

## Run order

This is a single notebook; run all cells top to bottom. The `%pip install` + `%restart_python`
pair at the top restarts the Python kernel — subsequent cells import from the freshly installed
wheel. Cells after the restart are safe to re-run individually once the wheel is installed.

1. **Install and restart** — `%pip install "geobrix[light_env5,stac,vizx,overture] @ file://…"` + `%restart_python`.
2. **Imports and registration** — `rx.register(spark)`, `gx.register(spark)`, and `register(spark)` install the SQL UDFs.
3. **Download DEM and land mask** — `DemDownloader.lidar_dtm().download(…)` fetches USGS 3DEP
   LiDAR-DTM (≈2 m) tiles (idempotent); `OvertureClient` fetches Overture Maps `base/water`
   features and builds the land multipolygon by subtracting water from the AOI box.
4. **Steps 1–3** — `raster_gbx` reader with `tileSize=512` creates virtual tiles; `rst_transform`
   reprojects each to EPSG:4326 (the LiDAR-DTM is UTM zone 10N); `rst_clip`
   masks each tile to land; `rst_isoband` extracts 12 bands at 25 m intervals (distributed Spark);
   `h3_try_coverash3` indexes each band at H3 res 10;
   `rst_h3_gridspec` computes the shared canvas.
5. **Steps 4–5** — `rst_h3_rasterize_agg` burns each band and materializes to a session temp table;
   `rst_frombands_agg` assembles the multi-band stack.

---

## Data flow

```text
DemDownloader.lidar_dtm()  →  USGS 3DEP LiDAR-DTM (≈2 m)  (Planetary Computer STAC → Volume)
OvertureClient             →  base/water features → land multipolygon (EPSG:4326)
        │
        ▼  raster_gbx reader (tileSize=512)  →  virtual tiles (bytes-free path + window)
        │
        ▼  rst_transform → EPSG:4326  (LiDAR-DTM is UTM zone 10N)
        │
        ▼  rst_clip  (land polygon, cutline_all_touched=True)  (Spark, distributed)
Land-clipped virtual tiles  (Bay/Pacific → NoData)
        │
        ▼  rst_isoband  25 m breaks 0–300 m  (Spark, distributed)
Elevation isobands: 12 polygon bands, WKB output              [Step 1]
        │
        ▼  h3_try_coverash3  @ H3 res 10  (Databricks product)
(band_level, cellid) rows                                      [Step 2]
        │
        ▼  rx.rst_h3_gridspec  (Spark)
Shared pixel-aligned canvas  (EPSG:4326)                       [Step 3]
        │
        ▼  rx.rst_h3_rasterize_agg groupBy band_level  (Spark)
Per-band presence-mask tiles → CREATE TEMP TABLE band_tiles    [Step 4]
        │
        ▼  rx.rst_frombands_agg  (Spark)
Multi-band GeoTIFF tile: 12 bands                              [Step 5]
        │
        ▼  plot_raster(composite="depth")
Coverage-depth figure: pixel = count of bands covering that location
```

---

## Key GeoBrix / Databricks functions shown

- **GeoBrix sample helpers**: `DemDownloader.lidar_dtm()` (3DEP LiDAR-DTM via Planetary Computer STAC), `OvertureClient` (Overture Maps `base/water` for the land mask).
- **GeoBrix RasterX** (`rx.*`): `rst_clip` (land-polygon cutline masking), `rst_isoband`, `rst_h3_gridspec`, `rst_h3_rasterize_agg`, `rst_frombands_agg`.
- **GeoBrix readers**: `raster_gbx` with `tileSize=512` (virtual tile splitting — one Spark row per 512×512-px tile, bytes-free, read directly by compute).
- **GeoBrix viz** (`gbx.vizx`): `plot_file` (staged DEM GeoTIFF — virtual tiles carry no bytes, so the file is plotted directly), `plot_static` (per-cell H3 footprints),
  `plot_interactive` (interactive multi-layer map), `cells_as_gdf` (H3 footprints as a GeoDataFrame;
  `dissolve_by="band_level"` with `dissolve_engine="product"` merges each band via Spark `st_union_agg`), `grid_as_gdf`
  (shared-canvas rectangle), `plot_mask_layers` (overlay cumulative tiers with distinct colours and a
  legend), `plot_raster` (stacked raster as `composite="depth"` coverage map).
- **Databricks product H3**: `h3_try_coverash3` (overlap — fills every H3 cell the polygon touches), accessed via `from pyspark.databricks.sql import functions as DBF` (DBR 16.3+ / Serverless). Accepts WKB BINARY geometry directly — no `ST_GeomFromWKB` needed.
- **Full API reference**: [RasterX functions](https://databrickslabs.github.io/geobrix/docs/api/raster-functions) · [Viz helpers](https://databrickslabs.github.io/geobrix/docs/api/vizx).

---

## Gotchas

- **Temp table vs cache.** Step 4 materializes the per-band tiles into a session-scoped
  temp table (`CREATE TEMP TABLE band_tiles`) because `.cache()` / `.persist()` are
  unavailable on Serverless. The temp table is dropped automatically when the session
  ends. If you are on a dedicated/single-user cluster (which does not support temp
  tables), replace the `CREATE TEMP TABLE` block with a managed Delta table write and
  a subsequent `spark.table(...)` read.
- **Product H3 takes WKB.** `h3_try_coverash3` accepts WKB geometry — exactly what
  `rst_isoband` produces. Do not convert to the native Databricks `GEOMETRY` type
  first; the function requires WKB input.
- **Virtual tiles are read directly by compute.** The `raster_gbx` reader emits bytes-free
  path + window structs; `rst_clip` and `rst_isoband` read each tile's pixels on the
  executor, so no raster bytes are shipped from the driver. `rst_isoband` and the product
  H3 functions run as distributed Spark columns — no driver-side loop is needed. For
  production pipelines ingesting many tiles, load them with
  `spark.read.format("raster_gbx").option("tileSize","512").load(path)` and pass the
  whole DataFrame through; the pipeline scales without modification.
- **Volume write is serverless-safe.** `DemDownloader` stages AOI-windowed 3DEP GeoTIFFs
  to the Unity Catalog Volume using sequential I/O — idempotent and safe on Serverless.
  Do not use `seek` on Volume paths.

---

## Related resources

- [H3 Rasterize example page](https://databrickslabs.github.io/geobrix/docs/notebooks/h3-rasterize)
- [GeoBrix RasterX API](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)
- [GeoBrix Viz API](https://databrickslabs.github.io/geobrix/docs/api/vizx)
- [EO-Series notebooks](../eo-series/) — STAC download, band stacking, clipping pipeline
