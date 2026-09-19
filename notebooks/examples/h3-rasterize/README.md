# H3 Cell Rasterize + Band Stacking — DEM extra

> **A DEM extra in the [Wireless Coverage](../wireless-coverage) series.**
> The main series builds elevation surfaces from a raw LiDAR point cloud; this notebook takes the
> complementary path — a ready-made **10 m seamless DEM** — through the full raster→H3 pipeline.
> It stands alone: no LiDAR required.

A self-contained notebook that runs the full DEM-to-H3-raster pipeline:
download a **USGS 3DEP seamless (10 m)** DEM via `DemDownloader()` and an
**Overture Maps** land mask via `OvertureClient`, tile the DEM into distributed
virtual tiles with the `raster_gbx` reader, clip each tile to land with `rst_clip`,
extract distributed isobands with `rst_isoband`, index each band using the
Databricks built-in `h3_try_coverash3` at H3 res 9, burn onto a shared aligned
canvas, and assemble a multi-band GeoTIFF stack. `cells_as_gdf` dissolves
per-group polygons on Spark via the Databricks product `st_union_agg` before
bringing merged footprints to the driver. Visualized throughout with the
`gbx.vizx` helpers.

![H3 Rasterize — DEM isobands to a multi-band H3 raster stack](../../../resources/images/diagrams/h3-rasterize/h3-rasterize.png)

> **Lightweight tier (Serverless) by default.** The notebook uses the lightweight
> tier — `geobrix[light_env5,stac,vizx,overture]` — pure Python/PySpark bindings with no
> JAR or GDAL init script required. It targets **Serverless environment 5**. See
> [Execution Tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

> **Data source: USGS 3DEP seamless (10 m), San Francisco Bay Area.** `DemDownloader()`
> fetches the finest seamless tier (10 m) for `BAY_BBOX = (-122.55, 37.45, -121.95, 37.95)` from
> Planetary Computer STAC and stages it to the sample-data Unity Catalog Volume on first
> run (idempotent — skipped if files already exist). `OvertureClient` fetches Overture Maps
> `base/water` features to build the land mask. No manual download is required.

---

## Notebooks at a glance

### h3\_rasterize\_isobands.ipynb

Full pipeline — seamless-DEM download and virtual tiling, Overture land mask, land-only clipping
with `rst_clip`, distributed isoband extraction with `rst_isoband`, product-H3 indexing,
shared-canvas computation, per-band rasterize, and multi-band stacking — producing a
multi-band GeoTIFF that stacks twelve 60 m elevation bands (0–720 m) over the San Francisco Bay
Area. Visualization appears after each major step: the raw DEM, per-cell H3 footprints on the
shared canvas, cumulative tier overlays, and a final coverage-depth composite.

---

## Files

| File | Purpose |
|---|---|
| `h3_rasterize_isobands.ipynb` | Complete pipeline notebook: DEM download → isoband extraction → product-H3 indexing → shared grid spec → per-band rasterize → band stack → visualization. |

---

## Prerequisites

- **Databricks Runtime 17.3 LTS / 18 LTS, or Serverless** (Python 3.12). The
  lightweight default runs on Serverless. Step 4 persists the
  per-tier tiles to a **permanent table** you own (`CATALOG.SCHEMA.wc_dem_tier_tiles`); set a
  catalog/schema you can write to.
- **GeoBrix 0.5.2.** Update the `%pip install` cell to point at your staged
  `geobrix-0.5.2-py3-none-any.whl`. The `[light_env5,stac,vizx,overture]` extras install
  rasterio, geopandas, matplotlib, mapclassify, the planetary-computer STAC client, and
  Overture Maps helpers — no other dependencies assumed pre-staged.
- **Unity Catalog Volumes.** `DemDownloader()` stages the 3DEP seamless (10 m) GeoTIFFs to
  `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/elevation-3dep/dem_10m`;
  `OvertureClient` stages Overture water features to `.../sf/overture-water-bay`.
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
3. **Download DEM and land mask** — `DemDownloader().download(…)` fetches USGS 3DEP
   seamless (10 m) tiles (idempotent); `OvertureClient` fetches Overture Maps `base/water`
   features and builds the land multipolygon by subtracting water from the AOI box.
4. **Steps 1–3** — `raster_gbx` reader with `tileSize=512` creates virtual tiles (already
   EPSG:4326 — no reprojection); `rst_clip` masks each tile to land; `rst_isoband` extracts
   12 bands at 60 m intervals (distributed Spark); `h3_try_coverash3` indexes each band at
   H3 res 9; `rst_h3_gridspec` computes the shared canvas.
5. **Steps 4–5** — `rst_h3_rasterize_agg` burns each tier and persists to a permanent table (`CATALOG.SCHEMA.wc_dem_tier_tiles`);
   `rst_frombands_agg` assembles the multi-band stack.

---

## Data flow

```text
DemDownloader()  →  USGS 3DEP seamless (10 m)  (Planetary Computer STAC → Volume, EPSG:4326)
OvertureClient   →  base/water features → land multipolygon (EPSG:4326)
        │
        ▼  raster_gbx reader (tileSize=512)  →  virtual tiles (bytes-free path + window)
        │
        ▼  rst_clip  (land polygon, cutline_all_touched=True)  (Spark, distributed)
Land-clipped virtual tiles  (Bay/Pacific → NoData)
        │
        ▼  rst_isoband  60 m breaks 0–720 m  (Spark, distributed)
Elevation isobands: 12 polygon bands, WKB output              [Step 1]
        │
        ▼  h3_try_coverash3  @ H3 res 9  (Databricks product)
(band_level, cellid) rows                                      [Step 2]
        │
        ▼  rx.rst_h3_gridspec  (Spark)
Shared pixel-aligned canvas  (EPSG:4326)                       [Step 3]
        │
        ▼  rx.rst_h3_rasterize_agg groupBy tier  (Spark)
Per-tier presence-mask tiles → permanent table wc_dem_tier_tiles [Step 4]
        │
        ▼  rx.rst_frombands_agg  (Spark)
Multi-band GeoTIFF tile: 12 bands                              [Step 5]
        │
        ▼  plot_raster(composite="depth")
Coverage-depth figure: pixel = count of tiers covering that location
```

---

## Key GeoBrix / Databricks functions shown

- **GeoBrix sample helpers**: `DemDownloader()` (3DEP seamless 10 m via Planetary Computer STAC), `OvertureClient` (Overture Maps `base/water` for the land mask).
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

- **Land clip is load-bearing over water.** Two-thirds of the Bay Area AOI is land; the rest is
  Bay and Pacific. Without the Overture land cutline the flat water surface floods the lowest
  elevation band with millions of H3 cells. `rst_clip` masks the water to NoData at the source
  so `rst_isoband` never emits water bands.
- **Match the top break to the terrain.** `rst_isoband` bands only the intervals between
  consecutive breaks — pixels above the highest break are dropped (they become holes). The Bay
  Area reaches ≈685 m, so the breaks run to 720 m; if you retarget the AOI, raise the top break
  above the DEM's maximum or the peaks disappear from the stack.
- **Permanent table, not a temp table.** Step 4 persists the per-tier tiles to a
  **permanent** table you own (`CATALOG.SCHEMA.wc_dem_tier_tiles`) via `saveAsTable` because
  `.cache()` / `.persist()` are unavailable on Serverless. A governed Delta table is reused
  across cells and sessions (no session-temp-table / DBR-18.1 constraint); `FORCE_RELOAD=False`
  reuses it, `True` rebuilds.
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
- **Volume write is serverless-safe.** `DemDownloader()` stages AOI-windowed 3DEP GeoTIFFs
  to the Unity Catalog Volume using sequential I/O — idempotent and safe on Serverless.
  Do not use `seek` on Volume paths.

---

## Related resources

- [Wireless Coverage series](https://databrickslabs.github.io/geobrix/docs/notebooks/wireless-coverage) — the main LiDAR-surface notebooks this extra complements
- [H3 Rasterize example page](https://databrickslabs.github.io/geobrix/docs/notebooks/h3-rasterize)
- [GeoBrix RasterX API](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)
- [GeoBrix Viz API](https://databrickslabs.github.io/geobrix/docs/api/vizx)
- [EO-Series notebooks](../eo-series/) — STAC download, band stacking, clipping pipeline
