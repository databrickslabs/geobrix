# H3 Cell Rasterize + Band Stacking

A single self-contained notebook that runs the full H3-cell rasterization pipeline
on a real DEM — loading elevation data, extracting isobands, filling them with H3
hexagons, burning each band onto a shared aligned canvas, and assembling a multi-band
GeoTIFF stack. Visualized with the `gbx.vizx` helpers throughout.

![H3 Rasterize — DEM isobands to a multi-band H3 raster stack](../../../resources/images/h3-rasterize.png)

> **Lightweight tier (Serverless) by default.** The notebook uses the lightweight
> tier — `geobrix[light,vizx]` — pure Python/PySpark bindings with no JAR or GDAL
> init script required. It runs on Serverless compute or a standard cluster. See
> [Execution Tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

> **Data source: San Francisco Bay Area SRTM DEM.** The notebook reads
> `srtm_n37w123.tif` (EPSG:4326, public AWS Terrain Tiles). The DEM is auto-staged
> from AWS to the sample-data Unity Catalog Volume on first run (idempotent — skipped
> if the file already exists). No manual download is required.

---

## Notebooks at a glance

### h3\_rasterize\_isobands.ipynb

Five pipeline steps — driver-side DEM load and H3 polyfill, then three distributed
Spark aggregations — producing a multi-band GeoTIFF that stacks eight 100 m
elevation bands (0–800 m) over the SF Bay Area. Visualization appears after each
major step: the raw DEM, the per-cell H3 footprints overlaid on the shared canvas,
overlapping mid-coverage band shapes, and a final coverage-depth composite of the
full stack.

---

## Files

| File | Purpose |
|---|---|
| `h3_rasterize_isobands.ipynb` | Complete pipeline notebook: DEM load → isoband extraction → H3 polyfill → shared grid spec → per-band rasterize → band stack → visualization. |

---

## Prerequisites

- **Databricks Runtime 17.3 LTS / 18 LTS, or Serverless** (Python 3.12). The
  lightweight default runs on Serverless. The `CREATE TEMP TABLE` materialization
  used in Step 4 requires Serverless or DBR 18.1+ — it is **not** supported on
  dedicated/single-user clusters.
- **GeoBrix 0.4.0.** Update the `%pip install` cell to point at your staged
  `geobrix-0.4.0-py3-none-any.whl`. The `[light,vizx]` extras install rasterio,
  geopandas, folium, matplotlib, and mapclassify — no other dependencies assumed
  pre-staged.
- **Unity Catalog Volume.** The DEM staging cell writes to
  `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/elevation/`.
  The Volume root must already exist; sub-directories are created automatically.
  Point `DEM_PATH` at your own SRTM tile to skip the AWS download entirely.

---

## Run order

This is a single notebook; run all cells top to bottom. The `%pip install` + `%restart_python` pair at the top restarts the Python kernel — subsequent cells import from the freshly installed wheel. Cells after the restart are safe to re-run individually once the wheel is installed.

---

## Data flow

```text
SRTM tile  srtm_n37w123.tif  (AWS Terrain Tiles → Volume, auto-staged)
        │
        ▼  rasterio read + quantize (driver)
Elevation isobands: one polygon per 100 m band  [Step 1]
        │
        ▼  h3.polygon_to_cells at resolution 8 (driver)
(band_level, cellid) rows                       [Step 2]
        │
        ▼  rx.rst_h3_gridspec  (Spark)
Shared pixel-aligned canvas: width=499 height=505 SRID=4326  [Step 3]
        │
        ▼  rx.rst_h3_rasterize_agg groupBy band_level  (Spark)
Per-band presence-mask tiles → CREATE TEMP TABLE band_tiles  [Step 4]
        │
        ▼  rx.rst_frombands_agg  (Spark)
Multi-band GeoTIFF tile: 8 bands × 499×505 px               [Step 5]
        │
        ▼  plot_raster(composite="depth")
Coverage-depth figure: pixel = count of bands covering that location
```

---

## Key GeoBrix / Databricks functions shown

- **GeoBrix RasterX** (`rx.*`): `rst_h3_gridspec`, `rst_h3_rasterize_agg`, `rst_frombands_agg`.
- **GeoBrix viz** (`gbx.vizx`): `plot_file` (raw DEM render), `cells_as_gdf` (per-cell H3 footprints; pass `dissolve_by="band_level"` for larger sets to merge each band into one footprint polygon), `grid_as_gdf` (shared-canvas rectangle on a folium map), `plot_mask_layers` (overlay two mid-coverage bands with distinct colours and a legend), `plot_raster` (stacked raster rendered as `composite="depth"` coverage map).
- **Databricks built-in H3** (used indirectly): `h3.polygon_to_cells`, `h3.str_to_int`.
- **Full API reference**: [RasterX functions](https://databrickslabs.github.io/geobrix/docs/api/raster-functions) · [Viz helpers](https://databrickslabs.github.io/geobrix/docs/api/vizx).

---

## Gotchas

- **Temp table vs cache.** Step 4 materializes the per-band tiles into a session-scoped
  temp table (`CREATE TEMP TABLE band_tiles`) because `.cache()` / `.persist()` are
  unavailable on Serverless. The temp table is dropped automatically when the session
  ends. If you are on a dedicated/single-user cluster (which does not support temp
  tables), replace the `CREATE TEMP TABLE` block with a managed Delta table write and
  a subsequent `spark.table(...)` read.
- **Driver-side polyfill scope.** Steps 1 and 2 run on the driver and are appropriate
  for a single DEM tile. For production pipelines ingesting many tiles, move isoband
  extraction and `h3.polygon_to_cells` inside a pandas UDF or UDTF so the full prep
  step fans out across executors — only the downstream Spark aggregations
  (`rst_h3_gridspec`, `rst_h3_rasterize_agg`, `rst_frombands_agg`) are distributed
  in this demo.
- **`cells_as_gdf` at scale.** With tens of thousands of cells the default per-cell
  rendering is slow. Pass `dissolve_by="band_level"` to merge each band into a single
  footprint geometry before passing to folium — far fewer geometries to render.
- **SRTM elevation range.** The N37W123 tile covers the SF Bay area; the raw DEM
  reports values down to −1967 m (ocean bathymetry artefact) and up to 986 m.
  `MIN_ELEV_M=0` clips the sub-sea-level artefacts; `MAX_ELEV_M=800` caps the
  isoband sweep below the coastal peaks, giving eight clean 100 m bands over
  inhabited terrain.
- **Volume I/O is sequential.** The DEM staging cell writes via `shutil.copy` from a
  node-local temp directory — FUSE-safe. Do not use `seek` on Volume paths.

---

## Telco / coverage-analysis analogy

San Francisco is a natural stand-in for a wireless-coverage scenario: a dense, hilly
coastal city where terrain directly shadows RF propagation. Replace *elevation* with
*signal strength* and the pipeline is identical.

| DEM demo | Coverage-analysis equivalent |
|---|---|
| Elevation isoband polygon | Signal contour polygon (e.g. −80 dBm zone) |
| `band_level` (integer) | Threshold index (e.g. tier 1 / tier 2 / …) |
| H3 polyfill at res 8 | H3 coverage cells per threshold |
| Multi-band stacked tile | Multi-threshold stacked coverage raster |

The stacked tile is ready for downstream analysis — joining against subscriber
locations or exporting as PMTiles for web visualization.

---

## Related resources

- [H3 Rasterize example page](https://databrickslabs.github.io/geobrix/docs/notebooks/h3-rasterize)
- [GeoBrix RasterX API](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)
- [GeoBrix Viz API](https://databrickslabs.github.io/geobrix/docs/api/vizx)
- [EO-Series notebooks](../eo-series/) — STAC download, band stacking, clipping pipeline
