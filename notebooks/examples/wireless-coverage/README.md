# Wireless Coverage — LiDAR Surface Foundations

A notebook series that builds the **elevation and surface data foundation** for
wireless-coverage analysis directly from a USGS 3DEP **LiDAR point cloud** — data first,
analytics later. The surfaces produced (CHM, DSM, bare-earth DTM) are the inputs later parts
consume for antenna siting and line-of-sight analysis.

![Wireless Coverage series — config_nb → 01 → 02a/02b → 03a → 03b → 04](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/diagrams/wireless-coverage/wireless-coverage-overview.png)

The LiDAR notebooks (Parts 1, 2a, 3a, 3b, and 4) work over **all of San Francisco**
(`SF_AOI = (-122.55, 37.70, -122.35, 37.85)`), reading the staged 3DEP LiDAR (`.laz` EPT
nodes) with the GeoBrix **`lidar_gbx`** reader and binning returns into 1 m rasters **per
spatial tile** with the bounded `rx.bin_points_tiled` (a single 1 m raster over SF is
~460 M pixels; the bounded binner keeps peak memory ∝ the raster grid, not the point count,
so full-density `DECIMATE=1` never OOMs). Part 2b covers the same SF area but starts from a
DSM raster rather than a point cloud. Binned surfaces are persisted to **permanent tables**
you own (a `CATALOG.SCHEMA` you set) and read back downstream.

## Executed output — full San Francisco

The series runs end to end over the whole San Francisco AOI (`DEMO=False`); the committed notebooks ship
the smaller Golden Gate Park demo (`DEMO=True`). A few renders from the executed notebooks — the full-SF
surfaces first, then the Golden Gate Park detail where the per-pixel and siting views are legible.

First, the surface stack over all of San Francisco — bare earth (DEM) beside the full surface (DSM), so buildings and canopy read as the difference between them:

![Full San Francisco — DEM (bare earth) vs DSM (surface: buildings + canopy) elevation bands at H3 resolution 10, side by side (Part 3a)](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/screenshots/wireless-coverage/wc-full-dem-vs-dsm.png)

Zooming into one neighborhood, Part 3a cross-checks that the H3 grid faithfully summarizes the underlying 1 m pixels — a hex reads as the color of the pixels beneath it:

![Golden Gate Park detail — the 1 m-pixel CHM raster beneath the H3-gridded maximum canopy height, same window and colormap (Part 3a)](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/screenshots/wireless-coverage/wc-full-chm-pixel-vs-h3.png)

Finally — zooming to the Golden Gate Park demo, where the ranked sites are legible — Part 4 scores each candidate by how many res-12 cells it can see, so the best sites rise to the top:

![Top-10 tower sites by coverage over Golden Gate Park — each site's res-12 H3 line-of-sight viewshed and 2.5 km buffer (Part 4)](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/screenshots/wireless-coverage/wc-top10-tower-viewsheds.png)

## Chain

| Notebook | Input | Output | Purpose |
|---|---|---|---|
| `config_nb` | — | functions, config | Shared imports, parameters, helpers (`_tbl`, `_persist`, `with_join_parent`, viz). `%run`-imported by every other notebook; not run on its own. |
| `01_pure_lidar_chm` | Staged `.laz` EPT nodes | `wc_surface_{dsm,dtm,chm}` (demo) | Shortest path: classify LiDAR returns → bin ground (`min`) = DTM; bin all (`max`) = DSM; `rst_chm` = CHM. Keeps only the CHM. |
| `02a_surfaces_dsm_dtm_chm` | Staged `.laz` EPT nodes | `wc_surface_{dsm,dtm,chm}` permanent tables + `OUT_DIR` GeoTIFFs | Main arc. Promotes a TIN bare-earth DTM (`rst_dtmfromgeoms_agg`) as the production terrain — gap-free from ground-classified returns. Writes the canonical surface tables Parts 3 and 4 consume. |
| `02b_surfaces_from_dsm` | DSM raster (no point cloud) | `wc_surface_{dsm,dtm,chm}` permanent tables | DSM-raster alternative. Approximates bare earth via morphological opening (`rst_filter` min→max). Lands the same canonical surface tables as Part 2a so Parts 3 and 4 work unchanged. |
| `03a_h3_gridding` | `wc_surface_{dsm,dtm,chm}` | `wc_h3_{dem,dsm,chm}_res10` + `parent_cellid` join key | H3 gridding. `rst_isoband` → `h3_try_coverash3` → elevation-tier grids; `gbx_rst_h3_rastertogridmax` → max-canopy; `h3_cellfill` (k=1, IDW) fills gaps. Adds a res-9 `parent_cellid` join key to every output table. |
| `03b_raster_pixel_surfaces` | `wc_surface_dsm`, Part 3a H3 grids | `wc_rpx_{raster_viewshed,h3_viewshed,h3_coverage,comparison}_demo` | Raster-pixel counterpoint. Visualizes 1 m-pixel DSM vs H3 cells over Golden Gate Park; then independently regenerates a candidate tower lattice and runs both `rst_viewshed_towers` (1 m raster) and `h3_los_visible` (res-12 H3) — two engines, same towers, head-to-head. |
| `04_tower_viewsheds` | `wc_surface_{dsm,dtm,chm}`, `wc_h3_{dem,dsm,chm}` | `wc_h3_{quickpass,candidate_sites,coverage,coverage_top10}` | Naive H3 tower siting. Res-9 centroid lattice → quick-pass rule-out (res-10, 30% threshold) → `h3_los_visible` exact viewshed (res-12, 2.5 km) → two-factor scoring (mast height, viewshed cells). |

## Data

- **Point cloud** — staged 3DEP LiDAR `.laz` (EPT nodes) under
  `/Volumes/geospatial_docs/wireless_coverage/data/lidar/sf/laz/`. The `lidar_gbx` reader
  recurses subfolders; each `.laz` becomes one Spark partition of points
  (`x, y, z, classification, return_number, …`, EPSG:3857).
- **Surface products (Part 2a / 2b output)** — DSM / DTM / CHM GeoTIFF tiles written to
  `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/lidar-surfaces` via
  the `gtiff_gbx` writer.

## Runtime

All notebooks run on the **lightweight tier** — `geobrix[light_env6,vizx]`, pure
Python/PySpark bindings with no JAR or GDAL init script — targeting **Databricks Serverless
environment 6** (`laspy`/`lazrs` for the LiDAR reader ship with the light base).
`DECIMATE=1` keeps **every** return (~830 M over SF) — an honest big-data run; raise
`DECIMATE` (e.g. 10) for a fast coarse pass while iterating. `DEMO=True` (default) limits
computationally heavy cells to a Golden Gate Park sub-AOI; set `DEMO=False` for the full
San Francisco AOI. See [Execution Tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

## Related

- **Production pipeline (Lakeflow SDP)** — [`./lakeflow`](./lakeflow/README.md) is the
  production counterpart to this notebook series: the same San Francisco AOI and USGS 3DEP
  LiDAR sources, packaged as a [Lakeflow Declarative Pipeline](https://docs.databricks.com/aws/en/dlt/)
  plus a tower-siting job in a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/)
  (`land → pipeline → siting → validate`), running the medallion bronze → silver → gold on
  Serverless. See its [README](./lakeflow/README.md) for deploy and run steps.
- **DEM extra** — [`../h3-rasterize`](../h3-rasterize) takes the complementary path: a
  ready-made **10 m seamless DEM** over the Bay Area through the full raster→H3 pipeline
  (`rst_clip` → `rst_isoband` → product H3 → `rst_h3_rasterize_agg` → `rst_frombands_agg`).
  No LiDAR required.
