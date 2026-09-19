# Wireless Coverage — LiDAR surface foundations

A notebook series that builds the **elevation and surface data foundation** for wireless-coverage
analysis directly from a USGS 3DEP **LiDAR point cloud** — data first, analytics later. The surfaces
produced here (CHM, DSM, bare-earth DTM) are the inputs later parts consume for antenna siting and
line-of-sight (`rst_viewshed`).

Both notebooks work over **all of San Francisco** (`SF_AOI = (-122.55, 37.70, -122.35, 37.85)`), reading
the staged 3DEP LiDAR (`.laz` EPT nodes) with the GeoBrix **`lidar_gbx`** reader and binning returns into
1 m rasters **per spatial tile** with the bounded `rx.bin_points_tiled` (a single 1 m raster over SF is
~460 M pixels; the bounded binner keeps peak memory ∝ the raster grid, not the point count, so
full-density `DECIMATE=1` never OOMs). Binned surfaces are persisted to **permanent tables** you own
(a `CATALOG.SCHEMA` you set) and read back downstream.

![Wireless Coverage — LiDAR point cloud to DSM / DTM / CHM](../../../resources/images/diagrams/wireless-coverage/wireless-coverage.png)

## Notebooks

| Notebook | What it builds | Key GeoBrix functions |
|---|---|---|
| **`01_pure_lidar_chm.ipynb`** | A **Canopy Height Model straight from the point cloud** — no external DEM, no coverage gaps. Bin ground returns (ASPRS class 2) → `min` = bare-earth DTM; bin all returns → `max` = DSM; `rst_chm(DSM, DTM)` = canopy. | `lidar_gbx` reader, `bin_points_tiled`, `rst_chm` |
| **`02_surfaces_dsm_dtm_chm.ipynb`** | The **surface products**: DSM and bare-earth DTM persisted as reusable **permanent tables** (a governed Delta of tiles; optional GeoTIFF export), plus a **binned-vs-TIN** bare-earth comparison, then CHM. | `bin_points_tiled`, `rst_dtmfromgeoms_agg` (+ product `st_makepoint`), `rst_chm`, optional `gtiff_gbx` export |

The difference in one line: **Part 1** keeps only the CHM (the shortest path from returns to canopy);
**Part 2** persists the DSM and DTM as products and shows the "if you didn't already have LiDAR-derived
surfaces" path, including a Delaunay-TIN bare earth that fills the small gaps a `min` bin can leave.

## Data

- **Point cloud** — staged 3DEP LiDAR `.laz` (Entwine Point Tile nodes) under
  `/Volumes/geospatial_docs/wireless_coverage/data/lidar/sf/laz/{GoldenGate,SanFranCoast}`. The
  `lidar_gbx` reader recurses the subfolders; each `.laz` becomes one Spark partition of points
  (`x, y, z, classification, return_number, …`, EPSG:3857).
- **Surface products (Part 2 output)** — DSM / DTM / CHM GeoTIFF tiles written to
  `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/lidar-surfaces` via the
  `gtiff_gbx` writer.

## Runtime

Both notebooks run on the **lightweight tier** — `geobrix[light_env5,vizx]`, pure Python/PySpark bindings
with no JAR or GDAL init script — targeting **Databricks Serverless environment 5** (`laspy`/`lazrs` for the
LiDAR reader ship with the light base). `DECIMATE=1` keeps **every** return (~830 M over SF) — an honest
big-data run; raise `DECIMATE` (e.g. 10) for a fast coarse pass while iterating. See
[Execution Tiers](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

## Related

- **DEM extra** — [`../h3-rasterize`](../h3-rasterize) takes the complementary path: a ready-made **10 m
  seamless DEM** over the Bay Area through the full raster→H3 pipeline (`rst_clip` → `rst_isoband` →
  product H3 → `rst_h3_rasterize_agg` → `rst_frombands_agg`). No LiDAR required.
