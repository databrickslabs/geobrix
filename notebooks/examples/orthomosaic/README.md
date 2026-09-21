# Orthomosaic Photogrammetry Example Series

CPU/sparse SfM on Serverless environment 5 — produces a georeferenced RGB orthomosaic
GeoTIFF (+ COG + PMTiles) from a drone image dataset.

## Chain

| Notebook | Input | Output | Purpose |
|---|---|---|---|
| `config_nb` | — | functions, config | Shared imports, parameters, SfM helpers |
| `01_sfm_orthomosaic` | GitHub JPEGs | `orthomosaic.tif` | Download → exif_gbx → QC → SfM → ortho |
| `02_color_correction` | `orthomosaic.tif` | `orthomosaic_corrected.tif` | Per-channel percentile stretch |
| `03_cog` | `orthomosaic_corrected.tif` | `orthomosaic_cog.tif` | COG layout via `rst_cog_convert` |
| `04_pmtiles` | `orthomosaic_cog.tif` | `orthomosaic.pmtiles` | PMTiles for interactive map serving |
| `monitor` | `output_dir/sparse/` | — | Optional live SfM progress watcher |

## Dependencies

Installed by the `config_nb` `%pip` cell:

```
geobrix[light_env5,photogrammetry,vizx]
```

The `photogrammetry` extra bundles: `pycolmap>=4.0.0`, `rasterio>=1.3`, `rio-cogeo>=3.5`,
`folium`, `pymbtiles`, `mercantile`, `pmtiles`.

The wheel is fetched from:
```
/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.5.2-py3-none-any.whl
```

## Runtime

**Serverless environment 5** (CPU only). Compute-intensive steps:

| Step | Typical runtime (Old Orchard, ~170 images) |
|---|---|
| `exif_gbx` extraction | < 1 min |
| Distributed feature extraction | 5–10 min |
| Distributed pair matching | 3–8 min |
| Incremental mapping (pycolmap) | 10–20 min |
| Orthomosaic projection | 2–5 min |

Adjust `MAX_ORTHO_WORKERS` and `GSD_CM` (config) to trade speed against resolution.

## Sparse SfM — Phase 1 caveat

This series uses **sparse SfM only** (pycolmap `incremental_mapping`): features, matches,
and camera poses are recovered but no dense point cloud or mesh is computed.  The
orthomosaic is assembled by back-projecting each registered JPEG onto the estimated ground
plane and blending overlapping contributions.

Dense reconstruction (MVS depth maps → textured mesh → true-ortho) is **Phase 2** and
requires GPU compute (CUDA-enabled classic cluster).  The sparse Phase 1 ortho is suitable
for GSD estimation, visual inspection, and georeferenced tile serving; Phase 2 adds
metric accuracy for measurement workflows.

## Group-key seam

`config_nb` exposes `GROUP_KEY_COL` (default `"_group"`, constant single group).  Change
it to a real metadata column (e.g. a `"flight_date"` column derived from `timestamp`) to
run the SfM + ortho pipeline once per flight automatically.
