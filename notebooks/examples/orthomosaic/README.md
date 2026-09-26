# Orthomosaic Photogrammetry Example Series

Drone imagery → georeferenced RGB orthomosaic (+ COG + PMTiles). Sparse SfM runs on
Serverless environment 5 (CPU); an optional dense MVS pass (`01b`) runs on the Serverless
GPU AI Runtime.

## Chain

| Notebook | Input | Output | Purpose |
|---|---|---|---|
| `config_nb` | — | functions, config | Shared imports, parameters, SfM helpers |
| `01a_sfm_orthomosaic` | GitHub JPEGs | `orthomosaic.tif` | Download → exif_gbx → QC → sparse SfM → ortho |
| `01b_sfm_orthomosaic_gpu` (optional) | `01a` sparse model | `orthomosaic_dense.tif`, `dsm_dense.tif`, `dense_merged.laz` (+ sharded `dense_<grp>_<cid>.laz` parts) | GPU dense MVS: patch_match_stereo → fusion → georef |
| `02_publish` | `orthomosaic.tif` | `orthomosaic_corrected.tif`, `orthomosaic_cog.tif`, `orthomosaic.pmtiles` | Publish: color correction → COG (`cog_gbx`) → PMTiles (`gbx_rst_xyzpyramid` + `pmtiles_gbx`) |
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

## Sparse vs dense reconstruction

`01a` runs **sparse SfM** (pycolmap `incremental_mapping`): features, matches, and camera
poses are recovered, and the orthomosaic is assembled by back-projecting each registered
JPEG onto the estimated ground plane and blending overlapping contributions.

`01b` (optional) adds **dense MVS** — COLMAP `patch_match_stereo` + stereo fusion on the
**Serverless GPU AI Runtime** (`pycolmap-cuda12`, CUDA 12), scheduled across the node's
GPUs by `dense_mvs_pool` — producing a dense orthomosaic, DSM, and LAZ point cloud
(per-cluster and merged). `02_publish` uses the dense orthomosaic when present, else the
sparse one. The sparse ortho suits GSD estimation, visual inspection, and tile serving;
the dense pass adds fidelity for measurement workflows.

## Group-key seam

`config_nb` exposes `GROUP_KEY_COL` (default `"_group"`, constant single group).  Change
it to a real metadata column (e.g. a `"flight_date"` column derived from `timestamp`) to
run the SfM + ortho pipeline once per flight automatically.
