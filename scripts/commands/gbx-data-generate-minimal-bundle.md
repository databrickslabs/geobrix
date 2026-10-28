# Generate Minimal Doc-Test Bundle

Generates a minimal sample-data bundle under `sample-data/Volumes/main/default/test-data/geobrix-examples/` by extracting subsets from the full bundle using bounding boxes around NYC (default: center of Manhattan) and London (default: center of London). Vector data is limited by a configurable row count; raster data is clipped to the bbox (all pixels within bounds). Used so doc tests can pass in CI using the minimal bundle and path token `sample-data/Volumes/main/default/test-data`.

---

## Usage

```bash
bash scripts/commands/gbx-data-generate-minimal-bundle.sh [OPTIONS]
```

## Options

- `--source <dir>` — Full bundle root (default: .../geobrix_samples/geobrix-examples)
- `--out <dir>` — Output root (default: .../test-data/geobrix-examples)
- `--nyc-lon`, `--nyc-lat` — NYC center (default: -73.9857, 40.7484, Manhattan)
- `--london-lon`, `--london-lat` — London center (default: -0.1276, 51.5074)
- `--bbox-size <float>` — Half-width/height of bbox in degrees (default: 0.02)
- `--max-rows <int>` — Max vector features per layer (default: 10)
- `--zarr-only` — Write only the synthetic Zarr ERA5-like cube (needs no source bundle; see below)
- `--log <path>` — Write output to log file
- `--help` — Show help

## Zarr ERA5-like cube

Besides the clipped datasets, the bundle holds a small synthetic weather cube, `global/era5-weather/`: `era5_like_v3.zarr` (Zarr v3, sharded) and `era5_like_v2.zarr` (Zarr v2). Two days of hourly `t2m`, `u10`, `v10` and `msl` on a 1 degree global grid, about 7 MB for the pair. It is made from code (`databricks.labs.gbx.sample.zarr_era5`) with a fixed seed, so a run in one environment writes the same bytes; it needs no source bundle, so `--zarr-only` writes just the cube. A normal run writes it after the clipped datasets. Needs `xarray` and `zarr` in the container.

## Requires

- Full bundle at default source (run `gbx:data:download` first) or set `--source`
- Docker container `geobrix-dev` (script runs inside container)
- geopandas, GDAL (available in container)

## Path token for doc tests

Use `sample-data/Volumes/main/default/test-data` in place of `/Volumes/main/default/geobrix_samples` so examples work with the minimal bundle.
