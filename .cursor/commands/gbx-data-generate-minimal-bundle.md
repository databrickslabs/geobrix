# Generate Minimal Doc-Test Bundle

Generates a minimal sample-data bundle under `sample-data/Volumes/main/default/test-data/geobrix-examples/` by extracting subsets from the full bundle using bounding boxes around NYC (default: center of Manhattan) and London (default: center of London). Vector data is limited by a configurable row count; raster data is clipped to the bbox (all pixels within bounds). Used so doc tests can pass in CI using the minimal bundle and path token `sample-data/Volumes/main/default/test-data`.

---

## Usage

```bash
bash .cursor/commands/gbx-data-generate-minimal-bundle.sh [OPTIONS]
```

## Options

- `--source <dir>` — Full bundle root (default: .../geobrix_samples/geobrix-examples)
- `--out <dir>` — Output root (default: .../test-data/geobrix-examples)
- `--nyc-lon`, `--nyc-lat` — NYC center (default: -73.9857, 40.7484, Manhattan)
- `--london-lon`, `--london-lat` — London center (default: -0.1276, 51.5074)
- `--bbox-size <float>` — Half-width/height of bbox in degrees (default: 0.02)
- `--max-rows <int>` — Max vector features per layer (default: 10)
- `--log <path>` — Write output to log file
- `--help` — Show help

## Requires

- Full bundle at default source (run `gbx:data:download` first) or set `--source`
- Docker container `geobrix-dev` (script runs inside container)
- geopandas, GDAL (available in container)

## Path token for doc tests

Use `sample-data/Volumes/main/default/test-data` in place of `/Volumes/main/default/geobrix_samples` so examples work with the minimal bundle.
