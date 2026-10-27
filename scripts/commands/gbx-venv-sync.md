# gbx:venv:sync

Create or refresh the isolated `uv` venv (`.venv-pyrx/`, no system site-packages) for the light-tier tests (`gbx:test:light`, `gbx:test:pyrx`, zarr included) and the light benchmarks. It installs the `[light_env6,zarr_env6]` extras from `python/geobrix` plus `pytest` and `pyspark>=4.0,<4.1` (not the `test` extra, whose `mapbox-vector-tile<2.2` pin no `light_env6` install can satisfy; pyspark is capped because the Delta test jars that `test/zarr` loads run on Spark 4.0.x only), then checks that the venv sees its own rasterio, zarr and pyspark. Re-running refreshes an existing venv in place.

**Usage:** `bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--log <path>]`

**Options:**
- `--python <ver>` — Python version for the venv (default 3.12)
- `--recreate` — delete and recreate the venv
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-venv-sync.sh`
- `bash scripts/commands/gbx-venv-sync.sh --recreate --log venv-sync.log`
