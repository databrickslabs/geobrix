# gbx:venv:sync

Create or refresh the isolated `uv` venv (`.venv-pyrx/`, no system site-packages) for the light-tier tests (`gbx:test:light`, `gbx:test:pyrx`, zarr included) and the light benchmarks. It installs what the light CI phase installs: the hash-pinned lock `python/geobrix/requirements-light-env6-ci.txt` (read-only; every test dependency at CI's exact version, so test results match CI), then `python/geobrix` itself (editable, `--no-deps`). An extras install is not used: it floats, an older venv keeps stale versions (h3, pyogrio, numpy) that change test results, and the `test` extra's `mapbox-vector-tile<2.2` pin cannot be installed with `light_env6`. The command then checks that the venv sees its own rasterio, zarr and pyspark (4.0.x, which the Delta test jars that `test/zarr` loads need). Re-running refreshes an existing venv in place.

Set `GBX_PYRX_VENV=<dir>` to put the venv somewhere other than `.venv-pyrx`, for example to try a new lock without disturbing a venv another test run is using. The test commands honour the same variable.

**Usage:** `bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--log <path>]`

**Options:**
- `--python <ver>` — Python version for the venv (default 3.12)
- `--recreate` — delete and recreate the venv
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-venv-sync.sh`
- `bash scripts/commands/gbx-venv-sync.sh --recreate --log venv-sync.log`
