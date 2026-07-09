# Run Function-Info Tests

Re-inventories `function-info.json` (with placeholders for full coverage) and runs the function-info test suite inside the **geobrix-dev** Docker container.

## Usage

```bash
bash scripts/commands/gbx-test-function-info.sh [OPTIONS]
```

## Options

- `--skip-generate` - Skip the generator; run only pytest in `docs/tests-function-info/`
- `--host` - Run on the host (arca), not the Docker container. Requires `source ~/.local/geobrix-gdal-env.sh` first (provisioned by the `geobrix-arca` plugin); builds/reuses `.venv-host-pyrx` from the pinned CI lock and runs against a host-built JAR. See "Host mode" below.
- `--rebuild-venv` - (with `--host`) force-rebuild the host test venv.
- `--log <path>` - Write output to log file
- `--help` - Display help

## Host mode (arca, no Docker)

With `--host` the command runs directly on the host instead of `docker exec geobrix-dev`. Prerequisites: `source ~/.local/geobrix-gdal-env.sh` (native GDAL + Java 17 + PYTHONPATH) and `uv` on PATH, with `PIP_INDEX_URL` pointing at the internal pip proxy. The pytest registers functions via the built JAR; no sample data is needed. The first run builds a host test venv from the exact CI-pinned lock via `uv` — `.venv-host-pyrx` from `python/geobrix/requirements-pyrx-ci.txt` (the light-tier deps: rasterio/pandas/h3/vizx). Neither CI lock contains `pdal` (source-only, unbuildable on arca and not needed here). See the `geobrix-arca` plugin for the full setup.

## Default behavior (inside Docker)

1. **Generate**: `python3 docs/scripts/generate-function-info.py` in container  
   Builds function-info.json from doc SQL examples only (no empty usage). Fails if any registered function has no doc example; fix upstream in `docs/tests/python/api/*_functions_sql.py`.

2. **Test**: `pytest docs/tests-function-info/ -v -s` in container  
   - Prints DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED for each RasterX, GridX, and VectorX function (JAR and Spark available in container).
   - Asserts full coverage of function-info.json vs registered list.

## Examples

```bash
# Full run: generate then test
gbx:test:function-info

# On the arca host (no Docker) — source the GDAL env first
source ~/.local/geobrix-gdal-env.sh
gbx:test:function-info --host

# Only run tests (do not regenerate JSON)
gbx:test:function-info --skip-generate

# With log
gbx:test:function-info --log function-info-tests.log
```

## Notes

- **Runs inside Docker**: Requires `geobrix-dev` container (e.g. `./scripts/docker/start_docker_with_volumes.sh`). Uses `check_docker` like other GeoBrix test commands.
- DESCRIBE tests use the GeoBrix JAR and PySpark in the container; coverage tests use `registered_functions.txt` and do not require Spark.
