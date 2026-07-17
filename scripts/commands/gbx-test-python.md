# Run Python Unit Tests

Runs Python unit tests (non-documentation tests) using pytest.

## Usage

```bash
bash scripts/commands/gbx-test-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run specific test file or directory
- `--host` - Run on the host (arca), not the Docker container. Requires `source ~/.local/geobrix-gdal-env.sh` first (provisioned by the `geobrix-arca` plugin) and a host-built JAR. Mirrors CI's two-environment split — see "Host mode" below.
- `--rebuild-venv` - (with `--host`) force-rebuild the host test venvs.
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--with-integration` - Include `@pytest.mark.integration` tests (network downloads, slow). Excluded by default.
- `--markers <expr>` - Override marker filter with a custom pytest expression (e.g. `"not slow"`). Disables the default `not integration` filter.
- `--help` - Display help message

## Host mode (arca, no Docker)

With `--host` the command runs directly on the host instead of `docker exec geobrix-dev`. There is no build step — the tests run against the already-built assembly JAR. Prerequisites: `source ~/.local/geobrix-gdal-env.sh` (native GDAL + Java 17 + PYTHONPATH) and `uv` on PATH, with `PIP_INDEX_URL` pointing at the internal pip proxy.

**Two-environment split (mirrors CI).** CI never runs all Python tests in one environment, and neither does `--host` — running everything in one bloated venv causes cross-suite SparkSession/fixture errors. Instead a default `--host` run does two legs, each in its own uv venv built from the corresponding CI lock:

- **Heavy leg** — `.venv-host-ci` from `requirements-ci.txt` (~27 pkgs: pyspark, py4j, numpy, pytest; no rasterio/pandas). Runs `test/` minus the light dirs. Matches CI's `python_build` job.
- **Light leg** — `.venv-host-pyrx` from `requirements-pyrx-ci.txt` (~104 pkgs: rasterio, shapely, pandas, pyarrow, h3, mapbox-vector-tile, vizx). Runs `test/{pyrx,pyvx,pygx,pmtiles_light,stac,vizx,ds,sample}`. Matches CI's `pyrx_build` job.

A single `--path` is routed to whichever venv fits (light dirs → pyrx venv, else ci venv). `osgeo` comes from the sourced arca env's PYTHONPATH (equivalent to CI's apt `python3-gdal`); neither lock contains `pdal` (source-only, unbuildable on arca — and not needed by these suites). See the `geobrix-arca` plugin for the full setup.

## Default marker filter

By default the script runs with `-m "not integration"`, matching CI's `python_build` action. This excludes `python/geobrix/test/sample/test_sample_bundle.py::test_run_*_bundle_returns_dict_shape`, which download hundreds of MB of sample data.

Opt in with `--with-integration` (drops the filter entirely) or `--markers <expr>` (replaces it with your own expression).

## Examples

```bash
# Unit tests only (default — fast, matches CI)
bash scripts/commands/gbx-test-python.sh

# On the arca host (no Docker) — source the GDAL env first
source ~/.local/geobrix-gdal-env.sh
bash scripts/commands/gbx-test-python.sh --host

# Include integration tests (network downloads)
bash scripts/commands/gbx-test-python.sh --with-integration

# Run specific test file
bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_operations.py

# Run with logging
bash scripts/commands/gbx-test-python.sh --log python-tests.log

# Custom marker expression (overrides the default)
bash scripts/commands/gbx-test-python.sh --markers "not slow"
```

## Test Location

- **Source**: `python/geobrix/test/`

## Notes

- Runs inside Docker container `geobrix-dev` by default; `--host` runs on the arca host (see "Host mode").
- Excludes documentation tests (use `gbx-test-python-docs` for those)
- Uses pytest with verbose output
- Default log location: `test-logs/` (if filename only provided)
