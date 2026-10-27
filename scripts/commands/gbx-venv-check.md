# gbx:venv:check

Compare the host venv (`.venv-pyrx`, or `GBX_PYRX_VENV`) with the running `geobrix-dev` container and print what differs. Exits 1 on any drift.

A host that drifts from the container proves nothing about it: a test that passes on the host may fail there. Run this at the start of a host (no-Docker) task. On a drift, fix the venv (`gbx:venv:sync --match-container --recreate`), not the test.

**What is compared**

- the Python patch version (for example 3.12.3 against 3.12.11);
- the version of every package both environments have (read with `importlib.metadata`; `pip`, `setuptools` and `wheel` are ignored: they depend on how an environment was seeded, not on what the tests import);
- the natives: GDAL (`rasterio.__gdal_version__`), PROJ (`pyproj.proj_version_str`) and GEOS (`shapely.geos_version_string`).

A package only one side has is not drift, because it cannot be compared; the counts are printed. The Spark Connect client packages that `gbx:venv:sync --connect` installs (grpcio, grpcio-status, googleapis-common-protos; recorded in `<venv>/.gbx-connect-extras`) are listed as *declared connect-tier extras* when the container lacks them. If the container does carry one, its version is compared like any other.

The container side is read with `docker exec` running a stdlib-only script (`scripts/ci/venv_env_report.py`) that the host interpreter also runs, so both sides report the same way. Nothing here runs Maven or a test JVM, so it takes no container lock.

**Usage:** `bash scripts/commands/gbx-venv-check.sh [--log <path>]`

**Options:**
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-venv-check.sh`
- `GBX_PYRX_VENV=/tmp/other-venv bash scripts/commands/gbx-venv-check.sh`
