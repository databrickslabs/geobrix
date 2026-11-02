# gbx:venv:check

Compare the host venv (`.venv-pyrx`, or `GBX_PYRX_VENV`) with the running `geobrix-dev` container and print what differs. It exits 3 on any drift, and 1 when the venv or the container cannot be read. It exits 0 with a WARN when only the expected payload-stack divergence remains.

A host that drifts from the container proves nothing about it: a test that passes on the host may fail there. Run this at the start of a host (no-Docker) task. On a drift, fix the venv (`gbx:venv:sync --match-container --recreate`), not the test. The failure prints that command on a `Fix:` line.

**What is compared**

- the Python patch version (for example 3.12.3 against 3.12.11);
- the version of every package both environments have (read with `importlib.metadata`; `pip`, `setuptools` and `wheel` are ignored: they depend on how an environment was seeded, not on what the tests import);
- the natives: GDAL (`rasterio.__gdal_version__`), PROJ (`pyproj.proj_version_str`) and GEOS (`shapely.geos_version_string`). A native one side cannot reach (the module is missing or broken) while the other can is drift, not a pass.

A package only one side has is not drift, because it cannot be compared; the counts are printed. The Spark Connect client packages that `gbx:venv:sync --connect` installs (grpcio, grpcio-status, googleapis-common-protos and zstandard; recorded in `<venv>/.gbx-connect-extras`) are listed as *declared connect-tier extras* when the container lacks them. If the container does carry one, its version is compared like any other.

**Expected divergence: the payload GDAL stack**

- The container carries the env wheel's payload stack: gdal 3.13.3, geos 3.15.0, proj 9.9.0 and rasterio `1.5.2+gbx1` (a local build).
- The host uses PyPI wheels, and a local version is on no index, so the host cannot match it.
- The rows apply when the container's rasterio has a local version (for example 1.5.2+gbx1).
  - A gdal, geos or proj difference is `EXPECTED`, not `DRIFT`.
  - rasterio is `EXPECTED` only when the host has exactly the public version (1.5.2). Any other host version (1.5.0) is `DRIFT`. The check passes and prints a WARN naming the expected rows.
- Any other difference is still `DRIFT` and fails the check. So is a native the host cannot reach, and a local version on any other package.
- With no local-version rasterio in the container, a GDAL, PROJ or GEOS difference is `DRIFT`.
- The WARN states the rule. A host (T0) run is valid only for a test that does not depend on GDAL 3.13 or PROJ 9.9 behaviour. Run such a test in the container.

The container side is read with `docker exec` running a stdlib-only script (`scripts/ci/venv_env_report.py`) that the host interpreter also runs, so both sides report the same way. Nothing here runs Maven or a test JVM, so it takes no container lock.

**Exit codes:** `0` match (with a WARN when only the expected payload-stack divergence remains), `1` the venv or the container could not be read, `3` drift.

**Usage:** `bash scripts/commands/gbx-venv-check.sh [--venv <dir>] [--log <path>]`

**Options:**
- `--venv <dir>` — check `<dir>` instead of `.venv-pyrx` (same as `GBX_PYRX_VENV`)
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-venv-check.sh`
- `GBX_PYRX_VENV=/tmp/other-venv bash scripts/commands/gbx-venv-check.sh`
