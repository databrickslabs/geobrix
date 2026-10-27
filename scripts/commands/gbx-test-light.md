# gbx:test:light

Run the light-tier Python tests natively on the host, in `.venv-pyrx`: pyrx, pyvx, pygx, ds, zarr, and the rest of `_LIGHT_TEST_DIRS` (`python/geobrix/test/conftest.py`). A thin front for `gbx:test:pyrx`, whose default is only `test/pyrx/`: with no `--path` this runs every light dir in one pytest process, in the order of the light CI phase's `pytest` line (`.github/actions/pyrx_build/action.yml`). The order is deliberate: the dirs share a process, and `test/pyvx` registers the PROJ grids that `test/bench` relies on when it builds rasters on worker threads. A wiring test pins the command's order to that line.

Build or refresh the venv first with `gbx:venv:sync` (it installs the Zarr extra and the Spark line the Delta test jars need). The host is the default because the dev container is x86_64 under emulation on an Apple-silicon Mac, about 10x slower for these tests; use `--docker` only for GDAL natives, the JAR and parity. The host run sets `TZ=UTC` like the container and CI do (see `gbx:test:pyrx`).

## Usage

`bash scripts/commands/gbx-test-light.sh [OPTIONS]`

## Options

- `--path <dir>` — test file or directory; repeatable (default: every light dir, in the light CI phase order)
- `--docker` — run in the `geobrix-dev` container instead of the host venv (takes the container lock; see `gbx:test:pyrx`)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--no-wait` — with `--docker`, fail at once (exit 75) if another test run holds the container lock (see `gbx:test:pyrx`)
- `--help`, `-h` — show usage

A whole-tier run is for the end of a batch, not a task: pin `--path` to the files a change touches while iterating.

Baseline for a whole-tier run: about 28 minutes on an Apple-silicon host (1,656 s: 6,296 passed, 56 skipped, 505 deselected), with another Spark JVM running beside it.

## Examples

- `bash scripts/commands/gbx-test-light.sh --path python/geobrix/test/zarr/test_manifest.py`
- `bash scripts/commands/gbx-test-light.sh --log light.log`
