# gbx:test:light

Run the light-tier Python tests natively on the host, in `.venv-pyrx`: pyrx, pyvx, pygx, ds, zarr, and the rest of `_LIGHT_TEST_DIRS` (`python/geobrix/test/conftest.py`). A thin front for `gbx:test:pyrx`, whose default is only `test/pyrx/`: with no `--path` this runs every light dir in one pytest process, in the order of the unsharded `pytest` line of the light CI action (`.github/actions/pyrx_build/action.yml`). The order is deliberate: the dirs share a process, and `test/pyvx` registers the PROJ grids that `test/bench` relies on when it builds rasters on worker threads. A wiring test pins the command's order to that line.

CI does not run it as one process: it runs the same dirs as four parallel shards (`pyrx`, `ds`, `zarr` and `rest`, in the `pyrx` job matrix of `.github/workflows/build_main.yml`; `pyvx` and `bench` stay together in `rest`, in that order), and every shard runs on every push, `test/bench` included. This command runs the same dirs in one process; a CI guard (`scripts/ci/test_ci_light_shards.py`) keeps the shards equal to `_LIGHT_TEST_DIRS`.

Build or refresh the venv first with `gbx:venv:sync` (it installs the Zarr extra and the Spark line the Delta test jars need). The host is the default (no container, no lock); use `--docker` only for GDAL natives, the JAR and parity. The host run sets `TZ=UTC` like the container and CI do (see `gbx:test:pyrx`). `GBX_PYRX_VENV=<dir>` runs on another venv than `.venv-pyrx`.

## Usage

`bash scripts/commands/gbx-test-light.sh [OPTIONS]`

## Options

- `--path <dir>` — test file or directory; repeatable (default: every light dir, in one process, in the unsharded CI order)
- `--markers <expr>` — extra pytest marker expression, ANDed with `(not integration) and (not perf)` (see `gbx:test:pyrx`)
- `--perf` — also run the perf-marked cases, which the unit loop skips (see `gbx:test:pyrx`). `gbx:test:light` takes its dirs from the CI action, but without `--perf` it runs fewer tests than CI does: pass it on a full run before a push
- `--docker` — run in the `geobrix-dev` container instead of the host venv (takes the container lock; see `gbx:test:pyrx`)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--no-wait` — with `--docker`, fail at once (exit 75) if another test run holds the container lock (see `gbx:test:pyrx`)
- `--help`, `-h` — show usage

`PYTEST_ADDOPTS` in the calling shell reaches pytest (`--durations=20`, `--junitxml=...`); see `gbx:test:pyrx`.

A whole-tier run is for the end of a batch, not a task: pin `--path` to the files a change touches while iterating.

Baseline for a whole-tier run: about 28 minutes on an Apple-silicon host (1,656 s: 6,296 passed, 56 skipped, 505 deselected), with another Spark JVM running beside it.

## Examples

- `bash scripts/commands/gbx-test-light.sh --path python/geobrix/test/zarr/test_manifest.py`
- `bash scripts/commands/gbx-test-light.sh --log light.log`
- `bash scripts/commands/gbx-test-light.sh --path python/geobrix/test/zarr/ --markers 'not delta'`
