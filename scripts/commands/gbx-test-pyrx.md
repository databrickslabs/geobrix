# gbx:test:pyrx

Run light-tier Python tests (JAR-free: pyrx by default, but any light dir, `zarr` included) natively in the host venv `.venv-pyrx`. Build or refresh the venv with `gbx:venv:sync`. To run every light dir, use `gbx:test:light`.

The host run is the default: it needs no container, takes no lock and starts at once. Use `--docker` only for work that needs the container: GDAL natives, the JAR, and parity. (The dev container is a native arm64 image on Apple silicon, so it is not emulated there; it is still the slower, shared, locked route for light-tier tests.)

`GBX_PYRX_VENV=<dir>` runs on a venv other than `.venv-pyrx` (created with `gbx:venv:sync` under the same variable), for example to try a new lock without disturbing a venv another run is using.

The host run sets `TZ=UTC` (as the container and the CI runners are), and points `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` at the venv python. Without `TZ=UTC`, timestamp tests such as `test/zarr/test_read.py` fail in a US time zone. `GBX_TEST_TZ=<zone>` (for example `America/New_York`) runs under another zone on purpose, to check that a timestamp test does not depend on UTC; the zone your shell has is still ignored.

**Spark Connect tests run in their own process.** A test that uses the `spark_connect_local` fixture (a local Spark Connect session; `gbx:venv:sync --connect` installs its client deps) cannot share a pytest process with classic-session tests: the fixture skips while a classic `SparkContext` is live, a live Connect session turns `getOrCreate` into a Connect client, and a light module imported before the session exists binds its UDFs to the classic one. The test conftest marks every such test `connect`. When the selected paths contain any, the command runs `-m '<markers> and (not connect)'` over the paths first, then `-m '<markers> and (connect)'` over each file that uses the fixture, a pytest process per file. The exit code is a failure of any process (pytest's "nothing collected", 5, counts as a pass while another process ran). With no Connect deps a Connect process skips its tests with a one-line reason. Nothing to add by hand: no `-k` or `PYTEST_ADDOPTS` workaround to run them.

## Usage

`bash scripts/commands/gbx-test-pyrx.sh [OPTIONS]`

## Options

- `--path <dir>` — test file or directory; repeatable. Any light dir listed in `_LIGHT_TEST_DIRS` (`python/geobrix/test/conftest.py`), e.g. `python/geobrix/test/zarr/test_manifest.py` (default: `python/geobrix/test/pyrx/`)
- `--markers <expr>` — extra pytest marker expression, ANDed with the default `(not integration) and (not perf)`: `--markers 'not delta'` runs `-m '((not integration) and (not perf)) and (not delta)'`. Integration tests stay excluded
- `--with-integration` — also run the `integration`-marked tests, which need the network or a long setup (for example installing a package into a scratch venv); the default loop and CI exclude them. Pick one with `--path` and `--markers`, e.g. `--path python/geobrix/test/pyrx/test_rio_tiler_typing_floor.py --with-integration`
- `--perf` — also run the `perf`-marked cases: slow tessellate, geomk and streamed-merge tests, three of them correctness oracles that are only slow (about two minutes in all). The unit loop skips them by default; CI (which calls pytest directly with `-m 'not integration'`) still runs them, so pass `--perf` on a full run before a push. A `--path` file whose only tests are perf-marked selects nothing and exits 5; the command then says to add `--perf`
- `--docker` — run in the `geobrix-dev` container instead of the host venv (takes the container lock; see below)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help`, `-h` — show usage

## The native payload

Set `GBX_NATIVE_ROOT` in the calling shell to run the container's pytest on a native payload, for example `GBX_NATIVE_ROOT=build/env-wheel/env6/payload` (from `gbx:build:wheel --env 6`). The command passes it into the container as its `/root/geobrix` path (a path under this checkout, relative or absolute; any other absolute path is taken as a container path), so the run's Python serves the payload's bindings (its `osgeo` and rasterio on one GDAL). Unset, nothing is passed.

## Other pytest options

Set `PYTEST_ADDOPTS` in the calling shell: pytest reads it as extra arguments, and both the host and the `--docker` run pass it on. For example `PYTEST_ADDOPTS='--durations=20 --junitxml=test-logs/pyrx.xml' gbx:test:pyrx --path python/geobrix/test/zarr/test_manifest.py`. (`gbx:test:light` takes the same.)

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command). Only `--docker` runs take it: the default host-venv run shares no container state, so it never waits for the lock.

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

- `bash scripts/commands/gbx-test-pyrx.sh`
- `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_functions_spark.py --log pyrx.log`
- `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/zarr/test_manifest.py` (host; Delta jars from `scripts/docker/m2` via the test conftest)
- `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/zarr/ --markers 'not delta'` (skip the Delta-dependent tests)
- `bash scripts/commands/gbx-test-pyrx.sh --docker --path python/geobrix/test/pyrx/test_functions_spark.py` (container)
