# gbx:test:pyrx

Run light-tier Python tests (JAR-free: pyrx by default, but any light dir, `zarr` included) natively in the host venv `.venv-pyrx`. Build or refresh the venv with `gbx:venv:sync`. To run every light dir, use `gbx:test:light`.

The host run is the default because the dev container is x86_64 under emulation on an Apple-silicon Mac: the same 131 tests of `test/zarr/test_manifest.py` take about 134 s on the host and 1,410 s in the container. Use `--docker` only for work that needs the container: GDAL natives, the JAR, and parity.

The host run sets `TZ=UTC` (as the container and the CI runners are), and points `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` at the venv python. Without `TZ=UTC`, timestamp tests such as `test/zarr/test_read.py` fail in a US time zone.

## Usage

`bash scripts/commands/gbx-test-pyrx.sh [OPTIONS]`

## Options

- `--path <dir>` — test file or directory; repeatable. Any light dir listed in `_LIGHT_TEST_DIRS` (`python/geobrix/test/conftest.py`), e.g. `python/geobrix/test/zarr/test_manifest.py` (default: `python/geobrix/test/pyrx/`)
- `--docker` — run in the `geobrix-dev` container instead of the host venv (takes the container lock; see below)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help`, `-h` — show usage

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
- `bash scripts/commands/gbx-test-pyrx.sh --docker --path python/geobrix/test/pyrx/test_functions_spark.py` (container)
