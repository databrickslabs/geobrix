# gbx:test:pyrx

Run the pyrx (lightweight, JAR-free raster API) test suite in the dev container.

## Usage

`bash scripts/commands/gbx-test-pyrx.sh [OPTIONS]`

## Options

- `--path <dir>` — specific test file or directory (default: `python/geobrix/test/pyrx/`)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help`, `-h` — show usage

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first: a `flock` on `/tmp/gbx-test.lock` inside the container, shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, and `gbx:docker:exec` with an `mvn` or `pytest` command), so two runs never share `target/`. Only `--docker` runs take it: the default host-venv run shares no container state.

- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- The lock is released when the command exits, and also when it is killed. Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

- `bash scripts/commands/gbx-test-pyrx.sh`
- `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_functions_spark.py --log pyrx.log`
