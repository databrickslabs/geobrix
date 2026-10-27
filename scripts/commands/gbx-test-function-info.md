# Run Function-Info Tests

Re-inventories `function-info.json` (with placeholders for full coverage) and runs the function-info test suite inside the **geobrix-dev** Docker container.

## Usage

```bash
bash scripts/commands/gbx-test-function-info.sh [OPTIONS]
```

## Options

- `--skip-generate` - Skip the generator; run only pytest in `docs/tests-function-info/`
- `--log <path>` - Write output to log file
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help` - Display help

## Default behavior (inside Docker)

1. **Generate**: `python3 docs/scripts/generate-function-info.py` in container  
   Builds function-info.json from doc SQL examples only (no empty usage). Fails if any registered function has no doc example; fix upstream in `docs/tests/python/api/*_functions_sql.py`.

2. **Test**: `pytest docs/tests-function-info/ -v -s` in container  
   - Prints DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED for each RasterX, GridX, and VectorX function (JAR and Spark available in container).
   - Asserts full coverage of function-info.json vs registered list.

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command).

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven/pytest process tree itself. If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Full run: generate then test
gbx:test:function-info

# Only run tests (do not regenerate JSON)
gbx:test:function-info --skip-generate

# With log
gbx:test:function-info --log function-info-tests.log
```

## Notes

- **Runs inside Docker**: Requires `geobrix-dev` container (e.g. `./scripts/docker/start_docker_with_volumes.sh`). Uses `check_docker` like other GeoBrix test commands.
- DESCRIBE tests use the GeoBrix JAR and PySpark in the container; coverage tests use `registered_functions.txt` and do not require Spark.
