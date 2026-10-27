# Run Scala Unit Tests

Runs Scala unit tests (non-documentation tests) using Maven.

## Usage

```bash
bash scripts/commands/gbx-test-scala.sh [OPTIONS]
```

## Options

- `--by-package` - Run tests per package in sequence (rasterx, gridx, vectorx, ds, expressions, util); reports pass/fail per package (same packages as CI matrix)
- `--suite <pattern>` - Run specific test suite (single pattern)
- `--suites <list>` - Run specific test suites (comma-separated class/package patterns; same as `-Dsuites=`)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--verbose` - Show detailed Maven output
- `--help` - Display help message

## Locale

The container has no `LANG`/`LC_ALL`, which would leave the JVM's file-name encoding at ASCII and cancel the tests that create non-ASCII file names (for example the non-ASCII case in `NodeFilePathUtilTest`). The command passes `-e LC_ALL=C.UTF-8` to the container, so those tests run.

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
# Run all Scala unit tests
bash scripts/commands/gbx-test-scala.sh

# Run specific test suite
bash scripts/commands/gbx-test-scala.sh --suite com.databricks.labs.gbx.rasterx.expressions.*

# Run multiple suites (comma-separated)
bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.rasterx.operations.SpatialRefOpsTest,com.databricks.labs.gbx.rasterx.ds.GTiff_DataSourceTest'

# Run with logging
bash scripts/commands/gbx-test-scala.sh --log scala-tests.log

# Run with verbose output
bash scripts/commands/gbx-test-scala.sh --verbose

# Run by package (sequence; same packages as CI "Scala tests (by package)" workflow)
bash scripts/commands/gbx-test-scala.sh --by-package
```

## Test Location

- **Source**: `src/test/scala/com/databricks/labs/gbx/` (excludes `tests.docs.scala.*`)

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-scala-docs` for those)
- Uses Maven profile to skip scoverage for faster execution
- Default log location: `test-logs/` (if filename only provided)
