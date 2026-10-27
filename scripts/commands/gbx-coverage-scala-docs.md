# Run Scala Documentation Test Coverage Analysis

Runs Scala documentation tests with scoverage code coverage analysis.

## Usage

```bash
bash scripts/commands/gbx-coverage-scala-docs.sh [OPTIONS]
```

## Options

- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--report-only` - Skip tests and only generate report from existing data
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

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
# Run documentation test coverage analysis
bash scripts/commands/gbx-coverage-scala-docs.sh

# Require minimum 75% coverage
bash scripts/commands/gbx-coverage-scala-docs.sh --min-coverage 75

# Generate report and open in browser
bash scripts/commands/gbx-coverage-scala-docs.sh --open

# Generate report only (no test execution)
bash scripts/commands/gbx-coverage-scala-docs.sh --report-only --open
```

## Coverage Report Location

- **HTML Report**: `target/scoverage-docs-report/index.html`
- **XML Report**: `target/scoverage-docs-report/scoverage.xml`

## What This Measures

- **Test source**: `docs/tests/scala/` (package: `tests.docs.scala.*`)
- **Coverage measured on**: `src/main/scala/com/databricks/labs/gbx/` (actual source code)

This command shows which parts of the GeoBrix source code are exercised by documentation examples, separate from unit test coverage.

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses Maven scoverage plugin for Scala code coverage
- Report goes to separate directory to distinguish from unit test coverage
- Documentation tests validate that code examples in docs compile and execute
- Default log location: `test-logs/` (if filename only provided)

## Comparison with Unit Test Coverage

| Command | Tests Run | Report Location | Purpose |
|---------|-----------|----------------|---------|
| `gbx:coverage:scala` | Unit tests (`src/test/scala/`) | `target/scoverage-report/` | Main coverage |
| `gbx:coverage:scala-docs` | Doc tests (`docs/tests/scala/`) | `target/scoverage-docs-report/` | Doc validation |

Both measure coverage of the same source code (`src/main/scala/`), but run different test suites.
