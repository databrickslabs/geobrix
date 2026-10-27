# Run Python Documentation Test Coverage Analysis

Runs Python documentation tests with pytest-cov code coverage analysis.

## Usage

```bash
bash scripts/commands/gbx-coverage-python-docs.sh [OPTIONS]
```

## Options

- `--path <path>` - Run coverage for specific test file or directory (relative to docs/tests/python/)
- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command).

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Run coverage analysis for all Python documentation tests
bash scripts/commands/gbx-coverage-python-docs.sh

# Generate report and open in browser
bash scripts/commands/gbx-coverage-python-docs.sh --open

# Require minimum 70% coverage
bash scripts/commands/gbx-coverage-python-docs.sh --min-coverage 70 --open

# Run coverage for specific test directory
bash scripts/commands/gbx-coverage-python-docs.sh --path api --open
```

## Coverage Report Location

- **HTML Report**: `docs/tests/coverage-report/index.html`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses pytest-cov plugin for Python code coverage
- **Test source**: `docs/tests/python/`
- **Coverage measured on**: `python/geobrix/src/databricks/labs/gbx/` (actual source code)
- Excludes integration tests by default
- Generates both HTML and terminal reports
- Default log location: `test-logs/` (if filename only provided)
- Validates that documentation code examples are properly tested
