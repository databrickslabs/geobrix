# Run Python Unit Test Coverage Analysis

Runs Python unit tests with pytest-cov code coverage analysis.

## Usage

```bash
bash scripts/commands/gbx-coverage-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run coverage for specific test file or directory
- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first: a `flock` on `/tmp/gbx-test.lock` inside the container, shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, and `gbx:docker:exec` with an `mvn` or `pytest` command), so two runs never share `target/`.

- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- The lock is released when the command exits, and also when it is killed. Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Run coverage analysis for all Python unit tests
bash scripts/commands/gbx-coverage-python.sh

# Require minimum 80% coverage
bash scripts/commands/gbx-coverage-python.sh --min-coverage 80

# Generate report and open in browser
bash scripts/commands/gbx-coverage-python.sh --open

# Run coverage for specific test directory
bash scripts/commands/gbx-coverage-python.sh --path python/geobrix/test/rasterx --open
```

## Coverage Report Location

- **HTML Report**: `python/coverage-report/index.html`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses pytest-cov plugin for Python code coverage
- **Test source**: `python/geobrix/test/` (non-documentation tests)
- **Coverage measured on**: `python/geobrix/src/databricks/labs/gbx/` (actual source code)
- Generates both HTML and terminal reports
- Default log location: `test-logs/` (if filename only provided)
