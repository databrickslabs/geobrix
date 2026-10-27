# Run Scala Code Coverage Analysis

Runs Scala unit tests with scoverage code coverage analysis using Maven.

## Usage

```bash
bash scripts/commands/gbx-coverage-scala.sh [OPTIONS]
```

## Options

- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--report-only` - Skip tests and only generate report from existing data
- `--clean` - Run `mvn clean` before coverage (default: incremental, no clean, for speed)
- `--parallel` - Run tests in parallel (`scoverage:test -T 1C` then `report-only`; faster on multi-core)
- `--by-package` - Run coverage per package in sequence, merge XMLs, then report (same package matrix as CI; no parallel in one container)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Speed tips (Docker)

- Maven runs with `MAVEN_OPTS=-Xmx4G -XX:+UseG1GC` for faster builds.
- Default flow: `scoverage:test -T 1C` (parallel, thread-safe) then `scoverage:report-only` with `aggregateOnly` (one report, no per-module HTMLs).
- Use `--clean` only when you need a full rebuild; default is incremental.
- `--parallel` uses the same 2-step flow (no extra behavior; tests already run with -T 1C).

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
# Run coverage analysis (incremental, no clean)
bash scripts/commands/gbx-coverage-scala.sh

# Parallel tests then report (faster on multi-core)
bash scripts/commands/gbx-coverage-scala.sh --parallel

# Full clean + coverage
bash scripts/commands/gbx-coverage-scala.sh --clean

# Generate report and open in browser
bash scripts/commands/gbx-coverage-scala.sh --open

# Generate report only (no test execution)
bash scripts/commands/gbx-coverage-scala.sh --report-only --open

# By package (sequence: rasterx, gridx, vectorx, ds, expressions, util; merge then report)
bash scripts/commands/gbx-coverage-scala.sh --by-package --open
```

## Coverage Report Location

- **HTML Report**: `target/scoverage-report/index.html` (or `target/site/scoverage/index.html`)
- **XML Report**: `target/scoverage.xml`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses Maven scoverage plugin for Scala code coverage
- Generates both HTML and XML reports
- Default log location: `test-logs/` (if filename only provided)
- Coverage data persists between runs; default is incremental (use `--clean` to force full rebuild)
