# Run Scala Documentation Tests

Runs Scala documentation tests from `docs/tests/scala/` using Maven.

## Usage

```bash
bash scripts/commands/gbx-test-scala-docs.sh [OPTIONS]
```

## Options

- `--suite <pattern>` – Maven suite pattern (default: `tests.docs.scala.*`). Example: `docs.tests.scala.api.*`
- `--log <path>` – Log file (filename → `test-logs/<name>`, absolute path as-is)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--skip-build` – Skip Maven compile before test (optional; `mvn test` still compiles)
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use env or full-bundle default in Scala)
- `--help` – Display help message

**Sample data (default):** The command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` in the container so doc tests use the minimal bundle (required for remote/CI). Use `--no-sample-data-root` to leave it unset so Scala uses the full-bundle default path.

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
# Run all Scala documentation tests
bash scripts/commands/gbx-test-scala-docs.sh

# Run specific docs test suite
bash scripts/commands/gbx-test-scala-docs.sh --suite tests.docs.scala.packages.*

# Run with logging
bash scripts/commands/gbx-test-scala-docs.sh --log scala-docs-tests.log

# Run specific suite (e.g. API examples only)
bash scripts/commands/gbx-test-scala-docs.sh --suite 'docs.tests.scala.api.*'

# Skip pre-build (mvn test still compiles)
bash scripts/commands/gbx-test-scala-docs.sh --skip-build
```

## Test Location

- **Source**: `docs/tests/scala/`
- **Pattern**: `tests.docs.scala.*`

## Notes

- Runs inside Docker container `geobrix-dev`
- Tests validate Scala code examples in documentation
- Ensures documentation examples compile and execute correctly
- Default log location: `test-logs/` (if filename only provided)
