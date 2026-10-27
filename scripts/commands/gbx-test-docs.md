# Run All Documentation Tests

Runs **all** documentation tests by invoking **gbx-test-python-docs**, **gbx-test-sql-docs**, and **gbx-test-scala-docs** in sequence. Pass-through options (e.g. `--skip-build`, `--no-sample-data-root`) are forwarded so `GBX_SAMPLE_DATA_ROOT` and build behavior are consistent. Use for pre-commit or CI-style validation.

---

## Usage

```bash
bash scripts/commands/gbx-test-docs.sh [OPTIONS]
```

## Options

**Targeting (Python only)**

- `--suite <name>` – Python subset: `quickstart` | `api` | `readers` | `rasterx` | `advanced` | `setup`.
- `--path <path>` – Directory or file relative to `docs/tests/python/`.
- `--test <nodeid>` – Single Python test node id.

**Common**

- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--markers <markers>` – Pytest markers for Python (e.g. `"not slow"`).
- `--include-integration` – Include Python integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build before Python tests.
- `--scala-suite <pattern>` – Scala test suite pattern (default: `tests.docs.scala.*`).
- `--python-only` – Run only Python doc tests (skip Scala).
- `--scala-only` – Run only Scala doc tests (skip Python).
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default; e.g. full bundle).
- `--help` – Help and examples.

**Sample data (default):** The command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` inside the container so doc tests use the minimal bundle (host path `sample-data/Volumes/main/default/test-data`). This is required for running docs unit tests on remote/CI. Use `--no-sample-data-root` to leave it unset (e.g. to use a full bundle or your own env).

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command). It holds the lock for the whole run; the doc suites it runs skip taking it.

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Full run with build
bash scripts/commands/gbx-test-docs.sh

# Fast run (skip build), with log. Uses in-repo minimal bundle; no download.
bash scripts/commands/gbx-test-docs.sh --skip-build --log docs.log

# Python doc tests only (e.g. API suite)
bash scripts/commands/gbx-test-docs.sh --python-only --suite api --skip-build

# Scala doc tests only
bash scripts/commands/gbx-test-docs.sh --scala-only --log scala-docs.log

# Custom Scala suite
bash scripts/commands/gbx-test-docs.sh --scala-only --scala-suite 'docs.tests.scala.api.*'
```

## Order and scope

1. **Python** – `gbx-test-python-docs` (default: all of `docs/tests/python/` except `api/`; use `--suite` / `--path` / `--test` for subsets).
2. **SQL/API** – `gbx-test-sql-docs` (docs/tests/python/api/).
3. **Scala** – `gbx-test-scala-docs` (Maven suite default `tests.docs.scala.*`).

If any phase fails, the command exits with a non-zero code. With `--log`, the log file is truncated at the start of the run and all output is written to it.
