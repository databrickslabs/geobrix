# Run SQL Documentation Tests

Runs documentation tests for the **SQL API Reference** and related Python API examples from `docs/tests/python/api/` (e.g. `test_sql_api.py`, `test_python_api.py`). Uses the same Docker and options as `gbx:test:python-docs`, scoped to the API suite.

---

## Usage

```bash
bash scripts/commands/gbx-test-sql-docs.sh [OPTIONS]
```

## Options

**Targeting**

- `--test <nodeid>` – Single test (e.g. `api/test_sql_api.py::test_register_and_show_functions`).
- `--path <path>` – File or directory relative to `docs/tests/python/` (default: `api/`).

**Common**

- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--timeout <seconds>` – Per-test deadline (default 600). A test still running after this many seconds fails with a `TimeoutError` (stacks are dumped first) instead of blocking the run; if the JVM stops responding the run ends with the stacks on the terminal. Sets `GBX_DOC_TEST_TIMEOUT_S` in the container. A test marked `@pytest.mark.deadline(n)` keeps its own limit.
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default).
- `--help` – Help and examples.

**Sample data (default):** Like `gbx:test:python-docs`, this command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` in the container for the minimal bundle (required for remote/CI). Use `--no-sample-data-root` to leave it unset.

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
# API/SQL doc tests only, skip build (uses in-repo minimal bundle)
bash scripts/commands/gbx-test-sql-docs.sh --skip-build

# Single test file with log
bash scripts/commands/gbx-test-sql-docs.sh --path api/test_sql_api.py --skip-build --log sql-docs.log

# A slow run: allow each test up to 15 minutes before it is failed as stuck
bash scripts/commands/gbx-test-sql-docs.sh --skip-build --timeout 900

# Full run (build + api tests)
bash scripts/commands/gbx-test-sql-docs.sh
```

## Before pytest: the Zarr doc tests

`api/` holds the Zarr function reference's doc tests (`api/test_zarr_functions_examples.py`), which keep a store's manifest in a Delta table. When the target covers one (the default `api/`, the whole suite, or any path or `--test` that names `zarr`) the command first installs the `zarr_dbr19` extra into the container when its pins are not met, and runs `scripts/ci/fetch-delta-test-jars.sh` under the work lock (a no-op when `scripts/docker/m2` already holds the delta-spark jars; a sha256 mismatch, exit 3, aborts; a missing jar is a warning and the Zarr tests then fail naming it). It is the same step, from the same helper (`ensure_zarr_test_env` in `common.sh`), that `gbx:test:python-docs` and `gbx:test:python` run. A run of other `api/` files pays nothing.

## Test layout

- **Source:** `docs/tests/python/api/` (SQL API Reference and Python API examples).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless an absolute path is given.
