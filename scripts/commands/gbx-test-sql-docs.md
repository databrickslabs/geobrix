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
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--timeout <seconds>` – Per-test deadline (default 600). A test still running after this many seconds fails with a `TimeoutError` (stacks are dumped first) instead of blocking the run; if the JVM stops responding the run ends with the stacks on the terminal. Sets `GBX_DOC_TEST_TIMEOUT_S` in the container. A test marked `@pytest.mark.deadline(n)` keeps its own limit.
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default).
- `--help` – Help and examples.

**Sample data (default):** Like `gbx:test:python-docs`, this command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` in the container for the minimal bundle (required for remote/CI). Use `--no-sample-data-root` to leave it unset.

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

## Test layout

- **Source:** `docs/tests/python/api/` (SQL API Reference and Python API examples).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless an absolute path is given.
