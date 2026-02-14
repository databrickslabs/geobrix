# Run SQL Documentation Tests

Runs documentation tests for the **SQL API Reference** and related Python API examples from `docs/tests/python/api/` (e.g. `test_sql_api.py`, `test_python_api.py`). Uses the same Docker and options as `gbx:test:python-docs`, scoped to the API suite.

---

## Usage

```bash
bash .cursor/commands/gbx-test-sql-docs.sh [OPTIONS]
```

## Options

**Targeting**

- `--test <nodeid>` – Single test (e.g. `api/test_sql_api.py::test_register_and_show_functions`).
- `--path <path>` – File or directory relative to `docs/tests/python/` (default: `api/`).

**Common**

- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--skip-download` – Skip sample-data download.
- `--data-bundle <type>` – `essential` | `complete` (default) | `both`.
- `--help` – Help and examples.

## Examples

```bash
# API/SQL doc tests only, skip build and download
bash .cursor/commands/gbx-test-sql-docs.sh --skip-build --skip-download

# Single test file with log
bash .cursor/commands/gbx-test-sql-docs.sh --path api/test_sql_api.py --skip-build --skip-download --log sql-docs.log

# Full run (build + download + api tests)
bash .cursor/commands/gbx-test-sql-docs.sh
```

## Test layout

- **Source:** `docs/tests/python/api/` (SQL API Reference and Python API examples).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless an absolute path is given.
