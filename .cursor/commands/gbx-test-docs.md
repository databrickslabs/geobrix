# Run All Documentation Tests

Runs **all** documentation tests by invoking **gbx-test-python-docs**, **gbx-test-sql-docs**, and **gbx-test-scala-docs** in sequence. Pass-through options (e.g. `--skip-build`, `--no-sample-data-root`) are forwarded so `GBX_SAMPLE_DATA_ROOT` and build behavior are consistent. Use for pre-commit or CI-style validation.

---

## Usage

```bash
bash .cursor/commands/gbx-test-docs.sh [OPTIONS]
```

## Options

**Targeting (Python only)**

- `--suite <name>` – Python subset: `quickstart` | `api` | `readers` | `rasterx` | `advanced` | `setup`.
- `--path <path>` – Directory or file relative to `docs/tests/python/`.
- `--test <nodeid>` – Single Python test node id.

**Common**

- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--markers <markers>` – Pytest markers for Python (e.g. `"not slow"`).
- `--include-integration` – Include Python integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build before Python tests.
- `--scala-suite <pattern>` – Scala test suite pattern (default: `tests.docs.scala.*`).
- `--python-only` – Run only Python doc tests (skip Scala).
- `--scala-only` – Run only Scala doc tests (skip Python).
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default; e.g. full bundle).
- `--help` – Help and examples.

**Sample data (default):** The command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` inside the container so doc tests use the minimal bundle (host path `sample-data/Volumes/main/default/test-data`). This is required for running docs unit tests on remote/CI. Use `--no-sample-data-root` to leave it unset (e.g. to use a full bundle or your own env).

## Examples

```bash
# Full run with build
bash .cursor/commands/gbx-test-docs.sh

# Fast run (skip build), with log. Uses in-repo minimal bundle; no download.
bash .cursor/commands/gbx-test-docs.sh --skip-build --log docs.log

# Python doc tests only (e.g. API suite)
bash .cursor/commands/gbx-test-docs.sh --python-only --suite api --skip-build

# Scala doc tests only
bash .cursor/commands/gbx-test-docs.sh --scala-only --log scala-docs.log

# Custom Scala suite
bash .cursor/commands/gbx-test-docs.sh --scala-only --scala-suite 'docs.tests.scala.api.*'
```

## Order and scope

1. **Python** – `gbx-test-python-docs` (default: all of `docs/tests/python/` except `api/`; use `--suite` / `--path` / `--test` for subsets).
2. **SQL/API** – `gbx-test-sql-docs` (docs/tests/python/api/).
3. **Scala** – `gbx-test-scala-docs` (Maven suite default `tests.docs.scala.*`).

If any phase fails, the command exits with a non-zero code. With `--log`, the log file is truncated at the start of the run and all output is written to it.
