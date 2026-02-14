# Run All Documentation Tests

Runs **all** documentation examples: **Python** (including SQL API and readers), then **Scala**. Uses the same Docker container and common options as `gbx:test:python-docs` and `gbx:test:scala-docs`. Use for pre-commit or CI-style validation.

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
- `--skip-download` – Skip sample-data download.
- `--data-bundle <type>` – `essential` | `complete` (default) | `both`.
- `--scala-suite <pattern>` – Scala test suite pattern (default: `tests.docs.scala.*`).
- `--python-only` – Run only Python doc tests (skip Scala).
- `--scala-only` – Run only Scala doc tests (skip Python).
- `--help` – Help and examples.

## Examples

```bash
# Full run with build and download
bash .cursor/commands/gbx-test-docs.sh

# Fast run (skip build and download), with log
bash .cursor/commands/gbx-test-docs.sh --skip-build --skip-download --log docs.log

# Python doc tests only (e.g. API suite)
bash .cursor/commands/gbx-test-docs.sh --python-only --suite api --skip-build --skip-download

# Scala doc tests only
bash .cursor/commands/gbx-test-docs.sh --scala-only --log scala-docs.log

# Custom Scala suite
bash .cursor/commands/gbx-test-docs.sh --scala-only --scala-suite 'docs.tests.scala.api.*'
```

## Order and scope

1. **Python** – `docs/tests/python/` (or subset via `--suite` / `--path` / `--test`). Includes SQL API and Python API examples.
2. **Scala** – `docs/tests/scala/` via Maven suite (default `tests.docs.scala.*`).

If either phase fails, the command exits with a non-zero code. With `--log`, all output is appended to the given file.
