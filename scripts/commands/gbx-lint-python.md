# Run Python Lint (isort, black, flake8)

Runs **isort**, **black**, and **flake8** on two scopes, each with its own `pyproject.toml`:

- the Python package: `python/geobrix` (`src` and `test`)
- the Lakeflow Connect connector: `connectors/lakeflow` (`sources`)

Same tools, versions, and scopes as CI. Exits non-zero if either scope fails any tool; a per-scope PASS/FAIL table is printed for each.

## Usage

```bash
bash scripts/commands/gbx-lint-python.sh [OPTIONS]
```

## Options

- `--check` - Check only (no edits). Default. Runs in Docker for CI parity.
- `--fix` - Apply isort and black, then run flake8. **Also runs in Docker** (Python 3.12 + the CI-pinned black/isort), so auto-formatted files are byte-identical to what CI's `black --check` gates on. It does NOT run on the host: a host Python (e.g. 3.10) can format some constructs differently even at the same black version, and that output then fails the CI gate.
- `--path <file|dir>` - Lint (or, with `--fix`, format) only this file or directory. Repeatable. It must be under `python/geobrix` or `connectors/lakeflow`; a relative path is taken from the current directory if it exists there, else from the repo root. Only the scopes that got a path run, with the scope's own config. Without `--path` both scopes run in full (`src test` and `sources`, as CI does).
- `--log <path>` - Write output to log file.
- `--help` - Display help.

An unscoped `--fix` reformats the whole tree, including files someone else (an agent working in parallel, or your own uncommitted edits elsewhere) is editing. Pass `--path` for each file you changed and use the unscoped form only on purpose.

## Examples

```bash
# Check only (Docker; same as CI)
gbx:lint:python
gbx:lint:python --check

# Auto-fix import order and formatting (Docker; py3.12, same as CI)
gbx:lint:python --fix

# Format only the files you changed
gbx:lint:python --fix --path python/geobrix/src/databricks/labs/gbx/ds/writer.py --path python/geobrix/test/ds/
```

## Notes

- **Config**: each scope uses its own `pyproject.toml` ([tool.isort], [tool.black], [tool.flake8]) — `python/geobrix/pyproject.toml` and `connectors/lakeflow/pyproject.toml` (the tools are run from the scope's directory so they pick up that file).
- **CI**: The Python build action (`.github/actions/python_build`) installs the hash-pinned `python/geobrix/requirements-ci.txt` (black 26.3.1, isort 8.0.1, flake8 7.3.0, flake8-pyproject 1.2.4) on **Python 3.12**, then runs `isort --check-only`, `black --check`, and `flake8` on `src test` (in `python/geobrix`) and on `sources` (in `connectors/lakeflow`). Keep the scope list in `gbx-lint-python.sh` and the "Lint Python" step of that action identical. The dev container matches those exactly, which is why both `--check` and `--fix` run there. (The `[dev]` extra pins the same lint versions for IDE/direct use — keep it in sync with `requirements-ci.in`.)
- **Fix mode**: Use `--fix` during dev to apply isort and black; flake8 remains check-only (no auto-fix). Requires the `geobrix-dev` container (start it with `gbx:docker:start`).
