# Run Python Lint (isort, black, flake8)

Runs **isort**, **black**, and **flake8** on the Python package (`python/geobrix/src` and `test`). Same tools and config as CI (`pyproject.toml`).

## Usage

```bash
bash scripts/commands/gbx-lint-python.sh [OPTIONS]
```

## Options

- `--check` - Check only (no edits). Default. Runs in Docker for CI parity.
- `--fix` - Apply isort and black, then run flake8. **Also runs in Docker** (Python 3.12 + the CI-pinned black/isort), so auto-formatted files are byte-identical to what CI's `black --check` gates on. It does NOT run on the host: a host Python (e.g. 3.10) can format some constructs differently even at the same black version, and that output then fails the CI gate.
- `--log <path>` - Write output to log file.
- `--help` - Display help.

## Examples

```bash
# Check only (Docker; same as CI)
gbx:lint:python
gbx:lint:python --check

# Auto-fix import order and formatting (Docker; py3.12, same as CI)
gbx:lint:python --fix
```

## Notes

- **Config**: `python/geobrix/pyproject.toml` ([tool.isort], [tool.black], [tool.flake8]).
- **CI**: The Python build action (`.github/actions/python_build`) installs the hash-pinned `python/geobrix/requirements-ci.txt` (black 26.3.1, isort 8.0.1, flake8 7.3.0, flake8-pyproject 1.2.4) on **Python 3.12**, then runs `isort --check-only`, `black --check`, and `flake8` on `src test`. The dev container matches those exactly, which is why both `--check` and `--fix` run there. (The `[dev]` extra pins the same lint versions for IDE/direct use — keep it in sync with `requirements-ci.in`.)
- **Fix mode**: Use `--fix` during dev to apply isort and black; flake8 remains check-only (no auto-fix). Requires the `geobrix-dev` container (start it with `gbx:docker:start`).
