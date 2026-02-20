# Run Python Lint (isort, black, flake8)

Runs **isort**, **black**, and **flake8** on the Python package (`python/geobrix/src` and `test`). Same tools and config as CI (`pyproject.toml`).

## Usage

```bash
bash .cursor/commands/gbx-lint-python.sh [OPTIONS]
```

## Options

- `--check` - Check only (no edits). Default. Runs in Docker for CI parity.
- `--fix` - Apply isort and black, then run flake8. Runs on **host** so your files are updated. If isort/black/flake8 are not on PATH, the script creates a venv at `python/geobrix/.venv` and installs `.[dev]` there, then runs the tools from that venv.
- `--log <path>` - Write output to log file.
- `--help` - Display help.

## Examples

```bash
# Check only (Docker; same as CI)
gbx:lint:python
gbx:lint:python --check

# Auto-fix import order and formatting (host; uses venv at python/geobrix/.venv if needed)
gbx:lint:python --fix
```

## Notes

- **Config**: `python/geobrix/pyproject.toml` ([tool.isort], [tool.black], [tool.flake8]).
- **CI**: The Python build action runs `isort --check-only`, `black --check`, and `flake8` on push; installs `python/geobrix[dev]`.
- **Fix mode**: Use `--fix` during dev to apply isort and black; flake8 remains check-only (no auto-fix). If the tools are missing, a venv is created at `python/geobrix/.venv` and dev deps are installed there (the directory is gitignored).
