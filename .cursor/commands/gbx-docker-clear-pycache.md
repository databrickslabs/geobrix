# Clear Python Cache

Clear Python bytecode cache in geobrix-dev Docker container

## Usage

```bash
bash .cursor/commands/gbx-docker-clear-pycache.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file
- `--verbose` - Show detailed output of files being removed
- `--help` - Display help message

## What It Clears

Removes all Python bytecode cache files from:
- **Documentation tests**: `docs/tests/python/`
- **Python package**: `python/geobrix/`

**Specifically removes:**
- `.pyc` files (compiled Python bytecode)
- `__pycache__/` directories
- `.pytest_cache/` directories

## When to Use

**Use this command when:**
- After editing Python code (`examples.py`, `conftest.py`, test files)
- Before running tests to ensure fresh module imports
- Seeing `AttributeError: module has no attribute` errors
- Python tests show stale code despite file changes
- After making changes that aren't being picked up

## Why This Is Needed

**Problem**: Docker volume mounts show file changes, but Python's import system caches bytecode (`.pyc` files). When you edit Python files on the host, the bytecode cache in the container becomes stale, causing tests to run against old code.

**Solution**: Clear all `.pyc` files and `__pycache__` directories to force Python to recompile and reimport modules.

## Examples

```bash
# Basic usage (quiet mode)
bash .cursor/commands/gbx-docker-clear-pycache.sh

# Show which files are being removed
bash .cursor/commands/gbx-docker-clear-pycache.sh --verbose

# With logging
bash .cursor/commands/gbx-docker-clear-pycache.sh --log cache-clear.log
```

## Typical Workflow

```bash
# 1. Edit Python code
vim docs/tests/python/readers/examples.py

# 2. Clear cache (this command)
bash .cursor/commands/gbx-docker-clear-pycache.sh

# 3. Run tests with fresh imports
bash .cursor/commands/gbx-test-python-docs.sh
```

## Notes

- **Safe to run anytime** - Only removes cache files, never source code
- **Fast operation** - Completes in 1-2 seconds
- **Required after edits** - Python doesn't auto-detect file changes in Docker volumes
- **Automatic cleanup** - Test Specialist subagent runs this before tests when appropriate

## Related Commands

- `gbx:test:python-docs` - Run Python documentation tests
- `gbx:test:python` - Run Python unit tests
- `gbx:docker:exec` - Execute arbitrary commands in container
