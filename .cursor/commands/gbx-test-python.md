# Run Python Unit Tests

Runs Python unit tests (non-documentation tests) using pytest.

## Usage

```bash
bash .cursor/commands/gbx-test-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run specific test file or directory
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--markers <markers>` - Run tests matching pytest markers (e.g., "not slow")
- `--help` - Display help message

## Examples

```bash
# Run all Python unit tests
bash .cursor/commands/gbx-test-python.sh

# Run specific test file
bash .cursor/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_operations.py

# Run with logging
bash .cursor/commands/gbx-test-python.sh --log python-tests.log

# Run fast tests only
bash .cursor/commands/gbx-test-python.sh --markers "not slow"
```

## Test Location

- **Source**: `python/geobrix/test/`

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-python-docs` for those)
- Uses pytest with verbose output
- Default log location: `test-logs/` (if filename only provided)
