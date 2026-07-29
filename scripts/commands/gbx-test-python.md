# Run Python Unit Tests

Runs Python unit tests (non-documentation tests) using pytest.

## Usage

```bash
bash scripts/commands/gbx-test-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run specific test file or directory
- `-k <expr>` - Pytest keyword filter (e.g. `"merge or fileName"`)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--with-integration` - Include `@pytest.mark.integration` tests (network downloads, slow). Excluded by default.
- `--markers <expr>` - Override marker filter with a custom pytest expression (e.g. `"not slow"`). Disables the default `not integration` filter.
- `--help` - Display help message

## Default marker filter

By default the script runs with `-m "not integration"`, matching CI's `python_build` action. This excludes `python/geobrix/test/sample/test_sample_bundle.py::test_run_*_bundle_returns_dict_shape`, which download hundreds of MB of sample data.

Opt in with `--with-integration` (drops the filter entirely) or `--markers <expr>` (replaces it with your own expression).

## Examples

```bash
# Unit tests only (default — fast, matches CI)
bash scripts/commands/gbx-test-python.sh

# Include integration tests (network downloads)
bash scripts/commands/gbx-test-python.sh --with-integration

# Run specific test file
bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_operations.py

# Run with logging
bash scripts/commands/gbx-test-python.sh --log python-tests.log

# Custom marker expression (overrides the default)
bash scripts/commands/gbx-test-python.sh --markers "not slow"
```

## Test Location

- **Source**: `python/geobrix/test/`

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-python-docs` for those)
- Uses pytest with verbose output
- Default log location: `test-logs/` (if filename only provided)
