# Run Python Unit Test Coverage Analysis

Runs Python unit tests with pytest-cov code coverage analysis.

## Usage

```bash
bash .cursor/commands/gbx-coverage-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run coverage for specific test file or directory
- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Examples

```bash
# Run coverage analysis for all Python unit tests
bash .cursor/commands/gbx-coverage-python.sh

# Require minimum 80% coverage
bash .cursor/commands/gbx-coverage-python.sh --min-coverage 80

# Generate report and open in browser
bash .cursor/commands/gbx-coverage-python.sh --open

# Run coverage for specific test directory
bash .cursor/commands/gbx-coverage-python.sh --path python/geobrix/test/rasterx --open
```

## Coverage Report Location

- **HTML Report**: `python/coverage-report/index.html`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses pytest-cov plugin for Python code coverage
- **Test source**: `python/geobrix/test/` (non-documentation tests)
- **Coverage measured on**: `python/geobrix/src/databricks/labs/gbx/` (actual source code)
- Generates both HTML and terminal reports
- Default log location: `test-logs/` (if filename only provided)
