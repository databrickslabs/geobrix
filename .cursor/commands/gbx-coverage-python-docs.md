# Run Python Documentation Test Coverage Analysis

Runs Python documentation tests with pytest-cov code coverage analysis.

## Usage

```bash
bash .cursor/commands/gbx-coverage-python-docs.sh [OPTIONS]
```

## Options

- `--path <path>` - Run coverage for specific test file or directory (relative to docs/tests/python/)
- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Examples

```bash
# Run coverage analysis for all Python documentation tests
bash .cursor/commands/gbx-coverage-python-docs.sh

# Generate report and open in browser
bash .cursor/commands/gbx-coverage-python-docs.sh --open

# Require minimum 70% coverage
bash .cursor/commands/gbx-coverage-python-docs.sh --min-coverage 70 --open

# Run coverage for specific test directory
bash .cursor/commands/gbx-coverage-python-docs.sh --path api --open
```

## Coverage Report Location

- **HTML Report**: `docs/tests/coverage-report/index.html`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses pytest-cov plugin for Python code coverage
- **Test source**: `docs/tests/python/`
- **Coverage measured on**: `python/geobrix/src/databricks/labs/gbx/` (actual source code)
- Excludes integration tests by default
- Generates both HTML and terminal reports
- Default log location: `test-logs/` (if filename only provided)
- Validates that documentation code examples are properly tested
