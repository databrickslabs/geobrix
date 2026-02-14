# Run Scala Documentation Test Coverage Analysis

Runs Scala documentation tests with scoverage code coverage analysis.

## Usage

```bash
bash .cursor/commands/gbx-coverage-scala-docs.sh [OPTIONS]
```

## Options

- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--report-only` - Skip tests and only generate report from existing data
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Examples

```bash
# Run documentation test coverage analysis
bash .cursor/commands/gbx-coverage-scala-docs.sh

# Require minimum 75% coverage
bash .cursor/commands/gbx-coverage-scala-docs.sh --min-coverage 75

# Generate report and open in browser
bash .cursor/commands/gbx-coverage-scala-docs.sh --open

# Generate report only (no test execution)
bash .cursor/commands/gbx-coverage-scala-docs.sh --report-only --open
```

## Coverage Report Location

- **HTML Report**: `target/scoverage-docs-report/index.html`
- **XML Report**: `target/scoverage-docs-report/scoverage.xml`

## What This Measures

- **Test source**: `docs/tests/scala/` (package: `tests.docs.scala.*`)
- **Coverage measured on**: `src/main/scala/com/databricks/labs/gbx/` (actual source code)

This command shows which parts of the GeoBrix source code are exercised by documentation examples, separate from unit test coverage.

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses Maven scoverage plugin for Scala code coverage
- Report goes to separate directory to distinguish from unit test coverage
- Documentation tests validate that code examples in docs compile and execute
- Default log location: `test-logs/` (if filename only provided)

## Comparison with Unit Test Coverage

| Command | Tests Run | Report Location | Purpose |
|---------|-----------|----------------|---------|
| `gbx:coverage:scala` | Unit tests (`src/test/scala/`) | `target/scoverage-report/` | Main coverage |
| `gbx:coverage:scala-docs` | Doc tests (`docs/tests/scala/`) | `target/scoverage-docs-report/` | Doc validation |

Both measure coverage of the same source code (`src/main/scala/`), but run different test suites.
