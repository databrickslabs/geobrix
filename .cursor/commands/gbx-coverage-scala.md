# Run Scala Code Coverage Analysis

Runs Scala unit tests with scoverage code coverage analysis using Maven.

## Usage

```bash
bash .cursor/commands/gbx-coverage-scala.sh [OPTIONS]
```

## Options

- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--report-only` - Skip tests and only generate report from existing data
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Examples

```bash
# Run coverage analysis
bash .cursor/commands/gbx-coverage-scala.sh

# Require minimum 80% coverage
bash .cursor/commands/gbx-coverage-scala.sh --min-coverage 80

# Generate report and open in browser
bash .cursor/commands/gbx-coverage-scala.sh --open

# Generate report only (no test execution)
bash .cursor/commands/gbx-coverage-scala.sh --report-only --open
```

## Coverage Report Location

- **HTML Report**: `target/scoverage-report/index.html`
- **XML Report**: `target/scoverage.xml`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses Maven scoverage plugin for Scala code coverage
- Generates both HTML and XML reports
- Default log location: `test-logs/` (if filename only provided)
- Coverage data persists between runs (use `mvn clean` to reset)
