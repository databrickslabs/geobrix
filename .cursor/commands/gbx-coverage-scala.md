# Run Scala Code Coverage Analysis

Runs Scala unit tests with scoverage code coverage analysis using Maven.

## Usage

```bash
bash .cursor/commands/gbx-coverage-scala.sh [OPTIONS]
```

## Options

- `--min-coverage <percent>` - Fail if coverage is below threshold (e.g., 80)
- `--report-only` - Skip tests and only generate report from existing data
- `--clean` - Run `mvn clean` before coverage (default: incremental, no clean, for speed)
- `--parallel` - Run tests in parallel (`scoverage:test -T 1C` then `report-only`; faster on multi-core)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--open` - Automatically open HTML coverage report in browser
- `--help` - Display help message

## Speed tips (Docker)

- Maven runs with `MAVEN_OPTS=-Xmx4G -XX:+UseG1GC` for faster builds.
- Default flow: `scoverage:test -T 1C` (parallel, thread-safe) then `scoverage:report-only` with `aggregateOnly` (one report, no per-module HTMLs).
- Use `--clean` only when you need a full rebuild; default is incremental.
- `--parallel` uses the same 2-step flow (no extra behavior; tests already run with -T 1C).

## Examples

```bash
# Run coverage analysis (incremental, no clean)
bash .cursor/commands/gbx-coverage-scala.sh

# Parallel tests then report (faster on multi-core)
bash .cursor/commands/gbx-coverage-scala.sh --parallel

# Full clean + coverage
bash .cursor/commands/gbx-coverage-scala.sh --clean

# Generate report and open in browser
bash .cursor/commands/gbx-coverage-scala.sh --open

# Generate report only (no test execution)
bash .cursor/commands/gbx-coverage-scala.sh --report-only --open
```

## Coverage Report Location

- **HTML Report**: `target/scoverage-report/index.html` (or `target/site/scoverage/index.html`)
- **XML Report**: `target/scoverage.xml`

## Notes

- Runs inside Docker container `geobrix-dev`
- Uses Maven scoverage plugin for Scala code coverage
- Generates both HTML and XML reports
- Default log location: `test-logs/` (if filename only provided)
- Coverage data persists between runs; default is incremental (use `--clean` to force full rebuild)
