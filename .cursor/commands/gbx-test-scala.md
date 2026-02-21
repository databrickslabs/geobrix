# Run Scala Unit Tests

Runs Scala unit tests (non-documentation tests) using Maven.

## Usage

```bash
bash .cursor/commands/gbx-test-scala.sh [OPTIONS]
```

## Options

- `--by-package` - Run tests per package in sequence (rasterx, gridx, vectorx, ds, expressions, util); reports pass/fail per package (same packages as CI matrix)
- `--suite <pattern>` - Run specific test suite (single pattern)
- `--suites <list>` - Run specific test suites (comma-separated class/package patterns; same as `-Dsuites=`)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--verbose` - Show detailed Maven output
- `--help` - Display help message

## Examples

```bash
# Run all Scala unit tests
bash .cursor/commands/gbx-test-scala.sh

# Run specific test suite
bash .cursor/commands/gbx-test-scala.sh --suite com.databricks.labs.gbx.rasterx.expressions.*

# Run multiple suites (comma-separated)
bash .cursor/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.rasterx.operations.SpatialRefOpsTest,com.databricks.labs.gbx.rasterx.ds.GTiff_DataSourceTest'

# Run with logging
bash .cursor/commands/gbx-test-scala.sh --log scala-tests.log

# Run with verbose output
bash .cursor/commands/gbx-test-scala.sh --verbose

# Run by package (sequence; same packages as CI "Scala tests (by package)" workflow)
bash .cursor/commands/gbx-test-scala.sh --by-package
```

## Test Location

- **Source**: `src/test/scala/com/databricks/labs/gbx/` (excludes `tests.docs.scala.*`)

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-scala-docs` for those)
- Uses Maven profile to skip scoverage for faster execution
- Default log location: `test-logs/` (if filename only provided)
