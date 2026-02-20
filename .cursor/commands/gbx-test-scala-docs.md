# Run Scala Documentation Tests

Runs Scala documentation tests from `docs/tests/scala/` using Maven.

## Usage

```bash
bash .cursor/commands/gbx-test-scala-docs.sh [OPTIONS]
```

## Options

- `--suite <pattern>` – Maven suite pattern (default: `tests.docs.scala.*`). Example: `docs.tests.scala.api.*`
- `--log <path>` – Log file (filename → `test-logs/<name>`, absolute path as-is)
- `--skip-build` – Skip Maven compile before test (optional; `mvn test` still compiles)
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use env or full-bundle default in Scala)
- `--help` – Display help message

**Sample data (default):** The command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` in the container so doc tests use the minimal bundle (required for remote/CI). Use `--no-sample-data-root` to leave it unset so Scala uses the full-bundle default path.

## Examples

```bash
# Run all Scala documentation tests
bash .cursor/commands/gbx-test-scala-docs.sh

# Run specific docs test suite
bash .cursor/commands/gbx-test-scala-docs.sh --suite tests.docs.scala.packages.*

# Run with logging
bash .cursor/commands/gbx-test-scala-docs.sh --log scala-docs-tests.log

# Run specific suite (e.g. API examples only)
bash .cursor/commands/gbx-test-scala-docs.sh --suite 'docs.tests.scala.api.*'

# Skip pre-build (mvn test still compiles)
bash .cursor/commands/gbx-test-scala-docs.sh --skip-build
```

## Test Location

- **Source**: `docs/tests/scala/`
- **Pattern**: `tests.docs.scala.*`

## Notes

- Runs inside Docker container `geobrix-dev`
- Tests validate Scala code examples in documentation
- Ensures documentation examples compile and execute correctly
- Default log location: `test-logs/` (if filename only provided)
