# gbx:test:python-dbr

Run Python Databricks Runtime (DBR) integration tests.

## Purpose

Executes Databricks-specific integration tests that require DBR SQL functions (`st_*` spatial functions, Databricks GEOMETRY types, etc.). These tests are **completely isolated** from regular unit tests and are **excluded** from:

- Regular CI/CD pipelines
- Coverage reporting  
- Automated test runs
- GitHub workflow triggers

## Usage

```bash
# Run all DBR tests
gbx:test:python-dbr

# Run specific test file
gbx:test:python-dbr --path readers/test_dbr_examples.py

# Run tests with specific marker
gbx:test:python-dbr --markers databricks

# Run with verbose output
gbx:test:python-dbr --verbose

# Save output to log file
gbx:test:python-dbr --log dbr-tests.log

# Run on a running DBR cluster instead of the local container
gbx:test:python-dbr --cluster-id <id> --profile oauth-fe --path readers/
```

## Options

| Option | Description |
|--------|-------------|
| `--path PATH` | Run specific test path (file or directory) |
| `--markers MARKERS` | Run tests matching pytest markers |
| `--verbose, -v` | Show verbose output |
| `--log FILE` | Save output to log file |
| `--cluster-id ID` | Run the tests on this running Databricks cluster. The cluster needs the geobrix wheel and JAR as libraries. |
| `--profile NAME` | Databricks CLI profile for `--cluster-id`. Required with it. |
| `--timeout-min N` | Time limit of the cluster run in minutes (default 60). |
| `--help, -h` | Show help message |

## Running on a cluster

- The command packs `docs/tests-dbr/python` into a generated notebook and runs it with `gbx:test:notebooks-serverless --existing-cluster-id`.
- The notebook unpacks the tests on the driver and runs pytest there, in the notebook process, with `GBX_DBR_CLUSTER=1`.
- With that variable the `spark` fixture uses the cluster session. It does not build a local session or look for the JAR.
- `--path` and `--markers` apply as in a local run. A failing test fails the run.
- The command never starts a cluster and never logs in. Use a profile that is valid.

## Requirements

⚠️ **These tests require a Databricks Runtime environment:**

- Databricks SQL functions (`st_geomfromwkb`, `st_transform`, `st_area`, etc.)
- Databricks GEOMETRY types
- GeoBrix JAR with all dependencies
- Sample data mounted at `/Volumes/`

**These tests will NOT work in:**
- Standard Apache Spark (open-source)
- Local development environments
- CI/CD pipelines (GitHub Actions, etc.)

## Test Markers

Tests are organized with pytest markers:

- `@pytest.mark.databricks` - Requires DBR SQL functions
- `@pytest.mark.spatial_sql` - Uses DBR spatial SQL functions  
- `@pytest.mark.integration` - Full integration tests

## Test Location

- **Tests**: `docs/tests-dbr/python/`
- **Config**: `docs/tests-dbr/pytest.ini`
- **Examples**: `docs/tests-dbr/python/*/dbr_*.py`

## Isolation Details

### What is Excluded

1. **GitHub Workflows**: `.github/workflows/doc-tests.yml` explicitly excludes `docs/tests-dbr/**`
2. **Pytest Discovery**: `docs/tests/pytest.ini` uses `testpaths = python` (not `tests-dbr`)
3. **Coverage Reporting**: `docs/tests/pytest.ini` omits `../tests-dbr/*` from coverage
4. **Separate Config**: `docs/tests-dbr/pytest.ini` has its own configuration (no coverage enabled)

### How to Run

DBR tests can ONLY be run:

1. **Manually** via `gbx:test:python-dbr` command
2. **In Databricks workspace** with DBR environment
3. **Via Databricks Asset Bundles (DAB)** (see `DATABRICKS-INTEGRATION-TESTING.md`)

## Examples

### Run All DBR Tests

```bash
gbx:test:python-dbr
```

### Run Specific Module

```bash
# Run all reader DBR tests
gbx:test:python-dbr --path readers/

# Run specific test file
gbx:test:python-dbr --path api/test_dbr_gridx_functions_sql.py
```

### Run by Marker

```bash
# Run only spatial SQL tests
gbx:test:python-dbr --markers spatial_sql

# Run all databricks tests
gbx:test:python-dbr --markers databricks
```

### Save Results

```bash
# Save to log file
gbx:test:python-dbr --log test-logs/dbr-$(date +%Y%m%d).log

# With verbose output
gbx:test:python-dbr --verbose --log dbr-detailed.log
```

## Related Commands

- `gbx:test:python-docs` - Run regular Python unit tests (excludes DBR)
- `gbx:coverage:python-docs` - Coverage for unit tests (excludes DBR)
- `gbx:docker:exec` - Execute commands in Docker container

## Documentation

For more information on DBR integration testing strategy, see:
- `docs/tests-dbr/README.md` - DBR test organization
- `docs/tests/DATABRICKS-INTEGRATION-TESTING.md` - Full DBR testing guide

## Notes

- ✅ **Isolated**: DBR tests never run with unit tests
- ✅ **No Coverage**: DBR tests excluded from coverage reporting
- ✅ **No CI/CD**: GitHub workflows ignore `docs/tests-dbr/`
- ✅ **Manual Only**: Must be explicitly invoked
- ⚠️ **Requires DBR**: Will fail in standard Spark environments
