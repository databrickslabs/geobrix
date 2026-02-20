# Databricks Runtime Integration Tests

This directory contains tests that require **Databricks Runtime SQL functions** (`st_*` functions) for spatial operations.

## Purpose

These tests validate code examples that integrate GeoBrix with Databricks Runtime's built-in spatial SQL functions. These examples demonstrate:

- Converting GeoBrix WKB output to Databricks GEOMETRY type
- Using Databricks ST functions (`st_geomfromwkb`, `st_area`, `st_centroid`, `st_intersects`, etc.)
- Performing spatial joins and queries
- Integrating GeoBrix readers with Databricks spatial functions

## Requirements

**These tests require Databricks Runtime** and will **not work in open-source Spark**.

### Prerequisites

1. **Databricks Runtime** (13.3 LTS or later recommended)
   - Must have `st_*` SQL functions available
   - Functions like `st_geomfromwkb`, `st_area`, `st_centroid`, `st_intersects`, `st_transform` must be available

2. **GeoBrix Library**
   - GeoBrix JAR must be installed on the cluster
   - Python bindings must be installed

3. **Sample Data**
   - Sample geospatial data must be available at `/Volumes/main/default/geobrix_samples/geobrix-examples/`
   - See `docs/docs/sample-data.md` for complete catalog

## Structure

```
tests-dbr/
├── README.md (this file)
├── scala/
│   └── api/
│       └── VectorXCompleteExample.snippet   # VectorX migration (st_*); integration tests in python/api
├── python/
│   ├── conftest.py (pytest configuration with DBR markers)
│   ├── api/
│   │   ├── vectorx_conversion.py (VectorX legacy conversion + SQL examples)
│   │   ├── sql_reading_dbr.py (SQL reading with shapefile + st_*)
│   │   └── test_*.py (tests)
│   ├── readers/
│   │   └── ogr/
│   │       ├── examples.py (OGR → GEOMETRY conversion)
│   │       └── test_examples.py
│   ├── sample_data/
│   │   ├── additional.py (synthetic points with st_point)
│   │   └── test_additional.py
│   ├── limitations/
│   │   ├── examples.py (convert to Databricks GEOMETRY workaround)
│   │   └── test_examples.py
│   └── databricks_spatial/
│       ├── examples.py (ST/H3 examples)
│       └── test_examples.py
```

## Isolation from Unit Tests

⚠️ **IMPORTANT**: These tests are **completely isolated** from regular unit tests and are **excluded** from:

### 1. **GitHub CI/CD Workflows**
- `.github/workflows/doc-tests.yml` explicitly excludes `docs/tests-dbr/**`
- Changes to `tests-dbr/` do NOT trigger GitHub Actions
- No automated test runs in CI/CD pipelines

### 2. **Pytest Test Discovery**
- `docs/tests/pytest.ini` configures `testpaths = python` (excludes `tests-dbr`)
- `docs/tests/pytest.ini` configures `norecursedirs = tests-dbr`
- Regular pytest runs do NOT collect tests from `tests-dbr/`

### 3. **Coverage Reporting**
- `docs/tests/pytest.ini` omits `../tests-dbr/*` from coverage
- DBR tests are NOT included in code coverage metrics
- Coverage reports only measure unit test coverage

### 4. **Separate Configuration**
- `docs/tests-dbr/pytest.ini` has its own independent configuration
- NO coverage reporting enabled for DBR tests
- Different markers and test organization

### Verification

```bash
# Regular unit tests: 403 tests collected (excludes DBR)
pytest docs/tests/python/ --collect-only

# DBR tests (isolated)
pytest docs/tests-dbr/python/ --collect-only
```

## Running Tests

### Using Cursor Command (Recommended)

```bash
# Run all DBR tests
gbx:test:python-dbr

# Run specific test file
gbx:test:python-dbr --path api/test_vectorx_conversion.py

# Run with specific markers
gbx:test:python-dbr --markers databricks

# Save to log
gbx:test:python-dbr --log dbr-tests.log
```

See `.cursor/commands/gbx-test-python-dbr.md` for full documentation.

### In Databricks Workspace

**Option 1: Run in Databricks Notebook**

```python
# Install pytest if needed
%pip install pytest pytest-cov

# Run all DBR tests
%sh
cd /Workspace/Repos/your-repo/geobrix
pytest docs/tests-dbr/python/ -v -m "databricks"
```

**Option 2: Run via Databricks CLI**

```bash
# From your local machine
databricks jobs run-now --job-id <job-id> \
  --python-params '["docs/tests-dbr/python/"]'
```

### Test Markers

All tests in this directory are automatically marked with `@pytest.mark.databricks`:

```bash
# Run all DBR tests
pytest docs/tests-dbr/python/ -v -m "databricks"

# Skip DBR tests (useful for open-source Spark environments)
pytest docs/tests/python/ -v -m "not databricks"
```

## Expected Behavior

### In Databricks Runtime

✅ **Tests should pass** - All `st_*` functions are available

### In Open-Source Spark

⚠️ **Tests will skip** - Functions are not available, tests gracefully skip with:
```
SKIPPED [1] docs/tests-dbr/python/api/test_vectorx_conversion.py::test_...
Databricks SQL functions not available: UNRESOLVED_ROUTINE. `st_geomfromwkb` is not a registered function
```

This is **expected behavior** - the tests are designed to skip when Databricks Runtime is not available.

## Doc-Referenced Modules

Only modules that are imported and displayed in documentation (via `CodeFromTest` / `raw-loader`) remain under `tests-dbr/`:

- **api/**: `vectorx_conversion.py`, `sql_reading_dbr.py` (Python/SQL API pages)
- **readers/ogr/**: OGR → GEOMETRY conversion (readers/ogr.mdx)
- **sample_data/**: Synthetic points with `st_point` (sample-data/additional.mdx)
- **limitations/**: Convert to Databricks GEOMETRY workaround (limitations.mdx)
- **databricks_spatial/**: ST/H3 examples (databricks-spatial.mdx)
- **scala/api/**: `VectorXCompleteExample.snippet` (api/scala.mdx)

## Related Documentation

- **Main Documentation Tests**: `docs/tests/` - Tests that work in open-source Spark
- **Sample Data Guide**: `docs/docs/sample-data.md` - Sample data catalog
- **Databricks Integration**: See `prompts/testing/2026-01-26-databricks-integration-testing.md`

## Notes

- These tests are **separate** from the main documentation tests in `docs/tests/`
- Main tests (`docs/tests/`) work in **both** open-source Spark and Databricks Runtime
- DBR tests (`docs/tests-dbr/`) **only** work in Databricks Runtime
- This separation ensures CI/CD can run main tests in open-source Spark environments
