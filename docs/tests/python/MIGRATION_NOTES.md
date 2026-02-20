# Documentation Tests

This directory contains tests that verify the code examples in the GeoBrix documentation compile and run correctly.

## Purpose

These tests ensure that:
1. Documentation code examples are syntactically correct
2. Examples produce expected results
3. Documentation stays up-to-date with code changes
4. Users can trust that documented examples actually work

## Structure

Tests mirror the documentation structure:

```
test/docs/
├── README.md
├── __init__.py
└── advanced/
    ├── __init__.py
    └── test_library_integration.py  -> docs/docs/advanced/library-integration.md
```

## Running Tests

### Prerequisites

Install test dependencies including rasterio:

```bash
cd python/geobrix
pip install -e ".[test]"
```

### Run All Documentation Tests

```bash
pytest test/docs/ -v
```

### Run Specific Documentation Test File

```bash
pytest test/docs/advanced/test_library_integration.py -v
```

### Run Specific Test

```bash
pytest test/docs/advanced/test_library_integration.py::test_basic_statistics -v
```

## Test Coverage

### advanced/test_library_integration.py

Tests for `docs/docs/advanced/library-integration.md` - Rasterio Integration:

1. **test_rasterio_import** - Verify rasterio can be imported
2. **test_basic_statistics** - Basic pattern: GeoBrix tile to rasterio with NumPy stats
3. **test_metadata_extraction** - Extract comprehensive metadata
4. **test_valid_pixel_count** - Count valid (non-nodata) pixels
5. **test_normalize_raster** - Normalize values to 0-255 range
6. **test_window_operations** - Process raster in windows
7. **test_quality_check** - Quality check UDF from pipeline example
8. **test_pixel_to_coords** - Convert pixel coordinates to geographic
9. **test_best_practice_memoryfile** - Verify MemoryFile pattern
10. **test_best_practice_handle_nodata** - Handle NoData properly

## Notes

- Tests use the same test data as other GeoBrix tests (e.g., MODIS data)
- Tests are skipped if optional dependencies (rasterio, numpy) are not available
- Tests follow the same fixture pattern as other Python tests in GeoBrix
- Each test documents which section of the documentation it verifies

## Maintenance

When updating documentation:
1. Update the relevant test file if examples change
2. Add new tests for new examples
3. Run tests to verify examples still work
4. Keep test docstrings in sync with documentation section references

## CI/CD Integration

These tests should run as part of the standard test suite to catch documentation drift early.

```bash
# In CI pipeline
cd python/geobrix
pip install -e ".[test]"
pytest test/docs/ -v --tb=short
```
