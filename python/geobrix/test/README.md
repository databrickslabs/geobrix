# GeoBriX Python Tests

This directory contains Python tests for the GeoBriX library using pytest.

## Structure

```
test/
├── README.md (this file)
├── rasterx/
│   ├── __init__.py
│   ├── test_rasterx_registration.py    # Registration and basic smoke tests
│   ├── test_rasterx_operations.py      # Transformation operations (clip, filter, etc.)
│   ├── test_rasterx_accessors.py       # Metadata and statistics accessors
│   └── test_rasterx_generators.py      # Tile generation and constructors
├── gridx/
│   ├── __init__.py
│   ├── test_bng_registration.py        # BNG registration tests
│   └── test_bng_operations.py          # BNG grid operations
└── vectorx/
    ├── __init__.py
    └── test_legacy_registration.py     # Legacy vector function tests
```

## Running Tests

### Prerequisites

Tests must be run inside the Docker environment with GDAL natives properly configured:

```bash
# Start the Docker container (if not already running)
cd scripts/docker/extras
./geobrix_docker.sh

# SSH into the container
docker exec -it <container_id> /bin/bash
```

### Run All Tests

```bash
cd /path/to/geobrix/python/geobrix
pytest test/
```

### Run Specific Test Modules

```bash
# RasterX tests
pytest test/rasterx/test_rasterx_operations.py
pytest test/rasterx/test_rasterx_accessors.py
pytest test/rasterx/test_rasterx_generators.py

# GridX tests
pytest test/gridx/test_bng_operations.py

# VectorX tests
pytest test/vectorx/test_legacy_registration.py
```

### Run with Verbose Output

```bash
pytest -v test/rasterx/test_rasterx_operations.py
```

### Run Specific Test Functions

```bash
pytest test/rasterx/test_rasterx_operations.py::test_rst_clip
pytest test/rasterx/test_rasterx_accessors.py::test_rst_avg
```

### Run with Output

```bash
pytest -s test/  # Show print statements
```

### Run with Coverage

```bash
pytest --cov=databricks.labs.gbx test/
```

## Test Organization

### RasterX Tests

**test_rasterx_operations.py**: Tests for raster transformation and processing operations
- Clipping (`rst_clip`)
- Filtering (`rst_filter`)
- Coordinate transformations (`rst_transform`)
- Convolution (`rst_convolve`)
- Merging and combining rasters
- NDVI calculation
- Format conversion
- Map algebra

**test_rasterx_accessors.py**: Tests for raster metadata and statistics
- Statistical measures (avg, min, max, median)
- Dimensions (width, height, pixel size)
- Georeference information
- Metadata and band information
- NoData values
- SRID and bounding box

**test_rasterx_generators.py**: Tests for raster tile generation and construction
- Retiling operations
- Tile generation by size
- Overlapping tiles
- Band separation and combination
- H3 tessellation
- Raster construction from files and content

### GridX Tests

**test_bng_operations.py**: Tests for British National Grid operations
- Cell ID conversions (WKB, WKT)
- Cell properties (area, centroid)
- Set operations (intersection, union)
- Distance calculations
- Coordinate conversions
- Polyfill and tessellation
- Aggregation functions

### VectorX Tests

**test_legacy_registration.py**: Tests for legacy vector functions
- Function registration
- Basic vector operations

## Test Data

Tests use resources from the main project:
- Raster files: `../../src/test/resources/modis/*.TIF`
- NetCDF files: `../../src/test/resources/binary/netcdf-CMIP5/*.nc`
- Shapefiles: `../../src/test/resources/binary/shapefile/*.shp`
- GeoJSON: `../../src/test/resources/text/*.geojson`

## Fixtures

Tests use pytest fixtures defined in each test file:

- `spark`: Module-scoped SparkSession with GeoBriX functions registered
- Configured with proper Java library paths for GDAL natives
- Reused across all tests in a module for performance

## Environment

Required environment configuration:
- Java library path: `/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native`
- GeoBriX JAR: Automatically detected from `lib/` directory
- Log level: ERROR (to reduce output noise)

## Troubleshooting

### ImportError: No module named 'databricks.labs.gbx'

Ensure the GeoBriX package is built and the JAR is in the `lib/` directory:
```bash
cd /path/to/geobrix
mvn clean package -DskipTests
```

### GDAL/OGR Errors

Ensure GDAL natives are properly configured in the Docker environment:
```bash
export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
```

### SparkSession Issues

If SparkSession fails to start, check:
1. Java is properly installed
2. Spark dependencies are available
3. GeoBriX JAR path is correct

### Test Failures

If tests fail:
1. Check that test resources exist in `../../src/test/resources/`
2. Verify GDAL drivers are available: `gdalinfo --formats`
3. Check Spark logs for detailed error messages

## Contributing

When adding new tests:
1. Follow the existing test structure and naming conventions
2. Use descriptive test function names starting with `test_`
3. Include docstrings explaining what each test verifies
4. Clean up resources properly (though Spark handles most cleanup)
5. Use appropriate assertions and error messages
6. Consider edge cases and error conditions

## Contact

For questions about tests, refer to:
- Main project README: `../../README.md`
- Test coverage summary: `../../TEST_COVERAGE_PHASE1_SUMMARY.md`

