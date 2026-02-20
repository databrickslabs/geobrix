# Run Function-Info Tests

Re-inventories `function-info.json` (with placeholders for full coverage) and runs the function-info test suite inside the **geobrix-dev** Docker container.

## Usage

```bash
bash .cursor/commands/gbx-test-function-info.sh [OPTIONS]
```

## Options

- `--skip-generate` - Skip the generator; run only pytest in `docs/tests-function-info/`
- `--log <path>` - Write output to log file
- `--help` - Display help

## Default behavior (inside Docker)

1. **Generate**: `python3 docs/scripts/generate-function-info.py` in container  
   Builds function-info.json from doc SQL examples only (no empty usage). Fails if any registered function has no doc example; fix upstream in `docs/tests/python/api/*_functions_sql.py`.

2. **Test**: `pytest docs/tests-function-info/ -v -s` in container  
   - Prints DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED for each RasterX, GridX, and VectorX function (JAR and Spark available in container).
   - Asserts full coverage of function-info.json vs registered list.

## Examples

```bash
# Full run: generate then test
gbx:test:function-info

# Only run tests (do not regenerate JSON)
gbx:test:function-info --skip-generate

# With log
gbx:test:function-info --log function-info-tests.log
```

## Notes

- **Runs inside Docker**: Requires `geobrix-dev` container (e.g. `./scripts/docker/start_docker_with_volumes.sh`). Uses `check_docker` like other GeoBrix test commands.
- DESCRIBE tests use the GeoBrix JAR and PySpark in the container; coverage tests use `registered_functions.txt` and do not require Spark.
