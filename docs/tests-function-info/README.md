# Function-Info Tests

Tests for **DESCRIBE FUNCTION** / **DESCRIBE FUNCTION EXTENDED** output and for full coverage of `function-info.json` (used by Spark to show function help).

## What’s here

- **`test_describe_rasterx.py`** – For each `gbx_rst_*` function: run DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED, print the output, assert non-empty.
- **`test_describe_gridx.py`** – Same for `gbx_bng_*`.
- **`test_describe_vectorx.py`** – Same for `gbx_st_*`.
- **`test_function_info_coverage.py`** – Asserts every function in `registered_functions.txt` has an entry in `function-info.json` with **non-empty examples**. Empty usage is a failure; fix upstream in the docs.
- **`registered_functions.txt`** – Canonical list of all GeoBrix registered function names (update when adding new functions).

## Run from project root

All of this runs **inside the geobrix-dev Docker container** (same as other doc/test commands).

```bash
# Generate function-info.json from doc SQL examples, then run tests
bash .cursor/commands/gbx-test-function-info.sh

# Or: only run tests (no generator)
bash .cursor/commands/gbx-test-function-info.sh --skip-generate
```

From inside the container (e.g. after `docker exec -it geobrix-dev bash`):

```bash
cd /root/geobrix
python3 -m pytest docs/tests-function-info/ -v -s   # -s so DESCRIBE output is printed
```

## No empty usage

The feeder for function-info is the **SQL API function ref in docs** (`docs/tests/python/api/rasterx_functions_sql.py`, `gridx_functions_sql.py`, `vectorx_functions_sql.py`). Empty or missing examples are not allowed. If the generator fails or the coverage test fails with “missing” or “empty examples”, fix the **upstream dependency**: add or repair the corresponding `*_sql_example()` in the docs so every registered function has a doc SQL example. Then re-run the generator and commit the updated `function-info.json`.
