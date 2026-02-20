# Documentation Testing

This directory contains tests that validate all code examples shown in the GeoBrix documentation.

## Philosophy: Single-Copy Pattern

**All non-trivial code examples in documentation are tested and validated.** This ensures:
- ✅ Users can trust that documented code actually works
- ✅ Breaking changes are caught immediately by CI/CD
- ✅ Documentation stays in sync with the codebase
- ✅ No drift between docs and reality

## Coverage

- **SQL Examples**: 57/58 (98%) - All SQL examples auto-imported from tested modules
- **Scala Examples**: 3/3 (100%) - All Scala examples validated through ScalaTest
- **Total**: 60/61 examples (98.4%)

## Structure

```
docs/tests/
├── python/
│   ├── api/
│   │   ├── rasterx_functions_sql.py          # 49 SQL example functions
│   │   ├── test_rasterx_functions_sql.py     # SQL tests for RasterX
│   │   ├── gridx_functions_sql.py            # 9 SQL example functions
│   │   └── test_gridx_functions_sql.py       # SQL tests for GridX
│   ├── integration/                          # Integration tests (DBR / full env); run with --suite integration
│   │   ├── test_quickstart_integration.py
│   │   └── test_setup_integration.py
│   └── ...
├── scala/
│   ├── api/
│   │   └── OverviewExamplesDocTest.scala     # API overview examples
│   └── packages/
│       ├── RasterxExamplesDocTest.scala      # RasterX usage examples
│       └── GridxExamplesDocTest.scala        # GridX usage examples
└── README.md (this file)
```

## Unit vs integration tests

- **Unit (default):** Tests that run in the standard Docker/doc-test environment with sample data. Default runs **exclude** tests marked `@pytest.mark.integration`.
- **Integration:** Tests that require Databricks Runtime (DBR), spatial SQL functions, or specific paths (e.g. `/data/`). Marked with `@pytest.mark.integration`; run only when requested.

**Run unit tests only (default):**
```bash
./scripts/ci/run-doc-tests.sh python
# or
pytest docs/tests/python/ -v -m "not integration"
```

**Run including integration tests** (e.g. on DBR or when you need those tests):
```bash
bash .cursor/commands/gbx-test-python-docs.sh --include-integration --skip-build
# or run only the integration suite (physical split under docs/tests/python/integration/)
bash .cursor/commands/gbx-test-python-docs.sh --suite integration --skip-build
# or
pytest docs/tests/python/ -v -m integration
```

Integration tests live in **`docs/tests/python/integration/`** (physical split). See `prompts/testing/2026-01-26-skipped-tests-inventory.md` for which tests are integration and why.

## Running Tests

### Python/SQL Tests

```bash
# All SQL tests (unit only)
cd docs
pytest tests/python/api/test_*_sql.py -v -m "not integration"

# Specific module
pytest tests/python/api/test_rasterx_functions_sql.py -v
pytest tests/python/api/test_gridx_functions_sql.py -v
```

### Scala Tests

```bash
# Compile
docker exec geobrix-dev mvn test-compile

# Run all doc tests
docker exec geobrix-dev mvn test -Dsuites='docs.tests.scala.*'

# Run specific test
docker exec geobrix-dev mvn test -Dsuites='docs.tests.scala.packages.RasterxExamplesDocTest'
```

### Documentation Build

```bash
cd docs
npm install
npm run build
```

## How It Works

### SQL Examples (Auto-Import Pattern)

1. **Define SQL in Python function**:
```python
# docs/tests/python/api/module_sql.py
def function_name_sql_example():
    """Description"""
    return """
    SELECT ...
    FROM ...;
    """
```

2. **Test it**:
```python
# docs/tests/python/api/test_module_sql.py
def test_function_name_sql_example(spark):
    sql = module_sql.function_name_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0
```

3. **Import into docs** (automatic):
```mdx
import sqlExamples from '!!raw-loader!../../tests/python/api/module_sql.py';

<CodeFromTest 
  code={sqlExamples} 
  functionName="function_name_sql_example" 
  language="sql" 
/>
```

### Scala Examples (Reference Pattern)

1. **Define test with example code**:
```scala
// docs/tests/scala/packages/ExamplesDocTest.scala
test("example_name") {
  // Example code from docs
  import com.databricks.labs.gbx.rasterx.{functions => rx}
  rx.register(spark)
  // ... rest of example
}
```

2. **Reference in docs**:
```mdx
:::note
This example is tested in [`ExamplesDocTest.scala`](link) (test: `example_name`)
:::

```scala
// Example code here
```
```

## Adding New Examples

### For SQL:

1. Add function to `docs/tests/python/api/module_sql.py`
2. Add test to `docs/tests/python/api/test_module_sql.py`
3. Use `<CodeFromTest>` in documentation
4. Run tests to verify

### For Scala:

1. Add test to appropriate `docs/tests/scala/*/DocTest.scala` file
2. Add reference note to documentation
3. Run `mvn test-compile` to verify compilation
4. Run `mvn test -Dsuites='docs.tests.scala.*'` to verify execution

## CI/CD Integration

All tests run automatically on:
- Pull requests affecting `docs/**`
- Pull requests affecting `src/main/scala/**`
- Commits to `main` branch

Failed tests block the PR merge.

## Benefits

| Benefit | Impact |
|---------|--------|
| **User Confidence** | Users trust that documented code works |
| **No Drift** | Docs auto-update from single source |
| **Breaking Changes** | CI catches API changes immediately |
| **Maintenance** | Update once, propagates everywhere |
| **Quality** | Enterprise-grade documentation |

## Statistics

- **Total Examples**: 60 validated
- **Test Coverage**: 98.4%
- **Languages**: Python, SQL, Scala
- **Test Functions**: 46
- **Lines of Test Code**: 1,454

## Contact

Questions about documentation testing? See:
- [Migration Reports](../../claude/docs/)
- [Contribution Guide](../../CONTRIBUTING.md)
