# Documentation Example Tests

## Purpose

This package contains tests that verify code examples shown in the GeoBrix documentation are valid, compile correctly, and use actual classes from the codebase.

## Structure

Tests mirror the documentation structure for easy maintenance:

```
src/test/scala/com/databricks/labs/gbx/docs/
├── README.md (this file)
├── advanced/
│   ├── CustomUdfsDocTest.scala      # tests for docs/docs/advanced/custom-udfs.md
│   └── OverviewDocTest.scala        # tests for docs/docs/advanced/overview.md
└── api/
    ├── RasterxFunctionsDocTest.scala  # tests for docs/docs/api/rasterx-functions.md (TODO)
    ├── GridxFunctionsDocTest.scala    # tests for docs/docs/api/gridx-functions.md (TODO)
    └── VectorxFunctionsDocTest.scala  # tests for docs/docs/api/vectorx-functions.md (TODO)
```

**Naming convention**: `{DocFileName}DocTest.scala` in matching directory structure

## Running Tests

### ⭐ Recommended: Docker (with GDAL natives)

```bash
# Compile doc tests
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test-compile"

# Run specific doc test file (use -Dsuites= not -Dtest=)
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites=com.databricks.labs.gbx.docs.advanced.CustomUdfsDocTest"

# Run all doc tests
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites='com.databricks.labs.gbx.docs.*'"

# Run tests in a specific docs directory
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites='com.databricks.labs.gbx.docs.advanced.*'"
```

**Why these flags?**
- `unset JAVA_TOOL_OPTIONS` - Avoids JVM option conflicts
- `export JUPYTER_PLATFORM_DIRS=1` - Suppresses irrelevant Jupyter warnings
- `cd /root/geobrix` - Ensures correct working directory in container

### Alternative: Local Maven (requires local GDAL installation)

```bash
# Compile only (fastest - just verifies code is valid)
mvn test-compile

# Run specific test file (use -Dsuites= for scalatest)
mvn test -Dsuites=com.databricks.labs.gbx.docs.advanced.CustomUdfsDocTest

# Run all doc tests
mvn test -Dsuites="com.databricks.labs.gbx.docs.*"
```

### Debug Mode (with jdwp agent)

```bash
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites=com.databricks.labs.gbx.docs.advanced.CustomUdfsDocTest -DagentLib='\${jdwp.agent}'"

# Then attach debugger to port 5005
```

**Note**: Use `-Dsuites=` (not `-Dtest=`) for scalatest-maven-plugin!

See `/src/test/0README.md` for more details on debugging.

## Adding New Tests

When you add code examples to documentation:

### 1. Create test file in matching location

For `docs/docs/advanced/my-feature.md`, create:  
`src/test/scala/com/databricks/labs/gbx/docs/advanced/MyFeatureDocTest.scala`

For `docs/docs/api/my-api.md`, create:  
`src/test/scala/com/databricks/labs/gbx/docs/api/MyApiDocTest.scala`

### 2. Use this template

```scala
package com.databricks.labs.gbx.docs.advanced

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for code examples in docs/docs/advanced/my-feature.md
  */
class MyFeatureDocTest extends AnyFunSuite {

    test("pattern description from docs") {
        // Your doc example code here
        // If it compiles and imports work, test passes
        succeed
    }
}
```

### 3. Test it in Docker

```bash
# Compile check and LOG (only compile when code changes!)
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test-compile" 2>&1 | tee test-logs/doc-tests-compile-$(date +%Y%m%d-%H%M%S).log

# Check result from log
tail -20 test-logs/doc-tests-compile-*.log | grep "BUILD"
```

## What We Test

✅ **Code Compiles**: Examples use real classes and methods  
✅ **Imports Are Valid**: No references to non-existent packages  
✅ **Patterns Work**: UDF patterns, execute methods, resource cleanup  
✅ **API Signatures**: Methods take expected parameters

## Current Coverage

| Doc File | Test File | Tests | Status |
|----------|-----------|-------|--------|
| `advanced/custom-udfs.md` | `advanced/CustomUdfsDocTest.scala` | 8 | ✅ |
| `advanced/overview.md` | `advanced/OverviewDocTest.scala` | 1 | ✅ |
| `api/rasterx-functions.md` | `api/RasterxFunctionsDocTest.scala` | 0 | ❌ TODO |
| `api/gridx-functions.md` | `api/GridxFunctionsDocTest.scala` | 0 | ❌ TODO |
| `api/vectorx-functions.md` | `api/VectorxFunctionsDocTest.scala` | 0 | ❌ TODO |

**Goal**: Cover all code examples in API reference and advanced usage docs.

## Recent Fixes

### 2026-01-11: Restructured to Mirror Docs

- **Before**: Flat structure (`DocExamplesTest.scala`)
- **After**: Mirrors docs structure (`advanced/`, `api/`)
- **Benefit**: Easier to maintain as docs grow

### 2026-01-11: Fixed MosaicRasterTile References

- **Problem**: Docs referenced non-existent `MosaicRasterTile` class
- **Fix**: Updated all examples to use `RasterDriver.readFromBytes()`
- **Impact**: 10+ examples corrected in `docs/advanced/custom-udfs.md`
- **Tests**: Added `CustomUdfsDocTest.scala` to prevent regression

## Docker Integration

### Why Docker?

- ✅ Consistent GDAL environment across machines
- ✅ GDAL natives already installed and configured
- ✅ Same environment as CI/CD
- ✅ No local GDAL installation required
- ✅ Easy debugging with jdwp agent

### Container Details

The `geobrix-dev` container includes:
- GDAL native libraries (`/usr/lib/libgdal*`)
- Java with GDAL JNI bindings
- Maven with all dependencies cached
- Project mounted at `/root/geobrix`
- Test resources available

### Quick Commands Reference

```bash
# Check if container is running
docker ps | grep geobrix-dev

# Start container if not running
cd scripts/docker && ./start_docker.sh

# Compile doc tests
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test-compile"

# Run all doc tests
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dtest='com.databricks.labs.gbx.docs.*'"

# Shell into container (for interactive debugging)
docker exec -it geobrix-dev /bin/bash
```

### Troubleshooting

If tests fail with `UnsatisfiedLinkError`:

```bash
# Check GDAL libs in container
docker exec geobrix-dev ls /usr/lib/libgdal*

# Verify java.library.path
docker exec geobrix-dev /bin/bash -c "cd /root/geobrix && mvn test -Dtest=CustomUdfsDocTest -X" | grep library.path
```

See `/scripts/docker/README-DOCKER.md` for full Docker documentation.

## Anti-Patterns to Catch

### ❌ Non-existent classes

```scala
// BAD - this class doesn't exist
val tile = MosaicRasterTile.deserialize(bytes)
```

### ✅ Actual classes

```scala
// GOOD - this is the real API
val ds = RasterDriver.readFromBytes(bytes, Map.empty)
```

### ❌ Missing resource cleanup

```scala
// BAD - leaks GDAL resources
val ds = gdal.Open(path)
RST_Width.execute(ds)
// forgotten cleanup!
```

### ✅ Proper cleanup

```scala
// GOOD - explicit cleanup
val ds = gdal.Open(path)
try {
  RST_Width.execute(ds)
} finally {
  RasterDriver.releaseDataset(ds)
}
```

## Maintenance Workflow

### When Adding/Updating Doc Examples

1. **Write the example in markdown**
   ```markdown
   ### My Feature
   
   \`\`\`scala
   import com.databricks.labs.gbx.rasterx.expressions.MyExpression
   val result = MyExpression.execute(dataset)
   \`\`\`
   ```

2. **Create matching test**
   ```scala
   // src/test/scala/com/databricks/labs/gbx/docs/advanced/MyFeatureDocTest.scala
   test("My Feature example") {
     import com.databricks.labs.gbx.rasterx.expressions.MyExpression
     // If imports work, test passes
     succeed
   }
   ```

3. **Verify in Docker**
   ```bash
   docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test-compile"
   ```

4. **Commit both together**
   ```bash
   git add docs/docs/advanced/my-feature.md
   git add src/test/scala/com/databricks/labs/gbx/docs/advanced/MyFeatureDocTest.scala
   git commit -m "Add my-feature docs with tests"
   ```

### Monthly Review

```bash
# Run all doc tests
docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dtest='com.databricks.labs.gbx.docs.*'"

# Review coverage
ls -la src/test/scala/com/databricks/labs/gbx/docs/advanced/
ls -la src/test/scala/com/databricks/labs/gbx/docs/api/

# Add tests for untested docs
```

## Integration with CI

Suggested GitHub Actions workflow:

```yaml
name: Doc Tests

on:
  pull_request:
    paths:
      - 'docs/docs/**/*.md'
      - 'src/main/scala/**'
      - 'src/test/scala/com/databricks/labs/gbx/docs/**'

jobs:
  doc-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Build Docker image
        run: |
          cd scripts/docker
          ./build.sh
      
      - name: Run doc tests
        run: |
          docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dtest='com.databricks.labs.gbx.docs.*'"
```

## Success Metrics

- **Compilation**: All doc example tests compile ✅
- **Structure**: Tests mirror docs structure ✅
- **Coverage**: 9 tests covering 2 doc files (goal: 50+ tests)
- **Freshness**: Zero references to non-existent classes ✅
- **CI Integration**: Automated testing on doc changes (TODO)

## Questions?

See:
- `/docs/doc-test-proposal.md` - Full automation strategy
- `/docs/doc-test-quickstart.md` - Implementation guide  
- `/docs/DOC_TEST_QUICKSTART_COMPLETE.md` - What we built
- `/scripts/docker/README-DOCKER.md` - Docker setup details
- `/src/test/0README.md` - General test execution notes
