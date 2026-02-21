# gbx:coverage:scala-package

**Run Scala code coverage for a specific package only**

## Purpose

Faster, targeted coverage analysis when working on a specific package. Runs only the tests for the selected package instead of the entire test suite.

## Time Savings

| Coverage Type | Time | Use Case |
|---------------|------|----------|
| Full coverage | ~10 min | Weekly baseline, pre-release |
| Package-targeted | ~1-3 min | Daily development, package improvement |
| **--class &lt;Name&gt;** | **~10-30 sec** | **Single test class (e.g. after adding tests)** |
| **Savings** | **~7-9 min** | **70-90% faster** |

## Usage

```bash
gbx:coverage:scala-package <package> [options]
```

## Available Packages

| Package | Description | Files | Est. Time |
|---------|-------------|-------|-----------|
| `rasterx` | Raster operations | 73 | ~2-3 min |
| `gridx` | Grid systems (BNG, H3) | 23 | ~1 min |
| `vectorx` | Vector operations | 18 | ~1-2 min |
| `ds` | Data sources | 10 | ~30 sec |
| `expressions` | Expression framework | 7 | ~30 sec |
| `util` | Utilities | 6 | ~30 sec |

## Options

- `--class <name>` - Run only this test class (e.g. `GDALRasterizeTest`); comma-separated for multiple. Much faster than full package.
- `--min-coverage <percent>` - Minimum coverage threshold (default: 90)
- `--clean` - Run `mvn clean` before coverage (default: incremental, no clean)
- `--parallel` - Run tests in parallel (`scoverage:test -T 1C` then `report-only`)
- `--log <path>` - Write output to log file
- `--open` - Open HTML report in browser after generation
- `--help` - Show help message

## Examples

### Basic Usage
```bash
# Run coverage for rasterx package
gbx:coverage:scala-package rasterx

# Single test class only (fast; e.g. to see coverage impact of new tests)
gbx:coverage:scala-package rasterx.operations --class GDALRasterizeTest --open

# With auto-open report
gbx:coverage:scala-package rasterx --open

# With custom threshold
gbx:coverage:scala-package gridx --min-coverage 85
```

### With Logging
```bash
# Log to test-logs/rasterx-coverage.log
gbx:coverage:scala-package rasterx --log rasterx-coverage.log

# Log to custom directory
gbx:coverage:scala-package vectorx --log coverage/vectorx-$(date +%Y%m%d).log
```

### Coverage Improvement Workflow
```bash
# 1. Check which packages need work
gbx:coverage:gaps scala

# 2. Target lowest coverage package
gbx:coverage:scala-package rasterx --open

# 3. Review report, identify uncovered code
# 4. Add tests
# 5. Re-run package coverage
gbx:coverage:scala-package rasterx --open

# 6. Repeat until target reached (90%)
```

## Output

**HTML Report**: `target/scoverage-report/index.html` (or `target/site/scoverage/index.html`)
**XML Report**: `target/scoverage.xml`

Uses the same 2-step flow as `gbx:coverage:scala`: `scoverage:test -T 1C` then `scoverage:report-only` with `aggregateOnly` (one aggregated report). Reports show coverage for the entire codebase; only the selected package's tests are executed.

## When to Use

### ✅ Use Package-Targeted Coverage

- **Daily development** - Working on features in specific package
- **Coverage improvement** - Focusing on low-coverage package
- **Quick validation** - After adding tests to specific package
- **Iterative improvement** - Incrementally improving coverage

### ❌ Don't Use Package-Targeted Coverage

- **Cross-package changes** - Multiple packages modified
- **Weekly baseline** - Need comprehensive results
- **Pre-release validation** - Need full test suite
- **Integration testing** - Tests span multiple packages

## How It Works

Uses Maven's `-Dsuites` parameter to filter tests:

```bash
# Example: rasterx package
mvn clean package \
  -DskipTests=false \
  -Dsuites='com.databricks.labs.gbx.rasterx.*'
```

Only tests matching the pattern execute, but coverage is still measured across the entire codebase.

## Package Coverage vs Full Coverage

### Full Coverage
- **Command**: `gbx:coverage:scala`
- **Tests run**: All tests
- **Time**: ~10 minutes
- **Use**: Weekly, pre-release

### Package-Targeted
- **Command**: `gbx:coverage:scala-package rasterx`
- **Tests run**: Only rasterx tests
- **Time**: ~2 minutes
- **Use**: Daily, targeted improvement

### Report-Only
- **Command**: `gbx:coverage:scala --report-only`
- **Tests run**: None (uses existing data)
- **Time**: ~5 seconds
- **Use**: Status checks, gap analysis

## Tips

1. **Start with gaps analysis**
   ```bash
   gbx:coverage:gaps scala
   ```
   Identifies which packages need attention

2. **Target lowest coverage first**
   Focus on packages below 90% for maximum impact

3. **Use report-only between runs**
   ```bash
   gbx:coverage:scala --report-only --open
   ```
   View results without re-running tests

4. **Weekly full coverage**
   Run full coverage once per week for comprehensive validation

5. **Combine with test commands**
   ```bash
   # Run tests without coverage (fast)
   gbx:test:scala --suite rasterx
   
   # Then run coverage when ready
   gbx:coverage:scala-package rasterx --open
   ```

## Limitations

- Coverage data only as accurate as the tests run
- Cross-package dependencies may show lower coverage
- Integration tests may be skipped
- For comprehensive coverage, use full coverage weekly

## Related Commands

- `gbx:coverage:scala` - Full Scala coverage
- `gbx:coverage:scala --report-only` - View existing coverage
- `gbx:coverage:gaps scala` - Analyze coverage gaps
- `gbx:test:scala` - Run tests without coverage

## Strategy

See `.cursor/rules/coverage-strategy.mdc` for comprehensive guidance on:
- When to use each coverage command
- Baseline + incremental pattern
- Coverage improvement workflow
- Time optimization strategies
