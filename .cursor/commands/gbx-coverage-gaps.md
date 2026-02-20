# gbx:coverage:gaps

**Analyze coverage data and identify improvement opportunities**

## Purpose

Quickly identify which packages/modules have low coverage without running tests. Uses existing coverage data to show package-level breakdown and recommend targets for improvement.

## Key Benefits

- ⚡ **Fast** - Parses existing data (no test execution)
- 📊 **Package-level view** - See coverage by package/module
- 🎯 **Prioritization** - Sorted by coverage (lowest first)
- 💡 **Recommendations** - Suggests specific actions

## Usage

```bash
gbx:coverage:gaps <language> [options]
```

## Languages

- `scala` - Analyze Scala coverage (uses `target/scoverage.xml`)
- `python` - Analyze Python coverage (uses `coverage.xml`)

## Options

- `--threshold <percent>` - Highlight packages below threshold (default: 90)
- `--verbose` - Show file-level details (planned)
- `--help` - Show help message

## Examples

### Basic Usage
```bash
# Analyze Scala coverage
gbx:coverage:gaps scala

# Analyze Python coverage
gbx:coverage:gaps python
```

### Custom Threshold
```bash
# Show packages below 85%
gbx:coverage:gaps scala --threshold 85

# Show packages below 95%
gbx:coverage:gaps python --threshold 95
```

## Sample Output

### Scala Coverage Gaps
```
📊 GeoBrix: Coverage Gaps Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📄 Coverage file: target/scoverage.xml
🎯 Threshold: ≥90%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📦 Package Coverage Summary:

Package              Coverage     Covered/Total         Status
──────────────────────────────────────────────────────────────────
vectorx                72.5%        450/ 620  statements   ⚠️
rasterx                78.3%       1250/1596  statements   ⚠️
ds                     85.2%        245/ 288  statements   ⚠️
util                   88.7%        142/ 160  statements   ⚠️
gridx                  92.1%        567/ 616  statements   ✅
expressions            94.3%        201/ 213  statements   ✅
──────────────────────────────────────────────────────────────────
Overall                82.4%       2855/3493  statements

⚠️  Packages Below Threshold (90%):

  • vectorx: 72.5% (need 108 more statements covered, 17.5% gap)
  • rasterx: 78.3% (need 186 more statements covered, 11.7% gap)
  • ds: 85.2% (need 14 more statements covered, 4.8% gap)
  • util: 88.7% (need 2 more statements covered, 1.3% gap)

💡 Recommended Actions:

  1. Target lowest package: vectorx
     gbx:coverage:scala-package vectorx --open

  2. Add tests to cover 170 uncovered statements

  3. Re-run package coverage to validate improvement

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 Tips:
  • Use --threshold <N> to adjust warning level
  • Run 'gbx:coverage:scala-package <pkg>' to target specific package
  • Run 'gbx:coverage:scala --report-only --open' to view full report
```

## Workflow Integration

### Coverage Improvement Workflow

```bash
# 1. Check gaps (FREE - uses existing data)
gbx:coverage:gaps scala

# 2. Identify lowest coverage package (e.g., vectorx at 72%)

# 3. Target that package (FOCUSED - ~2 min)
gbx:coverage:scala-package vectorx --open

# 4. Review HTML report, identify uncovered code

# 5. Add tests for uncovered code

# 6. Re-run package coverage
gbx:coverage:scala-package vectorx --open

# 7. Repeat until package reaches 90%

# 8. Move to next lowest package
gbx:coverage:gaps scala  # See updated rankings
```

### Weekly Coverage Check

```bash
# Monday: Generate baseline
gbx:coverage:scala --open

# Check status without re-running
gbx:coverage:gaps scala

# Plan week's work based on gaps
```

### Before Major PR

```bash
# 1. Check all packages meet threshold
gbx:coverage:gaps scala --threshold 90
gbx:coverage:gaps python --threshold 90

# 2. If any below threshold, target those packages
gbx:coverage:scala-package <lowest-package> --open

# 3. Add tests until threshold met
```

## Prerequisites

### Scala
Requires existing coverage data from:
- `gbx:coverage:scala` (full coverage)
- `gbx:coverage:scala-package <pkg>` (package coverage)

### Python
Requires existing coverage data from:
- `gbx:coverage:python` (unit test coverage)
- `gbx:coverage:python-docs` (docs test coverage)

## How It Works

1. **Parse Coverage XML** - Reads coverage data from XML files
2. **Group by Package** - Aggregates statements/lines by package
3. **Calculate Coverage** - Computes percentage for each package
4. **Sort by Coverage** - Orders packages from lowest to highest
5. **Identify Gaps** - Highlights packages below threshold
6. **Recommend Actions** - Suggests specific commands to improve

## Interpreting Results

### Status Indicators
- ✅ **Green** - Package meets threshold (≥90% by default)
- ⚠️ **Yellow** - Package below threshold (needs improvement)

### Prioritization
Packages are sorted by coverage (lowest first) to help you focus on highest-impact improvements.

### Gap Calculation
Shows how many statements/lines need coverage to reach threshold:
```
vectorx: 72.5% (need 108 more statements covered, 17.5% gap)
         ^^^^                ^^^                     ^^^^^
    current coverage   statements needed        gap to threshold
```

## Tips

### 1. Check Gaps Before Running Coverage
```bash
# Don't blindly run full coverage
gbx:coverage:scala  # ❌ 10 minutes

# Check gaps first (uses existing data)
gbx:coverage:gaps scala  # ✅ 5 seconds
```

### 2. Target Lowest Coverage First
Focus on packages with highest impact:
- Lowest coverage = most room for improvement
- Small packages = quick wins

### 3. Set Realistic Thresholds
```bash
# If far from 90%, use incremental thresholds
gbx:coverage:gaps scala --threshold 70  # Week 1 goal
gbx:coverage:gaps scala --threshold 80  # Week 2 goal
gbx:coverage:gaps scala --threshold 90  # Final goal
```

### 4. Combine with Package-Targeted Coverage
```bash
# 1. Identify target
gbx:coverage:gaps scala

# 2. Focus on that package
gbx:coverage:scala-package <lowest-package> --open

# 3. Iterate quickly (2 min vs 10 min)
```

### 5. Use Report-Only to View Details
```bash
# After identifying gaps, view detailed report
gbx:coverage:scala --report-only --open
```

## Limitations

- **Data Freshness** - Only as current as last coverage run
- **Package Granularity** - Shows package-level, not file-level (use HTML report for files)
- **No Code Analysis** - Doesn't show what's uncovered (use HTML report)

## Related Commands

- `gbx:coverage:scala` - Generate Scala coverage data
- `gbx:coverage:scala-package <pkg>` - Target specific package
- `gbx:coverage:scala --report-only` - View detailed HTML report
- `gbx:coverage:python` - Generate Python coverage data

## Strategy

See `.cursor/rules/coverage-strategy.mdc` for:
- When to run gaps analysis
- How to interpret results
- Coverage improvement workflow
- Integration with development process
