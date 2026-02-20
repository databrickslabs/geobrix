# gbx:coverage:baseline

**Generate comprehensive baseline coverage data for reference**

## Purpose

Run full test suite to establish baseline coverage data. This data is used by other coverage commands (like `gbx:coverage:gaps`) and provides a weekly reference point for coverage metrics.

## When to Run

### Recommended Schedule
- ✅ **Weekly** - Monday morning ritual
- ✅ **Before major PR** - Comprehensive validation
- ✅ **After major refactoring** - Re-establish baseline
- ✅ **When data is stale** - Coverage data >7 days old

### Don't Run
- ❌ After every change (use package-targeted instead)
- ❌ For quick checks (use report-only instead)
- ❌ Daily (wasteful - weekly is sufficient)

## Usage

```bash
gbx:coverage:baseline <language> [options]
```

## Languages

- `scala` - Full Scala coverage (~10 minutes)
- `python` - Full Python coverage (~30 seconds)

## Options

- `--min-coverage <percent>` - Minimum coverage threshold (default: 90)
- `--log <path>` - Write output to log file  
- `--open` - Open HTML report in browser
- `--help` - Show help message

## Examples

### Weekly Baseline
```bash
# Monday morning - Scala baseline
gbx:coverage:baseline scala --open

# Python baseline (always fast)
gbx:coverage:baseline python --open
```

### With Logging
```bash
# Log baseline for record keeping
gbx:coverage:baseline scala --log baseline-$(date +%Y%m%d).log
```

### Custom Threshold
```bash
# Set different target
gbx:coverage:baseline scala --min-coverage 85
```

## Workflow Integration

### Monday Morning Ritual
```bash
# 1. Generate baseline (comprehensive data)
gbx:coverage:baseline scala --open

# 2. Check gaps (identify priorities)
gbx:coverage:gaps scala

# 3. Plan week's coverage work
# Target: Improve lowest package by 5-10%
```

### Daily Development (Use Package-Targeted)
```bash
# DON'T re-run baseline daily
# gbx:coverage:baseline scala  # ❌ 10 min wasted

# DO use package-targeted coverage
gbx:coverage:scala-package rasterx --open  # ✅ 2 min
```

### Before Major PR
```bash
# Fresh baseline for validation
gbx:coverage:baseline scala --open
gbx:coverage:baseline python --open

# Check all packages meet threshold
gbx:coverage:gaps scala --threshold 90
gbx:coverage:gaps python --threshold 90
```

## What Happens

### Scala Baseline
```bash
# Delegates to full coverage command
gbx:coverage:scala \
  --min-coverage 90 \
  --open

# Runs:
# 1. mvn clean package -DskipTests=false
# 2. All tests execute (~10 min)
# 3. Generates scoverage.xml
# 4. Creates HTML report
```

### Python Baseline
```bash
# Delegates to full coverage command  
gbx:coverage:python \
  --min-coverage 90 \
  --open

# Runs:
# 1. pytest with coverage (~30 sec)
# 2. Generates coverage.xml
# 3. Creates HTML report
```

## Output

### Scala
- **XML**: `target/scoverage.xml`
- **HTML**: `target/scoverage-report/index.html`
- **Valid for**: ~7 days

### Python
- **XML**: `python/coverage-report/coverage.xml`
- **HTML**: `python/coverage-report/index.html`
- **Valid for**: Always run (fast)

## Next Steps After Baseline

```bash
# 1. Analyze gaps
gbx:coverage:gaps scala
# Output: Shows packages below 90%

# 2. Target lowest package
gbx:coverage:scala-package vectorx --open
# ~2 min vs 10 min

# 3. Add tests, repeat
gbx:coverage:scala-package vectorx --open

# 4. View combined results (FREE)
gbx:coverage:scala --report-only --open
```

## Time Investment

### Scala
| Activity | Time | Frequency |
|----------|------|-----------|
| Baseline | 10 min | Weekly |
| Package-targeted | 2 min | Daily |
| Report-only | 5 sec | As needed |
| **Weekly total** | **20 min** | **(vs 50 min without strategy)** |

### Python
| Activity | Time | Frequency |
|----------|------|-----------|
| Baseline | 30 sec | Always (fast enough) |
| Report-only | 5 sec | Quick checks |

## Tips

### 1. Schedule Baseline Runs
```bash
# Monday morning
gbx:coverage:baseline scala --log baseline-monday.log --open

# Review over coffee
# Plan week's coverage improvements
```

### 2. Don't Over-Run Baseline
```bash
# Once per week is sufficient
# Data is valid for ~7 days
# Re-running daily wastes 40+ minutes per week
```

### 3. Combine with Gaps Analysis
```bash
# After baseline
gbx:coverage:baseline scala --open

# Immediate gap analysis
gbx:coverage:gaps scala

# Shows: Which packages need work this week
```

### 4. Python is Always Fast
```bash
# No need to save baseline strategy for Python
# Just run full coverage every time (~30 sec)
gbx:coverage:python --open
```

### 5. Use Report-Only Between Baselines
```bash
# Day 1: Baseline
gbx:coverage:baseline scala

# Day 2-7: Report-only (uses baseline data)
gbx:coverage:scala --report-only --open

# Day 8: New baseline
gbx:coverage:baseline scala
```

## Comparison with Other Commands

| Command | Tests Run | Time | Use Case |
|---------|-----------|------|----------|
| `baseline` | All tests | 10 min | Weekly reference |
| `scala-package` | Package tests | 2 min | Daily development |
| `--report-only` | No tests | 5 sec | Quick checks |

## Related Commands

- `gbx:coverage:scala` - Full Scala coverage (same as baseline scala)
- `gbx:coverage:python` - Full Python coverage (same as baseline python)
- `gbx:coverage:gaps <lang>` - Analyze baseline data
- `gbx:coverage:scala-package <pkg>` - Target specific package

## Strategy

See `.cursor/rules/coverage-strategy.mdc` for:
- Baseline + incremental pattern
- When to run baseline vs package-targeted
- Weekly coverage improvement workflow
- Time optimization strategies

## Key Insight

**Baseline is not about running coverage - it's about establishing a reference point.**

Use baseline weekly to:
- ✅ Know true state of coverage
- ✅ Enable gap analysis
- ✅ Validate incremental improvements
- ✅ Track progress over time

Don't use baseline:
- ❌ After every change
- ❌ For quick status checks
- ❌ Multiple times per day
