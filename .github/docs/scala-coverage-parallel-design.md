# Granular Scala coverage: packages in parallel + report assembly

## Goal

Speed up Scala coverage on CI by:

1. **Splitting tests by package** (rasterx, gridx, vectorx, ds, expressions, util) and running each in its own job in parallel.
2. **Assembling a single report** by merging per-package scoverage data, then uploading to Codecov.

## Why it helps

- **Current (single job):** One runner runs all tests with `-T 1C`. Wall time ≈ 30–45 min with scoverage.
- **Granular (matrix):** N jobs run in parallel (e.g. 6 packages). Wall time ≈ max(rasterx, gridx, …) + merge + upload, e.g. ~8–12 min if the slowest package is ~10 min and merge is ~1 min.
- **Cost:** More runner-minutes (6 jobs × ~10 min) but faster feedback.

## Approach

### 1. Matrix job per package

- **Strategy:** `matrix: package: [rasterx, gridx, vectorx, ds, expressions, util]`
- **Each job:**  
  `mvn -T 1C -q clean scoverage:test -Dsuites='com.databricks.labs.gbx.<package>.*' -Druntime=large -Dscalastyle.fail=false`  
  then upload `target/scoverage.xml` (or `target/scoverage-report/`) as artifact `scoverage-${{ matrix.package }}`.

### 2. Merge job

- **After:** All matrix jobs complete.
- **Steps:**
  1. Download all `scoverage-*` artifacts.
  2. Run merge script (see below) to produce a single `scoverage.xml` (and optionally HTML) from the per-package XMLs.
  3. Upload merged result to Codecov (and optionally keep as artifact).

### 3. Merge script

- **Input:** N scoverage XML files (one per package).
- **Logic:** For each statement identified by `(source, line, start, end)`, take **max(invocation-count)** across the N files (a line is “covered” if any package run hit it). Preserve structure (packages/classes/methods) from one “base” XML and update invocation counts from all files.
- **Output:** One `scoverage.xml` (and optionally run `scoverage:report-only` to get HTML, or a small report generator).

## When to use

- **Manual workflow** (e.g. “Upload coverage to Codecov” or “Scala coverage (parallel)”): Use this granular flow when you want full Scala coverage with faster wall time.
- **Main build:** Keep Scala coverage **off** in the main build (Python-only coverage) so CI stays ~10–15 min; use this workflow when you need Scala coverage.

## Implementation status

- **Design:** This doc.
- **Merge script:** `scripts/ci/merge_scoverage.py` (merges scoverage XMLs by statement identity, outputs one XML).
- **Workflow:** Optional `.github/workflows/codecov-scala-parallel.yml` (matrix by package + merge job + Codecov upload) can be added when desired.

## Cursor commands (local)

- `gbx:coverage:scala-package <pkg>` already runs one package at a time; no merge needed.
- For “all packages in parallel then report” locally, you could run multiple `gbx:coverage:scala-package` in parallel and then run the merge script on the resulting `target/scoverage.xml` from each (each run overwrites `target/`, so you’d run in separate dirs or copy after each run). That’s more involved; the main win is on CI with matrix jobs.
