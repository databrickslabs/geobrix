# Codecov badge and coverage uploads

## Badge (shield)

The README Codecov badge is:

- **Badge image:** `https://codecov.io/gh/databrickslabs/geobrix/branch/main/graph/badge.svg`
- **Project page:** https://codecov.io/gh/databrickslabs/geobrix

The badge updates when Codecov receives a new coverage upload for this repo.

## How coverage gets uploaded

1. **build main** (on push/PR): Runs Scala and Python tests with coverage, then uploads to Codecov.
2. **Upload coverage to Codecov** (manual): Actions → “Upload coverage to Codecov” → Run workflow. Use this to refresh the badge without waiting for a push.

Both require the repo secret **CODECOV_TOKEN** (from Codecov → this repo → Settings → copy token, then GitHub repo → Settings → Secrets → Actions → add `CODECOV_TOKEN`).

## Report paths (where the workflows look for coverage)

| Source        | Path(s) |
|---------------|--------|
| Scala (scoverage) | `target/scoverage.xml`, `target/scoverage-report/scoverage.xml` |
| Python (pytest-cov) | `python/geobrix/coverage.xml` |

- **build main** and **Upload coverage to Codecov** upload all of the above (Scala + Python).
- **build_scala** uploads only the Scala paths.

If the badge does not update, confirm `CODECOV_TOKEN` is set and that the upload step in the workflow run succeeded (check the Actions run logs).
