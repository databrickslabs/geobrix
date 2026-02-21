# Codecov badge and coverage uploads

## Badge (shield)

The README Codecov badge is:

- **Badge image:** `https://codecov.io/gh/databrickslabs/geobrix/branch/main/graph/badge.svg`
- **Project page:** https://codecov.io/gh/databrickslabs/geobrix

The badge updates when Codecov receives a new coverage upload for this repo.

## How coverage gets uploaded (tests run once)

1. **build main** (on push/PR): Runs Scala and Python tests **once** with coverage on the `larger` runner, uploads coverage as workflow artifacts, then a separate **codecov** job (lightweight, `ubuntu-latest`) downloads those artifacts and uploads to Codecov. So the slow Codecov step does not block the build job, and tests are never run twice.
2. **Upload coverage to Codecov** (manual): Actions → “Upload coverage to Codecov” → Run workflow. Use only to refresh the badge without a new push; it runs the full test suite again.

Both require the repo secret **CODECOV_TOKEN** (from Codecov → this repo → Settings → copy token, then GitHub repo → Settings → Secrets → Actions → add `CODECOV_TOKEN`).

## Report paths (where coverage is produced and uploaded)

| Source        | Path(s) |
|---------------|--------|
| Scala (scoverage) | `target/scoverage.xml`, `target/scoverage-report/scoverage.xml` |
| Python (pytest-cov) | `python/geobrix/coverage.xml` |

The build job stages files into `coverage-reports/`; the codecov job downloads that directory and passes it to Codecov (`directory: coverage-reports`).

If the badge does not update, confirm `CODECOV_TOKEN` is set and that the "Upload to Codecov" job in the Actions run succeeded (check the run logs).
