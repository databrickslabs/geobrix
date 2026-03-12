# Codecov badge and coverage uploads

## Badge (shield)

The README Codecov badge is:

- **Badge image:** `https://codecov.io/gh/databrickslabs/geobrix/branch/main/graph/badge.svg`
- **Project page:** https://codecov.io/gh/databrickslabs/geobrix

The badge updates when Codecov receives a new coverage upload for this repo. This repo is public, so the default badge works.

**Private repos (e.g. forks):** If the README badge does not update after a successful upload, the repo may be private. Codecov’s default badge is for public repos; for private repos you may need to install the [Codecov GitHub App](https://docs.codecov.com/docs/github-app) or use a [token in the badge URL](https://docs.codecov.com/docs/adding-the-codecov-badge). The upload itself can still succeed; only the public badge display may be restricted.

## How coverage gets uploaded (tests run once)

1. **build main** (on push/PR): Runs Scala and Python tests **once** with coverage on the `larger` runner, uploads coverage as workflow artifacts, then a separate **codecov** job (lightweight, `ubuntu-latest`) downloads those artifacts and uploads to Codecov. So the slow Codecov step does not block the build job, and tests are never run twice.
2. **Upload coverage to Codecov** (manual): Actions → “Upload coverage to Codecov” → Run workflow. Use only to refresh the badge without a new push; it runs the full test suite again.

Both require the repo secret **CODECOV_TOKEN**. If you see **"Token required - not valid tokenless upload"** in the Codecov step, the secret is missing or empty.

**To fix:** In [Codecov](https://codecov.io), open this repo → Settings → General → copy the **Upload token** (or use the GitHub App). In GitHub: repo **Settings → Secrets and variables → Actions → New repository secret** → name `CODECOV_TOKEN`, value = the Codecov token. Save. Re-run the workflow or push a commit.

## Report paths (where coverage is produced and uploaded)

| Source        | Path(s) |
|---------------|--------|
| Scala (scoverage) | `target/scoverage.xml`, `target/scoverage-report/scoverage.xml` |
| Python (pytest-cov) | `python/geobrix/coverage.xml` |

The build job stages files into `coverage-reports/`; the codecov job downloads that directory and passes it to Codecov (`directory: coverage-reports`).

If the badge does not update: (1) Confirm `CODECOV_TOKEN` is set (see above); (2) Confirm the "Upload to Codecov" job succeeded (no "Token required" error); (3) For private repos, the badge may still need the [Codecov GitHub App](https://docs.codecov.com/docs/github-app) or a [badge token](https://docs.codecov.com/docs/adding-the-codecov-badge).
