# Audit pinned Python tooling versions

Inventories every place a pinned package version lives across CI, the dev container Dockerfile, the Databricks cluster init script, and the lint venv setup. Use it to **detect drift** between locations, and as the entry point for **bumping pins to a new DBR LTS**.

## Usage

```bash
bash scripts/commands/gbx-versions-audit.sh [OPTIONS]
```

## Options

- `--package <name>` — Show only one package (e.g. `pip`, `numpy`, `pyspark`).
- `--log <path>` — Write output to log file.
- `--help` — Display help.

## Examples

```bash
# Full audit (all tracked packages, all locations)
gbx:versions:audit

# Just numpy
gbx:versions:audit --package numpy

# Just pyspark — useful when bumping Spark
gbx:versions:audit --package pyspark
```

## What's tracked

Source of truth (priority order):

1. **DBR LTS release notes** — for any package shipped with the runtime:
   - Core build/runtime: `pip`, `setuptools`, `wheel`, `cython`, `numpy`, `pandas`, `pyspark`/`spark`, `requests`, `python`.
   - Jupyter / notebook stack (dev container): `ipython`, `ipykernel`, `ipywidgets`, `jupyterlab`, `jupyter_server`, `jupyter_core`, `jupyter_client`, `tornado`, `nbformat`, `nbconvert`.
2. **Current PyPI stable** — for tools NOT in DBR: `build`, `pytest-cov`, `geopandas` (pinned to be compatible with DBR's pandas 2.2.x).
3. **CI matrix** — for tools intentionally divergent from DBR: `pytest 8.4.2` (kept ahead of DBR 8.3.5 because tests don't run in DBR).
4. **System apt + ubuntugis PPA** — for GDAL native + Python bindings; auto-detected from `gdal-config --version`.
5. **Build tool** — `maven`: the Maven version the wrapper pins for CI (`.mvn/wrapper/maven-wrapper.properties`), the dev and local-CI images' `MAVEN_VERSION`, and the pom's enforcer floor. `scripts/ci/test_maven_wrapper.py` fails when they differ.
6. **Intentional DBR divergence** — `pyzmq 25.1.1` (Dockerfile only). DBR has 26.2.0 but our Jupyter ZMQ workaround requires the older pin. Re-evaluate on next DBR LTS bump.
7. **Zarr extra** — `zarr` (`--package zarr`): the `[zarr_dbr19]` pins in `python/geobrix/pyproject.toml` (`zarr`, `numcodecs`, `zarrs`, `tensorstore`, `obstore`, `ml_dtypes`, `msgspec`, `donfig`). None is preinstalled on DBR 19, Serverless env 6 or DBR 19 ML, so they follow the Zarr runtime-deps report, not a DBR release-notes table.

## Files audited

- `.github/actions/scala_build/action.yml`
- `.github/actions/python_build/action.yml`
- `.github/workflows/build_*.yml` and `codecov-*.yml` (matrix values)
- `scripts/docker/Dockerfile`
- `scripts/ci-local/Dockerfile.gha-runner`, `.mvn/wrapper/maven-wrapper.properties`, `pom.xml` (the Maven pin)
- `scripts/geobrix-gdal-init.sh`
- `scripts/commands/gbx-lint-python.sh`
- `python/geobrix/pyproject.toml` (dev/test extras — currently unpinned)
- `examples/tests/requirements.txt` (lower bounds — not pinned)

If a new file pins versions, add it to the `FILES` array in `gbx-versions-audit.sh`.

## Workflow: bumping to a new DBR LTS

1. Get the DBR LTS release-notes URL (e.g. `https://docs.databricks.com/aws/en/release-notes/runtime/18.3lts`).
2. Run `gbx:versions:audit` and capture the output.
3. Ask Claude:
   > "Bump pinned versions to DBR 18.3 LTS from `<URL>`. Here's the current audit: `<paste>`. Favor DBR versions where listed; pin packages not in DBR (`build`, `pytest-cov`) to current PyPI stable; keep `pytest` at the current CI matrix value (we don't run tests in DBR)."
4. Re-run `gbx:versions:audit` to verify everything moved together.
5. Commit: `chore(deps): align pins to DBR 18.3 LTS`.

## Notes

- The audit is **read-only** — never modifies files. All edits happen via Claude after you review the audit.
- Pyproject.toml dev/test extras (`isort`, `black`, `flake8`, `build`, `pytest`, etc.) are intentionally unpinned to allow `pip install -e .[dev]` to resolve cleanly. The hard pins live in CI/Docker/init script — those are the trust boundaries.
- The audit prints a "Files audited" section at the end with `✓`/`✗` markers — a `✗` means a tracked file is missing (e.g. renamed/deleted). Update `FILES` in the script to fix.
