# Audit pinned Python tooling versions

Inventories every place a pinned package version lives across CI, the dev container Dockerfile, the Databricks cluster init script, and the lint venv setup. Use it to **detect drift** between locations, and as the entry point for **bumping pins to a new DBR LTS**.

## Usage

```bash
bash scripts/commands/gbx-versions-audit.sh [OPTIONS]
```

## Options

- `--package <name>` — Show only one package (e.g. `pip`, `numpy`, `pyspark`).
- `--native` — Audit every consumer of the native pins instead of printing the package inventory (see "Native pins" below).
- `--host` — Check the host venv AND this checkout's dev container against the native pins.
- `--check` — With `--native` or `--host`: exit 1 on drift. Without it the report is printed and the exit is 0.
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

## Native pins

`scripts/native/pins.conf` is the one place GeoBrix pins GDAL, PROJ, GEOS, every library they are built from, the Rust toolchain (with `rust-toolchain.toml`), maturin, cargo-deny and the Python bindings (rasterio, pyogrio, pyproj, shapely, netCDF4). What feeds no GDAL build is in `scripts/native/rust-pins.conf` (PyO3, the georust crates, the rust-native stage's libclang), so a bump there never rebuilds GDAL. Each source has its version, URL, second URL (or `single-source`), file name, SHA-256 and SPDX licence. The sums were identical from two sources, or compared with the project's published checksum, when they were resolved.

```bash
# every consumer against the pins (CI-style: exit 1 on drift)
bash scripts/commands/gbx-versions-audit.sh --native --check

# the host venv and the dev container against the pins: Python patch version, the bound
# packages, and GDAL/PROJ/GEOS as rasterio, pyproj and shapely report them
bash scripts/commands/gbx-versions-audit.sh --host --check
```

`--native` runs `python3 scripts/native/pins.py audit`: the pin files themselves (`pins.conf` and `rust-pins.conf`: no placeholder, every `X_VERSION` has its checksum, and no Rust pin in `pins.conf`, where every value is the GDAL build's cache key), the `rust-toolchain.toml` channel, every `Cargo.toml` that depends on `pyo3` (`=PYO3_VERSION`), every `Cargo.lock` against the `CRATE_<name>_VERSION` pins (a locked crate at another version, and a pin no lock uses), every maturin `pyproject.toml` (`maturin==MATURIN_VERSION`), `scripts/native/build-requirements.in`, the `gdal.java.version` property in `pom.xml` when present, and the legacy consumers still on an older GDAL (`scripts/native/legacy-consumers.txt`: a changed value is drift; a regex that no longer matches means the consumer was retired, so delete its line). `--host` reuses the collector and comparison of `gbx:venv:check`; it is red while the environments still carry the previous GDAL stack. `pins.py get KEY` and `pins.py build-args` read the pins for scripts and Docker builds.

## What's tracked

Source of truth (priority order):

1. **DBR LTS release notes** — for any package shipped with the runtime:
   - Core build/runtime: `pip`, `setuptools`, `wheel`, `cython`, `numpy`, `pandas`, `pyspark`/`spark`, `requests`, `python`.
   - Jupyter / notebook stack (dev container): `ipython`, `ipykernel`, `ipywidgets`, `jupyterlab`, `jupyter_server`, `jupyter_core`, `jupyter_client`, `tornado`, `nbformat`, `nbconvert`.
2. **Current PyPI stable** — for tools NOT in DBR: `build`, `pytest-cov`, `geopandas` (pinned to be compatible with DBR's pandas 2.2.x).
3. **CI matrix** — for tools intentionally divergent from DBR: `pytest 8.4.2` (kept ahead of DBR 8.3.5 because tests don't run in DBR).
4. **System apt + ubuntugis PPA** — for GDAL native + Python bindings; auto-detected from `gdal-config --version`.
5. **Intentional DBR divergence** — `pyzmq 25.1.1` (Dockerfile only). DBR has 26.2.0 but our Jupyter ZMQ workaround requires the older pin. Re-evaluate on next DBR LTS bump.

## Files audited

- `.github/actions/scala_build/action.yml`
- `.github/actions/python_build/action.yml`
- `.github/workflows/build_*.yml` and `codecov-*.yml` (matrix values)
- `scripts/docker/Dockerfile`
- `scripts/geobrix-gdal-init.sh`, `scripts/build-gdal-artifacts.sh`, `scripts/docker/dcs/Dockerfile`, `pom.xml`
- `scripts/native/pins.conf`, `scripts/native/rust-pins.conf`, `rust-toolchain.toml`
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
