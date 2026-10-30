# Run Serverless Notebook Tests

Imports local `.ipynb` notebooks into the Databricks workspace (stripping `%pip`/`%restart_python` cells — they fail in Serverless JOB compute), then submits each as a one-time Serverless notebook job via `jobs.submit`, polling to terminal and stopping on the first failure. Dependencies are injected via the Serverless environment spec (not `%pip`).

---

## Usage

```bash
bash scripts/commands/gbx-test-notebooks-serverless.sh [OPTIONS]
```

## Options

- `--notebook PATH` – Local `.ipynb` file to run (repeatable).
- `--dir DIR` – Directory of `.ipynb` files to run (all `*.ipynb`, sorted).
- `--dep-notebook PATH` – Local `.ipynb` to import into `--ws-dir` but NOT submit (e.g. a shared `config_nb.ipynb` referenced via `%run ./config_nb`). Repeatable.
- `--set-var NAME=VALUE` – Inject a Python assignment into the **uploaded** notebook copy, right after the last cell that assigns that name (so it also wins over a series notebook's own parameter cell), else after the `%run`/config cell, so a validation run forces compute steps the committed notebook would skip. Example series notebooks gate expensive reader/`rst_*` rebuilds on `do_overwrite=FORCE_REBUILD` with `FORCE_REBUILD=False` committed — so if the output tables already exist, a default run skips the very logic under test. `--set-var FORCE_REBUILD=True` overrides that in the run only; the committed notebook keeps its safe default. `VALUE` is inlined as a Python literal. Repeatable; applies to every submitted notebook.
- `--ws-dir WSPATH` – Workspace folder to import notebooks into (default: `/Users/<current-user>/GeoBrix/serverless-run`).
- `--extras CSV` – Geobrix extras whose deps to install (default: `stac,vizx`). With an env wheel (`geobrix-<version>+env5` / `+env6`, the Databricks install) these are that wheel's extras, read from `python/geobrix/envs/env<N>/extra-<name>.txt`: `databricks`, `stac`, `vizx`, and on env6 `zarr`. A `light_*` name is refused (the env wheel needs no light extra), and so is an extra the env wheel does not have, such as `overture` or `earthdata` (pass their packages with `--extra-deps`). With the plain wheel they are read from `pyproject.toml`.
- `--extra-deps CSV` – Additional pip requirements (comma-separated; a specifier keeps its own commas, so `rich,overturemaps>=1.0.2,<2` is two requirements).
- `--wheel VOLPATH` – Volume path to the geobrix wheel (default: the env6 wheel, `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env6-py3-none-any.whl`; stage it with `gbx:data:stage-wheel --env 6`). An env wheel must match `--env-version` (the env5 wheel with `--env-version 5`): the other environment's wheel refuses to import, so the runner stops before submitting.
- `--env-version VER` – Serverless environment version (default: `6`).
- `--new-cluster SPARK_VERSION` – Run each notebook on a classic job cluster of that Databricks Runtime (e.g. `19.x-scala2.13`), created for the run and terminated when it ends, so there is nothing to delete; dedicated (single user) access mode, one worker. The notebook's own `%pip` cells are kept (classic clusters install through them), so `--extras`, `--wheel`, `--env-version` and `--hardware-accelerator` do not apply. Exclusive with `--existing-cluster-id`.
- `--node-type NODE_TYPE` – The `--new-cluster` job cluster's node type (default: `i3.xlarge`).
- `--profile PROFILE` – Databricks config profile (default: `oauth-fe` or `DATABRICKS_CONFIG_PROFILE`).
- `--poll-secs N` – Polling interval in seconds (default: `20`).
- `--no-strip-pip` – Do NOT strip `%pip`/`%restart_python` cells from uploaded notebooks.
- `--harvest-outputs` – After each successful run, export it (`databricks jobs export-run`) and write its cell outputs back into the local `.ipynb`: cells are matched by source (stripped `%pip` cells and injected `--set-var` cells are skipped), only outputs/`execution_count` change, and the file's JSON serialization is preserved. A requested harvest that fails fails the run.
- `--log <path>` – Write output to log file (filename → `test-logs/<name>`).
- `--help` – Help and examples.

## Interpreter

The runner needs `databricks-sdk` and Python 3.11 or newer (`tomllib`). It runs on `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`) when that venv is Python 3.11+ and can import the SDK (the CI-lock venv from `gbx:venv:sync` cannot), otherwise on `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter (Python 3.12 when there is no usable venv), otherwise system `python3` if it is 3.11+ and has the SDK, otherwise it stops with a hint to install uv. The SDK version is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`). The choice is the shared `gbx_python_with_sdk` helper in `common.sh`, which `gbx:data:stage-jar`, `gbx:data:stage-wheel`, `gbx:bench:cluster`, `gbx:test:bundle-databricks` and `gbx:test:primitive-databricks` use too.

## Each run is submitted once

Every `jobs.submit` carries an `idempotency_token` (one fresh token per notebook run, shared by every retry of that submit), so a lost response cannot leave a second, orphan run. A transient submit failure (network error, 429/5xx) is retried with the same token and the server returns the run it already created. The helper is `examples/tests/_job_submit.py`; every launcher in `examples/tests/` goes through it.

## Default behavior

Without `--notebook` or `--dir`, runs all four Helios example notebooks
(`examples/notebooks/helios/0*.ipynb`) on the env6 wheel with `extras=stac,vizx`
and `--extra-deps rich,overturemaps>=1.0.2,<2` (the Overture CLI fast path the notebooks use).

## Examples

```bash
# Run all four Helios notebooks on Serverless (default)
bash scripts/commands/gbx-test-notebooks-serverless.sh

# Run a single notebook with only the vizx extra
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook 'examples/notebooks/helios/01. Vector Engine (MVT).ipynb' \
  --extras vizx

# A Serverless environment 5 notebook: the env5 wheel
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook examples/notebooks/orthomosaic/02_publish.ipynb \
  --wheel /Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env5-py3-none-any.whl \
  --env-version 5 --extras vizx

# The same notebook on classic DBR 19 (a job cluster that ends with the run; the notebook's %pip installs)
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook examples/notebooks/h3-rasterize/h3_rasterize_isobands.ipynb \
  --new-cluster 19.x-scala2.13

# Run nb04 with output harvesting, forcing compute with FORCE_RELOAD=True
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --dep-notebook examples/notebooks/wireless-coverage/config_nb.ipynb \
  --notebook examples/notebooks/wireless-coverage/04_tower_viewsheds.ipynb \
  --set-var FORCE_RELOAD=True \
  --harvest-outputs \
  --extras vizx
```
