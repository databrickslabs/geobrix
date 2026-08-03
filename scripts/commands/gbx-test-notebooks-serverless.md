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
- `--set-var NAME=VALUE` – Inject a Python assignment into the **uploaded** notebook copy, immediately after the `%run`/config cell, so a validation run forces compute steps the committed notebook would skip. Example series notebooks gate expensive reader/`rst_*` rebuilds on `do_overwrite=FORCE_REBUILD` with `FORCE_REBUILD=False` committed — so if the output tables already exist, a default run skips the very logic under test. `--set-var FORCE_REBUILD=True` overrides that in the run only; the committed notebook keeps its safe default. `VALUE` is inlined as a Python literal. Repeatable; applies to every submitted notebook.
- `--ws-dir WSPATH` – Workspace folder to import notebooks into (default: `/Users/<current-user>/GeoBrix/serverless-run`).
- `--extras CSV` – Geobrix extras whose deps to install (default: `light,stac,vizx,overture`).
- `--extra-deps CSV` – Additional pip requirements (comma-separated).
- `--wheel VOLPATH` – Volume path to the geobrix wheel.
- `--env-version VER` – Serverless environment version (default: `5`).
- `--profile PROFILE` – Databricks config profile (default: `oauth-fe` or `DATABRICKS_CONFIG_PROFILE`).
- `--poll-secs N` – Polling interval in seconds (default: `20`).
- `--no-strip-pip` – Do NOT strip `%pip`/`%restart_python` cells from uploaded notebooks.
- `--log <path>` – Write output to log file (filename → `test-logs/<name>`).
- `--help` – Help and examples.

## Default behavior

Without `--notebook` or `--dir`, runs all four Helios example notebooks
(`notebooks/examples/helios/0*.ipynb`) with `extras=light,stac,vizx,overture`
and `--extra-deps rich`.

## Examples

```bash
# Run all four Helios notebooks on Serverless (default)
bash scripts/commands/gbx-test-notebooks-serverless.sh

# Run a single notebook with only the light extra
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook 'notebooks/examples/helios/01. Vector Engine (MVT).ipynb' \
  --extras light
```
