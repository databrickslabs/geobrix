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
- `--pip-wheel VOLPATH` – In the classic modes (`%pip` kept), point every `%pip install ... @ file:///Volumes/....whl` line of the uploaded copy at this wheel, for example the env6 wheel of an isolated staging folder. The committed notebook is unchanged.
- `--set-var NAME=VALUE` – Inject a Python assignment into the **uploaded** notebook copy, right after the last cell that assigns that name (so it also wins over a series notebook's own parameter cell), else after the `%run`/config cell, so a validation run forces compute steps the committed notebook would skip. Example series notebooks gate expensive reader/`rst_*` rebuilds on `do_overwrite=FORCE_REBUILD` with `FORCE_REBUILD=False` committed — so if the output tables already exist, a default run skips the very logic under test. `--set-var FORCE_REBUILD=True` overrides that in the run only; the committed notebook keeps its safe default. `VALUE` is inlined as a Python literal. Repeatable; applies to every submitted notebook.
- `--ws-dir WSPATH` – Workspace folder to import notebooks into (default: `/Users/<current-user>/GeoBrix/serverless-run`).
- `--extras CSV` – Geobrix extras whose deps to install (default: `stac,vizx`). With an env wheel (`geobrix-<version>+env5` / `+env6`, the Databricks install) these are that wheel's extras, read from `python/geobrix/envs/env<N>/extra-<name>.txt`: `databricks`, `stac`, `vizx`, and on env6 `zarr`. A `light_*` name is refused (the env wheel needs no light extra). So is an extra the env wheel does not have, such as `overture` or `earthdata` (pass their packages with `--extra-deps`). With the plain wheel they are read from `pyproject.toml`.
- `--extra-deps CSV` – Additional pip requirements (comma-separated; a specifier keeps its own commas, so `rich,overturemaps>=1.0.2,<2` is two requirements).
- `--wheel VOLPATH` – Volume path to the geobrix wheel (default: the env6 wheel, `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env6-py3-none-any.whl`; stage it with `gbx:data:stage-wheel --env 6`). An env wheel must match `--env-version` (the env5 wheel with `--env-version 5`): the other environment's wheel refuses to import, so the runner stops before submitting.
- On `--new-cluster`, `--wheel` is also installed as a library of the cluster, and its environment must match the runtime (DBR 17.3 and 18: env5, DBR 19: env6).
- With `--hardware-accelerator`, pass the env6 wheel and `--env-version 6`: the UDF workers of Serverless GPU run environment 6. An env5 wheel prints a warning.
- `--no-wheel-guard` – Skip the wheel guard. By default each uploaded notebook gets a guard cell after its install that fails the run, with a Fix, when the driver or a Python worker imports another geobrix build (commit, source digest, module path printed) than the staged wheel: the wheel the notebook's own `%pip` installs, else `--wheel`. On a classic cluster (`--existing-cluster-id`, `--pool`, `--new-cluster`) a forced install of the wheel (`--force-reinstall --no-deps`, geobrix only) is added before each geobrix `%pip` line of the uploaded copy, since a same-version cluster library makes a plain install a no-op. On an existing cluster with no `--wheel` and no geobrix `%pip` cell the guard only reports. The committed notebook is never changed.
- `--no-wheel` – Install no geobrix wheel and no `--extras`; only `--extra-deps` are installed. Use it for a probe notebook that runs on plain pyspark. Exclusive with `--wheel`. On `--new-cluster` the cluster gets no wheel library.
- `--env-version VER` – Serverless environment version (default: `6`).
- `--existing-cluster-id ID` – Run on an existing (warm) classic cluster instead of Serverless; the notebook's `%pip` cells are kept and install its deps (`--extras`/`--wheel`/`--env-version` do not apply).
- `--pool NAME`, `--pool-volume VOLROOT` – Run on an idle cluster leased from the pool NAME (`gbx:bench:pool up --name NAME`) instead of Serverless. There is no cluster start, only one run per cluster at a time. An ended cluster is started, and `--jar` (or none) is made the cluster's jar. `%pip` cells are kept. Exclusive with `--new-cluster`, `--existing-cluster-id` and `--hardware-accelerator`. See `gbx-bench-pool.md`.
- `--new-cluster [SPARK_VERSION]` – Run each notebook on a classic job cluster made for its run, which Databricks terminates when the run ends, so there is nothing to delete. An optional Databricks Runtime (e.g. `19.x-scala2.13`) sets the runtime, as `--spark-version` does. The notebook's `%pip` cells are kept and install its deps (`--extras`/`--wheel`/`--env-version`/`--hardware-accelerator` do not apply); the task has `max_retries=0`. Exclusive with `--existing-cluster-id` and `--hardware-accelerator`.
  - `--jar VOLPATH` – Install this JAR (a `/Volumes` path) as a library of the cluster, so a notebook can run the heavyweight functions. The notebook's `%pip` still installs the env wheel. Without `--jar` the task has no library. Only `--new-cluster` and `--jar-task` accept it.
  - `--access-mode dedicated|standard` – `dedicated` (default) is single-user for the caller; `standard` is `USER_ISOLATION`.
  - `--spark-version VER` (default `19.x-scala2.13`), `--node-type TYPE` (default `m5d.xlarge`, x86_64), `--num-workers N` (default `1`; `0` makes a single node cluster).
  - `--docker-image URL` – Start the cluster from a Databricks Container Service image, for example `mjohns797/geobrix:0.6.0-env6-x86` (match `--node-type`'s architecture). It sets `spark.databricks.unityCatalog.volumes.enabled true`. The image carries the wheel and the JAR, so the run installs no `--wheel` and refuses `--jar`.
  - Runs submitted at the same time need their own `--ws-dir`. A job reads its notebook when it starts, so two launches of one notebook into one folder can run each other's copy.
- `--profile PROFILE` – Databricks config profile (default: `oauth-fe` or `DATABRICKS_CONFIG_PROFILE`).
- `--poll-secs N` – Polling interval in seconds (default: `20`).
- `--timeout-min N` – Time limit of each run in minutes (default: `60`); the Jobs service ends a run that outlives it. The command runs from a snapshot of its files and copies each notebook at launch. An edit during the run changes nothing (`GBX_NO_SNAPSHOT=1` runs in place).
- `--no-strip-pip` – Do NOT strip `%pip`/`%restart_python` cells from uploaded notebooks.
- `--jar-task MAIN`, `--jar VOLPATH`, `--jar-args CSV` (`--jar` also serves `--new-cluster`, above) – Run ONE Serverless JAR task instead of notebooks. The task is `spark_jar_task(main_class_name=MAIN, parameters=<the CSV>)`. The JAR (a `/Volumes` path) is the environment's `java_dependencies`, which is the Serverless JAR task form (environment version 4 or later; `--env-version`, default `6`). It is submitted once (`max_retries=0`, auto-optimization retries off), and the run's state message and the end of its output are printed. It runs alone: no `--notebook`, `--dir`, `--existing-cluster-id` or `--new-cluster`.
- `--python-task PY`, `--python-args CSV`, `--extra-jar VOLPATH` – Run ONE `spark_python_task` instead of notebooks. The local `.py` file is imported into `--ws-dir` as a workspace file. It runs on a new job cluster (`--new-cluster` is required; `--python-args` are its parameters). The cluster installs `--wheel`, `--jar` and each `--extra-jar` (repeatable) as libraries before the file runs, so the file can call Scala code on the same session through `spark._jvm`. Pass the env wheel that fits the runtime (`+env5` for DBR 17.3 and 18). The run is submitted once (`max_retries=0`), and the task's stdout, state message and cluster id are printed. It runs alone: no `--notebook`, `--dir`, `--existing-cluster-id` or `--hardware-accelerator`.
- `--harvest-outputs` – After each successful run, export it (`databricks jobs export-run`) and write its cell outputs back into the local `.ipynb`: cells are matched by source (stripped `%pip` cells and injected `--set-var` cells are skipped), only outputs/`execution_count` change, and the file's JSON serialization is preserved. A requested harvest that fails fails the run.
- `--log <path>` – Write output to log file (filename → `test-logs/<name>`).
- `--help` – Help and examples.

## Interpreter

The runner needs `databricks-sdk` and Python 3.11 or newer (`tomllib`). It runs on `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`) when that venv is Python 3.11+ and can import the SDK. The CI-lock venv from `gbx:venv:sync` cannot. Otherwise it runs on `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter (Python 3.12 when there is no usable venv). Otherwise it runs on system `python3` if that is 3.11+ and has the SDK. Otherwise it stops with a hint to install uv. The SDK version is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`). The choice is the shared `gbx_python_with_sdk` helper in `common.sh`, which `gbx:data:stage-jar`, `gbx:data:stage-wheel`, `gbx:bench:cluster`, `gbx:test:bundle-databricks` and `gbx:test:primitive-databricks` use too.

## Each run is submitted once

Every `jobs.submit` carries an `idempotency_token` (one fresh token per notebook run, shared by every retry of that submit). A lost response therefore cannot leave a second, orphan run. A transient submit failure (network error, 429/5xx) is retried with the same token and the server returns the run it already created. The helper is `examples/tests/_job_submit.py`; every launcher in `examples/tests/` goes through it.

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

# A plain-pyspark probe: no geobrix wheel is installed
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook examples/tests/probe_arrow_empty_chunks.ipynb --no-wheel

# Run a probe notebook on a DBR 19 standard-access job cluster (terminated with its run)
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook examples/tests/probe_proj_data_access.ipynb \
  --new-cluster --access-mode standard \
  --ws-dir /Users/me@example.com/GeoBrix/probe-standard

# Run nb04 with output harvesting, forcing compute with FORCE_RELOAD=True
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --dep-notebook examples/notebooks/wireless-coverage/config_nb.ipynb \
  --notebook examples/notebooks/wireless-coverage/04_tower_viewsheds.ipynb \
  --set-var FORCE_RELOAD=True \
  --harvest-outputs \
  --extras vizx
```
