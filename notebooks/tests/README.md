# Notebook tests

Tests that validate structure and execution of notebooks under `notebooks/`. The folder structure under `notebooks/tests/` mirrors the notebook hierarchy (e.g. `notebooks/sample-data/` → `notebooks/tests/sample-data/`).

## Default: cell-by-cell (no kernel)

**gbx:test:notebooks** runs notebooks **cell-by-cell** by default (no Jupyter kernel). It discovers `notebooks/tests/fixtures/*.ipynb` and `notebooks/sample-data/*.ipynb`, executes each code cell with `exec()`, and reports per-cell status. Magic-only cells (e.g. `%pip install`) are skipped.

- No `--path`: run all default notebooks (fixtures + sample-data).
- `--path sample-data`: run only notebooks under `notebooks/sample-data/`.
- `--path fixtures`: run only `notebooks/tests/fixtures/*.ipynb`.
- `--path test_notebook_via_script.py`: run **pytest** for that test file instead.

Verbosity: `GBX_NOTEBOOK_VERBOSITY=quiet|truncated|full` (see table below).

## Options for testing notebooks (when ZMQ/kernel is broken)

| Approach | Tool | Needs kernel? | What you get |
|----------|------|----------------|--------------|
| **Script conversion** | `nbconvert --to script` + `python script.py` | **No** | Smoke test: does the notebook code run without error? No cell-level asserts. |
| **Testbook** | testbook | Yes (ZMQ) | Run cells, `tb.ref("var")`, full fidelity. Fails if kernel doesn’t respond. |
| **Structure only** | nbformat | No | Check file exists, has code cells, contains expected strings. No execution. |

**If the Jupyter kernel never responds in Docker (ZMQ handshake timeout):** use the **script conversion** path so you can still test that notebook code runs. See `test_notebook_via_script.py` and the helper `run_notebook_as_script()` / `notebook_to_script()`.

## Basic testbook example

- **Notebook:** `notebooks/tests/fixtures/minimal.ipynb` (one code cell: `x = 1 + 1`).
- **Tests:** `notebooks/tests/test_basic_testbook.py`
  - `test_minimal_notebook_exists` — checks the file exists (no kernel).
  - `test_minimal_notebook_executes_with_testbook` — runs the notebook with testbook and asserts `x == 2` (requires working kernel).

Run only this example:

```bash
bash .cursor/commands/gbx-test-notebooks.sh --path test_basic_testbook.py
```

## No-kernel option: run notebook as script

**Tests:** `notebooks/tests/test_notebook_via_script.py`

- Converts a notebook to a Python script with nbconvert, then runs it with `python` (no Jupyter kernel).
- `test_minimal_notebook_runs_as_script` — converts `fixtures/minimal.ipynb` to `.py` and runs it; passes if exit code is 0.
- Reusable helpers: `notebook_to_script(notebook_path, out_dir)` and `run_notebook_as_script(notebook_path, cwd, timeout)`.

Use this when the kernel is broken so you can still smoke-test notebook code:

```bash
bash .cursor/commands/gbx-test-notebooks.sh --path test_notebook_via_script.py
```

To test another notebook, call `run_notebook_cell_by_cell(path_to_ipynb, cwd=repo_root)` (or `run_notebook_as_script(...)`) in a new test.

**Verbosity** (env `GBX_NOTEBOOK_VERBOSITY`):

| Value | Output |
|-------|--------|
| `quiet` | Notebook name and status only (e.g. `minimal.ipynb ... PASSED`). |
| `truncated` (default) | Notebook name; per cell: label source/result as `(full)` or `(truncated)`, then print actual content if (full) or truncated content (300 chars) if (truncated). |
| `full` | Full notebook contents, full cell source, full execution result per cell. |

Example: `GBX_NOTEBOOK_VERBOSITY=full bash .cursor/commands/gbx-test-notebooks.sh --path test_notebook_via_script.py`

## Run tests (Docker — required)

Notebook tests **must** run inside the `geobrix-dev` Docker container. From **repo root**:

```bash
# Default: cell-by-cell run of fixtures + sample-data notebooks
bash .cursor/commands/gbx-test-notebooks.sh

# Only sample-data notebooks
bash .cursor/commands/gbx-test-notebooks.sh --path sample-data

# Run pytest for a specific test file
bash .cursor/commands/gbx-test-notebooks.sh --path test_notebook_via_script.py
```

## Kernel timeouts in Docker

If the Jupyter kernel does not become ready in time (e.g. in Docker), **execution** tests **fail** (they are not skipped). Fix kernel startup or environment so the suite passes.

## Requirements (inside container)

- Python 3.11+
- GeoBrix: `pip install -e python/geobrix`
- Test deps: `pytest`, `nbformat`, `nbconvert`, **testbook**

The gbx-test-notebooks script installs these if needed.

## Markers

- **integration**: Full notebook run (downloads data). Skip in fast CI; run when you need to verify end-to-end execution.

## Test bundle on a live Databricks cluster

You can test the Essential sample-data bundle against a **real Databricks workspace** (Unity Catalog Volumes) in two ways.

### 1. Local test (bundle runs on your machine, uploads via SDK)

The bundle code runs locally; uploads go to the configured Volume via the Databricks SDK (`WorkspaceClient`). No code runs on a cluster.

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (or `DATABRICKS_CONFIG_PROFILE`) and `GBX_BUNDLE_VOLUME_CATALOG`, `GBX_BUNDLE_VOLUME_SCHEMA`, `GBX_BUNDLE_VOLUME_NAME`.
3. Ensure the Volume exists in the workspace. Install: `pip install -e python/geobrix[databricks]` (or `pip install databricks-sdk`). For the full Essential bundle (Sentinel-2, etc.) also install `requests`, `pystac-client`, `planetary-computer`.
4. Run the test (from repo root, or with `python -m pytest` from `notebooks/tests/`):

   ```bash
   pytest notebooks/tests/test_bundle_on_databricks.py -v
   ```

   Or run the bundle directly:

   ```bash
   python notebooks/tests/test_bundle_on_databricks.py
   ```

If the env is not set, the test is **skipped** (no failure).

### 2. Push and run on cluster (job runs on a Databricks cluster)

Use **push_and_run_bundle_on_cluster.py** to upload a small runner notebook to the workspace and run it as a **one-off job** on a configured cluster. The cluster must have GeoBrix installed (e.g. as a cluster library), or you can set `GBX_BUNDLE_WHEEL_VOLUME_PATH` so the script builds a wheel, uploads it to a Volume, and the runner notebook installs it first.

1. Set the same env as above, plus `CLUSTER_ID` (existing cluster id).
2. Optional: `GBX_BUNDLE_WHEEL_VOLUME_PATH=/Volumes/main/default/geobrix_samples/wheels/geobrix.whl` so the script builds and uploads a wheel; the runner will `%pip install` it.
3. Run:

   ```bash
   python notebooks/tests/push_and_run_bundle_on_cluster.py       # wait for run to finish
   python notebooks/tests/push_and_run_bundle_on_cluster.py --no-wait   # submit and exit
   ```

This uses the [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html) and [Databricks Utilities with Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/databricks-utilities) pattern: workspace upload + one-off job submit.
