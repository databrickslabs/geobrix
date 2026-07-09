# Run Notebook Tests

Runs notebooks **cell-by-cell** (no Jupyter kernel) by default: discovers `notebooks/tests/fixtures/*.ipynb` and `notebooks/sample-data/*.ipynb`, executes each code cell with `exec()`, and reports per-cell status. **All runs happen inside the `geobrix-dev` Docker container** via `docker exec`.

---

## Usage

```bash
bash scripts/commands/gbx-test-notebooks.sh [OPTIONS]
```

## Options

**Common**

- `--host` – Run on the host (arca), not the Docker container. Requires `source ~/.local/geobrix-gdal-env.sh` first (provisioned by the `geobrix-arca` plugin); builds/reuses `.venv-host-pyrx` from the pinned CI lock and runs against a host-built JAR. See "Host mode" below.
- `--rebuild-venv` – (with `--host`) force-rebuild the host test venv.
- `--log <path>` – Write output to log (filename → `test-logs/<name>`).
- `--path <path>` – Limit scope: subdir (e.g. `sample-data`, `fixtures`), a single `.ipynb`, or a test file (e.g. `test_notebook_via_script.py`). With a `.py` path, runs **pytest** for that file instead of the cell-by-cell runner.
- `--include-integration` – Include full-notebook execution tests when running **pytest** (default: **false**).
- `--help` – Help and examples.

## Host mode (arca, no Docker)

With `--host` the command runs directly on the host instead of `docker exec geobrix-dev`. Prerequisites: `source ~/.local/geobrix-gdal-env.sh` (native GDAL + Java 17 + PYTHONPATH) and `uv` on PATH, with `PIP_INDEX_URL` pointing at the internal pip proxy. The notebook runner executes from the host venv against the built JAR. The first run builds a host test venv from the exact CI-pinned lock via `uv` — `.venv-host-pyrx` from `python/geobrix/requirements-pyrx-ci.txt` (the light-tier deps: rasterio/pandas/h3/vizx), plus `nbformat`/`nbconvert` (needed by the runner, installed on demand). Neither CI lock contains `pdal` (source-only, unbuildable on arca and not needed here). Unlike the container, the host path runs the notebooks directly in `.venv-host-pyrx` (`GBX_NOTEBOOK_ISOLATED_ENV=0`) rather than a nested `python -m venv` (which fails on arca — no `ensurepip`/`python3-venv`).

**Note (bare host):** notebooks that write sample bundles to the literal `/Volumes` path (the data-download notebook) fail on a bare host with `Permission denied: '/Volumes'` — those need a real UC Volumes mount (`sudo ln -sfn "$PWD/sample-data/Volumes" /Volumes`) and, for Sentinel-2 fixtures, `pystac-client`/`planetary-computer`. The runner itself works on host; these are data/mount limitations. See the `geobrix-arca` plugin for the full setup.

**Read/write path behavior (absolute vs relative)**

- **Default**: Absolute, non-temp paths in notebook cells are **remapped** to the cell-by-cell workdir so reads and writes go under a temp directory and runs are fully isolated. That includes **`/Volumes/`** so you can test setup bundles without touching real volume data; existence checks (e.g. `Path("/Volumes/...").exists()`) and file reads see the workdir tree. Paths under `/tmp` (or `tempfile.gettempdir()`) are left unchanged; relative paths are unchanged.
- **Reads** affected: `open(..., 'r')`, `Path(...).read_text()`, `os.stat()`, `os.path.exists()`, `os.listdir()`, `os.scandir()`, `os.walk()`, `shutil.copy`/`copy2`/`copytree`/`move` (src).
- **Writes** affected: `open(..., 'w'|'a'|'x')`, `Path(...).write_text()`/`write_bytes()`, shutil copy/move (dst).
- `--allow-absolute-reads` – Do **not** remap absolute read paths (same as `GBX_NOTEBOOK_ALLOW_ABSOLUTE_READS=1`).
- `--allow-absolute-writes` – Do **not** remap absolute write paths (same as `GBX_NOTEBOOK_ALLOW_ABSOLUTE_WRITES=1`).
- When running cell-by-cell, if a cell had any write path remapped, the result line shows: **📁 N write path(s) remapped under workdir**.

## Default behavior

- **Cell-by-cell (no kernel)**: Default run executes notebooks with the runner in `notebooks/tests/run_notebooks_cell_by_cell.py`. `%pip install` cells are run in the isolated venv (same interpreter as subsequent cells). The venv is pre-installed with pystac-client, planetary-computer, and geopandas so notebooks that use Sentinel-2 or other sample-data deps work with or without a %pip cell.
- **Verbosity**: `GBX_NOTEBOOK_VERBOSITY=quiet|truncated|full` (default: `truncated`). Controls how much source/result is printed per cell.
- **Volumes mount required**: The command checks that `/Volumes` exists in the container. If not, it exits with instructions to start the container using `./scripts/docker/start_docker_with_volumes.sh`.
- **Pytest when `--path` is a .py file**: e.g. `--path test_notebook_via_script.py` runs pytest for that test file instead of the cell-by-cell runner.

## Examples

```bash
# Cell-by-cell run of fixtures + sample-data notebooks (default)
bash scripts/commands/gbx-test-notebooks.sh

# On the arca host (no Docker) — source the GDAL env first
source ~/.local/geobrix-gdal-env.sh
bash scripts/commands/gbx-test-notebooks.sh --host

# Only sample-data notebooks
bash scripts/commands/gbx-test-notebooks.sh --path sample-data

# Run pytest for a specific test file
bash scripts/commands/gbx-test-notebooks.sh --path test_notebook_via_script.py

# With log
bash scripts/commands/gbx-test-notebooks.sh --log notebooks.log

# Allow absolute read and/or write paths (no remapping)
bash scripts/commands/gbx-test-notebooks.sh --allow-absolute-reads
bash scripts/commands/gbx-test-notebooks.sh --allow-absolute-writes
```

## Test location

- **Runner**: `notebooks/tests/run_notebooks_cell_by_cell.py` (discovers and runs notebooks).
- **Tests**: `notebooks/tests/` (structure mirrors `notebooks/`, e.g. `notebooks/tests/sample-data/` for `notebooks/sample-data/`).
