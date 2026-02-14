# Run Notebook Tests

Runs notebooks **cell-by-cell** (no Jupyter kernel) by default: discovers `notebooks/tests/fixtures/*.ipynb` and `notebooks/sample-data/*.ipynb`, executes each code cell with `exec()`, and reports per-cell status. **All runs happen inside the `geobrix-dev` Docker container** via `docker exec`.

---

## Usage

```bash
bash .cursor/commands/gbx-test-notebooks.sh [OPTIONS]
```

## Options

**Common**

- `--log <path>` – Write output to log (filename → `test-logs/<name>`).
- `--path <path>` – Limit scope: subdir (e.g. `sample-data`, `fixtures`), a single `.ipynb`, or a test file (e.g. `test_notebook_via_script.py`). With a `.py` path, runs **pytest** for that file instead of the cell-by-cell runner.
- `--include-integration` – Include full-notebook execution tests when running **pytest** (default: **false**).
- `--help` – Help and examples.

**Read/write path behavior (absolute vs relative)**

- **Default**: Absolute, non-temp paths in notebook cells are **remapped** to the cell-by-cell workdir so reads and writes go under a temp directory and runs are fully isolated. That includes **`/Volumes/`** so you can test setup bundles without touching real volume data; existence checks (e.g. `Path("/Volumes/...").exists()`) and file reads see the workdir tree. Paths under `/tmp` (or `tempfile.gettempdir()`) are left unchanged; relative paths are unchanged.
- **Reads** affected: `open(..., 'r')`, `Path(...).read_text()`, `os.stat()`, `os.path.exists()`, `os.listdir()`, `os.scandir()`, `os.walk()`, `shutil.copy`/`copy2`/`copytree`/`move` (src).
- **Writes** affected: `open(..., 'w'|'a'|'x')`, `Path(...).write_text()`/`write_bytes()`, shutil copy/move (dst).
- `--allow-absolute-reads` – Do **not** remap absolute read paths (same as `GBX_NOTEBOOK_ALLOW_ABSOLUTE_READS=1`).
- `--allow-absolute-writes` – Do **not** remap absolute write paths (same as `GBX_NOTEBOOK_ALLOW_ABSOLUTE_WRITES=1`).
- When running cell-by-cell, if a cell had any write path remapped, the result line shows: **📁 N write path(s) remapped under workdir**.

## Default behavior

- **Cell-by-cell (no kernel)**: Default run executes notebooks with the runner in `notebooks/tests/run_notebooks_cell_by_cell.py`. Magic-only cells (e.g. `%pip install`) are skipped.
- **Verbosity**: `GBX_NOTEBOOK_VERBOSITY=quiet|truncated|full` (default: `truncated`). Controls how much source/result is printed per cell.
- **Volumes mount required**: The command checks that `/Volumes` exists in the container. If not, it exits with instructions to start the container using `./scripts/docker/start_docker_with_volumes.sh`.
- **Pytest when `--path` is a .py file**: e.g. `--path test_notebook_via_script.py` runs pytest for that test file instead of the cell-by-cell runner.

## Examples

```bash
# Cell-by-cell run of fixtures + sample-data notebooks (default)
bash .cursor/commands/gbx-test-notebooks.sh

# Only sample-data notebooks
bash .cursor/commands/gbx-test-notebooks.sh --path sample-data

# Run pytest for a specific test file
bash .cursor/commands/gbx-test-notebooks.sh --path test_notebook_via_script.py

# With log
bash .cursor/commands/gbx-test-notebooks.sh --log notebooks.log

# Allow absolute read and/or write paths (no remapping)
bash .cursor/commands/gbx-test-notebooks.sh --allow-absolute-reads
bash .cursor/commands/gbx-test-notebooks.sh --allow-absolute-writes
```

## Test location

- **Runner**: `notebooks/tests/run_notebooks_cell_by_cell.py` (discovers and runs notebooks).
- **Tests**: `notebooks/tests/` (structure mirrors `notebooks/`, e.g. `notebooks/tests/sample-data/` for `notebooks/sample-data/`).
