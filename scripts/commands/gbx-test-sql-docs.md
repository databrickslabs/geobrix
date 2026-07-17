# Run SQL Documentation Tests

Runs documentation tests for the **SQL API Reference** and related Python API examples from `docs/tests/python/api/` (e.g. `test_sql_api.py`, `test_python_api.py`). Uses the same Docker and options as `gbx:test:python-docs`, scoped to the API suite.

---

## Usage

```bash
bash scripts/commands/gbx-test-sql-docs.sh [OPTIONS]
```

## Options

**Targeting**

- `--test <nodeid>` – Single test (e.g. `api/test_sql_api.py::test_register_and_show_functions`).
- `--path <path>` – File or directory relative to `docs/tests/python/` (default: `api/`).

**Common**

- `--host` – Run on the host (arca), not the Docker container. Requires `source ~/.local/geobrix-gdal-env.sh` first (provisioned by the `geobrix-arca` plugin); builds/reuses `.venv-host-pyrx` from the pinned CI lock and a host-built JAR. See "Host mode" below.
- `--rebuild-venv` – (with `--host`) force-rebuild the host test venv.
- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default).
- `--help` – Help and examples.

**Sample data (default):** Like `gbx:test:python-docs`, this command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` (in the container, or the on-disk `sample-data/…/test-data` mirror on the host) for the minimal bundle (required for remote/CI). Use `--no-sample-data-root` to leave it unset.

## Host mode (arca, no Docker)

With `--host` the command runs directly on the host instead of `docker exec geobrix-dev`. Prerequisites: `source ~/.local/geobrix-gdal-env.sh` (native GDAL + Java 17 + PYTHONPATH) and `uv` on PATH, with `PIP_INDEX_URL` pointing at the internal pip proxy. The first run builds a host test venv from the exact CI-pinned lock via `uv` — `.venv-host-pyrx` from `python/geobrix/requirements-pyrx-ci.txt` (the light-tier deps: rasterio/pandas/h3/vizx). Neither CI lock contains `pdal` (source-only, unbuildable on arca and not needed here). See the `geobrix-arca` plugin for the full setup.

## Examples

```bash
# API/SQL doc tests only, skip build (uses in-repo minimal bundle)
bash scripts/commands/gbx-test-sql-docs.sh --skip-build

# On the arca host (no Docker) — source the GDAL env first
source ~/.local/geobrix-gdal-env.sh
bash scripts/commands/gbx-test-sql-docs.sh --host

# Single test file with log
bash scripts/commands/gbx-test-sql-docs.sh --path api/test_sql_api.py --skip-build --log sql-docs.log

# Full run (build + api tests)
bash scripts/commands/gbx-test-sql-docs.sh
```

## Test layout

- **Source:** `docs/tests/python/api/` (SQL API Reference and Python API examples).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless an absolute path is given.
