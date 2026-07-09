# Run All Documentation Tests

Runs **all** documentation tests by invoking **gbx-test-python-docs**, **gbx-test-sql-docs**, and **gbx-test-scala-docs** in sequence. Pass-through options (e.g. `--skip-build`, `--no-sample-data-root`) are forwarded so `GBX_SAMPLE_DATA_ROOT` and build behavior are consistent. Use for pre-commit or CI-style validation.

---

## Usage

```bash
bash scripts/commands/gbx-test-docs.sh [OPTIONS]
```

## Options

**Targeting (Python only)**

- `--suite <name>` – Python subset: `quickstart` | `api` | `readers` | `rasterx` | `advanced` | `setup`.
- `--path <path>` – Directory or file relative to `docs/tests/python/`.
- `--test <nodeid>` – Single Python test node id.

**Common**

- `--host` – Run on the host (arca), not the Docker container. Passed through to each child suite (python-docs, sql-docs, scala-docs). Requires `source ~/.local/geobrix-gdal-env.sh` first (provisioned by the `geobrix-arca` plugin). See "Host mode" below.
- `--rebuild-venv` – (with `--host`) force-rebuild the host test venv; forwarded to the venv-based child suites (python-docs, sql-docs).
- `--log <path>` – Log file (filename → `test-logs/<name>`).
- `--markers <markers>` – Pytest markers for Python (e.g. `"not slow"`).
- `--include-integration` – Include Python integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build before Python tests.
- `--scala-suite <pattern>` – Scala test suite pattern (default: `tests.docs.scala.*`).
- `--python-only` – Run only Python doc tests (skip Scala).
- `--scala-only` – Run only Scala doc tests (skip Python).
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default; e.g. full bundle).
- `--help` – Help and examples.

## Host mode (arca, no Docker)

With `--host` the orchestrator runs on the host instead of `docker exec geobrix-dev`, forwarding `--host` to each child suite (python-docs, sql-docs, scala-docs). Prerequisites: `source ~/.local/geobrix-gdal-env.sh` (native GDAL + Java 17 + PYTHONPATH) and `uv` on PATH, with `PIP_INDEX_URL` pointing at the internal pip proxy. The venv-based child suites build `.venv-host` from `python/geobrix/requirements-dev-container.txt` on first run (the exact CI pins, minus native-source-only `pdal` which arca can't build); `--rebuild-venv` is forwarded to them. The scala-docs child runs `mvn` directly and needs only the sourced GDAL env. See the `geobrix-arca` plugin for the full setup.

**Sample data (default):** The command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` inside the container so doc tests use the minimal bundle (host path `sample-data/Volumes/main/default/test-data`). This is required for running docs unit tests on remote/CI. Use `--no-sample-data-root` to leave it unset (e.g. to use a full bundle or your own env).

## Examples

```bash
# Full run with build
bash scripts/commands/gbx-test-docs.sh

# On the arca host (no Docker) — source the GDAL env first
source ~/.local/geobrix-gdal-env.sh
bash scripts/commands/gbx-test-docs.sh --host

# Fast run (skip build), with log. Uses in-repo minimal bundle; no download.
bash scripts/commands/gbx-test-docs.sh --skip-build --log docs.log

# Python doc tests only (e.g. API suite)
bash scripts/commands/gbx-test-docs.sh --python-only --suite api --skip-build

# Scala doc tests only
bash scripts/commands/gbx-test-docs.sh --scala-only --log scala-docs.log

# Custom Scala suite
bash scripts/commands/gbx-test-docs.sh --scala-only --scala-suite 'docs.tests.scala.api.*'
```

## Order and scope

1. **Python** – `gbx-test-python-docs` (default: all of `docs/tests/python/` except `api/`; use `--suite` / `--path` / `--test` for subsets).
2. **SQL/API** – `gbx-test-sql-docs` (docs/tests/python/api/).
3. **Scala** – `gbx-test-scala-docs` (Maven suite default `tests.docs.scala.*`).

If any phase fails, the command exits with a non-zero code. With `--log`, the log file is truncated at the start of the run and all output is written to it.
