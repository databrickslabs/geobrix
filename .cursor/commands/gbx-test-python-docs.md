# Run Python Documentation Tests

Runs Python documentation tests from `docs/tests/python/` using pytest **inside the `geobrix-dev` Docker container**.

**Isolation from SQL tests:** By default (full suite), the **api/** directory (SQL API Reference tests) is **excluded** so that `gbx:test:python-docs` and `gbx:test:sql-docs` stay isolated. To run SQL/API tests use `gbx:test:sql-docs` or `gbx:test:python-docs --suite api`.

**Full suite takes 10+ minutes.** Use **targeted runs** (e.g. `--suite`, `--test`, `--path`) for day-to-day work and reserve the full suite for pre-commit or CI. See [Test organization and logging](.cursor/rules/test-organization-logging.mdc).

---

## Run targeted tests (recommended)

| Goal | Command | Approx time |
|------|---------|-------------|
| One test | `--test quickstart/test_examples.py::test_sql_constants_are_valid_strings` | ~10–30 s |
| Quickstart only | `--suite quickstart` | ~1–2 min |
| API tests only | `--suite api` | ~3–5 min |
| Readers only | `--suite readers` | ~2–4 min |
| RasterX only | `--suite rasterx` | ~1–2 min |
| Advanced only | `--suite advanced` | ~1 min |
| Setup only | `--suite setup` | ~0.5 min |
| Integration only | `--suite integration` | varies (DBR/integration env) |
| One file | `--path api/test_rasterx_functions_sql.py` | varies |
| Full suite | (no `--test`/`--suite`/`--path`) | **10+ min** |

Use `--skip-build` when the tree is already built to avoid extra time. Doc tests use the in-repo minimal bundle (no download step).

**Unit vs integration:** By default, integration tests are **excluded** (`-m "not integration"`). They require DBR or specific data paths. Use `--include-integration` to run them (e.g. on Databricks or when validating those flows). See `docs/tests/SKIPPED-TESTS-INVENTORY.md` for the list.

**Logging for tracking:** use a timestamped log so runs don’t overwrite each other:

```bash
--log test-logs/python-docs-$(date +%Y%m%d-%H%M%S).log
```

---

## Default behavior (before pytest)

1. **Volumes check** – Ensures `/Volumes` exists in the container. If not, script exits with instructions to run `./scripts/docker/start_docker_with_volumes.sh`.
2. **Sample data** – By default the command sets `GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data` in the container so doc tests use the minimal bundle (host path `sample-data/Volumes/main/default/test-data`). This is required for remote/CI. Use `--no-sample-data-root` to leave it unset. No download step; use `gbx:data:generate-minimal-bundle` once if needed.
3. **Maven** – `mvn package -DskipTests`.
4. **Python build** – `python3 -m build` in `python/geobrix/`, then `pip install -e python/geobrix`. Use `--skip-build` when already built.

## Usage

```bash
bash .cursor/commands/gbx-test-python-docs.sh [OPTIONS]
```

## Options

**Targeting**

- `--test <nodeid>` – Single test (e.g. `quickstart/test_examples.py::test_exec_sql_read_and_use_snippet`).
- `--suite <name>` – Subset: `quickstart` | `api` | `readers` | `rasterx` | `advanced` | `setup`.
- `--path <path>` – Directory or file relative to `docs/tests/python/` (overridden by `--test`/`--suite` if both given last).

**Other**

- `--log <path>` – Log file (filename → `test-logs/<name>`). Prefer timestamped names for tracking.
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default).
- `--help` – Help and suite timing.

## Examples

```bash
# Quickstart only, no build, with log (typical during edits)
bash .cursor/commands/gbx-test-python-docs.sh --suite quickstart --skip-build --log quickstart.log

# Single failing test
bash .cursor/commands/gbx-test-python-docs.sh --test quickstart/test_examples.py::test_convert_to_databricks_geometry_with_nyc_data --skip-build

# One test file
bash .cursor/commands/gbx-test-python-docs.sh --path api/test_rasterx_functions_sql.py --skip-build

# Full suite with timestamped log (e.g. before commit)
bash .cursor/commands/gbx-test-python-docs.sh --skip-build --log test-logs/python-docs-$(date +%Y%m%d-%H%M%S).log

# Full run (build + all tests; uses in-repo minimal bundle)
bash .cursor/commands/gbx-test-python-docs.sh

# Include integration tests (DBR / integration env)
bash .cursor/commands/gbx-test-python-docs.sh --include-integration --skip-build
```

## Test layout and log location

- **Source:** `docs/tests/python/` (mirrors docs: `quickstart/`, `api/`, `readers/`, `rasterx/`, `advanced/`, `setup/`).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless you pass an absolute path.
