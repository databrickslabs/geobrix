# Run Python Documentation Tests

Runs Python documentation tests from `docs/tests/python/` using pytest **inside the `geobrix-dev` Docker container**.

**Isolation from SQL tests:** By default (full suite), the **api/** directory (SQL API Reference tests) is **excluded** so that `gbx:test:python-docs` and `gbx:test:sql-docs` stay isolated. To run SQL/API tests use `gbx:test:sql-docs` or `gbx:test:python-docs --suite api`.

**Full suite takes 10+ minutes.** Use **targeted runs** (e.g. `--suite`, `--test`, `--path`) for day-to-day work and reserve the full suite for pre-commit or CI. Log-path conventions (`--log`): [Adding or fixing a `gbx:*` command](../../.claude/rules/gbx-commands.md).

---

## Run targeted tests (recommended)

| Goal | Command | Approx time |
|------|---------|-------------|
| One test | `--test quickstart/test_examples.py::test_sql_constants_are_valid_strings` | ~10–30 s |
| Quickstart only | `--suite quickstart` | ~1–2 min |
| API tests only | `--suite api` | ~3–5 min |
| Readers only | `--suite readers` | ~2–4 min |
| Formats (Zarr) only | `--path formats/` | ~3–5 min |
| One Zarr page | `--path readers/test_zarr_read_examples.py` (or `writers/test_zarr_write_examples.py`, `api/test_zarr_functions_examples.py`) | ~2–4 min |
| Ingestion pages only | `--suite ingestion` | ~1–2 min |
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
5. **Delta jars (Zarr doc tests only)** – When the target covers a Zarr doc test (the full suite, `--path formats/`, a whole `readers/`, `writers/` or `api/` directory, or any path or `--test` that names `zarr`) the command first runs `scripts/ci/fetch-delta-test-jars.sh` in the container, under the work lock: the Zarr examples keep a store's manifest in a Delta table and need the delta-spark jars on the JVM classpath. It does nothing when `scripts/docker/m2` already holds them; a sha256 mismatch (exit 3) aborts, and a missing jar is a warning (the Zarr tests then fail naming it). Steps 5 and 6 are one shared helper (`ensure_zarr_test_env` in `common.sh`), also used by `gbx:test:sql-docs` and `gbx:test:python`.
6. **Zarr extra (Zarr doc tests only)** – Under the same condition the command first checks that every pin of the `zarr_dbr19` extra in `pyproject.toml` is installed in the container and installs the extra when it is not (an image built before it was baked in would otherwise skip the Zarr tests with exit 0).

## Usage

```bash
bash scripts/commands/gbx-test-python-docs.sh [OPTIONS]
```

## Options

**Targeting**

- `--test <nodeid>` – Single test (e.g. `quickstart/test_examples.py::test_exec_sql_read_and_use_snippet`).
- `--suite <name>` – Subset: `quickstart` | `api` | `readers` | `ingestion` | `rasterx` | `advanced` | `setup` | `integration`.
- `--path <path>` – Directory or file relative to `docs/tests/python/` (overridden by `--test`/`--suite` if both given last). A single file (`--path api/test_x.py`, or a `--test` node id) gets its directory and that directory's parent on `PYTHONPATH`, so its sibling example modules (`from _fixtures import ...`) import exactly as they do when the whole directory is collected; before, a lone `api/` file failed to collect with `ModuleNotFoundError: No module named '_fixtures'` while `--suite api` passed.

**Other**

- `--log <path>` – Log file (filename → `test-logs/<name>`). Prefer timestamped names for tracking.
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--markers <markers>` – Pytest markers (e.g. `"not slow"`).
- `--timeout <seconds>` – Per-test deadline (default 600). A test still running after this many seconds fails with a `TimeoutError` (stacks are dumped first) instead of blocking the run; if the JVM stops responding the run ends with the stacks on the terminal. Sets `GBX_DOC_TEST_TIMEOUT_S` in the container. A test marked `@pytest.mark.deadline(n)` keeps its own limit.
- `--include-integration` – Include integration tests (excluded by default).
- `--skip-build` – Skip Maven and Python build.
- `--no-sample-data-root` – Do **not** set `GBX_SAMPLE_DATA_ROOT` (use your env or path_config default).
- `--sample-data-root <dir>` – Container path to use for `GBX_SAMPLE_DATA_ROOT` instead of the minimal bundle, e.g. `/Volumes/main/default/geobrix_samples` to check a test against the complete bundle (`gbx:data:download --bundle complete`). The command fails if `<dir>/geobrix-examples` is missing.
- `--help` – Help and suite timing.

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command).

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Quickstart only, no build, with log (typical during edits)
bash scripts/commands/gbx-test-python-docs.sh --suite quickstart --skip-build --log quickstart.log

# Single failing test
bash scripts/commands/gbx-test-python-docs.sh --test quickstart/test_examples.py::test_convert_to_databricks_geometry_with_nyc_data --skip-build

# One test file
bash scripts/commands/gbx-test-python-docs.sh --path api/test_rasterx_functions_sql.py --skip-build

# Full suite with timestamped log (e.g. before commit)
bash scripts/commands/gbx-test-python-docs.sh --skip-build --log test-logs/python-docs-$(date +%Y%m%d-%H%M%S).log

# Full run (build + all tests; uses in-repo minimal bundle)
bash scripts/commands/gbx-test-python-docs.sh

# One file against the complete sample bundle instead of the minimal one
bash scripts/commands/gbx-test-python-docs.sh --path ingestion/test_read_paths_examples.py --skip-build --sample-data-root /Volumes/main/default/geobrix_samples

# A slow suite: allow each test up to 15 minutes before it is failed as stuck
bash scripts/commands/gbx-test-python-docs.sh --path readers/ --skip-build --timeout 900

# Include integration tests (DBR / integration env)
bash scripts/commands/gbx-test-python-docs.sh --include-integration --skip-build
```

## Test layout and log location

- **Source:** `docs/tests/python/` (mirrors docs: `quickstart/`, `api/`, `readers/`, `rasterx/`, `advanced/`, `setup/`).
- **Logs:** With `--log filename.log`, logs go to `test-logs/filename.log` unless you pass an absolute path.
