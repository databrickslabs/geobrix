# Stage Python wheel to Volume

Builds the JAR first (via stage-jar: `mvn clean package` + JAR upload) unless **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** or **GBX_BUNDLE_SKIP_JAR=1** — either flag skips the JAR build AND upload entirely (no Maven), for light-tier-only wheel changes (the locally staged JAR in `python/geobrix/lib/` is left untouched; it is cleared and refilled only when the JAR step runs, and the wheel itself is always JAR-less) — then runs **python3 -m build** and stages the wheel to **two locations**: **GBX_ARTIFACT_VOLUME**/ (init-script dir; dedupes to exactly one `geobrix-*.whl` — before upload, every other-version `geobrix-*.whl` in that dir is deleted via the SDK's `files.list_directory_contents`; nothing else is touched, and a listing/delete failure aborts the run with a non-zero exit) and the **bundle volroot** (`/Volumes/$GBX_BUNDLE_VOLUME_CATALOG/$GBX_BUNDLE_VOLUME_SCHEMA/$GBX_BUNDLE_VOLUME_NAME/`, the bundle/%pip path used by notebooks and the bench launcher). Both overwrite if present. Set **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-wheel.sh [--env 5|6|both [--versioned]] [--profile NAME] [--no-prune] [--log <path>] [--no-wait]
```

- `--env 5|6|both` - stage the env wheel(s), the Databricks install, instead of building the plain wheel: `dist/env<N>/geobrix-<version>+env<N>-py3-none-any.whl` as `gbx:build:wheel --env <N>` left it (one wheel there, or `PRECONDITION MISSING`). No JAR step and no Maven (the env wheel carries its JAR), no build, and no container lock. The wheel goes to the bundle volroot only (the `%pip` path notebooks and docs use), under its own `+env<N>` name, size-verified; the init-script dir (`GBX_ARTIFACT_VOLUME`, not required with `--env`) and the plain wheel in it are not touched.
- `--versioned` - with `--env`: put each wheel in `<volroot>/geobrix/<version>/env<N>/`, not in the volroot. Without it, `--env` stages flat to the volroot: the per-version folder needs `--versioned`.
  - It removes every other `geobrix-*.whl` in that folder and checks the uploaded size.
  - The wheels in the volroot itself are not touched.
  - Without `--env` it exits 2.
- `--profile NAME` - the Databricks config profile for the uploads. Without it the profile is `DATABRICKS_CONFIG_PROFILE` from the environment, else from `examples/tests/databricks_cluster_config.env`.
- `--no-prune` - keep every other `geobrix-*.whl` in the artifact directory: skip the single-version cleanup of the plain-wheel stage.

An option the command does not know is an error (exit 2), never ignored.

## Config

1. Copy `examples/tests/databricks_cluster_config.example.env` to `examples/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload; **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** (or **GBX_BUNDLE_SKIP_JAR=1**) to skip the JAR build and upload when running stage-wheel. A skip-JAR flag plus **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** is a fully local wheel-only build (no Maven, no auth).

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command). Only the JAR step takes it (`mvn clean package` wipes `target/`); with `GBX_BUNDLE_SKIP_JAR=1` or `GBX_BUNDLE_SKIP_JAR_UPLOAD=1` no Maven runs and no lock is taken. It is held through the uploads too.

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Interpreter

This command talks to a workspace, so it needs `databricks-sdk` and Python 3.11 or newer. It does not use whatever `python` is on the PATH, and it needs the `build` package too (the wheel is built on that interpreter): the shared `gbx_python_with_sdk` helper in `common.sh` picks `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`) when that venv is Python 3.11+ and can import the SDK (the CI-lock venv from `gbx:venv:sync` cannot), otherwise `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter (Python 3.12 when there is no usable venv), otherwise system `python3` if it is 3.11+ and has the SDK, otherwise it stops with a hint to install uv. The SDK is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`) to the release `uv` resolved on the dev workstation when it was pinned (0.157.0; the launchers' `databricks.sdk` imports were checked against it), so a run does not float to a newer release. `GBX_PYRX_VENV=<dir>` moves the venv.

## Requires

- `python3 -m build`, `databricks-sdk` (e.g. `pip install build databricks-sdk` or `pip install -e python/geobrix[databricks]`).
