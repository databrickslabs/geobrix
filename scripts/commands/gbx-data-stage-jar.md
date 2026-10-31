# Stage JAR to Volume

With `--from-wheel`, stages the JAR bundled in an env wheel instead (no Maven).

Runs **mvn clean package -DskipTests** and stages **both** jars it produces (one build): the product **target/*-jar-with-dependencies.jar** → **GBX_ARTIFACT_VOLUME**/ (the artifact directory; dedupes to exactly one `geobrix-*-jar-with-dependencies.jar` — any stale other-version product JAR in that dir is removed before upload), and the bench **target/*-tests.jar** → the **bundle volroot** (`/Volumes/$GBX_BUNDLE_VOLUME_CATALOG/$GBX_BUNDLE_VOLUME_SCHEMA/$GBX_BUNDLE_VOLUME_NAME/`, where the bench launcher attaches it as a cluster library). Both overwrite if present. Set **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to build the JAR locally but skip the Databricks upload (no auth needed), **GBX_BUNDLE_SKIP_JAR=1** to skip the JAR step entirely (no build, no upload), or **GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1** to stage only the product jar. Override the tests.jar destination with **GBX_BENCH_TESTS_JAR_VOLUME_PATH**.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-jar.sh [--from-wheel <whl>] [--profile NAME] [--log <path>] [--no-wait]
```

## Options

- `--from-wheel <whl>` - stage the JAR bundled in an env wheel. It runs no Maven and no container, and takes no container lock (it builds nothing). It runs even when `GBX_BUNDLE_SKIP_JAR=1` is set.
  - It takes the one `_jar/geobrix-<version>-jar-with-dependencies.jar` in the wheel.
  - It refuses a JAR that is not the wheel's version, or whose sha256 is not the one in the wheel's `RECORD`.
  - It runs the packaging guard on the JAR.
  - It uploads the JAR, then `<jar>.sha256` (`sha256sum` format), to `<volroot>/geobrix/<version>/jar/`, checking each size.
  - `GBX_ARTIFACT_VOLUME` is not used. `GBX_BUNDLE_SKIP_JAR_UPLOAD=1` does the checks and stops before the upload.
- `--profile NAME` - the Databricks config profile for the upload. Default: `DATABRICKS_CONFIG_PROFILE` from the environment, else from `examples/tests/databricks_cluster_config.env`.
- `--log <path>`, `--no-wait` - as in the other commands.

## Config

1. Copy `examples/tests/databricks_cluster_config.example.env` to `examples/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to build the JAR locally and skip the upload; **GBX_BUNDLE_SKIP_JAR=1** to skip build and upload.

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command). Only the Maven build step takes it (`mvn clean package` wipes `target/`); with `GBX_BUNDLE_SKIP_JAR=1` no Maven runs and no lock is taken. It is held through the Databricks upload too.

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Interpreter

This command talks to a workspace, so it needs `databricks-sdk` and Python 3.11 or newer. It does not use whatever `python` is on the PATH: the shared `gbx_python_with_sdk` helper in `common.sh` picks `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`) when that venv is Python 3.11+ and can import the SDK (the CI-lock venv from `gbx:venv:sync` cannot), otherwise `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter (Python 3.12 when there is no usable venv), otherwise system `python3` if it is 3.11+ and has the SDK, otherwise it stops with a hint to install uv. The SDK is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`) to the release `uv` resolved on the dev workstation when it was pinned (0.157.0; the launchers' `databricks.sdk` imports were checked against it), so a run does not float to a newer release. `GBX_PYRX_VENV=<dir>` moves the venv.

## Requires

- Maven (for `mvn clean package -DskipTests`).
- `databricks-sdk` (e.g. `pip install databricks-sdk` or `pip install -e python/geobrix[databricks]`).
