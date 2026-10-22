# Run Python Unit Tests

Runs Python unit tests (non-documentation tests) using pytest.

With **no `--path`** it runs the whole suite as CI-style **phases** (separate pytest processes, separate logs, per-phase summary) because the heavy and light tiers cannot share one pytest process. With `--path` it runs exactly the paths you give in a single pytest process.

## Usage

```bash
bash scripts/commands/gbx-test-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run specific test file or directory (repeatable). Single pytest run, no phasing. Cross-tier paths in one call can still hit the heavy/light process conflict; pass one tier per call.
- `-k <expr>` - Pytest keyword filter (e.g. `"merge or fileName"`)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path). In phased mode this is the combined log; each phase also gets `<path minus .log>-<phase>.log`.
- `--with-integration` - Include `@pytest.mark.integration` tests (network downloads, slow). Excluded by default.
- `--markers <expr>` - Override marker filter with a custom pytest expression (e.g. `"not slow"`). Disables the default `not integration` filter.
- `--help` - Display help message

## Phased default run (no `--path`)

| Phase | What runs | Where the list comes from |
|---|---|---|
| `heavy` | `test/` minus every light dir (`pytest test --ignore=test/pyrx ...`) | `LIGHT_IGNORES` in `.github/actions/python_build/action.yml` |
| `light` | the light dirs in one process (`pytest test/pyrx test/ds ...`) | the `pytest test/... -m ...` line in `.github/actions/pyrx_build/action.yml` |

- The lists are **read from the CI action files**, not copied into the script, so the command cannot drift from CI. If those lines change shape the command stops with exit 2 and a message (update `load_ci_phases()` in `gbx-test-python.sh`).
- Each phase runs from `python/geobrix` (like CI) with `-m "not integration"` (unless `--with-integration`/`--markers`), `-k` and `-rfE` (failing test ids are listed in the summary).
- Per-phase logs: `test-logs/python-tests-<phase>.log` by default, or `<--log minus .log>-<phase>.log`.
- After the last phase a summary prints each phase's pytest result line and, for failing phases, the FAILED/ERROR test ids (first 25; full list in the log).
- Exit code is non-zero if **any** phase fails (a phase that selects no tests under `-k` is not a failure). A missing JAR fails the `heavy` phase before pytest starts (see below).
- CI-only extras are not mirrored: heavy `--cov`, `python -m build`, and the fresh hash-pinned install.

## Staged JAR (heavy tier only)

The heavy tests load `python/geobrix/lib/geobrix-*-jar-with-dependencies.jar` (`spark.jars`). With that directory empty every heavy module fails at collection with `IndexError: list index out of range`, so the command checks it first:

- **Where**: before the `heavy` phase, and before a `--path` run when any path can select heavy tests (anything under `python/geobrix/test/` that is not inside a light dir from `LIGHT_IGNORES`, or `python/geobrix` / `python/geobrix/test` itself). A path wholly inside a light dir (e.g. `python/geobrix/test/ds/`) and the `light` phase never need the JAR and skip the check.
- **Staging**: if no JAR is staged, or the newest `target/geobrix-*-jar-with-dependencies.jar` is newer than the staged one, that JAR is copied into `python/geobrix/lib/` (older staged JARs removed). This is the same copy the Maven `package` phase does in CI (`copy-files-on-build` in `pom.xml`) and that `gbx:test:parity` does after its build; both use `stage_jar_from_target` in `common.sh`.
- **Never runs Maven.** With no JAR in `target/` and none staged, the heavy phase (or the `--path` run) fails immediately with one line, `PRECONDITION MISSING: no built JAR in target/ and none staged in python/geobrix/lib/; heavy tests not run. Build it: gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"`, and the exit code is non-zero. In the phased run the `light` phase still runs and the summary shows the heavy phase as FAIL with that line.
- `gbx:data:stage-wheel` with a skip-JAR flag empties `python/geobrix/lib/` (it keeps the wheel JAR-less); the next heavy run here re-stages the JAR from `target/`.

## Zarr tests (`test/zarr`)

`test/zarr` needs two things the base dev image does not carry. When `test/zarr` is among the selected tests (a `--path` at or above it, or the phased run, whose light phase includes it), the command provides both before pytest starts, so a plain `--path python/geobrix/test/zarr/` just works. Runs that do not select `test/zarr` skip all of this.

- **The `zarr_dbr19` extra** (zarr, numcodecs, zarrs, tensorstore, obstore and their transitive packages). The command reads the extra's pins from `pyproject.toml` and checks each against what is installed; if any is missing or out of range it runs `pip install -e python/geobrix[zarr_dbr19]`. The pins are never repeated in the script. Once the dev image bakes the extra in, this is a no-op.
- **The OSS Delta Lake jars** (`delta-spark_2.13` + `delta-storage`, 4.0.1), which the manifest tests run against in a local Spark session. They are not a pip package, so `scripts/ci/fetch-delta-test-jars.sh` fetches them with `mvn -C dependency:get` (pinned plugin, strict checksums) into the persistent Maven repo (`scripts/docker/m2/`, gitignored) when missing. Whether they were already there or just fetched, the script then verifies a fixed sha256 for each jar; a mismatch exits 3 and the command refuses to run. Any other failure (no `mvn`, no network) is a warning: the Delta-backed zarr tests then fail with a message naming the missing jars (they never skip).
- **`GBX_M2_REPO`** (optional) names the Maven repository the jars live in. Both the fetch script and `test/conftest.py` search it first, and the script also passes it to Maven as `-Dmaven.repo.local`. The command does not forward it into the container: set it there (for example through `gbx:docker:exec`) or in a CI step. Without it the checkout's `scripts/docker/m2` and then `~/.m2/repository` are searched.
- `test/conftest.py` puts the jars on the JVM classpath (`PYSPARK_SUBMIT_ARGS=--driver-class-path ...`, not `--jars`, so `spark.jars` stays empty) only when `test/zarr` tests are collected, only when both jar files exist, and never overrides a `PYSPARK_SUBMIT_ARGS` you set. This matters because PySpark reads its launch options only when it starts the JVM: in the light phase every light dir shares one process, so the jars must be there before the first session of any dir. The heavy phase ignores `test/zarr` and never sees them.

## Default marker filter

By default the script runs with `-m "not integration"`, matching CI's `python_build` action. This excludes `python/geobrix/test/sample/test_sample_bundle.py::test_run_*_bundle_returns_dict_shape`, which download hundreds of MB of sample data.

Opt in with `--with-integration` (drops the filter entirely) or `--markers <expr>` (replaces it with your own expression).

## Examples

```bash
# Everything, as CI-style phases (heavy, light) with a per-phase summary
bash scripts/commands/gbx-test-python.sh

# Include integration tests (network downloads)
bash scripts/commands/gbx-test-python.sh --with-integration

# Run specific test file
bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_operations.py

# Run with logging
bash scripts/commands/gbx-test-python.sh --log python-tests.log

# Custom marker expression (overrides the default)
bash scripts/commands/gbx-test-python.sh --markers "not slow"
```

## Test Location

- **Source**: `python/geobrix/test/`

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-python-docs` for those)
- Uses pytest with verbose output
- Default log location: `test-logs/` (if filename only provided)
- Tier rules and the CI lists: see the docstring in `python/geobrix/test/conftest.py` (`_LIGHT_TEST_DIRS`)
