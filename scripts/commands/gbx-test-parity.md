# Cross-Tier Parity Tests

Run the light-vs-heavy (`pyvx` vs `vectorx`) parity suites as a **real gate**: rebuild the assembly JAR, stage it where the light tier loads it, run the integration-marked parity suites, and fail if nothing actually ran.

## Usage

```bash
bash scripts/commands/gbx-test-parity.sh [OPTIONS]
```

## Options

- `--path <path>` - A suite file, or a directory whose parity suites to run; repeat it to run several (relative to the repo root; default: every parity suite, see below)
- `-k <expr>` - Pytest keyword filter (e.g. `crs`)
- `--skip-build` - Reuse the already-staged JAR (fast; only when Scala is unchanged)
- `--log <path>` - Write output to a log file (filename → `test-logs/<name>`)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help` - Display help message

## The native payload

Set `GBX_NATIVE_ROOT` in the calling shell to run the container's pytest on a native payload, for example `GBX_NATIVE_ROOT=build/env-wheel/env6/payload` (from `gbx:build:wheel --env 6`). The command passes it into the container as its `/root/geobrix` path (a path under this checkout, relative or absolute; any other absolute path is taken as a container path), so the run's Python serves the payload's bindings (its `osgeo` and rasterio on one GDAL). Unset, nothing is passed.

## Why this command exists

Parity suites are marked `@pytest.mark.integration` and need the assembly JAR staged in `python/geobrix/lib/`, because they start a Spark session with `spark.jars` pointing at it to call the heavy tier. In a normal `gbx:test:python` run they are excluded by the default `not integration` marker filter, and even with `--with-integration` they self-skip when no JAR is staged.

That means the parity gate can silently not-run. **A skipped suite reads as green, which is worse than an absent one** — so this command:

1. **Rebuilds the JAR by default.** A staged JAR older than the Scala sources makes every heavy call fail with `UNRESOLVED_ROUTINE`: a mass failure that looks like a code bug but is only a stale artifact. Rebuilding first removes that failure mode entirely.
2. **Stages exactly one JAR**, deleting older ones so a stale artifact cannot be picked up instead.
3. **Runs with integration enabled** — parity is the whole point of the run.
   It first prepares the container the way `gbx:test:python` does (one shared step): an editable install of geobrix, because Spark's Python workers do not see pytest's `pythonpath` and would fail every light row with `ModuleNotFoundError`; and the optional light-tier test deps, which a fresh container lacks (the Column-API legs import them). Both are idempotent and cheap when present.
4. **Leaves the Spark Connect tests out** (`-m 'not connect'`): they run in their own process (`gbx:test:pyrx`), and the container has no gRPC client, so in a parity file's process they would only ever skip.
5. **Fails when any test skipped**, printing the likely cause (no staged JAR, a JAR-free Spark session already live in the process, or **sample data missing**: the container was started without the sample-data mounts, so the suites that read the sample bundle skip as `sample not mounted`; start it with `gbx:docker:start`). This is what stops a fully-skipped run from being mistaken for a pass.

## Which suites run

The suites are **discovered**, not listed: every `python/geobrix/test/**/test_parity*.py`, `test_*_parity.py` and `*_cross_tier.py` (the cross-tier suites that need the JAR are parity suites in intent), sorted, minus a short exclusion list (`PARITY_EXCLUDE` in the script) of files that are not cross-tier gates: `core/test_check_binding_parity.py` (a unit test of the binding-parity checker, run by `gbx:test:bindings`), `ds/test_vector_writer_parity.py` and `ds/test_writer_parity.py` (light-only writer round trips), and `ds/test_reader_parity.py` (skips by design when the heavy `gdal` reader yields no rows). These four are not part of any parity gate: they run only under `gbx:test:python --with-integration` (which also runs their `ds` tests that need the container's sample data). A new parity suite is gated by default; excluding one is a deliberate edit with a reason. Each file runs in its own pytest process.

`--path` may be given several times and each value may be a file or a directory (`./x` and `x/` are the same path, so the exclusions still apply); they run once each, in the order given:

```bash
bash scripts/commands/gbx-test-parity.sh --skip-build \
  --path python/geobrix/test/pyvx/test_parity_mvt.py \
  --path python/geobrix/test/pygx/
```

## When to use

- Before pushing a change that touches CRS handling, geometry encoding, or anything implemented in both tiers.
- After changing any Scala expression that has a light-tier counterpart (the rebuild is what makes the heavy side current).
- As the final gate for a both-tiers feature, alongside `gbx:test:bindings`.

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
# Full gate: rebuild, stage, run everything
bash scripts/commands/gbx-test-parity.sh

# Just the CRS-family parity, reusing the staged JAR
bash scripts/commands/gbx-test-parity.sh --skip-build -k crs

# Two named suites
bash scripts/commands/gbx-test-parity.sh --skip-build --path python/geobrix/test/pyvx/test_parity_mvt.py --path python/geobrix/test/pygx/test_parity_bng.py

# With a log file
bash scripts/commands/gbx-test-parity.sh --log parity.log
```
