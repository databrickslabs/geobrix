# Cross-Tier Parity Tests

Run the light-vs-heavy (`pyvx` vs `vectorx`) parity suites as a **real gate**: rebuild the assembly JAR, stage it where the light tier loads it, run the integration-marked parity suites, and fail if nothing actually ran.

## Usage

```bash
bash scripts/commands/gbx-test-parity.sh [OPTIONS]
```

## Options

- `--path <path>` - Suite path (default: `python/geobrix/test/pyvx/`)
- `-k <expr>` - Pytest keyword filter (e.g. `crs`)
- `--skip-build` - Reuse the already-staged JAR (fast; only when Scala is unchanged)
- `--log <path>` - Write output to a log file (filename → `test-logs/<name>`)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help` - Display help message

## Why this command exists

Parity suites are marked `@pytest.mark.integration` and need the assembly JAR staged in `python/geobrix/lib/`, because they start a Spark session with `spark.jars` pointing at it to call the heavy tier. In a normal `gbx:test:python` run they are excluded by the default `not integration` marker filter, and even with `--with-integration` they self-skip when no JAR is staged.

That means the parity gate can silently not-run. **A skipped suite reads as green, which is worse than an absent one** — so this command:

1. **Rebuilds the JAR by default.** A staged JAR older than the Scala sources makes every heavy call fail with `UNRESOLVED_ROUTINE`: a mass failure that looks like a code bug but is only a stale artifact. Rebuilding first removes that failure mode entirely.
2. **Stages exactly one JAR**, deleting older ones so a stale artifact cannot be picked up instead.
3. **Runs with integration enabled** — parity is the whole point of the run.
4. **Fails when 0 tests passed**, printing the likely cause (no staged JAR, or a JAR-free Spark session already live in the process). This is what stops a fully-skipped run from being mistaken for a pass.

## When to use

- Before pushing a change that touches CRS handling, geometry encoding, or anything implemented in both tiers.
- After changing any Scala expression that has a light-tier counterpart (the rebuild is what makes the heavy side current).
- As the final gate for a both-tiers feature, alongside `gbx:test:bindings`.

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first: a `flock` on `/tmp/gbx-test.lock` inside the container, shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, and `gbx:docker:exec` with an `mvn` or `pytest` command), so two runs never share `target/`.

- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- The lock is released when the command exits, and also when it is killed. Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Full gate: rebuild, stage, run everything
bash scripts/commands/gbx-test-parity.sh

# Just the CRS-family parity, reusing the staged JAR
bash scripts/commands/gbx-test-parity.sh --skip-build -k crs

# With a log file
bash scripts/commands/gbx-test-parity.sh --log parity.log
```
