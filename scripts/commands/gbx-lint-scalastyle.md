# Run ScalaStyle (Scala Lint)

Runs ScalaStyle on `src/main/scala` using the same config as CI (`scalastyle-config.xml`). Use this to catch style warnings and errors before pushing.

## Usage

```bash
bash scripts/commands/gbx-lint-scalastyle.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file (filename → test-logs/filename.log)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help` - Display help

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first: a `flock` on `/tmp/gbx-test.lock` inside the container, shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, and `gbx:docker:exec` with an `mvn` or `pytest` command), so two runs never share `target/`.

- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- The lock is released when the command exits, and also when it is killed. Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Run ScalaStyle (in Docker)
gbx:lint:scalastyle

# Run and save output
gbx:lint:scalastyle --log scalastyle.log
```

## Notes

- Runs inside Docker container `geobrix-dev`.
- Same rules as CI: `scalastyle-config.xml`; CI fails on **errors** only (`failOnWarning: false`).
- Fix common issues: trailing whitespace, missing newline at EOF, non-ASCII in comments (use ASCII equivalents: `->` not `→`, `-` not `–`, `...` not `…`).
