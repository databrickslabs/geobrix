# Run Scala Unit Tests

Runs Scala unit tests (non-documentation tests) using Maven.

## Usage

```bash
bash scripts/commands/gbx-test-scala.sh [OPTIONS]
```

## Options

- `--by-package` - Run tests per package in sequence (rasterx, gridx, vectorx, expressions, util); reports pass/fail per package (same packages as CI matrix)
- `--suite <pattern>` - Run specific test suite (single pattern)
- `--suites <list>` - Run specific test suites (comma-separated class/package patterns; same as `-Dsuites=`)
- `--spark-version <v>` - Build and test against another Spark release: `4.0.x` (default build), `4.1.x`, `4.2.x` or `4.3.x`. See "Spark version builds".
- `--compile-only` - Compile main and test sources (`mvn test-compile`) and run no suites.
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--verbose` - Show detailed Maven output
- `--help` - Display help message

## Locale

The container has no `LANG`/`LC_ALL`. That would leave the JVM's file-name encoding at ASCII and cancel the tests that create non-ASCII file names. An example is the non-ASCII case in `NodeFilePathUtilTest`. The command passes `-e LC_ALL=C.UTF-8` to the container, so those tests run.

## The native payload

Set `GBX_NATIVE_ROOT` in the calling shell to run the test JVM on a native payload, for example `GBX_NATIVE_ROOT=build/env-wheel/env6/payload` (from `gbx:build:wheel --env 6`). The command passes it into the container as its `/root/geobrix` path (a path under this checkout, relative or absolute). Any other absolute path is taken as a container path. `GDALManager` loads that payload's GDAL JNI instead of the system one. Suites that need the manifest's pinned GDAL (the spec engine runs in `GdalSpecSmokeTest`) cancel with a `PRECONDITION MISSING` line without it. Unset, nothing is passed.

## Spark version builds

The shipped JAR is built on Spark 4.0.0 and must run on 4.0 to 4.3. `--spark-version` checks that.

- It passes `-Dspark.version`, `-Dscala.version` and `-Djackson.version` to Maven. The Scala and jackson versions come from that Spark release's `spark-parent_2.13` POM. Spark 4.1 uses 2.13.17 and jackson 2.20.0. Spark 4.2 and 4.3 use 2.13.18 and 2.21.2.
- A version other than 4.0.0 builds into `target/spark-<version>/` (`-Dgbx.build.dir`). The default `target/` and its JARs are not touched.
- With no option, or `--spark-version 4.0.0`, the build is the default build.
- The suite list the heavy CI job runs is `.github/actions/scala_build/suites-heavy.txt`. Pass it with `--suites "$(grep -vE '^(#|$)' .github/actions/scala_build/suites-heavy.txt | paste -sd, -)"`.
- Another Spark minor needs its scala and jackson versions added to the `case` in the script.

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command).

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`). The *run* lock is held while this command or any child of it is alive, which makes whole runs take turns. The *work* lock is held by the Maven or pytest process itself for as long as it runs. The lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork. They normally exit with their parent. If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees. The container's Maven or pytest keeps running and keeps the work lock. The next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits. It gives up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Run all Scala unit tests
bash scripts/commands/gbx-test-scala.sh

# Run specific test suite
bash scripts/commands/gbx-test-scala.sh --suite com.databricks.labs.gbx.rasterx.expressions.*

# Run multiple suites (comma-separated)
bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.rasterx.operations.SpatialRefOpsTest,com.databricks.labs.gbx.rasterx.operations.RasterAccessorsTest'

# Run with logging
bash scripts/commands/gbx-test-scala.sh --log scala-tests.log

# Run with verbose output
bash scripts/commands/gbx-test-scala.sh --verbose

# Test against Spark 4.3.0, or only compile against 4.2.0
bash scripts/commands/gbx-test-scala.sh --spark-version 4.3.0 --suites 'com.databricks.labs.gbx.gridx.*'
bash scripts/commands/gbx-test-scala.sh --spark-version 4.2.0 --compile-only

# Run by package (sequence; same packages as CI "Scala tests (by package)" workflow)
bash scripts/commands/gbx-test-scala.sh --by-package
```

## Test Location

- **Source**: `src/test/scala/com/databricks/labs/gbx/` (excludes `tests.docs.scala.*`)

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-scala-docs` for those)
- Uses Maven profile to skip scoverage for faster execution
- Default log location: `test-logs/` (if filename only provided)
