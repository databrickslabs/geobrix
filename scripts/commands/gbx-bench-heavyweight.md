# gbx:bench:heavyweight

Run the heavyweight (Scala/JNI RasterX) benchmark in the `geobrix-dev` container over a generated corpus, writing a `heavyweight.jsonl` shard (same schema as the lightweight runner). Pure-core opens each tile with GDAL and calls `RST_*.execute`; spark-path times the registered `gbx_rst_*` Column on a local Spark DataFrame.

**Prereq:** generate the corpus first with `gbx:bench:gen-data` (its default output lands under the container mount at `/Volumes/main/default/bench-corpus`).

**Usage:** `bash scripts/commands/gbx-bench-heavyweight.sh [options]`

**Options:** `--corpus <dir>` (container path), `--out <path>` (container path), `--run-id <id>`, `--functions <list>`, `--set core|full`, `--modes pure-core|spark-path|both`, `--row-counts <list>`, `--warmup <n>`, `--measured <n>`, `--log <path>`, `--no-wait`, `--help`.

`--set` chooses the tier (`core` default, or `full`). The Scala heavy runner reads an explicit function list, so the selected tier is resolved to concrete `rst_*` names via the pyrx registry on the host and passed as `gbx.bench.functions`. An explicit `--functions` overrides `--set`.

**Examples:**
- `bash scripts/commands/gbx-bench-heavyweight.sh --run-id r1 --log hw.log`
- `bash scripts/commands/gbx-bench-heavyweight.sh --functions rst_slope,rst_ndvi --modes pure-core`
- `bash scripts/commands/gbx-bench-heavyweight.sh --set full --modes pure-core`

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command).

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

