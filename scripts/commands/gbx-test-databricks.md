# gbx:test:databricks

Run GeoBrix's test shards on warm Databricks pool clusters, side by side. It beats the local cores and the Docker test lock, and tests on real DBR 19 with env6. The local Docker loop stays for the inner loop and the Docker-only suites.

## Usage

`bash scripts/commands/gbx-test-databricks.sh --profile <name> --purpose "<why>" [--shards <spec>] [OPTIONS]`

## Options

- `--profile <name>` - the Databricks profile. Required on a launch; the command never picks one.
- `--purpose "<why>"` - required on a launch. It heads each runner notebook and rides on the run name.
- `--shards <spec>` - what to run. Default: `affected`, the pre-push pass.
  - `all-light`: every light shard of CI, plus `scripts/ci`.
  - `all-heavy`: one shard per heavy test dir (needs the JAR pool).
  - `affected`: the shards that hold a test touched by the files changed between `--base` and `--sha`.
  - Names or groups, comma separated: `pyrx` (both halves), `"zarr 2/4"`, `scripts/ci`.
- `--base <rev>` - for `affected`. Default: `origin/streamline/0.6.0`.
- `--sha <rev>` - the commit to test. Default: `HEAD`. Uncommitted edits are not in the run; the command names them.
- `--wheel <path>` - the env6 wheel. Default: `dist/env6/`. It carries the dependencies and the native payload, not the code under test.
- `--jar-path <path>` - the JAR of the heavy shards, or the folder that holds it. Default: `python/geobrix/lib/`.
- `--max-parallel <n>` - runners per cluster, so shards side by side. Default: the node's cores / 8 (4 on 32 vCPU).
- `--clusters <n>` - clusters to spread over. Default: every free cluster the pool has. A busy shared pool gives fewer, never a failure.
- `--split auto|off` - `auto` (default) cuts each shard longer than `--target-min` into parts of whole files. `off` keeps CI's shards. The cut sets the queue's order and grain, not which slot runs a part.
- `--target-min <n>` - the length a cut shard aims at. Default: chosen for about 3.5 chunks per slot, between 3.5 and 10 min. A task spends about 0.7 min before its first test.
- `--timings <path>` - per-file seconds from earlier runs. Default: `test-logs/databricks-tests/timings.json`. The run reads it to cut and order the queue (longest first) and writes it from the junit files.
- `--serverless` - run the light shards on Serverless env 6 (a fidelity leg). Spark-free dirs only.
- `--fresh-cluster` - use a new cluster that ends with the run, not the pools. Every run then pays a cold start.
- `--pool-light <name>`, `--pool-jar <name>` - the pools. Defaults: `gbx-test-1n` (no JAR), `gbx-test-1n-jar` (the JAR).
- `--node-type <t>` - the node of a pool cluster this run makes. Default: `m5d.8xlarge` (32 vCPU).
- `--shard-cap-min <n>` - stop a shard after n minutes; its runner takes the next. Default: 45.
- `--stall-min <n>` - stop a shard when no test finished for n minutes; its runner takes the next. Default: 10.
- `--start-cap-min <n>` - cancel when no shard has beaten n minutes after the submit. Default: 25.
- `--pool-wait-min <n>` - wait up to n minutes for a free pool cluster. Default: 30.
- `--pip-extra "<reqs>"` - more pip requirements the tests import.
- `--run-id <id>` - default `tst-<date>-<time>-<4 hex>`.
- `--keep-stage` - keep the Volume folder, the notebooks and the queue table.
- `--explain-only` - print the plan and stop. No network, nothing staged.
- `--log <path>` - write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` - show usage.

## Examples

- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --purpose "dirs my change touches"` (the `affected` shards)
- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --shards all-light --clusters 3 --purpose "light tier before a release"`
- Add `--explain-only` to see the plan first.

## What it does

- Reads the shards from `.github/workflows/build_main.yml` (the `light` matrix and `light-scripts`) and from `LIGHT_IGNORES` in `.github/actions/python_build/action.yml` (one heavy shard per remaining dir). A shard added to CI is a shard here.
- Stages a tarball of the committed repo files at `--sha`, the env6 wheel and `plan.json` to `<volume>/test-runs/<run id>/`. The tarball holds every committed file except `src/test/resources` (staged once per content) and the doc images in `resources/`. On the node it becomes a one-commit repository, so `git ls-files` sees every file. It downloads the wheel and tarball back and compares sha256. The JAR goes to `<volume>/test-pool-jars/<sha256 prefix>/`, so the pool reinstalls it only when it differs.
- Leases the pool's single-node cluster (DBR 19, dedicated). It makes one when the pool has none. It never ends a pool cluster; the pool ends itself after 60 idle minutes (`gbx:bench:pool down` is the owner's call).
- Cuts long shards by file, so no part is much longer than the rest. The cut balances the recorded timings, or CI's minutes without timings.
- Leases the free clusters of the pool, up to `--clusters` (default: the pool's cap). It starts stopped clusters, and makes new ones while under the cap. It prints the plan: the queue, the clusters, the runners, the expected wall.
- Submits one job per pool. Its first task puts the shards into a queue table, longest first.
- Starts one runner task per slot (clusters x `--max-parallel`). Each runner claims the next shard, runs it, and claims again until the queue is empty.
- No slot idles while a shard waits, however far the actual times stray from the estimates.
- The queue is a Delta table in the Volume's schema (`gbx_test_queue_<run id>_<pool kind>`). A claim is an `UPDATE` of one row, from no owner to the runner.
- Delta commits are atomic, and two writes to the same row conflict: one claim wins, the other retries. A UC Volume's FUSE mount documents no exclusive create or atomic rename, so the claim does not use files.
- Each shard runs as a notebook run of its own (`dbutils.notebook.run`). It has its own Python process and Spark session, as a task of its own had.
- A failed, stopped or timed-out shard never stops its runner. The runner records it as not finished and takes the next.
- Reports the wall time against the longest shard and against running the shards one after another.
- Each shard installs the wheel in its own session, unpacks the tarball and runs pytest there. The tests use the session the cluster provides (`python/geobrix/test/_provided_spark.py`).
- Downloads `junit.xml` and `pytest.log` per shard to `test-logs/databricks-tests/<run id>/<shard>/`. It prints one table and writes `summary.md` with the first lines of each failure and the skip reasons.
- Removes the Volume folder, the notebooks and the queue table after the download. Exit code 1 when any shard failed, errored or did not finish.

## What to expect

- The first all-light pass ran 8 chunks at once on one 32 vCPU node (2026-11-08). Each chunk ran 1.6 to 3.4 times slower than alone, because the tasks share one Spark driver JVM.
- A shard alone ran about 20% faster than CI's runner. The plan therefore uses 4 slots per node and gets its speed from more clusters.
- Estimate for the full light bank: about 205 min of work in 44 chunks. On 1 cluster that is about 50 min of wall; on 3 clusters about 18 min; on 4 about 14 min. Raise the pool's cap with `gbx:bench:pool up --name gbx-test-1n --n 3 --warm 0 --single-node --node-type m5d.8xlarge`; the run starts clusters on demand up to the cap.
- These numbers come from one partial run (cancelled with 12 chunks unstarted) and the CI minutes.
- The next real run measures the plan and rewrites `timings.json`.
- The summary prints a line per slot: its busy time, its wait while shards were queued, and its idle time at the end. Read those first.
  - The wait should be a few seconds per shard (the claim). A large wait means a slot idled while work was queued.
  - The idle time at the end is the tail: the last shards end at different times.
- The summary also checks isolation: one Python process per shard, and no temp view or conf left by an earlier shard.
- Each runner adds about 6 s per shard: the claim and the start of the shard's notebook run (2026-11-10: 107 s over 18 shards on 4 slots).
- The order of the queue follows the estimates. A long shard estimated short starts last and makes the tail; the next run's timings correct it.
- `affected` runs only the shards that hold a changed test, which is usually a few chunks and a few minutes.

## Known failures on a node

- Spark reads a bare path from DBFS on Databricks. The provided session therefore reads a bare local path (a test's `tmp_path`) as a `file:` URI in `DataFrameReader.load` and `DataStreamReader.load`. A test that writes through Spark to a bare local path still writes to DBFS.
- The overture tests that mix Spark and file I/O on a bare path skip under the provided session, with the reason.
- `test/bench/test_levels.py`'s worker-peak tests inspect Spark's local-mode Python workers and cannot pass against the cluster's session.
- The family oracle chunks need several minutes each; the Scala suites never run here.

## Limits

- The Scala suites stay local (`gbx:test:scala`): a cluster has no Maven build.
- A task runs its tests from the tarball's package. A change to the dependencies or the native payload needs a new wheel (`gbx:build:wheel --env 6`); the command warns when the wheel predates one.
- Tests that need a Docker-only path or the local sample-data mount skip or fail as they do without the mount. `summary.md` lists the skips.
- Cost: one on-demand node per pool while a run is on it and for 60 idle minutes after.
