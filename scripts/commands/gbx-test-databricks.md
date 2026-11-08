# gbx:test:databricks

Run GeoBrix's test shards on warm Databricks pool clusters, side by side. It beats the local cores and the Docker test lock, and tests on real DBR 19 with env6. The local Docker loop stays for the inner loop and the Docker-only suites.

## Usage

`bash scripts/commands/gbx-test-databricks.sh --profile <name> --purpose "<why>" [--shards <spec>] [OPTIONS]`

## Options

- `--profile <name>` - the Databricks profile. Required on a launch; the command never picks one.
- `--purpose "<why>"` - required on a launch. It heads each runner notebook and rides on the run name.
- `--shards <spec>` - what to run. Default: `all-light`.
  - `all-light`: every light shard of CI, plus `scripts/ci`.
  - `all-heavy`: one shard per heavy test dir (needs the JAR pool).
  - `affected`: the shards that hold a test touched by the files changed between `--base` and `--sha`.
  - Names or groups, comma separated: `pyrx` (both halves), `"zarr 2/4"`, `scripts/ci`.
- `--base <rev>` - for `affected`. Default: `origin/streamline/0.6.0`.
- `--sha <rev>` - the commit to test. Default: `HEAD`. Uncommitted edits are not in the run; the command names them.
- `--wheel <path>` - the env6 wheel. Default: `dist/env6/`. It carries the dependencies and the native payload, not the code under test.
- `--jar-path <path>` - the JAR of the heavy shards. Default: `python/geobrix/lib/`.
- `--max-parallel <n>` - shards side by side per cluster. Default: the node's cores / 4 (8 on 32 vCPU).
- `--clusters <n>` - clusters to spread over. Default: every free cluster the pool has. A busy shared pool gives fewer, never a failure.
- `--split auto|off` - `auto` (default) cuts each shard longer than `--target-min` into parts of whole files. `off` keeps CI's shards.
- `--target-min <n>` - the length a cut shard aims at. Default: 6. A task spends about 0.7 min before its first test, so keep it several times that.
- `--timings <path>` - per-file seconds from earlier runs. Default: `test-logs/databricks-tests/timings.json`. The run reads it to cut and order tasks (longest first) and writes it from the junit files.
- `--serverless` - run the light shards on Serverless env 6 (a fidelity leg). Spark-free dirs only.
- `--fresh-cluster` - use a new cluster that ends with the run, not the pools. Every run then pays a cold start.
- `--pool-light <name>`, `--pool-jar <name>` - the pools. Defaults: `gbx-test-1n` (no JAR), `gbx-test-1n-jar` (the JAR).
- `--node-type <t>` - the node of a pool cluster this run makes. Default: `m5d.8xlarge` (32 vCPU).
- `--shard-cap-min <n>` - stop a shard after n minutes. Default: 45.
- `--stall-min <n>` - stop a shard when no test finished for n minutes. Default: 10.
- `--start-cap-min <n>` - cancel when no shard has beaten n minutes after the submit. Default: 25.
- `--pool-wait-min <n>` - wait up to n minutes for a free pool cluster. Default: 30.
- `--pip-extra "<reqs>"` - more pip requirements the tests import.
- `--run-id <id>` - default `tst-<date>-<time>-<4 hex>`.
- `--keep-stage` - keep the Volume folder and the runner notebook.
- `--explain-only` - print the plan and stop. No network, nothing staged.
- `--log <path>` - write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` - show usage.

## Examples

- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --shards all-light --purpose "light tier before a push"`
- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --shards affected --purpose "dirs my change touches"`
- Add `--explain-only` to see the plan first.

## What it does

- Reads the shards from `.github/workflows/build_main.yml` (the `light` matrix and `light-scripts`) and from `LIGHT_IGNORES` in `.github/actions/python_build/action.yml` (one heavy shard per remaining dir). A shard added to CI is a shard here.
- Stages a tarball of the committed repo files at `--sha`, the env6 wheel and `plan.json` to `<volume>/test-runs/<run id>/`. It downloads the wheel and tarball back and compares sha256. The JAR goes to `<volume>/test-pool-jars/<sha256 prefix>/`, so the pool reinstalls it only when it differs.
- Leases the pool's single-node cluster (DBR 19, dedicated). It makes one when the pool has none. It never ends a pool cluster; the pool ends itself after 60 idle minutes (`gbx:bench:pool down` is the owner's call).
- Cuts long shards by file (stable, balanced by the timings; without timings, by the shard's CI minutes), so no task is much longer than the rest.
- Leases the free clusters of the pool, up to `--clusters`. It prints the plan: tasks, clusters, slots, expected wall.
- Submits one job per pool. Each shard is a task, so it runs in its own Python process. The tasks go into clusters x slots lanes, longest first; the lanes run side by side, and a failed shard does not skip the next one in its lane.
- Reports the wall time against the longest task and against running the tasks one after another.
- Each task installs the wheel in its own session, unpacks the tarball and runs pytest in that process, on the session the cluster provides (`python/geobrix/test/_provided_spark.py`).
- Downloads `junit.xml` and `pytest.log` per shard to `test-logs/databricks-tests/<run id>/<shard>/`, prints one table and writes `summary.md` with the first lines of every failure and the skip reasons.
- Removes the Volume folder and the runner notebooks after the download. Exit code 1 when any shard failed, errored or did not finish.

## Limits

- The Scala suites stay local (`gbx:test:scala`): a cluster has no Maven build.
- A task runs its tests from the tarball's package. A change to the dependencies or the native payload needs a new wheel (`gbx:build:wheel --env 6`); the command warns when the wheel predates one.
- Tests that need a Docker-only path or the local sample-data mount skip or fail as they do without the mount. `summary.md` lists the skips.
- Cost: one on-demand node per pool while a run is on it and for 60 idle minutes after.
