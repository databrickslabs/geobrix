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
  - `affected`: the test files and family oracle tables the files changed between `--base` and `--sha` reach through the import graph (see "Affected"). It runs those files and tables only, never whole CI shards.
  - Names or groups, comma separated: `pyrx` (both halves), `"zarr 2/4"`, `scripts/ci`.
  - Test files, comma separated (`test/pyrx/test_env.py`, from `python/geobrix` or from the repo root): shards of just those files. For a quick re-run of the files a change touched.
    - Each file runs where its CI shard runs: `files` (light), `files heavy` (the JAR pool, with its integration tests).
    - The family file runs as `files family`, timed and cut by its oracle tables. `--env GBX_ORACLE_TABLES=<T>` plans it by those tables only.
- `--base <rev>` - for `affected`. Default: `origin/streamline/0.6.0`.
- `--allow-broad` - let `affected` run a selection over 25% of the light bank. Without it, such a selection is refused with the reason.
- `--sha <rev>` - the commit to test. Default: `HEAD`. Uncommitted edits are not in the run; the command names them.
- `--wheel <path>` - the env6 wheel, or a folder that holds it (the newest `geobrix-*+env6-py3-none-any.whl` there by mtime, then by version, as `--jar-path` takes a JAR from a folder). Default: `dist/env6/`. It carries the dependencies and the native payload, not the code under test.
- `--jar-path <path>` - the JAR of the heavy shards, or the folder that holds it. Default: `python/geobrix/lib/`.
- `--max-parallel <n>` - runners per cluster, so shards side by side. Default: the node's cores / 8 (4 on 32 vCPU); 8 runner tasks on Serverless.
- `--clusters <n>` - clusters to spread over. Default: every free cluster the pool has. A busy shared pool gives fewer, never a failure.
- `--split auto|off` - `auto` (default) cuts each shard longer than `--target-min` into parts of whole files. `off` keeps CI's shards. The cut sets the queue's order and grain, not which slot runs a part.
- `--target-min <n>` - the most minutes a chunk is planned at: a hard cap. Default: chosen from the history for about 3.5 chunks per slot, between 3 and 5 min. A file or table longer than it alone is cut into test parts.
- `--timings <path>` - the timing history of every run. Default: `test-logs/databricks-tests/timings-history.json`. The run cuts and orders the queue by it, then merges its own times in.
- `--serverless` - run the light shards on Serverless env 6, in small chunks (see "Serverless"). Pass a `--wheel` built at `--sha`: a Spark test's UDFs run the wheel, not the checkout.
- `--fresh-cluster` - use a new cluster that ends with the run, not the pools. Every run then pays a cold start.
- `--pool-light <name>`, `--pool-jar <name>` - the pools. Defaults: `gbx-test-1n` (no JAR), `gbx-test-1n-jar` (the JAR).
- `--node-type <t>` - the node of a pool cluster this run makes. Default: `m5d.8xlarge` (32 vCPU).
- `--shard-cap-min <n>` - stop a shard after n minutes; its runner takes the next. Default: 45.
- `--stall-min <n>` - stop a shard when no test finished for n minutes; its runner takes the next. Default: 10.
- `--start-cap-min <n>` - cancel when no shard has beaten n minutes after the submit. Default: 25.
- `--pool-wait-min <n>` - wait up to n minutes for a free pool cluster. Default: 30.
- `--first-failed <k>` - stop the run when the first k chunks to finish all failed. Default: 2. 0 turns it off.
- `--max-failed-chunks <n>` - stop the run when n chunks have failed. Default: 3. 0 turns it off.
- `--max-failures <n>` - stop the run when n tests have failed or errored. Default: 20. 0 turns it off.
- `--maxfail <n>` - pytest's `--maxfail` in each chunk, so a broken chunk stops early. Default: 3, and 1 with `--rerun-failed`. 0 turns it off.
- `--test-timeout <s>` - fail a test whose call runs longer than s seconds (setup and teardown get 3x). Default: 120. 0 turns it off.
- `--overrun-x <x>` - stop a chunk at x times its estimate, never under 3 min (5 min on Serverless). Default: 2. 0 turns it off.
  - Such a chunk is OVERRUN, not FAILED: its run and its runner go on, and its times become lower bounds. The run still exits non-zero, because its later tests did not run.
  - The smoke chunk has no overrun stop.
- `--max-wall-min <n>` - stop the run at n minutes of wall. Default: 20. 0 turns it off.
  - The budget of each pool starts when its job is submitted, after its cluster lease, not during the lease wait.
- `--start-chunks <n>` - runners that start at once. The rest start when n chunks have passed. Default: 2. 0 starts them all.
- `--rerun-failed <run id>` - run only the tests that failed in that earlier run (from its junit files under `test-logs/databricks-tests/<run id>/`), a chunk per file.
  - It also reruns a test that `--test-timeout` stopped with its chunk (the chunk's `result.json`, `timed_out`). Its junit holds no failure.
  - Each chunk runs where its file's CI shard runs: a heavy dir's file on the JAR pool, with its integration tests.
  - It takes the env of the chunk the test failed in (the run's `plan.json`).
  - A rerun chunk that collects nothing fails the run (a selection error).
- `--no-fail-fast` - run every chunk to its end: no stop rule and no `--maxfail`. The smoke chunk stays.
- `--no-smoke` - start the whole queue at once, without the smoke chunk first.
- `--pip-extra "<reqs>"` - more pip requirements the tests import.
- `--env KEY=VAL` - an environment variable every shard sets before its tests (repeatable). For example, `GBX_CAP_ARROW_TILE_TRANSPORT=0` runs the light tile functions with pickled rows.
- `--spark-conf KEY=VAL` - a Spark conf every shard's session sets before its tests (repeatable). For example, `spark.sql.execution.arrow.maxBytesPerBatch=16m` makes the Arrow batches of tile rows smaller.
- `--run-id <id>` - default `tst-<date>-<time>-<4 hex>`.
- `--keep-stage` - keep the Volume folder, the notebooks and the queue table.
- `--explain-only` - print the plan and stop. No network, nothing staged.
- `--log <path>` - write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` - show usage.

## Examples

- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --purpose "dirs my change touches"` (the `affected` shards)
- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --shards all-light --clusters 3 --purpose "light tier before a release"`
- `bash scripts/commands/gbx-test-databricks.sh --profile oauth-fe --serverless --shards "manager family 1/3" --purpose "family 1/3 on env6"`
- Add `--explain-only` to see the plan first.

## What it does

- Reads the shards from `.github/workflows/build_main.yml` (the `light` matrix and `light-scripts`) and from `LIGHT_IGNORES` in `.github/actions/python_build/action.yml` (one heavy shard per remaining dir). A shard added to CI is a shard here.
- Stages a tarball of the committed repo files at `--sha`, the env6 wheel and `plan.json` to `<volume>/test-runs/<run id>/`. The tarball holds every committed file except `src/test/resources` (staged once per content) and the doc images in `resources/`. On the node it becomes a one-commit repository, so `git ls-files` sees every file. It downloads the wheel and tarball back and compares sha256. The JAR goes to `<volume>/test-pool-jars/<sha256 prefix>/`, so the pool reinstalls it only when it differs.
- Leases the pool's single-node cluster (DBR 19, dedicated). It makes one when the pool has none. It never ends a pool cluster; the pool ends itself after 60 idle minutes (`gbx:bench:pool down` is the owner's call).
- Cuts long shards by file, so no part is much longer than the rest. The cut balances the recorded timings, or CI's minutes without timings.
- Leases the free clusters of the pool, up to `--clusters` (default: the pool's cap). It starts stopped clusters, and makes new ones while under the cap. It prints the plan: the queue, the clusters, the runners, the expected wall.
- Submits one job per pool. Its first task puts the shards into a queue table, longest first. A smoke task then runs the shortest chunk alone.
- Starts one runner task per slot (clusters x `--max-parallel`). Each runner claims the next shard, runs it, and claims again until the queue is empty.
- No slot idles while a shard waits, however far the actual times stray from the estimates.
- The queue is a Delta table in the Volume's schema (`gbx_test_queue_<run id>_<pool kind>`). A claim is an `UPDATE` of one row, from no owner to the runner.
- Delta commits are atomic, and two writes to the same row conflict: one claim wins, the other retries. A UC Volume's FUSE mount documents no exclusive create or atomic rename, so the claim does not use files.
- At most one heavy chunk runs on a cluster at a time (see "Heavy chunks").
- Each shard runs as a notebook run of its own (`dbutils.notebook.run`). It has its own Python process and Spark session, as a task of its own had.
- A failed, stopped or timed-out shard never stops its runner. The runner records it as not finished and takes the next.
- Reports the wall time against the longest shard and against running the shards one after another.
- Each shard installs the wheel in its own session. It forces `--force-reinstall --no-deps` of geobrix first, so a same-version geobrix already on the cluster cannot stand in for it.
  - A guard cell then fails the shard unless the driver and a Python worker import the staged wheel. It prints the build commit and the source digest.
  - The shard then unpacks the tarball and runs pytest there.
  - A pool cluster carries only JARs, and the notebook-scoped install lasts one session. An earlier run's wheel does not carry over.
  - The guard also covers a wheel library someone attached to the cluster.
  - The tests use the session the cluster provides (`python/geobrix/test/_provided_spark.py`).
- Prints a row per chunk the moment it ends, and stops the run early on failures (see "Fail fast").
- Downloads `junit.xml` and `pytest.log` per shard to `test-logs/databricks-tests/<run id>/<shard>/`. It prints one table and writes `summary.md` with the first lines of each failure and the skip reasons.
- Removes the Volume folder, the notebooks and the queue table after the download. Exit code 1 when any shard failed, errored or did not finish, or the run stopped early.

## What to expect

- The first all-light pass ran 8 chunks at once on one 32 vCPU node (2026-11-08). Each chunk ran 1.6 to 3.4 times slower than alone, because the tasks share one Spark driver JVM.
- A shard alone ran about 20% faster than CI's runner. The plan therefore uses 4 slots per node and gets its speed from more clusters.
- Estimate for the full light bank: about 205 min of work in 44 chunks. On 1 cluster that is about 50 min of wall; on 3 clusters about 18 min; on 4 about 14 min. Raise the pool's cap with `gbx:bench:pool up --name gbx-test-1n --n 3 --warm 0 --single-node --node-type m5d.8xlarge`; the run starts clusters on demand up to the cap.
- These numbers come from one partial run (cancelled with 12 chunks unstarted) and the CI minutes.
- Each run adds its times to the timing history (see below), so each plan cuts and orders by more runs.
- The summary prints a line per slot: its busy time, its wait while shards were queued, and its idle time at the end. Read those first.
  - The wait should be a few seconds per shard (the claim). A large wait means a slot idled while work was queued.
  - The idle time at the end is the tail: the last shards end at different times.
- The summary also checks isolation: one Python process per shard, and no temp view or conf left by an earlier shard.
- Each runner adds about 6 s per shard, for the claim and the start of the shard's notebook run. One run took 107 s over 18 shards on 4 slots.
- The order of the queue follows the estimates. A long shard estimated short starts last and makes the tail; the next run's timings correct it.
- `affected` runs only the test files and tables a change reaches. That is usually a few chunks and a few minutes.
  - Example: the TIN Rust kernel change (`7274d554d~1..66d5ead5b`) touched `rust/gbxrs/src/*`, `_rs/__init__.py`, `pyvx/_tin.py`, `pyvx/_conforming.py` and the pyvx tests.
  - It selects 22 files and 1 table, in 7 chunks.
  - The earlier per-directory rule selected 48 chunks, nearly the whole light bank.

## Fail fast

- Each chunk writes a one-line result the moment it ends: `<stage>/results/<shard>.json`. It holds the counts, the first failure (test id and a short message) and the chunk's own run ids.
- The launcher reads those every 15 s and prints a row per chunk. The row shows when it ended, the verdict, the counts, the chunk's run page and the first failure. It downloads the chunk's files at once.
- Every 2 min it prints a status line per pool. The line counts chunks done, failed, running and queued. It also names the first failure's test id and the running chunks' pages.
- The run stops at the first rule that fires:
  - the smoke chunk failed;
  - the first 2 chunks to finish both failed (`--first-failed`);
  - 3 chunks failed (`--max-failed-chunks`);
  - 20 tests failed or errored over the chunks (`--max-failures`);
  - the wall reached 20 min (`--max-wall-min`). The summary lists the chunks that did not run.
- A failed chunk has a failed or erroring test (an import error at collection included), or it ended without a result. A part that collects nothing is not a failed chunk, and neither is a chunk stopped at 2x its estimate with no test failed.
- A chunk that collects nothing is EMPTY in the summary. It fails the run unless the empty selection is intended:
  - a part of a split shard whose files the marker deselects;
  - a test part whose unit's other parts collected tests;
  - an `affected` subset.
- A unit none of whose test parts collected a test fails, and so does a `--rerun-failed` chunk that collects nothing.
- To stop, the launcher writes `<stage>/stop.json` and cancels every pool's job: the running chunks and the queue. Each runner also checks the rules, the marker and the wall before it claims.
- It prints `stopped early: <rule>` and the first failure of each failed chunk. `summary.md` and the run entry of the timing history record the rule.
- A chunk the stop cut short, or that pytest stopped at `--maxfail`, leaves lower bounds for what it did not finish, flagged with the rule.
- Job status tells the truth. A chunk with a failed test fails its own notebook run. A runner task that ran one fails too.
- Each chunk names its run first (`<stage>/out/<shard>/child.json`) and in the queue table (`child_run_id`, `child_job_id`). When it dies before pytest, its row shows that run's own error.
- Each launch has its own scratch folder on the node, named by run id and attempt. So a relaunch under the same run id meets no files of the last one.

## The smoke chunk

- It runs alone first, as a task the runners depend on. A failure stops the job before the queue, in about 1-3 min.
- It collects every test file the pool's chunks run, so an import error anywhere shows.
- It then runs the first 2 tests of the fastest chunk that collects any, with that chunk's env, `-k`, markers and test part. It tries the 3 fastest chunks in turn.
- When none of them collects a test, the smoke fails as a selection error, and says so.
- A pool that runs only `scripts/ci`, or a run of one chunk, has no smoke. `--no-smoke` starts the queue at once.

## The ramp and the caps

- Only 2 runners start after the smoke (`--start-chunks`). The others start once 2 chunks have passed (the smoke counts), or once nothing is left to wait for.
- No chunk is planned over `--target-min`. A file or table longer than that alone is cut into test parts: each part runs the tests whose node id hashes to it.
- A part's times count for its whole file or table (times the parts).
- A test that runs past `--test-timeout` fails, and the chunk goes on. The shard raises an exception inside the test. pytest-timeout does it where the env has it. A test that does not stop stops its chunk 60 s later.
- A chunk past 2x its estimate stops (`--overrun-x`). Its times are lower bounds, so the next plan cuts it finer.
- `--rerun-failed <run id>` runs only the tests that failed in that run, a chunk per file, each stopping at its first failure.
- Run `--no-fail-fast` for a full list of failures, for example before a fix that touches many tests.

## Serverless

- `--serverless` runs one runner task per slot, 8 by default (`--max-parallel`). Each takes chunks from the same queue as the pools do.
- Each runner task has compute of its own, so no chunk slows a neighbour. There is no heavy gate and no contention factor.
- The cut aims at 3-5 min per chunk, by test file or oracle table. A file or table that takes longer alone is cut into test parts.
- A Serverless chunk spends 1.5-3 min before its first test. A 3-5 min chunk there is about half setup.
- Each chunk records its setup phases from the runner's call: `phase=<name> t=<s>` in its output, `phases` in `result.json`, and a table in `summary.md`.
  - Run `tst-sls-ph1` (2026-11-12): the notebook run's start 24 s, the wheel guard cell 4-5 s, the checkout 13-22 s.
  - Then the worker wheel check 17-23 s, the queue note 3 s, collection 5 s. The first test started at 77-79 s.
  - The worker check now runs in a thread during the unpack: Serverless workers run the wheel, not the overlaid checkout.
- A Serverless run records its times under `sls:<key>` in the timing history. A Serverless plan uses them, else the pools' solo times; a pool plan never reads them.
- Run `t26-eq-sls3` (2026-11-11) planned 26 chunks of 30-76 min each from the pools' contention factors. The same selection now plans 22 chunks of 2-16 min.

## Affected

- The graph covers every module of `python/geobrix/src` and `python/geobrix/test` at `--sha`. It holds each module's imports (also those inside functions), its top-level symbols, and the names each symbol uses. A test also depends on the `conftest.py` of its folder and of each folder above it. The graph is read with `ast` from the commit's blobs and cached per commit in `test-logs/databricks-tests/.import-graph/`.
- A changed test file selects itself. A changed `conftest.py` selects the tests of its folder. A changed helper of the test tree selects the tests that import it.
- A changed source module selects the test files that import it, directly or through other modules.
  - Some modules are reached by more than 10% of the test files: a package `__init__`, a loader such as `_rs`, or `pyvx/functions.py`. Such a module is followed by symbol. Only the importers that use the changed symbols go on, or that use the names the module took from a changed module.
  - When the change in such a module is module-level code, not a symbol, only the tests that import it directly are selected.
- A change under `rust/gbxrs/src/` maps to the functions and classes the extension exports whose code changed, or whose code calls changed code (`TinKdTree`, `tin_delaunay`). It then maps to the Python modules that call those exports (the `_rs` call sites), followed as above. A change only inside the crate's `#[cfg(test)]` code reaches no Python test.
- An oracle table module (`bench/oracle_tables/*`) maps to its tables. They run as `GBX_ORACLE_TABLES` parts of the family harness, not the whole family shard.
- A changed manifest row maps to its oracle table and to the tests that name the row. A function whose light code (its row's `module:symbol` references) the change reaches maps the same way.
- `scripts/**` and `examples/tests/**` map to the `scripts/ci` tests that name the file, or that name a file that names it.
- The CI workflow and actions, the package's `pyproject.toml`, its requirements and `envs/` select the whole bank.
- A table reached only through changed manifest rows, or their light code, runs just those functions' rows: `-k <names>` on its chunk. Its estimate is the rows' share of the table. `--explain-only` marks it (`[only the rows of ...: -k]`, `, -k ...`). A table whose own module changed runs whole.
- The selected files run as one chunk set per CI shard group (`pyrx affected`, `manager family 2/3 affected`), with that shard's marker, pool and options; `--split auto` cuts them as usual. A part that collects nothing (only integration tests) does not fail the run.
- `--explain-only` prints, per selected file and table, the changed file and the import chain (`test.pyvx.test_tin_mode_arg -> pyvx.functions -> pyvx._tin`).
- A selection over 25% of the light bank's units (files, and the family file as its tables) is refused unless `--allow-broad` is given. The message is loud and names why the selection is so broad.

## Heavy chunks and the watchdog

- A chunk is heavy when it holds a table of the GDAL spec engine (`SPEC_*`, listed in `HEAVY_TABLES` of `_test_shards.py`). It is also heavy when it holds a file or table that the timing history records at 15 min or more alone.
  - The engine tables of family 3/3 once slowed the chunks beside them by about 10x on the node's one driver.
  - Both parts of that shard then stopped at the 45 min cap.
- Heavy units are cut into chunks apart from the light ones, so a chunk is heavy as a whole. `--explain-only` marks them (`, heavy`) and lists them.
- The runners of one cluster share a lock on the node's local disk. A runner takes it before it claims, and keeps it only for a heavy chunk, so at most one heavy chunk runs per cluster. The other slots take light chunks meanwhile; when only heavy chunks are left, a runner waits for the one that runs. The expected wall in the plan counts those waits, and the runner task's timeout allows for them.
- The stall cap (`--stall-min`, no test finished) of a chunk is at least half of the recorded time of its longest file or table, never past `--shard-cap-min`. `--explain-only` lists the chunks whose cap is above the flat one.
- When the watchdog stops a shard (the shard cap or the stall cap), the shard's reason, the runner's record and `summary.md` say `killed by watchdog: <why>`.
  - The watchdog ends a stuck process with SIGKILL (exit 137).
  - Databricks reports that as "Execution ran out of memory" or "kernel unresponsive". The message is the platform's label for any killed process. It does not mean an out-of-memory.

## Contention

- On 2 clusters x 4 slots (run `tst-20261110-210406-c575`), some estimates collapsed:
  - `zarr 4/4` was expected at 9 min and took about 46.
  - `zarr 3/4` was expected at 9 min and took about 30.
  - `manager family 3/3` (GDAL engine tables) was expected at 9 min and took 45, the cap.
- Each run records, per unit (a test file or `table:<T>`), its actual time over its solo-expected time. The solo-expected time is the plan's estimate without contention factors. The record also holds the chunks that ran beside it on its cluster.
- A unit's contention factor is the median of those ratios over its last 5 runs with neighbours. A factor of 2 or more marks it contention-heavy. It then joins the heavy gate: at most one per cluster at a time, and the other slots take light chunks meanwhile.
- The planner plans a unit at its solo time times its factor, for the queue order and the expected wall. A sample taken beside neighbours is divided by its run's ratio before it joins the solo estimate, so the factor is not counted twice.
- A ratio counts only when the unit ran 60 s or more. A part of a split CI shard estimated by its share of CI's minutes gives no ratio, because that share is a guess. A chunk stopped at a cap gives its ratio only when it is 2 or more, since it is then a lower bound.
- `--explain-only` marks a chunk whose estimate carries a factor (`x4.76`) and lists the contention-heavy units. The summary gives each chunk's measured contention, how many chunks ran beside it, and each cluster's peak concurrency.
- The factors were backfilled once from today's runs under `test-logs/databricks-tests/tst-*` with `python examples/tests/_timings.py <history> <runs dir> contention`. That run flagged 10 units: `test/zarr/test_write.py` (5.1x), `test_maint.py` (3.4x), `test_register.py` and `test_sync.py` (2.6x), and the tables `SPEC_GDAL` (5.1x), `SPEC_ANALYSIS`, `SPEC_WARP` (2.2x), `MVP_S`, `GRID_GENERATOR` and `RASTER_TO_GRID` (2.1x).

## Timing history

- Every run merges its times into the history: passed, failed, partial or cancelled. No run overwrites another's samples.
- A key is a test file, or an oracle table of `test_family_cross_tier.py` (`table:<TABLE>`). The family file's other tests are `table:*`.
- A sample is normalized to a chunk running alone, so runs at different slot counts pool. An estimate is the median of a key's last 5 samples.
- A chunk stopped by `--shard-cap-min`, `--stall-min` or a crash leaves lower bounds for the files it did not finish.
  - The next plan plans such a file at its bound at least, cut into test parts when that passes the target. A later full run replaces the bound.
- History wins over CI's minutes from a key's first sample. A file with no sample takes its share of CI's minutes.
- The cut uses the history too. A file or table longer than `--target-min` gets a chunk of its own; the family file is cut by table.
- `--explain-only` marks each chunk's estimate: `h` history, `p` partly history, `lb` a lower bound, `ci` CI's minutes.
- The summary prints each chunk's predicted and actual seconds and the error. The actual time is the chunk's notebook run.
- Writers lock `<history>.lock` and replace the file atomically, so runs that end together keep both their samples.
- The history started from the junit files of the runs under `test-logs/databricks-tests/` (`python examples/tests/_timings.py <history> <runs dir>`).

## Known failures on a node

Run `tst-nodefix-1` (commit `109a45ad80`: the `ds` shards, `bench 3/3` and `scripts/ci`, 19 chunks) passed every test: 4738 passed, 0 failed, 6 skipped. What a node does differently from a local session, and how the tests allow for it:

- Spark resolves a bare path on DBFS. The provided session (`python/geobrix/test/_provided_spark.py`) therefore reads a bare local path as a `file:` URI. A test's `tmp_path` is an example.
  - It does so in `load` and in the format readers (`parquet`, `csv`, `json`, `orc`, `text`).
  - It writes a bare local path as a `file:` URI in `DataFrameWriter.save`, in the format writers and in `DataStreamWriter.start`.
  - It leaves alone a path in `/Volumes`, `/dbfs` or `/Workspace`, and a path with a scheme.
  - Not covered: `option("path", ...)`, a `saveAsTable` path, and `CREATE TABLE ... LOCATION '<local path>'`. Unity Catalog refuses the last one ("Missing cloud file system scheme").
  - A test that needs a table uses a managed table in the session's default schema.
- A node probes as a FILE tier (`read_files`), which lists a local path through DBFS. The FILE-leg tests of `test/bench` (`fuse_tier` in its `conftest.py`) pin the FUSE tier they are about.
- The runtime's own SQL parser accepts the Databricks-only statements that open-source Spark rejects; `test_t28_files` checks that rejection only off Databricks.
- A node's interpreter reads no `.pth`; the shard runner activates the native payload through a `sitecustomize` on `PYTHONPATH`. A test that starts a Python child keeps that `PYTHONPATH` (the readdir probe of `bench/local_timing.py` does).
- The wheel's `rasterio` and `osgeo` sit under `_vendor/` in the package. Tests that scan the package source for GeoBrix's own anti-patterns skip it.
- A run that ignores SIGHUP (a notebook, `nohup`) hands that to a bash child, which cannot trap it. The `scripts/ci` sandbox resets SIGINT and SIGHUP in the children it starts.
- The Python data source calls `read` once with a bare `InputPartition` when no partition is planned (the open-source session passes `None`); the raster reader returns nothing for either.
- With 32 slots `DataFrame.repartition(n)` over one-row partitions is not even; `levels_family.spread_evenly` is.
- The overture tests that mix Spark and file I/O on a bare path skip under the provided session, with the reason.
- `test/bench/test_levels.py`'s worker-peak tests inspect Spark's local-mode Python workers and cannot pass against the cluster's session.
- These skips remain:
  - no Rust toolchain or vendored crates on the node;
  - no QC judge;
  - no mutation venv;
  - `/tmp` is not a symlink off macOS.
- The family oracle chunks need several minutes each, and the GDAL engine tables are heavy (see above); the Scala suites never run here.

## Limits

- The Scala suites stay local (`gbx:test:scala`): a cluster has no Maven build.
- A task runs its tests from the tarball's package. A change to the dependencies or the native payload needs a new wheel (`gbx:build:wheel --env 6`); the command warns when the wheel predates one.
- Tests that need a Docker-only path or the local sample-data mount skip or fail as they do without the mount. `summary.md` lists the skips.
- Cost: one on-demand node per pool while a run is on it and for 60 idle minutes after.
