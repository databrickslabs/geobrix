# gbx:bench:cluster

Submit the heavy-vs-light benchmark as a one-off notebook job to a **provisioned** Databricks cluster. Both APIs run on the same cluster, which is the true same-hardware comparison. Results append to the `bench_results` Delta table, and `comparison.csv` and `summary.md` land on the configured Volume.

## Usage

`bash scripts/commands/gbx-bench-cluster.sh [options]`

Prerequisites (operator):

- A provisioned cluster and a filled `examples/tests/databricks_cluster_config.env`.
- Heavyweight needs a dedicated cluster on DBR 17.3, 18 or 19. Install the env wheel for the runtime, the GeoBrix JAR and the bench `geobrix-*-tests.jar` as cluster libraries.
- Lightweight (including ARM) needs the env wheel for the runtime: `geobrix-<version>+env5` on DBR 17.3 / 18, `+env6` on DBR 19 and Serverless environment 6. Use no light extra.

## Options

### Target and run

- `--cluster-id <id>`: the target cluster. Sets the same env var as `CLUSTER_ID` in the env file, and overrides it.
- `--existing-cluster-id <id>`: attach the run to a warm all-purpose cluster. This skips the 4-8 min job-cluster startup. The Python launcher receives it. It is the same as `CLUSTER_ID` in the env file.
- `--serverless`: submit to Serverless (lightweight only, no JAR). It skips `--cluster-id`. The profile must point to a workspace that supports Serverless jobs.
- `--env-version <N>`: the Serverless environment version to pin. Used only with `--serverless`. Default: 6.
- `--run-id <id>`: the run id. Default: `cluster-<date>-<time>-<4 hex>`, new for every launch. See [Run id and notebook path](#run-id-and-notebook-path).
- `--no-wait`: submit without waiting for the run to end.
- `--log <path>`: write the output to a log. A bare filename goes to `test-logs/<name>`.
- `--help`, `-h`: show the help.

### Job cluster

`--new-cluster` runs the notebook on a cluster made for the run. Databricks ends the cluster when the run ends. `CLUSTER_ID` from the config file is not used. The task has `max_retries=0`.

- `--new-cluster`: use a job cluster.
- `--spark-version <v>`: the Spark version. Default: `19.x-scala2.13`.
- `--node-type <t>`: the node type. Default: `m6gd.xlarge`.
- `--num-workers <n>`: the worker count. Default: 1.
- `--access-mode dedicated|standard`: the access mode. Default: `dedicated` (single-user for the caller).
- `--on-demand`: AWS only. Puts every node on demand, so a spot node taken back cannot cut a measurement. Without it, the workspace's spot policy applies.

### Which functions run

- `--functions <csv>`: the `rst_*` names to run. It overrides `--set`. With `--grid-bng-only`, `bng_*` names pick the BNG legs. See [BNG legs](#bng-legs).
- `--set core|full`: the selection tier. Default: `core`.
- `--modes pure-core|spark-path|both`: which legs run. Default: `both`.
- `--row-counts <csv>`: the spark-path row ladder. Default: `10,100,1000,10000`.
- `--warmup <n>`: pure-core warmup iterations. Default: 1.
- `--measured <n>`: pure-core measured iterations. Default: 3.
- `--spark-warmup <n>`: spark-path warmup iterations. Default: 1.
- `--spark-measured <n>`: spark-path measured iterations. Default: 1.
- `--heavyweight-only`: skip the lightweight leg.
- `--lightweight-only`: skip the heavyweight leg.
- `--input-tile materialized|virtual`: the input tile mode for the lightweight spark-path leg. `materialized` is a bytes column and matches all earlier bench runs. `virtual` is a path-and-window tile and exercises the virtual-tile reader path. Default: `materialized`.
- `--disable-file`: set `GBX_DISABLE_FILE=1` in the notebook. This is the FILE-off A/B leg. Use it with `--input-tile virtual`. Do not combine it with `--grouped-file`.
- `--max-partition-bytes <v>`: set `spark.sql.files.maxPartitionBytes` (for example `32m` or `16m`) in the run's preamble.
  - It caps bytes per partition on file and Delta scans. Smaller partitions mean fewer rows and less memory per decode task.
  - Unlike AQE, this conf is settable on Serverless, so it works on classic and `--serverless` runs.
  - Default: empty (the platform default, 128 MB).

### BNG legs

- `--grid-bng-only`: run only the British National Grid benchmark (both tiers, then the exact-output parity gates). Each explode leg checks that its query reaches its own tier before timing.
- `--benchmark-grid-bng`: add the BNG benchmark to a function run.

With `--functions`, `bng_*` names (for example `bng_kringexplode,bng_tessellateexplode`) run just those BNG legs.

- Names that are not `bng_*` do not filter the BNG legs. With no `bng_*` name, all legs run.
- A `bng_*` name that is neither a BNG leg nor a registered bench function (a typo, for example) stops the run.
- Each `*explode` leg times its own tier. The lightweight table function runs through `FROM t, LATERAL f(...)`. The heavyweight generator runs in the select list.
- When both tiers are registered, each `*explode` leg checks that the two forms give the same row count.

These legs read the plan of the query they are about to time:

- Every two-tier leg of the grid (BNG, quadbin, custom).
- The TIN, legacy, MVT/PMTiles aggregate and fan-out benchmarks.

A leg whose query runs the other tier's implementation stops the run with `TierResolutionError`. It records no row.

### Reader and writer legs

Each `--benchmark-*` flag adds a leg. Each `--*-only` flag runs only that leg, with no function benchmarks.

- `--benchmark-readers` / `--readers-only`: reader benchmarks (`raster_gbx` vs `gdal`).
- `--benchmark-pmtiles` / `--pmtiles-only`: the PMTiles writer benchmark (`pmtiles_gbx` vs `pmtiles`).
- `--benchmark-vector` / `--vector-only`: the vector reader benchmark (lightweight `*_gbx` vs heavyweight `*_ogr`).
- `--benchmark-netcdf` / `--netcdf-only`: the NetCDF reader benchmark (`netcdf_gbx` vs `netcdf_gdal`).
- `--benchmark-lidar` / `--lidar-only`: the LiDAR reader and writer benchmark (`lidar_gbx` only).
- `--benchmark-exif` / `--exif-only`: the EXIF reader benchmark (`exif_gbx` only).

### Grouped FILE leg

- `--grouped-file`: add the grouped FILE-amortization benchmark.
  - `rst_clip_grouped` and the pixel-op `_grouped` functions run df-to-df over a MULTIWINDOW COG corpus (many windows per source).
  - Three tile modes are timed: materialized, virtual with FILE off, and virtual with FILE on.
  - This leg exercises the per-source open-cost amortization of `grouped_tile_map`. The scalar bench misses that FILE win.
  - The leg toggles FILE on and off internally per mode. Do not combine it with `--disable-file`.
- `--grouped-file-only`: run just that leg.
- `--multiwindow-corpus <path>`: the corpus directory that holds `cog_multiwindow_manifest.json`. Default: `<corpus>/bench-corpus-cog-multiwindow`. The leg skips cleanly if the manifest is absent.

### Levels leg

See [`--levels` details](#levels-details) for the metrics.

- `--levels <S|G|A|H|SE|IO>`: add the levels leg for one set of functions.
  - `S`: session functions. `G`: generators. `A`: aggregates. `H`: DataFrame helpers.
  - `SE`: the GDAL spec engine's rows (slope, transformcrs, mapalgebra against today's implementations).
  - `IO`: the raster read and decode forms (its own ladder: 1x, 4x and 10x).
  - Every form of the set is timed at each execution level the compute offers, at each scale (1x, 4x and 16x its base row count unless the set has its own ladder). It writes one row per form and scale in the same results table.
  - The levels are L1 (Catalyst expression), L2 (Java UDF), L3 (light) and L4 (Unity Catalog function).
- `--levels-only`: run just this leg, without the heavy tests JAR.
- `--levels-scales <csv>`: the multiples of each form's base row count. Default: the set's own ladder: `1,4,10` for `IO`, `1,4,16` for the rest. An explicit value is used exactly.
  - A scale whose client-built inputs pass 64 MiB is refused at once (exit 2). The message names the scales that fit.
  - `--levels SE` fits only 1x, so give it `--levels-scales 1`.
- `--levels-forms <csv>`: time only the forms with these names (the `form` column of the results). Default: every form of the set. Use it to rerun the rest of a leg after one slow form.
- `--levels-uc <catalog.schema>`: where the L4 functions were created (`gbx:uc:sql` / `databricks.labs.gbx.uc`). Without it, the L4 forms record a skipped row.

### FILE legs (lightweight only, serialized)

- `--file-matrix`: add the FILE-access matrix. GeoTIFF and GeoPackage reads are swept across `file_mode` in `{fuse, external, managed}`.
  - Each mode runs fully isolated, with no cross-mode warm state.
  - GeoTIFF reuses `{corpus}/rows` (10k tiles, which exceeds cluster slots).
  - GeoPackage stages a copies-ladder corpus at `--gpkg-corpus` automatically (idempotent).
  - `external` and `managed` yield `na_by_design` on FUSE-only tiers.
- `--file-matrix-only`: run just that leg.
- `--gpkg-chunksize`: add the GeoPackage `chunkSize` sweep (fuse mode, chunk sizes 1k, 10k and 100k).
  - It confirms fanout-invariance: the partition count must be stable across chunk sizes.
  - It stages the same `--gpkg-corpus` corpus automatically.
- `--gpkg-chunksize-only`: run just that leg.
- `--layout-sweep`: add the FILE write layout sweep.
  - A GeoTIFF `tile_df` (read from `{corpus}/rows`) and a single GeoPackage file are written across the layouts `(order, cluster, plain)`.
  - Each layout writes to an isolated per-layout target, so no layout inherits a prior write's on-disk grouping.
  - The `cluster` layout runs `OPTIMIZE` after the write.
  - It needs `--file-filespace` for FILE-table targets. Without it, the sweep falls back to fuse paths.
- `--layout-sweep-only`: run just that leg.
- `--layout-scan`: add the layout-scan comparison. It measures sequential-scan and shuffle-input cost for each layout's FILE table. The tables must exist from a `--layout-sweep` run. It skips cleanly if `--file-filespace` is not set.
- `--layout-scan-only`: run just that leg.
- `--file-filespace <id>`: the filespace identifier for FILE EXTERNAL and MANAGED table creation (for example a `TBLPROPERTIES` filespace path under a Volume). Default: empty. The managed and external legs then yield `na_by_design`.
- `--gpkg-corpus <path>`: the GeoPackage bench corpus base directory (staged by `stage_gpkg_bench_corpus`). Default: `<corpus>/bench-corpus-gpkg`.

### Zarr legs (lightweight only)

`--benchmark-zarr` adds the Zarr benchmark, and `--zarr-only` runs only it. Either flag forces the lightweight tier. Use the env6 wheel, because the Zarr extra is env6's. See [Zarr benchmark details](#zarr-benchmark-details).

- `--benchmark-zarr`: add the Zarr benchmark.
- `--zarr-only`: run only the Zarr benchmark.
- `--zarr-store <path>`: the plain ERA5 store. Default: `<volume>/_probe/zarr-spike/read/era5/era5_v3_shard_zstd.zarr`.
- `--zarr-cog-dir <path>`: the GeoTIFF-per-hour copy, staged on first use. Default: `<volume>/_probe/zarr/bench/cog_hourly`.
- `--zarr-scratch <path>`: the directory the write legs fill and empty. Default: `<volume>/_probe/zarr/bench/scratch/<run-id>`.
- `--zarr-schema <catalog.schema>`: the schema of the write legs' manifest tables. Each table is dropped after its run. Default: `<catalog>.gbx_zarr_scratch`.
- `--zarr-cases <csv>`: the cases to run. Default: `R1,R2,R3,W1,W2`.
- `--zarr-reps <n>`: runs per leg, the first cold. Default: 3.
- `--extras <csv>`: other feature extras of the env wheel (`stac`, `vizx`, `databricks`). The extras install after the wheel. A `light_*` name is refused.

### Smoke, preflight and watchdog options

See [Hardening](#hardening) for what each stage does.

- `--no-smoke`: skip the smoke stage (logged).
- `--smoke-only`: run the smoke stage and stop. Exit 0 when clean. It proves a compute and a launch cheaply.
- `--smoke-rows <n>`: rows per form at the smoke. Default: 16.
- `--smoke-timeout-min <n>`: the time limit of the smoke run. Default: 15.
- `--smoke-allow-errors`: a form's error row at the smoke does not stop the real run.
- `--skip-preflight`: skip the local preflight (logged).
- `--timeout-min <n>`: the time limit of the real run. Default: 360.
- `--form-cap-min <n>`: the watchdog cancels the run when one form runs longer. Default: 15 (smoke: 5).
- `--form-cap-max-min <n>`: the ceiling on a form's cap after the scale multiply. Default: 60.
- `--stale-min <n>`: the watchdog cancels the run when the heartbeat stops changing for this long. Default: 5.
- `--inject-stall <seconds>`: test only. Adds a form that sleeps on the driver, to prove the watchdog.

## Examples

- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id cl1 --functions rst_slope,rst_ndvi`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id clfull --set full`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0202-arm --lightweight-only` (ARM)
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id virt1 --input-tile virtual`
- `bash scripts/commands/gbx-bench-cluster.sh --existing-cluster-id 0101-x --run-id bngx1 --grid-bng-only --functions bng_kringexplode,bng_kloopexplode --spark-warmup 1 --spark-measured 5`
- `bash scripts/commands/gbx-bench-cluster.sh --new-cluster --zarr-only --run-id zarr1` (DBR 19 job cluster, dedicated)
- `bash scripts/commands/gbx-bench-cluster.sh --serverless --env-version 6 --zarr-only --run-id zarr1-sl`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id gfile1 --grouped-file-only --multiwindow-corpus /Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow`
- `bash scripts/commands/gbx-bench-cluster.sh --existing-cluster-id 0101-x --run-id lv1 --levels S --levels-only --levels-uc main.gbx_uc`
- `bash scripts/commands/gbx-bench-cluster.sh --existing-cluster-id 0101-x --run-id lvh --levels H --levels-only --levels-scales 1,4` (`--levels-scales`: the multiples of each form's base row count; default: the set's own ladder, `1,4,10` for `IO` and `1,4,16` for the rest)
- `bash scripts/commands/gbx-bench-cluster.sh --serverless --run-id lvs --levels S --levels-only --levels-uc main.gbx_uc` (smoke, then the real run)

## Run id and notebook path

- Without `--run-id`, a launch gets an id of its own (`cluster-<date>-<time>-<4 hex>`).
- `--resume` needs the `--run-id` of the run it resumes.
- `--truncate-results` needs the `--run-id` whose rows it clears. With a fresh id it has nothing to clear.
- Each run uploads its notebook to `/Users/<you>/geobrix_bench_runner_<run-id>.ipynb`. `GBX_BENCH_RUNNER_NOTEBOOK_PATH` replaces the `/Users/<you>/geobrix_bench_runner` prefix.
- After the run ends, a launch that waited for it deletes the notebook. The run keeps its output.
- A job reads its notebook when it is submitted. Launches at the same time need their own paths. With one shared path, an earlier job ran a later launch's notebook.

## `--levels` details {#levels-details}

Each row of the levels leg carries these columns:

- `level` and `form`.
- `compute`: `dedicated`, `standard` or `serverless`, detected from the session.
- The attribution split:
  - Bytes sent to and returned from Python workers.
  - `py_eval_s`: Python-worker task-seconds.
  - `py_eval_frac`: their share of the execution's executor task-seconds, in (0, 1].
  - Shuffle and spill bytes.
  - Peak execution memory.
  - GC seconds.
  - The Python workers' peak RSS (`py_worker_rss_mb`).
- `ser_sent_bytes`, `native_s` and `note`.

How the split is read:

- The split is the plan of the last measured iteration. The timing is the median over all iterations.
- A classic session reads it from the driver's Spark REST API.
- A Connect session (standard access mode, Serverless) reads it from the query's `executionInfo`. That has no GC time.

Other columns and forms:

- `ser_sent_bytes` is NULL unless it grows with the row count across the scales. Spark stops counting the numeric input it sends past one Arrow batch.
- `native_s` is NULL until a level reports it.
- For the BNG function, the leg also times a `floor` form: the same Arrow UDF shell and input with a constant body.
  - Its median (`floor_median_s` in the summary table, beside each variant's median at the same row count) is what the shell costs without a kernel.
  - `py_eval_frac` cannot tell the two apart. A stage whose time is the UDF scores about 1 either way.
- A form whose function returns NULL for some rows is an `error` row ("N of M outputs NULL") unless the form allows it.
- Tile inputs are cached next to the results table from `<corpus>/rows`. The leg skips the raster forms with a `na_by_design` row when it has none.
- L1 and L2 need the geobrix JAR on the cluster.

### NULL rules

- A metric with no value is NULL, never 0. The row's `note` says why.
- `note` reads `unavailable: ...` for what the session could not report. The REST API leaves out a metric no node ever updated, so "no spill" reads as NULL on a classic session.
- `note` is empty for what does not apply. A JVM level or a Unity Catalog function has no Spark Python worker.

### `py_worker_rss_mb`

- It is the kernel's high-water mark of the Python workers that ran the shape. GeoBrix resets it on every worker just before the shape ran, so it is that shape's peak and not an earlier one's. An idle pooled worker is not counted.
- It needs all of these:
  - Linux.
  - A classic session whose default parallelism matches the executors' slots (read from the Spark REST API).
  - A fixed-size cluster that nothing else is using. The reset waits up to 60 s for a free slot on every worker.
- Otherwise it is NULL with the reason in `note`. On Connect it is always NULL.
- It is NULL, not a number that may leave a worker out, in these cases:
  - No Python worker ran during the shape.
  - `spark.python.worker.reuse=false`.
  - A worker that was there at the reset has exited, in any pool. This includes another pool's idle worker, because the reading cannot tell that it did not run. Spark stops a worker that has idled 60 s. A long shape in a pool with idle workers, or whose early tasks end long before its last, is NULL.
  - The REST API shows a failed, killed or retried task, or gives no answer for the shape's jobs.
  - A shape longer than 50 s has a wait of more than 50 s between task ends on an executor. A worker created by the shape may have idled out and been stopped.
  - The shape's physical plan has an early stop, or the plan cannot be read. The early-stop nodes are `CollectLimit`, `GlobalLimit`, `LocalLimit` and `TakeOrderedAndProject`. A `take`, `head`, `first` or `limit` can end a worker before the read without failing a task.
  - The shape ran Python workers in more than one environment (two pools that ran on one executor). A cold pool has no baseline, so a worker of one can exit unseen while the other keeps releasing.
  - The executors' slots differ from the default parallelism, a live worker cannot be reset, or no slot frees up within 60 s.
- The row's `note` gives the reason in each case.

### Known limits of `py_worker_rss_mb`

These limits are documented and not coded. None is NULL, and none is checked.

- A consumer other than a limit node can stop reading early without failing a task. For example, a sort-merge join can exhaust one side over sorted input. This can end a worker unseen.
- A JVM tail of more than 10 s after a task's Python output ends defeats the margin of the 50 s guard. The 60 s is OSS Spark 4.0.0's idle constant. It is not checked on DBR.
- The early-stop check reads only the DataFrame the shape ran. It does not see an early stop in another query, or a `run` callable that reads rows on the side. No bench form does one.

## Zarr benchmark details {#zarr-benchmark-details}

- The cases are the `ZARR_CASES` in `bench/spec.py`:
  - R1: one hour of the globe.
  - R2: 1,000 points x every hour.
  - R3: a 30-day box.
  - Each read case runs against one tiled GeoTIFF per hour, read with `raster_gbx`.
  - W1: a 1 GB create against plain `xarray.to_zarr`, with its gate.
  - W2: eight appenders at once against one after another.
- A shard-index probe and a sample-density probe follow (`bench/zarr_bench.py`).
- Each leg runs `--zarr-reps` times, the first cold.
- Rows land in the results table with `category = 'zarr'`. The walls are in `note`.
- The GeoTIFF copy is staged at `--zarr-cog-dir` on first use (`readers.stage_zarr_cog_corpus`). Staging is skipped when its `cog_hourly_manifest.json` exists.
- On a cluster, the install cell installs `geobrix[zarr]` from the wheel.
- On Serverless, the job environment installs the wheel and the requirements of the wheel's `zarr` extra. They are read from `python/geobrix/envs/env6/extra-zarr.txt`. A pip install from the notebook reaches only the Serverless driver, so the install cell only reports the version.
- `--extras <csv>` installs other extras the same way on both.
- The Zarr cell removes its `--zarr-scratch` directory when it ends.

## Hardening (every run) {#hardening}

- **Smoke first.** A SMOKE run goes before the real scales. Only a clean smoke submits the real run. Otherwise the command exits 4.
  - A worker-side import check: a one-row UDF imports `databricks.labs.gbx` and reports its version and arch.
  - Each form of the `--levels` set runs once at `--smoke-rows` rows (default 16). The smoke reports the Spark task count of each form's input scan. A tile form over a one-file table runs on 1-2 tasks.
  - `--no-smoke` skips it (logged). `--smoke-only` stops after it.
  - `--smoke-allow-errors` lets a form's error row through. `--smoke-timeout-min` (default 15) bounds it.
- **Preflight, no compute.** Exit 5 when the Serverless environment spec lacks the wheel (or the wheel's `+env<N>` is not the environment version), the staged wheel's sha256 is not the local `dist/env<N>` build, a levels form's SQL cannot run on a local Spark session with a few rows (a Java UDF inside `sum()`, a raw-hash sum that overflows under ANSI), or a client-built input would pass 64 MiB. `--skip-preflight` skips it (logged).
- **Time limits.** Every run has `timeout_seconds` (`--timeout-min`, default 360, with or without `--levels`). The smoke run has its own, default 15.
  - Both must be a whole number of minutes, at least 1.
  - Anything else is refused before any work starts.
- **Watchdog.** The notebook writes `<out_dir>/heartbeat.json` every 30 s. It holds the form in progress, its elapsed time and the working thread's stack. The notebook sinks each form's rows when the form finishes.
  - The launcher cancels the run when the heartbeat stops changing for `--stale-min` (default 5), or when one form passes its cap.
  - It then prints `STALLED: <form> @<scale>x after <m> min` with the driver stack, and exits 3.
  - The cap is `--form-cap-min` (default 15, smoke 5) for a 1x form. It is multiplied by the scale for the larger ones: a 16x form gets 16 times the cap. A long 1x/4x/16x ladder is judged one scale at a time, not as a whole.
  - The multiplied cap is held to `--form-cap-max-min` (default 60). At the defaults 4x gets 60 min and 16x gets 60 min, not 240. Raise the ceiling for a ladder whose large scales really need longer.
  - A heartbeat that cannot be read (a 403, a dropped connection) is not a stall. The quiet clock stands still while reads fail. After 12 failed polls in a row (about a minute) the launcher prints one `WARNING: heartbeat unreadable` line, and only the run's time limit then ends a stalled run.
  - The watchdog never retries. If the cancel call itself fails, it says so and names the run to cancel by hand (still exit 3).
  - The per-form cap applies only to the `--levels` leg, which reports its progress form by form. The other bench legs get the worker import check, the stale-heartbeat check and the run time limit, but no per-form cap.
  - The notebook also arms `faulthandler.dump_traceback_later` at the cap (stderr of the driver).
- **Snapshot.** The command copies the scripts, the launcher and the bench package to a temp dir at launch and runs from it. An edit during the run cannot change it. `GBX_NO_SNAPSHOT=1` runs in place.
- `--inject-stall <seconds>` (test only) adds a form that sleeps on the driver, to prove the watchdog.

## Interpreter

The launcher needs `databricks-sdk` and Python 3.11 or newer. It does not use whatever `python` is on the PATH. The shared `gbx_python_with_sdk` helper in `common.sh` picks the first of these that works:

1. `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`), when that venv is Python 3.11+ and can import the SDK. The CI-lock venv from `gbx:venv:sync` cannot.
2. `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter, so the venv's own packages (rasterio, the bench harness) stay importable.
3. With no usable venv, Python 3.12 plus the SDK only.
4. System `python3`, if it is 3.11+ and has the SDK.
5. Otherwise the command stops with a hint to install uv.

The SDK is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`), so a run does not float to a newer release.
