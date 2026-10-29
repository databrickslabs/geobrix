# gbx:bench:cluster

Submit the heavy-vs-light benchmark as a one-off notebook job to a **provisioned** Databricks cluster. Both APIs run on the same cluster (the true same-hardware comparison); results append to the `bench_results` Delta table and `comparison.csv`/`summary.md` land on the configured Volume.

**Prerequisites (operator):** a provisioned cluster + filled `examples/tests/databricks_cluster_config.env`. Heavyweight needs an x86 DBR 17.3 or 18 LTS cluster with the init script + bundle + geobrix wheel + the bench `geobrix-*-tests.jar` staged; lightweight (incl. ARM) needs a light-tier extra (e.g. `[light_dbrN]` on classic or `[light_env6]` on Serverless). On ARM clusters use `--lightweight-only` (heavyweight is x86-only by design).

**Usage:** `bash scripts/commands/gbx-bench-cluster.sh [options]`

**Options:** `--cluster-id`, `--existing-cluster-id`, `--run-id`, `--functions`, `--set core|full`, `--grid-bng-only`, `--benchmark-grid-bng`, `--spark-warmup`, `--spark-measured`, `--modes`, `--row-counts`, `--warmup`, `--measured`, `--heavyweight-only`, `--lightweight-only`, `--input-tile materialized|virtual`, `--grouped-file`, `--grouped-file-only`, `--multiwindow-corpus <path>`, `--file-matrix`, `--file-matrix-only`, `--gpkg-chunksize`, `--gpkg-chunksize-only`, `--layout-sweep`, `--layout-sweep-only`, `--layout-scan`, `--layout-scan-only`, `--file-filespace <id>`, `--gpkg-corpus <path>`, `--max-partition-bytes <v>`, `--benchmark-zarr`, `--zarr-only`, `--zarr-store`, `--zarr-cog-dir`, `--zarr-scratch`, `--zarr-schema`, `--zarr-cases`, `--zarr-reps`, `--extras`, `--new-cluster`, `--spark-version`, `--node-type`, `--num-workers`, `--access-mode`, `--on-demand`, `--no-wait`, `--help`.

`--existing-cluster-id <id>` attaches the run to a warm all-purpose cluster, skipping the 4-8 min job-cluster startup. Passed through to the Python launcher; equivalent to setting `CLUSTER_ID` in the env file. (`--cluster-id` in the shell command sets the same env var for back-compat.)

`--set` chooses the tier (`core` default, or `full`). An explicit `--functions` overrides `--set`.

`--grid-bng-only` runs only the British National Grid benchmark (both tiers, then the exact-output parity gates); `--benchmark-grid-bng` adds it to a function run. `--functions` with `bng_*` names (e.g. `bng_kringexplode,bng_tessellateexplode`) runs just those BNG legs; names that are not `bng_*` do not filter it, and a `bng_*` name that is neither a BNG leg nor a registered bench function (e.g. a typo) stops the run. Each `*explode` leg times its own tier — the lightweight table function through `FROM t, LATERAL f(...)`, the heavyweight generator in the select list — and, when both tiers are registered, checks the two forms give the same row count.

Every two-tier leg of the grid (BNG, quadbin, custom), TIN, legacy, MVT/PMTiles aggregate and fan-out benchmarks reads the plan of the query it is about to time: a leg whose query runs the other tier's implementation stops the run with `TierResolutionError` instead of recording a row.

`--input-tile` selects the input tile mode for the light spark-path leg: `materialized` (bytes column, default — matches all prior bench runs) or `virtual` (path+window tile, exercises the virtual-tile reader path).

`--grouped-file` adds the grouped FILE-amortization benchmark: `rst_clip_grouped` plus pixel-op `_grouped` fns run df→df over a MULTIWINDOW COG corpus (many windows per source), timed across three tile modes — materialized, virtual+FILE-off, virtual+FILE-on. This is the leg that exercises `grouped_tile_map`'s per-source open-cost amortization (the FILE win the scalar bench misses); the leg toggles FILE on/off internally per mode, so do not combine it with `--disable-file`. `--grouped-file-only` runs just that leg. `--multiwindow-corpus <path>` points at the corpus dir holding `cog_multiwindow_manifest.json` (default `<corpus>/bench-corpus-cog-multiwindow`); the leg skips cleanly if the manifest is absent.

**Phase-2 FILE legs (light-only, serialized, each `*-only`-capable):**

`--file-matrix` adds the FILE-access matrix: GeoTIFF + GeoPackage reads swept across `file_mode` ∈ `{fuse, external, managed}`. Each mode runs fully isolated (no cross-mode warm state). GeoTIFF reuses `{corpus}/rows` (10k tiles, exceeds cluster slots). GeoPackage stages a copies-ladder corpus at `--gpkg-corpus` automatically (idempotent). `external`/`managed` yield `na_by_design` on FUSE-only tiers. `--file-matrix-only` runs just that leg.

`--gpkg-chunksize` adds the GeoPackage chunkSize sweep (fuse mode, chunk sizes 1k/10k/100k) that confirms fanout-invariance (partition count must be stable across chunk sizes). Stages the same `--gpkg-corpus` corpus automatically. `--gpkg-chunksize-only` runs just that leg.

`--layout-sweep` adds the FILE write layout sweep: GeoTIFF tile_df (read from `{corpus}/rows`) and a single GeoPackage file are written across layouts `(order, cluster, plain)` each to an **isolated per-layout target** (no layout inherits a prior write's on-disk grouping). `cluster` layout runs `OPTIMIZE` after the write. Requires `--file-filespace` for FILE-table targets; falls back to fuse paths if not set. `--layout-sweep-only` runs just that leg.

`--layout-scan` adds the layout-scan comparison: sequential-scan + shuffle-input cost measured for each layout's FILE table (the tables must already exist from a `--layout-sweep` run). Skips cleanly if `--file-filespace` is not set. `--layout-scan-only` runs just that leg.

`--file-filespace <id>` passes the filespace identifier for FILE EXTERNAL/MANAGED table creation (e.g. a TBLPROPERTIES filespace path under a Volume). Default: empty string — managed/external legs yield `na_by_design`.

`--gpkg-corpus <path>` points at the GeoPackage bench corpus base dir (staged by `stage_gpkg_bench_corpus`). Default: `<corpus>/bench-corpus-gpkg`.

**Zarr legs (light-only):** `--benchmark-zarr` adds, and `--zarr-only` runs only, the Zarr benchmark: the cases of `ZARR_CASES` in `bench/spec.py` (R1 one hour of the globe, R2 1,000 points x every hour, R3 a 30-day box, each against one tiled GeoTIFF per hour read with `raster_gbx`; W1 a 1 GB create against plain `xarray.to_zarr`, with its gate; W2 eight appenders at once against one after another), then a shard-index probe and a sample-density probe (`bench/zarr_bench.py`). Each leg runs `--zarr-reps` times (default 3), the first cold; rows land in the results table with `category = 'zarr'` and the walls in `note`. The GeoTIFF copy is staged at `--zarr-cog-dir` on first use (`readers.stage_zarr_cog_corpus`; skipped when its `cog_hourly_manifest.json` exists). `--zarr-store`, `--zarr-scratch`, `--zarr-schema` and `--zarr-cases` set the rest (defaults in `--help`). Either flag forces the lightweight tier. On a cluster the install cell adds `geobrix[light_dbr19,zarr_dbr19]` after the wheel; on Serverless the job environment installs the wheel and the requirements of `light_env6,zarr_env6` (expanded from `pyproject.toml`), because a pip install from the notebook reaches only the Serverless driver, and the install cell only reports the version. `--extras <csv>` names other extras, the same way on both. The Zarr cell removes its `--zarr-scratch` directory when it ends.

**Run id and notebook path:** without `--run-id` a launch gets an id of its own (`cluster-<date>-<time>-<4 hex>`), so `--resume` needs the `--run-id` of the run it resumes, and `--truncate-results` the `--run-id` whose rows it clears (with a fresh id it has nothing to clear). Each run uploads its notebook to `/Users/<you>/geobrix_bench_runner_<run-id>.ipynb` (`GBX_BENCH_RUNNER_NOTEBOOK_PATH` replaces the `/Users/<you>/geobrix_bench_runner` prefix), and once the run has ended, a launch that waited for it deletes the notebook (the run keeps its output). A job reads its notebook when it is submitted, so launches at the same time need their own paths: with one shared path an earlier job ran a later launch's notebook.

**Job cluster (`--new-cluster`):** runs the notebook on a cluster made for the run, which Databricks terminates when the run ends, instead of an existing one: `--spark-version` (default `19.x-scala2.13`), `--node-type` (default `m6gd.xlarge`), `--num-workers` (default 1) and `--access-mode dedicated|standard` (default `dedicated`; dedicated is single-user for the caller). `--on-demand` (AWS) puts every node on demand, so a measurement is not cut by a spot node being taken back; without it the workspace's spot policy applies. `CLUSTER_ID` from the config file is not used. The task has `max_retries=0`.

`--max-partition-bytes <v>` sets `spark.sql.files.maxPartitionBytes` (e.g. `32m`, `16m`) in the run's preamble to cap bytes-per-partition on file/Delta scans — smaller partitions mean fewer rows per task (less memory per decode task). Unlike AQE, this conf **is settable on Serverless**, so it works on both classic and `--serverless` runs. Default: empty (leave the platform default, 128 MB).

**Interpreter:** the launcher needs `databricks-sdk` and Python 3.11 or newer. It does not use whatever `python` is on the PATH: the shared `gbx_python_with_sdk` helper in `common.sh` picks `.venv-pyrx` (or the venv in `GBX_PYRX_VENV`) when that venv is Python 3.11+ and can import the SDK (the CI-lock venv from `gbx:venv:sync` cannot), otherwise `uv run --with databricks-sdk==<pinned version>` layered over the venv's interpreter, so the venv's own packages (rasterio, the bench harness) stay importable; with no usable venv it uses Python 3.12 plus the SDK only; otherwise system `python3` if it is 3.11+ and has the SDK, otherwise it stops with a hint to install uv. The SDK is pinned (`GBX_DATABRICKS_SDK_SPEC` in `common.sh`) so a run does not float to a newer release.

**Examples:**
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id cl1 --functions rst_slope,rst_ndvi`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id clfull --set full`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0202-arm --lightweight-only` (ARM)
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id virt1 --input-tile virtual`
- `bash scripts/commands/gbx-bench-cluster.sh --existing-cluster-id 0101-x --run-id bngx1 --grid-bng-only --functions bng_kringexplode,bng_kloopexplode --spark-warmup 1 --spark-measured 5`
- `bash scripts/commands/gbx-bench-cluster.sh --new-cluster --zarr-only --run-id zarr1` (DBR 19 job cluster, dedicated)
- `bash scripts/commands/gbx-bench-cluster.sh --serverless --env-version 6 --zarr-only --run-id zarr1-sl`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id gfile1 --grouped-file-only --multiwindow-corpus /Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow`
