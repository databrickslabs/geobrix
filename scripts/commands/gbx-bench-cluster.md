# gbx:bench:cluster

Submit the heavy-vs-light benchmark as a one-off notebook job to a **provisioned** Databricks cluster. Both APIs run on the same cluster (the true same-hardware comparison); results append to the `bench_results` Delta table and `comparison.csv`/`summary.md` land on the configured Volume.

**Prerequisites (operator):** a provisioned cluster + filled `notebooks/tests/databricks_cluster_config.env`. Heavyweight needs an x86 DBR 17.3 or 18 LTS cluster with the init script + bundle + geobrix wheel + the bench `geobrix-*-tests.jar` staged; lightweight (incl. ARM) needs just the `[light]` wheel. On ARM clusters use `--lightweight-only` (heavyweight is x86-only by design).

**Usage:** `bash scripts/commands/gbx-bench-cluster.sh [options]`

**Options:** `--cluster-id`, `--existing-cluster-id`, `--run-id`, `--functions`, `--set core|full`, `--modes`, `--row-counts`, `--warmup`, `--measured`, `--heavyweight-only`, `--lightweight-only`, `--input-tile materialized|virtual`, `--grouped-file`, `--grouped-file-only`, `--multiwindow-corpus <path>`, `--file-matrix`, `--file-matrix-only`, `--gpkg-chunksize`, `--gpkg-chunksize-only`, `--layout-sweep`, `--layout-sweep-only`, `--layout-scan`, `--layout-scan-only`, `--file-filespace <id>`, `--gpkg-corpus <path>`, `--max-partition-bytes <v>`, `--no-wait`, `--help`.

`--existing-cluster-id <id>` attaches the run to a warm all-purpose cluster, skipping the 4-8 min job-cluster startup. Passed through to the Python launcher; equivalent to setting `CLUSTER_ID` in the env file. (`--cluster-id` in the shell command sets the same env var for back-compat.)

`--set` chooses the tier (`core` default, or `full`). An explicit `--functions` overrides `--set`.

`--input-tile` selects the input tile mode for the light spark-path leg: `materialized` (bytes column, default — matches all prior bench runs) or `virtual` (path+window tile, exercises the virtual-tile reader path).

`--grouped-file` adds the grouped FILE-amortization benchmark: `rst_clip_grouped` plus pixel-op `_grouped` fns run df→df over a MULTIWINDOW COG corpus (many windows per source), timed across three tile modes — materialized, virtual+FILE-off, virtual+FILE-on. This is the leg that exercises `grouped_tile_map`'s per-source open-cost amortization (the FILE win the scalar bench misses); the leg toggles FILE on/off internally per mode, so do not combine it with `--disable-file`. `--grouped-file-only` runs just that leg. `--multiwindow-corpus <path>` points at the corpus dir holding `cog_multiwindow_manifest.json` (default `<corpus>/bench-corpus-cog-multiwindow`); the leg skips cleanly if the manifest is absent.

**Phase-2 FILE legs (light-only, serialized, each `*-only`-capable):**

`--file-matrix` adds the FILE-access matrix: GeoTIFF + GeoPackage reads swept across `file_mode` ∈ `{fuse, external, managed}`. Each mode runs fully isolated (no cross-mode warm state). GeoTIFF reuses `{corpus}/rows` (10k tiles, exceeds cluster slots). GeoPackage stages a copies-ladder corpus at `--gpkg-corpus` automatically (idempotent). `external`/`managed` yield `na_by_design` on FUSE-only tiers. `--file-matrix-only` runs just that leg.

`--gpkg-chunksize` adds the GeoPackage chunkSize sweep (fuse mode, chunk sizes 1k/10k/100k) that confirms fanout-invariance (partition count must be stable across chunk sizes). Stages the same `--gpkg-corpus` corpus automatically. `--gpkg-chunksize-only` runs just that leg.

`--layout-sweep` adds the FILE write layout sweep: GeoTIFF tile_df (read from `{corpus}/rows`) and a single GeoPackage file are written across layouts `(order, cluster, plain)` each to an **isolated per-layout target** (no layout inherits a prior write's on-disk grouping). `cluster` layout runs `OPTIMIZE` after the write. Requires `--file-filespace` for FILE-table targets; falls back to fuse paths if not set. `--layout-sweep-only` runs just that leg.

`--layout-scan` adds the layout-scan comparison: sequential-scan + shuffle-input cost measured for each layout's FILE table (the tables must already exist from a `--layout-sweep` run). Skips cleanly if `--file-filespace` is not set. `--layout-scan-only` runs just that leg.

`--file-filespace <id>` passes the filespace identifier for FILE EXTERNAL/MANAGED table creation (e.g. a TBLPROPERTIES filespace path under a Volume). Default: empty string — managed/external legs yield `na_by_design`.

`--gpkg-corpus <path>` points at the GeoPackage bench corpus base dir (staged by `stage_gpkg_bench_corpus`). Default: `<corpus>/bench-corpus-gpkg`.

`--max-partition-bytes <v>` sets `spark.sql.files.maxPartitionBytes` (e.g. `32m`, `16m`) in the run's preamble to cap bytes-per-partition on file/Delta scans — smaller partitions mean fewer rows per task (less memory per decode task). Unlike AQE, this conf **is settable on Serverless**, so it works on both classic and `--serverless` runs. Default: empty (leave the platform default, 128 MB).

**Examples:**
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id cl1 --functions rst_slope,rst_ndvi`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id clfull --set full`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0202-arm --lightweight-only` (ARM)
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id virt1 --input-tile virtual`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id gfile1 --grouped-file-only --multiwindow-corpus /Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-cog-multiwindow`
