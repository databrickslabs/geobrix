# gbx:bench:gen-data

Generate the seeded benchmark corpus (GeoTIFF tiles + `corpus.json` manifest) in the isolated pyrx venv, then run the validity gate.

**Usage:** `bash scripts/commands/gbx-bench-gen-data.sh [options]`

**Options:** `--out <dir>`, `--tile-px <list>`, `--bands <list>`, `--dtypes <list>`, `--srids <list>`, `--nodata-frac <list>`, `--row-rows <n>`, `--row-tile-px <n>` (row-pool tile size — the tile the spark-path sweep benchmarks; default 1024), `--row-bands <n>`, `--row-dtype <t>`, `--seed <n>`, `--set core|full` (accepted for pipeline symmetry; the corpus is function-agnostic, so it does not change corpus content), `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-gen-data.sh --log gen.log`
- `bash scripts/commands/gbx-bench-gen-data.sh --tile-px 256,512 --bands 1,4 --nodata-frac 0.02,0.5 --row-rows 100`
- `bash scripts/commands/gbx-bench-gen-data.sh --tile-px 512 --bands 4 --dtypes float32 --srids 4326 --nodata-frac 0.0 --row-rows 10 --row-tile-px 512`

## On Databricks

`--on-databricks` generates the row pool (`rows/r<N>.tif`) on Serverless, into the corpus Volume, instead of uploading it from this machine. The tiles are byte-identical to the local ones for the same index and seed.

**Usage:** `bash scripts/commands/gbx-bench-gen-data.sh --on-databricks --profile <name> [--rows-from <n>] [--rows-to <n>] [options]`

- `--profile <name>` — Databricks profile. Required. The command never logs in.
- `--serverless` — accepted for symmetry with `gbx:bench:cluster`. Serverless is the only compute.
- `--rows-from <n>` — first tile index. Default 0.
- `--rows-to <n>` — last tile index, included. Default `--row-rows` minus 1.
- `--skip-existing` — skip a tile whose file has the expected size. This is the default. `--no-skip-existing` rewrites it.
- `--partitions <n>` — Spark partitions per chunk. Default 64.
- `--chunk <n>` — tiles per chunk. The notebook prints a `[GENCORPUS] progress` line after each chunk. Default 512.
- `--verify-indices <csv>` — tiles whose sha256 is compared with the local files. Default: the range ends, its middle, and 0/999/1000/5000/9999 where in range.
- `--corpus-out <path>` — corpus directory on the Volume. Default `<volume>/bench-corpus`.
- `--env-version <n>`, `--run-id <id>`, `--timeout-min <n>`, `--status-file <path>`, `--no-wait`, `--no-smoke`, `--smoke-only` — as in `gbx:bench:cluster`.
- `--seed`, `--srids`, `--row-tile-px`, `--row-bands`, `--row-dtype`, `--out` (the local corpus the sha256 is compared with) — as for the local run.

**Examples:**
- `bash scripts/commands/gbx-bench-gen-data.sh --on-databricks --profile oauth-fe --rows-from 1000 --rows-to 1099`
- `bash scripts/commands/gbx-bench-gen-data.sh --on-databricks --profile oauth-fe --rows-from 1000 --rows-to 9999 --status-file /tmp/gen-status.md`

**How it runs:**
- A smoke run goes first: the workers import geobrix and make the host's bytes for one tile per SRID, with no Volume I/O. A mismatch stops the launch (exit 4).
- The real run fans out a DataFrame of tile indices with `repartition(N, column)`. Each task writes its tiles through the Volume mount from a local temp file.
- Nothing on the Volume is deleted. A short file left by an interrupted run is overwritten.
- The launcher shows the latest notebook progress line every minute. The run's notebook output is saved to `test-logs/bench/<run-id>/notebook-output.html`.
- At the end it compares the count of complete tiles and the sha256 sample with the local corpus (exit 1 on a difference).
