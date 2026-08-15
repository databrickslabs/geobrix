# gbx:bench:cluster

Submit the heavy-vs-light benchmark as a one-off notebook job to a **provisioned** Databricks cluster. Both APIs run on the same cluster (the true same-hardware comparison); results append to the `bench_results` Delta table and `comparison.csv`/`summary.md` land on the configured Volume.

**Prerequisites (operator):** a provisioned cluster + filled `notebooks/tests/databricks_cluster_config.env`. Heavyweight needs an x86 DBR 17.3 or 18 LTS cluster with the init script + bundle + geobrix wheel + the bench `geobrix-*-tests.jar` staged; lightweight (incl. ARM) needs just the `[light]` wheel. On ARM clusters use `--lightweight-only` (heavyweight is x86-only by design).

**Usage:** `bash scripts/commands/gbx-bench-cluster.sh [options]`

**Options:** `--cluster-id`, `--existing-cluster-id`, `--run-id`, `--functions`, `--set core|full`, `--modes`, `--row-counts`, `--warmup`, `--measured`, `--heavyweight-only`, `--lightweight-only`, `--input-tile materialized|virtual`, `--no-wait`, `--help`.

`--existing-cluster-id <id>` attaches the run to a warm all-purpose cluster, skipping the 4-8 min job-cluster startup. Passed through to the Python launcher; equivalent to setting `CLUSTER_ID` in the env file. (`--cluster-id` in the shell command sets the same env var for back-compat.)

`--set` chooses the tier (`core` default, or `full`). An explicit `--functions` overrides `--set`.

`--input-tile` selects the input tile mode for the light spark-path leg: `materialized` (bytes column, default — matches all prior bench runs) or `virtual` (path+window tile, exercises the virtual-tile reader path).

**Examples:**
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id cl1 --functions rst_slope,rst_ndvi`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id clfull --set full`
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0202-arm --lightweight-only` (ARM)
- `bash scripts/commands/gbx-bench-cluster.sh --cluster-id 0101-x --run-id virt1 --input-tile virtual`
