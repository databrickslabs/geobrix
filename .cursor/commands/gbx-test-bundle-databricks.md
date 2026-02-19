# Test bundle on Databricks (live workspace)

**Default:** Pushes the runner notebook to the workspace (`GBX_BUNDLE_RUNNER_NOTEBOOK_PATH`) and runs it **on the cluster** (`CLUSTER_ID`) as a one-off job. The bundle executes on Databricks, so Volume paths work without running anything on your machine.

---

## Usage

```bash
bash .cursor/commands/gbx-test-bundle-databricks.sh [OPTIONS]
```

## Options

- **Default** – Push notebook and run on cluster (no local execution).
- `--local` – Run the bundle on this machine instead (requires token with UC volume access).
- `--no-wait` – Submit the job and exit without waiting for the run to finish.
- `--debug` – With `--local`: enable progress prints.
- `--help` – Show help and config.

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (or profile), **`CLUSTER_ID`**, `GBX_BUNDLE_VOLUME_CATALOG`, `GBX_BUNDLE_VOLUME_SCHEMA`, `GBX_BUNDLE_VOLUME_NAME`.
3. Optional: `GBX_BUNDLE_RUNNER_NOTEBOOK_PATH`, `GBX_BUNDLE_WHEEL_VOLUME_PATH`.

The cluster must have GeoBrix installed (or set `GBX_BUNDLE_WHEEL_VOLUME_PATH` so the runner notebook installs it from a wheel on a Volume).

See `notebooks/tests/README.md` (section "Test bundle on a live Databricks cluster") for full details.
