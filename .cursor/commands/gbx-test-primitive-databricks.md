# Primitive Volume tests on Databricks

Pushes the **primitive runner** notebook to the workspace and runs it **on the cluster** as a one-off job. The notebook tests only fundamental Volume operations via the SDK: (a) volume exists, (b) create subdirs, (c) read from volume, (d) write to volume, (e) write to local then copy to volume. Use this to confirm the cluster can see and use Unity Catalog volumes before running the full bundle.

---

## Usage

```bash
bash .cursor/commands/gbx-test-primitive-databricks.sh [OPTIONS]
```

## Options

- `--no-wait` – Submit the job and exit without waiting for the run to finish.
- `--help` – Show help and config.

## Config

Same as the bundle: copy `notebooks/tests/databricks_cluster_config.example.env` to `databricks_cluster_config.env`, set `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (or profile), **`CLUSTER_ID`**, and `GBX_BUNDLE_VOLUME_*`. Optional: `GBX_RUNNER_DIR`, `GBX_PRIMITIVE_RUNNER_NOTEBOOK` (default: user folder and `geobrix_primitive_runner.ipynb`).

No wheel or GeoBrix install is required on the cluster for primitives; the notebook uses only `databricks-sdk`.
