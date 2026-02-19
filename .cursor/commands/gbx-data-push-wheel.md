# Push Python wheel to Volume

Builds the GeoBrix Python wheel and uploads it to the path in **GBX_BUNDLE_WHEEL_VOLUME_PATH** (overwrites if present). Used so a cluster can install the wheel via the runner notebook before running the bundle.

---

## Usage

```bash
bash .cursor/commands/gbx-data-push-wheel.sh
```

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set **GBX_BUNDLE_WHEEL_VOLUME_PATH** (e.g. `/Volumes/main/default/geobrix_samples/wheels/geobrix.whl`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).

The parent directory of the volume path is created if needed. Upload uses `overwrite=True`.

## Requires

- `pip` and `databricks-sdk` (e.g. `pip install -e python/geobrix[databricks]`).
