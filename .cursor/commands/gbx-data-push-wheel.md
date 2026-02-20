# Push Python wheel to Volume

Builds the JAR first (unless **GBX_BUNDLE_SKIP_JAR_UPLOAD=1**), then runs **python3 -m build** and uploads the wheel to **GBX_ARTIFACT_VOLUME**/ (no subpath). Overwrites if file already exists. Set **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload.

---

## Usage

```bash
bash .cursor/commands/gbx-data-push-wheel.sh
```

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload; **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to skip JAR push when running push-wheel.

## Requires

- `python3 -m build`, `databricks-sdk` (e.g. `pip install build databricks-sdk` or `pip install -e python/geobrix[databricks]`).
