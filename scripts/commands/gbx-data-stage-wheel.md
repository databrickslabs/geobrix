# Stage Python wheel to Volume

Builds the JAR first (unless **GBX_BUNDLE_SKIP_JAR_UPLOAD=1**), then runs **python3 -m build** and stages the wheel to **two locations**: **GBX_ARTIFACT_VOLUME**/ (init-script dir; dedupes to exactly one `geobrix-*.whl` — any stale other-version wheel in that dir is removed before upload) and the **bundle volroot** (`/Volumes/$GBX_BUNDLE_VOLUME_CATALOG/$GBX_BUNDLE_VOLUME_SCHEMA/$GBX_BUNDLE_VOLUME_NAME/`, the bundle/%pip path used by notebooks and the bench launcher). Both overwrite if present. Set **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-wheel.sh
```

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload; **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to skip JAR stage when running stage-wheel.

## Requires

- `python3 -m build`, `databricks-sdk` (e.g. `pip install build databricks-sdk` or `pip install -e python/geobrix[databricks]`).
