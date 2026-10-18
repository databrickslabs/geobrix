# Stage Python wheel to Volume

Builds the JAR first (via stage-jar: `mvn clean package` + JAR upload) unless **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** or **GBX_BUNDLE_SKIP_JAR=1** — either flag skips the JAR build AND upload entirely (no Maven), for light-tier-only wheel changes (the locally staged JAR in `python/geobrix/lib/` is left untouched; it is cleared and refilled only when the JAR step runs, and the wheel itself is always JAR-less) — then runs **python3 -m build** and stages the wheel to **two locations**: **GBX_ARTIFACT_VOLUME**/ (init-script dir; dedupes to exactly one `geobrix-*.whl` — before upload, every other-version `geobrix-*.whl` in that dir is deleted via the SDK's `files.list_directory_contents`; nothing else is touched, and a listing/delete failure aborts the run with a non-zero exit) and the **bundle volroot** (`/Volumes/$GBX_BUNDLE_VOLUME_CATALOG/$GBX_BUNDLE_VOLUME_SCHEMA/$GBX_BUNDLE_VOLUME_NAME/`, the bundle/%pip path used by notebooks and the bench launcher). Both overwrite if present. Set **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-wheel.sh
```

## Config

1. Copy `examples/tests/databricks_cluster_config.example.env` to `examples/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** to skip wheel build/upload; **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** (or **GBX_BUNDLE_SKIP_JAR=1**) to skip the JAR build and upload when running stage-wheel. A skip-JAR flag plus **GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1** is a fully local wheel-only build (no Maven, no auth).

## Requires

- `python3 -m build`, `databricks-sdk` (e.g. `pip install build databricks-sdk` or `pip install -e python/geobrix[databricks]`).
