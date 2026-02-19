# Push JAR to Volume

Runs **mvn clean package -DskipTests** and uploads **target/*-jar-with-dependencies.jar** to **GBX_ARTIFACT_VOLUME**/ (no subpath). Overwrites if file already exists. Set **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to use existing JAR (no build/upload).

---

## Usage

```bash
bash .cursor/commands/gbx-data-push-jar.sh
```

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to skip build/upload.

## Requires

- Maven (for `mvn clean package -DskipTests`).
- `databricks-sdk` (e.g. `pip install databricks-sdk` or `pip install -e python/geobrix[databricks]`).
