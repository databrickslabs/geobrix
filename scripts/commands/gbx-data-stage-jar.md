# Stage JAR to Volume

Runs **mvn clean package -DskipTests** and stages **both** jars it produces (one build): the product **target/*-jar-with-dependencies.jar** → **GBX_ARTIFACT_VOLUME**/ (the init-script dir; dedupes to exactly one `geobrix-*-jar-with-dependencies.jar` — any stale other-version product JAR in that dir is removed before upload), and the bench **target/*-tests.jar** → the **bundle volroot** (`/Volumes/$GBX_BUNDLE_VOLUME_CATALOG/$GBX_BUNDLE_VOLUME_SCHEMA/$GBX_BUNDLE_VOLUME_NAME/`, where the bench launcher attaches it as a cluster library). Both overwrite if present. Set **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to skip the build/upload entirely, or **GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1** to stage only the product jar. Override the tests.jar destination with **GBX_BENCH_TESTS_JAR_VOLUME_PATH**.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-jar.sh
```

## Config

1. Copy `notebooks/tests/databricks_cluster_config.example.env` to `notebooks/tests/databricks_cluster_config.env`.
2. Set **GBX_ARTIFACT_VOLUME** (e.g. `/Volumes/catalog/schema/volume/artifacts`).
3. Set **DATABRICKS_HOST**, **DATABRICKS_TOKEN** (or **DATABRICKS_CONFIG_PROFILE**).
4. Optional: **GBX_BUNDLE_SKIP_JAR_UPLOAD=1** to skip build/upload.

## Requires

- Maven (for `mvn clean package -DskipTests`).
- `databricks-sdk` (e.g. `pip install databricks-sdk` or `pip install -e python/geobrix[databricks]`).
