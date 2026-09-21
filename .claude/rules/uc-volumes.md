---
paths:
  - "python/geobrix/src/databricks/labs/gbx/**"
  - "notebooks/**"
---

# Unity Catalog Volumes

On a Databricks cluster, `/Volumes/<catalog>/<schema>/<volume>/...` is **FUSE-mounted** — use `pathlib`/`os`, not the Databricks Files SDK.

- The Volume root **must pre-exist**; only paths under it can be created.
- `os.makedirs(volume_root, exist_ok=True)` is a no-op (idempotent).
- Avoid `seek` on volume files; use sequential I/O.
- For writes, prefer `shutil.copy` from a temp file.
- Sanitize env-derived strings (strip BOM/invisible Unicode) before building volume paths.

Env vars: `GBX_BUNDLE_VOLUME_CATALOG`, `GBX_BUNDLE_VOLUME_SCHEMA`, `GBX_BUNDLE_VOLUME_NAME`. Volume name must match Data Explorer exactly (hyphen vs underscore matters).

## Heavy-tier /Volumes reads

Heavy `/Volumes` reads need **BARE** paths (UC connector) — never `file:/Volumes`. On Serverless,
worker `/Volumes` LARGE reads can intermittently `FileNotFound` (eventual consistency) — retry ~10x.

## Serverless parallelism

Light-tier Serverless parallelism comes ONLY via `repartition(N, column)` (fan-out by column).
Serverless forbids `sparkContext` / `.rdd` / `_jvm` / `_jsc`; guard any `spark.conf` mutation.
