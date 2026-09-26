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

## Source-file listing & path normalization

Use `ds/_listing` for **every** consumer that enumerates source files or normalizes a path —
readers, writers, functions, bench, and `preparer.py`:

```python
from databricks.labs.gbx.ds._listing import list_files, to_local_path, to_spark_uri
```

- `list_files(root, pattern)` — walks a Volume root with the `_retry_transient` guard (Serverless
  eventual-consistency: `FileNotFound` on large reads retried ~10×). Returns absolute local paths.
- `to_local_path(uri)` — strips `dbfs:` / `file:` schemes and returns a bare `/Volumes/...` path
  suitable for `pathlib` / `os`.
- `to_spark_uri(path)` — converts a bare Volume path to the `dbfs:` URI Spark readers expect.

### Patterns to avoid

The `centralized-primitives` QC check (`file-listing` row) flags these outside `_listing.py`:

```python
os.walk(path)          # misses the Serverless retry guard
glob.glob(pattern)     # same
```

Also avoid manual scheme-stripping (`path.replace("dbfs:", "")`, `path.lstrip("file:")`) — use
`to_local_path(uri)` instead. The QC check does not flag these directly, but they are equally
fragile and miss the same edge cases.

Bare `os.walk` / `glob.glob` over a Volume path silently drops files when Serverless FUSE hasn't
fully propagated a large write. Always route source-file enumeration through `list_files`.
