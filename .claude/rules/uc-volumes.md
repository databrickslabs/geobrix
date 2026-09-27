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

Use `ds/_listing` for consumers that **enumerate source files** on UC Volumes —
readers, bench corpus listings, and `preparer.py`:

```python
from databricks.labs.gbx.ds._listing import list_files, to_local_path, to_spark_uri
```

- `list_files(root, pattern, recursive=True, raise_on_empty=True)` — enumerates a Volume root
  with the `_retry_transient` guard built in (Serverless eventual-consistency: transient
  `FileNotFound`/`OSError` retried ~10×). Returns sorted absolute local paths.
  - `recursive=False` for flat (top-level only) listings.
  - `raise_on_empty=False` for existence checks (returns `[]` instead of raising).
- `to_local_path(uri)` — strips `dbfs:` / `file:` schemes and returns a bare `/Volumes/...` path
  suitable for `pathlib` / `os`.
- `to_spark_uri(path)` — converts a bare Volume path to the `dbfs:` URI Spark readers expect.

**Scope:** `list_files` targets **Volume source enumeration** (reading input files).
Do NOT route through it: writer stale-cleanup (deletion), scratch/temp-dir operations,
target-exists / copy-tree / path-type checks, or `tmp_path`-based tests.  Those sites use
`glob.glob` / `os.listdir` legitimately and carry no retry obligation.

### Patterns to avoid in Volume source enumeration

The `centralized-primitives` QC check (`file-listing` row, **warn** level — advisory) flags
these outside `_listing.py`:

```python
os.walk(path)          # misses the Serverless retry guard when listing source files
glob.glob(pattern)     # same
```

The check is **advisory (warn), not blocking (fail)** — `os.walk` and `glob.glob` are
general Python idioms used pervasively for legitimate non-Volume operations (deletion,
scratch dirs, tests), so a hard gate would require a large, fragile allow-list.  The check
flags candidates for human review; it does not block CI.

Also avoid manual scheme-stripping (`path.replace("dbfs:", "")`, `path.lstrip("file:")`) — use
`to_local_path(uri)` instead.

Known advisory blind spots (documented, not chased): aliased `import glob as _glob` (regex
misses it), `iglob`, `rglob`, and `os.listdir`.
