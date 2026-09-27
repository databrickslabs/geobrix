---
paths:
  - "python/geobrix/src/databricks/labs/gbx/ds/**"
---

# DataSource writer scaffolding

All DataSource writers reuse the shared scaffolding in `ds/writer.py` and `ds/_scratch.py`.

## Merge / output helpers (`ds/writer.py`)

```python
from databricks.labs.gbx.ds.writer import _publish_merged, _glob_merge_inputs, _safe_name, assert_write_schema
```

- `_publish_merged(parts, dest, ...)` — gathers part files and writes a single merged output.
- `_glob_merge_inputs(scratch_dir, pattern)` — collects part paths from a scratch directory.
- `_safe_name(raw)` — sanitizes a user-supplied output name (strips path separators, normalizes extension).
- `assert_write_schema(df, expected)` — raises early with a clear message when the caller passes a DataFrame whose schema doesn't match the writer's expectation.

## Scratch directories (`ds/_scratch.py`)

The scratch primitives target **Volume-based fragment staging** for multi-phase writers: each
write phases fragments into a uniquely namespaced `<parent>/.gbx_scratch/<uuid>` subdir, then
the driver merges and removes it.

```python
from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir
```

- `new_scratch_dir(parent)` — **pure path construction**: returns `<parent>/.gbx_scratch/<uuid4.hex>`.
  It does NOT create the directory, does NOT register or track it, and does NOT log. The caller
  calls `os.makedirs(scratch, exist_ok=True)` when it first writes a fragment, and
  `remove_scratch_dir(scratch)` when done (or in `finally`).
- `remove_scratch_dir(path)` — best-effort `shutil.rmtree(path)` followed by `os.rmdir` on the
  now-empty `.gbx_scratch` container (only succeeds when no sibling writes are in-flight). Never
  raises. No tracking registry involved.

**Local-disk temp is a separate, legitimate pattern.** The product sites that call
`tempfile.mkdtemp` create local driver-side temp dirs for FUSE-safe copies, executor staging,
rasterio seek buffers, and CLI/HTTP download caches. They cannot use `new_scratch_dir` (which
is Volume-parent-based). These sites are correct as long as each one cleans up in
`try/finally: shutil.rmtree(...)`. Do NOT replace them with `new_scratch_dir` and do NOT add
new `tempfile.mkdtemp` sites without a `try/finally` guard.

## Patterns to avoid in writers

The `centralized-primitives` QC check (`scratch` row, **warn — advisory, not a FAIL gate**) flags
this outside `_scratch.py`:

```python
tempfile.mkdtemp(...)   # outside _scratch.py → advisory (warn); legitimate for local-disk temp with try/finally
```

The check is non-blocking: `scratch` stays `level=warn` because the remaining `tempfile.mkdtemp`
calls are a legitimate stdlib pattern for local-disk temp, not hand-rolled Volume-scratch.

Also avoid bespoke merge loops, glob patterns, and output-name sanitizers — they diverge from
`_publish_merged` / `_glob_merge_inputs` / `_safe_name` on edge cases (Unicode names, sparse
part counts). Reuse the shared helpers.
