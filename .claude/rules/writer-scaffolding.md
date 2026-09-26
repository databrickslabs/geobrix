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

```python
from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir
```

- `new_scratch_dir(base)` — creates a tracked temp directory under `base`; logs the path.
- `remove_scratch_dir(path)` — removes the directory and deregisters it from tracking.

## Patterns to avoid in writers

The `centralized-primitives` QC check flags this outside the canonical file:

```python
tempfile.mkdtemp(...)   # untracked — leaked on failure; the `scratch` check flags it as an advisory (warn) until the writer-scaffolding consolidation pass clears the debt and flips scratch to FAIL
```

Also avoid bespoke merge loops, glob patterns, and output-name sanitizers — they diverge from `_publish_merged` / `_glob_merge_inputs` / `_safe_name` on edge cases (Unicode names, sparse part counts). Reuse the shared helpers.
