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

## Local-disk temp (canonical: pyrx/core/local_temp.py)

Product `src/` code creates local-disk temp ONLY via the canonical helpers:

    from databricks.labs.gbx.pyrx.core.local_temp import (
        local_temp_root, new_local_temp_dir, new_local_temp_file,
    )

- `new_local_temp_dir(prefix)` / `new_local_temp_file(suffix, prefix)` — atomic create
  under `local_temp_root()` (env-tunable via `GBX_LOCAL_TEMP_DIR`; defaults to the system
  temp dir). Keep the existing `try/finally` cleanup.
- Manual temp paths use `local_temp_root()` as the base, not `tempfile.gettempdir()`.
- A self-cleaning `tempfile.TemporaryDirectory(dir=local_temp_root())` is allowed.

The `centralized-primitives` `scratch` row is **FAIL** (blocking) for the create forms
`tempfile.mkdtemp|mkstemp|NamedTemporaryFile` outside the allow-list (the canonical module;
`bench/`, tests, and notebooks are exempt). This is distinct from the Volume-parent scratch
(`new_scratch_dir`) which stays in `ds/_scratch.py`.

## Patterns to avoid in writers

Also avoid bespoke merge loops, glob patterns, and output-name sanitizers — they diverge from
`_publish_merged` / `_glob_merge_inputs` / `_safe_name` on edge cases (Unicode names, sparse
part counts). Reuse the shared helpers.
