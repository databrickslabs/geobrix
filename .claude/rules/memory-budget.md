---
paths:
  - "python/geobrix/src/databricks/labs/gbx/ds/**"
  - "python/geobrix/src/databricks/labs/gbx/pyrx/**"
---

# Memory budgets

All driver-side and worker-side RAM decisions belong to the canonical budget helpers.

```python
from databricks.labs.gbx.pyrx.core.budget import budget_for, materialize_decision
```

- `budget_for(intent)` — driver-side, RAM-measured. Returns available bytes for a named intent (e.g. `"driver_merge"`, `"driver_thumb"`). Reads real `/proc/meminfo` once and caches; all callers share the measurement.
- `materialize_decision(path, budget_bytes)` — worker-side. Decides whether a file should be materialized into RAM (vs. streamed) given the per-task budget.

Canonical explanation of budgets and Serverless limits: `docs/docs/serverless-and-memory.mdx`.

## Patterns to avoid outside `budget.py` / `file_gbx.py` / `mvs.py`

The `centralized-primitives` QC check flags any of these outside the allowed files:

```python
open("/proc/meminfo").read()        # raw proc read — not shared, not cached
os.environ["SPARK_WORKER_MEMORY"]   # not populated on Serverless
os.environ["SPARK_EXECUTOR_MEMORY"] # same
STREAM_CAP = 512 * 1024 * 1024      # hardcoded cap — skips intent-based sizing
```

Hardcoded caps and ad-hoc memory probes produce behavior that diverges from `budget_for` on Serverless (where `/proc/meminfo` reflects the container, not the task allocation). Route every cap through `budget_for`; route every materialize decision through `materialize_decision`.
