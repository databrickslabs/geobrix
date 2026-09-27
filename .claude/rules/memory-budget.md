---
paths:
  - "python/geobrix/src/databricks/labs/gbx/ds/**"
  - "python/geobrix/src/databricks/labs/gbx/pyrx/**"
---

# Memory budgets

All driver-side RAM decisions belong to the canonical `budget_for` authority.
Worker-side materialize decisions use `materialize_decision` (binary cap; separate).

```python
from databricks.labs.gbx.pyrx.core.budget import budget_for
from databricks.labs.gbx.ds.file_gbx import materialize_decision
```

- `budget_for(intent, est_bytes=None, *, override_mb=None, infra=None)` — **driver-side,
  RAM-measured** (`/proc/meminfo` via `_probe_infra`). All intents are `_assert_driver`-guarded
  (calling from a Spark worker raises `RuntimeError` because `/proc/meminfo` on a worker
  reports node-total RAM, not the ~1 GB task quota).
  - `"driver_merge"` — lidar merge RAM budget; reserve 6 GiB (GPU) / 2 GiB (CPU);
    floor 512 MiB; `override_mb` / `GBX_MERGE_MAX_MB` wins.
  - `"dense_alloc"` — GPU dense MVS RAM budget; reserve 6 GiB (always GPU context);
    returns available-minus-reserve bytes; `override_mb` / `GBX_DENSE_ALLOC_MAX_MB` wins.
    Concurrency/GPU-slot math stays in `mvs.recommend_dense_allocation` on top.
  - All other intents (`worker_read`, `cog_write`, `tile_split`) are deferred (not implemented).
- `materialize_decision(size_bytes, kind, spark=None, cap_bytes=None)` — **worker-side,
  binary Connect (64 MiB) / classic (256 MiB) cap**. Does NOT read `/proc/meminfo`.
  This is NOT a `budget_for` intent. A Serverless worker cannot measure its ~1 GB task
  quota via `/proc/meminfo` (reports node total) — the empirically bisected binary cap
  is the correct mechanism and must stay separate. Callers pre-capture the cap on the
  driver and pass it as `cap_bytes`.

## Patterns the QC check flags (level: FAIL) outside `budget.py` / `file_gbx.py` / `mvs.py`

The `centralized-primitives` QC check (level: **FAIL** — blocking) flags these in any
tracked file outside the three allow-listed paths:

```python
open("/proc/meminfo").read()        # raw proc read — not shared, not cached
os.environ["SPARK_WORKER_MEMORY"]   # not populated on Serverless
os.environ["SPARK_EXECUTOR_MEMORY"] # same
```

Route every RAM probe through `budget_for`; route every materialize decision through
`materialize_decision`. Ad-hoc probes and hardcoded byte caps diverge from the canonical
helpers on Serverless.
