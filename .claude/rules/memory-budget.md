---
paths:
  - "python/geobrix/src/databricks/labs/gbx/ds/**"
  - "python/geobrix/src/databricks/labs/gbx/pyrx/**"
---

# Memory budgets

Canonical map: driver-side RAM decisions, worker-side binary caps, and tile-split decoded
sizes each have a dedicated helper — they are intentionally kept separate because a
Serverless worker cannot RAM-probe its ~1 GB task quota via `/proc/meminfo` (reports node
total).

```python
from databricks.labs.gbx.pyrx.core.budget import budget_for, decoded_budget_bytes
from databricks.labs.gbx.ds.file_gbx import materialize_decision
```

- `budget_for(intent, est_bytes=None, *, override_mb=None, infra=None)` — **driver-side,
  RAM-measured** (`/proc/meminfo` via `_probe_infra`). All intents are `_assert_driver`-guarded
  (calling from a Spark worker raises `RuntimeError` because `/proc/meminfo` on a worker
  reports node-total RAM, not the ~1 GB task quota). `session`/`kind`/`override_bytes` are
  intentionally absent — they would only serve the deferred worker/cog facade; add them with it,
  not speculatively.
  - `"driver_merge"` — lidar merge RAM budget; reserve 6 GiB (GPU) / 2 GiB (CPU);
    floor 512 MiB; `override_mb` / `GBX_LIDAR_MERGE_MAX_MB` / `GBX_MERGE_MAX_MB` wins.
  - `"dense_alloc"` — GPU dense MVS RAM budget; reserve 6 GiB (always GPU context);
    returns available-minus-reserve bytes; `override_mb` / `GBX_DENSE_ALLOC_MAX_MB` wins.
    Concurrency/GPU-slot math stays in `mvs.recommend_dense_allocation` on top.
- `materialize_decision(size_bytes, kind, spark=None, cap_bytes=None)` — **worker-side,
  binary cap**: Serverless/Connect 64 MiB, classic 256 MiB. Handles `worker_read` and
  `cog_write` — intentionally NOT `budget_for` intents (a worker cannot measure its
  ~1 GB task quota; the empirically bisected binary cap is the correct mechanism).
  `cog_write` additionally enforces a driver-side COG conversion ceiling
  (`_COG_DRIVER_MAX_BYTES` = 10 GiB default, `GBX_COG_DRIVER_MAX_BYTES` override).
  Does NOT read `/proc/meminfo`. Callers pre-capture the cap on the driver and pass it
  as `cap_bytes`.
- `decoded_budget_bytes(strategy)` — **tile-split decoded-byte budget** (Serverless 64 MiB /
  classic 1536 MiB / none 0). Handles `tile_split` — intentionally NOT a `budget_for`
  intent (no RAM probe; pure strategy-to-bytes table lookup). Used by `ds/raster.py` to
  size tiles before decode.

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

## Also avoid outside the canonical files (advisory — not checked by QC)

Bespoke byte-cap literals (e.g. `256 * 1024 * 1024`, `64 * 1024**2`) outside `budget.py`,
`file_gbx.py`, `mvs.py`, and `cog_writer.py` carry the same divergence risk as the flagged
patterns above, but are not individually caught by the QC regex — `* 1024` arithmetic is
pervasive in legitimate non-cap contexts (buffer sizes, PMTiles limits, warn thresholds),
so a gate would require a large, fragile allow-list. Use `materialize_decision` or
`decoded_budget_bytes` instead and let the canonical helpers own the cap constants.
