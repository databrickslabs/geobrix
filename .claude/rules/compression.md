---
paths:
  - "python/geobrix/src/databricks/labs/gbx/**"
---

# GTiff compression options

All GeoTIFF creation-option logic belongs in `databricks.labs.gbx.pyrx.core.compression`.

```python
from databricks.labs.gbx.pyrx.core.compression import creation_opts, predictor_for
```

- `creation_opts(compress, dtype, ...)` — returns the complete GTiff creation-option dict for a given codec and data type (handles `predictor`, `zlevel`, `zstd_level`, tile/strip layout).
- `predictor_for(dtype)` — maps a NumPy dtype to the correct PREDICTOR value (1 / 2 / 3).

## Patterns the QC check flags outside `compression.py` (level: FAIL after SP3)

The `centralized-primitives` QC check (level: FAIL) flags any of these outside
`compression.py` and the sanctioned test directory:

```python
def predictor_for(...):        # duplicate — correctness diverges silently
def _creation_opts(...):       # duplicate — same risk
profile["compress"] = ...
profile["zlevel"] = ...
profile["predictor"] = ...
profile["zstd_level"] = ...    # added SP3 — catches bespoke level-key dicts
"COMPRESS": ...                # added SP3 — catches uppercase bespoke COG dicts
```

All 16 production modules + `ds/_write.py` (folded SP3) route through `creation_opts`.
The bench consumers `compression_sweep.py` and `datagen.py` were folded in SP3.

## Also avoid outside `compression.py` (advisory — not checked by QC)

Dict-literal forms such as `{"compress": ..., "zlevel": ..., "predictor": ...}` outside
`creation_opts` carry the same divergence risk as the flagged patterns above but are not
individually caught by the regex (a dict spread over multiple lines can evade the pattern).
Call `creation_opts` once and spread its return value into the profile.
