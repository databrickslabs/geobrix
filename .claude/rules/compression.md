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

## Patterns to avoid outside `compression.py`

The `centralized-primitives` QC check flags any of these outside the canonical file:

```python
def predictor_for(...):        # duplicate — correctness diverges silently
def _creation_opts(...):       # duplicate — same risk
profile["compress"] = ...
profile["zlevel"] = ...
profile["predictor"] = ...
```

Bespoke profile dicts and local copies of `predictor_for` / `_creation_opts` accumulate inconsistencies across codecs. Call `creation_opts` once and use its return value.
