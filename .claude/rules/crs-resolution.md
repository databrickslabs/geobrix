---
paths:
  - "python/geobrix/src/databricks/labs/gbx/**"
  - "notebooks/**"
---

# CRS resolution

All CRS classification, normalization, and round-trip operations belong in
`databricks.labs.gbx.pyrx.core.crs`.

```python
from databricks.labs.gbx.pyrx.core.crs import resolve_crs, crs_to_canonical, authority_srid_of
```

- `resolve_crs(src)` — classify user or file input into a `CRSSource` (EPSG, ESRI authority, WKT, proj-string).
- `crs_to_canonical(src)` — normalize to `AUTHORITY:CODE` when a known authority exists, WKT otherwise. This preserves ESRI authority on round-trips; bare `pyproj.CRS.to_wkt()` drops it.
- `authority_srid_of(src)` — extract the numeric SRID when an authority code is known; `None` otherwise.

## Patterns to avoid outside `crs.py`

The `centralized-primitives` QC check flags any of these outside the canonical files:

```python
ProjCRS.from_epsg(...)        # pyproj private API; loses ESRI authority
CRS.from_epsg(...)            # same
CRS.from_authority(...)       # bypasses classify step
parse_crs().to_wkt()          # drops authority on ESRI CRS round-trip
```

Use the canonical functions above instead; they handle the ESRI authority edge-cases and are the single authority tested for non-EPSG CRS equivalence.
