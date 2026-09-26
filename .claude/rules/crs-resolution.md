---
paths:
  - "python/geobrix/src/databricks/labs/gbx/**"
  - "notebooks/**"
---

# CRS resolution

All CRS classification, normalization, and round-trip operations belong in
`databricks.labs.gbx.core.crs` (the tier-neutral canonical module; the
`pyrx.core.crs` shim re-exports a subset for backward compatibility).

```python
from databricks.labs.gbx.core.crs import (
    resolve_crs,
    crs_to_canonical,
    authority_srid_of,
    to_pyproj_crs,    # rasterio→pyproj bridge; never for storage
    crs_equal,        # semantic CRS equality via pyproj .equals()
    crs_to_proj4,     # OGR-parity PROJ4 output only; never for storage
)
```

- `resolve_crs(src)` — classify user or file input into a `CRSSource` (EPSG, ESRI authority, WKT, proj-string).
- `crs_to_canonical(src)` — normalize to `AUTHORITY:CODE` when a known authority exists, WKT otherwise. This preserves ESRI authority on round-trips; bare `pyproj.CRS.to_wkt()` drops it.
- `authority_srid_of(src)` — extract the numeric SRID when an authority code is known; `None` otherwise.
- `to_pyproj_crs(src)` — convert a rasterio `CRS` to a `pyproj.CRS`; use for inter-library bridges, not for canonical string output.
- `crs_equal(a, b)` — semantic equality via `pyproj.CRS.equals()`; handles ESRI/EPSG variants and PROJ4 strings.
- `crs_to_proj4(src)` — emit an OGR-parity PROJ4 string; never use the output as a canonical storage form.

## Patterns the QC check flags outside `crs.py`

The `centralized-primitives` QC check (level: FAIL) flags any of these outside the canonical files:

```python
ProjCRS.from_epsg(...)          # loses ESRI authority
CRS.from_epsg(...)              # same
ProjCRS.from_wkt(...)           # bypasses authority round-trip
CRS.from_wkt(...)               # same
CRS.from_user_input(...)        # bypasses classify step
CRS.from_string(...)            # same
CRS.from_authority(...)         # bypasses classify step
parse_crs().to_wkt()            # drops authority on ESRI CRS round-trip
```

Use the canonical functions above instead; they handle the ESRI authority edge-cases and are the single authority tested for non-EPSG CRS equivalence.

## Also avoid outside `crs.py` (advisory — not checked by QC)

These inspection methods are legitimate in isolation but commonly signal a
round-trip bug when used to build a new CRS or an output string:

```python
crs.to_wkt()     # fine for CF/OGR WKT-expecting APIs; not for canonical strings (use crs_to_canonical)
crs.to_epsg()    # returns None for ESRI CRS; use authority_srid_of for SRID extraction
```
