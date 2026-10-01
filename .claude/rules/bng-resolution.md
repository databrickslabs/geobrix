---
paths:
  - "src/main/scala/com/databricks/labs/gbx/gridx/**"
  - "python/geobrix/src/databricks/labs/gbx/gridx/**"
  - "docs/docs/api/gridx-functions.mdx"
---

# BNG resolution

Only **integer indices ±1..±6** (1=100km, 2=10km, 3=1km, 4=100m, 5=10m, 6=1m; negatives = quadrants) or string keys from `BNG.resolutionMap` (e.g. `"1km"`, `"100m"`).

**Never** treat metres-as-Int (e.g. `1000`) as a resolution — that interpretation is not supported by `BNG.getResolution`.

`bng_pointascell` expects BNG eastings/northings (EPSG:27700), not WGS84 lon/lat. Use BNG coords in examples (e.g. `POINT(530000 180000)` for London). `gbx_bng_cellarea` returns **square kilometres**, not square metres.

Docs examples for BNG need a GB raster (not the NYC/London sample rasters, which are outside BNG's valid extent).

## Grid tessellation / geom-aware ops

- `rst_h3_tessellate` default = full-hex OVERLAP (mosaic mode); single-assignment in `rastertogrid`.
- Geom-aware kring/kloop is implemented for all grids (h3, quadbin, bng, custom) — light tier.
  Do NOT convert h3 geomk to O(perimeter) boundary-as-line (23–96x regression; reverted). h3
  uses native fill; quadbin/bng/custom use the boundary approach.
- `bng_*explode` functions are streaming UDTFs (no Column form) — permanent `[B]` waiver.
- quadbin has a known light≠heavy divergence on tile-boundary polygon edges (xfail'd, tracked for 0.5.2).
