# Raster CRS-String Handling (Sub-spec R of CRS-across-the-board) — Design

**Status:** Approved design (2026-08-03). Feeds an implementation plan. First of four CRS sub-specs (R raster / V vector / G grid / X viz); the keystone the others build on.

**One-liner:** Make non-EPSG CRS (ESRI codes, WKT, PROJ4) survive raster read → operate → write, filling the gap where Databricks product spatial is SRID-(EPSG-int)-only.

---

## Problem

Databricks product spatial accepts only an integer SRID (`st_geomfromwkb(wkb, srid)`), and GeoBrix's raster tier inherited that assumption everywhere: `rst_srid` is `ds.crs.to_epsg()` which returns `None`/`0` for any CRS with no EPSG code (ESRI:54008, raw WKT, PROJ4), and `rst_setsrid`/`rst_transform` take an int and `require(srid > 0)`. A recon (`.superpowers/sdd/crs-recon.md`) found:

- **The v2 tile struct already has a `crs: string` field** (nullable) — but every reader leaves it `None`. The GTiff bytes embed full WKT; the struct field is unused.
- **~18 `.to_epsg()` gap points** (12 light production + ~6 heavy) where non-EPSG input causes real bugs, not just null returns:
  - `pyrx/core/tessellate.py` (5 sites): `dst_epsg = ds.crs.to_epsg()` → `None` → wrong reproject branch / error for non-EPSG rasters into H3/BNG.
  - `pyrx/core/gridagg.py:268`: `already_bng = ...to_epsg() == 27700` → a non-EPSG raster equivalent to 27700 is needlessly re-warped.
  - `pyrx/core/agg.py:109` (`_pick_ref_crs`): non-EPSG tiles excluded from the ref-CRS sort → indeterminate reference CRS.
  - `pyrx/core/warp.py:24`, `open_tile.py:226`, `functions.py:872`: identity short-circuit never fires for non-EPSG → always warps (missed optimization; correctness OK).
  - `pyrx/core/accessors.py:41,290` (`rst_srid`, `rst_summary`): return `None`/drop the CRS.
  - `ds/_write_netcdf.py:473,561`: **NetCDF writer embeds only the EPSG int → a non-EPSG raster writes a NetCDF with NO CRS (lost georeference — data corruption).**
  - Heavy: `RST_SRID` returns `0`; `RST_Transform`/`RST_SetSrid` `require(srid > 0)` → throw; `RasterProject` builds `gdalwarp -t_srs authName:authCode` → breaks when authCode is null (ESRI). Escape hatch precedent: `RST_Clip.scala:77` already does `ImportFromWkt` fallback.
- **`_epsg_of()` (open_tile.py) only parses `EPSG:<int>`** — a PROJ4/WKT value in `tile.crs` is silently dropped (no warp).
- **The vector tier already solved the analogous problem** with a dual-column `geom_0_srid` + `geom_0_srid_proj` (PROJ4 fallback) — the model this borrows, adapted to raster's single existing `crs` field.

---

## Design

### 1. Carrier — populate the existing `tile.crs` string field

No v2 schema change (the field exists, nullable). Readers populate `tile.crs` with a **canonical CRS string**: an authority string (`"EPSG:4326"`, `"ESRI:54008"`) when the CRS has one, else the full WKT (or PROJ4). The materialized GTiff bytes remain the embedded source of truth; `tile.crs` is provenance on a materialized tile and the reprojection/relabel *instruction* on a virtual tile (consistent with the virtual-tile reference-vs-instruction model). Canonical strings from GDAL/rasterio are unambiguous (never a bare `"4326"`).

### 2. Shared `_resolve_crs` helper (both tiers) — the int-cast rule lives here

A single helper is the one definition of how any CRS value becomes a CRS object:

```
_resolve_crs(value) -> CRS object / WKT:
  - value is int, OR a string that casts cleanly to int  -> EPSG:<int>   (the SRID rule)
  - else                                                  -> from_user_input(value)   # "EPSG:x" | "ESRI:x" | WKT | PROJ4
```

Light: `rasterio.crs.CRS.from_epsg(n)` / `CRS.from_user_input(s)`. Heavy: `ImportFromEPSG(n)` / `SetFromUserInput(s)` (GDAL's universal parser; `ImportFromWkt` for WKT). **Every CRS entry point calls it** — `rst_setcrs`/`rst_transformcrs`, `tile.crs` population where a raw value could arrive, and the `_epsg_of` replacement — so `"4326"`, `"ESRI:54008"`, `"EPSG:4326"`, and WKT all resolve consistently regardless of path.

### 3. Accessors

- **`rst_crs(tile)`** (NEW, both tiers) → the CRS as a **string, always** (authority string, else WKT/PROJ4). The universal accessor for the full-CRS world. Reads `tile.crs` when set, else derives from the raster bytes' embedded CRS.
- **`rst_srid(tile)`** — unchanged: int EPSG, `null`/`0` when none. Preserves back-compat and the Databricks native-ST bridge (which needs the int).

### 4. Operations — string-taking siblings

- **`rst_setcrs(tile, crs)`** (NEW, both tiers) — relabel the CRS from a string (int-castable → EPSG; else CRS string). The string analog of the int-only `rst_setsrid`. No reproject.
- **`rst_transformcrs(tile, crs)`** (NEW, both tiers) — reproject to a target given as a string. The string analog of the int-only `rst_transform`.

These are **distinct operations**, not aliases — int-SRID assignment vs. arbitrary-CRS-string assignment — consistent with the "one canonical name per function" rule; each is documented for its distinct purpose. `rst_setsrid`/`rst_transform` remain int-only and unchanged.

### 5. Fix the `.to_epsg()` correctness bugs (the "actually works" part)

Replace `.to_epsg()`-based identity/branch decisions with **direct `ds.crs` comparisons** (rasterio `CRS.__eq__` / GDAL `IsSame`), which work for any CRS:

- `warp.reproject_to_srid`, `open_tile` warp-skip, `functions._transform_bytes`: compare `src.crs == dst_crs` directly (identity skip now fires for non-EPSG too).
- `tessellate.py` (×5): decide "already in grid CRS (4326/27700)?" by `ds.crs == CRS.from_epsg(4326/27700)` instead of `to_epsg() == …`.
- `gridagg.py:268`: `already_bng` via `ds.crs == CRS.from_epsg(27700)`.
- `agg._pick_ref_crs`: make ref-CRS selection deterministic for non-EPSG tiles (compare CRS objects; don't exclude non-EPSG from the choice).
- `_epsg_of` → routed through `_resolve_crs` so a PROJ4/WKT `tile.crs` reprojects instead of no-op.
- **NetCDF writer** (`_write_netcdf.py:473,561`): write the CRS as a string/WKT into the CF `grid_mapping` (via rasterio/GDAL CRS serialization) instead of only an EPSG int, so a non-EPSG raster keeps its georeference.
- Heavy: `RST_SRID` stays int (0 for non-EPSG — back-compat); the new string accessor/ops carry the non-EPSG path; `RasterProject`'s warp target gains a **WKT fallback** when `authCode` is null (mirror the existing `RST_Clip.scala:77` `ImportFromWkt` escape hatch) so ESRI/WKT reprojection targets work.

### 6. Cross-tier parity

The raster parity gate is decoded-pixel (established in the compression work), so CRS-string changes that reproject/relabel identically across tiers keep parity on pixels + georeference. A parity check should now include CRS equality (both tiers report the same CRS for a non-EPSG fixture) — verify a non-EPSG tile round-trips the same CRS through both tiers.

---

## Non-Goals (→ separate follow-on specs)

- **Sub-spec V (vector):** the `geom_0_srid` authority-name completeness ("54008" → "ESRI:54008"). Vector already has the dual-column PROJ4 fallback; small incremental fix, its own spec.
- **Sub-spec X (viz):** `plot_static(srid=...)` accepting a CRS string. Depends on R surfacing the CRS string first; its own spec.
- **Sub-spec G (grid):** custom-grid non-EPSG CRS (`GridConf.crsWkt`). Not needed for BNG/H3/quadbin (CRS intrinsic); future-proofing only.
- **No v2 tile-struct schema change** (uses the existing `crs` field).
- **`rst_srid` stays int** — no breaking change to it or the native-ST bridge.

---

## Testing

Real rasters incl. a **non-EPSG source** (the repo has `ESRI:54008` MODIS test files — the exact case that surfaced this). No mocking rasterio/GDAL.

1. **`_resolve_crs` unit (both tiers):** int → EPSG; int-like string "4326" → EPSG:4326; "EPSG:4326" → EPSG:4326; "ESRI:54008" → the ESRI CRS; a WKT string → that CRS; a PROJ4 string → that CRS; garbage → clear error.
2. **Carrier:** reading an ESRI:54008 raster populates `tile.crs` with a non-null canonical string; a virtual tile carries it; materialize preserves it.
3. **`rst_crs`:** returns the authority string for an EPSG raster and a non-null WKT/authority string for the ESRI:54008 raster (where `rst_srid` returns null/0).
4. **`rst_setcrs` / `rst_transformcrs`:** setcrs("ESRI:54008") relabels; setcrs("4326") == setsrid(4326) (int-cast rule); transformcrs to a non-EPSG target reprojects (was: `require srid>0` threw); pixels/bounds correct.
5. **`.to_epsg()` bug fixes:** an ESRI:54008 raster through tessellate (H3 + BNG), gridagg, and merge/agg produces correct results (not the wrong branch / not silently un-reprojected) — the pre-fix behavior is the RED.
6. **NetCDF writer:** writing a non-EPSG raster to NetCDF preserves its CRS (read back → CRS matches, not dropped).
7. **Heavy:** `rst_crs`/`rst_setcrs`/`rst_transformcrs` Scala expressions; `RasterProject` reprojects to an ESRI/WKT target via the WKT fallback.
8. **Cross-tier:** a non-EPSG fixture reports the same CRS + same decoded pixels through both tiers (parity).
9. **Binding parity:** `rst_crs`/`rst_setcrs`/`rst_transformcrs` added to all bindings (Scala `override def name`, Python `functions.py`, `function-info.json`, `registered_functions.txt`) — `gbx:test:bindings` green. Adding 3 heavy RasterX functions also trips the hardcoded `BenchDispatch.all.size == N` assert — bump it (grep for the count) and add the new expressions to BenchDispatch. New functions must join a performance-doc family only if they fit one (per the perf-doc convention).

---

## Files (anticipated; finalized in the plan)

- Light: `pyrx/core/crs.py` (NEW — `_resolve_crs` + CRS-string helpers); `pyrx/core/accessors.py` (`rst_crs` core, fix `srid`/`summary`); `pyrx/core/warp.py`, `open_tile.py`, `functions.py`, `tessellate.py`, `gridagg.py`, `agg.py` (`.to_epsg()` → `ds.crs` compare, `_epsg_of` → `_resolve_crs`); `pyrx/functions.py` (register `rst_crs`/`rst_setcrs`/`rst_transformcrs`); `ds/raster.py`/`_encode.py`/`open_tile.py` (populate `tile.crs` in readers); `ds/_write_netcdf.py` (CRS into grid_mapping).
- Heavy: new `RST_Crs`/`RST_SetCrs`/`RST_TransformCrs` expressions + a Scala `_resolve_crs` (SetFromUserInput/int-cast); `RasterProject` WKT fallback; register in the function registry.
- Bindings: `functions.py`, `function-info.json` source, `registered_functions.txt`, doc SQL examples.
- Docs: raster-functions page + a short CRS note (authority string vs SRID int; the int-cast rule; `rst_crs`/`rst_setcrs`/`rst_transformcrs`); voice-clean; sidebar if a new page.
- Tests: `pyrx` + `ds` + heavy Scala suites per §Testing.

---

## Open items for the plan

- Exact canonical-string form to store in `tile.crs` — prefer authority string (`AUTHORITY:CODE`) when `AutoIdentifyEPSG`/`to_authority` yields one, else WKT (compact WKT2?) vs PROJ4. Pick one canonical form; the recon shows vector uses PROJ4 — decide whether raster matches vector (PROJ4) or uses authority-string-else-WKT for readability.
- Whether `rst_setsrid`/`rst_transform` (int) should internally delegate to the string ops via `_resolve_crs` (dedupe) or stay separate.
- Heavy `RST_SRID` returning `0` vs `null` for non-EPSG — leave as `0` (back-compat) but confirm `rst_crs` is the documented path for non-EPSG.
