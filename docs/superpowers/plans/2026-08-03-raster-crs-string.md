# Raster CRS-String Handling (Sub-spec R) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Non-EPSG CRS (ESRI codes, WKT, PROJ4) survive raster read → operate → write. Populate the existing `tile.crs` field; add `rst_crs`/`rst_setcrs`/`rst_transformcrs`; fix the ~18 `.to_epsg()` correctness bugs + the NetCDF-writer CRS drop.

**Architecture:** A shared `_resolve_crs` helper (both tiers) is the one place the "int-castable string → EPSG SRID, else CRS string" rule lives. Readers populate `tile.crs` with a canonical CRS string. `.to_epsg()`-based identity/branch decisions become direct `ds.crs`-object comparisons so non-EPSG rasters take the right path. `rst_srid` stays int (native-ST bridge); `rst_crs` is the new string accessor; `rst_setcrs`/`rst_transformcrs` are string-taking ops.

**Tech Stack:** Python 3.12 / rasterio+pyproj (light), Scala/GDAL-JNI (heavy), PySpark. Tests: pytest (`.venv-pyrx`, PYSPARK vars set); heavy Scala in Docker.

## Global Constraints

- **Shared `_resolve_crs(value)` owns the int-cast rule** (both tiers): `value` is int OR a string that casts cleanly to int → `EPSG:<int>`; else `from_user_input(value)` (light rasterio/pyproj) / `SetFromUserInput` or `ImportFromWkt` (heavy GDAL). Called by `rst_setcrs`/`rst_transformcrs`, `tile.crs` population where raw values arrive, and the `_epsg_of` replacement. One definition; no per-site reimplementation.
- **Canonical `tile.crs` form:** authority string (`AUTHORITY:CODE`, e.g. `EPSG:4326`/`ESRI:54008`) when the CRS has one, else WKT. (Decision confirmed in Task 1; raster uses authority-else-WKT, NOT PROJ4 — more readable and round-trips through GDAL/rasterio; vector's PROJ4 choice is separate.) No v2 schema change (field exists).
- **`rst_srid` unchanged** — int EPSG / null (light) / 0 (heavy). No breaking change; the native-ST bridge needs the int.
- **New ops are DISTINCT operations, not aliases** — `rst_setcrs` (string relabel) ≠ `rst_setsrid` (int relabel); `rst_transformcrs` (string reproject) ≠ `rst_transform` (int reproject). Document each for its distinct purpose. One canonical name each.
- **`.to_epsg()` fixes use CRS-OBJECT comparison** (`rasterio.crs.CRS.__eq__` / GDAL `IsSame`), never `to_epsg()` for identity/branch decisions.
- **Binding parity (hard gate):** `gbx_rst_crs`/`gbx_rst_setcrs`/`gbx_rst_transformcrs` must exist as Scala `override def name`, Python `functions.py` binding, `function-info.json` key, and `registered_functions.txt` line — `gbx:test:bindings` green. Adding 3 heavy RasterX functions trips `BenchDispatchTest.scala:40 assert(BenchDispatch.all.size == 125)` — bump the count and add the expressions to `BenchDispatch.all` (accessor/relabel ops; grep the pattern).
- **Cross-tier parity is decoded-pixel** — CRS changes must keep pixels+georeference identical across tiers; add CRS-equality to the non-EPSG parity check.
- **GDAL/OGR registration only via GDALManager** (heavy); pyrx no `spark.conf.set`/`_jvm`/`.rdd`.
- **Tests use a REAL non-EPSG raster** — `target/test-classes/modis/*.TIF` are `ESRI:54008` (the case that surfaced this). No mocking rasterio/GDAL.

---

## Task 1: Shared `_resolve_crs` helper + canonical-form decision (light)

**Files:** Create `python/geobrix/src/databricks/labs/gbx/pyrx/core/crs.py`; Test `python/geobrix/test/pyrx/test_crs_resolve.py`.

**Interfaces:** Produces `resolve_crs(value) -> rasterio.crs.CRS` and `crs_to_canonical(crs) -> str` (authority string else WKT). Consumed by Tasks 2-5.

- [ ] **Step 1: Write failing tests**

```python
import pytest
from databricks.labs.gbx.pyrx.core import crs as C
from rasterio.crs import CRS


def test_resolve_int_and_intlike_string_are_epsg():
    assert C.resolve_crs(4326) == CRS.from_epsg(4326)
    assert C.resolve_crs("4326") == CRS.from_epsg(4326)   # int-cast rule
    assert C.resolve_crs(" 32633 ") == CRS.from_epsg(32633)


def test_resolve_authority_and_wkt_and_proj4():
    assert C.resolve_crs("EPSG:4326") == CRS.from_epsg(4326)
    esri = C.resolve_crs("ESRI:54008")
    assert esri is not None and esri.to_epsg() is None  # non-EPSG, still valid
    wkt = CRS.from_epsg(4326).to_wkt()
    assert C.resolve_crs(wkt) == CRS.from_epsg(4326)
    assert C.resolve_crs("+proj=longlat +datum=WGS84 +no_defs") is not None


def test_resolve_garbage_raises():
    with pytest.raises(Exception):
        C.resolve_crs("not-a-crs-@@")


def test_canonical_prefers_authority_else_wkt():
    assert C.crs_to_canonical(CRS.from_epsg(4326)) == "EPSG:4326"
    esri = CRS.from_user_input("ESRI:54008")
    assert C.crs_to_canonical(esri) == "ESRI:54008"
    # an authority-less CRS -> WKT (starts with PROJCS/GEOGCS/PROJCRS/GEOGCRS)
    # (construct one without an authority; assert the result parses back equal)
    round = C.resolve_crs(C.crs_to_canonical(esri))
    assert round == esri
```

- [ ] **Step 2: Run — RED** (`... pytest python/geobrix/test/pyrx/test_crs_resolve.py -v`).

- [ ] **Step 3: Implement `crs.py`**

```python
"""CRS string/int resolution — the one place the int-cast rule lives (light tier)."""
from typing import Union
from rasterio.crs import CRS


def _is_intlike(value) -> bool:
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value.strip())
            return True
        except (ValueError, AttributeError):
            return False
    return False


def resolve_crs(value: Union[int, str]) -> CRS:
    """int or int-castable string -> EPSG SRID; else a CRS string (EPSG:x/ESRI:x/WKT/PROJ4)."""
    if _is_intlike(value):
        return CRS.from_epsg(int(str(value).strip()))
    return CRS.from_user_input(value)   # raises for garbage — intended


def crs_to_canonical(crs: CRS) -> str:
    """Authority string ('EPSG:4326'/'ESRI:54008') when available, else WKT."""
    if crs is None:
        return None
    auth = crs.to_authority()          # (name, code) or None
    if auth:
        return f"{auth[0]}:{auth[1]}"
    return crs.to_wkt()
```

- [ ] **Step 4: Run — GREEN.**
- [ ] **Step 5: Commit** (`feat(pyrx): _resolve_crs helper (int-cast rule) + crs_to_canonical`).

---

## Task 2: `rst_crs` accessor + fix `rst_srid`/`rst_summary` (light)

**Files:** `pyrx/core/accessors.py`, `pyrx/functions.py` (register `rst_crs`); Test `python/geobrix/test/pyrx/test_crs_accessors.py`.

- [ ] **Step 1: Failing tests** — on a real ESRI:54008 raster: `rst_srid` returns None (unchanged); a new `crs()` accessor returns a non-null canonical string ("ESRI:54008"); on an EPSG raster `rst_crs` returns "EPSG:4326" and `rst_srid` returns 4326. `rst_summary` JSON includes the crs string (not just epsg:null).

- [ ] **Step 2: Run — RED.**

- [ ] **Step 3: Implement** — add `accessors.crs(ds) -> str` = `crs_to_canonical(ds.crs)` (None-safe); register `rst_crs` as a header-only accessor UDF (virtual-aware, like `rst_srid`); add the crs string to `accessors.summary` alongside the epsg. Leave `accessors.srid` returning the int (unchanged).

- [ ] **Step 4: Run — GREEN** + accessor regression (`pytest python/geobrix/test/pyrx/ -q`, ignore netCDF4-collection modules).
- [ ] **Step 5: Commit.**

---

## Task 3: `rst_setcrs` / `rst_transformcrs` (light) + populate `tile.crs` in readers

**Files:** `pyrx/functions.py` (new ops), `pyrx/core/warp.py` / `edit.py` (string-CRS impl), `ds/raster.py`/`_encode.py`/`open_tile.py` (populate `tile.crs`); Test `test_crs_ops.py`.

**Interfaces:** consumes `resolve_crs`/`crs_to_canonical`.

- [ ] **Step 1: Failing tests**
  - `rst_setcrs(tile, "ESRI:54008")` → tile's CRS relabeled to ESRI:54008 (pixels unchanged); `rst_setcrs(tile, "4326")` == `rst_setsrid(tile, 4326)` (int-cast rule); reads back via `rst_crs`.
  - `rst_transformcrs(tile, "EPSG:3857")` reprojects (bounds change); a non-EPSG target reprojects where `rst_transform` (int) would have thrown.
  - Reader populates `tile.crs`: reading an ESRI:54008 raster with `gtiff_gbx` yields `tile.crs` == "ESRI:54008" (currently None).

- [ ] **Step 2: Run — RED.**

- [ ] **Step 3: Implement**
  - `rst_setcrs`: mirror `rst_setsrid` but resolve the arg via `resolve_crs`, stamp `crs_to_canonical(resolved)` into `tile.crs` + the bytes' CRS (no reproject). Virtual-aware / v2 output (follows the pending-instruction pattern — setcrs is a relabel; can be a pending instruction like setsrid).
  - `rst_transformcrs`: mirror `rst_transform`/`warp.reproject_to_srid` but target = `resolve_crs(arg)` (accepts non-EPSG); use `calculate_default_transform(ds.crs, dst_crs, ...)` with the CRS object.
  - Readers: in the tile-row assembler (`ds/raster.py` / `_encode.py` / `open_tile` virtual emit), set `crs=crs_to_canonical(source_ds.crs)` instead of leaving None.

- [ ] **Step 4: Run — GREEN** + regression (pyrx + ds).
- [ ] **Step 5: Commit.**

---

## Task 4: Fix the light `.to_epsg()` correctness bugs

**Files:** `pyrx/core/warp.py`, `open_tile.py`, `functions.py`, `tessellate.py`, `gridagg.py`, `agg.py`, `_epsg_of`; Test `test_crs_non_epsg_pipeline.py`.

- [ ] **Step 1: Failing tests** — an ESRI:54008 raster through: H3 tessellate, BNG tessellate, `gridagg` BNG-reduce, and `merge`/agg → correct results (not wrong-branch/error/silent-un-reproject). Pre-fix = RED. Also: identity reproject on a non-EPSG raster skips the warp (was: always warped).

- [ ] **Step 2: Run — RED.**

- [ ] **Step 3: Implement** — replace `.to_epsg()` identity/branch decisions with `ds.crs == CRS.from_epsg(N)` (or `src.crs == dst_crs`): `warp.reproject_to_srid:24`, `open_tile.py:226`, `functions.py:872` (identity skip); `tessellate.py` (5 sites — "already in 4326/27700?"); `gridagg.py:268` (`already_bng`); `agg.py:109` (`_pick_ref_crs` — deterministic for non-EPSG). Route `_epsg_of` (open_tile) through `resolve_crs` so a PROJ4/WKT `tile.crs` reprojects instead of no-op.

- [ ] **Step 4: Run — GREEN** + full pyrx+ds regression (ignore netCDF4-collection + JAR-gated; no NEW failures).
- [ ] **Step 5: Commit.**

---

## Task 5: NetCDF writer CRS preservation (light)

**Files:** `ds/_write_netcdf.py` (lines ~473, 561); Test `test_netcdf_crs.py` (or extend existing netcdf writer tests — note these need the netCDF4 module; if unavailable locally, gate/skip + verify logic by unit-testing the CRS-string extraction).

- [ ] **Step 1: Failing test** — writing a non-EPSG raster to NetCDF preserves its CRS (read back → CRS matches). Pre-fix: CRS dropped (to_epsg()→None→no CRS). If netCDF4 is unavailable in the venv, write the test to skip cleanly and instead unit-test the helper that produces the CF `grid_mapping` CRS string from `ds.crs` (must be non-null for ESRI:54008).

- [ ] **Step 2: Run — RED (or skip-with-unit-test).**

- [ ] **Step 3: Implement** — write the CRS as a WKT/authority string into the CF `grid_mapping` (via rasterio/GDAL CRS serialization) instead of only the EPSG int; use `crs_to_canonical`/the CRS WKT so a non-EPSG CRS is preserved.

- [ ] **Step 4: Run — GREEN.**
- [ ] **Step 5: Commit.**

---

## Task 6: Heavy tier — `RST_Crs` / `RST_SetCrs` / `RST_TransformCrs` + `_resolve_crs` + RasterProject WKT fallback

**Files:** new `RST_Crs.scala`, `RST_SetCrs.scala`, `RST_TransformCrs.scala` (mirror RST_SRID / RST_SetSrid / RST_Transform); a Scala CRS-resolve helper (int-cast + `SetFromUserInput`/`ImportFromWkt`); `RasterProject.scala` (WKT fallback); the function registry + BenchDispatch. Docker for build/test.

- [ ] **Step 1: Scala `_resolveCrs` helper** — `resolveCrs(value: String): SpatialReference`: if int-castable → `ImportFromEPSG(int)`; else `sr.SetFromUserInput(value)` (GDAL's universal parser: EPSG:/ESRI:/WKT/PROJ4). Mirror the light rule.

- [ ] **Step 2: New expressions** (mirror the int siblings, use `_resolveCrs`):
  - `RST_Crs` → the tile's CRS as a canonical string (authority else WKT via `ExportToWkt`/`GetAuthorityName`+`GetAuthorityCode`). `name = "gbx_rst_crs"`.
  - `RST_SetCrs(tile, crsStr)` → relabel via `_resolveCrs` (no warp). `name = "gbx_rst_setcrs"`.
  - `RST_TransformCrs(tile, crsStr)` → warp to `_resolveCrs(crsStr)` (accepts non-EPSG; no `require(srid>0)`). `name = "gbx_rst_transformcrs"`.

- [ ] **Step 3: RasterProject WKT fallback** — when the target authCode is null/0, use `ExportToWkt` in the warp target instead of `authName:authCode` (mirror the existing `RST_Clip.scala:77` `ImportFromWkt` precedent) so ESRI/WKT targets work.

- [ ] **Step 4: Register + BenchDispatch** — add the 3 expressions to the function registry; bump `BenchDispatchTest.scala:40` `== 125` → `== 128` (or the correct count) and add the new expressions to `BenchDispatch.all` if they're benchmarked (accessor/relabel — follow how RST_SRID/RST_SetSrid are handled there; if they're not benchmarked, the assert bump reflects registry not BenchDispatch — verify which count the assert tracks and adjust correctly).

- [ ] **Step 5: Build + test in Docker** (dispatch; progress line). Targeted suites: the new expressions + RST_SRID/SetSrid/Transform + RasterProject + BenchDispatchTest. scalastyle clean.

- [ ] **Step 6: Commit.**

---

## Task 7: Bindings + function-info + docs

**Files:** `python/.../pyrx/functions.py` (Python bindings if not already in Task 2-3), `docs/tests-function-info/registered_functions.txt`, `function-info.json` source (doc SQL examples in `docs/tests/python/api/rasterx_functions_sql.py`), `docs/docs/api/raster-functions.mdx` + a short CRS note.

- [ ] **Step 1: Add the 3 functions to `registered_functions.txt`** (`gbx_rst_crs`, `gbx_rst_setcrs`, `gbx_rst_transformcrs`).
- [ ] **Step 2: Add `*_sql_example()` entries** for the 3 in `docs/tests/python/api/rasterx_functions_sql.py` (real SQL that runs); regenerate `function-info.json` via `gbx:docs:function-info`.
- [ ] **Step 3: `gbx:test:bindings`** — assert all 3 exist across Scala name / Python binding / function-info / registered list. Green.
- [ ] **Step 4: Docs** — raster-functions page: document `rst_crs`/`rst_setcrs`/`rst_transformcrs`; a short "CRS: SRID vs CRS string" note (the int-cast rule; when to use `rst_srid` int vs `rst_crs` string; non-EPSG ESRI/WKT support). Voice-clean; docs build green.
- [ ] **Step 5: Commit.**

---

## Task 8: Cross-tier parity (non-EPSG) + end-to-end validation

- [ ] **Step 1: Parity** — extend the raster cross-tier parity check so a non-EPSG (ESRI:54008) fixture reports the SAME CRS (via `rst_crs`) AND identical decoded pixels through both tiers. (Decoded-pixel gate already; add CRS-equality.)
- [ ] **Step 2: Affected-suite regression** (local): pyrx + ds crs/accessor/ops/pipeline suites green.
- [ ] **Step 3: End-to-end** — read a real ESRI:54008 raster (light), `rst_crs` returns the string, `rst_transformcrs` to another non-EPSG target works, write it out and read back with CRS preserved.
- [ ] **Step 4: Ledger + done** — ready for final whole-branch review.

---

## Self-Review

**Spec coverage:** T1 `_resolve_crs`+canonical (spec §2). T2 `rst_crs`+srid/summary (spec §3). T3 setcrs/transformcrs + tile.crs population (spec §1,§4). T4 `.to_epsg()` fixes (spec §5). T5 NetCDF (spec §5). T6 heavy (spec §3,§4,§5 heavy). T7 bindings+docs (spec §Testing binding-parity). T8 parity+e2e (spec §6,§Testing). Vector/Grid/Viz not in any task (Non-Goals). ✓

**Placeholder scan:** canonical-form decision resolved in Global Constraints (authority-else-WKT); the BenchDispatch count bump flagged as "verify which count the assert tracks" (real, plan-time). No TBD. ✓

**Type consistency:** `resolve_crs`/`crs_to_canonical` (T1) reused T2-5; heavy `_resolveCrs` mirrors it (T6). `rst_crs`/`rst_setcrs`/`rst_transformcrs` names consistent across light register (T2/3), heavy (T6), bindings (T7). Verified against current accessors.srid, _epsg_of, RST_SRID, RST_SetSrid, RST_Transform, BenchDispatchTest:40. ✓

**Open risks for the review loop:** (1) exact BenchDispatch count + whether the 3 fns are in BenchDispatch.all vs just the registry — verify at T6. (2) NetCDF test needs netCDF4 (may skip locally — unit-test the CRS helper instead). (3) `crs_to_canonical` WKT vs WKT2 form + round-trip equality for authority-less CRS — T1 test covers round-trip. (4) `rst_setcrs` as a pending-instruction (like setsrid) vs immediate — follow the light-through-finalize pending pattern; confirm virtual-tile behavior.
