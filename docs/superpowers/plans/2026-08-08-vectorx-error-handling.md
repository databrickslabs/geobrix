# VectorX Error Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every VectorX function degrade bad geometry *data* to NULL and raise a clear error for a bad CRS *argument*, across both tiers — retiring the garbage-bytes divergence and adding a conservative area_of_use domain check — plus a new Error Handling docs page.

**Architecture:** One principle drives everything: **bad/non-executable PARAMETER → raise; bad DATA → NULL/degrade.** Light lives in `pyvx/_crs.py` + shared `core/crs.py` (pyproj); heavy in `vectorx/expressions/ST_TransformCrs.scala` + `ST_SetCrs.scala` + shared `operations/SpatialRefOps.scala` (GDAL) using the existing `CrsOutcome` sealed type (no `safeEval` in VectorX). Cross-tier parity is the spine.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / Java 17 / GDAL (heavy); Python 3.12 + shapely + pyproj (light pyvx). All test/build via the `gbx:*` palette inside the `geobrix-dev` Docker container.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-08-08-vectorx-error-handling-design.md`. VectorX ONLY — do NOT touch rasterx, gridx, pyrx, pygx. GridX and PROJ grid-shift are later CRS-thread items.
- **The contract (verbatim):**
  - Bad geometry DATA (unparseable WKB/WKT, corrupt bytes, non-finite reprojection output, out-of-domain reprojection, **embedded SRID present but unresolvable**, **plain geom with no resolvable source CRS**) → **NULL**. One bad row must not kill the stage.
  - Bad CRS ARGUMENT (bad/unparseable target CRS literal; authority-less CRS on `st_setcrs`; **explicitly-provided `source_crs` that is unresolvable**) → **RAISE** (`IllegalArgumentException` heavy / `ValueError` light), message names the offending CRS.
- **Source-CRS data/parameter split (the subtle part — apply exactly):**

  | condition | class | outcome |
  |---|---|---|
  | embedded SRID present but unresolvable | data (SRID rides in the geom) | **NULL** |
  | embedded SRID absent, explicit `source_crs` provided but unresolvable | parameter | **RAISE** |
  | embedded SRID absent, no `source_crs` provided (plain geom) | data has no CRS | **NULL** (was: unchanged) |
  | embedded SRID absent, explicit `source_crs` provided and resolvable | OK | transform |
  | bad/unresolvable TARGET crs (either arity) | parameter | **RAISE** (already does — keep) |
- **"Return unchanged" is fully retired** on `st_transformcrs` (both tiers). Every former `Unchanged(geom)` / `return geom` for a DATA condition becomes NULL; the only non-degrade exit is a successful transform.
- **Domain check:** on the transform path only. Obtain the input geometry's coordinates in **lon/lat (EPSG:4326)** (source is already geographic in the common case; transform source→4326 when it is not), and bbox-test every coordinate against the **target CRS's `area_of_use` bounds** (west,south,east,north in 4326). ANY coordinate outside → out-of-domain → **NULL** (straddling → NULL). If the target CRS has **no** area_of_use metadata → **skip the check** (never NULL what cannot be disproved). Light: `pyproj.CRS.area_of_use.bounds`; heavy: GDAL `SpatialReference.GetAreaOfUse()`.
- **No new per-call knob.** No `strict`/`check_domain` parameter (SQL binds positionally). Domain check is on-by-default, conservative.
- **`git add` EXPLICIT paths only** — never `git add -A`. Commit ≤72-char subject + WHY body + `Co-authored-by: Isaac`.
- **Tests real, not mocked**; run ONLY affected suites via `gbx:test:scala` / `gbx:test:python` in Docker. Heavy Scala eval suites need no staged JAR; light pyvx is pure Python.
- **Docs voice:** no internal planning vocabulary (QC `internals-leak`); wire any new page into `docs/sidebars.js` (per repo norm).
- **Facts that are NOT findings:** VectorX has zero `safeEval` by design (uses `CrsOutcome`); `st_crs`/`st_setcrs`/`st_transformcrs` SQL surface always returns BINARY (WKB); a bad *target* CRS already raises on both tiers (correct).

---

### Task 1: Light domain-check helper in shared `core/crs.py`

The reusable primitive light consumes; heavy reimplements the same logic in Scala (Task 3).

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/core/crs.py` (add helper near `get_transformer`, ~line 200)
- Test: `python/geobrix/test/pyvx/test_crs.py` (unit tests for the helper)

**Interfaces:**
- Produces: `in_target_domain(lonlat_coords, target_crs) -> Optional[bool]` where `lonlat_coords` is an (N,2) array of lon/lat pairs and `target_crs` is a resolved `pyproj.CRS`. Returns `True` (all inside), `False` (any outside → out-of-domain), or `None` (target has no `area_of_use` → caller skips the check). Task 2 consumes this.

- [ ] **Step 1: Write the failing tests**

In `test/pyvx/test_crs.py` (module already has `importorskip("pyproj")`):
```python
import numpy as np
import pyproj
from databricks.labs.gbx.core.crs import in_target_domain

def test_in_target_domain_inside_gb():
    # London lon/lat is inside EPSG:27700 (British National Grid) area_of_use.
    tgt = pyproj.CRS.from_epsg(27700)
    assert in_target_domain(np.array([[-0.13, 51.5]]), tgt) is True

def test_in_target_domain_far_outside_gb():
    # lon 150, lat -80 is nowhere near GB — the finite-nonsense survivor.
    tgt = pyproj.CRS.from_epsg(27700)
    assert in_target_domain(np.array([[150.0, -80.0]]), tgt) is False

def test_in_target_domain_straddling_is_false():
    tgt = pyproj.CRS.from_epsg(27700)
    coords = np.array([[-0.13, 51.5], [150.0, -80.0]])  # one in, one out
    assert in_target_domain(coords, tgt) is False

def test_in_target_domain_bounds_absent_returns_none():
    # A CRS with no area_of_use (raw PROJ4 with no bounds) -> None (skip).
    tgt = pyproj.CRS.from_proj4("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs")
    result = in_target_domain(np.array([[0.0, 0.0]]), tgt)
    assert result is None or result is True  # None if no area_of_use; helper must not raise
```

- [ ] **Step 2: Run, verify FAIL** (`in_target_domain` undefined)

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_crs.py --log vx-domain-helper.log`

- [ ] **Step 3: Implement the helper**

Add to `core/crs.py`:
```python
def in_target_domain(lonlat_coords, target_crs) -> "Optional[bool]":
    """Is every lon/lat coordinate inside target_crs's area_of_use bbox?

    lonlat_coords: (N,2) ndarray of (lon, lat) in EPSG:4326 degrees.
    Returns True (all inside), False (any outside -> out-of-domain, incl. straddling),
    or None when target_crs has no area_of_use metadata (caller skips the check —
    never NULL what cannot be disproved). Empty input -> True (no coords to reject).
    """
    aou = getattr(target_crs, "area_of_use", None)
    if aou is None or aou.bounds is None:
        return None
    west, south, east, north = aou.bounds
    if lonlat_coords.shape[0] == 0:
        return True
    lon = lonlat_coords[:, 0]
    lat = lonlat_coords[:, 1]
    inside = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    return bool(inside.all())
```
Ensure `Optional` is imported (it is — `from typing import Optional, Union` is already at the top of `core/crs.py`).

- [ ] **Step 4: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_crs.py --log vx-domain-helper.log`

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/core/crs.py \
        python/geobrix/test/pyvx/test_crs.py
git commit -m "feat(core): area_of_use domain-check helper for CRS transforms

in_target_domain(lonlat_coords, target_crs) returns True/False/None
(None = target has no area_of_use -> skip). Bbox-tests lon/lat against
the target's valid area; any coordinate outside (incl. straddling) is
out-of-domain. Consumed by pyvx st_transformcrs.

Co-authored-by: Isaac"
```

---

### Task 2: Light VectorX contract — `st_transformcrs` + `st_setcrs` (`pyvx/_crs.py`)

Retire the passthrough, apply the source-CRS data/param split, wire the domain check; verify `st_setcrs`.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/_crs.py` — `st_transformcrs` (~line 438-540), `st_setcrs` (~386-436)
- Test: `python/geobrix/test/pyvx/test_crs.py`

**Interfaces:**
- Consumes: `in_target_domain` (Task 1); existing `resolve_crs`, `get_transformer`, `get_coordinates`, `_parse_geom_safe`, `_has_nonfinite_xy`, `_drop_partial_z`.
- Produces: `pyvx._crs.st_transformcrs` / `st_setcrs` with the ratified data→NULL / param→raise behavior. Task 4 (parity) consumes these.

- [ ] **Step 1: Write the failing tests**

In `test/pyvx/test_crs.py`:
```python
import pytest
from databricks.labs.gbx.pyvx import _crs

# helper: EWKB POINT with embedded SRID
def _ewkb(lon, lat, srid):
    from shapely import Point, set_srid, to_wkb
    return to_wkb(set_srid(Point(lon, lat), srid), include_srid=True)

def test_transformcrs_unparseable_data_returns_none():
    assert _crs.st_transformcrs(b"THIS_IS_NOT_WKB", "EPSG:3857") is None

def test_transformcrs_out_of_domain_returns_none():
    # POINT(150 -80) SRID=4326 -> EPSG:27700 is finite but ~16,500km outside GB.
    g = _ewkb(150.0, -80.0, 4326)
    assert _crs.st_transformcrs(g, "EPSG:27700") is None

def test_transformcrs_in_domain_succeeds():
    g = _ewkb(-0.13, 51.5, 4326)  # London, inside GB
    out = _crs.st_transformcrs(g, "EPSG:27700")
    assert out is not None and isinstance(out, (bytes, bytearray))

def test_transformcrs_embedded_srid_unresolvable_returns_none():
    g = _ewkb(1.0, 1.0, 99999)  # SRID 99999 rides in the geometry = DATA
    assert _crs.st_transformcrs(g, "EPSG:3857") is None

def test_transformcrs_bad_explicit_source_crs_raises():
    from shapely import Point, to_wkb
    g = to_wkb(Point(1.0, 1.0))  # plain WKB, no SRID
    with pytest.raises(ValueError):
        _crs.st_transformcrs(g, "EPSG:3857", source_crs="EPSG:99999")

def test_transformcrs_no_source_crs_returns_none():
    from shapely import Point, to_wkb
    g = to_wkb(Point(1.0, 1.0))  # plain WKB, no SRID, no source_crs
    assert _crs.st_transformcrs(g, "EPSG:3857") is None

def test_transformcrs_bad_target_raises():
    g = _ewkb(-0.13, 51.5, 4326)
    with pytest.raises(ValueError):
        _crs.st_transformcrs(g, "EPSG:99999")
```

- [ ] **Step 2: Run, verify FAIL**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_crs.py --log vx-light-transform.log`
Expected FAILs: unparseable/out-of-domain/embedded-unresolvable/no-source currently return the input unchanged (not None); bad-explicit-source currently returns unchanged (not raises).

- [ ] **Step 3: Rewrite `st_transformcrs` degrade paths**

In `pyvx/_crs.py` `st_transformcrs`, apply these exact changes:
1. Unparseable geom (currently `if g is None: return geom` at ~line 476) → `if g is None: return None`.
2. Embedded-SRID-unresolvable branch (currently `except Exception: return geom` at ~490) → `return None` (data).
3. Explicit-`source_crs`-unresolvable branch (currently `except Exception: return geom` at ~495) → **remove the try/except** so `resolve_crs(source_crs)` raises `ValueError` (parameter). (Bad `source_crs` is a provided argument → raise, per the split table.)
4. No-source-CRS branch (currently `if src is None: return geom` at ~500) → `return None` (data has no CRS).
5. After `g = _drop_partial_z(g)` and after `src`/`tgt` are resolved and `g_proj = transform(...)` is computed, keep the existing `_has_nonfinite_xy` → `return None` guard, then ADD the domain check BEFORE the SRID-stamp/return block:
```python
   from databricks.labs.gbx.core.crs import in_target_domain
   from shapely import get_coordinates
   # Domain check: is the INPUT geometry within the target CRS's valid area?
   # Obtain input coords in lon/lat (4326). Common case: source is geographic
   # (embedded 4326 SRID or a geographic source_crs) -> coords already lon/lat.
   lonlat = get_coordinates(g) if src.is_geographic else get_coordinates(
       transform(get_transformer(src, resolve_crs(4326)).transform, g))
   dom = in_target_domain(lonlat, tgt)
   if dom is False:
       return None
   # dom is True (in-domain) or None (target has no area_of_use -> skip) -> proceed
```
Leave the SRID-stamp/output-encoding block that follows unchanged.

- [ ] **Step 4: Verify `st_setcrs` already conforms; add regression tests**

`st_setcrs` already returns None on unparseable geom (data) and raises `ValueError` on authority-less CRS (parameter) — this matches the contract, no code change expected. Add regression tests to lock it:
```python
def test_setcrs_unparseable_data_returns_none():
    assert _crs.st_setcrs(b"NOT_WKB", "EPSG:4326") is None

def test_setcrs_authority_less_crs_raises():
    from shapely import Point, to_wkb
    with pytest.raises(ValueError):
        _crs.st_setcrs(to_wkb(Point(1, 1)), "+proj=merc +datum=WGS84")
```
If either fails, fix `st_setcrs` to match the contract (data→None, authority-less-param→raise) — but per current source, both should pass as-is.

- [ ] **Step 5: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_crs.py --log vx-light-transform.log`
Expected: all pass. Also confirm the docstrings of `st_transformcrs`/`st_setcrs` are updated to state the new NULL-on-bad-data / raise-on-bad-CRS-arg behavior (the current docstrings still say "return unchanged" — fix them to match; a stale docstring is a defect).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyvx/_crs.py \
        python/geobrix/test/pyvx/test_crs.py
git commit -m "fix(pyvx): st_transformcrs degrades bad data to NULL, raises on bad CRS arg

Retire the silent 'return input unchanged' on unparseable data, an
unresolvable embedded SRID, and a source-less geometry -> all NULL now.
An explicitly-provided but unresolvable source_crs raises (a bad
parameter, not data). Add the area_of_use domain check: an input
outside the target CRS's valid area (the finite-nonsense survivor) ->
NULL, skipped when the target has no area_of_use. st_setcrs already
conformed; add regression tests.

Co-authored-by: Isaac"
```

---

### Task 3: Heavy VectorX contract — `ST_TransformCrs` + `ST_SetCrs` + `SpatialRefOps` domain check

Mirror Task 2 on the heavy tier: retire `Unchanged` for data, split source data/param, ADD a non-finite guard (heavy lacks one today), add the GDAL area_of_use domain check.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_TransformCrs.scala` (`TransformCrsImpl.apply`, ~line 149-190)
- Modify: `src/main/scala/com/databricks/labs/gbx/operations/SpatialRefOps.scala` (add a domain-check + lon/lat helper near `crsInfo`/`transformPlan`, ~line 230-262)
- Verify (likely no change): `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_SetCrs.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/ST_CrsFamilyTest.scala`

**Interfaces:**
- Consumes: existing `CrsOutcome.{NullOut, Geom}`, `SpatialRefOps.{crsInfo, transformPlan, transformWithCachedCT}`, GDAL `SpatialReference.GetAreaOfUse()`.
- Produces: heavy `gbx_st_transformcrs`/`gbx_st_setcrs` with data→NULL / param→raise + domain check, byte-parity with light (Task 4 asserts).

- [ ] **Step 1: Write the failing tests**

In `ST_CrsFamilyTest.scala` (uses `ST_TransformCrs`, `GDALManager` init in `beforeAll`; `evalSql` returns `Array[Byte]`, null = NULL). Add:
```scala
test("transformcrs: unparseable data returns NULL (not unchanged)") {
    val out = ST_TransformCrs.evalSql(Array[Byte](1,2,3,4), UTF8String.fromString("EPSG:3857"))
    assert(out == null, "unparseable geom must degrade to NULL")
}
test("transformcrs: out-of-domain point returns NULL") {
    // POINT(150 -80) SRID=4326 -> EPSG:27700, finite but far outside GB
    val g = ewkbPoint(150.0, -80.0, 4326)
    val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:27700"))
    assert(out == null, "out-of-domain reprojection must be NULL")
}
test("transformcrs: in-domain point succeeds") {
    val g = ewkbPoint(-0.13, 51.5, 4326)  // London
    val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:27700"))
    assert(out != null, "in-domain reprojection must produce a geometry")
}
test("transformcrs: unresolvable embedded SRID returns NULL (data)") {
    val g = ewkbPoint(1.0, 1.0, 99999)
    val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"))
    assert(out == null)
}
test("transformcrs: bad explicit source_crs raises (parameter)") {
    val g = wkbPoint(1.0, 1.0)  // plain, no SRID
    assertThrows[IllegalArgumentException] {
        ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"), UTF8String.fromString("EPSG:99999"))
    }
}
test("transformcrs: no source CRS returns NULL (data)") {
    val g = wkbPoint(1.0, 1.0)
    val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"))
    assert(out == null)
}
test("transformcrs: bad target raises") {
    val g = ewkbPoint(-0.13, 51.5, 4326)
    assertThrows[IllegalArgumentException] {
        ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:99999"))
    }
}
```
Add `ewkbPoint(lon, lat, srid)` / `wkbPoint(lon, lat)` private helpers to the test using `JTS`/`GeometryFactory` (mirror the existing `ewkb(srid)` helper at `ST_CrsFamilyTest.scala:34`).

- [ ] **Step 2: Run, verify FAIL**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.ST_CrsFamilyTest' --log vx-heavy-transform.log`
Expected FAILs: unparseable/embedded-unresolvable/no-source currently return `Unchanged` (non-null); out-of-domain currently returns a non-null finite geom; bad-explicit-source currently returns unchanged (not raises). NOTE: the invalid-latitude case may currently raise from GDAL — the out-of-domain finite case (150,-80) is the one that returns finite garbage today.

- [ ] **Step 3: Add the heavy domain-check + lon/lat helper to `SpatialRefOps`**

In `operations/SpatialRefOps.scala`, add (near `crsInfo`):
```scala
    /** Target CRS's area_of_use bbox in EPSG:4326 (west,south,east,north), or None
      * when the CRS carries no area-of-use metadata (caller skips the domain check). */
    def areaOfUse(canonical: String): Option[(Double, Double, Double, Double)] = {
        val sr = resolveCrs(canonical)
        try {
            val a = sr.GetAreaOfUse()  // GDAL 3.0+: AreaOfUse or null
            if (a == null) None
            else Some((a.getWest_lon_degree, a.getSouth_lat_degree,
                       a.getEast_lon_degree, a.getNorth_lat_degree))
        } finally sr.delete()
    }

    /** True when every (lon,lat) is inside the bbox; straddling -> false. */
    def allInBBox(lonLat: Array[(Double, Double)], b: (Double, Double, Double, Double)): Boolean = {
        val (w, s, e, n) = b
        lonLat.forall { case (lon, lat) => lon >= w && lon <= e && lat >= s && lat <= n }
    }
```
(Match the file's existing SR lifecycle discipline — resolve, read, `delete()` in `finally`, per the thread-local cache comments already there.)

- [ ] **Step 4: Rewrite `TransformCrsImpl.apply` degrade paths + add guards**

In `ST_TransformCrs.scala` `apply`:
1. Parse failure (line 152 `return CrsOutcome.Unchanged(geom)`) → `return CrsOutcome.NullOut`.
2. Source resolution: keep the embedded-SRID branch degrading to NULL (data), but make the explicit-`sourceCrs` branch RAISE on unresolvable. Restructure the `srcInfo` block so: embedded-SRID unresolvable → `NullOut`; explicit `sourceCrs` present-but-unresolvable → let `crsInfo` throw (remove the `Try(...).getOrElse(null)` for that branch only); neither present → `NullOut`. Replace the shared `if (srcInfo == null ...) return CrsOutcome.Unchanged(geom)` (line ~163) accordingly — data conditions → `NullOut`.
3. After `val gProj = ...transformWithCachedCT(...)` (line ~189), and BEFORE `CrsOutcome.Geom(gProj, dstInfo.authoritySrid)`, add:
```scala
        // Non-finite guard (heavy previously had none — mirror light's _has_nonfinite_xy):
        val coords = gProj.getCoordinates
        val nonFinite = coords.exists(c => c.x.isNaN || c.x.isInfinite || c.y.isNaN || c.y.isInfinite)
        if (nonFinite) return CrsOutcome.NullOut
        // Domain check: input coords in lon/lat vs target area_of_use (skip if absent).
        SpatialRefOps.areaOfUse(dstInfo.canonical) match {
            case Some(bbox) =>
                val lonLat: Array[(Double, Double)] =
                    if (srcInfo.canonical == "EPSG:4326") g.getCoordinates.map(c => (c.x, c.y))
                    else {
                        val toWgs = SpatialRefOps.transformPlan(srcInfo.canonical, "EPSG:4326")
                        val gWgs = if (toWgs.identity) g else transformWithCachedCT(g, toWgs.transformation)
                        gWgs.getCoordinates.map(c => (c.x, c.y))
                    }
                if (!SpatialRefOps.allInBBox(lonLat, bbox)) return CrsOutcome.NullOut
            case None => // no area_of_use -> skip
        }
        CrsOutcome.Geom(gProj, dstInfo.authoritySrid)
```
Keep the target-CRS `crsInfo` call raising on a bad target (line ~168, unchanged — that is the correct parameter→raise).

- [ ] **Step 5: Verify `ST_SetCrs` conforms; add regression tests**

`ST_SetCrs` already returns `CrsOutcome.NullOut` for null/unparseable geom (data) and raises `IllegalArgumentException` for authority-less CRS (parameter) — matches the contract. Add regression tests to `ST_CrsFamilyTest`:
```scala
test("setcrs: unparseable data returns NULL") {
    assert(ST_SetCrs.evalSql(Array[Byte](1,2,3), UTF8String.fromString("EPSG:4326")) == null)
}
test("setcrs: authority-less CRS raises") {
    assertThrows[IllegalArgumentException] {
        ST_SetCrs.evalSql(ewkb(4326), UTF8String.fromString("+proj=merc +datum=WGS84"))
    }
}
```
If either fails, fix `ST_SetCrs` — but current source should pass.

- [ ] **Step 6: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.ST_CrsFamilyTest' --log vx-heavy-transform.log`
Also run `ST_CrsCatalystTest` (the registered-SQL surface) to confirm no regression: `--suite 'com.databricks.labs.gbx.vectorx.ST_CrsCatalystTest'`.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_TransformCrs.scala \
        src/main/scala/com/databricks/labs/gbx/operations/SpatialRefOps.scala \
        src/test/scala/com/databricks/labs/gbx/vectorx/ST_CrsFamilyTest.scala
git commit -m "fix(vectorx): heavy transformcrs degrades bad data to NULL + domain check

Retire CrsOutcome.Unchanged for data conditions (unparseable geom,
unresolvable embedded SRID, source-less geom) -> NullOut. An
explicitly-provided unresolvable source_crs now raises (parameter).
Add a non-finite X/Y guard (heavy previously had none, unlike light)
and the GDAL GetAreaOfUse domain check (out-of-domain -> NULL, skipped
when the target has no area_of_use). ST_SetCrs already conformed.

Co-authored-by: Isaac"
```

---

### Task 4: Cross-tier parity tests (the spine)

Assert heavy and light produce the SAME NULL/raise/coordinates for the same degenerate input.

**Files:**
- Modify: `python/geobrix/test/pyvx/test_crs_parity.py` (JAR-gated heavy↔light parity harness; `importorskip("shapely"/"pyproj")`, `_GATE_CMD` present)

**Interfaces:**
- Consumes: Tasks 2 + 3 (both tiers' new behavior).

- [ ] **Step 1: Add the shared-corpus parity tests**

Following the file's existing `heavy`/`light` fixtures + `assert_geom_parity` helper, add parametrized parity tests over the corpus. For each input assert BOTH tiers agree on the OUTCOME CLASS (null / raise / value):
```python
def test_transformcrs_parity_unparseable_both_null(heavy, light):
    # both tiers -> NULL on garbage bytes
    assert light.transformcrs(b"NOT_WKB", "EPSG:3857") is None
    assert heavy.transformcrs(b"NOT_WKB", "EPSG:3857") is None

def test_transformcrs_parity_out_of_domain_both_null(heavy, light):
    g = ewkb_point(150.0, -80.0, 4326)
    assert light.transformcrs(g, "EPSG:27700") is None
    assert heavy.transformcrs(g, "EPSG:27700") is None

def test_transformcrs_parity_in_domain_coords_agree(heavy, light):
    g = ewkb_point(-0.13, 51.5, 4326)  # London -> 27700
    assert_geom_parity(light.transformcrs(g, "EPSG:27700"),
                       heavy.transformcrs(g, "EPSG:27700"), expect_srid=27700)

def test_transformcrs_parity_bad_target_both_raise(heavy, light):
    g = ewkb_point(-0.13, 51.5, 4326)
    with pytest.raises(Exception): light.transformcrs(g, "EPSG:99999")
    with pytest.raises(Exception): heavy.transformcrs(g, "EPSG:99999")

def test_transformcrs_parity_bad_source_arg_both_raise(heavy, light):
    g = wkb_point(1.0, 1.0)  # plain, no SRID
    with pytest.raises(Exception): light.transformcrs(g, "EPSG:3857", source_crs="EPSG:99999")
    with pytest.raises(Exception): heavy.transformcrs(g, "EPSG:3857", source_crs="EPSG:99999")

def test_transformcrs_parity_bounds_absent_not_nulled(heavy, light):
    # A target CRS with no area_of_use must NOT be NULL'd (conservative skip).
    # Use a raw-PROJ4 mercator target with no bounds; an in-range point survives.
    g = ewkb_point(0.0, 0.0, 4326)
    proj4 = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    assert light.transformcrs(g, proj4) is not None
    assert heavy.transformcrs(g, proj4) is not None
```
Match the harness's actual `heavy`/`light` invocation shape (it wraps `evalSql`/the pyvx callable — use the existing helpers in the file; the calls above are illustrative of the assertions, adapt to the fixture API). Reuse/define `ewkb_point`/`wkb_point` helpers consistent with the file.

- [ ] **Step 2: Run, verify PASS (both tiers agree)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_crs_parity.py --log vx-parity.log`
(This is JAR-gated — a staged JAR must be present; if it SKIPS for lack of JAR, report that and build/stage per repo norm rather than claiming pass on a skip.)

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyvx/test_crs_parity.py
git commit -m "test(pyvx): cross-tier parity for VectorX error-handling contract

Assert heavy and light agree on the degrade outcome for the shared
corpus: unparseable/out-of-domain -> both NULL; bad target / bad
source_crs arg -> both raise; in-domain coords agree; a target with no
area_of_use is not NULL'd on either tier (conservative skip).

Co-authored-by: Isaac"
```

---

### Task 5: Error Handling docs page + sidebar wiring

**Files:**
- Create: `docs/docs/api/error-handling.mdx`
- Modify: `docs/sidebars.js` (Functions category, after `api/coordinate-reference-systems`)

**Interfaces:**
- Consumes: nothing code-wise; documents the shipped RasterX + this VectorX behavior.

- [ ] **Step 1: Write the page**

Create `docs/docs/api/error-handling.mdx`. Lead with the organizing principle, then the per-surface expression. Structure:
- Frontmatter (`title: Error Handling`, an `id`/`sidebar_position` consistent with the sibling pages in `api/`).
- **Thesis:** "GeoBrix distinguishes a bad *parameter* from bad *data*. A bad or non-executable parameter (an invalid CRS code, a malformed argument) raises a clear error — it is a fix-your-code problem. Bad *data* flowing through a column (a corrupt geometry, an out-of-domain reprojection) degrades to NULL (or an empty result) rather than failing the whole job — one bad row never kills the stage."
- **RasterX** (already shipped): scalar accessors → NULL on a corrupt raster; tile ops → an empty tile carrying an `error_message` in metadata; aggregators skip a corrupt member; generators emit one error row. The heavyweight `spark.databricks.labs.gbx.expressions.crash.on.error` conf flips data errors to hard failures for debugging.
- **VectorX**: bad geometry data → NULL; a bad CRS argument → error; the reprojection domain check (a point outside the target CRS's valid area → NULL). Note VectorX returns geometries/strings with no metadata carrier, so NULL is its degrade signal.
- **How to catch degraded rows:** `WHERE geom IS NOT NULL`, or inspect `metadata.error_message` on a RasterX tile.
- User-facing voice — NO internal vocabulary (no "wave", no task/spec references). Run the leak check before commit: `grep -rniE "wave [0-9]+|last_error|CrsOutcome|safeEval" docs/docs/api/error-handling.mdx` — internal symbol names like `CrsOutcome`/`safeEval`/`last_error` (the internal key) must NOT appear; describe behavior, use the user-facing `error_message` metadata key for RasterX.

- [ ] **Step 2: Wire into the sidebar**

In `docs/sidebars.js`, in the "Functions" category `items` array (line ~103), add `'api/error-handling'` immediately after `'api/coordinate-reference-systems'` (line 106).

- [ ] **Step 3: Build the docs, verify clean**

Run: `cd docs && npm run build 2>&1 | tee /tmp/vx-docs-build.log; cd ..`
Then: `grep -iE "broken link|error-handling" /tmp/vx-docs-build.log` — expect the page builds, `docs/build/docs/api/error-handling/index.html` exists, and NO broken-link warning names error-handling.
Also run the internals-leak grep: `grep -rniE "wave [0-9]+|wave-[0-9]+" docs/docs/api/error-handling.mdx` → must print nothing.

- [ ] **Step 4: Commit**

```bash
git add docs/docs/api/error-handling.mdx docs/sidebars.js
git commit -m "docs: add Error Handling page (bad parameter -> error, bad data -> NULL)

New api/error-handling page explaining the whole-GeoBrix approach:
the parameter-vs-data axis, how RasterX (NULL accessors / empty tile
with error_message / skip-corrupt aggregators / one error row) and
VectorX (NULL for bad data, error for a bad CRS arg, the reprojection
domain check) each express it, and how to catch degraded rows. Wired
into the Functions sidebar after Coordinate Reference Systems.

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:**
- Contract (bad data → NULL, bad CRS arg → raise) → Tasks 2 (light) + 3 (heavy). ✓
- Garbage-bytes divergence retired (transformcrs no longer returns unchanged) → Task 2 Step 3, Task 3 Step 4, parity Task 4. ✓
- Domain/extent check (area_of_use, out-of-domain → NULL, skip-when-absent, straddling → NULL) → Task 1 (helper) + Task 2 (light wire) + Task 3 (heavy wire) + Task 4 (parity incl. bounds-absent). ✓
- Source-CRS data/param split → encoded verbatim in Global Constraints; implemented Task 2 Step 3 (light) + Task 3 Step 4 (heavy); parity Task 4. ✓
- Heavy non-finite guard (parity gap: heavy lacked one) → Task 3 Step 4. ✓
- `st_setcrs` conformance (data→NULL, authority-less→raise) → Task 2 Step 4 + Task 3 Step 5 (verify + regression; no change expected). ✓
- Cross-tier parity spine → Task 4. ✓
- Error Handling docs page + sidebar → Task 5. ✓

**FLAGGED for pre-flight confirmation (beyond the two named loose ends):** the **"plain geom, no resolvable source CRS → NULL"** ruling (Global Constraints table row 3). The spec ratified "return unchanged is retired" + "NULL is the uniform data signal," and this plan resolves the common plain-WKB-without-source case to NULL on that basis. This is the one behavior change that is a *derived* consequence rather than an explicitly-named spec item, and it is the most common misuse path (all-NULL column if a user forgets `source_crs`). Surface it to the human at the pre-flight scan: NULL (chosen, data-degrade-consistent) vs. raise (treat missing-source as a usage error). If the human prefers raise, it moves from the "data → NULL" column to "parameter → raise" in Tasks 2/3/4.

**Placeholder scan:** no TBD/TODO. Every code step has real before/after tied to verified file:line anchors + the `in_target_domain`/`areaOfUse`/`allInBBox` bodies. The parity-test fixture calls are marked "adapt to the existing harness API" (the harness's exact `heavy`/`light` wrapper shape is in the file) — that is a real instruction, not a placeholder, since the assertions are concrete. ✓

**Type consistency:** `in_target_domain(lonlat_coords, target_crs) -> Optional[bool]` (Task 1) consumed identically in Task 2. Heavy `areaOfUse -> Option[(Double,Double,Double,Double)]` + `allInBBox` consistent in Task 3. `CrsOutcome.NullOut` used for all heavy data-degrades. NULL / `None` / `null` degrade signal consistent across tiers and the parity task. ✓

**Ordering:** Task 1 (helper) precedes Task 2 (consumes it). Tasks 2 (light) and 3 (heavy) are independent tiers but run sequentially per subagent-driven-development. Task 4 (parity) depends on 2 + 3. Task 5 (docs) is independent. The pre-flight FLAG above should be resolved before Task 2 begins.
