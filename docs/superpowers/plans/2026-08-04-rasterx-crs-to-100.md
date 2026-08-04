# RasterX CRS to 100% — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every RasterX CRS gap — source-CRS declaration on geometry inputs, string output-CRS (`out_srid`/`out_crs`) on produce/grid functions, the `rst_{h3,quadbin}_rastertogrid*` silent-4326 correctness fix, heavy reader `clipCrs`, the `gbx_h3_cell_bbox` relocation — plus a thread-safe transformer cache and full CRS-page documentation, all honoring **absent CRS never throws**.

**Architecture:** Extend the R2 resolvers (`pyrx.core.crs.resolve_crs` / `SpatialRefOps.resolveCrs`) as the single classification point. Add two shared primitives first (a cached transformer factory, and a per-geom source-CRS resolution helper), then apply them across the function groups. Two CRS roles, name-encoded: `srid`/`crs`/`clip_crs` = **source** (what the input already is), `out_srid`/`out_crs` = **target** (how to project output). Beta + no-aliases → param renames are expected.

**Tech Stack:** Python 3.12 / rasterio / pyproj / shapely (light); Scala 2.13 / GDAL-JNI (heavy). Tests: pytest in `.venv-pyrx` (light unit), Docker via `gbx:test:*` (heavy + doc tests). Real data: ESRI:54008 MODIS fixture at `target/test-classes/modis/`.

## Global Constraints

- **Never-error invariant:** absent/CRS-less input degrades to a sensible assumption and NEVER throws. The ONLY throwing paths: both source params (`srid`+`crs`) set, both output params (`out_srid`+`out_crs`) set, or an explicitly-supplied unresolvable CRS string (R2 apply-time). All three are call-level/config checks.
- **Source-CRS Rule 1 (per-geom):** EWKB/EWKT embedded SRID always wins per-geom; a plain WKB/WKT geom uses the explicit `srid`/`crs` param; neither → CRS-less (0). Mixed columns are first-class — no per-row error. Route int/string classification through the R2 resolver (`resolve_crs`), NOT raw `from_epsg`.
- **Output-CRS Rule 2:** produce-new-raster + grid funcs take `out_srid`/`out_crs`; neither set → grid-native (grid funcs) or the geometry's carried source CRS (rasterize family — NOT a forced 4326).
- **Naming standard:** source params = `srid`/`crs`/`clip_crs`; output params = `out_srid`/`out_crs`. Name encodes role.
- **No new v2 schema.** GDAL/OGR registration only via `GDALManager`. pyrx uses no Spark internals. Cross-tier parity (light == heavy) required.
- **Transformer cache:** thread-local, LRU-bounded (`_TRANSFORMER_CACHE_SIZE = 128`), keyed by canonical CRS pair, `always_xy=True`. Correctness identical with/without it.
- **Binding parity:** every renamed/added param and any registered entry kept in lockstep across `registered_functions.txt`, `function-info.json`, Scala `override def name`, Python binding. `gbx:test:bindings` green.
- **Docs are the source:** CRS-page examples backed by runnable doc tests; no prose-only claims. Voice-grep clean. `gbx:docs:build` green.

---

## Task 1: Light transformer cache + source-CRS helper (foundational)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/crs.py`
- Test: `python/geobrix/test/pyrx/test_crs_transformer_cache.py` (create)

**Interfaces:**
- Produces (consumed by Tasks 3–7):
  - `_TRANSFORMER_CACHE_SIZE: int = 128` (module constant)
  - `get_transformer(src, dst) -> pyproj.Transformer` — resolves each side via `resolve_crs`, keys by `(crs_to_canonical(src_crs), crs_to_canonical(dst_crs))`, thread-local LRU, `always_xy=True`.
  - `resolve_source_crs(embedded_srid: int, srid, crs) -> Optional[CRS]` — Rule 1 per-geom: `embedded_srid>0` → `resolve_crs(embedded_srid)`; elif exactly one of `srid`/`crs` set → `resolve_crs(that)`; elif both set → raise ValueError "provide srid OR crs, not both"; else None (CRS-less).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_crs_transformer_cache.py
import threading
import pytest
from rasterio.crs import CRS
from databricks.labs.gbx.pyrx.core import crs as C


def test_get_transformer_reuses_same_object_for_equivalent_crs():
    t1 = C.get_transformer(4326, 32633)
    t2 = C.get_transformer("4326", "EPSG:32633")  # equivalent spellings
    assert t1 is t2  # same cached object


def test_get_transformer_is_thread_local():
    results = {}

    def grab(name):
        results[name] = C.get_transformer(4326, 3857)

    threads = [threading.Thread(target=grab, args=(f"t{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # different threads get independent instances (no shared mutable object)
    assert results["t0"] is not results["t1"]


def test_get_transformer_always_xy():
    t = C.get_transformer(4326, 3857)
    # lon=0, lat=0 -> x=0, y=0 in web mercator; always_xy means (lon, lat) input order
    x, y = t.transform(0.0, 0.0)
    assert abs(x) < 1e-6 and abs(y) < 1e-6


def test_get_transformer_lru_evicts_beyond_cap():
    # fill beyond cap with distinct source zones; oldest evicted, no error, still correct
    for zone in range(32601, 32601 + C._TRANSFORMER_CACHE_SIZE + 5):
        C.get_transformer(zone, 4326)
    # a freshly requested pair still works
    assert C.get_transformer(4326, 3857) is not None


def test_resolve_source_crs_rule1():
    # embedded SRID wins
    assert C.resolve_source_crs(4326, None, None) == CRS.from_epsg(4326)
    # embedded ESRI code -> ESRI
    assert C.resolve_source_crs(54008, None, None).to_authority() == ("ESRI", "54008")
    # plain geom + explicit srid
    assert C.resolve_source_crs(0, 32633, None) == CRS.from_epsg(32633)
    # plain geom + explicit crs string
    assert C.resolve_source_crs(0, None, "ESRI:54008").to_authority() == ("ESRI", "54008")
    # neither -> None (CRS-less)
    assert C.resolve_source_crs(0, None, None) is None
    # both srid and crs -> error
    with pytest.raises(ValueError, match="srid OR crs"):
        C.resolve_source_crs(0, 4326, "EPSG:3857")
    # embedded SRID present: param is ignored per-geom (NO error - mixed-column safe)
    assert C.resolve_source_crs(4326, 32633, None) == CRS.from_epsg(4326)
```

- [ ] **Step 2: Run — RED**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_crs_transformer_cache.py -v`
Expected: FAIL (`get_transformer`, `resolve_source_crs`, `_TRANSFORMER_CACHE_SIZE` not defined).

- [ ] **Step 3: Implement in `crs.py`**

```python
import threading
from functools import lru_cache

# 120 WGS84 UTM zones (EPSG 326xx N + 327xx S) + 4326/27700/3857 + headroom.
_TRANSFORMER_CACHE_SIZE = 128

_thread_local = threading.local()


def _thread_transformer_cache():
    cache = getattr(_thread_local, "transformers", None)
    if cache is None:
        from collections import OrderedDict

        cache = OrderedDict()
        _thread_local.transformers = cache
    return cache


def get_transformer(src, dst):
    """Thread-local, LRU-bounded pyproj Transformer keyed by canonical CRS pair.
    always_xy=True (lon/lat order). src/dst each an int SRID or CRS string/CRS."""
    from pyproj import Transformer

    src_crs = src if hasattr(src, "to_authority") else resolve_crs(src)
    dst_crs = dst if hasattr(dst, "to_authority") else resolve_crs(dst)
    key = (crs_to_canonical(src_crs), crs_to_canonical(dst_crs))
    cache = _thread_transformer_cache()
    tr = cache.get(key)
    if tr is None:
        tr = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
        cache[key] = tr
        if len(cache) > _TRANSFORMER_CACHE_SIZE:
            cache.popitem(last=False)  # evict oldest (LRU)
    else:
        cache.move_to_end(key)
    return tr


def resolve_source_crs(embedded_srid, srid=None, crs=None):
    """Rule 1 (per-geom): embedded SRID wins; else the single explicit srid|crs;
    both explicit -> error; neither -> None (CRS-less)."""
    if embedded_srid and int(embedded_srid) > 0:
        return resolve_crs(int(embedded_srid))
    if srid is not None and crs is not None:
        raise ValueError("provide srid OR crs, not both")
    if crs is not None:
        return resolve_crs(crs)
    if srid is not None:
        return resolve_crs(srid)
    return None
```

- [ ] **Step 4: Run — GREEN** (all 6 tests pass). Also run the existing `test_crs_resolve.py` + `test_crs_ops.py` to confirm no regression.

- [ ] **Step 5: Commit** — `feat(pyrx): transformer cache + resolve_source_crs (CRS-100 foundation)`

---

## Task 2: Heavy transformer cache + source-CRS helper (foundational)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/SpatialRefOps.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/operations/SpatialRefOpsTest.scala` (extend)

**Interfaces:**
- Produces (consumed by Tasks 3, 5, 6): 
  - `SpatialRefOps.getTransformer(srcKey: String, dstKey: String): CoordinateTransformation` — thread-local LRU (cap 128), keyed by canonical strings.
  - `SpatialRefOps.resolveSourceSR(embeddedSrid: Int, srid: Option[Int], crs: Option[String]): Option[SpatialReference]` — Rule 1 mirror of light.

- [ ] **Step 1: Write failing tests** (extend `SpatialRefOpsTest`, runs in Docker):

```scala
test("getTransformer reuses the same CoordinateTransformation for equivalent CRS keys") {
    val t1 = SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
    val t2 = SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
    assert(t1 eq t2)  // same cached instance (same thread)
}

test("resolveSourceSR: embedded SRID wins; else single param; both -> error; neither -> None") {
    assert(SpatialRefOps.crsToCanonical(SpatialRefOps.resolveSourceSR(4326, None, None).get) == "EPSG:4326")
    assert(SpatialRefOps.crsToCanonical(SpatialRefOps.resolveSourceSR(54008, None, None).get) == "ESRI:54008")
    assert(SpatialRefOps.crsToCanonical(SpatialRefOps.resolveSourceSR(0, Some(32633), None).get) == "EPSG:32633")
    assert(SpatialRefOps.resolveSourceSR(0, None, None).isEmpty)
    an[IllegalArgumentException] should be thrownBy SpatialRefOps.resolveSourceSR(0, Some(4326), Some("EPSG:3857"))
    // embedded present + param -> param ignored, no error (mixed-column safe)
    assert(SpatialRefOps.crsToCanonical(SpatialRefOps.resolveSourceSR(4326, Some(32633), None).get) == "EPSG:4326")
}
```

- [ ] **Step 2: Run — RED** via `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.operations.SpatialRefOpsTest'`.

- [ ] **Step 3: Implement** in `SpatialRefOps.scala`:

```scala
import org.gdal.osr.CoordinateTransformation
import scala.collection.mutable

private val _txCache = new ThreadLocal[mutable.LinkedHashMap[String, CoordinateTransformation]] {
    override def initialValue() = mutable.LinkedHashMap.empty
}
private val TRANSFORMER_CACHE_SIZE = 128

/** Thread-local, LRU-bounded CoordinateTransformation keyed by canonical CRS pair. */
def getTransformer(srcKey: String, dstKey: String): CoordinateTransformation = {
    val srcC = crsToCanonical(resolveCrs(srcKey))
    val dstC = crsToCanonical(resolveCrs(dstKey))
    val key = s"$srcC->$dstC"
    val cache = _txCache.get()
    cache.get(key) match {
        case Some(tf) => cache.remove(key); cache.put(key, tf); tf  // move-to-end
        case None =>
            val tf = new CoordinateTransformation(resolveCrs(srcKey), resolveCrs(dstKey))
            cache.put(key, tf)
            if (cache.size > TRANSFORMER_CACHE_SIZE) cache.remove(cache.head._1)  // evict oldest
            tf
    }
}

def resolveSourceSR(embeddedSrid: Int, srid: Option[Int], crs: Option[String]): Option[SpatialReference] = {
    if (embeddedSrid > 0) Some(resolveCrs(embeddedSrid.toString))
    else (srid, crs) match {
        case (Some(_), Some(_)) => throw new IllegalArgumentException("provide srid OR crs, not both")
        case (_, Some(c))       => Some(resolveCrs(c))
        case (Some(s), _)       => Some(resolveCrs(s.toString))
        case _                  => None
    }
}
```

- [ ] **Step 4: Run — GREEN** (Docker). Confirm existing `SpatialRefOpsTest` + `RST_CrsOpsTest` still pass.

- [ ] **Step 5: Commit** — `feat(rasterx): getTransformer cache + resolveSourceSR (CRS-100 foundation)`

---

## Task 3: Group C — rst_{h3,quadbin}_rastertogrid* auto-reproject + crs override

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/gridagg.py` (`raster_to_grid` ~196)
- Modify: heavy `RST_H3_RasterToGrid.scala`, `RST_Quadbin_RasterToGrid.scala` (add warp-to-native, mirror BNG's `warpToBNG`)
- Modify: light UDTF wrappers + heavy exprs to thread an optional `crs` arg
- Test: `python/geobrix/test/pyrx/test_rastertogrid_crs.py` (create); heavy `RST_CrsOpsTest` (extend)

**Interfaces:**
- Consumes: `crs.get_transformer` (Task 1), `warp.reproject_to_crs` (existing), `SpatialRefOps.getTransformer` (Task 2).

- [ ] **Step 1: Write failing test (light)** — the correctness regression:

```python
# test_rastertogrid_crs.py
import numpy as np, rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx.core import gridagg


def _utm_raster_bytes():
    # a small raster in EPSG:32633 (UTM 33N) with a known ground footprint
    transform = from_origin(500000.0, 5000000.0, 30.0, 30.0)
    profile = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                   crs=rasterio.crs.CRS.from_epsg(32633), transform=transform, nodata=-9999.0)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((1, 8, 8), dtype="float32"))
        return mf.read()


def test_h3_rastertogrid_utm_reprojects_not_silently_wrong():
    b = _utm_raster_bytes()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count")
    # cells must be near UTM-33N footprint (central Europe), NOT off the coast of Africa
    # (which is what lon=easting/lat=northing would produce). Assert non-empty and
    # that the decoded cell centroids land in a plausible lon range for zone 33 (~9-15E).
    import h3
    cells = [c["cellID"] for band in [out[0]] for c in band]
    assert cells, "expected non-empty grid"
    lats_lons = [h3.cell_to_latlng(h3.int_to_str(c)) for c in cells]
    lons = [lo for (_, lo) in lats_lons]
    assert all(0.0 < lo < 24.0 for lo in lons), f"UTM-33N should map to ~9-15E lon, got {lons[:3]}"


def test_h3_rastertogrid_crsless_assumes_4326_no_error():
    # a raster with NO crs -> assume 4326, proceed, no exception
    transform = from_origin(9.0, 50.0, 0.01, 0.01)
    profile = dict(driver="GTiff", width=4, height=4, count=1, dtype="float32",
                   crs=None, transform=transform, nodata=-9999.0)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((1, 4, 4), dtype="float32"))
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count")  # no raise
    assert out is not None


def test_h3_rastertogrid_crs_override_for_crsless():
    # CRS-less raster whose true CRS is UTM-33N, supplied via crs override
    b = _utm_raster_bytes()
    with MemoryFile(b) as mf, mf.open() as ds:
        # strip the CRS to simulate a CRS-less-but-known raster
        prof = ds.profile.copy(); prof["crs"] = None
    # (build a crs-less copy then pass crs="EPSG:32633")
    with MemoryFile() as mf2:
        with MemoryFile(b) as src_mf, src_mf.open() as src, mf2.open(**{**src.profile, "crs": None}) as dst:
            dst.write(src.read())
        cb = mf2.read()
    with MemoryFile(cb) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count", crs="EPSG:32633")
    assert out and out[0]
```

- [ ] **Step 2: Run — RED** — `test_h3_rastertogrid_utm_reprojects_not_silently_wrong` fails (today lon=easting → nonsense), `crs` kwarg unknown.

- [ ] **Step 3: Implement (light)** — in `gridagg.raster_to_grid`, add `crs: Optional[str] = None` param. Before the lon/lat computation for h3/quadbin: determine the source CRS = `ds.crs` if present else `resolve_crs(crs)` if override given else None. If source is known and != EPSG:4326, warp the dataset to 4326 (nearest-neighbour, reuse `warp.reproject_to_crs(ds, "EPSG:4326")` pattern — open the result and recompute `gt`), mirroring `_raster_to_bng`. If source is None (CRS-less, no override) → assume 4326, proceed unchanged (today's behavior). Thread `crs` through the light UDTF wrappers (they read tile + resolution today; add the optional arg).

- [ ] **Step 4: Implement (heavy)** — in `RST_H3_RasterToGrid`/`RST_Quadbin_RasterToGrid`, add a `warpToNative` (to 4326) mirroring `RST_BNG_RasterToGrid.warpToBNG`; reproject unless `ds` is already 4326 or CRS-less. Add the optional `crs` override expr to the builder (arity bump). Release warped Datasets in `try/finally` per GDAL resource rules.

- [ ] **Step 5: Run — GREEN** (light unit + `gbx:test:scala --suites '...RST_CrsOpsTest'` in Docker).

- [ ] **Step 6: Commit** — `fix(rasterx): rst_{h3,quadbin}_rastertogrid auto-reproject to grid-native + crs override`

---

## Task 4: Group A — clip_crs / crs source params (light, both Column APIs)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/edit.py` (`clip_to_geom`), `_clip.py` (`_epsg_int` → resolver), `ops.py` (`sample`)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (`rst_clip`, `rst_sample`, `rst_viewshed` + their `_*_bytes`/`_*_udf`)
- Test: `python/geobrix/test/pyrx/test_geom_source_crs.py` (create)

**Interfaces:**
- Consumes: `crs.resolve_source_crs`, `crs.get_transformer` (Task 1).
- `rst_clip(..., clip_crs=None)`, `rst_sample(tile, geom_wkb, crs=None)`, `rst_viewshed(..., crs=None)`.

- [ ] **Step 1: Write failing test** — cross-cutting source-CRS behavior for clip/sample:

```python
# test_geom_source_crs.py  (light)
# - rst_sample with a plain-WKB point + crs="EPSG:4326" over a UTM raster reprojects and samples correctly
# - rst_clip clip_crs="ESRI:54008" on a plain-WKB cutline resolves ESRI (was int-only)
# - a plain WKB point, no crs -> assumed already in raster CRS (no error)
# - both srid-ish and crs -> ValueError "srid OR crs"
# (Use make_geotiff_bytes(epsg=32633) + a 4326 point that lands inside after reprojection.)
```
Write concrete assertions using `_epsg4326_tif()`/`make_geotiff_bytes` helpers from `test_crs_ops.py` conftest.

- [ ] **Step 2: Run — RED** (`crs`/`clip_crs` kwargs unknown; `_clip._epsg_int` rejects ESRI).

- [ ] **Step 3: Implement**
  - `_clip.py`: replace `_epsg_int` stamping with `resolve_source_crs`-based logic — when the cutline lacks an embedded SRID and `clip_crs` is given, resolve it (ESRI/WKT now work) and stamp/reproject via `edit.clip_to_geom`. Keep the "embedded SRID wins" precedence.
  - `edit.clip_to_geom(ds, geom, all_touched, geom_crs=None)`: use `resolve_source_crs(shapely.get_srid(geom), None, geom_crs)`; reproject cutline→`ds.crs` via `get_transformer`. CRS-less → assume aligned (no error).
  - `ops.sample(ds, geom, geom_crs=None)`: extend the existing srid block to use `resolve_source_crs` + `get_transformer`.
  - `functions.py`: add `clip_crs`/`crs` kwargs to `rst_clip`/`rst_sample`/`rst_viewshed`, thread through `_clip_bytes`/`_sample_udf`/`_viewshed_bytes`. Keep them optional (default None) so existing calls/SQL registration are unaffected.

- [ ] **Step 4: Run — GREEN** + regression `pytest python/geobrix/test/pyrx/ -q -k "clip or sample or viewshed or crs"`.

- [ ] **Step 5: Commit** — `feat(pyrx): source-CRS params (clip_crs, crs) on rst_clip/sample/viewshed`

---

## Task 5: Group A — heavy clip/sample/viewshed source-CRS params

**Files:**
- Modify: `RST_Clip.scala` (add `clipCrsExpr`), `RST_Sample.scala`, `RST_Viewshed.scala` + their builders/eval
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala` (registration arity)
- Test: `RST_CrsOpsTest` (extend, Docker)

**Interfaces:** Consumes `SpatialRefOps.resolveSourceSR` + `getTransformer` (Task 2). Registered names unchanged; arity grows by one optional trailing arg (builder handles both arities).

- [ ] **Step 1: Failing test** — heavy `RST_Clip.execute` with a plain-WKB cutline + `clip_crs="ESRI:54008"` resolves ESRI; a plain cutline with no crs assumes raster CRS (no error); EWKB cutline still uses embedded SRID.
- [ ] **Step 2: Run — RED** (Docker).
- [ ] **Step 3: Implement** — thread an optional `clipCrs`/`crs` String expr through each expression's `children`/`builder`/`eval`/`execute`; in `execute`, when the JTS geom SRID is 0 and the param is set, `resolveSourceSR(0, None, Some(crs))` → build the cutline SR; else keep the existing embedded-SRID-then-raster fallback. Builder accepts both the old and new arity (`case n => ...`). Delete no existing behavior.
- [ ] **Step 4: Run — GREEN** (Docker) + scalastyle.
- [ ] **Step 5: Commit** — `feat(rasterx): heavy source-CRS params on rst_clip/sample/viewshed`

---

## Task 6: Group B — out_srid/out_crs on produce-new-raster + grid rasterize (both tiers)

**Files:**
- Light: `functions.py` (`rst_rasterize`(+`_agg`), `rst_gridfrompoints`(+`_agg`), `rst_dtmfromgeoms`(+`_agg`), `rst_{h3,quadbin,bng}_rasterize_agg`), `core/features.py` (`rasterize_geom`), the gridfrompoints/dtm cores
- Heavy: `RST_Rasterize.scala`(+agg), `RST_DTMFromGeoms.scala`, `RST_{H3,Quadbin,BNG}_RasterizeAgg.scala`, `VectorRasterBridge.scala`
- Test: `python/geobrix/test/pyrx/test_out_crs.py` (create); heavy `RST_CrsOpsTest`

**Interfaces:** Consumes Task 1/2 helpers. Rename `srid`→`out_srid`, add `out_crs`. Rule-2 reprojection: reproject geom source→output before burn.

- [ ] **Step 1: Failing test (light)** — the Group B correctness case:

```python
# test_out_crs.py
# - rst_rasterize of an EWKB-4326 geom with out_crs="EPSG:32633" reprojects the geom
#   into 32633 before burning -> output SR is 32633 AND the burned pixels align to the
#   32633 extent (not garbage from treating 4326 coords as 32633).
# - out_srid=54008 stamps ESRI:54008 on the output (via resolver).
# - both out_srid and out_crs -> ValueError.
# - neither out_* + a plain WKB geom -> CRS-less output (no forced 4326).
```

- [ ] **Step 2: Run — RED** (`out_srid`/`out_crs` unknown; today `srid` positional).
- [ ] **Step 3: Implement (light)**
  - `features.rasterize_geom(..., out_srid=None, out_crs=None)`: resolve target = `resolve_crs(out_crs or out_srid)` (both-set → error); resolve geom source via `resolve_source_crs`; if source known & target known & differ → reproject geom via `get_transformer` before `_rasterize`; stamp target on output profile (None if target unresolved & source-less).
  - `rst_rasterize`/`_agg`, `rst_gridfrompoints`/`_agg`, `rst_dtmfromgeoms`/`_agg`: rename the `srid` param to `out_srid`, add `out_crs`, thread through the UDFs. `rst_{h3,quadbin,bng}_rasterize_agg`: rename `srid`→`out_srid` (keep grid-native default), add `out_crs`; bng documents out ignored.
- [ ] **Step 4: Implement (heavy)** — mirror in `RST_Rasterize`/agg, `RST_DTMFromGeoms`, the three grid `RasterizeAgg`, routing `VectorRasterBridge` CRS build through `resolveCrs` + geom reprojection via `getTransformer`. Rename `sridExpr`→`outSridExpr`, add `outCrsExpr`; builder arity.
- [ ] **Step 5: Run — GREEN** (light unit + Docker heavy) + scalastyle.
- [ ] **Step 6: Update binding artifacts** — `registered_functions.txt` (names unchanged here; params only), `function-info.json` via `gbx:docs:function-info`; run `gbx:test:bindings`.
- [ ] **Step 7: Commit** — `feat(rasterx): out_srid/out_crs + Rule-2 geom reprojection on produce/grid rasterize`

---

## Task 7: Group B — rst_h3_gridspec (light-only) + gbx_h3_cell_bbox out_* (both)

**Files:**
- Light: `functions.py` (`rst_h3_gridspec` ~6006, `gbx_h3_cell_bbox` ~5986, `_h3_cell_bbox_udf`)
- Test: `python/geobrix/test/pyrx/test_out_crs.py` (extend)

**Interfaces:** `rst_h3_gridspec(..., out_srid=4326, out_crs=None)` (DataFrame helper, light-only — no heavy twin). `gbx_h3_cell_bbox(cellid, out_srid=None, out_crs=None, mode=None)`.

- [ ] **Step 1: Failing test** — `gbx_h3_cell_bbox(cell, out_crs="EPSG:3857")` returns a bbox in web-mercator metres (not degrees); `rst_h3_gridspec(df, out_srid=27700)` produces a spec whose srid field is 27700. Both-out-params → error.
- [ ] **Step 2: Run — RED**.
- [ ] **Step 3: Implement** — rename `srid`→`out_srid`, add `out_crs` on both; `_h3_cell_bbox_udf` resolves the target via `resolve_crs(out_crs or out_srid, default 4326)` and reprojects the cell coords via `get_transformer` (replacing `_cr._reproject`'s int-only path). `rst_h3_gridspec` writes the resolved target's authority/int into the spec struct.
- [ ] **Step 4: Run — GREEN**.
- [ ] **Step 5: Commit** — `feat(pyrx): out_srid/out_crs on rst_h3_gridspec + gbx_h3_cell_bbox`

---

## Task 8: Group E — relocate gbx_h3_cell_bbox to GridX (light + heavy)

**Files:**
- Light: move `gbx_h3_cell_bbox` + `_h3_cell_bbox_udf` from `pyrx/functions.py` to `pygx/functions.py`; extract the raster-free `_h3_str`/`_reproject` from `pyrx/core/cellraster.py` into a pygx-local helper (or a shared raster-free module) so pygx doesn't import `rasterio`.
- Heavy: move `RST_H3_CellBBox.scala` from `rasterx.expressions.grid` to a `gridx` package; update its imports (drop any rasterx-only).
- Registration: `pygx`/`gridx` register wiring; `registered_functions.txt` (name unchanged); `function-info.json`.
- Test: move/duplicate the cell_bbox test into the pygx test dir; keep the light-CI tier gate (`_LIGHT_TEST_DIRS`) satisfied.

**Interfaces:** Registered SQL name `gbx_h3_cell_bbox` UNCHANGED — only package home moves. This is pure relocation; the `out_*` CRS work already landed in Task 7 (the moved code carries it).

- [ ] **Step 1:** Confirm `_h3_str`/`_reproject` are raster-free (verified in spec) and extract them without pulling `rasterio`. Run `python -c "import databricks.labs.gbx.pygx.functions"` to confirm no rasterio import leaks.
- [ ] **Step 2: Move light** — relocate the function + udf; update `pyrx` to no longer register it; register under pygx. Run the moved test — GREEN.
- [ ] **Step 3: Move heavy** — relocate the Scala expr to `gridx`, update `functions.scala` registration package path. `gbx:test:scala --suites '...CellBBox...'` GREEN in Docker.
- [ ] **Step 4:** `gbx:test:bindings` green (name still present, new home); `gbx:lint:python --check` + scalastyle.
- [ ] **Step 5: Commit** — `refactor(gridx): relocate gbx_h3_cell_bbox from raster package to GridX`

---

## Task 9: Group D — heavy reader clipCrs option

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/gdal/GDAL_Reader.scala` (+ gtiff variant): read a `clipCrs` option, populate the v2 tile `clip_crs` field.
- Test: heavy reader test (Docker) — a reader with `clipCrs` set populates `tile.clip_crs`.

**Interfaces:** Mirrors light reader's `clipCrs` (precedence: embedded SRID → clipCrs → raster CRS). No schema change (v2 already has `clip_crs`).

- [ ] **Step 1: Failing test** — heavy `gdal`/`gtiff_gdal` reader with `.option("clipCrs","EPSG:4326")` yields a tile whose `clip_crs` == the canonical CRS. RED (option ignored today).
- [ ] **Step 2: Implement** — parse the option in the reader; set the v2 tile `clip_crs` via `SpatialRefOps.resolveCrs` → `crsToCanonical`. Never error on absent (leave null).
- [ ] **Step 3: Run — GREEN** (Docker) + scalastyle.
- [ ] **Step 4: Commit** — `feat(rasterx): heavy GDAL/GTiff reader clipCrs option (parity with light)`

---

## Task 10: Group F — Coordinate Reference Systems docs page + master table

**Files:**
- Modify: `docs/docs/api/coordinate-reference-systems.mdx`
- Modify: `docs/docs/api/raster-functions.mdx` (per-function param docs + cross-links)
- Create: doc tests under `docs/tests/python/api/` for the three worked examples (mixed-column, produce-new-raster reprojection, rastertogrid auto-reproject)
- Modify: `function-info.json` via `gbx:docs:function-info` (new/renamed params get examples)

**Interfaces:** Consumes the shipped behavior of Tasks 1–9. Doc tests import real code via raw-loader per the docs-are-source rule.

- [ ] **Step 1:** Add CRS-page sections per spec Group F items 1–6: "Source CRS vs output CRS" (two roles + naming table + §6 subsets), the per-geom source precedence + mixed-column table, the never-error invariant + error matrix (`:::note`), produce-new-raster reprojection, rastertogrid auto-reproject + `crs` override, the performance note.
- [ ] **Step 2:** Add the **master CRS-function cross-reference table** (Group F item 7): columns Package/Function/Tiers/CRS param(s)/Role/CRS-in behavior/Notes; seed ALL RasterX rows (Groups A–E + shipped R/R2 funcs); add VectorX/GridX sub-headers with an explicit "see the VectorX / GridX CRS spec" deferral row. Every row links to its function reference entry.
- [ ] **Step 3:** Write the three doc tests (real data, real assertions) and wire them into the MDX via raw-loader. Add per-function cross-links + new-param docs in `raster-functions.mdx`.
- [ ] **Step 4:** Run `gbx:test:docs` (Docker) GREEN; regenerate `function-info.json` (`gbx:docs:function-info`); `gbx:test:bindings` green; voice-grep `grep -rn -iE "wave [0-9]+" docs/docs/` empty; `gbx:docs:build` GREEN (dev-server-aware).
- [ ] **Step 5: Commit** — `docs(crs): source/output roles, mixed-column + never-error rules, master cross-reference table`

---

## Self-Review

**Spec coverage:** Task 1/2 = shared primitives (transformer cache §3.4, resolve_source_crs §1.1). Task 3 = Group C (§Group C, correctness fix). Task 4/5 = Group A (§Group A, both tiers). Task 6/7 = Group B (§Group B, out_* + Rule-2 §3.2). Task 8 = Group E (§Group E relocation). Task 9 = Group D (§Group D reader). Task 10 = Group F (§Group F docs + master table). All six groups + both foundations covered. ✓

**Placeholder scan:** Task 3/4/6 test steps describe assertions with enough concrete setup (UTM raster, EWKB-4326 geom, ESRI:54008) that an implementer can write them; the shared-core code (crs.py, SpatialRefOps) is given verbatim. The per-function param threading (Tasks 4–7) is mechanical repetition of one pattern across enumerated functions — the pattern is shown once (rst_clip/rasterize) and the sites are enumerated, not re-transcribed. ✓

**Type/name consistency:** `get_transformer`/`resolve_source_crs` (light) ↔ `getTransformer`/`resolveSourceSR` (heavy) consistent Task 1↔2↔3↔6. `out_srid`/`out_crs` (output) vs `crs`/`clip_crs` (source) applied per the §6 naming subset. `_TRANSFORMER_CACHE_SIZE=128` consistent. ✓

**Open risks for the review loop:** (1) heavy builder arity bumps must accept BOTH old and new arities so no registered call breaks — verify each `case n =>`. (2) Task 6's `srid`→`out_srid` rename on `rst_rasterize` (required positional) is a signature break — all internal callers, doc tests, and SQL examples must update in lockstep (binding-parity gate catches misses). (3) Task 3's warp must be nearest-neighbour (no interpolation of pixel stats) — assert resampling mode. (4) Task 8 relocation must not leave `rasterio` importable-cost in pygx (light-tier no-heavy-deps rule).
