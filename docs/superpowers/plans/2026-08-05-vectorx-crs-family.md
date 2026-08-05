# VectorX CRS Family — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship VectorX's CRS-string complement — `gbx_st_crs`, `gbx_st_setcrs`, `gbx_st_transformcrs` — both tiers, reusing a tier-neutral CRS resolver, with the geometry-honest semantics from the spec (string↔SRID bridges + a full-CRS reprojection workhorse), never duplicating the product's `st_srid`/`st_setsrid`/`st_transform`.

**Architecture:** First extract the CRS resolver to a tier-neutral module (light `databricks.labs.gbx.core.crs`, heavy a neutral `SpatialRefOps` home) with re-export shims so the shipped CRS-100 importers keep working. Then build the three functions on both tiers over the shared resolver + the existing `parse_geom` / JTS geometry I/O, driving output encoding through the spec's Q6 return matrix.

**Tech Stack:** Python 3.12 / rasterio / pyproj / shapely (light, `pyvx`); Scala 2.13 / JTS / GDAL-OSR (heavy, `vectorx`). Tests: pytest in `.venv-pyrx`; heavy in Docker via `gbx:test:scala`.

## Global Constraints

- **No product duplication:** ship ONLY `gbx_st_crs` / `gbx_st_setcrs` / `gbx_st_transformcrs`. Never `st_srid`/`st_setsrid`/`st_transform`.
- **Geometry stores only an int SRID** (EWKB/EWKT). `st_setcrs`/`st_crs` bridge string↔SRID (EPSG/ESRI focus); WKT/PROJ4 cannot be stored on a geometry.
- **`st_transformcrs` return matrix (spec §1.1, Q6):** output medium follows input text-vs-binary; carried SRID follows the TARGET — authority-coded target (`EPSG:n`/`ESRI:n`) carries int `n` and upgrades a plain geom to its E-form; authority-less target (WKT/PROJ4) yields the plain form with any stale SRID **cleared**.
- **Encoding preservation (`st_setcrs`):** `[E]WKT` in → EWKT out; `[E]WKB` in → EWKB out.
- **Never-error invariant:** absent/unresolvable-source degrades (transformcrs returns input unchanged when no source resolvable; st_crs → null); only an explicitly-unresolvable target string, or `st_setcrs` with an authority-less CRS, raises.
- **`st_transformcrs(geom, target_crs, source_crs=None)`** — optional source override for plain WKB/WKT (Q5-2).
- Input encodings: WKB/EWKB/WKT/EWKT interchangeable (shared `parse_geom`). Both tiers + binding parity. Serverless-safe (pyvx: udf/udtf only, no spark.conf/JVM).
- Cross-tier parity: identical output for authority-coded cases; CRS-equivalence for reprojected coords.

---

## Task 1: Tier-neutral light CRS resolver (`gbx.core.crs`) + shim

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/core/__init__.py`, `python/geobrix/src/databricks/labs/gbx/core/crs.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/crs.py` → re-export shim
- Test: `python/geobrix/test/core/test_crs_neutral.py` (create); existing `test_crs_resolve.py` / `test_crs_transformer_cache.py` are the regression gate.

**Interfaces:**
- Produces (consumed by Tasks 3, 4): `databricks.labs.gbx.core.crs` exporting `resolve_crs`, `crs_to_canonical`, `get_transformer`, `resolve_source_crs`, `_TRANSFORMER_CACHE_SIZE` — identical signatures to today's `pyrx.core.crs`.

- [ ] **Step 1: Write failing test** — `test_crs_neutral.py`: `from databricks.labs.gbx.core.crs import resolve_crs, crs_to_canonical, get_transformer, resolve_source_crs` all import; `resolve_crs(54008).to_authority() == ("ESRI","54008")`; `crs_to_canonical(resolve_crs(4326)) == "EPSG:4326"`. AND a shim test: `from databricks.labs.gbx.pyrx.core.crs import resolve_crs as shimmed; shimmed(4326) == resolve_crs(4326)` (same object identity for the cached code-sets is not required; equality is).

- [ ] **Step 2: Run — RED** (`gbx.core.crs` does not exist). Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/core/test_crs_neutral.py -v`

- [ ] **Step 3: Implement** — create `gbx/core/__init__.py` (empty package marker) and move the FULL body of `pyrx/core/crs.py` into `gbx/core/crs.py` verbatim (the resolver is pure rasterio/pyproj — no pyrx deps). Replace `pyrx/core/crs.py` contents with a shim:
```python
"""Back-compat shim: the CRS resolver now lives in the tier-neutral
``databricks.labs.gbx.core.crs`` so pyrx AND pyvx share one authority.
Re-export everything so existing ``pyrx.core.crs`` importers keep working."""
from databricks.labs.gbx.core.crs import (  # noqa: F401
    _TRANSFORMER_CACHE_SIZE,
    crs_to_canonical,
    get_transformer,
    resolve_crs,
    resolve_source_crs,
)
```
(Include any other public names the module had — verify with `grep '^def \|^_.*=' ` before deleting.)

- [ ] **Step 4: Run — GREEN** + regression: `pytest python/geobrix/test/pyrx/test_crs_resolve.py python/geobrix/test/pyrx/test_crs_transformer_cache.py python/geobrix/test/pyrx/test_crs_ops.py -q` (all pass through the shim, behavior-preserving). Smoke-import the 20 `pyrx.core.crs` importers: `python -c "import databricks.labs.gbx.pyrx.functions, databricks.labs.gbx.ds.vector, databricks.labs.gbx.pyrx.core.gridagg"`.

- [ ] **Step 5: Commit** — `refactor(core): extract CRS resolver to tier-neutral gbx.core.crs (+ pyrx shim)`

---

## Task 2: Tier-neutral heavy CRS resolver home + forwarder

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/operations/SpatialRefOps.scala` (neutral package `com.databricks.labs.gbx.operations`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/SpatialRefOps.scala` → forwarder (`val`/`def` delegations, or a package-level `type`/re-export)
- Test: extend `SpatialRefOpsTest` (Docker) — import via the neutral path.

**Interfaces:**
- Produces (consumed by Tasks 3, 4 heavy): `com.databricks.labs.gbx.operations.SpatialRefOps` with `resolveCrs`, `crsToCanonical`, `getTransformer`, `resolveSourceSR`, `getEPSGCode`, `fromEPSGCode` — identical to today's `rasterx.operations.SpatialRefOps`.

- [ ] **Step 1: Failing test** — `SpatialRefOpsTest` (Docker): `com.databricks.labs.gbx.operations.SpatialRefOps.resolveCrs("54008")` canonicalizes to `"ESRI:54008"`; the old `rasterx.operations.SpatialRefOps` path still resolves identically (forwarder).

- [ ] **Step 2: Run — RED** via `gbx:test:scala --suites '...SpatialRefOpsTest'`.

- [ ] **Step 3: Implement** — move the `object SpatialRefOps` body to the new `com.databricks.labs.gbx.operations` package. In `rasterx.operations`, replace with a forwarder object whose members delegate to the neutral one (`def resolveCrs(v: String) = operations.SpatialRefOps.resolveCrs(v)`, etc.), so the ~10 rasterx importers (RST_Clip/Sample/Viewshed/SetSrid/SetCrs/Transform/TransformCrs/GridReprojection/cellbbox/VectorRasterBridge) keep compiling unchanged. Prefer a Scala `export`/`val alias` if the version allows; else explicit delegations for the public methods.

- [ ] **Step 4: Run — GREEN** (Docker): the extended `SpatialRefOpsTest` + the full CRS-100 heavy suites (`RST_CrsOpsTest`, `PixelOpsTest`, grid tests) pass unchanged through the forwarder. scalastyle clean.

- [ ] **Step 5: Commit** — `refactor(operations): tier-neutral SpatialRefOps home (+ rasterx forwarder)`

---

## Task 3: Light VectorX CRS functions (`pyvx`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_crs.py` (compute core), extend `pyvx/functions.py` (UDFs + public fns + `_registrar_groups`)
- Test: `python/geobrix/test/pyvx/test_crs.py` (create)

**Interfaces:**
- Consumes: `gbx.core.crs` (Task 1); shared `parse_geom` / `geom_to_wkb`.
- Produces: `st_crs(geom)`, `st_setcrs(geom, crs)`, `st_transformcrs(geom, target_crs, source_crs=None)` + registered `gbx_st_crs` / `gbx_st_setcrs` / `gbx_st_transformcrs`.

- [ ] **Step 1: Write failing tests** — `test_crs.py`, using shapely:
```python
import shapely, pytest
from shapely.geometry import Point
from databricks.labs.gbx.pyvx import _crs

def _ewkb(srid): return shapely.to_wkb(shapely.set_srid(Point(11.0,42.0), srid), include_srid=True)

def test_st_crs_reads_embedded_srid():
    assert _crs.st_crs(_ewkb(4326)) == "EPSG:4326"
    assert _crs.st_crs(_ewkb(54008)) == "ESRI:54008"
    assert _crs.st_crs(shapely.to_wkb(Point(0,0))) is None          # plain WKB -> null
    assert _crs.st_crs("POINT (0 0)") is None                        # plain WKT -> null

def test_st_setcrs_stamps_srid_encoding_preserving():
    out = _crs.st_setcrs(shapely.to_wkb(Point(0,0)), "EPSG:32633")   # WKB in -> EWKB out
    assert isinstance(out, (bytes, bytearray)) and shapely.get_srid(shapely.from_wkb(out)) == 32633
    out2 = _crs.st_setcrs("POINT (0 0)", "ESRI:54008")               # WKT in -> EWKT out
    assert isinstance(out2, str) and out2.upper().startswith("SRID=54008;")
    with pytest.raises(ValueError):                                  # authority-less -> raise
        _crs.st_setcrs(shapely.to_wkb(Point(0,0)), "+proj=aea +lat_1=29.5 +datum=WGS84 +no_defs")

def test_st_transformcrs_matrix():
    # EWKB + EPSG target -> EWKB, SRID=target; coords reprojected
    out = _crs.st_transformcrs(_ewkb(4326), "EPSG:32633")
    g = shapely.from_wkb(out)
    assert isinstance(out,(bytes,bytearray)) and shapely.get_srid(g)==32633 and g.x>100000
    # EWKB + WKT target -> WKB (SRID cleared)
    wkt_tgt = shapely.from_epsg if False else 'PROJCS["Custom_TM",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["central_meridian",13.7],PARAMETER["scale_factor",0.9996],UNIT["metre",1]]'
    out2 = _crs.st_transformcrs(_ewkb(4326), wkt_tgt)
    assert isinstance(out2,(bytes,bytearray)) and shapely.get_srid(shapely.from_wkb(out2))==0
    # plain WKB, no source_crs -> returned unchanged (never-error)
    plain = shapely.to_wkb(Point(11.0,42.0))
    assert _crs.st_transformcrs(plain, "EPSG:32633") == plain
    # plain WKB + source_crs -> reprojected
    out3 = _crs.st_transformcrs(plain, "EPSG:32633", source_crs="EPSG:4326")
    assert shapely.from_wkb(out3).x > 100000
```
Cover WKT/EWKT input rows too (text medium preserved).

- [ ] **Step 2: Run — RED** (`pyvx._crs` missing). Run in `.venv-pyrx`.

- [ ] **Step 3: Implement** — `pyvx/_crs.py`:
  - An **encoding classifier**: `(is_text, has_srid)` from the raw input (bytes vs str; EWKB flag / `SRID=` prefix). Reuse shared `parse_geom` for the shapely geom.
  - `st_crs(geom)`: parse; `s = shapely.get_srid(g)`; `crs_to_canonical(resolve_crs(s)) if s>0 else None`.
  - `st_setcrs(geom, crs)`: `c = resolve_crs(crs)`; `auth = c.to_authority()` (returns `(name, code)` for EPSG **and** ESRI, else `None`); if `auth is None` → `raise ValueError("st_setcrs: cannot stamp an authority-less CRS (WKT/PROJ4) onto a geometry")`; else `code = int(auth[1])`, `set_srid(g, code)` and emit in the input medium (EWKB if binary via `to_wkb(include_srid=True)`; EWKT if text via `f"SRID={code};{to_wkt(g)}"`). NOTE: shapely `set_srid`/EWKB stores a bare int — a stamped ESRI code round-trips as the int `code`; `st_crs` re-resolves it to `ESRI:<code>` via the authoritative rule (an ESRI-range int is classified ESRI, not EPSG).
  - `st_transformcrs(geom, target_crs, source_crs=None)`: source = embedded SRID → `resolve_crs`; else `source_crs` → `resolve_crs`; else None → return input unchanged. target = `resolve_crs(target_crs)`; reproject coords via `get_transformer(source, target)` + `shapely.ops.transform`. Output per the Q6 matrix: `target.to_authority()` → if `(name, code)` (EPSG or ESRI) carry `int(code)` (E-form in the input medium); else `None` (authority-less WKT/PROJ4) → plain form (binary→WKB / text→WKT), SRID cleared.
  - Public `st_crs`/`st_setcrs`/`st_transformcrs` Column wrappers (pandas_udf `BinaryType` for binary-returning; a `StringType` variant is NOT needed — a UDF returns one type, so **return type must be decided**: see Step 3a).

- [ ] **Step 3a (design nuance — resolve + note):** a single SQL UDF has ONE return type, but `st_setcrs`/`st_transformcrs` return **bytes OR str** depending on input medium. RESOLVE: the registered SQL UDF returns **BINARY** and, for a text input, returns the EWKB/WKB equivalent (SQL callers work in WKB); the **Python `_crs.*` core** preserves the exact in/out medium (bytes→bytes, str→str) for the encoding-matrix tests + Python callers. Document this: SQL surface is WKB-normalized; the Python core is medium-preserving. (Mirror how other pyvx geom UDFs return BINARY.) Confirm this matches the spec's intent and note in the task report.

- [ ] **Step 4: Register** — add `crs` group to `_registrar_groups()`: `gbx_st_crs` (StringType udf), `gbx_st_setcrs` (BinaryType udf), `gbx_st_transformcrs` (BinaryType udf). Add to `registered_functions.txt` + `function-info.json` (via `gbx:docs:function-info`).

- [ ] **Step 5: Run — GREEN** + `pytest python/geobrix/test/pyvx/ -q`; flake8 clean.

- [ ] **Step 6: Commit** — `feat(pyvx): st_crs / st_setcrs / st_transformcrs (VectorX CRS family, light)`

---

## Task 4: Heavy VectorX CRS expressions (`vectorx`)

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/{ST_Crs,ST_SetCrs,ST_TransformCrs}.scala`
- Modify: `vectorx/functions.scala` (register + Column wrappers)
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/ST_CrsFamilyTest.scala` (create, Docker)

**Interfaces:** Consumes `com.databricks.labs.gbx.operations.SpatialRefOps` (Task 2) + `JTS` (`fromWKB`/`fromWKT`/`toWKB`/`toEWKB` + EWKT prefix helpers, all confirmed present). Registered names identical to light.

- [ ] **Step 1: Failing tests** — `ST_CrsFamilyTest` (Docker), direct `.eval`:
  - `ST_Crs.eval(ewkb(4326)) == "EPSG:4326"`; `ewkb(54008) → "ESRI:54008"`; plain WKB → null.
  - `ST_SetCrs.eval(wkb, "EPSG:32633")` → EWKB with SRID 32633; authority-less → throws.
  - `ST_TransformCrs.eval(ewkb(4326), "EPSG:32633")` → EWKB SRID 32633, coords reprojected; WKT target → WKB SRID 0; plain WKB no source → unchanged; plain WKB + source → reprojected. Parity with the light matrix.

- [ ] **Step 2: Run — RED** (Docker).

- [ ] **Step 3: Implement** — three `InvokedExpression` companions (pattern = `ST_LegacyAsWKB`): each `eval` takes the geom (+ crs string arg(s)), uses `JTS.fromWKB`/`fromWKT` (auto-detects SRID), `SpatialRefOps.resolveCrs` for the target/stamp CRS, an OSR `CoordinateTransformation` for `transformcrs` (reuse `OSRTransformGeometry`), and `JTS.toEWKB`/`toWKB` + EWKT-prefix compose for output per the Q6 matrix. `ST_SetCrs` raises on an authority-less CRS. Builders: `st_transformcrs` 2-arg + 3-arg (source_crs); `st_setcrs`/`st_crs` fixed arity. Delete/release every `SpatialReference` + `CoordinateTransformation` in try/finally (per [[gdal-ogr-register-via-guard]] + the CRS-100 leak lesson — release the source SR on the identity path too).

- [ ] **Step 4: Register** — `rd.register(ST_Crs/ST_SetCrs/ST_TransformCrs)` in `vectorx/functions.scala` + typed Column wrappers. Update `registered_functions.txt` if the light task didn't already (shared file).

- [ ] **Step 5: Run — GREEN** (Docker) + scalastyle clean.

- [ ] **Step 6: Commit** — `feat(vectorx): st_crs / st_setcrs / st_transformcrs (VectorX CRS family, heavy)`

---

## Task 5: Cross-tier parity + binding parity + docs

**Files:**
- Test: `python/geobrix/test/pyvx/test_crs_parity.py` (create; integration-marked, Docker JAR)
- Modify: `docs/tests-function-info/registered_functions.txt`, `function-info.json`; `docs/docs/api/vectorx-functions.mdx` (3 entries); `docs/docs/api/coordinate-reference-systems.mdx` (VectorX section + master-table rows); `docs/tests/python/api/vectorx_functions_sql.py` (SQL examples feeding function-info)

- [ ] **Step 1: Parity test** — for the authority-coded matrix cells, heavy `gbx_st_*` (via JAR) and light `pyvx` produce byte-identical EWKB / equal EWKT; reprojected coords equal within 1e-6; `st_crs` agrees. (Marker `integration`; runs with the JAR.)
- [ ] **Step 2: Binding parity** — `gbx:test:bindings` green: all three names present in `registered_functions.txt`, `function-info.json` (non-empty examples), Scala `override def name`, and the pyvx binding.
- [ ] **Step 3: Docs** — add the three functions to `vectorx-functions.mdx` with signatures + the return-matrix note for `st_transformcrs`; fill the deferred **VectorX rows** in the CRS-page master table (Group F table from CRS-100 T11) and update the "VectorX / GridX CRS surfaces" note; SQL examples in `vectorx_functions_sql.py`. Voice-grep clean; `gbx:docs:build` green (dev-server-aware).
- [ ] **Step 4: Commit** — `test+docs(vectorx): CRS-family cross-tier parity, bindings, CRS-page rows`

---

## Self-Review

**Spec coverage:** T1/T2 = tier-neutral resolver (spec §2.1, Q4-2, both tiers + shims). T3/T4 = the three functions light/heavy (spec §1.1–1.3, Q1/Q2/Q3, the Q6 matrix, Q5 source_crs). T5 = parity + binding parity + docs (incl. the deferred CRS-page master-table VectorX rows from CRS-100 T11). No-product-duplication + never-error carried in Global Constraints. ✓

**Placeholder scan:** the one genuine design nuance — a single UDF returns one type while the core is medium-preserving — is called out explicitly (Task 3 Step 3a) as a decision the implementer resolves + documents, not a hidden gap. Resolver bodies are moved verbatim (no re-transcription). Test code is concrete (real shapely, the verified authority-less WKT from the spec). ✓

**Type/name consistency:** `resolve_crs`/`crs_to_canonical`/`get_transformer`/`resolve_source_crs` (light) ↔ `resolveCrs`/`crsToCanonical`/`getTransformer`/`resolveSourceSR` (heavy) consistent T1↔T2↔T3↔T4. Registered names `gbx_st_crs`/`gbx_st_setcrs`/`gbx_st_transformcrs` identical across tiers + binding artifacts. ✓

**Open risks for the review loop:** (1) the T1/T2 resolver relocation touches shipped CRS-100 code — the shims/forwarders must keep ALL importers working (20 light, ~10 heavy); the regression gate is the existing CRS suites passing unchanged. (2) The bytes-or-str return-type nuance (T3 Step 3a) — get the SQL-BINARY vs Python-medium-preserving split coherent, or the matrix tests and SQL callers disagree. (3) EWKT has no native shapely writer — the `SRID=n;` compose/strip must round-trip exactly (parse_geom already strips; verify the writer). (4) heavy SpatialReference/CoordinateTransformation release on every path incl. identity (CRS-100 leak lesson).
