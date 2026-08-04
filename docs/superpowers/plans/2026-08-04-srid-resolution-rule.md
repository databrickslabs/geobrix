# SRID Resolution Rule (epsg→esri) — Implementation Plan (Spec R2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One authoritative rule for classifying an integer SRID as a CRS — `n` in the PROJ EPSG code set → `EPSG:<n>`, else in the ESRI set → `ESRI:<n>`, else raise (at apply time) — applied consistently in both tiers, documented on the CRS page. Plus: relax `rst_setsrid` to `>= 0` (dumb storage).

**Architecture:** Light `pyrx.core.crs.resolve_crs` classifies the int case via cached PROJ code sets (`pyproj.database.get_codes`) instead of the lenient `from_epsg`. Heavy GDAL already labels ESRI codes correctly (verified) — heavy is a verify + parity test, changing only if a mislabel is found. `rst_srid`/set-srid become dumb non-negative storage; the epsg→esri classification + the "unresolvable code" raise live only at the apply moment (`resolve_crs`/`resolveCrs`). Docs get the canonical rule section + per-function links.

**Tech Stack:** Python 3.12 / pyproj / rasterio (light), Scala/GDAL-JNI (heavy). Tests: pytest (`.venv-pyrx`); heavy Scala in Docker.

## Global Constraints

- **Classify via authoritative PROJ code sets, NOT the lenient constructor.** `CRS.from_epsg(54008)` succeeds and mislabels 54008 as EPSG; membership from `pyproj.database.get_codes("EPSG"/"ESRI", ...)` (disjoint: 4326=EPSG, 54008/102008=ESRI, 99999999=neither) is the source of truth (PROJ's `proj.db`). Build the sets once, cache (`lru_cache`).
- **The rule (apply-time only):** `n` in EPSG set → `CRS.from_epsg(n)`; elif in ESRI set → `CRS.from_authority("ESRI", n)` (labels ESRI correctly); else `raise ValueError`. Lives in `resolve_crs` (light) / `resolveCrs` (heavy).
- **SRID is dumb storage, `srid >= 0`.** set/get never classify, never raise except the **negative guard** (`>= 0`). Any non-negative int stores (incl. `0`=no-CRS, ESRI codes, currently-unresolvable). `rst_srid` returns the stored int or NULL. Relax R's `rst_setsrid` `> 0` → `>= 0` (light `edit.set_srid:208`, heavy `RST_SetSrid.scala:77`).
- **rst_srid unchanged in return semantics** (int/null); it does NOT resolve. rst_crs is the string companion (unchanged).
- **Propagate Q1→1:** route apply-sites that turn a data/caller int into a CRS through the resolver; do NOT churn EPSG-only internals (grid 4326/27700 pins, `fromEPSGCode` WGS84 default).
- **No new registered function names.** No v2 schema change. pyrx no spark internals. GDAL reg via GDALManager only.
- **Reader leniency preserved:** vector reader `geom_0_srid="0"`/absent stays "no CRS" (null), never raises.
- **Tests execute real code** (real pyproj/GDAL, the ESRI:54008 fixture). No mocking the CRS libs.

---

## Task 1: Light `resolve_crs` authoritative epsg/esri classification

**Files:** Modify `python/geobrix/src/databricks/labs/gbx/pyrx/core/crs.py`; Test `python/geobrix/test/pyrx/test_crs_resolve.py` (extend).

- [ ] **Step 1: Write failing tests** — extend `test_crs_resolve.py`:

```python
def test_resolve_int_epsg_vs_esri_authoritative():
    from databricks.labs.gbx.pyrx.core import crs as C
    # EPSG-only codes -> EPSG authority
    assert C.resolve_crs(4326).to_authority() == ("EPSG", "4326")
    assert C.resolve_crs(27700).to_authority() == ("EPSG", "27700")
    # ESRI-only codes -> ESRI authority (NOT mislabeled EPSG)
    assert C.resolve_crs(54008).to_authority() == ("ESRI", "54008")
    assert C.resolve_crs(102008).to_authority() == ("ESRI", "102008")
    assert C.resolve_crs("54008").to_authority() == ("ESRI", "54008")  # int-like string
    # int-classified == string form
    assert C.resolve_crs(54008) == C.resolve_crs("ESRI:54008")

def test_resolve_unresolvable_int_raises():
    import pytest
    from databricks.labs.gbx.pyrx.core import crs as C
    with pytest.raises(ValueError, match="valid EPSG or ESRI"):
        C.resolve_crs(99999999)

def test_canonical_labels_esri_code_as_esri():
    from databricks.labs.gbx.pyrx.core import crs as C
    assert C.crs_to_canonical(C.resolve_crs(54008)) == "ESRI:54008"
    assert C.crs_to_canonical(C.resolve_crs(4326)) == "EPSG:4326"
```

- [ ] **Step 2: Run — RED** (`54008` currently resolves to EPSG:54008 via lenient from_epsg → the authority/canonical assertions fail). Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_crs_resolve.py -v`

- [ ] **Step 3: Implement** — in `crs.py`, add cached `_epsg_codes()` / `_esri_codes()` (`lru_cache(maxsize=1)`, `pyproj.database.get_codes` over the CRS PJTypes — PROJECTED/GEOGRAPHIC_2D/GEOGRAPHIC_3D/GEOCENTRIC/COMPOUND, `allow_deprecated=True`; return `frozenset` of str). Rewrite `resolve_crs` int branch per the spec: normalise `n=str(int(...))`; EPSG set → `from_epsg`; ESRI set → `from_authority("ESRI", int(n))`; else `raise ValueError`. String branch unchanged (`from_user_input`).

- [ ] **Step 4: Run — GREEN** + the existing test_crs_resolve cases still pass.

- [ ] **Step 5: Commit** (`feat(pyrx): resolve_crs classifies int via authoritative PROJ epsg/esri sets`).

---

## Task 2: Relax `rst_setsrid` to `>= 0` (both tiers)

**Files:** `python/geobrix/src/databricks/labs/gbx/pyrx/core/edit.py` (`set_srid`, ~208); `src/main/scala/.../rasterx/expressions/pixel/RST_SetSrid.scala` (~77). Tests: light `test_crs_ops.py`; heavy `RST_CrsOpsTest` (Docker, Task 6-style).

- [ ] **Step 1: Failing test (light)** — `rst_setsrid(tile, 0)` stores 0 without raising; `rst_setsrid(tile, 54008)` stores 54008 without raising (dumb storage — no resolution); `rst_setsrid(tile, -1)` raises. Assert via the edit.set_srid path + rst_srid readback.

- [ ] **Step 2: Run — RED** (current `if srid <= 0: raise` rejects 0 and stores nothing).

- [ ] **Step 3: Implement** — light `edit.set_srid`: change `if srid <= 0: raise ... "positive EPSG"` to `if srid < 0: raise ValueError(f"rst_setsrid: SRID must be >= 0; got {srid}")`; stamp any `>= 0` code onto the CRS metadata WITHOUT resolving (it's a relabel/store — GDAL `SetFromEPSG`-style stamp is fine, but do NOT fail if the code isn't a live EPSG; store the tag). Heavy `RST_SetSrid.scala`: `require(srid > 0, ...)` → `require(srid >= 0, s"gbx_rst_setsrid: SRID must be >= 0; got $srid")`. NOTE: verify how set_srid actually stamps — if it currently calls `CRS.from_epsg(srid)` (which would raise on a non-EPSG code), decouple the STORE (tag the srid int) from RESOLVE; storing an ESRI-or-unresolvable code must not raise. If stamping genuinely requires building a CRS, then set_srid should build via `resolve_crs` (so ESRI codes work) and only raise for a truly-invalid code — reconcile this with "dumb storage": the tile's crs bytes need *some* CRS stamped, so `set_srid(54008)` stamps ESRI:54008 (resolvable), `set_srid(0)` clears/none, `set_srid(<unresolvable>)`... decide: does an unresolvable-but-nonneg code stamp raise here or defer? Per spec, storage shouldn't raise — but stamping into GTiff bytes needs a valid CRS. RESOLVE THIS in the task: likely set_srid on a materialized tile MUST resolve (to write real CRS bytes) so an unresolvable code raises AT set for materialized (that IS an apply), while a virtual tile records pending_srid without resolving. Document the decision in the report.

- [ ] **Step 4: Run — GREEN** (light); heavy deferred to the Docker task.

- [ ] **Step 5: Commit.**

> **Design reconciliation flagged for the implementer + review:** "dumb storage, no raise" is clean for the SRID *integer field* (virtual tile pending_srid, `rst_srid` readback), but stamping a CRS into materialized GTiff bytes inherently resolves. The likely correct split: virtual `rst_setsrid` records the int (no resolve, only `< 0` guard); materialized `rst_setsrid` must resolve to write CRS bytes (so it applies the rule → an unresolvable code raises there, which IS the apply moment). This keeps the invariant coherent. Confirm in Task 2 and note it.

---

## Task 3: Propagate the rule to apply-sites (light)

**Files:** `pyrx/core/edit.py` (clip_to_geom srid, ~82-87), `open_tile.py` (pending-srid materialisation ~147/293/541), `ops.py` (sample ~167). Test: `test_crs_non_epsg_pipeline.py` (extend).

- [ ] **Step 1: Failing test** — a data/caller path that supplies an ESRI code (54008) to an apply-site now yields the ESRI CRS (not a mislabel/raise). Pick the site(s) where a non-EPSG int genuinely flows (per the spec open-item: MAP which sites can receive non-EPSG first — a site pinned to EPSG-only gets no test). E.g. pending-srid materialisation with `pending_srid="54008"` → opens as ESRI:54008.

- [ ] **Step 2-4:** route those sites' `CRS.from_epsg(srid)` calls through `crs.resolve_crs(srid)`; RED→GREEN; regression `pytest python/geobrix/test/pyrx/ python/geobrix/test/ds/ -q` (ignore netCDF4-collection; no new failures). Do NOT touch EPSG-only pins.

- [ ] **Step 5: Commit.**

---

## Task 4: Docs — the canonical rule on the CRS page + per-function links

**Files:** `docs/docs/api/coordinate-reference-systems.mdx` (rule section + collision caveat + `srid>=0` storage note); one-line links from `docs/docs/api/raster-functions.mdx` (rst_srid/rst_crs/rst_setsrid/rst_setcrs/rst_transform/rst_transformcrs) and the vector reader `geom_0_srid` doc + grid srid params to the CRS page.

- [ ] **Step 1:** Document the rule: three-step authoritative classification (EPSG set → ESRI set → raise at apply), the "dumb storage `>= 0`, resolution/raise only at apply" model, and the collision caveat (a code in both authorities → epsg-first; explicit `ESRI:<n>` string is the escape). Note the source is PROJ's `proj.db`.
- [ ] **Step 2:** Add per-function links (a short "See [Coordinate Reference Systems](../api/coordinate-reference-systems)" line where each SRID/CRS function/param is documented).
- [ ] **Step 3:** Voice grep clean (`wave/inc N` empty); Docker docs build green (use `gbx:docs:build` — dev-server-aware — NOT a raw host build, per the cache-corruption guard).
- [ ] **Step 4: Commit.**

---

## Task 5: Heavy verify + parity (Docker)

**Files:** verify `SpatialRefOps.resolveCrs` / `crsToCanonical`; `RST_SetSrid.scala` `>= 0`; tests in the heavy suite.

- [ ] **Step 1: Verify GDAL labeling** (in container): confirm `ImportFromEPSG(54008)`→`GetAuthorityName=="ESRI"`, `4326`→"EPSG", `99999999`→non-zero (raise). (Confirmed this session; re-confirm as the task's RED/GREEN basis.) If GDAL labels everything correctly, heavy `resolveCrs` needs NO classification change — only confirm invalid→raise. If any ESRI-range code mislabels as EPSG, add the proj.db `crs_view` membership check heavy-side (spec option B).
- [ ] **Step 2:** Apply the `RST_SetSrid` `require(srid > 0)` → `>= 0` relax (from Task 2); reconcile the materialized-stamp-resolves decision heavy-side too.
- [ ] **Step 3:** Heavy tests: `resolveCrs`/`crsToCanonical` for 4326/54008/102008/99999999; `RST_SetSrid` accepts 0 and a non-neg code, rejects -1. Bump any count-assert if a test class is added. Build + run targeted suites in Docker (progress line). scalastyle clean.
- [ ] **Step 4: Commit.**

---

## Task 6: Cross-tier parity + end-to-end

- [ ] **Step 1: Parity** — `resolve_crs(54008)` (light) and `resolveCrs("54008")` (heavy) both yield ESRI:54008; `crs_to_canonical`/`crsToCanonical` both emit "ESRI:54008"; `rst_crs` agrees cross-tier for an ESRI raster (via the decoded/authority-equivalence check established in R's T8).
- [ ] **Step 2: e2e** — the vector reader's `geom_0_srid = "54008"` now round-trips as ESRI (via `_srid_to_crs` → `resolve_crs`), confirming the old authority-name gap is subsumed. A raster tile with pending_srid=54008 opens as ESRI.
- [ ] **Step 3: Regression** — light pyrx+ds CRS suites green; heavy CRS suites green.
- [ ] **Step 4: Ledger + done** — ready for final whole-branch review.

---

## Self-Review

**Spec coverage:** T1 = light authoritative classification (spec §rule, §resolvers-light). T2 = `rst_setsrid >= 0` (spec §set-time contract). T3 = propagation Q1→1 (spec §propagation). T4 = docs + links (spec §docs). T5 = heavy verify/parity (spec §resolvers-heavy). T6 = cross-tier + e2e + subsumes-V (spec §testing). ✓

**Placeholder scan:** the Task-2 "materialized-stamp-resolves vs dumb-storage" reconciliation is called out explicitly as a decision the implementer resolves + documents (not a hidden gap) — it's the one genuinely subtle point (storing a SRID int is dumb, but writing CRS *bytes* resolves). The "which sites receive non-EPSG" mapping (T3 Step 1) is flagged. No TBD. ✓

**Type consistency:** `resolve_crs`/`crs_to_canonical`/`_epsg_codes`/`_esri_codes` names consistent T1→T3→T6; heavy `resolveCrs`/`crsToCanonical` mirror. Verified against current `crs.py`, `edit.py:208`, `RST_SetSrid.scala:77`, and the pyproj `get_codes` API (confirmed this session). ✓

**Open risks for the review loop:** (1) the materialized-stamp reconciliation (T2) — get this coherent or the "dumb storage" invariant leaks. (2) `get_codes` PJType coverage — ensure the sweep captures all CRS types a real srid could be (projected/geographic/compound at minimum; verify no valid EPSG code is missed → a false "invalid" raise). (3) `allow_deprecated=True` — a deprecated-but-real EPSG code must still classify as EPSG, not raise. (4) heavy may be a genuine no-op — don't invent a change if GDAL already labels correctly; a parity test is the deliverable then.
