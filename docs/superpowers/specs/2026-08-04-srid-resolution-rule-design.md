# SRID Resolution Rule (epsg→esri) — Design (Spec R2)

**Status:** Approved design (2026-08-04). Feeds an implementation plan. The keystone of the CRS-completeness effort — the follow-on specs (VectorX CRS family, GridX surface, columnar `crs` params) all build on this rule.

**One-liner:** Define and apply one consistent rule for turning an integer SRID into a CRS — try `EPSG:<n>`, then `ESRI:<n>`, else invalid — across both tiers, and document it as the canonical reference.

---

## Terminology (firm)

- **SRID** = the **integer** tag: a stored numeric code interpreted (at resolution time) as EPSG-then-ESRI. `*srid*` functions are integer-only.
- **CRS** = the **catch-all string**: any of `EPSG:x` / `ESRI:x` / WKT / PROJ4. `*crs*` functions take strings.
- **SRID is dumb integer storage, matching the Databricks product.** A SRID can be *set* and *retrieved* as any integer, whether or not it is a real EPSG or ESRI code — get/set never resolve and never raise. `rst_srid` returns the stored int (which may be an ESRI code, or a not-yet-meaningful one), or NULL when the tile carries no code. The epsg→esri interpretation and any error happen **only when the SRID is applied** to build a CRS (projection/reproject), never on get/set. `rst_crs` is the companion for the full CRS string (authority string, or WKT/PROJ4 that no integer can express) and for the *resolved* canonical form.

## Problem

GeoBrix turns an integer SRID into a CRS in ~15 places (light `pyrx.core.crs.resolve_crs` + heavy `SpatialRefOps` + bypass sites), and every one assumes **EPSG only**: `CRS.from_epsg(n)` / `ImportFromEPSG(n)`. So an ESRI-coded dataset that carries only a numeric code (e.g. the vector reader's `geom_0_srid = "54008"` for World Sinusoidal) either fails to resolve or is silently mis-read as `EPSG:54008`. There is no defined, shared rule for "an integer that isn't a valid EPSG code might be an ESRI code."

## The rule

Given an integer (or int-castable string) `n`, resolved against **authoritative PROJ code registries** (not the lenient CRS constructors):

1. **`n` in the EPSG code set** → `EPSG:<n>`.
2. **else `n` in the ESRI code set** → `ESRI:<n>`.
3. **else invalid** — a code in neither registry is an error **at apply time** (raise when used to build a CRS/project). Storing or retrieving such an int is fine (dumb storage); the error surfaces only on application. A null / empty / `"0"` SRID is "no CRS declared" → the caller falls back to a CRS string (an explicit `crs` param, `geom_0_srid_proj`, or embedded WKT).

**Why authoritative membership, not the constructor:** `rasterio.crs.CRS.from_epsg(n)` (and even GDAL to a degree) is **lenient** — `from_epsg(54008)` *succeeds* and mislabels the result `EPSG:54008`, even though 54008 is an ESRI code (World Sinusoidal), not EPSG. So "try `from_epsg` first, catch the exception" does NOT distinguish EPSG from ESRI. Instead, membership is checked against the PROJ database's per-authority code lists — `pyproj.database.get_codes("EPSG", ...)` (~7500 codes) and `get_codes("ESRI", ...)` (~2900 codes), which are **disjoint and correct**: 4326/32618/27700 are EPSG-only, 54008/102008/102100/53008 are ESRI-only, 99999999 is in neither. The sets are built once and cached at module load. This makes the light tier label ESRI codes correctly — matching the **heavy** tier, where GDAL's `ImportFromEPSG` already resolves ESRI-range codes and reports `GetAuthorityName = "ESRI"` correctly.

**This mirrors the Databricks product:** an SRID integer is stored/retrieved freely; it is classified EPSG-then-ESRI only when applied for projection, and a code in neither registry errors only then.

**Collision caveat (documented):** EPSG and ESRI numeric code-spaces can overlap — a code valid in both resolves to the EPSG one under this rule (epsg-first). When a dataset is genuinely in the ESRI CRS of a colliding code, the escape is an explicit CRS string (`"ESRI:<n>"`) via a `crs` param or `geom_0_srid_proj` — not the bare int.

## Design

### Shared resolvers (both tiers) classify via authoritative PROJ code sets

**Light** — `pyrx/core/crs.py`, authoritative membership from the PROJ db (built once, cached):

```python
from functools import lru_cache

@lru_cache(maxsize=1)
def _epsg_codes() -> frozenset:
    from pyproj.database import get_codes
    from pyproj.enums import PJType
    s = set()
    for t in (PJType.PROJECTED_CRS, PJType.GEOGRAPHIC_2D_CRS, PJType.GEOGRAPHIC_3D_CRS,
              PJType.GEOCENTRIC_CRS, PJType.COMPOUND_CRS):
        s |= set(get_codes("EPSG", t, allow_deprecated=True))
    return frozenset(s)

@lru_cache(maxsize=1)
def _esri_codes() -> frozenset:
    # same PJType sweep for authority "ESRI"
    ...

def resolve_crs(value):
    if _is_intlike(value):
        n = str(int(str(value).strip()))
        if n in _epsg_codes():
            return CRS.from_epsg(int(n))            # 1. EPSG
        if n in _esri_codes():
            return CRS.from_authority("ESRI", int(n))  # 2. ESRI (labels correctly)
        raise ValueError(f"resolve_crs: {n} is not a valid EPSG or ESRI code")  # 3. raise
    return CRS.from_user_input(value)               # string form unchanged
```

`get_codes` returns code strings; normalise `n` to a string for membership. `crs_to_canonical` continues to read `to_authority()` — which now returns the correct `("ESRI", "54008")` because the CRS was built via `from_authority("ESRI", ...)`, not `from_epsg`.

**Source of truth = PROJ's `proj.db`.** Since PROJ 6, all CRS data lives in a SQLite `proj.db` (shipped in the pyproj/GDAL data dir). `get_codes` is just a wrapper over its `crs_view` table filtered by `auth_name`. Two equivalent implementations:
- **(A, light — recommended)** `pyproj.database.get_codes("EPSG"/"ESRI", ...)`, cached. pyproj-native, no SQLite handling.
- **(B, direct)** query `crs_view` in `proj.db` directly (`pyproj.datadir.get_data_dir()/proj.db`): `SELECT auth_name FROM crs_view WHERE code = ?` returns the authority list for a code — this also surfaces **collisions** (a code in both EPSG and ESRI returns both rows; apply epsg-first). B is the likely path for **heavy/Scala** (GDAL ships its own `proj.db`; no pyproj there) if heavy needs the membership check at all (it may not — GDAL already labels correctly, see below).

Confirmed against the shipped `proj.db`: `crs_view` has 7537 EPSG + 2902 ESRI CRS rows; `4326`→[EPSG], `54008`/`102008`→[ESRI], `99999999`→[] (invalid).

**Heavy** — `SpatialRefOps.resolveCrs` int case. VERIFY first (Task): GDAL's `ImportFromEPSG(54008)` already succeeds AND reports `GetAuthorityName = "ESRI"` (confirmed in the container), so heavy may need **no change** — `crsToCanonical` already emits `ESRI:54008` correctly. The only heavy change needed is the **raise on a genuinely-invalid code** (`ImportFromEPSG(99999999)` returns non-zero → already raises in the current `resolveCrs`). Confirm heavy's int path matches the light rule's outcomes (EPSG→EPSG, ESRI-range→ESRI, invalid→raise); if GDAL's labeling is already correct across the board, heavy is a no-op + a parity test. If any ESRI-range code is mislabeled `EPSG` by GDAL, mirror the authoritative-set check heavy-side.

### Propagation (Q1→1: fix the resolver + plausible sites; don't churn EPSG-only internals)

- Route the bypass sites that turn a **caller-supplied / data-derived** integer into a CRS through the shared resolver so they inherit the epsg-then-esri classification: light `edit.set_srid`, `open_tile` pending-srid materialisation, `ops.sample`; heavy `RST_SetSrid`, `VectorRasterBridge`, `RST_Clip`, the rasterize-agg srid paths, `RST_GridFromPoints`.
- **Do NOT refactor** sites that only ever see a fixed, hardcoded EPSG (grid internals pinned to 4326/27700, `fromEPSGCode`'s WGS84 default) — they can't receive an ESRI code, so the classification is dead weight there. (Q1→1.)
- **Reader leniency preserved:** the vector reader's `geom_0_srid` and any path where `0`/absent is normal stays lenient (→ null / "no CRS"), NOT a raise. The raise (Q2→1) applies only to a genuinely-unresolvable **positive** int a caller explicitly supplied.

### Behavior on unresolvable int — apply-time only

Get/set of a SRID stores/returns the integer as-is (dumb storage, matching the product), with **one bound: `srid >= 0`** — a negative SRID is unambiguously invalid and is rejected at set time. Any non-negative int is accepted and stored, whether or not it currently resolves to an EPSG/ESRI CRS (no upper gate, no resolution at set time). The epsg→esri resolution and the "unresolvable code" error live in `resolve_crs` / `resolveCrs`, the **apply moment** (building a CRS to project/reproject): a non-negative int that resolves to neither EPSG nor ESRI **raises** there (`ValueError` / `IllegalArgumentException` naming the code). `0` / null / empty at apply time is "no CRS" (fall back to the CRS string), not an error. So a tile can carry a non-negative-but-unresolvable SRID indefinitely; the failure surfaces only when something tries to project with it.

### Docs — the canonical explanation + per-function links

The **Coordinate Reference Systems** page (`docs/docs/api/coordinate-reference-systems.mdx`) documents the rule as the single source of truth: the three-step resolution, the collision caveat, and the SRID(int)-vs-CRS(string) distinction. Every function/reader that deals with SRID or CRS gets a one-line link to that page (raster-functions rst_srid/rst_crs/rst_setsrid/rst_setcrs/rst_transform/rst_transformcrs; the vector reader `geom_0_srid` doc; grid srid params). No behavior claims duplicated across pages — they link.

## Non-Goals (→ follow-on specs)

- **VectorX CRS family** (`st_setcrs`/`st_crs`/`st_transformcrs`) — separate spec; entirely absent today.
- **GridX complete surface** (BNG cell SRID stamping, grid CRS accessor) — separate spec.
- **Columnar `crs` params** (`rst_clip` clip_crs, `rst_sample`, `rst_viewshed`, rasterize srid→crs, grid-input reprojection) — separate spec(s). R2 only makes the *rule* consistent; the *new params* come later.
- **No new registered function names** in R2 (it's a resolver + docs change). No `rst_srid` behavior change.
- **The old "vector reader authority-name" fix is subsumed:** with authoritative classification, `geom_0_srid = "54008"` resolves as ESRI:54008 correctly; the reader needs no value/shape change.

## Testing

1. **`resolve_crs` unit (both tiers) — the apply moment:** `4326` → EPSG:4326; `"4326"` → EPSG:4326; `54008` → ESRI:54008 (via authoritative-set classification — RED before the fix, which mislabels it EPSG); `"ESRI:54008"` string → same; a WKT/PROJ4 string → its CRS; a bogus int (`99999999`) → **raises** with a clear message (apply-time error); `0` treated as "no CRS" by callers (guarded before calling).
   **Set/get never resolve:** set-srid accepts any **non-negative** int (e.g. `0`, `54008`, or an unresolvable one) and `rst_srid` returns it unchanged WITHOUT raising — assert storing+reading a non-negative bogus SRID does not error (only applying it does). A **negative** SRID (`-1`) is rejected at set time (the one set-time bound, `>= 0`).
2. **epsg/esri agreement:** `resolve_crs(54008)` (int, classified ESRI) == `resolve_crs("ESRI:54008")` (string) — same CRS object, both tiers.
3. **Propagated sites:** a function/reader fed an ESRI numeric code (54008) now yields the ESRI CRS, not a failure or a mis-tagged EPSG. Cover one light + one heavy propagated site (e.g. `rst_setsrid`-int path can't take ESRI by design — pick a site where a data-derived code flows, e.g. the vector-reader round-trip through `_srid_to_crs`, or `RST_SetSrid` if it should now accept an esri-coded int — VERIFY which sites legitimately receive a non-EPSG int vs only-EPSG).
4. **Reader leniency:** a `geom_0_srid = "0"` / absent stays "no CRS" (null), does not raise.
5. **No regression:** every existing EPSG path (4326/32618/27700) resolves identically; `rst_srid` unchanged.
6. **Docs:** rule documented on the CRS page; per-function links present; voice-clean; docs build green.

## Files (anticipated; finalized in the plan)

- Light: `pyrx/core/crs.py` (`resolve_crs` authoritative epsg/esri classification); route the plausible bypass sites (`edit.py`, `open_tile.py`, `ops.py`) through it where they turn a data/caller int into a CRS.
- Heavy: `SpatialRefOps.scala` (`resolveCrs` — verify GDAL labels correctly, else add proj.db membership; consider `fromEPSGCode` — but it defaults WGS84 on ≤0, EPSG-only otherwise — decide if it should route through resolveCrs for >0 codes); the bypass sites that receive a data/caller int.
- Docs: `docs/docs/api/coordinate-reference-systems.mdx` (the rule section + collision caveat) + one-line links from raster-functions / vector reader / grid srid docs.
- Tests: `pyrx/test/test_crs_resolve.py` (extend with the ESRI-classification + raise cases) + heavy `SpatialRefOpsTest` / resolveCrs test.

## Open items for the plan

- **`rst_setsrid` set-time validation:** the R work's `rst_setsrid` has `require(srid > 0)` (heavy) / a positive-EPSG guard (light) at SET time. Per the corrected contract, relax it to **`srid >= 0`** — reject only negatives; accept any non-negative int (incl. `0` = "no CRS", and codes like `54008` or currently-unresolvable ones), stamping it without resolving. The epsg→esri→raise moves entirely to the resolve/apply path. (This revises R's `rst_setsrid` `> 0` → `>= 0`; nothing pushed, clean to adjust. Verify tile SRID storage still round-trips.)
- Confirm which propagated sites legitimately receive a **non-EPSG** integer at APPLY time (the rule lives in `resolve_crs`; a site benefits only if a non-EPSG int reaches the apply moment). Map precisely so we don't add dead classification.
- RESOLVED (verified this session): light resolves ESRI via `from_authority("ESRI", n)` (works; labels correctly). Membership from `pyproj.database.get_codes` / `proj.db` `crs_view` — EPSG/ESRI sets are disjoint + authoritative. from_epsg is lenient (accepts ESRI codes, mislabels) → do NOT use "try from_epsg, catch" for classification.
- Heavy: VERIFY whether GDAL needs the explicit membership check at all — `ImportFromEPSG` already resolves ESRI-range codes and `GetAuthorityName` reports "ESRI" correctly (confirmed in-container for 54008/102008). If so, heavy `resolveCrs` is a near-no-op (just confirm invalid→raise); if any code is mislabeled, add the proj.db `crs_view` membership check heavy-side (option B).
