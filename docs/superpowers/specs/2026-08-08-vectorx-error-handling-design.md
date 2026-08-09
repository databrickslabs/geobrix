# Consistent VectorX Error Handling (both tiers) — design

**Date:** 2026-08-08
**Status:** ratified (this doc), pending plan
**Scope:** VectorX only (heavy Scala + light pyvx). Sibling of the shipped
RasterX error-handling work (`2026-08-08-rasterx-error-handling-design.md`).
Item 1 of the CRS thread (order: VectorX → GridX → PROJ grid-shift).

## Goal

Make every VectorX function degrade consistently on degenerate input, absorbing
the two deferred CRS loose ends: the **garbage-bytes divergence** (`st_transformcrs`
returns the input unchanged on unparseable data while `st_setcrs` returns NULL) and
the **finite-nonsense out-of-domain survivor** (`POINT(150 -80)` → `EPSG:27700`
yields a finite point ~16,500 km outside Great Britain, which the non-finite guard
misses). Also deliver a user-facing **Error Handling docs page** explaining the
whole GeoBrix approach.

## The organizing principle (the whole thesis)

> **A bad / non-executable PARAMETER is a usage error → raise an exception.
> Bad DATA flowing through → degrade (NULL / empty result), never kill the stage.**

This one axis reconciles RasterX and VectorX:
- **RasterX** (shipped) already lives it: a null/corrupt *tile* (data) → NULL accessor
  / empty tile / skipped aggregator member / one error-row; while `crashExpressions`
  and `VirtualTileException` (misuse) raise. Framed there as "return-type-derived,"
  but the deeper axis is data-vs-parameter.
- **VectorX** (this spec) states it directly: bad geom *data* → NULL; bad CRS
  *argument* → raise.

## Constraints & context (verified against current code)

- **VectorX has NO metadata carrier.** Functions return bare `BinaryType` (WKB/EWKB),
  `StringType` (WKT/EWKT or a CRS string for `st_crs`), or aggregate/generator shapes.
  There is nowhere to attach a `last_error` reason (unlike RasterX's tile metadata).
  So **NULL is the only data-degrade signal a geometry function can carry** — which is
  exactly why the data-degrade contract is "NULL, uniformly."
- **Heavy VectorX has ZERO `safeEval`** (like GridX) and raises `IllegalArgumentException`
  directly. `ST_SetCrs` already has a `CrsOutcome.NullOut` sealed-type path for
  null/unparseable geom; `ST_TransformCrs` currently passes unparseable input through
  unchanged and raises on a bad target CRS (deliberate: "user asked for a bad CRS").
- **Light pyvx** (`pyvx/_crs.py`) routes every parse through `_parse_geom_safe`
  (never-error parse → None on garbage). `st_transformcrs` returns the input
  **unchanged** when no source CRS is resolvable / geom unparseable (its "never-error"
  invariant); `st_setcrs` returns None on unparseable geom but **raises `ValueError`**
  on an authority-less CRS. `_has_nonfinite_xy` guards only Inf/NaN X/Y post-transform.
- **Registered VectorX functions:** `gbx_st_crs`, `gbx_st_setcrs`, `gbx_st_transformcrs`,
  `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`, `gbx_st_triangulate`,
  `gbx_st_interpolateelevationbbox`, `gbx_st_interpolateelevationgeom`,
  `gbx_st_legacyaswkb`.
- **`crashExpressions` is a heavy RasterX mechanism; VectorX has none today.** This spec
  does NOT add one to VectorX (no per-call knob — SQL binds positionally, per
  `strict-mode-workstream`); the raise-on-bad-parameter path is the "loud" signal.
- Prior thinking: `prompts/refactoring/2026-08-05-crs-loose-ends-and-consistency-handoff.md`
  Part 2.1; memories `crs-consistency-handoff-doc`, `vectorx-crs-family-decisions`.

## The contract

| Failure source | Treatment | Rationale |
|---|---|---|
| **Bad geometry DATA** — unparseable WKB/WKT, corrupt bytes, non-finite reprojection output, **out-of-domain reprojection** | **NULL** | Per-row data; one bad row must not kill the stage. `WHERE geom IS NOT NULL` catches all of it uniformly. Retires `st_transformcrs`'s silent "return unchanged" (a lie — caller thinks a reproject happened) and `st_setcrs`'s NULL-vs-raise split for data. |
| **Bad CRS ARGUMENT** — `transformcrs(geom, "EPSG:99999")`, unparseable CRS literal, authority-less CRS on `st_setcrs` | **RAISE** (`IllegalArgumentException` heavy / `ValueError` light), message names the offending CRS | One value for the whole query; a typo'd literal is fix-your-code, not per-row data. An all-NULL column with no signal is worse UX. Matches existing deliberate heavy behavior. |

Applies uniformly across the geometry-returning functions. `st_crs` (returns a CRS
string) degrades bad-data to NULL likewise. The MVT aggregators (`st_asmvt`,
`st_asmvt_pyramid`) and TIN generators (`st_triangulate`, `st_interpolateelevation*`)
keep their return-type-appropriate NULL/empty degrade for bad data and raise on a bad
parameter — same axis.

## The domain / extent check

**Gap:** `_has_nonfinite_xy` (light) / the heavy equivalent catch only Inf/NaN output.
A reprojection yielding *finite-but-meaningless* coordinates (`POINT(150 -80)` →
`EPSG:27700`) is not caught and `WHERE y IS NOT NULL` cannot catch it.

**Mechanism:** on the `st_transformcrs` path, test whether the input geometry's
coordinates **in lon/lat** fall inside the **target CRS's `area_of_use` bounds** (a
cheap bbox containment test). Both tiers already have the bounds source, used nowhere yet:
- Light: `pyproj.CRS.area_of_use.bounds` → `(west, south, east, north)` in EPSG:4326 degrees.
- Heavy: GDAL `SpatialReference.GetAreaOfUse()` → same lon/lat bounds (GDAL 3.0+).

Both `area_of_use` values are already in EPSG:4326, so the comparison frame is consistent
(convert the source point to lon/lat if the source CRS is not already geographic).

**Behavior:**
- Out-of-domain → **NULL** (folds into the same data-degrade signal — no new third outcome).
- **On by default**, but **only rejects a point provably outside a KNOWN `area_of_use`**.
  If the target CRS has no `area_of_use` metadata → **skip the check** (cannot prove
  out-of-domain → never NULL a point we cannot disprove). Conservative — never false-positives.
- **Transform path only.** `st_setcrs` (stamps an SRID, no coordinates touched) and
  `st_crs` (reads one) do not reproject, so the check is meaningless there.
- **Straddling geometry** (some vertices inside the bounds, some outside) → treated as
  **out-of-domain → NULL**. A geometry partly outside the CRS's valid area cannot be
  reliably reprojected as a whole; a per-vertex partial result would be a silent lie.

## Components

### 1. Heavy (Scala)
- **`ST_TransformCrs`**: unparseable/degenerate geometry DATA → `CrsOutcome.NullOut`
  (retire the passthrough); keep bad target-CRS → raise.
- **`ST_SetCrs`**: bad geometry DATA → `NullOut` (already does for unparseable geom);
  keep authority-less CRS *argument* → raise.
- **Domain check**: add to the transform path in shared `operations/SpatialRefOps.scala`
  via GDAL `GetAreaOfUse()`, gated on bounds-present; out-of-domain → NULL.
- No `safeEval` is introduced — the NULL path uses the existing `CrsOutcome` mechanism.

### 2. Light (pyvx `_crs.py` + shared `core/crs.py`)
- **`st_transformcrs`**: retire "return `geom` unchanged" on unparseable data → return
  **NULL**; add the domain check (`pyproj.CRS.area_of_use.bounds`) → NULL when provably outside.
- **`st_setcrs`**: bad geometry DATA → NULL (already does for unparseable geom); keep the
  authority-less **CRS argument** `ValueError`.
- Domain-check helper lives in shared `core/crs.py` so both the light path and the
  cross-tier contract stay aligned.

### 3. Docs — new `api/error-handling` page
- Create `docs/docs/api/error-handling.mdx`; wire into `docs/sidebars.js` under the
  **"Functions"** category, after `api/coordinate-reference-systems` (its conceptual
  companion).
- **Scope = the WHOLE GeoBrix error-handling philosophy**, not just VectorX. Lead with the
  organizing principle (bad parameter → exception; bad data → NULL/degrade). Then show how
  each surface expresses it: RasterX (NULL accessors / empty-tile+`last_error` /
  skip-corrupt aggregators / one error-row generators — already shipped), VectorX (NULL for
  bad data, RAISE for bad CRS argument, the domain check), and the heavy `crashExpressions`
  dev escape hatch.
- User-facing voice — no internal planning vocabulary (per `user-facing-docs-voice`; QC
  `internals-leak` check).

## Testing / acceptance

Cross-tier parity is the spine — same degenerate input, same signal on both tiers.

1. **Shared corrupt-input corpus:** (a) unparseable WKB bytes; (b) mixed-dimensionality /
   garbage WKT; (c) authority-less CRS *argument* (raw WKT/PROJ4, `OGC:CRS84`); (d) invalid
   CRS literal (`EPSG:99999`); (e) out-of-domain point (`POINT(150 -80)` → `EPSG:27700`);
   (f) geometry straddling the target's area_of_use; (g) target CRS with NO area_of_use
   metadata (must NOT be NULL'd); (h) non-finite reprojection output.
2. **Per-behavior assertions (both tiers):**
   - Bad data (a,b,e,f,h) → **NULL**, no raise. Explicit regression: `st_transformcrs(garbage)`
     returns NULL (no longer the input unchanged) — the garbage-bytes-divergence fix.
   - Bad CRS argument (c,d) → **RAISE** a clear error naming the offending CRS.
   - Bounds-absent (g) → **NOT NULL'd** (domain check skipped) — proves never-false-positive.
   - In-domain control: a valid in-GB point → `EPSG:27700` succeeds unchanged (check
     transparent on good input).
3. **Where run:** heavy Scala suites + `gbx:test:python` (pyvx) in the `geobrix-dev`
   container, via the `gbx:*` palette. Real geometries/bytes, not mocks (mock only
   external/expensive/flaky per repo doctrine).
4. **Docs page:** built clean (`npm run build`, no broken-link warnings), wired into
   `sidebars.js`; a doc-test-backed example if it shows code.

## Out of scope

- **GridX** error handling (item 2 of the CRS thread — its own spec). The `BNG.parse`
  legible-exception fix already shipped, but the BNG/Quadbin/Custom degrade-policy
  reconciliation is separate.
- **PROJ grid-shift / custom datum grids** (item 3 of the CRS thread).
- Broader domain validation beyond the area_of_use bbox test (e.g. per-operation accuracy
  thresholds, `only_best` transform selection) — the bbox containment test is the scoped
  mechanism here.
- Adding a `crashExpressions`-style knob to VectorX, or any new per-call `strict`/`check`
  parameter.
- Mixed-Z / 3D handling (tracked separately; `vectorx-crs-family-decisions` provisional
  mixed-Z-as-2D ruling stands).

## Outcome

Every VectorX function degrades bad geometry data to NULL and raises a clear error for a
bad CRS argument — uniformly across both tiers. The garbage-bytes divergence is gone
(`st_transformcrs` no longer silently returns the input unchanged), the finite-nonsense
out-of-domain survivor is caught by a conservative area_of_use bbox check (out-of-domain →
NULL, skipped when bounds are unknown so it never false-positives), and a new Error
Handling docs page explains the "bad parameter → exception, bad data → NULL" philosophy
across all of GeoBrix.
