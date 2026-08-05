# VectorX CRS Family (`st_crs` / `st_setcrs` / `st_transformcrs`) — Design

**Status:** Design (decisions Q1–Q6 + Q4/Q5 approved). Follow-on to the CRS-completeness effort (after RasterX CRS to 100%, which shipped the resolver, transformer cache, and the source/output-role model this spec reuses). Branch `branch/0.5.0`, v0.5.0 beta.

**Goal:** Add VectorX's **CRS-string** complement to the Databricks product's int-SRID built-ins — three `gbx_st_*` functions, both tiers, mirroring the RasterX `rst_crs`/`rst_setcrs`/`rst_transformcrs` split — with **geometry-honest** semantics: a geometry stores only an integer SRID (via EWKB/EWKT), so the string functions bridge string↔SRID, and `st_transformcrs` is the full-CRS reprojection workhorse. **VectorX never duplicates the product's `st_srid`/`st_setsrid`/`st_transform`.**

---

## 1. The three functions

### 1.1 `st_transformcrs(geom, target_crs, source_crs=None)` — reproject to a CRS string (the workhorse)

Reproject a geometry to a **target CRS given as a string**: `EPSG:x`, `ESRI:x`, **WKT**, or **PROJ4** (Q1) — the forms the product's int-only `st_transform` cannot name.

- **Source CRS (Rule 1 + Q5):** the geometry's embedded EWKB/EWKT SRID is the source; for a plain WKB/WKT geometry with no SRID, the optional **`source_crs`** parameter (any CRS string) declares it. No source resolvable (plain geom, no `source_crs`) → **return the geometry unchanged** (never-error; documented that you must supply the source).
- **Coordinates are always reprojected** to the target.
- **Output encoding — the return matrix (Q6):**

  | Input geom | Target `EPSG:n` / `ESRI:n` (has int code) | Target WKT / PROJ4 (authority-less) |
  |---|---|---|
  | **EWKB** | **EWKB**, SRID = `n` | **WKB** (SRID cleared) |
  | **WKB**  | **EWKB**, SRID = `n` (upgraded) | **WKB** |
  | **EWKT** | **EWKT**, `SRID=n;…` | **WKT** (prefix dropped) |
  | **WKT**  | **EWKT**, `SRID=n;…` (upgraded) | **WKT** |

  Principle: output medium follows input **text-vs-binary**; the carried SRID is a function of the **target** — an authority-coded target carries its int `n` (upgrading a plain geom to its E-form), an authority-less target carries **no** SRID (plain form, and any stale source SRID is **cleared** — never mislabel reprojected coordinates with the old code).

### 1.2 `st_setcrs(geom, crs)` — string→SRID stamper (Q2)

Stamp a geometry's SRID from a CRS **string**, focused on **`EPSG:*` / `ESRI:*`** (and int-castable strings like `"4326"`): resolve the string to its **integer SRID** and set it on the geometry. Does **not** reproject (coordinates unchanged) — the string-convenience over the product's int `st_setsrid`.

- **WKT / PROJ4 rejected:** an authority-less CRS has no int code to stamp onto a geometry → raise a clear error (a geometry cannot hold a WKT).
- **Encoding-preserving:** `[E]WKT` in → **EWKT** out (`SRID=n;…`); `[E]WKB` in → **EWKB** out (embedded SRID).

### 1.3 `st_crs(geom)` — SRID→string reader (Q3)

Return the **canonical string of the geometry's embedded SRID** (EWKB/EWKT) — `EPSG:*` / `ESRI:*`. The string companion to the product's int `st_srid`.

- **Plain WKB/WKT (no SRID) → null** (no-op): a bare geometry carries no CRS and no WKT to return.

---

## 2. Architecture

### 2.1 Tier-neutral CRS resolver (Q4-2) — a prerequisite refactor

The CRS resolver currently lives per-package: light in `pyrx.core.crs` (`resolve_crs` / `crs_to_canonical` / `get_transformer` / `resolve_source_crs`), heavy in `rasterx.operations.SpatialRefOps` (`resolveCrs` / `crsToCanonical` / `getTransformer` / `resolveSourceSR`). VectorX must reuse the *same* authority, not fork it. Per Q4-2, **extract the resolver to a tier-neutral module both packages import:**

- **Light:** new `databricks.labs.gbx.core.crs` (pure-Python: rasterio/pyproj only, no raster/vector deps). Move the resolver bodies there; leave `pyrx.core.crs` as a **re-export shim** (`from databricks.labs.gbx.core.crs import *`) so the ~dozen pyrx importers and the just-shipped CRS-100 code keep working unchanged. pyvx imports from `gbx.core.crs`.
- **Heavy:** new `com.databricks.labs.gbx.operations.SpatialRefOps` (or `gbx.crs.SpatialRefOps`) — tier-neutral home. Leave `rasterx.operations.SpatialRefOps` as a thin forwarder/`type` alias, or update the rasterx importers to the new path. VectorX heavy imports the neutral one.

This is a **refactor of shipped, working code** — the risk is import churn. Mitigation: shims/aliases keep old import paths valid; a full test pass (both tiers) after the move is the gate. It is its own task, landed **before** the function tasks.

### 2.2 Geometry SRID I/O

Reuse the shared `parse_geom` / `geom_to_wkb` (accept WKB/EWKB/WKT/EWKT — `[[geom-input-consistency-across-st]]`), plus:
- **Light:** shapely `set_srid` + `to_wkb(include_srid=True/False)`; EWKT has **no native shapely writer**, so compose the `SRID=n;` prefix manually onto `to_wkt(...)` (and strip it on parse — `parse_geom` already does). Reprojection via the cached `get_transformer` (shapely `ops.transform`).
- **Heavy:** JTS `setSRID` + the EWKB writer (`JTS.toWKB`/EWKB), EWKT prefix compose; reprojection via `OSRTransformGeometry` / a proj transform, target SR from `SpatialRefOps.resolveCrs`.
- An **input-encoding classifier** (text-vs-binary; E-vs-plain by presence of SRID/prefix) drives the Q6 output-encoding matrix. Centralize it so light and heavy agree cell-for-cell.

### 2.3 Constraints

- **No product duplication** — ship only `st_crs`/`st_setcrs`/`st_transformcrs`; never `st_srid`/`st_setsrid`/`st_transform`.
- Both tiers + binding parity (registered name, `function-info.json`, Scala `override def name`, Python binding, `registered_functions.txt`) — `[[new-feature-dep-and-tier-checklist]]`.
- **Never-error invariant:** absent/unresolvable-source degrades (transformcrs returns as-is; st_crs returns null); only an explicitly-unresolvable target string, or `st_setcrs` with a WKT/PROJ4 (no int code), raises.
- Cross-tier parity: identical output bytes/text for authority-coded cases; CRS-equivalence (not string-eq) for reprojected coordinates (`[[rst-crs-cross-tier-string]]`).

---

## 3. Testing

Per repo rules (tests execute real code; heavy in Docker):
- **Return matrix (§1.1):** all 12 cells — for each of {EWKB, WKB, EWKT, WKT} × {EPSG target, ESRI target, WKT target, PROJ4 target}, assert the output **encoding** (binary/text, E-vs-plain) and **carried SRID** (int `n` / cleared) match the matrix, and coordinates are reprojected (a known point transforms to the expected target coordinate).
- **`st_setcrs`:** EPSG/ESRI/int-string stamps the SRID (encoding-preserving); WKT/PROJ4 raises; coordinates unchanged.
- **`st_crs`:** EWKB/EWKT SRID → `EPSG:x`/`ESRI:x`; plain WKB/WKT → null.
- **Never-error:** transformcrs on a plain geom with no `source_crs` returns input unchanged; unresolvable target string raises.
- **Cross-tier parity:** light == heavy for the authority-coded matrix cells; reprojected coords equal within tolerance.
- **Resolver refactor (§2.1):** the full existing pyrx + rasterx CRS suites pass unchanged through the shims (the relocation is behavior-preserving).

---

## 4. Out of scope

- **GridX complete surface** (custom-CRS input reprojection into grid SRID; grid CRS accessors) — separate follow-on.
- **VectorX geometry-validity** (`st_make_valid`/`st_explain_validity`) — separate spec ([[vectorx-geometry-validity-fns]]).
- Storing a WKT/PROJ4 *on a geometry* — impossible (geometry holds only an int SRID); explicitly not attempted.
- Adding rows to the CRS-page master table for these functions — done as part of this spec's docs task (append to the table seeded by CRS-100 T11).
