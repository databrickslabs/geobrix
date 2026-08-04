# RasterX CRS to 100% — Design (CRS Sub-spec, follow-on to R2)

**Status:** Design (approved decisions Q1–Q10 baked in). Successor to Spec R (raster CRS-string) and Spec R2 (SRID resolution rule). Branch `branch/0.5.0`, v0.5.0 beta.

**Goal:** Close every remaining CRS gap in the **RasterX** package so that (a) every geometry-accepting raster function lets the caller declare the geometry's *source* CRS, (b) every function that projects an *output* accepts a CRS string (ESRI/WKT/PROJ4), not just an int EPSG code, (c) `rst_{h3,quadbin}_rastertogrid*` stop silently assuming EPSG:4326, (d) **VizX** plots the same CRS-aware way with a matching `crs` override (Group G), and (e) **the full nuance of every rule below is taught on the Coordinate Reference Systems docs page** (Group F) — all routed through the R2 resolver, and all honoring the invariant **absent CRS never throws**. VectorX (`st_crs`/`st_setcrs`/`st_transformcrs`) and GridX remain separate follow-on specs.

---

## 1. The two CRS roles (the standardization at the heart of this spec)

A geometry argument to any function has **two independent CRS roles**. Conflating them under one word (`srid`) is the root of the current gaps.

1. **Source CRS** — what CRS the geometry's *own coordinates* are in.
2. **Target / output CRS** — what CRS the operation *reprojects into or stamps on its produced output*.

These become a **GeoBrix-wide standard** (documented on the Coordinate Reference Systems page; VectorX/GridX inherit it):

| CRS role | User-facing param names | Meaning |
|---|---|---|
| **Target / output** | `out_srid` / `out_crs` | "project the output into this" |
| **Source** | `srid` / `crs` (or `clip_crs`) | "my input geometry is already in this" |

Param **name encodes role**: an `out_`-prefixed param controls output projection; a bare `srid`/`crs`/`clip_crs` declares what the input already is.

### 1.1 Rule 1 — Source CRS of any geometry (universal)

Precedence, applied wherever a WKB/EWKB/WKT/EWKT geometry is consumed, both tiers, every package:

| Input | Source CRS |
|---|---|
| **EWKB / EWKT** (embedded SRID) | the embedded SRID — **always the source** |
| **WKB / WKT** + explicit `srid` **or** `crs` param | that param (via the R2 resolver) |
| **WKB / WKT**, nothing provided | **0 / CRS-less** |

Tight corollaries:
- `srid` and `crs` are two spellings of the **same source param**. Both provided → **error** `"provide srid OR crs, not both"`. This is a **call-level config** check (one value for the whole invocation, statically known) — cheap, unambiguous, always enforced.
- The explicit source param is a **per-geom fallback for plain WKB/WKT only** (Q12). It is applied **per geometry**, and ONLY to geometries lacking an embedded SRID. A geometry carrying an embedded SRID always uses it; the param is ignored for that geometry — **no error**. This is exactly the existing `_clip.clip_dataset` behavior ("clip_crs applies ONLY when the geometry carries no embedded SRID").
- **Mixed columns are first-class** (Q12). A Column of geometries may freely mix EWKB/EWKT (embedded SRID) and WKB/WKT (no SRID) rows; the scalar `srid`/`crs` param labels only the plain-WKB rows, per-geom. There is NO per-row "embedded SRID + param present → error": that case cannot be evaluated statically for a column, and erroring per-row would break the valid mixed-column workflow. The only conflict error is the call-level `srid`-AND-`crs`-both-set config mistake above.

### 1.2 Rule 2 — Target / output CRS (only functions that reproject or produce output)

| Function class | Target CRS source | Explicit `out_*` param? |
|---|---|---|
| **Reproject-to-tile** (`rst_clip`, `rst_sample`, `rst_viewshed`) | the tile's own CRS (intrinsic) | **No** — target is the raster you operate on |
| **Reproject-to-grid** (`rst_*_rastertogrid`) | the grid's native CRS (intrinsic: 4326 h3/quadbin, 27700 bng) | **No** |
| **Produce-new-raster** (`rst_rasterize`, `rst_gridfrompoints`, `rst_dtmfromgeoms`; grid `rasterize_agg`, `gridspec`, `CellBBox`) | **must be stated** — the output raster's CRS; the extent coords are in it | **Yes** — `out_srid`/`out_crs` |

`out_srid` and `out_crs` are two spellings of the **same output param**: both provided → **error**. Neither provided → grid-native (grid funcs) or the geometry's source CRS carried through (rasterize/gridfrompoints/dtm — see §3.2).

### 1.3 The unified never-error fallback (holds everywhere)

Given a resolved source CRS `S` and target CRS `T`:
- `S` known, `T` known, `S != T` → **reproject** `S → T`.
- `S == T` → no-op.
- **`S` unknown (CRS-less)** → assume the geometry is already in `T`; proceed; **never throw**.
- The ONLY throwing path is an **explicitly-supplied unresolvable CRS string** (R2 apply-time semantics), or the two conflict errors in §1.1.

This is the spine: absence always degrades to a sensible assumption; only explicit contradiction or explicit garbage raises.

---

## 2. Change inventory

All routed through the R2 resolver (`resolve_crs` light / `SpatialRefOps.resolveCrs` heavy). No new v2 schema. GDAL registration only via `GDALManager`. Beta + no-aliases → param renames (`srid`→`out_srid`) are expected, not a break to avoid.

### 2.0 Verified tier map (audited directly against Scala + Python sources, NOT the recon table)

The recon table's tier labels were wrong for several functions. The following was verified by matching every function against `override def name` in `src/main/scala/` (heavy) and `def rst_*` / registered names in `pyrx/functions.py` (light):

| Function | Heavy | Light | Note |
|---|---|---|---|
| `rst_clip` | ✅ | ✅ | both |
| `rst_sample` | ✅ | ✅ | **both** (recon said light-only — WRONG) |
| `rst_viewshed` | ✅ | ✅ | **both** (recon said light-only — WRONG) |
| `rst_rasterize` / `rst_rasterize_agg` | ✅ | ✅ | both |
| `rst_gridfrompoints` / `rst_gridfrompoints_agg` | ✅ | ✅ | **both** (recon said light-only — WRONG) |
| `rst_dtmfromgeoms` / `rst_dtmfromgeoms_agg` | ✅ | ✅ | both |
| `rst_{h3,quadbin,bng}_rasterize_agg` | ✅ | ✅ | both |
| `rst_{h3,quadbin,bng}_rastertogrid*` (24 fns) | ✅ | ✅ | both |
| `rst_h3_gridspec` | ❌ | ✅ | **light-only** (confirmed no heavy expr); a **DataFrame-level helper** (`df` first arg), not a per-row Column expression |
| `gbx_h3_cell_bbox` | ✅ | ✅ | both; registered under the **`gbx_h3_*` (GridX)** namespace, NOT `rst_*`. No raster dependency (light uses only `cellraster._h3_str`/`_reproject`; heavy uses only `H3.*` + OSR reprojection). **Relocated to GridX by this spec (Q11-3).** |

**Every function in this spec is BOTH-tiers unless the table above says otherwise.** Only `rst_h3_gridspec` is light-only.

### Group A — source-CRS declaration on geometry inputs (keeps bare `crs`/`clip_crs`)

| Fn | Tiers | Change |
|---|---|---|
| **A1 `rst_clip`** | Both | Add `clip_crs` (string) to the **Column API** + heavy `RST_Clip` builder/eval. Light reader + `_clip.clip_dataset` already honor `clipCrs`; wire the public function. **Fix** `_clip.py:_epsg_int` (int-castable only) → route through `resolve_crs` so ESRI/WKT cutline CRS work. |
| **A2 `rst_sample`** | Both | Add `crs` (string). `ops.sample` already routes embedded SRID through `resolve_crs` (R2 T3); add the explicit-param fallback path. Heavy `RST_Sample` mirrors. |
| **A3 `rst_viewshed`** | Both | Add `crs` (string) for a plain-WKB observer, both tiers. |

### Group B — output-CRS as a string (`srid`→`out_srid`, add `out_crs`)

| Fn | Tiers | Change |
|---|---|---|
| **B1 `rst_rasterize` / `rst_rasterize_agg`** | Both | `srid`→`out_srid`; add `out_crs`. Adopt Rule-2 reprojection (§3.2). |
| **B2 `rst_gridfrompoints` / `_agg`** | Both | `srid`→`out_srid`; add `out_crs`. Rule-2 reprojection. |
| **B3 `rst_dtmfromgeoms` / `_agg`** | Both | `srid`→`out_srid`; add `out_crs`. Rule-2 reprojection. |
| **B4 `rst_{h3,quadbin,bng}_rasterize_agg`** | Both | `srid`→`out_srid` (already optional, grid-native default); add `out_crs`. bng output stays 27700 (out param ignored, documented). |
| **B5a `rst_h3_gridspec`** | **Light only** | `srid`→`out_srid`; add `out_crs`. DataFrame-level helper (`df` first arg) — no heavy parity twin; the `out_crs` change is DataFrame-shaped, not a Column expression. |
| **B5b `gbx_h3_cell_bbox`** | Both | `srid`→`out_srid`; add `out_crs` (Rule-2 output CRS for the bbox). **AND relocate** (see Group E) — this is the H3-rasterize helper that gets the `rst_h3_*` surface to 100%. |

### Group C — the `rastertogrid` correctness fix (source `crs`, target intrinsic)

| Fn | Tiers | Change |
|---|---|---|
| **C1 `rst_{h3,quadbin}_rastertogrid*`** (16 fns, shared `raster_to_grid` core) | Both | **Auto-reproject** the tile to grid-native 4326 (nearest-neighbour, so pixel stats aren't interpolated) when the tile HAS a CRS that differs — mirroring what BNG already does for 27700. Add optional **`crs`** override (source role) for a CRS-less-but-known raster. **Never error:** absent + CRS-less → assume 4326 (today's behavior preserved). |
| **C2 `rst_bng_rastertogrid*`** (8 fns) | Both | Already auto-warps to 27700; add the same `crs` override for parity; confirm. |

### Group D — heavy reader `clipCrs` (parity with light)

| Fn | Tiers | Change |
|---|---|---|
| **D1 heavy GDAL/GTiff reader** | Heavy | Add `clipCrs` reader option (light has it), populating the v2 tile's `clip_crs` field via the reader path. |

### Group E — relocate `gbx_h3_cell_bbox` to GridX (Q11-3)

`gbx_h3_cell_bbox` registers under the `gbx_h3_*` (GridX) SQL prefix and has **no raster dependency** (light uses only `cellraster._h3_str` / `_reproject`, both raster-free; heavy uses only `H3.*` + OSR reprojection, no `Dataset`). It was placed in the raster package by proximity to its consumer (the H3-rasterize workflow). This spec relocates it to its architectural home while doing its CRS work (B5b):

| Tier | From | To |
|---|---|---|
| Light | `pyrx/functions.py` (`gbx_h3_cell_bbox`, `_h3_cell_bbox_udf`); the two symbols it uses from `pyrx/core/cellraster.py` | `pygx` (light GridX) — extract the raster-free `_h3_str`/`_reproject` helpers or import them without pulling raster deps |
| Heavy | `com.databricks.labs.gbx.rasterx.expressions.grid.RST_H3_CellBBox` | `com.databricks.labs.gbx.gridx.*` (GridX package) |

Update `register` wiring, `registered_functions.txt`, `function-info.json`, and binding parity in lockstep. The registered SQL name `gbx_h3_cell_bbox` is unchanged (no user-facing rename), only its package home.

---

### Group G — VizX CRS consistency (the "X" surface of raster CRS)

VizX already reads a tile's CRS and reprojects correctly for basemaps and PMTiles (static COG/raster plots pass `ds.crs` to contextily's `add_basemap(crs=…)`; dynamic vector layers `to_crs(4326)` before tiling). Two consistencies are missing versus the model this spec standardizes:

1. **No shared-helper routing.** VizX reads `ds.crs` / `src.crs` directly (`_cog.py`) rather than through the canonical CRS authority (`crs_to_canonical` / `resolve_crs`). It is a parallel CRS implementation, not the one authority.
2. **No `crs` override for a CRS-less raster.** When `ds.crs is None`, VizX silently drops the basemap (`if crs is not None`). That honors the never-error invariant (no crash) but leaves a gap: a user with a CRS-less-but-known raster can now get a correct **grid** result (Group C's `crs` override) yet still **cannot** get a correct basemap **plot**. Inconsistent escape-hatch coverage.

Changes (light `vizx` only — VizX is a light-tier visualization surface):

| Fn | File | Change |
|---|---|---|
| **G1 `plot_tile` / `plot_cog` / `plot_raster` / `plot_file` / `plot_static`** | `vizx/_cog.py`, `vizx/_raster.py`, entry points | Add an optional **`crs`** param (source role, same semantics as Group C): when the tile/raster is CRS-less, use it as the raster's CRS for basemap alignment; ignored when the raster already carries a CRS; absent + CRS-less → basemap-less (today's behavior), never errors. |
| **G2 canonical routing** | `vizx/_cog.py` | Route the CRS the plot hands to contextily through `crs_to_canonical`/`resolve_crs` so VizX shares the one authority (equivalent spellings, ESRI/WKT support) instead of a parallel read. No behavior change for a CRS-bearing raster. |

Never-error invariant preserved throughout: an absent CRS with no override degrades to a basemap-less plot (as today), never a raise. This is the recon's "Sub-spec X depends on R" work, folded in to complete the raster-CRS story end-to-end (read → operate → **visualize**).

---

### Group F — the Coordinate Reference Systems docs page (the nuance must land here)

Every decision in this spec must be **user-visible on `docs/docs/api/coordinate-reference-systems.mdx`** — per the repo's "docs are the source of truth" rule, and because these rules only become real to users if the page teaches them. The page today covers SRID-vs-CRS-string, the int-cast rule, the resolution rule (R2), and canonical form; it does **not** yet cover the two-role model, the naming standard, the per-geom/mixed-column semantics, or the never-error invariant. This spec's docs task adds, with runnable examples wired from doc tests (`docs/tests/**`, no prose-only claims):

1. **A new "Source CRS vs output CRS" section** — the two roles (§1), the naming standard table (`srid`/`crs`/`clip_crs` = source, "what my input already is"; `out_srid`/`out_crs` = target, "how to project the output"), and *why* the name tells you the role. Include the full subset lists from §6.
2. **The source-CRS precedence + per-geom fallback (§1.1)** — the EWKB-wins / plain-WKB-uses-param / neither→CRS-less table, stated as universal. Explicitly document **mixed columns** (Q12): a Column can mix EWKB and plain-WKB rows; the scalar `crs`/`srid` labels only the plain-WKB rows, per-geom; this is **not** an error. Show a mixed-column example.
3. **The never-error invariant (§1.3) + the conflict/error matrix (§3.3)** — spell out the *only* three things that raise (both source params, both output params, an explicitly-unresolvable string) and that everything else — including CRS-less inputs — degrades to a sensible assumption and never throws. This is the single most important user-facing promise; make it prominent (a `:::note`).
4. **Produce-new-raster reprojection (§3.2)** — document that `rst_rasterize`/`rst_gridfrompoints`/`rst_dtmfromgeoms` reproject the geometry from its source CRS into `out_crs`/`out_srid` before burning, so a geometry in one CRS and an output in another is correct (not garbage), and that absent `out_*` carries the geometry's source CRS (not a forced 4326).
5. **The `rastertogrid` auto-reproject + `crs` override (Group C)** — `rst_{h3,quadbin}_rastertogrid*` now reproject a differently-CRS'd raster to grid-native automatically; the `crs` override is for CRS-less-but-known rasters; a CRS-less raster with no override is assumed grid-native (never errors). Note this closes a prior silent-wrong-answer footgun.
6. **A brief performance note (§3.4)** — that CRS resolution and reprojection are cached internally (transformer reuse), so callers do not need to pre-warp or batch by CRS for performance; keep it short and non-normative (internal detail, not API).
7. **A master CRS-function cross-reference table** — a single tabular enumeration of **every CRS-touching function across all of GeoBrix**, for easy reference/cross-reference. Columns:

   | Package | Function | Tiers | CRS param(s) | Role | CRS-in behavior | Notes |
   |---|---|---|---|---|---|---|

   - **Package**: RasterX / VectorX / GridX. **Role**: `source` / `output` / `both` / `accessor`. **CRS param(s)**: `srid`, `crs`, `clip_crs`, `out_srid`, `out_crs`, or "—" (intrinsic/accessor). **CRS-in behavior**: one-liner (e.g. "reproject cutline→tile CRS", "auto-reproject raster→grid-native", "reproject geom→output before burn", "returns SRID int", "returns CRS string"). **Notes**: e.g. "bng out ignored (always 27700)", "light-only DataFrame helper".
   - **This is a LIVING MATRIX seeded here with the complete RasterX surface** (all Group A–E functions, plus the already-shipped R/R2 functions: `rst_srid`, `rst_crs`, `rst_setsrid`, `rst_setcrs`, `rst_transform`, `rst_transformcrs`). Rows for **VectorX and GridX are added by their own follow-on specs** as those functions ship — each spec owns its rows and appends them. The table's column shape and the source/output-role vocabulary are fixed here so later specs slot in consistently.
   - Group the rows by Package (RasterX rows populated; VectorX/GridX sub-headers present with a "see the VectorX / GridX CRS spec" placeholder row so the structure is visible and the deferral is explicit — this placeholder is documentation of scope, not a TODO).
8. **Cross-link** — every function's reference entry that gained a `crs`/`out_crs`/`clip_crs` param links to the relevant section here (extends the R2 per-function-link work); and every row of the master table (item 7) links to that function's reference entry. Update the RasterX subsection's function bullets to name the new params.

Voice-grep clean (no internal planning vocabulary); Docker docs build green via `gbx:docs:build` (dev-server-aware).

## 3. Behavioral details

### 3.1 Source-CRS resolution (Groups A, C)

`rst_clip`/`rst_sample`/`rst_viewshed`: the geometry's **source** is resolved by Rule 1; the **target** is the tile's own CRS (intrinsic). Reproject source→tile per §1.3. A CRS-less cutline/observer is assumed already-in-tile-CRS (no error) — this preserves the current reader precedence *except* it now (Q8) errors when an EWKB geom *and* an explicit `clip_crs` are both supplied, and (A1 fix) accepts ESRI/WKT strings, not just int-castable ones.

`rst_{h3,quadbin}_rastertogrid*`: the **source** is the tile's CRS, or the `crs` override when the tile is CRS-less; the **target** is grid-native (intrinsic). Reproject to grid-native when source is known and differs; assume grid-native when source is unknown and no override (never error).

### 3.2 Rule-2 reprojection for produce-new-raster (Group B, Q9)

Today `rst_rasterize`/`gridfrompoints`/`dtmfromgeoms` do **no** reprojection — they assume the geometry is already in the output CRS, so an EWKB-4326 geometry burned into a UTM-declared extent silently produces garbage. The fix:

1. Resolve the geometry's **source** CRS by Rule 1 (embedded SRID / none — these functions have **no** explicit source param; the only explicit param is the `out_*` target).
2. Resolve the **target** CRS from `out_srid`/`out_crs` (Rule 2). Neither provided → the output carries the geometry's source CRS (or CRS-less if source is unknown) — **not** a forced 4326.
3. If source known and target known and differ → **reproject the geometry** source→target before burning. Source unknown → assume already-in-target (no reproject, no error).

This makes Group B obey the same framework as everything else and closes the silent-garbage case.

### 3.3 Conflict & error matrix (the only throwing paths)

| Condition | Result |
|---|---|
| both `srid` & `crs` (same source pair) provided | **error** — call-level config, statically checkable |
| both `out_srid` & `out_crs` provided | **error** — call-level config |
| explicit `crs`/`out_crs`/`clip_crs` string unresolvable | **error** — R2 apply-time semantics |
| EWKB/EWKT geom + explicit source param (per-geom) | **no error** — embedded SRID wins per-geom, param ignored for that geom (Q12) |
| mixed column (some EWKB, some WKB) + scalar param | **no error** — param labels plain-WKB rows per-geom (Q12) |
| everything else (incl. all absent, CRS-less inputs) | proceed, never throw |

Only the three call-level/explicit-garbage conditions throw. Every per-geom data condition — including embedded-SRID-plus-param and mixed columns — degrades to a sensible per-geom assumption.

---

### 3.4 Performance — transformer caching in the centralized helpers (Q13, Q14)

The per-geom source-CRS check (Rule 1) is an O(1) field read (`shapely.get_srid` light / JTS `getSRID` heavy) on an already-parsed geometry — negligible against the WKB parse + reprojection that dominate these functions. It is **not** a perf concern and needs no user-facing mode; there is **no `strict`/`permissive` switch** (it would skip the cheap check while leaving the expensive one, and a permissive mode ignoring embedded SRIDs would reintroduce the silent-wrong-answer class Q12 eliminates).

The real cost is **re-creating reprojection objects per row** — a `pyproj.Transformer` (light) / GDAL `CoordinateTransformation` (heavy). The fix is a bounded, thread-safe transformer cache in the **centralized helpers**, so no hot path rebuilds these per row:

- **Light** — add `crs.get_transformer(src, dst)` in `pyrx/core/crs.py` (alongside the R2 resolver + its `lru_cache` code-sets). Returns a `pyproj.Transformer` built with **`always_xy=True`** (pin axis order — a classic silent-wrong-answer source). Also cache the resolved `CRS` object from `resolve_crs`. Route the scattered per-row transform sites through it: `cellraster.py:56`, `ops.py:171`, `tessellate.py:101/276`, and the Group-A/B/C reprojection paths.
- **Heavy** — add `SpatialRefOps.getTransformer(srcKey, dstKey)` returning a cached `CoordinateTransformation`, replacing the raw `new CoordinateTransformation(...)` at `BoundingBox.scala:25`, `RasterTessellate.scala:168/441`, and the new Group-A/B/C sites.

Cache design (identical semantics both tiers):
- **Thread-local**, NOT process-global. Executors run multiple Spark tasks per JVM; a `pyproj.Transformer` / GDAL `CoordinateTransformation` is **not thread-safe** for concurrent use (same hazard class as [[gdal-ogr-register-via-guard]]). One LRU per worker thread gives intra-thread reuse with zero cross-thread contention — no lock on the hot path.
- **Keyed by canonical CRS pair** (`src→dst`, each via `crs_to_canonical`), so `4326`, `"4326"`, `"EPSG:4326"` all hit the same entry.
- **LRU-bounded, evict oldest** (your design). Size = a named constant `_TRANSFORMER_CACHE_SIZE = 128`, justified: 120 WGS84 UTM zones (EPSG:326xx north + 327xx south) + 4326/27700/3857 + headroom → a workload touching every UTM zone plus the common CRSes never evicts. Keyed by *pair*, but the target is near-always fixed (tile CRS / grid-native), so distinct pairs ≈ distinct sources ≈ the UTM count. A pathological many-distinct-target workload degrades gracefully (rebuild the evicted transformer, as today). Constant is tunable, not a magic literal.

**Correctness is identical with or without the cache** — it's a pure speedup layered under the Q12 rules. It is an implementation-time optimization applied where a hot path warrants it, validated per the perf-review discipline ([[perf-parity-light-vs-heavy]], [[pyrx-udf-boundary-tax]]), not a premature build-out.

---

## 4. Testing (doc-tests-are-the-source; real data; Docker for heavy)

Per repo rules: tests execute real code with real assertions; no mocking pyproj/GDAL; heavy in Docker with the real ESRI:54008 MODIS fixture.

Per function, cross-tier:
1. explicit `crs`/`out_crs` string (incl. an **ESRI:54008** and a WKT) drives reprojection/stamp;
2. embedded EWKB SRID honored when no explicit source param;
3. **CRS-less input does not raise** and yields the tile-CRS / grid-native / carried-source result;
4. conflict cases (§3.3) raise the expected clear errors (both-source-params, both-out-params, unresolvable string);
5. `out_crs` wins semantics: with `out_crs` set, output SR == that CRS.

**Mixed-column test (Q12), cross-tier:** a Column mixing EWKB(4326), WKB(no SRID), EWKB(32633) rows + a scalar `crs`/`srid` param → each EWKB row uses its embedded SRID, each plain-WKB row uses the param, **no error**; assert the per-row reprojection is correct for each row class in one call.

**Transformer-cache test (Q14):** `crs.get_transformer` / `SpatialRefOps.getTransformer` returns the **same object** for equivalent CRS spellings (`4326` == `"4326"` == `"EPSG:4326"`), is thread-local (concurrent threads get independent instances — no shared-state corruption), evicts LRU beyond `_TRANSFORMER_CACHE_SIZE`, and produces `always_xy` axis order. This is a helper-level unit test, not per-function.

Group C correctness regression: a **non-4326 raster (UTM)** fed to `rst_h3_rastertogrid*` now yields correct cell assignments (guards against the silent-wrong-answer bug); a CRS-less raster still returns the 4326-assumed result with no error.

Group B correctness: an **EWKB-4326 geometry** rasterized with `out_crs="EPSG:32633"` reprojects the geometry before burning (extent coords in 32633), vs. today's garbage.

Binding parity (`gbx:test:bindings`) updated for every renamed/added param and any new registered entry; `registered_functions.txt` + `function-info.json` + Scala `override def name` + Python binding all in lockstep.

**Docs (Group F):** the CRS-page examples are backed by runnable doc tests under `docs/tests/**` (per the docs-are-the-source rule) — the mixed-column example, the produce-new-raster reprojection example, and the `rastertogrid` auto-reproject example each execute real code with assertions, so the page cannot drift from behavior. `gbx:test:docs` green; `gbx:docs:build` green.

---

## 5. Out of scope (explicitly deferred to later CRS specs)

- **VectorX CRS family** — `st_crs`, `st_setcrs`, `st_transformcrs` (string complement; NOT duplicating product `st_srid`/`st_setsrid`/`st_transform`). Absent both tiers today. **That spec appends its rows to the master CRS cross-reference table (Group F item 7) and follows the source/output-role vocabulary + `out_*` naming standard fixed here.**
- **GridX complete surface** — reproject input geom in a custom CRS into a grid's fixed SRID for `bng/quadbin/h3` `polyfill`/`tessellate`/`pointascell`; BNG cell-geometry SRID stamp; grid `crs`/`srid` accessors. **That spec appends its rows to the master CRS cross-reference table (Group F item 7); `gbx_h3_cell_bbox` is relocated to GridX by this spec (Group E) but its master-table row is seeded here.**
- **`st_asmvt_pyramid`** EPSG:4326 assumption (VectorX; B2 in recon) — deferred with VectorX.
- **`st_interpolateelevation*`** `out_*` rename — deferred with VectorX (applies the same standard there).
- **GridX CRS surface** (`bng/quadbin/h3` `polyfill`/`tessellate`/`pointascell` input-geom `crs`, BNG cell-geometry SRID stamp, grid `crs`/`srid` accessors) — deferred to the GridX spec. Note `gbx_h3_cell_bbox` lands in GridX (Group E) but its CRS work is done here because it serves the `rst_h3_*` 100% goal.

---

## 6. Naming standard summary (codify on the CRS page)

- **Output-CRS params → `out_srid` / `out_crs`.** Subset: `rst_rasterize`(+`_agg`), `rst_gridfrompoints`(+`_agg`), `rst_dtmfromgeoms`(+`_agg`), `rst_{h3,quadbin,bng}_rasterize_agg`, `rst_h3_gridspec` (light DataFrame helper), `gbx_h3_cell_bbox` (relocated to GridX).
- **Source-CRS params → `srid` / `crs` / `clip_crs`.** Subset: `rst_clip`(`clip_crs`), `rst_sample`(`crs`), `rst_viewshed`(`crs`), `rst_{h3,quadbin,bng}_rastertogrid*`(`crs`).
- No RasterX function needs both an explicit source param and an explicit output param in one signature, so `srid`/`out_srid` never collide.
