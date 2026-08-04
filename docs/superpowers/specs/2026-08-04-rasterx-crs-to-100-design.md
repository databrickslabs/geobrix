# RasterX CRS to 100% — Design (CRS Sub-spec, follow-on to R2)

**Status:** Design (approved decisions Q1–Q10 baked in). Successor to Spec R (raster CRS-string) and Spec R2 (SRID resolution rule). Branch `branch/0.5.0`, v0.5.0 beta.

**Goal:** Close every remaining CRS gap in the **RasterX** package so that (a) every geometry-accepting raster function lets the caller declare the geometry's *source* CRS, (b) every function that projects an *output* accepts a CRS string (ESRI/WKT/PROJ4), not just an int EPSG code, and (c) `rst_{h3,quadbin}_rastertogrid*` stop silently assuming EPSG:4326 — all routed through the R2 resolver, and all honoring the invariant **absent CRS never throws**. VectorX (`st_crs`/`st_setcrs`/`st_transformcrs`) and GridX remain separate follow-on specs.

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
- `srid` and `crs` are two spellings of the **same source param**. Both provided → **error** `"provide srid OR crs, not both"`.
- The explicit source param is a fallback **for plain WKB/WKT only**. (Q8) An EWKB/EWKT geometry that *also* carries an explicit `srid`/`crs` param → **error** `"geometry already carries an SRID; do not also pass srid/crs"`.

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
| both `srid` & `crs` (same source pair) provided | error |
| both `out_srid` & `out_crs` provided | error |
| EWKB/EWKT geom + explicit `srid`/`crs` source param | error (Q8) |
| explicit `crs`/`out_crs`/`clip_crs` string unresolvable | error (R2 apply-time) |
| everything else (incl. all absent, CRS-less inputs) | proceed, never throw |

---

## 4. Testing (doc-tests-are-the-source; real data; Docker for heavy)

Per repo rules: tests execute real code with real assertions; no mocking pyproj/GDAL; heavy in Docker with the real ESRI:54008 MODIS fixture.

Per function, cross-tier:
1. explicit `crs`/`out_crs` string (incl. an **ESRI:54008** and a WKT) drives reprojection/stamp;
2. embedded EWKB SRID honored when no explicit source param;
3. **CRS-less input does not raise** and yields the tile-CRS / grid-native / carried-source result;
4. conflict cases (§3.3) raise the expected clear errors;
5. `out_crs` wins semantics: with `out_crs` set, output SR == that CRS.

Group C correctness regression: a **non-4326 raster (UTM)** fed to `rst_h3_rastertogrid*` now yields correct cell assignments (guards against the silent-wrong-answer bug); a CRS-less raster still returns the 4326-assumed result with no error.

Group B correctness: an **EWKB-4326 geometry** rasterized with `out_crs="EPSG:32633"` reprojects the geometry before burning (extent coords in 32633), vs. today's garbage.

Binding parity (`gbx:test:bindings`) updated for every renamed/added param and any new registered entry; `registered_functions.txt` + `function-info.json` + Scala `override def name` + Python binding all in lockstep.

---

## 5. Out of scope (explicitly deferred to later CRS specs)

- **VectorX CRS family** — `st_crs`, `st_setcrs`, `st_transformcrs` (string complement; NOT duplicating product `st_srid`/`st_setsrid`/`st_transform`). Absent both tiers today.
- **GridX complete surface** — reproject input geom in a custom CRS into a grid's fixed SRID for `bng/quadbin/h3` `polyfill`/`tessellate`/`pointascell`; BNG cell-geometry SRID stamp; grid `crs`/`srid` accessors.
- **`st_asmvt_pyramid`** EPSG:4326 assumption (VectorX; B2 in recon) — deferred with VectorX.
- **`st_interpolateelevation*`** `out_*` rename — deferred with VectorX (applies the same standard there).
- **GridX CRS surface** (`bng/quadbin/h3` `polyfill`/`tessellate`/`pointascell` input-geom `crs`, BNG cell-geometry SRID stamp, grid `crs`/`srid` accessors) — deferred to the GridX spec. Note `gbx_h3_cell_bbox` lands in GridX (Group E) but its CRS work is done here because it serves the `rst_h3_*` 100% goal.

---

## 6. Naming standard summary (codify on the CRS page)

- **Output-CRS params → `out_srid` / `out_crs`.** Subset: `rst_rasterize`(+`_agg`), `rst_gridfrompoints`(+`_agg`), `rst_dtmfromgeoms`(+`_agg`), `rst_{h3,quadbin,bng}_rasterize_agg`, `rst_h3_gridspec` (light DataFrame helper), `gbx_h3_cell_bbox` (relocated to GridX).
- **Source-CRS params → `srid` / `crs` / `clip_crs`.** Subset: `rst_clip`(`clip_crs`), `rst_sample`(`crs`), `rst_viewshed`(`crs`), `rst_{h3,quadbin,bng}_rastertogrid*`(`crs`).
- No RasterX function needs both an explicit source param and an explicit output param in one signature, so `srid`/`out_srid` never collide.
