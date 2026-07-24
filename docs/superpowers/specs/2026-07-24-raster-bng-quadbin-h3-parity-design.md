# Raster BNG + Quadbin functions (H3 parity) — Design

**Date:** 2026-07-24
**Status:** Approved (design), pending implementation plan
**Roadmap item:** 05x-roadmap-backlog item (2) — "raster BNG + quadbin functions (H3-style)"
**Originating request:** [databrickslabs/geobrix#49](https://github.com/databrickslabs/geobrix/issues/49) — a customer running computer-vision models on image tiles asked for Mosaic-style **BNG tessellate on rasters** (raster column in, BNG scale like `1km`/`100m`, one row per tile out). That request is satisfied by `gbx_rst_bng_tessellate` (this design's BNG tessellate); the rest of the +9 surface completes quadbin/BNG raster parity around it. Closing #49 requires the BNG tessellate to ship (Phase 1 heavy is sufficient for their classic-cluster CV workload).
**Related:** `pygx-light-gridx-design`, `h3-raster-tessellation-pedigree`, `tessellate-overlap-default-mosaic-mode`, `heavy-tier-nullable-numeric-return`; issue #59 / PR #52 (0.4.2 empty-band NULL reducers) — reconciled in §2.6

## 1. Goal & scope

Bring the **quadbin** and **BNG** discrete-grid families up to full parity with **H3** on the
raster side of RasterX. H3 is the complete reference; quadbin and BNG have gaps:

| Operation family | H3 (reference) | Quadbin (now) | BNG (now) |
|---|---|---|---|
| `rst_<grid>_rastertogrid{avg,count,max,min,median}` (raster → per-cell zonal stats, ×5) | ✅ | ✅ | ❌ |
| `rst_<grid>_tessellate` (raster → clipped per-cell chips) | ✅ | ❌ | ❌ |
| `rst_<grid>_rasterize_agg` (cells → one raster, UDAF) | ✅ | ❌ | ❌ |

**Deliverable: +9 canonical functions.**

- Quadbin: `gbx_rst_quadbin_tessellate`, `gbx_rst_quadbin_rasterize_agg` (**+2**)
- BNG: `gbx_rst_bng_rastertogrid{avg,count,max,min,median}`, `gbx_rst_bng_tessellate`,
  `gbx_rst_bng_rasterize_agg` (**+7**)

**Explicitly out of scope (YAGNI):**

- No `tessellateexplode` variants — matches H3 (whose `tessellate` *is* the generator; there is no
  separate explode). Confirmed with user.
- No new reducer statistics (`sum`, `stddev`, …). Mirror H3's exact five. Justify by parity, not
  scope-creep.
- No H3 changes. H3 is the untouched reference implementation.
- No new user-facing custom-grid raster ops.

### 1.1 Issue #49 acceptance criteria (the originating customer ask)

`gbx_rst_bng_tessellate` must satisfy all of these to close #49:

1. **Operates on a raster column** (not vector) — input is a raster tile, output is derived from it.
2. **Takes a BNG scale** as its resolution argument, accepting the string keys the customer named
   (`"1km"`, `"100m"`, …) as well as the integer indices — via `BNG.getResolution` (spec §3.4).
3. **Emits one row per tessellated tile** — a `CollectionGenerator` yielding one output row per BNG
   cell, each carrying a clipped raster tile (uniform-size chips suitable for CV model input).
4. **Does NOT inherit the GridX vector-tessellate problems** the customer hit. The raster path
   enumerates BNG cells for the raster bbox and clips per cell; it must not route through the vector
   `bng_tessellate` expression (whose inherited Mosaic bugs — mosaic#423 spurious POINT/LINESTRING
   chips, mosaic#434/#580 half-size cells — are tracked separately under `pygx-light-gridx-design`
   phase 2, not here). Verify the raster tessellate builds cell geometry directly from
   `BNG.cellIdToGeometry` and does not depend on the vector tessellate codepath.

The BNG cell id is carried in tile metadata (`RASTERX_CELL_ID`), matching `rst_h3_tessellate` — this
georeferences each chip. A first-class cell-id output column was **not** requested by #49 and is out of
scope (would be a small follow-up if the customer needs it).

## 2. Grounding facts (verified against current code, 2026-07-24)

These shape the design and correct two assumptions made during brainstorming.

### 2.1 The raster→grid seam is already clean

`RST_Quadbin_RasterToGrid` (`src/main/scala/.../rasterx/expressions/grid/RST_Quadbin_RasterToGrid.scala`)
is the reference shape: a shared object with

- `cellPixel(gt, x, y, resolution) => cellId` — the only grid-specific delegate (calls
  `Quadbin.pointToCell`); and
- generic accumulator machinery: read each band + mask, walk pixels, accumulate valid pixel
  values into `mutable.LongMap[cellId -> ArrayBuffer[Double]]`, then apply a per-family reducer
  `fAgg: ArrayBuffer[Double] => T`.

The five reducer expressions (`RST_Quadbin_RasterToGridAvg` etc.) are thin wrappers binding `fAgg`.

### 2.2 BNG cell ids are `Long` internally, `String` only at the output boundary

**This corrects the brainstorming assumption that BNG needs a String-keyed accumulator.** Verified in
`src/main/scala/.../gridx/grid/BNG.scala`:

- `BNG.pointToCellID(eastings, northings, resolution): Long` — the pixel→cell delegate returns `Long`.
- `BNG.format(id: Long): String` (line ~129) and `BNG.parse(cellID: String): Long` (line ~427) — a
  clean bijection between the internal `Long` and the user-facing `String`.
- `BNG.cellIdToGeometry(cellID: Long)`, `cellIdToCenter(cellID: Long)`, `cellIdToBoundary(cellID: Long)`
  — all operate on `Long`.
- User-facing BNG expressions render `String` at the edge: `BNG_PointAsCell.dataType = StringType`
  (does `UTF8String.fromString(BNG.format(...))` internally), `kring`/`polyfill`/`tessellate` return
  `ArrayType(StringType)`.

**Consequence:** all three raster families key the hot loop on `Long` and reuse the existing
`LongMap`/`Long`-serde machinery verbatim. BNG differs only by (a) its `cellPixel` delegate calling
`BNG.pointToCellID`, and (b) emitting `BNG.format(cellId): String` when materialising the output cell
id column (output element type `String` instead of `Long`).

### 2.3 CRS contract differs between quadbin and BNG (the one real semantic divergence)

- **Quadbin grid is Web Mercator (EPSG:3857)**, but its *API input contract is EPSG:4326 lon/lat*:
  `Quadbin.pointToCell(lon, lat, z)` takes geographic lon/lat and performs the Web-Mercator slippy-tile
  projection internally (`lonLatToTile`: `log(tan(latRad)+1/cos(latRad))`, latitude clamped to
  ±85.05°). So the *raster CRS contract* is **EPSG:4326**: the caller reprojects upstream (via
  `RST_Transform`), and `cellPixel` feeds the pixel's 4326 coordinate straight to `pointToCell`. This
  matches H3's existing contract. **No auto-reproject for quadbin.**

- **BNG has no lon/lat input path.** `BNG.pointToCellID` expects **EPSG:27700 eastings/northings** (a
  projected national grid over Great Britain). Therefore BNG raster fns **auto-reproject the input
  raster to EPSG:27700 internally** before pixel→cell math (heavy: GDAL `Warp`; light: `rasterio.warp`).
  Pixels landing outside the BNG-defined GB extent are **silently dropped** (same policy as H3/quadbin
  dropping pixels with no valid cell). This divergence is deliberate and documented as BNG-specific.

### 2.4 `rasterize_agg` is the heaviest port

`RST_H3_RasterizeAgg` is a `TypedImperativeAggregate[H3RasterizeAcc]` with a **12-arg signature**
(`cellid, value, srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad`),
a `Long`-keyed accumulator with binary serde (`[count:Int][cellId:Long, value:Double]*N`), a
`resolutionOf` helper, and a `computeGridspec` that is a port of the light-tier
`pyrx.core.cellraster.compute_gridspec`. It burns cells into one raster by pixel-centroid mapping
(inverse of `rastertogrid`), NoData `-9999.0`, last-wins overlap with a canonical fold order for
determinism.

Because BNG ids are `Long` (§2.2), the accumulator and serde stay `Long`-based for all grids. The
per-grid work is: the `resolutionOf` source (H3Core → `Quadbin.resolution` → `BNG.getResolution`), the
centroid/boundary source (`H3.cellIdToCenter`/`Boundary` → quadbin/BNG equivalents), and the
grid-relative pixel-size default. BNG additionally maps in EPSG:27700 rather than reprojecting to WGS84.

### 2.5 Light-tier reference exists

`python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py` implements the light-tier
`rastertogrid`/`cells_to_raster`/`compute_gridspec`/`snap_bounds` used by the H3 (and quadbin reducer)
light bindings. New light work mirrors this module.

### 2.6 Empty-cell / NoData semantics — reconciled with issue #59 / PR #52 (SAFETY-CRITICAL)

Issue #59 (0.4.2) changed the **per-chip value reducers** (`rst_max`/`rst_min`/`rst_avg`/`rst_median`)
to return SQL `NULL` for a zero-valid-pixel band, because those reducers are *handed* a chip (possibly
all-nodata) and previously leaked `0.0` (heavy) / `NaN` (light) into an aggregatable numeric column,
poisoning `MAX()`/`AVG()`. The two directions of the raster↔grid conversion in *this* design must each
be shown safe against that same class of skew — and they are, for **structural** reasons, not by
copying a NULL/-9999 rule blindly:

- **`rastertogrid` reducers are immune by construction, NOT by NULL-handling.** The
  `RST_*_RasterToGrid.execute` accumulator only inserts a cell when a **valid** pixel lands in it
  (`if (maskBuf(idx) != 0)`). A zero-valid-pixel cell is **never emitted** — there is no empty-cell
  element to hold a sentinel or skew an aggregate. `avg = sum/length` cannot divide by zero (the buffer
  has ≥1 value or the cell is absent); `min`/`max`/`median` operate only on non-empty buffers. This is
  categorically different from #59's per-chip reducers, which cannot decline to emit. **Requirement:**
  the BNG (and any new) `rastertogrid` family MUST preserve this build-from-valid-pixels contract —
  never emit a cell for zero valid pixels, and never substitute a `0.0`/`NaN`/sentinel for an absent
  cell. No NULL element is needed because no element exists. Verified: this holds in the current H3 and
  quadbin implementations, so #59 never applied to this family and there is no pre-#52 skew for BNG to
  inherit.

- **`rasterize_agg`'s `-9999.0` is safe because it is registered as the output band's NoData
  metadata.** Verified: the H3 path builds the output via `VectorRasterBridge.buildEmptyRaster`, which
  calls `band.SetNoDataValue(-9999.0)` then `band.Fill(-9999.0)` (`VectorRasterBridge.scala:84-85`)
  before burning cell values. The sentinel is therefore the band's *declared* NoData, not a bare magic
  number: any GeoBrix reducer reading the tile back masks those pixels out (`mask == 0`), so they never
  enter avg/count/etc.; and the value cannot reach a SQL aggregate because the output is a raster tile,
  not a numeric column. SQL `NULL` is not available for a Float64 GDAL pixel — a NoData sentinel is the
  correct and only mechanism, and is the inverse-direction analog of #59's scalar-NULL fix.
  **Requirement:** the new quadbin/BNG `rasterize_agg` expressions MUST build their output through
  `buildEmptyRaster` (or an equivalent that calls `SetNoDataValue` before `Fill`), so the sentinel is
  always band-registered NoData. Writing `-9999.0` into pixels without registering it as NoData would
  reintroduce exactly the #59 skew in raster form — this is the specific failure mode to guard against.

- **Sentinel-collision caveat (inherited, grid-independent).** `-9999.0` is the long-standing
  GDAL/mosaic NoData convention. If a user's *real* measure legitimately contains `-9999.0`, those
  pixels are masked on read-back. This is a pre-existing H3/mosaic caveat, is identical across H3,
  quadbin, and BNG (grid choice does not change it), and is out of scope to re-litigate here — noted so
  the docs carry the same caveat H3 does.

## 3. Approach — B: parallel families with an extracted shared scan

Chosen over (A) generalising the base over cell-id type `[C]` and (C) bit-packing BNG ids into `Long`.
Rationale: zero blast radius on the working H3/quadbin reference implementations; each family stays
independently readable (one-file-per-expression, matching repo convention); §2.2 removed the only
force that pushed toward a generic `[C]` (String keys). If future grids make the duplication bite,
promote to (A) then.

### 3.1 `rastertogrid` (BNG only; quadbin already complete)

- Extract the shared inner scan (band read + mask + pixel walk + `LongMap` accumulate + reducer apply)
  from `RST_Quadbin_RasterToGrid` into a small helper `RasterGridScan.execute(ds, resolution,
  pointToCell, fAgg)` parameterized by the `pointToCell` delegate. Refactor `RST_Quadbin_RasterToGrid`
  (and, only if trivially safe, `RST_H3_RasterToGrid`) to call it — but **not required**; the helper
  can be introduced fresh and adopted by BNG alone if touching the reference feels risky. Decision
  deferred to the implementation plan; parity of behaviour is the invariant.
- Add `RST_BNG_RasterToGrid` object: `cellPixel` warps the raster to 27700 (once, at `eval`), then
  computes each pixel's easting/northing under the warped geotransform and calls `BNG.pointToCellID`.
  Output element renders `BNG.format(cellId): String` (element schema `(cellId STRING, measure DOUBLE)`
  vs quadbin/H3 `(cellId LONG, measure DOUBLE)`).
- Add five reducer expressions `RST_BNG_RasterToGrid{Avg,Count,Max,Min,Median}` binding `fAgg`,
  cloned from the quadbin reducers.
- **Empty-cell semantics:** preserve the build-from-valid-pixels contract of §2.6 — a cell is emitted
  only when ≥1 valid pixel lands in it; zero-valid-pixel cells are never emitted (no sentinel, no NULL
  element). This family is structurally immune to the issue #59 skew; do not add #59-style NULL
  handling to it (there is no empty element to null).

### 3.2 `tessellate` (quadbin + BNG)

- Add `tessellateQuadbinIter` and `tessellateBngIter` to
  `src/main/scala/.../rasterx/operations/RasterTessellate.scala`, mirroring `tessellateH3Iter`.
- Add `RST_Quadbin_Tessellate` and `RST_BNG_Tessellate` generator expressions cloned from
  `RST_H3_Tessellate` (`CollectionGenerator`, `elementSchema = StructField("tile", tileType)`,
  2-or-3-arg builder with default `mode="covering"`).
- **Mode contract mirrors H3 exactly:** `"covering"` (default) keeps every cell whose polygon overlaps
  the raster bbox (chips may share pixels); `"centroid"` single-assigns each valid pixel to the cell
  containing its centroid (chips partition valid pixels). `RasterTessellate.Modes` gates the value.
- Square cells (quadbin, BNG) make both modes cheaper than H3's hexagons; no algorithmic change.
- BNG tessellate warps to 27700 first (§2.3); cells are BNG `String` ids at the boundary.

### 3.3 `rasterize_agg` (quadbin + BNG)

- Add `RST_Quadbin_RasterizeAgg` and `RST_BNG_RasterizeAgg`, cloned from `RST_H3_RasterizeAgg`, reusing
  the `Long`-keyed accumulator + binary serde shape (§2.4). Per-grid substitutions:
  - `resolutionOf`: `Quadbin.resolution(cell)` / `BNG.getResolution(...)` in place of
    `H3Core.h3GetResolution`.
  - centroid / boundary sample points for `computeGridspec`: quadbin `centroid`/bbox helpers,
    BNG `cellIdToCenter`/`cellIdToBoundary`.
  - grid-relative default `pixel_size`: quadbin from zoom (Web-Mercator tile edge), BNG from resolution
    (metre edge: 1=100km … 6=1m).
  - projection: quadbin maps pixel centre → WGS84 → cell (as H3 does); BNG maps pixel centre in
    EPSG:27700 → cell (no WGS84 hop).
  - **cellid input type:** H3/quadbin accept `LONG`/`INT`; BNG accepts `STRING` (parse via
    `BNG.parse` to the internal `Long` on `update`), keeping the accumulator `Long`-keyed.
- NoData `-9999.0`, last-wins overlap, canonical fold order — unchanged from H3. **Per §2.6, the
  output MUST be built through `VectorRasterBridge.buildEmptyRaster` (or an equivalent that calls
  `SetNoDataValue` before `Fill`)** so `-9999.0` is the band's registered NoData, not a bare pixel
  value — writing the sentinel without registering it reintroduces the #59 skew in raster form.

### 3.4 BNG resolution argument

Raster BNG fns accept the **same resolution contract as the vector BNG fns**: integer indices
±1..±6 (1=100km … 6=1m; negatives = quadrants) or string keys from `BNG.resolutionMap`
(`"1km"`, `"100m"`, …), via `BNG.getResolution`. Never metres-as-Int. (Confirmed with user.)

## 4. Tiers & phased delivery

Both tiers, **heavy-first**, because light-tier BNG depends on the not-yet-implemented pygx BNG codec
port (`pygx-light-gridx-design` phase 2).

- **Phase 1 — Heavy (Scala/GDAL): all 9.** Unblocked now. This phase alone satisfies roadmap item (2)
  on classic compute and can ship independently.
- **Phase 2 — Light quadbin (`pyrx`/`pygx`): quadbin tessellate + rasterize_agg.** Unblocked (pygx
  quadbin phase 1 complete). Mirrors `cellraster.py`.
- **Phase 3 — Light BNG: all 7.** **Gated behind pygx BNG phase 2** (the `BNG.scala` → Python codec
  port). Does not start until that codec lands. Spec records the dependency explicitly.

Each phase updates the tier badges in `execution-tiers.mdx` as it lands (heavy-only → both).

## 5. Testing & parity

- **Scala unit + execute tests** per new expression, mirroring `RST_Quadbin_RasterToGridTest`,
  `RST_H3IntegrationTest`, `RST_GridEvalTest`, `RST_GridExecuteTest`. Tests execute real GDAL on real
  sample rasters (no mocking Spark/GDAL), per repo convention.
- **BNG reproject correctness:** a fixture raster in EPSG:3857 (or 4326) and the same raster
  pre-warped to EPSG:27700 must yield **identical** cell assignments and measures (within tolerance) —
  proves the internal auto-warp matches an explicit upstream warp.
- **Cross-tier parity (Phases 2–3):** exact cell-set match + measure within tolerance (1e-6 geometry
  / issue-59 numeric tolerance), per the pygx exact-parity bar. `rasterize_agg` output raster compared
  byte-band-wise within NoData-aware tolerance.
- **`rasterize_agg` round-trip:** `rastertogrid` then `rasterize_agg` on the same cell set recovers the
  per-cell values (inverse-operation invariant), matching the H3 round-trip test.
- **Empty-cell / NoData regression (§2.6):** (a) a `rastertogrid` cell covering only nodata pixels is
  **absent** from the output array (not emitted with `0.0`/`NaN`/NULL); (b) a `rasterize_agg` output
  tile reports `-9999.0` as its band NoData metadata (`GetNoDataValue`), and feeding that tile into a
  `rastertogrid` reducer excludes the filled pixels — proving the sentinel is masked, not aggregated.
  This is the direct guard against re-skewing the #59 class of bug.
- **Doc tests** (`docs/tests/python|scala`) provide the `*_sql_example()` used by
  `generate-function-info.py`; they execute real code with real assertions on
  `/Volumes/main/geobrix_samples/...` and run only in Docker via `gbx:test:*-docs`.

## 6. Surfaces to update (per repo conventions)

- `docs/tests-function-info/registered_functions.txt` — **+9** entries.
- Scala registration in `src/main/scala/.../rasterx/functions.scala` — `rd.register(...)` for each new
  expression + the Scala `functions` column wrappers (2- and 3-arg where applicable).
- Python bindings: `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` (heavy call_function
  wrappers) + `pyrx` light implementations (Phases 2–3).
- `function-info.json` via `gbx:docs:function-info` — every new function needs a non-empty
  `*_sql_example()` (no placeholders; binding-parity test enforces this).
- Docs: `raster-functions.mdx` (new function reference), `execution-tiers.mdx` (tier badges, phased),
  `performance.mdx` (add to the existing execution-shape families: `rastertogrid` reducers,
  tessellate generator, rasterize UDAF — these join existing families, so classify not invent per
  `performance-doc-update-on-new-function`), `benchmarking.mdx` if the bench harness gains grid flags.
- README badge: RasterX **108 → 117**, Functions total **156 → 165** (see README badge comment for the
  derivation; run `gbx:test:bindings` to confirm parity).
- Binding-parity: `gbx:test:bindings` must pass (Scala `override def name` + Python `functions.py` +
  `function-info.json` all present for each of the 9).
- **Benchmark harness (in scope):** register all 9 in `BenchDispatch` (heavy-tier bench registry)
  so the existing `gbx:bench:*` harness discovers them — reducers + tessellate as `DGGS` shape, the
  two `rasterize_agg` UDAFs routed through the grid-aggregate branch that today holds
  `rst_h3_rasterize_agg`. Benchmarked on the **same 20-node cluster config as every other function**
  (`notebooks/tests/databricks_cluster_config.env`, read by `gbx:bench:cluster` via `CLUSTER_ID`) — no
  new cluster spec is authored. Results recorded in `benchmarking.mdx` (`bench-changes-update-docs`).
  Note BNG timings include the internal EPSG:27700 reproject (unlike the 4326-native H3/quadbin), so
  like-for-like comparison should account for the warp. The actual cluster run is user-gated (shared
  cluster).

## 7. Risks & mitigations

- **Touching the H3/quadbin reference during scan extraction (§3.1)** could regress working reducers.
  *Mitigation:* extraction is optional; BNG can adopt a fresh helper without refactoring the
  reference. Behaviour parity (existing reducer tests stay green) is the gate.
- **BNG auto-warp resampling error** could shift borderline pixels vs. an upstream warp.
  *Mitigation:* the reproject-correctness test (§5) pins this; use nearest-neighbour resampling for
  categorical fidelity of cell assignment (cell math is on pixel centres, so warp method affects only
  which cell a centre lands in — nearest is the honest default).
- **`rasterize_agg` per-grid drift** (resolution/centroid/gridspec) is the largest new surface.
  *Mitigation:* round-trip + cross-tier tests; clone structure verbatim and substitute only the four
  documented per-grid points (§3.3).
- **GDAL thread-safety:** any new GDAL/OGR registration goes through the synchronized `GDALManager`
  guards, never raw per-task `AllRegister` (`gdal-ogr-register-via-guard`). The auto-warp path must use
  the guarded init.

## 8. Open questions

None blocking. Implementation-plan-time decision: whether §3.1 extracts the shared scan and refactors
the reference reducers, or introduces the helper for BNG only. Both preserve the behavioural invariant;
the plan picks based on how safe the refactor looks against the existing reducer test suite.
