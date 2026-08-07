# Canonical Parameter Names — Orphan Tail (design)

**Date:** 2026-08-07
**Status:** ratified (this doc), pending plan
**Predecessor:** `docs/superpowers/specs/2026-08-06-canonical-param-names-design.md`
(the ratified R1–R7 / N1 / N9 rules, executed as commits `6192e82d..35711b80`
on `beta/0.5.0`, now pushed).
**Scratch input:** `prompts/refactoring/2026-08-07-param-naming-orphan-tail-followon.md`

## Goal

Ratify canonical parameter names for the 30 functions still waived in
`docs/tests-function-info/param_name_waiver.txt` — the divergences no ratified
rule covered — and drive the waiver from 30 down to 8 (the 8 remainder being
permanently-waived, documented light-arity gaps). Naming-only: no feature work.

## Scope

**In scope:** parameter-name consistency across the four name-bearing surfaces
(Scala case-class field, heavy-Python shim, light-Python binding, frozen
fixture `canonical_param_names.txt`) plus `DESCRIBE FUNCTION` accuracy (derived
from the Scala field via `extend-function-metadata.py`).

**Out of scope (naming-only bound):**
- Light-tier *arity* gaps `[B]` — the light tier missing params the heavy tier
  has. These are feature parity, not naming. They stay permanently waived and
  move to a documented-gap list (Class 4 below).
- Description population and docs↔DESCRIBE sync — that is the follow-on spec #2.
- SRID-handling prose nuances (see "Deferred notes" below).

**No public function-name changes** (same standing constraint as the predecessor
spec — `geom` stays `geom` in function names, only params are touched). **No
builder-arity changes.** SQL binds positionally, so no SQL caller is affected by
any rename.

## Governing principle

The **Scala case-class field name** (with the `Expr` suffix stripped and
camelCase → snake_case) is canonical. Sync every Python surface to it, and fix
the frozen fixture only where the fixture itself diverged from Scala.

**One exception — array-input arguments take the plural (D4).** When a single
argument is an `ArrayType` column carrying more than one tile, the param is
plural (`tiles`), naming the contents, not the argument slot. This already has
ratified precedent: `rst_frombands` names its array argument `bands`. The
`_agg` aggregator variants take one tile *per row* and stay singular `tile`.

This principle resolves most of the scratch doc's "RULING NEEDED" items
automatically, because the Scala source already carries the better name:
`x`/`y` (not `pixel_x`/`world_x`), `srid` (not `target_srid`), `geom` (not
`point`), `json_spec` (not `expression`).

## Decisions (ratified)

- **D1 — `_idx` vs `_index` in the spectral-index family → `_idx`.**
  `rst_evi`/`rst_nbr`/`rst_ndwi`/`rst_savi` already use `redIdx`/`nirIdx` (ratified,
  guard-passing). `rst_ndvi` is the lone `_index` holdout. Conform the outlier:
  rename `rst_ndvi` only.
- **D2 — `rst_clip` cutline geometry → `geom`.** Scala field is `geometryExpr`
  and fixture says `geometry`, but N1 ratified bare `geom` as the geometry-input
  convention. Conform to N1.
- **D3 — three heavy-Python shims are behind Scala; add the trailing optional.**
  `rst_clip` (`clip_crs`), `rst_sample` (`crs`), `rst_viewshed` (`crs`): the
  Scala builder + SQL surface already accept these trailing optionals; only the
  heavy-Python shim omits them. `DESCRIBE FUNCTION` reflects the Scala builder
  regardless, so the coherent fix is to make the shim faithful — add the param.
  No builder change.
- **D4 — `tile` vs `tiles` → plural for array inputs.** Rename the Scala field
  `tile → tiles` on `rst_merge`, `rst_combineavg`, `rst_mapalgebra` (single
  `ArrayType` argument holding many tiles), and update the fixture to match.
  Heavy- and light-Python are already `tiles`, so they need no change. The
  `_agg` variants stay singular.

### `geom` params honor the [E]WKB/[E]WKT contract

Verified against source: all three `geom`-named params (`rst_clip` after D2,
`rst_sample`, `bng_pointascell`) dispatch through JTS dual entry points —
`UTF8String → JTS.fromWKT`, `Array[Byte] → JTS.fromWKB` — so each accepts WKB,
WKT, EWKB, and EWKT, matching `[[geom-input-consistency-across-st]]`.

**Acceptance criterion:** a param may be named `geom` only where it dispatches
`JTS.fromWKT`/`JTS.fromWKB` on both `UTF8String` and `Array[Byte]`. All three
targeted params pass; no aspirational names are introduced.

## Resolution table (all 30 waived functions)

### Class 1 — Python-only renames (no Scala change, no fixture change)

Scala field + fixture already canonical; only the Python surfaces drifted.

| Function | Fix (Python surfaces → canonical) |
|---|---|
| `rst_rastertoworldcoord` / `x` / `y` | heavy+light `pixel_x, pixel_y` → `x, y` |
| `rst_worldtorastercoord` / `x` / `y` | heavy+light `world_x, world_y` → `x, y` |
| `rst_transform` | heavy+light `target_srid` → `srid` |
| `rst_proximity` | heavy+light `distunits` → `dist_units` |
| `rst_derivedband` / `_agg` | heavy `tile_expr, pyfunc` → `tile, python_func`; `_agg` heavy `pyfunc` → `python_func` |
| `rst_getsubdataset` | light `name` → `subset_name` |
| `bng_pointascell` | heavy `point` → `geom` (contract verified) |
| `bng_eastnorthasbng` | heavy `east, north` → `easting, northing`; light `e, n` → `easting, northing` |
| `custom_grid` | light `x_min, x_max, y_min, y_max, cell_splits, root_x, root_y, srid` → `bound_x_min, bound_x_max, bound_y_min, bound_y_max, cell_splits, root_cell_size_x, root_cell_size_y, srid` |

### Class 1b — guard parser fix, no code rename

| Function | Fix |
|---|---|
| `rst_evi` | **Not a naming divergence.** Light `rst_evi` is already canonical (`tile, red_idx, nir_idx, blue_idx, l, c1, c2, g, …`). The guard's `_find_def` (in `check-param-names.py`) does not strip `#` comments before tokenizing the arg list, so the `def rst_evi(  # noqa: E741` inline comment glues onto the capture and the parser silently drops `tile`, producing a false prefix mismatch. Fix: strip inline `#` comments in `_find_def` (the only def in any guard-read surface carrying an inline comment). After the fix, `rst_evi` leaves the waiver with zero code change. |

### Class 2 — Scala field rename + fixture + Python (D1, D2, D4)

| Function | Fix | Decision |
|---|---|---|
| `rst_ndvi` | Scala `redIndex/nirIndex` → `redIdx/nirIdx`; fixture `red_index/nir_index` → `red_idx/nir_idx`; heavy-py `red_band/nir_band` → `red_idx/nir_idx` | D1 |
| `rst_clip` | Scala `geometryExpr` → `geomExpr`; fixture `geometry` → `geom`; heavy-py `clip` → `geom` | D2 |
| `rst_merge` | Scala `tile` → `tiles`; fixture `tile` → `tiles`; Python already `tiles` | D4 |
| `rst_combineavg` | Scala `tile` → `tiles`; fixture `tile` → `tiles`; Python already `tiles` | D4 |
| `rst_mapalgebra` | Scala `tile` → `tiles` (keep `jsonSpecExpr` = `json_spec`); fixture `tile, json_spec` → `tiles, json_spec`; heavy-py `tiles, expression` → `tiles, json_spec` | D4 |

### Class 3 — heavy-Python shim behind Scala; add trailing optional (D3)

| Function | Fix |
|---|---|
| `rst_clip` | heavy-py add `clip_crs` (also gets the D2 `geom` rename) |
| `rst_sample` | heavy-py add `crs` |
| `rst_viewshed` | heavy-py add `crs` |

### Class 4 — permanently waived, documented `[B]` light-arity gaps (no work)

These 8 stay in the waiver with a documented reason. They are feature-parity
gaps, not naming divergences.

| Function | Gap |
|---|---|
| `bng_kloopexplode` | light is a LATERAL-only stub (raises `NotImplementedError`) |
| `bng_kringexplode` | light is a LATERAL-only stub |
| `bng_geomkloopexplode` | light is a LATERAL-only stub |
| `bng_geomkringexplode` | light is a LATERAL-only stub |
| `bng_tessellateexplode` | light is a LATERAL-only stub |
| `rst_histogram` | light missing `max`, `include_nodata` (arity 3 vs 5) |
| `rst_xyzpyramid` | light missing `rescale` (arity 6 vs 7) |
| `bng_tessellate` | light missing `keep_core_geom` (arity 2 vs 3) |

## The four name-bearing surfaces (per rename)

For every renamed param, update in lockstep — the same discipline as the
predecessor spec (`[[signature-change-touches-seven-surfaces]]`):

1. Scala case-class field (Class 2/D4 only) — internal rename, no arity change.
2. Heavy-Python shim `python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py`.
3. Light-Python binding `.../pyrx|pyvx|pygx/functions.py`.
4. Frozen fixture `docs/tests-function-info/canonical_param_names.txt`.

Then regenerate `function-info.json` via `gbx:docs:function-info` (derives
`usageArgs` from the Scala field) and remove the function from the waiver.
Docs `**Signature:**` lines and any `*_sql_example()` are checked for the
renamed param but are positional in SQL, so they don't gate correctness.

## Testing / verification

- `docs/scripts/check-param-names.py` (Invariant A heavy-exact / light-prefix,
  Invariant B arity) must stay green throughout (normal mode, exit 0). Class-4
  functions remain waived with their documented reasons; every other function
  (Class 1/1b/2/3) moves out of the waiver.
- `--report` mode (which ignores the waiver) goes from 52 violations down to
  exactly the 8 Class-4 `[B]` light-arity lines — every naming divergence and
  the `rst_evi` false positive are gone; the 8 remaining are the real, waived,
  documented arity gaps.
- `gbx:test:bindings` (binding parity) stays green — names only, but confirms no
  surface was dropped.
- Affected per-package pytest suites run for each rename batch (rasterx, gridx).
- Full Scala compile after any Scala field rename (Class 2/D4).

## Outcome

Waiver 30 → 8 (all Class 4, each with a documented reason). 22 functions leave
the waiver: 21 via renames (Class 1/2/3) plus `rst_evi` via the Class-1b guard
fix. `DESCRIBE FUNCTION` usageArgs become accurate for the renamed functions
(`rst_evi`'s were already correct). No public function name, no builder arity,
and no SQL caller changes.

## Deferred notes (for follow-on spec #2, recorded so they are not lost)

- **SRID-handling nuance belongs in `description`, not the param name.**
  `rst_clip`/`rst_sample` honor an embedded EWKB/EWKT SRID (it wins over the
  explicit `clip_crs`/`crs` arg); `bng_pointascell` parses geometry but uses only
  the centroid X/Y and requires EPSG:27700 coordinates (`[[BNG resolution]]`).
  Capture this per-function when descriptions are populated.
- The `_idx`-vs-`_index` and `geom`-vs-`geometry` reconciliations from the
  scratch doc are settled here (D1, D2); no further reconciliation needed.
