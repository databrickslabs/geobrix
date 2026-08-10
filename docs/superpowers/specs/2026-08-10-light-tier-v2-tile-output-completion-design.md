# Complete light-tier v2-tile output — design

**Date:** 2026-08-10
**Branch:** `beta/0.5.0`
**Status:** design approved (user), ready for planning
**Related:** finishes the v2 arc — `2026-08-01-functions-virtual-aware-design.md` (the contract),
`2026-08-02-heavy-tier-v2-tiles-design.md` (heavy, already landed), `2026-08-03-light-through-finalize-design.md`.
Memory: [[light-through-finalize-spec]], [[rein-in-migration-test-bloat]], [[light-agg-struct-return-convention]],
[[light-tier-no-jar]], [[describe-function-is-heavy-only]].

## Problem & contract

**Ratified contract** (`functions-virtual-aware` spec + `pyrx/_serde.py` header): every tile-returning
function, BOTH tiers, returns the **8-field v2 tile struct**
`{cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata}`. The struct is identical whether
the tile is **virtual** (`raster` NULL, `path`+`window` set) or **materialized** (`raster` bytes present,
provenance fields NULL) — a tile moves between states WITHOUT changing shape. The lightweight tier defaults
to **virtual** in the common path (named readers emit virtual by default; reference/passthrough ops stay
virtual; pixel-producing ops materialize; `virtualize_dir`/`materialize` force either way). See
`docs/docs/api/virtual-tiles.mdx` for the canonical model.

**Defect (light-tier only; heavy is correct).** The heavy tier's `RST_ExpressionUtil.v2TileType` is the
8-field schema and every heavy tile expression returns it — DONE. In the **light** tier the v2 migration
was started and abandoned mid-catalog: only 5 default UDFs (`_setsrid_udf`, `_init_nodata_udf`,
`_setcrs_udf`, `_transformcrs_udf`, `_band_udf`) emit v2. The other **~35 default `_*_udf`s are still
declared `@f.udf(_serde.TILE_SCHEMA)`** (the legacy 3-field `{cellid, raster, metadata}`) and return
`_serde.build_tile(...)`. So `rst_clip("tile", …)` and its SQL registration emit a 3-field struct today.
This violates the 0.5.0 guarantee (verified live: `rst_clip`→3 fields, `rst_setsrid`→8 fields) and blocks
the RasterX tabbed-docs standardization (which shows a v2-tile output for every tile-returning function).

This is **non-negotiable for 0.5.0**: only v2 tiles are output, both tiers.

## The fix (mechanical; already-supported pattern; op bodies untouched)

Per botched default UDF, a two-line change — the operation body is NOT modified:

1. Decorator: `@f.udf(_serde.TILE_SCHEMA)` → `@f.udf(V2_TILE_SCHEMA)`.
2. Return: `return _serde.build_tile(new_bytes, drv, cellid)` →
   `return VirtualTile.from_v1(cellid, new_bytes, {<driver metadata>}).to_row()`.

`VirtualTile.from_v1(cellid, raster, metadata).to_row()` (`pyrx/core/virtual_tile.py:86`) is purpose-built
for exactly this "widen to v2-materialized, all provenance fields NULL" case, and is lossless (open_tile's
raster-precedence path treats it identically). Each `_*_v2_udf` sibling already proves the 8-field pattern
in production. The default (non-force-output) path now emits materialized v2 instead of legacy v1.

## Scope (Q1=all, no exceptions)

**Convert every light-tier tile-returning UDF to 8-field v2 — 100%, no exceptions.** This includes the
sibling-less ones that have no `_v2_udf` today and must be converted directly (they use `from_v1().to_row()`
identically):

- `_getsubdataset_udf`, `_fromcontent_udf`, `_fromfile_udf` (constructors),
- `_rasterize_udf`, `_gridfrompoints_udf`, `_dtmfromgeoms_udf` (generators),
- the `_udf` (non-v2) halves of `_merge_udf`/`_combineavg_udf`/`_frombands_udf`,
- the full set with existing v2 siblings: `resample`/`resample_to_size`/`resample_to_res`, `transform`,
  `to_webmercator`, `clip`, `update_type`, `asformat`, `buildoverviews`, `proximity`, `viewshed`,
  `cog_convert`, `threshold`, `filter`, `convolve`, `mapalgebra`, `index`/`ndvi`/`ndwi`/`nbr`/`savi`/`evi`,
  `fillnodata`, `slope`/`aspect`/`hillshade`/`tri`/`tpi`/`roughness`/`color_relief`, `derivedband`, `as_tile`.

Authoritative list = every `@f.udf(_serde.TILE_SCHEMA)` declaration in `pyrx/functions.py` (the migration
executes off a fresh grep of that decorator, not this prose list, so nothing is missed).

**Aggregators:** the light grouped-agg family returns BINARY by convention ([[light-agg-struct-return-convention]] —
pandas_udf can't return StructType in a grouped agg). Those are NOT tile-struct UDFs and are OUT of this
fix; verify they aren't double-touched. Only scalar tile-returning `@f.udf(...TILE_SCHEMA)` sites are in scope.

## SQL registration (Q2=yes, align to heavy)

The light UDFs are registered as SQL functions (`"gbx_rst_clip": _clip_udf`, the `_SQL_FUNCTIONS`/register
map in `pyrx/functions.py`). Widening the UDF schema widens the SQL output struct 3→8. This is **desired
alignment**: heavy SQL already returns 8-field v2, so light SQL now matches it. Acceptable breaking change
for 0.5.0 beta. No new registered names; no arity change; the registration map just points at UDFs that now
carry `V2_TILE_SCHEMA`.

## Delete v1 (Section 2 = delete)

Once no default UDF references the legacy output path:

- **Delete `_serde.build_tile()`** and the legacy **`_serde.TILE_SCHEMA`** output constant — nothing should
  *emit* legacy v1 anymore.
- **Keep `VirtualTile.from_v1(...)`** — readers/`_open` still ACCEPT a v1 tile on INPUT (the "v1 supported
  indefinitely on input" contract); `from_v1` is the input-widening path and stays.
- Grep-guard: after deletion, `grep -rn "TILE_SCHEMA\b\|build_tile" pyrx/` returns only `V2_TILE_SCHEMA`,
  `from_v1`, and comments. A lingering import is a build break, caught immediately.

## Output shape in docs (Section 5, CORRECTED)

We do **NOT** switch the docs to show a real materialized row. The representative tabbed-docs output stays
the single illustrative v2-tile string, all four tabs, as ratified:

```
{cellid, raster=<raster bytes>, path=<virtual path>, ..., metadata}
```

Rationale (corrected from an earlier wrong claim): virtual is the lightweight-tier DEFAULT across named
readers AND the passthrough-op rules — not merely "reader-produced". The v2 struct is identical for virtual
and materialized, so this string represents the shared v2 tile the lightweight tier defaults to producing.
It is honest BECAUSE it is plainly illustrative ("representative of a v2 tile"), not a literal row dump —
the user's standing ruling: *output need not be real, just representative of a v2 tile; don't get fancy
between heavy and light tabs.* This code fix is what makes that string **structurally** truthful: every
light tile output now genuinely has the 8 fields including `path`. There is NO per-tier output note and NO
"path only for reader tiles" caveat (that earlier claim is struck). The docs standardization resumes
unchanged after this lands.

## Testing — data-driven, anti-bloat ([[rein-in-migration-test-bloat]]; Q3=1 + standing guidance)

Do NOT add ~35 per-function "is-it-v1" tests. Two data-driven guards that cannot go stale, plus ad-hoc
migration verification:

- **G1 — structural invariant (standing guard).** ONE parametrized test that enumerates the ACTUAL light
  SQL registration map (`_SQL_FUNCTIONS`) / the tile-returning UDF set, and asserts each member's output
  schema **is** `V2_TILE_SCHEMA` (8 fields, exact field names + order). Sourced from the registry itself, so
  new functions are auto-covered and a one-off regression fails here. This is the definition-of-done: G1
  green == migration complete.
- **G2 — cross-tier parity (standing guard).** ONE parametrized test asserting light and heavy emit the
  byte-identical 8-field schema for a representative op set (clip / resample / a terrain op / a constructor).
  Locks the two tiers together so this can't silently diverge a third time.
- **Migration verification = AD-HOC, not committed.** Drive the port with a throwaway probe (the
  `.show()`/`asDict().keys()` script already used), not permanent per-function tests. Op-body correctness is
  ALREADY covered by existing behavior tests (bodies unchanged) — add only G1/G2.
- **Consolidate on the way in.** Fold any existing per-function tile-schema assertions into G1 and DELETE the
  redundant copies. Net standing test count should DROP or hold, not grow by 35.

## Risk & out of scope

- **Risk — breaking SQL output width.** Downstream positional reads of the 3-field struct break; acceptable
  per 0.5.0 beta and it aligns light↔heavy (net less surprise). Release-note it under breaking changes.
- **Out of scope:** heavy tier (already v2); the RasterX tabbed-docs standardization (resumes after this);
  the reader/`virtualize_dir` behavior (unchanged — this only fixes the DEFAULT output SCHEMA of ops that
  were emitting legacy). No op semantics change.
- **`function-info.json` / DESCRIBE FUNCTION:** heavy-only ([[describe-function-is-heavy-only]]); light
  schema widening doesn't touch it. Regenerate only if a doc example changes (it doesn't here).

## Success criteria

1. `grep -c "@f.udf(_serde.TILE_SCHEMA)" pyrx/functions.py` == 0 (all tile UDFs on `V2_TILE_SCHEMA`).
2. `_serde.build_tile` and legacy `TILE_SCHEMA` constant deleted; no references remain.
3. Live probe: every previously-legacy light op (`rst_clip`, `rst_resample`, `rst_asformat`, a terrain op, a
   constructor, `getsubdataset`) returns an 8-field v2 row; materialized rows have `raster` set + provenance
   NULL.
4. G1 green (all registered tile-returning light fns emit `V2_TILE_SCHEMA`); G2 green (light≡heavy schema).
5. Net standing-test count did not grow by per-function copies; redundant per-fn schema assertions folded
   into G1.
6. Affected light suites pass (`python/geobrix/test/pyrx/`, `test/rasterx/` shim, `test/ds/` writers); no
   new failures beyond the tracked pre-existing set.
7. Docs standardization can resume: the representative `<virtual path>` v2-tile string is now structurally
   truthful for every light tile output.
