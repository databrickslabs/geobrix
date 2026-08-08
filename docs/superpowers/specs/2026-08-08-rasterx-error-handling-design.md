# Consistent RasterX Error Handling (both tiers) — design

**Date:** 2026-08-08
**Status:** ratified (this doc), pending plan
**Scope:** RasterX only (heavy Scala + light pyrx). VectorX and GridX error
handling are explicitly out of scope — separate future specs.

## Goal

Make every RasterX function, on degenerate input (corrupt/unreadable raster, a
failed operation), **degrade rather than raise**, and leave a diagnosable trace
wherever the return type can carry one. The behavior is **derived from the
return type**, so it is predictable per function without a per-function lookup
table. No new user-facing knob is added; heavy's existing `crashExpressions`
Spark-conf escape hatch is left untouched.

This is the RasterX slice of the queued strict-mode workstream (see
`prompts/refactoring/2026-08-05-crs-loose-ends-and-consistency-handoff.md`
Part 2.1). It absorbs exactly one of the three CRS loose ends — the
`rst_srid=0` ambiguity. The other two (VectorX garbage-bytes divergence, the
finite-nonsense out-of-domain survivor `POINT(150 -80)→EPSG:27700`) are
**VectorX geometry** problems and are deferred to a future VectorX
error-handling spec, along with the domain/extent-check design.

## Constraints & decided positions

- **No `strict`/`debug` per-call argument.** Rejected: SQL binds positionally
  (no `SupportsNamedArguments` anywhere in `src/main/scala/`), and ~12 functions
  already use argument *count* as their sole optionality discriminator, so a new
  trailing optional flag is a real hazard. Behavior is fixed, not knob-driven.
- **Heavy's `crashExpressions` flag is left as-is.**
  `ExpressionConfig.crashExpressions` reads
  `spark.databricks.labs.gbx.expressions.crash.on.error` (default `false`). It
  is the dev "blow up on error" escape hatch; this spec does not rename, remove,
  or reconcile it. A negative test confirms it still raises.
- **The light metadata carrier already exists — no schema change.** `pyrx`
  `TILE_SCHEMA` and the v2 tile schema both carry
  `metadata: map<string,string>` (`pyrx/_serde.py`). `metadata["last_error"]`
  needs no new field. NOTE: the legacy `TILE_SCHEMA` declares `raster` as
  **non-nullable**, so the empty/error tile shape (`raster=None`) must use the
  **v2 nullable-raster schema**, not the legacy one.
- **Heavy already has most of the machinery.** `RST_ErrorHandler.safeEval`
  (four overloads) already swallows exceptions and, for tile/row/generator
  return shapes, already emits an empty tile / error-tile-row with error
  metadata. This spec fixes where that machinery is *inconsistently applied*
  (accessors substitute sentinels; aggregators bypass `safeEval`), not build a
  new mechanism.
- **`_crs_col` footgun already fixed** (commits `b600c149`, `c3590bb0`) — not in
  scope here.

## The contract (return-type-derived)

| Return shape | Functions | Degrade behavior |
|---|---|---|
| **Tile struct** | `rst_clip`, `rst_transform`, `rst_setcrs`, `rst_transformcrs`, and other single-tile ops | Empty tile (`raster=NULL`, `path=NULL`) + `metadata.last_error` = reason. Heavy already does this via `safeEval`; light already has the carrier. |
| **Scalar** (Int / Double / String / Map) | `rst_width`, `rst_height`, `rst_numbands`, `rst_memsize`, `rst_srid`, `rst_scalex`, `rst_scaley`, `rst_skewx`, `rst_skewy`, `rst_upperleftx`, `rst_upperlefty`, `rst_pixelwidth`, `rst_pixelheight`, `rst_rotation`, `rst_metadata`, `rst_rastertoworldcoordx`, `rst_rastertoworldcoordy` | **NULL** — replaces the `0` / `-1` / `-1L` / `Double.NaN` / `Map.empty` sentinels. Kills the `rst_srid=0` ambiguity. No reason is carried (a scalar cannot; the upstream tile-op that produced the corrupt tile already recorded it). |
| **Aggregate tile** | `rst_combineavg_agg`, `rst_merge_agg`, `rst_derivedband_agg` | Skip the corrupt member (do **not** raise); record on the emitted aggregate tile's `metadata.last_error` that N inputs were dropped. |
| **Multiple tile rows** (generators / UDTFs) | `rst_retile`, `rst_maketiles`, `rst_tooverlappingtiles`, `rst_separatebands`, `rst_xyzpyramid`, `rst_h3_tessellate`, `rst_bng_tessellate`, `rst_quadbin_tessellate` | **Exactly one error-tile row** (`raster=NULL`, `path=NULL`, `metadata.last_error`). Light stops yielding zero rows and mirrors heavy so row counts match. |

**Empty/error-tile shape (both tiers):** `raster=NULL` AND `path=NULL` (this is
the empty discriminator — `raster=NULL` alone is the *virtual-tile* signal;
`path` must also be NULL or `open_tile` will try to stage the path),
`window`/`clip_*`/`crs`=NULL, `metadata={last_error: "<detail>", ...}`.

**`last_error` message format:** a short, stable, greppable string prefixed with
the function name: `"<RST_FnName>: <cause>"` (e.g.
`"RST_Clip: unreadable raster"`). Same token both tiers. Not a full stack trace.

## Components

### 1. Heavy accessors → NULL (~17 expressions)

The accessor template is `Option(safeEval(...)).map(...).getOrElse(<sentinel>)`
(e.g. `accessors/RST_Width.scala:47` → `0`; `RST_Height` → `-1`;
`RST_SRID.scala:50` → `0`; the geo-transform accessors → `Double.NaN`;
`RST_MetaData` → `Map.empty`). Change each `getOrElse(<sentinel>)` to yield
`null`, and set `override val nullable: Boolean = true` where not already set.
The `safeEval` swallow itself is unchanged; only the sentinel-substitution step
changes. Full sentinel list: `prompts/refactoring/2026-08-05-degrade-vs-raise-inventory.md` §4.1.

### 2. Heavy aggregators → skip corrupt member + record count

`RST_CombineAvgAgg`, `RST_MergeAgg`, `RST_DerivedBandAgg` already have
`nullable = true` and an `if (value != null)` null-skip in `update()`
(commit `f60c4e88`), but none route through `safeEval` (0 refs each) and a
**non-null-but-corrupt** member still flows unguarded into `rowToTile` and kills
the task. Wrap the per-member tile handling in a try/catch: on failure, skip the
member and increment a dropped-count. Emit the aggregate over the good members;
if any were dropped, stamp `metadata.last_error` on the output tile noting the
count. An all-corrupt / empty group returns the existing empty-buffer NULL.

### 3. Heavy tile ops & generators — confirm coverage only

Single-tile ops and generators already emit the empty-tile / one-error-row shape
via `safeEval` (`RST_ErrorHandler.scala:37,67,137-149`). No heavy code change
expected beyond verifying every RasterX tile-op/generator is actually wrapped;
any found unwrapped is brought under `safeEval`.

### 4. Light accessors → None

Where a light accessor returns a sentinel, return `None`. Mirror the heavy list
exactly so cross-tier parity holds.

### 5. Light aggregators → skip + reason

Light aggregators already return `None` for an empty/all-None group
(`pyrx/core/agg.py`). Add the per-member skip + `last_error` dropped-count to
match heavy component 2.

### 6. Light UDTFs → one error-tile row (largest lift)

Light UDTFs that currently `return` (yield nothing) on a corrupt/empty tile
(`pyrx/functions.py` — the streaming `@udtf` classes; e.g. `_RstSeparateBandsUDTF`,
retile/tessellate UDTFs) must instead **yield exactly one** error-tile dict
(`raster=None, path=None, metadata={last_error: ...}`) using the **v2
nullable-raster schema**. This is the change that makes light row-counts match
heavy's error-row behavior.

## Testing / acceptance

Cross-tier parity is the spine: every degenerate input asserts *both tiers
produce the same degrade signal*.

1. **Shared corrupt-input corpus:** (a) corrupt/truncated raster bytes;
   (b) unreadable/empty tile; (c) an aggregator group mixing valid + corrupt
   members; (d) a corrupt tile fed to a UDTF that must now yield exactly one
   error row. Real bytes, not mocks (mock only external/expensive/flaky per repo
   doctrine).
2. **Per return-shape assertions (both tiers):**
   - **Accessors:** every listed accessor returns NULL/`None` (not a sentinel).
     Explicit regression: `rst_srid` → NULL, not `0`.
   - **Tile ops:** result is the empty-tile shape (`raster` NULL, `path` NULL)
     with non-empty `metadata.last_error`.
   - **Aggregators:** a mixed group returns a valid aggregate over the good
     members, `last_error` records N dropped, and it does **not** raise.
   - **UDTFs/generators:** corrupt input yields **exactly one** error-tile row;
     assert heavy row-count == light row-count (count parity).
3. **`last_error` token check:** message contains the `<RST_FnName>:` prefix
   (stable/greppable), both tiers.
4. **Negative guard:** `crashExpressions=true` still raises (the dev escape
   hatch is intact).
5. **Where run:** heavy Scala suites + `gbx:test:python` (heavy shim) in the
   `geobrix-dev` container; light via the pyrx test dirs. All through the
   `gbx:*` palette.

## Out of scope

- **VectorX** error handling: the garbage-bytes divergence (`st_transformcrs`
  passes bad bytes through vs `st_setcrs` → None) and the finite-nonsense
  out-of-domain survivor (`POINT(150 -80)→EPSG:27700`). Both are geometry
  problems; VectorX returns bare WKB/String with no reason-carrier, a different
  contract that needs its own design. The **domain/extent check** (the
  highest-value functional item overall) belongs there.
- **GridX** error handling: `gridx/` has zero `safeEval`; BNG/Quadbin/Custom
  disagree on every input class. Needs a from-scratch decision. (The
  `BNG.parse` legible-exception fix, commit `4d73a613`, was a one-off and does
  not constitute a GridX policy.)
- Any new user-facing `strict`/`debug` argument; renaming or removing
  `crashExpressions`.
- The 3D/Z vector work and mixed-NaN-Z handling.

## Outcome

Within RasterX, both tiers behave identically on degenerate input: accessors
return NULL (no more `0`/`-1`/`NaN` sentinels, no more `rst_srid=0` ambiguity),
single-tile ops and generators emit a diagnosable empty/error tile with
`metadata.last_error`, generators/UDTFs emit exactly one error row on both tiers
(no silent zero-row divergence), and aggregators skip corrupt members while
recording the drop count instead of killing the task — all with no new knob and
the `crashExpressions` dev escape hatch preserved.
