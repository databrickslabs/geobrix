# Heavy-tier v2 tile handling — design

**Date:** 2026-08-02
**Branch:** `feature/large-raster-reader`
**Status:** design approved; planned as a two-phase plan (`docs/superpowers/plans/2026-08-02-heavy-tier-v2-tiles.md`).
**Related:** the v2 virtual-tile arc — Inc 1 (`2026-07-31-v2-virtual-tile-reader-design.md`) through Inc 5 (`2026-08-02-inc5-transform-combinators-design.md`); [[light-virtual-tiling-by-reference]]. This is the increment **after Inc 5 and before the capstone** (Virtual Tiles page + hero diagram), per the user's sequencing — the capstone waits on this so it can show the light↔heavy bridge as first-class.

> **Scope refinements after this spec was drafted (user decisions 2026-08-02), reflected in the plan:**
> 1. **Two phases.** Phase 1 is a behavior-preserving TEARDOWN (no tile-shape change): fully remove the dead path-tile + checkpoint machinery and collapse the `evalPath`/`evalBinary` split. Phase 2 is the v2 functional change described below. The dead-code surface is larger than "delete two branches" — it includes `CheckpointManager`/`CheckpointCleaner`/`CleanupListener`, `RasterDriver.write`, `ExpressionConfig.useCheckpoint`/`getRasterCheckpointDir` + both checkpoint config keys, the `GDALManager` checkpoint vars, `rstInvoke`, and all 138 `evalPath` methods across ~103 files.
> 2. **`eval` collapse.** With path-tiles gone, `evalBinary` → a single `eval` per expression; `rstInvoke` deleted.
> 3. **Deserialize by layout, not by a named-field carrier.** Since `InternalRow` carries no schema, the chokepoint resolves v1 (3-field) vs v2 (8-field) by `row.numFields()` — the concrete realization of "by name" given exactly two canonical layouts. Where this spec says `tileFromRow`, the plan keeps the existing `rowToTile`/`rowToDS` names (signatures preserved so ~80 callers stay untouched) and makes them layout-aware.
> 4. **Python heavy tier is a pure passthrough** — Phase 1 touches no Python; Phase 2 fixes only three stale `rasterx/functions.py` docstrings (`source`→`cellid`, v2 fields) and the `pyrx/_serde.py` shared-schema note.

## Problem & motivation

The light tier now emits a **v2 tile struct** by default:

```
struct<cellid:bigint(nn), raster:binary(nullable), path:string(nullable),
       window:struct<col_off,row_off,width,height>(nullable), clip_polygon:binary(nullable),
       clip_crs:string(nullable), crs:string(nullable), metadata:map<string,string>(nullable)>
```

A **virtual** tile has `raster=NULL` + `path` set (bytes read lazily from `/Volumes` at use, light-tier only). A **materialized** tile has `raster` bytes present; its `window`/`clip_polygon`/`clip_crs`/`crs` are the **pedigree of what is already in the raster payload** (reference, not instruction).

The heavy (Scala/rasterx) tier does not understand v2. It reads tiles **positionally** — `row.getMap(2)` for metadata, `array.getStruct(i, 3)` for array inputs — against a fixed **v1 3-field** struct `(cellid, raster, metadata)`. Because v2 moved `metadata` from position 2 to position 7 and inserted `path` at position 2, **any v2 tile fed to a heavy `rst_*` crashes at runtime** with a `ClassCastException` (`getMap(2)` sees the `StringType path`). There is no analysis-time guard; the failure is an opaque runtime crash. So a light→heavy handoff of the light reader's default output is broken today, and a virtual tile reaching heavy fails with an unhelpful error instead of a clear "materialize first."

## Requirements (user-pinned)

1. **Virtual tiles are a lightweight-tier capability** — the light tier generates them and materializes them. Docs and code must make this explicit.
2. **Heavy operates only on materialized tiles.**
3. **Heavy accepts BOTH v1 and v2 tile inputs**, but **only ever emits v2 materialized tiles** (raster bytes present).
4. **A virtual tile reaching heavy raises a clear, useful, actionable error** (materialize in light first).
5. Pedigree rule (identical to light): on a materialized tile, `window`/`clip_polygon`/`clip_crs`/`crs` are the pedigree of what is in the raster payload — carried through when the op preserves/establishes them, null otherwise; the self-describing bytes remain the source of truth.
6. **Centralized helpers** (mirroring light's `open_tile`/`shape_output` chokepoints) that all heavy `rst_*` expressions and readers route through, for aligned consistency — not per-function positional reads.

## The three "path" notions — as-is vs to-be

Heavy today conflates three distinct "path" concepts. This design deprecates two of them.

| Notion | As-is | To-be |
|---|---|---|
| **#1 v1 `raster`-field-as-String-path (read).** The tile's field 1 (`raster`) is polymorphic — `BinaryType` (bytes) OR `StringType` (a filesystem path). `RasterSerializationUtil.rowToTile`/`rowToDS` (`:30,46`) do `getString(1)` → `RasterDriver.read(path)` → `copyToLocal` (`/Volumes` → node-local temp → GDAL open). The "path instead of raster" experiment. | **DEPRECATED / removed.** Heavy's deserialize accepts a v1 tile only as `(cellid, raster:Binary, metadata)`. A v1 tile whose `raster` field is a `String` path → **clear error** ("path-tiles are no longer supported; materialize to bytes in the lightweight tier"). No copy-to-local raster open remains. |
| **#2 checkpoint `StringType` output.** Output half of #1: when a heavy expr's output dataType is `StringType`, `tileToRow` (`:72-101`) writes the result to `CheckpointManager.getCheckpointPath/…/raster_<uuid>.<ext>` and emits a path-tile (field 1 = checkpoint path, `metadata["path"]=outPath`). | **REMOVED outright** (user ruling 2026-08-02). Heavy always emits v2 **materialized** (raster=bytes). The `StringType`/checkpoint branch of `tileToRow` is deleted. Task 0 confirms this branch is not wired to a default/enabled code path before deletion (safety check, not redesign); if it is load-bearing, surface it rather than silently break it. |
| **#3 `metadata["path"]` (map entry).** Informational provenance string in the metadata map (`:87`). Nothing opens a raster from it. | **KEPT unchanged.** Informational provenance only; coexists with the raster bytes. |
| **v2 `path` field.** n/a (heavy doesn't know v2). | **The only allowed path.** `raster=null` + `path` set = a **virtual tile** → heavy **refuses** with the guard error. Heavy never lazily reads a path. |

**Net:** heavy stops treating a path as a way to obtain raster bytes entirely. Bytes come only from the `raster` binary field. A path exists only as v2's virtual-tile marker, which heavy rejects. This also removes the `StringType | BinaryType` polymorphism from the tile's raster slot.

## Architecture — two centralized chokepoints

Mirror light's chokepoint model in `RasterSerializationUtil` (already the de-facto center), and route every heavy `rst_*` + reader through them. Today's positional reads are scattered across expressions (`RST_ErrorHandler`, `RST_FromBandsAgg`, `RST_MergeAgg`, `arrayToTiles`, plus `rowToTile`/`rowToDS`); centralizing removes the per-function fragility and gives one deserialize point + one serialize point.

### Deserialize chokepoint — `tileFromRow` (by name, not ordinal)

- Reads fields **by name** from the input tile's `StructType`, so it transparently accepts **v1** `(cellid, raster, metadata)` and **v2** (8-field). Name-based lookup is resilient to field-position changes (the exact bug that broke v2).
- **Field 1 (`raster`) must be `BinaryType`.** A v1 `StringType` raster field (path-tile, notion #1) → clear deprecation error.
- **The single home of the virtual-tile guard** (below).
- Returns an internal carrier (e.g. `HeavyTile`) with: `cellid`, open-able raster bytes, `metadata`, and the v2 pedigree fields (`window`/`clip_polygon`/`clip_crs`/`crs`) read through when present (null for a v1 input). Callers open the dataset via the existing `RasterDriver.readFromBytes`.
- Array inputs: replace `array.getStruct(i, 3)` with a name-based per-element `tileFromRow` (the hardcoded `3` is itself a v2 bug — it truncates an 8-field row).

### Virtual-tile guard (clear, useful error)

- A tile is **virtual** iff `raster IS NULL AND path IS NOT NULL` (v2 only; v1 has no `path` field).
- `tileFromRow` throws immediately (heavy cannot lazily read `/Volumes` from the JVM — recon confirmed only a copy-then-open bridge exists, which is being deprecated). Message is actionable, e.g.:
  > `Heavyweight rst_* received a VIRTUAL tile (raster is null, path=<path>). The heavyweight tier operates only on materialized tiles. Materialize it in the lightweight tier first — call the lightweight rst_* with materialize=True, or write it out and read it back — then pass the result to the heavyweight function. See Execution Tiers → light↔heavy bridge.`
- Thrown once, centrally — every `rst_*` inherits it. A v2 tile with `raster` present is materialized → proceeds; pedigree fields are read as reference.

### Serialize chokepoint — `tileToRow` (always v2 materialized)

- Always emits the **v2 8-field struct**, `raster`=bytes.
- Pedigree fields set per the reference rule: `cellid`/`metadata` as today; `window`/`clip_polygon`/`clip_crs`/`crs` carried through from the input tile's pedigree when the op preserves it, stamped when the op newly establishes it (e.g. a heavy clip records the clip polygon it applied), null otherwise. Null pedigree is safe because the bytes are self-describing (same as light's pixel-producers).
- The `StringType`/checkpoint branch is deleted.
- Every heavy expression's `dataType` override returns the v2 struct via a shared `RST_ExpressionUtil.tileDataTypeV2` (replacing the 3-field `tileDataType`). Result: heavy output is structurally identical to what the light reader/functions emit — a heavy result flows back into light and vice-versa with no shape mismatch.

## Scope

### In scope
- Task 0: confirm the `CheckpointManager`/`StringType` output branch is not on a default/enabled path (safety check) before deletion.
- `tileFromRow` by-name deserialize (accepts v1 + v2; binary-raster-only; array path included).
- Central virtual-tile guard with the actionable message.
- `tileToRow` always emits v2 materialized + pedigree carry-through; delete the StringType/checkpoint output branch.
- `tileDataTypeV2` shared output schema; every heavy `rst_*` `dataType` returns v2.
- Route all positional-read sites (`RST_ErrorHandler`, `RST_FromBandsAgg`, `RST_MergeAgg`, `arrayToTiles`, `rowToTile`/`rowToDS`) through the chokepoints.
- Docs: execution-tiers.mdx + `_virtual-tile-overrides.mdx` — virtual=light-only; heavy accepts v1+v2 materialized, emits v2; virtual→guard error with remedy. Voice-clean.
- Scala tests (below).

### Explicitly NOT in scope (deferred)
- Any replacement for checkpoint-to-disk (if a large-tile disk escape hatch is later wanted, that is a separate feature, not this path-tile mechanism).
- Heavy lazily reading `/Volumes` (explicitly rejected — that is the light-tier virtual-tile contract).
- The capstone (Virtual Tiles page + hero diagram) — the next increment after this.
- Light-tier changes (light already emits/consumes v2).

## Error handling & edge cases
- **v1 String path-tile input** → deprecation error (notion #1), distinct from the virtual-tile guard message.
- **v2 virtual tile** → virtual-tile guard error.
- **v2 materialized tile** → proceeds; pedigree read as reference.
- **v1 binary tile** → proceeds (back-compat); output is upgraded to v2 (pedigree null).
- **Array-of-tiles** with a mix → each element runs through `tileFromRow`; a virtual element raises the guard.
- **Analysis-time vs runtime:** prefer failing at analysis (`checkInputDataTypes`/schema inspection) where a virtual/path-tile is statically detectable; otherwise the central runtime guard catches it with the actionable message (no more opaque `ClassCastException`).

## Testing (Scala)
- v1 binary tile input still works (back-compat), output is v2.
- v2 materialized tile input works and round-trips; pedigree carried through.
- Virtual tile (raster null + path) input raises the guard error containing the materialize-first remedy text.
- v1 String path-tile input raises the deprecation error.
- Heavy output is v2 and re-consumable by heavy AND by the light tier (light↔heavy round-trip parity).
- Array-of-tiles: mixed input handled; virtual element guarded.
- Positional-access guard test: no `getMap(2)` / `getStruct(_, 3)` / raster-slot `getString(1)` outside the chokepoint (mirrors light's `test_no_v1_open_tile_pattern`).
- `tileToRow` no longer has a StringType/checkpoint branch (removed-path assertion).

## Success criteria
- A v2 tile (materialized) from the light reader flows into any heavy `rst_*` without crashing; a virtual tile yields the clear guard error, not a `ClassCastException`.
- Heavy always emits v2 materialized tiles; light and heavy share one struct shape end-to-end.
- v1 binary inputs remain supported; v1 path-tiles and the checkpoint output branch are removed with tests proving the new errors/shape.
- Docs state the virtual=light-only / heavy=v1+v2-materialized-in, v2-out contract, voice-clean.
- No new registered functions; binding parity unchanged.
