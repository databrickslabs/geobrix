# v2 Tile Struct Field Reorder: `path_mode` after `path` — Design

**Status:** Proposed (2026-08-19)
**Scope:** Reorder the v2 raster **tile** struct so `path_mode` sits immediately
after `path`, restoring `metadata` as the final field. Cross-tier (Scala heavy +
Python light). Beta 0.5.0 — nothing external depends on field *positions* yet.

## Target field order

```
(cellid, raster, path, path_mode, window, clip_polygon, clip_crs, crs, metadata)
```

`path_mode` moves from index 8 (last) to index 3 (right after `path`);
`window / clip_polygon / clip_crs / crs / metadata` each shift +1; `metadata`
returns to last (index 8).

## Motivation

Increment-2 introduced `path_mode` by **appending it as the 9th/last field**,
after `metadata`. That was expedient but wrong on two counts:

1. It broke the long-standing **`tile[-1] == metadata`** invariant consumers
   relied on — surfaced as the CI regression in
   `test/ds/test_raster_datasource.py` + `test/ds/test_raster_large.py`, which
   read metadata as `tile[-1]` and crashed on the `None` path_mode.
2. `path_mode` describes the `path` / FILE reference; it belongs next to `path`,
   not orphaned after the `metadata` map.

Keeping `metadata` **last** is a durable contract: any future provenance field
inserts before it, and `tile[-1] == metadata` holds forever.

## Root cause of the cost/risk (and the fix)

The tile row is a **positional** structure on both tiers:

- **Scala:** Catalyst `InternalRow` has no field names — ~31 constructors
  hand-inline `InternalRow.fromSeq(Seq(… nulls …))` and readers use hardcoded
  `row.getX(n)`. A reorder shifts every index; a partial change silently
  corrupts cross-tier tiles (e.g. reading `window` as `path_mode`).
- **Python:** the reader emits positional tuples via `_v2_tile_row()` — but this
  is already a **single centralized function**, and consumers are already
  name-based (`tile["metadata"]`).

**Fix:** make field ORDER a single source of truth (the schema) and derive every
position from it **by name**. `InternalRow` stays positional, but:
- **reads** derive ordinals from the schema: `v2TileType.fieldIndex("metadata")`
  (Scala) / `V2_TILE_SCHEMA.fieldNames().index("metadata")` (Python);
- **writes** go through one **named builder** that assembles the row in schema
  order internally.

Then the reorder is a schema edit, not a ~40-site hunt, and the corruption class
is permanently gone.

## Approach: two phases

Two phases so a refactor bug and a reorder bug are attributable to different,
independently-reviewable, independently-revertable diffs.

### Phase 1 — Name-based hardening (behavior-preserving; `path_mode` STAYS at index 8)

No field moves. All existing tests stay green. Purely "stop hardcoding positions."

**Scala:**
- Add one named builder `V2Tile.row(cellid=, raster=, path=, pathMode=, window=,
  clipPolygon=, clipCrs=, crs=, metadata=)` (all defaulting to null) in a shared
  util, assembling the `InternalRow` in `v2TileType` order in ONE place.
- Route the ~31 `InternalRow.fromSeq` tile constructors (RST_FromContent,
  RST_Rasterize, the RST_*RasterizeAgg family, generators, …) through it.
- Convert positional readers (`RasterSerializationUtil.normalizeToV2Row`, the
  `TileLayout` v2 case, any `row.getX(n)` on a tile) to derive ordinals from
  `v2TileType.fieldIndex(name)`.
- `v2TileType` stays the sole order declaration.

**Python:**
- `_v2_tile_row()` stays the single positional builder (assembles the tuple in
  `V2_TILE_SCHEMA` order).
- Convert any raw-tuple positional consumers to
  `V2_TILE_SCHEMA.fieldNames().index(name)` (already applied to the two ds tests;
  the plan sweeps for any others).

**Verify:** heavy Scala suite + light pyrx suite + on-cluster cross-tier
round-trip — all green, behavior unchanged.

### Phase 2 — The reorder (small diff on hardened foundations)

- Flip to the target order in the **only** order-bearing places (now that
  Phase 1 made everything schema-driven): `v2TileType` (Scala), `V2_TILE_SCHEMA`
  (Python), the `VirtualTile` dataclass, the named builder's internal order, and
  `_v2_tile_row()`'s tuple order.
- Update order-assertion tests: `test_v2_tile_output_invariant.py` (expected
  list), `test_path_mode.py` (path_mode no longer `[-1]`), `test_tile_schema_parity.py`
  (`_HEAVY_V2_FIELDS`), Scala `RasterSerializationV2Test` (position + "last field"
  assertions).
- **Add** a positive invariant test: `metadata` is the final field AND `path_mode`
  immediately follows `path` (locks the contract).
- Update docs: `docs/docs/api/tile-structure.mdx` struct block + field table;
  diagram generator `resources/images/generators/rasterx-tile-structure.py`
  (FIELDS order + the two example tuples) → regenerate PNG; docstrings in
  `pyrx/_serde.py` + `pyrx/file_table.py`.
- Rebuild + stage JAR; run heavy + light suites; on-cluster cross-tier round-trip
  re-validation (the exact failure mode if Scala/Python diverge).

## Risk & mitigation

- **Riskiest surface:** the Scala↔Python row-materialization crossing (silent
  corruption). Two-phase isolates the mechanical, behavior-preserving refactor
  (Phase 1) from the tiny reorder (Phase 2).
- **On-cluster round-trip validation is mandatory in both phases** — unit tests
  within one tier cannot catch a cross-tier order divergence.

## Out of scope

- No change to tile semantics, field types, or v1→v2 read compatibility.
- No new fields.

## Definition of done

- Both tiers declare the target order from a single schema each; **no hardcoded
  tile positions remain** (Scala reads via `fieldIndex`, builds via `V2Tile.row`;
  Python via `V2_TILE_SCHEMA` + `_v2_tile_row`).
- All existing + new invariant/parity tests green (heavy + light).
- On-cluster cross-tier round-trip validated.
- Docs + diagram reflect the new order.
