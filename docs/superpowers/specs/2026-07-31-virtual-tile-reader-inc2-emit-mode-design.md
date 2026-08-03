# v2 Virtual-Tile Reader — Increment 2: reader `virtualTiles` emit mode + v2 cutover

**Date:** 2026-07-31
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** `2026-07-31-v2-virtual-tile-reader-design.md` (Increment 1), `2026-07-29-large-raster-reader-design.md`

## Problem & motivation

Increment 1 built the bytes-free `VirtualTile` representation and the `open_tile`/`materialize`
front-door, proven locally and on Serverless (full parity + large-raster rounds). But nothing
*produces* virtual tiles yet — Inc-1 tests hand-built `VirtualTile` rows. Increment 2 wires the
light-tier `raster_gbx` reader to **emit** bytes-free virtual tiles, so
`spark.read.format("raster_gbx").option("virtualTiles","true").load(dir)` yields a DataFrame of
`(path, window)` descriptor rows with no raster payload — the headline OOM-dissolving win of the v2
model.

## Scope

### In scope (increment 2)

1. The `raster_gbx` reader emits **`V2_TILE_SCHEMA`** always (outer envelope stays `(source, tile)`;
   only the `tile` struct changes v1→v2). This is the **v2 cutover** for the light reader.
2. New reader option **`.option("virtualTiles", "true")`** (default `false`):
   - **virtual mode (true):** emit bytes-free tiles — `raster=null`, `path`=source, `window`=whole-file
     `(0,0,W,H)`, `metadata` carried; **one virtual tile per file**; no driver-side layout planning,
     no staging, no encode.
   - **materialized mode (false, default):** today's behavior (passthrough or windowed-encode →
     `raster` populated) **emitting the v2 struct** — `raster` filled, `path`/`window` provenance,
     `clip_polygon`/`clip_crs`/`crs` null.
3. Existing reader tests migrated to the v2 struct (by field name). Downstream `rst_*` keep working
   because they read via `open_tile` (raster-precedence).

### Explicitly NOT in scope (deferred)

- **Heavy-tier v2 handling** (Scala). The cutover makes even the default reader output the v2 struct,
  so heavy functions won't round-trip it until heavy gets minimal v2-struct handling (read `raster`,
  treat new fields as provenance). Inc 2 is **light-tier only**. Heavy v2 adoption is gated on
  heavy-tier parity work. **Not needed now** (user-confirmed).
- **Window taxonomy** — overlapPercent / user-bounds-array / user x,y tiling / overview `z` (Inc 3).
- **Bbox windowing in virtual mode** — virtual mode emits the whole-file window only this increment;
  bbox-driven virtual windows land with the taxonomy (Inc 3).
- **Routing `rst_*` through `open_tile` catalog-wide** (Inc 4).
- **Catalog / binding registration** — this is a reader *option* only; no new SQL surface, no
  `registered_functions.txt` / `function-info.json` / bindings changes. Stays exploratory; QC +
  binding-parity stay green.

## Success criteria

- `spark.read.format("raster_gbx").option("virtualTiles","true").load(dir)` yields one bytes-free row
  per file: `raster is None`, `path` set, `window == (0,0,W,H)`, `metadata` non-empty; row byte-size
  tiny (no pixels).
- A virtual row fed into `open_tile` materializes a window whose pixels equal the same slice of a
  full read (end-to-end reader → open_tile).
- Default (materialized) load emits the v2 struct with `raster` populated and pixels **byte-identical**
  to the pre-cutover reader; passthrough parity preserved.
- Reader `.schema()` == `(source, V2_TILE_SCHEMA)` in both modes; `raster` field nullable.
- Serverless: `virtualTiles=true` reader → collect → `open_tile` on workers against real `/Volumes`
  succeeds (mirrors the Inc-1 smoke round, sourced from the reader).

## Architecture

The reader is a Spark **DataSource V2** (`RasterGbxDataSource` / `RasterGbxReader`), registered as
`"raster_gbx"` in `ds/register.py`. Today it emits `(source, tile)` where `tile` is the v1
`TILE_SCHEMA` (`cellid, raster (non-null), metadata`). Increment 2 changes only the reader; the
`open_tile` consumer (Inc 1) already handles v2.

### Unit A — schema surface (`ds/raster.py`)

- `reader_schema_v2() -> StructType`: `(source: string non-null, tile: V2_TILE_SCHEMA non-null)`,
  importing `V2_TILE_SCHEMA` from `pyrx/core/virtual_tile.py`. `RasterGbxDataSource.schema()` returns
  it (one schema, both modes).
- `RasterGbxReader.__init__` parses `self.emit_virtual = str(options.get("virtualTiles","false")).lower() == "true"`.

### Unit B — planning (`_plan_partitions_for_file`, driver)

- Thread an `emit_virtual: bool` flag. In virtual mode: open the header once for `(W,H)` and return
  **one** `_TilePartition` with `window=(0,0,W,H)`, `is_passthrough=False`, `is_whole=True`, and a new
  `emit_virtual=True` marker. No `plan_layout`, no budget, no bbox split.
- Materialized mode: unchanged planning (passthrough / whole-encode / split), carrying
  `emit_virtual=False`.
- `_TilePartition` gains an `emit_virtual: bool = False` field.

### Unit C — emission (`RasterGbxReader.read`, worker)

- Single decision point. If `partition.emit_virtual`: build a v2 row with `raster=None` —
  `(cellid=CELLID_FRESH, raster=None, path=source_path, window=partition.window, clip_polygon=None,
  clip_crs=None, crs=None, metadata=<header-subset>)`. **No staging, no decode, no `encode_tile`.**
  Emit exactly one row. Metadata is the cheap header-derived subset (driver/format/dims/sourcePath),
  not the full 11-key encode metadata.
- Else (materialized): run today's passthrough/`encode_tile` path, then **widen** the resulting
  `(cellid, raster_bytes, meta)` into the v2 row — `path`=source, `window`=partition.window (or
  `(0,0,W,H)` for passthrough where window was `None`), new fields null.

### Unit D — v2 row builder (shared helper)

- `_v2_tile_row(cellid, raster, path, window, metadata, clip_polygon=None, clip_crs=None, crs=None)`
  used by both paths — the single struct-assembly point (kin to the `open_tile` single-chokepoint
  discipline). Window serialized as the nested `{col_off,row_off,width,height}` struct or null.

### Passthrough in virtual mode

Today passthrough emits original bytes with `window=None`. A virtual tile *is* a reference, so
"passthrough" collapses into the whole-file virtual tile (`window=(0,0,W,H)`, `raster=None`). Virtual
mode has no separate passthrough concept.

## Data flow

**Virtual mode:**
```
read.format("raster_gbx").option("virtualTiles","true").load(dir)
  partitions(): list files → per file open header (W,H) → _TilePartition(emit_virtual=True, window=(0,0,W,H))
  read(p): _v2_tile_row(cellid=FRESH, raster=None, path=source, window, metadata=header-subset)
  → DataFrame[(source, tile<v2, raster=null>)]        ← bytes-free
downstream: rst_*(open_tile(tile)) → lazy stage+read window (Inc-1 machinery)
```

**Materialized/default mode:**
```
partitions(): today's planning (passthrough / whole-encode / split), emit_virtual=False
read(p): encode_tile|passthrough → _v2_tile_row(..., raster=bytes, path=source, window=p.window)
  → DataFrame[(source, tile<v2, raster=bytes>)]       ← byte-identical pixels to pre-cutover
```

## Error handling

- Header open failure in virtual planning → same file-skip/error behavior as today's planning; do not
  stage.
- `window=None` cannot occur in virtual mode (always `(0,0,W,H)`), so the `VirtualTile` non-null-window
  invariant holds by construction.
- Unreadable file at downstream materialize → surfaces in `open_tile` (`TileMaterializeError`), not the
  reader — correct, since virtual defers the read.
- Empty directory / no matching files → empty DataFrame with the v2 schema (schema resolves with zero
  partitions).

## Testing (TDD, local Docker first)

Run via `gbx:test:pyrx --path python/geobrix/test/ds/...`. Reuse Inc-1 `_layouts` 3-layout fixture.

- **Schema:** reader `.schema()` == `(source, V2_TILE_SCHEMA)` in both modes; `tile.raster` nullable.
- **Virtual emit:** load 3-layout corpus with `virtualTiles=true` → one row/file, each `raster is None`,
  `path` set, `window == (0,0,W,H)`, `metadata` non-empty; assert tiny row byte-size (no pixels).
- **Round-trip:** feed a virtual row into `open_tile` → materialized window == full-read slice
  (end-to-end reader → open_tile).
- **Materialized cutover:** default load emits v2; `raster` populated, pixels byte-identical to
  pre-cutover; passthrough parity preserved.
- **v1→v2 test migration:** update `test_raster_datasource.py` / `test_raster_bbox.py` assertions that
  indexed the 3-field tile to the 8-field struct (by field name, not position).
- **Serverless (fire directly, oauth-fe + `.venv-pyrx` py3.12):** one round proving `virtualTiles=true`
  reader → collect → `open_tile` on workers against real `/Volumes`, mirroring the Inc-1 smoke round but
  sourced from the reader.

## Deliverables

- `ds/raster.py`: `reader_schema_v2`, `virtualTiles` option parse, `_TilePartition.emit_virtual`,
  virtual planning branch, `read` emission branch, `_v2_tile_row` helper.
- Tests: new `test/ds/test_raster_virtual.py` (virtual emit + round-trip) + migrated
  `test_raster_datasource.py` / `test_raster_bbox.py` assertions.
- Serverless experiment notebook under `prompts/features/` (gitignored scratch): reader-sourced
  virtual tiles → worker-side `open_tile`.

## Known gaps / follow-ons (tracked, not built here)

- **Heavy-tier v2 handling** — required before heavy Scala functions can consume the (now-default) v2
  reader output. Light-tier-only this increment. Gated on heavy-tier parity.
- **Window taxonomy** (Inc 3): overlapPercent / user-bounds / x,y tiling / overview `z`; bbox-driven
  virtual windows.
- **Functions virtual-aware via `open_tile`** (Inc 4).
- Carried from Inc 1: `materialize_to_bytes` clean-profile fix (striped inflation compounds block-key
  inheritance); dedup `_epsg_of`/`_epsg_int`; non-EPSG (WKT2/PROJ) CRS handling.
