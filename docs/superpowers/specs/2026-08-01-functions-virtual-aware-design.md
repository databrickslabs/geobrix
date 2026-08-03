# v2 Virtual-Tile Reader — Increment 4: functions virtual-aware + force-output + writer round-trip

**Date:** 2026-08-01
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** Inc 1 (`2026-07-31-v2-virtual-tile-reader-design.md`), Inc 2 (emit mode), Inc 2.5 (clip/window selection), Inc 3 (tileSize/overlap)

## Problem & motivation

Increments 1–3 built the bytes-free `VirtualTile`, the `open_tile` chokepoint, and a reader that
emits virtual tiles (whole-file, windows, clipPolygons, tileSize+overlap). But the ~127 light-tier
`rst_*` functions still open tiles via `_serde.open_tile(bytes(tile["raster"]))` — they only consume
**materialized** tiles. And the raster **writers** still require the v1 tile schema, so a v2
DataFrame (the reader's current output) fails to write at all — a read→write round-trip is broken on
this branch today.

Increment 4 makes the whole light-tier catalog virtual-aware: every function consumes virtual OR
materialized tiles through one shared front-door; header-only accessors answer without materializing
pixels; tile-returning functions gain force-output params to flip a heavy materialized DataFrame into
light virtual rows (or force bytes); and the writers accept v2 and auto-materialize virtual tiles on
write. This is the payoff of the whole virtual-tiling direction — lazy chains of ops on bytes-free
DataFrames, pixels touched only when an op needs them.

## Scope

### In scope (increment 4)

Three consumption capabilities + writer round-trip + docs:

1. **Consume virtual tiles everywhere.** Every `rst_*` function that opens a tile gets its dataset
   via a shared `_open(tile)` adapter (accepts a v1 tile struct, a v2 tile struct, or a `VirtualTile`
   → yields an open rasterio dataset), delegating to the Inc-1 `open_tile`. Function operation bodies
   unchanged.
2. **Header-only accessors.** Accessors whose answer is in the file header answer from a virtual
   tile's `path` **without materializing pixels** (a lazy header open). Pixel-dependent accessors
   materialize the window.
3. **Force-output params** on tile-returning functions — `virtualize_dir`, `virtualize_prefix`,
   `materialize` — via one shared `shape_output(...)` helper applied at each function's return.
4. **Writer round-trip (fixes a pre-existing regression).** Writers emit raster **files** (GTiff/COG
   on disk) — a writer never "writes a tile struct"; the tile (v1 or v2) is only the in-DataFrame
   *input*. So this is purely about INPUT acceptance: `raster_gbx`/`gtiff_gbx`/`cog_gbx` writers
   dual-accept a v1 **or** v2 tile-envelope DataFrame, then internally obtain the bytes to write —
   auto-materializing a virtual tile (writing is a materialization boundary), or, for `cog_gbx`,
   path-direct converting a whole-file virtual tile with no bytes round-trip. Output is always a
   format file, unchanged.
5. **Docs.** A per-function override note on every light `rst_*` doc; a Light-vs-Heavy blurb; content
   for the queued Virtual Tiles page; the virtual↔materialized advice (the class taxonomy).

### Explicitly NOT in scope (deferred)

- Heavy-tier support (heavy cannot consume virtual tiles — no in-JVM `/Volumes` lazy read; see the
  light→heavy bridge below).
- `rst_transform` lazy-warp *internals* (Inc 5) — but `rst_transform` inherits the force-output
  params defined here.
- Catalog / binding registration changes are limited to what already exists (no new registered
  functions this increment; the params are optional args on existing functions).

## The force-output params (the headline capability)

On tile-returning functions, via one shared `shape_output(tile, *, virtualize_dir=None,
virtualize_prefix=None, materialize=None)` helper wrapping the op's produced tile:

- **`virtualize_dir=<durable path>`** (+ optional **`virtualize_prefix`**): write the produced bytes
  to `virtualize_dir/[<prefix>_]<cellid>_<col>_<row>_<w>_<h>.tif` (overwrite on conflict) and return a
  **light virtual tile** (`raster=None`, `path=<written>`, `window=full-extent`, provenance in
  `metadata`). No-op if the produced tile is already virtual. This is *dematerialize-to-virtual* as a
  per-function knob — its point is to flip a heavy raster-payload column into light path+window rows.
- **`materialize=True|False`**: ensure the produced tile carries bytes (`materialize_to_bytes` if it
  was virtual). No-op if already materialized.
- **Neither (auto, the default):** the op returns its natural shape — deferrable ops (clip, setsrid,
  transform-intent) stay virtual if the *input* was virtual; pixel-producing ops materialize.
- **Conflict:** `virtualize_dir` set together with `materialize=True` → `ValueError`.

Durability: `virtualize_dir` must be a durable location (a Volume path), never a temp dir — a virtual
tile is a reference that must outlive the operation and be readable later on a (possibly different)
executor. The helper writes via the FUSE-safe local-temp→copy pattern. Filenames are provenance-based
and idempotent; `virtualize_prefix` lets the user deconflict (e.g. per-run, per-layer, or two
different function outputs sharing a directory).

**`virtualize_dir` virtualizes the RESULT, not the computation.** A pixel-producing op still computes
its pixels; virtualizing means externalizing that result to a reference. So *any* tile-returning
function can virtualize — even slope/focal — because it writes the computed bytes to the durable path
and returns the light row.

## Header-only accessors

Answer from the virtual tile's `path` header (a lazy `rasterio.open` that never `.read()`s pixels):
`width`, `height`, `numbands`, `srid`, `scalex/y`, `upperleftx/y`, `rotation`, `skewx/y`,
`boundingbox`, `type`, `getnodata`, `format`, `metadata`, `georeference`.

Materialize the window (via `_open`): `avg`, `min`, `max`, `median`, `pixelcount`, `summary`,
`histogram`, `sample`, `isempty` — these need pixels. A per-accessor marker (header-only vs pixel)
selects which contextmanager the wrapper uses.

## Materialize is the light→heavy bridge (forward constraint)

Heavy Scala functions cannot consume a virtual tile (the UC credential lives in the Python worker; no
in-JVM `/Volumes` lazy read — the lattice item-2 boundary). So the sanctioned workflow is: do lazy
virtual work in light, then invoke a light function with `materialize=True` (or write via a writer,
which materializes) to produce bytes heavy can read. The force-output params are **light-tier only** —
heavy functions have none of them. This is stated in every light `rst_*` function doc and the
Light-vs-Heavy page.

## Architecture

Three shared helpers carry the increment; per-function change is a call swap, not a rewrite. Operation
logic is never touched — only how the dataset is opened and how the result is shaped.

### Unit A — `_open(tile)` consume adapter (`pyrx/core/open_tile.py`)

`@contextmanager _open(tile) -> DatasetReader`: accepts a v1/v2 tile struct/dict/Row OR a
`VirtualTile`; normalizes (`VirtualTile.from_row` / `from_v1` as needed) and delegates to `open_tile`.
Every `with _serde.open_tile(bytes(tile["raster"])) as ds:` becomes `with _open(tile) as ds:`.
Multi-tile ops (agg, mapalgebra) use a sibling that maps `_open` over the list.

### Unit B — header-only accessor path (`pyrx/core/accessors.py` + wrappers)

`@contextmanager open_header(tile)`: bytes present → open bytes (as today); virtual →
`rasterio.open(_stage_local_if_needed(path))`, header only (never `.read()`). A table in
`accessors.py` marks header-only vs pixel accessors; wrappers pick the right contextmanager.

### Unit C — `shape_output(...)` (`pyrx/core/open_tile.py`)

The single output-shaping helper (semantics above). Reuses `materialize_to_bytes` (Inc 1) for
`materialize=True`; a durable FUSE-safe write for `virtualize_dir`; validation for the conflict case.

### Unit D — writers (`ds/writer.py`, `ds/cog_writer.py`)

- `assert_write_schema` accepts v1 **or** v2 envelope (dual-accept INPUT); the write path normalizes
  each tile to a `VirtualTile` internally to obtain bytes, then emits format files as before (the
  writer's on-disk output — GTiff/COG — is unchanged; only input acceptance widens).
- `RasterGbxWriter.write`: normalize → if virtual, `materialize_to_bytes` → existing `tile_to_bytes`
  write. Removes the `bytes(None)` crash (Inc-2 deferred item) AND the v1-only schema rejection
  (pre-existing round-trip regression).
- `CogGbxWriter`: for a v2 tile, read `tile.path`; **whole-file virtual** (window == full extent, no
  `clip_polygon`) → `cog_convert_file(path)` direct (no bytes round-trip); **windowed/clipped virtual
  or materialized** → materialize then convert. Existing top-level-`path` (file_gbx) input still
  works.

### Two phases within this increment

Inc 4 runs in two phases (both in-increment, not a separate increment):

- **Phase A — front-door + proof.** Build the three shared helpers (`_open`, `open_header`,
  `shape_output`), the writer dual-accept + auto-materialize + cog path-direct, and the docs
  scaffolding, wiring + testing them against a **representative function per family** end-to-end
  (a header accessor, a pixel accessor, `rst_clip`, `rst_slope`, an aggregator, a UDTF) including a
  Serverless run. This validates the uniform pattern on a handful before touching the rest.
- **Phase B — full catalog sweep.** Apply the mechanical `_serde.open_tile(bytes(tile["raster"])) →
  _open(tile)` swap across the remaining ~100 call sites, the header-only-vs-pixel accessor split
  across all accessors, and the force-output param passthrough across all tile-returning wrappers.
  Phase B is pure repetition of the Phase-A pattern; it starts only after Phase A is green.

## Data flow (the payoff)

```
read(virtualTiles=true) -> light rows (path+window, no bytes)
  -> rst_clip(tile)                         [auto: stays virtual, sets clip_polygon]
  -> rst_slope(tile)                        [auto: materializes window, returns bytes]
     -- or -- rst_slope(tile, virtualize_dir="/Volumes/out")
                                            [computes slope, writes result, returns light row]
  -> .write.format("cog_gbx")               [whole-file virtual -> path-direct convert; else materialize]
light -> heavy: rst_x(tile, materialize=True) -> bytes -> heavy Scala function reads
```

## Error handling

- `virtualize_dir` + `materialize=True` → `ValueError` (conflict).
- `virtualize_dir` that is not writable / not durable → clear error from the write; document the
  Volume requirement.
- Writer receiving a schema that is neither v1 nor v2 → clear `ValueError` (dual-accept, else reject).
- Header accessor on a virtual tile whose `path` is unreadable → the same `TileMaterializeError`
  surface as `open_tile`.
- A virtual tile with a windowed/clipped extent handed to `cog_gbx` → materialized first (never
  path-direct converted, which would ignore the window/clip).

## Testing (TDD, Docker first; Serverless final gate)

- **`_open` adapter**: v1 struct / v2 materialized / v2 virtual all yield the correct dataset.
- **Header accessors**: `rst_width`/`rst_srid`/`rst_boundingbox` on a virtual tile return correct
  values with **no pixel read** (assert `.read` not called, e.g. via a spy or a header-only fixture).
- **`shape_output`**: `virtualize_dir` writes a provenance-named file + returns a virtual row that
  round-trips through `open_tile`; `virtualize_prefix` deconflicts; overwrite-on-conflict; a
  pixel-producing op (`rst_slope`) with `virtualize_dir` returns a light row whose materialization
  equals the direct slope result; `materialize=True` forces bytes; `virtualize_dir`+`materialize=True`
  → error.
- **Writers**: v1 round-trip still works (regression guard); v2 materialized writes; v2 **virtual
  auto-materializes** (gtiff); `cog_gbx` whole-file-virtual → path-direct, windowed-virtual →
  materialize.
- **Serverless (fire directly)**: read virtual → `rst_slope(..., virtualize_dir=<Volume>)` → write
  `cog_gbx`, worker-side, real `/Volumes`; assert the written COG round-trips.

## Deliverables

- `pyrx/core/open_tile.py`: `_open` adapter (+ multi-tile sibling), `shape_output`.
- `pyrx/core/accessors.py`: `open_header` + header-only/pixel marker table.
- `pyrx/functions.py`: tile-returning UDF wrappers gain `virtualize_dir`/`virtualize_prefix`/
  `materialize` passthrough to `shape_output`; accessor wrappers pick header-only vs pixel; the
  `_serde.open_tile → _open` swap catalog-wide (after the representative-family proof).
- `ds/writer.py`, `ds/cog_writer.py`: dual-accept schema, virtual auto-materialize, cog path-direct.
- Docs: per-function override note (every light `rst_*`), Light-vs-Heavy blurb, Virtual Tiles page
  content, virtual↔materialized advice. Doc examples live in doc-test source and execute.
- Serverless experiment notebook under `prompts/features/` (gitignored scratch).

## Known gaps / follow-ons (tracked, not built here)

- Heavy-tier v2 handling.
- `rst_transform` lazy-warp internals (Inc 5) — inherits these params.
- Carried: COG `format` metadata reads `"gtiff"`; `materialize_to_bytes` clean-profile fix; dedup
  `_epsg_of`/`_epsg_int`; non-EPSG (WKT2/PROJ) CRS.

## The virtual↔materialized advice (taxonomy, for the docs)

- **Metadata accessors** (header-only): free on virtual tiles — no pixels read.
- **Pixel accessors / stats**: read the window (materialize transiently), return scalars/arrays.
- **Deferrable tile ops** (clip, setsrid, transform-intent): stay virtual under `auto` if input was
  virtual — chain them for free.
- **Pixel-producing tile ops** (slope, focal, mapalgebra, resample, indices, rasterize, …):
  materialize the window and return bytes; use `virtualize_dir` to re-externalize the result as a
  light row.
- **Writers**: always a materialization boundary (bytes to disk); a virtual DataFrame is directly
  writable (auto-materialized), and `cog_gbx` converts whole-file virtual tiles path-direct.
- **Crossing to heavy**: materialize first (`materialize=True` or write) — heavy cannot read virtual.
