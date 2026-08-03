# Light-Through-Finalize: Pending-Instruction Class for Virtual Tiles — Design

**Status:** Approved design (2026-08-03). Feeds an implementation plan.

**One-liner:** Let cheap, common raster ops accumulate as *pending instructions*
on a virtual tile and stay virtual (no pixel read, no new backing file), applying
together at the single next materialization — so a lightweight pipeline stays
light from full-size ingest through to the tessellation cut-over.

---

## Problem

In GeoBrix 0.5.0 the lightweight raster readers emit **virtual tiles** by default
(`raster=None`, `path`+`window` set). The intent is that a pipeline stays
bytes-free through full-size handling and materializes only where pixels are
genuinely produced (e.g. H3 tessellation). But execution testing of the
`eo-series/03` pipeline (and a direct accessor audit) found the full-size stage
does **not** stay light:

- **`rst_initnodata` forces a full materialization.** Its default UDF
  (`_init_nodata_udf`) reads all pixels, rewrites nodata into the bytes, and
  returns a **v1 3-field** struct (`_serde.TILE_SCHEMA`) — dropping `path`/`window`
  and emitting raster bytes. So the whole scene materializes at
  `finalize_tiled_band_tbl`, before tessellation.
- **`rst_memsize` returns `None` on a virtual tile.** `rst_memsize(tile)` →
  `_memsize_udf(_raster_field(tile))` reads the raster byte field directly; on a
  virtual tile that field is null, so the result is null. It never consults `path`.

Both are library-level correctness gaps, not notebook wiring. The user's goal:
**tables carry metadata + references; heavy raster bytes live in Volume files, not
in Delta**. Materializing at `finalize` (then persisting bytes to a Delta table)
is exactly the model to avoid.

### Audit results (venv, EPSG:4326 bench file, `tileSize=512`, virtual tile)

Virtual-safe already (header accessors, read through `path`): `rst_srid` (→4326),
`rst_numbands`, `rst_height`, `rst_width`, `rst_tryopen`, `rst_boundingbox`.

Broken on virtual: `rst_memsize` (→None), `rst_initnodata` (→v1 3-field, path
dropped, forces materialize).

> Note: an earlier "`rst_srid`=None bug" was a **false alarm** — the local MODIS
> test file is `ESRI:54008` (legitimately no EPSG). Real Sentinel-2 scenes are
> UTM/EPSG and return a valid srid. The underlying CRS-vs-SRID concern is real for
> non-EPSG data but is **out of scope here** (see Non-Goals).

---

## Design

### The pending-instruction class

Introduce a general notion of a **pending instruction**: a cheap, common,
pixel-parameter operation that, applied to a **virtual** tile, is *recorded* rather
than *executed*. The tile stays virtual (`raster=None`, `path`/`window` intact);
the instruction is applied later, at the single next read, together with the
window/clip/crs work `open_tile` already does.

This mirrors the existing v2 **reference-vs-instruction** model: `window`,
`clip_polygon`, `clip_crs`, `crs` are already *instructions* on a virtual tile
(pending operations applied at read) and *provenance* on a materialized one. The
new members join that model.

**Carrier: `metadata` map keys** — no v2 struct schema change. The v2 struct
just stabilized at 8 fields across both tiers; widening it is a shared-struct
behavior change with heavy-tier and doc ripple, and these are *non-essential*
directives, so they belong in the existing `metadata: map<string,string>` field:

- `metadata['pending_nodata']` = e.g. `"-9999"`
- `metadata['pending_srid']`   = e.g. `"4326"`
- `metadata['pending_bands']`  = e.g. `"1,2,3"` (1-based, comma-separated)

(Exact key names are finalized in the plan; `pending_*` prefix keeps them
self-describing and collision-free from format metadata like `driver`/`extension`.)

### The one rule (behavior keyed on input state)

For each member op, on invocation with **no force-output args**:

- **Virtual input** (`raster is None`, `path` set): append the pending key to
  `metadata`, return a still-virtual v2 tile. No pixel read, no backing-file write.
- **Materialized input** (`raster` bytes present): apply to the bytes now (today's
  eager behavior) and return a **v2** tile. There is no reference to defer to, so
  it stays materialized.

This is a single rule, not two ad-hoc paths: "defer if there's a reference to
defer to, else apply now."

### Apply-at-open (the chokepoint)

`core/open_tile.py::open_tile` is the single place a virtual tile becomes pixels.
It already does: stage `path` local → read `window` → (warp if `tile.crs` differs)
→ clip (`clip_polygon`/`clip_crs`). The pending instructions slot into that
existing pipeline, applied in a **defined order** regardless of the order the ops
were called (order-safe accumulation):

1. **band-select** (`pending_bands`) → `src.read(indexes=[...])` / restrict bands
2. **nodata** (`pending_nodata`) → set on the profile/dataset
3. **setsrid** (`pending_srid`) → stamp CRS metadata (relabel; **no reproject**)
4. **window** (existing) → the pixel window
5. **clip** (existing `clip_polygon`/`clip_crs`)
6. **crs/reproject** (existing `tile.crs` warp)

`open_header` (header-only accessors) applies the instructions that change the
header view without a pixel read: **band-select** changes `count`, **setsrid**
changes `crs`, **nodata** changes the profile's `nodata` (dimensions/transform
unchanged). So header accessors (`rst_numbands`, `rst_srid`, …) stay correct on a
tile with pending instructions.

### Opt-out / force-apply: existing force-output params

No new API. Every member op already carries the force-output triple
(`virtualize_dir`, `virtualize_prefix`, `materialize`):

- **Default (no args):** stay virtual + accumulate (the new behavior).
- **`materialize=True`:** apply all pending instructions now, bake raster bytes
  into the row. Documented as: on a virtual tile, nodata/setsrid/band-select are
  *allowed pending instructions* that stay virtual by default; `materialize=True`
  forces them applied.
- **`virtualize_dir=<path>`:** apply now, write a new backing GeoTIFF to the
  durable path, return a virtual tile referencing it.

### Members this spec

`nodata` (`rst_initnodata`), `setsrid` (`rst_setsrid`), `band-select`
(`rst_band`). All pure read-parameters — no warp. All three already have the
force-output triple and dispatch to a `_v2_udf`; the change is making the
no-force-output path on a virtual input **record** rather than **produce bytes**,
and ensuring v2 output in every path.

### Also fixed

- **`rst_memsize` virtual-aware:** answer from the header/window
  (`count * width * height * itemsize` via `open_header`) when `raster` is null,
  instead of reading the null raster byte field.
- **v2 everywhere:** `rst_initnodata`/`rst_setsrid`/`rst_band` emit the v2 8-field
  struct in all paths (they currently drop to v1 3-field on the default path).

---

## Non-Goals (explicit)

- **CRS-handling-across-the-board (Spec B).** The broader effort — GeoBrix filling
  the gap where Databricks product spatial is SRID-only, carrying non-EPSG CRS
  (ESRI codes, WKT, PROJ4) consistently across raster/vector/grid/viz — is a
  separate, user-driven brainstorm. Here, `setsrid` covers only the safe "assign
  an EPSG code" subset. This spec does the **minimum** so a non-EPSG tile does not
  *break* the finalize pipeline; it does not redesign CRS carriage.
- **No v2 struct schema change.** Pending instructions ride in `metadata`.
- **Reproject stays materializing.** `rst_transform`/warp is not a pending
  instruction; it produces pixels.
- **Heavy tier untouched.** Heavy operates on materialized tiles only; pending
  instructions are a lightweight-tier concept.

---

## Testing (TDD)

Per member (`nodata`, `setsrid`, `band-select`):

1. Virtual tile + op (no args) → stays virtual: `raster is None`, `path` set, the
   `pending_*` key recorded in `metadata`. (RED before fix: today it materializes
   / drops to v1.)
2. Read that tile via `open_tile` → the instruction is correctly applied to the
   pixels (nodata set; CRS relabeled; only selected bands present).
3. `materialize=True` → instruction applied, raster bytes present, v2 struct.
4. Materialized input + op → applied eagerly (today's semantics), v2 struct.
5. Multi-instruction accumulation (e.g. band-select + nodata + setsrid on one
   virtual tile) → all present as keys; applied in the defined pipeline order at
   read; result independent of call order.
6. `rst_memsize` on a virtual tile → correct byte/size estimate (not None).
7. `open_header` on a tile with pending instructions → count/crs/profile reflect
   the instructions without a pixel read.

**End-to-end validation:** wire `eo-series` `finalize_tiled_band_tbl` to stay
virtual (drop the eager materialize; `rst_initnodata` now records the instruction),
then re-run `eo-series/03` on Serverless with `--set-var FORCE_REBUILD=True`
(forces the compute steps; the committed notebook keeps `FORCE_REBUILD=False`).
Confirm: the `band_*_tile` table rows are virtual (bytes-free references), and
materialization happens at the tessellation step — not at finalize.

---

## Files (anticipated; finalized in the plan)

- `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` — apply pending
  instructions in `open_tile` / `open_header` in defined order.
- `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` —
  `rst_initnodata` / `rst_setsrid` / `rst_band` record-when-virtual + v2 output;
  `rst_memsize` virtual-aware.
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py` /
  `_serde.py` — helpers to read/write `pending_*` metadata keys if needed.
- Tests under `python/geobrix/test/` (pyrx/ds) — the cases above.
- `notebooks/examples/eo-series/config_nb.ipynb` — `finalize_tiled_band_tbl` stays
  virtual; narrative tracks the change.
- Docs: virtual-tiles page / execution-tiers "virtual↔materialized advice" — note
  the pending-instruction ops (nodata/setsrid/band-select) stay virtual by default,
  `materialize=True` forces apply. `rst_*` function docs updated.
