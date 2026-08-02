# v2 Virtual-Tile Reader — Increment 5: transform/combinator coherence + corrected virtualize taxonomy

**Date:** 2026-08-02
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** Inc 1 (`2026-07-31-v2-virtual-tile-reader-design.md`), Inc 2 (emit mode), Inc 2.5 (clip/window selection), Inc 3 (tileSize/overlap), **Inc 4 (`2026-08-01-functions-virtual-aware-design.md`)**

## Problem & motivation

Increment 4 swept the whole light-tier `rst_*` catalog virtual-aware and, in doing so, already
attached the force-output params (`virtualize_dir` / `virtualize_prefix` / `materialize`) to the
pixel-producing functions — `rst_transform`, `rst_merge`, `rst_combineavg`, `rst_frombands`,
`rst_tobands`/webmercator — via the shared `_shaped_result_row` helper. Empirically, on the branch
today:

- `rst_transform` is already **eager**: `_transform_bytes` calls `warp.reproject_to_srid`, which
  always warps and returns fresh GTiff bytes.
- `rst_merge(3 virtual tiles, virtualize_dir=…)` already returns a **virtual** row (test
  `test_rst_merge_virtual_virtualize_dir` passes): the combinator materializes its inputs, mosaics,
  and `shape_output` writes the result to the dir and hands back a light row.

So the headline *mechanics* of Inc 5 landed with Inc 4's sweep. What Inc 5 must still settle is the
**semantic coherence** the brainstorm exposed — the parts the sweep left implicit or slightly wrong:

1. **The `virtualize_dir` no-op rule is stated too coarsely.** Inc 4 documented "if a tile is already
   virtual, `virtualize_dir` is a no-op." That is true only for **reference/passthrough** outputs
   (the op returns a tile that still references pixels already on a backing store). For a
   **pixel-producing** op (transform, merge, combineavg, frombands) the natural output is
   materialized bytes and `virtualize_dir` is the **only** way to get a virtual result back — never a
   no-op. The rule must be re-expressed by *output nature*, not by *input shape*, and this correction
   must propagate to the docstrings, the docs, and the memory blurb.

2. **Identity transform still warps.** `rst_transform(tile, target=source_srid)` produces no new grid
   (identity), yet `reproject_to_srid` runs a full `reproject` pass and re-encodes. By the governing
   principle ("produces new pixels → must materialize; produces no new pixels → passthrough"), an
   identity transform is a **passthrough**: it must not resample or re-encode, and `virtualize_dir`
   on it must behave like the reference/passthrough bucket (no-op when the input is already virtual).

3. **Produced-tile provenance fields are left null.** `_shaped_result_row` builds
   `VirtualTile(cellid, raster=bytes)` with `window`/`crs`/`clip_polygon` all `None`. For a
   materialized tile that is coherent (the bytes are self-describing, and null fields mean "read from
   the bytes"). But when `virtualize_dir` externalizes it, the emitted virtual row also carries
   `crs=None`, i.e. "read the CRS from the file" — which is correct *only because* the written file
   embeds its CRS. We make this reliance explicit and assert it, so a virtualized pixel-producer
   result is always openable and self-consistent (the founding invariant: `window`/`crs`/
   `clip_polygon` agree with real backing pixels).

There are **no new registered functions** and **no new force-output plumbing** in Inc 5 — that
shipped in Inc 4. Inc 5 is a correctness/coherence increment plus the Serverless proof and the
taxonomy documentation.

## The governing principle (settled in brainstorm)

> **A virtual tile must reference real, self-consistent backing bytes.** Any op that *produces new
> pixels* (warp/transform, mosaic/merge, combineavg, band-stack/frombands, and single-source pixel
> ops like slope/focal) must **materialize** to produce a coherent tile, because the only bytes its
> `window`/`crs`/`clip_polygon` can agree with are the ones it just generated — which exist nowhere
> until written. `virtualize_dir` supplies the "where": it writes the generated pixels to a durable
> path and returns a light row referencing them. Without `virtualize_dir`, the only coherent output
> is materialized bytes (the auto default). A lazy "recipe" tile (carry input refs + op, combine at
> read) is **illegal** — it would advertise a grid backed by pixels that do not yet exist (pending),
> reintroducing the mixed-state ambiguity we rejected for pending-warp.
>
> Ops that *do not* produce new pixels (header reads, reader-side window/clip selection, identity
> transform) yield **reference/passthrough** tiles directly: their backing pixels already exist, so
> `virtualize_dir` is a no-op and `materialize` forces bytes.

### The corrected `virtualize_dir` taxonomy (three buckets)

| Bucket | Examples | Auto output | `virtualize_dir` | `materialize=True` |
|---|---|---|---|---|
| **1. Reference / passthrough** | header accessors, reader window/clip selection, **identity `rst_transform`** | virtual (references existing backing pixels) | **no-op** (already references real bytes) | forces bytes (materialize the window) |
| **2. Single-source pixel op** | slope, focal, clip-to-window on one backing raster | materialized bytes | **externalizes** the generated result to a light row | no-op (already bytes) |
| **3. Multi-source combinator** | `rst_merge`, `rst_combineavg`, `rst_frombands` | materialized bytes | **REQUIRED** to get a virtual result — the *only* way `rst_merge(3 virtual)` returns virtual | no-op (already bytes) |

"Produces new pixels" is the exact discriminator; it decides every case with no special-case list.

## Scope

### In scope (increment 5)

1. **Identity-transform passthrough.** `rst_transform` short-circuits when `target_srid` equals the
   source CRS's EPSG code: no `reproject`, no re-encode. On a virtual input it stays a
   reference/passthrough tile (so `virtualize_dir` is a no-op, `materialize` forces the window bytes);
   on a materialized input it returns the input bytes verbatim. This mirrors the bucket-1 contract
   and avoids a needless resample+re-encode (which would also perturb any downstream raw-bytes
   sort-key parity, like merge's).

2. **Provenance coherence assertion for pixel-producer outputs.** Add a test-enforced invariant: a
   `virtualize_dir` result of any pixel-producer (transform/merge/combineavg/frombands) is openable
   via `open_tile` and yields a dataset whose CRS/width/height match the pre-virtualize materialized
   result. This nails down that leaving `crs=None` on the emitted row is safe *because* the written
   file embeds CRS — and guards against a future change that writes headerless bytes.

3. **Corrected taxonomy documentation.** Update:
   - the `virtualize_dir` docstrings on `shape_output` and each pixel-producer to state the
     by-output-nature rule (not the coarse "already-virtual → no-op");
   - the Inc-4 doc blurb / Light-vs-Heavy page where the coarse rule was written;
   - the queued Virtual Tiles page content (taxonomy table) — content only, page build stays the
     capstone;
   - the memory blurb (`light-virtual-tiling-by-reference`) lines that assert the coarse rule and the
     "rst_transform lazy-warp default" contradiction.

4. **Serverless proof.** One throwaway notebook (gitignored, `prompts/features/`) firing directly on
   Serverless over real `/Volumes`, proving on worker side:
   - `rst_merge` of 3 **virtual** tiles with `virtualize_dir=<Volume>` → a **light virtual row** whose
     path round-trips to the union mosaic (correct union extent);
   - `rst_transform(virtual, target=source_srid)` (identity) → passthrough, no re-encode;
   - `rst_transform(virtual, target=other_srid, virtualize_dir=<Volume>)` → light row in target CRS,
     round-trips and reports the target CRS.

### Explicitly NOT in scope (deferred)

- **Force-output plumbing / new params** — shipped in Inc 4; not re-touched beyond the identity
  short-circuit and docstring corrections.
- **New combinator functions** — "mosaic" = `rst_merge`, "stack" = `rst_frombands`; both exist. No
  `rst_stack` alias (no aliases; beta).
- **Lazy "recipe" combinator tiles** — rejected by the governing principle (pending trap).
- **Heavy-tier v2 handling** — the standing big deferral; heavy still cannot consume virtual tiles.
- **Virtual Tiles page + hero diagram build** — the capstone; Inc 5 only writes the taxonomy content
  that page will host.
- **`rst_transform` window/clip co-transform beyond CRS** — the reader already emits window/clip in
  the tile's own CRS, and an eager transform re-encodes to target CRS with embedded georeference, so
  the materialized result is self-consistent; no separate window/clip re-projection field bookkeeping
  is needed once the transform is eager (the bytes carry the truth). This is a consequence of the
  eager decision, called out so it is not re-opened.

## Architecture / where the changes land

- `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — `_transform_bytes` (or a thin wrapper)
  gains the identity short-circuit; `rst_transform`/combinator docstrings corrected.
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/warp.py` — `reproject_to_srid` gains an identity
  guard (return `ds`-bytes verbatim when `target_srid` == source EPSG), or the guard lives in
  `_transform_bytes`; the plan picks the single site (prefer `warp.py` so the guard is shared by
  `to_webmercator` and any future warp caller).
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` — `shape_output` docstring
  correction only (behavior already correct); no logic change.
- `python/geobrix/test/pyrx/test_virtual_aware_family.py` — add identity-transform passthrough test +
  the pixel-producer provenance-coherence test (merge/transform).
- Docs: the per-function override note already exists (Inc 4); correct the taxonomy wording in the
  Light-vs-Heavy blurb and stage the Virtual Tiles taxonomy content.
- Memory: correct `light-virtual-tiling-by-reference.md`.

## Error handling & edge cases

- **Source CRS with no EPSG code** (e.g. ESRI:54008, custom WKT): the identity check compares by EPSG
  code; if the source has no EPSG, `target_srid` can never equal "no code", so it correctly falls
  through to a real warp (never mis-short-circuits). Assert this path stays a warp.
- **`virtualize_dir` + `materialize=True`** — already mutually-exclusive (raises `ValueError`);
  unchanged.
- **Empty array / all-empty inputs** to combinators — already return `None`; unchanged.
- **Overlap sort-key parity** — the identity short-circuit returns input bytes verbatim, preserving
  the raw-bytes sort key that `merge_tiles` relies on for heavy parity. Assert an identity transform
  feeding a merge does not perturb the overlap winner.

## Testing

Local (no Spark, `.func` direct like the existing family tests):
- `test_rst_transform_identity_is_passthrough` — target == source EPSG returns input bytes verbatim
  (bitwise identical), no resample.
- `test_rst_transform_identity_virtual_is_noop_virtualize_dir` — identity on a virtual input with
  `virtualize_dir` returns the input virtual tile unchanged (bucket-1 no-op).
- `test_rst_transform_reproject_stamps_target_crs` — non-identity returns bytes whose embedded CRS is
  the target.
- `test_pixel_producer_virtualize_dir_roundtrips` — merge + transform `virtualize_dir` results open
  and match the materialized result's CRS/dims (provenance coherence).
- `test_transform_no_epsg_source_still_warps` — custom-WKT source never mis-short-circuits.

Serverless (throwaway notebook, fired directly, env v5, light extras): the three worker-side checks
in the Scope §4 list; self-report via `dbutils.notebook.exit(json)`; gate `all_ok == true`.

## Success criteria

- Identity `rst_transform` is a verified passthrough (no resample/re-encode), locally and on
  Serverless.
- `rst_merge`/combinator `virtualize_dir` results are proven-coherent (round-trip, correct union
  extent, correct CRS) on real `/Volumes`.
- The corrected three-bucket `virtualize_dir` taxonomy is documented in docstrings, the Light-vs-Heavy
  blurb, the staged Virtual Tiles content, and the memory blurb — and `grep` shows no surviving
  statement of the coarse "already-virtual → no-op" rule as if it were universal.
- No new registered functions; binding parity unchanged (zero-diff).
