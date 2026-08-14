# Design: Extend FILE/FILEREF support to all light-tier functions

- **Date:** 2026-08-14
- **Status:** Design (approved direction; spec under review)
- **Scope:** GeoBrix **light tier** (`pyrx`), virtual tiles. Extends the MVP.
- **Builds on:** `docs/superpowers/specs/2026-08-13-file-ref-central-read-design.md` (the MVP: feature-detect, `open_windowed_via_fileref`, `open_tile(file_ref=)`, 9 accessor bindings via 2-arg `_uf_*` UDFs + `file_ref_arg`). Same invariants apply.

## 1. Problem & goal
The MVP FILE-wired 9 accessors. This extends FILE/FILEREF to **all remaining tile-consuming light `rst_*` functions** — full coverage — rolled out in **validated groups** (each group's tests must pass, plus a dogfood spot-check, before the next group starts). The perf payoff (byte-range windowed reads via `fref.open()`) is concentrated in **pixel-reading** and **tile-producing** ops; header-only ops get UC-governed access (`as_local_file()`) without byte-range.

## 2. Goals / Non-goals
**Goals**
- Every tile-consuming light `rst_*` op uses FILE when available, else today's Volume-path fallback (byte-identical when `file_ref=None`).
- A reusable **tile-producing** FILE factory (the MVP only covered scalar accessors).
- Phased, validated rollout (Group 1 → 2 → 3), each gated on green tests + a dogfood check.

**Non-goals**
- Heavy tier; FILE *write* path.
- Changing the tile struct or any public signature.
- The reader-listing performance work (separate spec) and reader FILE support (separate, bench-justified).

## 3. Invariants (carried from the MVP — non-negotiable)
- pyrx stays **Serverless-safe**: no `.rdd`/`sparkContext`/`_jvm`/`_jsc`/`spark.conf.set`. `file_ref_arg` uses `getActiveSession()` + `F.call_function("try_to_file", tile_col['path'])`; `file_supported()` is the fixture-free detect (commit `fb7fb059`).
- **SQL registry stays single-arg** (`_u_*` / `gbx_rst_*`, positional). FILE wiring is Python-binding-only via **separate 2-arg `_uf_*`** UDFs. Never 2-arg the SQL path.
- `open_tile`/`_open`/`open_header` `file_ref=None` default → byte-identical fallback.
- **C1 guard applies to every FILE read that can warp/clip**: the FILE windowed fast-path is taken only when no clip (`clip_polygon is None`) and no reprojection is needed; otherwise it degrades to the local-path branch (which warps/clips). This is a hard prerequisite for any tile-producing op that clips/reprojects (`rst_clip`, `rst_reproject`).
- Tile struct unchanged; FileRef never stored/displayed — minted per-op, consumed in the UDF.

## 4. Grouping & validation gates
**Group 1 — remaining scalar accessors** (single input tile → scalar/array): `rst_avg`, `rst_min`, `rst_max`, `rst_median`, `rst_pixelcount`, `rst_histogram`, `rst_type`, `rst_getnodata`, and the coord fns (`raster_to_world_x/y`, `world_to_raster_x/y`). Pure extension of the proven 2-arg accessor pattern (`_header_accessor_udf_file` / `_pixel_accessor_udf_file` → new `_uf_*`; public binding calls `_uf_x(tc, file_ref_arg(tc))`). Pixel-reading ones (avg/min/max/median/pixelcount/histogram) go through the pixel path (`_open`) → byte-range win; the rest are header-only. **Lowest risk; mechanical.**

**Group 2 — single-input tile-producing ops** (read 1 tile → emit 1 tile): `rst_initnodata`, `rst_clip`, `rst_reproject`/`transformcrs`, `rst_resample`, `rst_updatetype`, `rst_setnodata`, `rst_threshold`, terrain (`slope`/`hillshade`/`aspect`), `setcrs`/`setsrid`. Uses the **new tile-producing factory** (§5). C1 guard mandatory for clip/reproject. Medium.

**Group 3 — multi-input / array / aggregator ops**: `rst_frombands` (tile *array* input), `rst_merge`, `rst_combineavg`, `rst_mapalgebra` (multi-input), `rst_*_agg` (grouped agg). Inject a FileRef **per input** — for an array input, `F.transform(tiles_col, lambda t: F.call_function("try_to_file", t['path']))`; for multi-arg ops, one `file_ref_arg` per input tile column; for aggregators, the read happens over many tiles per group (design the per-tile FileRef threading in this group). **Highest complexity.**

**Validation gate between groups (all three must pass before the next group starts):**
- (a) **local CI unit tests** (fallback + stub-FileRef): byte-range-equals-fallback for pixel ops; clip/warp-equivalence per C1 for tile-producing ops; the single-arg SQL registry entries still map to `_u_*`.
- (b) **dogfood correctness spot-check** on a FILE-enabled DBR 19 dedicated cluster (a couple of the group's ops read FILE == fallback, pixel-equal).
- (c) **scoped FILE-vs-Volume A/B** for the group's FILE-benefiting ops (pixel accessors / tile-producing input-reads / aggregators), run on the fixed 20-worker DBR 19 dedicated cluster via the same 3-leg harness scoped with `--functions <group's ops>` (materialized / virtual FILE-off / virtual FILE-on), over the `bench-corpus-1024-1k` corpus, into `bench_results`. This **flags the byte-range win/regression early** — a group showing ~no win is a signal to reprioritize before investing in the next group. Spin the 20-worker cluster up for the bench and shrink/terminate it after (don't hold 20 workers across the whole rollout).
Do not start the next group until (a)+(b)+(c) are green/measured.

## 5. The tile-producing FILE factory (crux, Group 2+)
Add a factory that produces a UDF taking `(input_tile, file_ref, *op_args)`:
- reads the input via `with open_tile(input_tile, file_ref=file_ref) as ds:` (FILE fast-path per C1, else fallback),
- runs the op's `core_fn(ds, *op_args)` to produce the **output tile bytes/struct** exactly as today.
The public binding threads `file_ref_arg(input_tile_col)` alongside the op's existing args **and** its force-output kwargs (`virtualize_dir`/`virtualize_prefix`/`materialize`): e.g. `rst_initnodata(tile, …) → tc=_col(tile); _uf_initnodata(tc, file_ref_arg(tc), …)`. The single-arg SQL variant (no file_ref) stays registered for `gbx_rst_*`. Positional-binding care: the file_ref arg slots in a fixed position the UDF expects; the SQL path never passes it.

## 6. Testing
- CI (FILE absent → `file_supported()` False → fallback): every group's ops behave byte-identically to today; new stub-FileRef tests prove the FILE branch equals the fallback (pixel-equal), incl. clip/warp cases (C1). Reuse the MVP test patterns; build WKB via `struct.pack` (CI has no shapely).
- Dogfood spot-check per group on a FILE-enabled classic DBR 19 dedicated cluster. **Wheel staging (canonical):** wheel on the Volume at `/Volumes/geospatial_docs/geobrix/sample-data/geobrix/geobrix-<ver>-py3-none-any.whl` (staged via FUSE copy from the cluster — dogfood DBFS `fs` API is admin-gated), installed `%pip install "geobrix[light] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix/geobrix-<ver>-py3-none-any.whl"`.

## 7. Rollout
Ships in a 0.5.x release, group by group. Dormant-with-fallback everywhere FILE isn't enabled. How aggressively to prioritize the byte-range groups is informed by the FILE-vs-Volume A/B results (in progress); the mechanism is settled here regardless.

## 8. Next step
On spec approval → writing-plans (one plan, staged by the three groups, each group a validated milestone). Subagent-driven execution, TDD, per the MVP's proven loop.
