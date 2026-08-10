# RasterX Tabbed Examples — Corrected Design (one co-designed example per function)

**Date:** 2026-08-10
**Status:** ratified (this doc), pending plan
**Supersedes:** `2026-08-10-complete-rasterx-tabbed-examples-design.md` (the "authoring model / batching" spec). The tabbing MECHANISM (FunctionExamples wrapper, generate-function-info.py bindings text-scan glob, gbx-example-lang CSS) is unchanged and reused as-is; what changes is the AUTHORING STANDARD for the examples themselves.
**Scope:** RasterX API docs page (`docs/docs/api/raster-functions.mdx`) + its per-tier example doc-tests. The corrected standard also governs the eventual GridX/VectorX pages.

## The problem this corrects

The first pass authored each tier's example INDEPENDENTLY, so the four tabs of one function drifted to DIFFERENT examples — different fixtures, inputs, even operations. E.g. `rst_height`: SQL queries a `rasters` view; light uses a synthetic in-memory `_tile_df(4x3)` → `3`; Scala loads `nyc_sentinel2_red.tif`. The earlier "output normalization" only matched output *strings* (cosmetic), not the underlying example (structural). A reader flipping tabs cannot tell whether a difference is a genuine tier difference, a different chosen example, or an inaccurate doc — the tabs look like they were not co-designed. This spans ALL of Batch A (34 functions), not the 2 originally flagged, and must reshape the authoring contract for the remaining families and other packages.

## The ratified standard (user)

**For each function there is ONE example**, shared across all four tabs: the same input fixture, the same operation, the same argument values — expressed in each tier's language (SQL / pyrx-light / rasterx-heavy / Scala), rendered in each tier's NATURAL form. Where a tier's rendering genuinely differs, use a consistent shorthand + a short clarifying NOTE (e.g. `...` + "(WKB binary)" for geometry; `SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> ...` for subdataset maps; similar for band metadata maps). The goal: a reader never has to reason about *why* tabs differ — sameness is visible, residual formatting differences are noted.

## Core model — the example IS the invocation on a conventional `tile`

The tabs drifted because each tier hand-authored a full standalone example. The correction removes that freedom: **a function's example IS its invocation on a conventionally-defined `tile` column — nothing more.** A page-level Conventions section defines the shared setup ONCE; each function's four tabs then show only the tier-idiomatic invocation of that function on `tile`. The tabs are therefore structurally identical BY CONSTRUCTION, not by hand-matched strings.

- SQL tab: `SELECT gbx_rst_height(tile) AS height FROM rasters`
- Python-light: `df.select(rx.rst_height("tile").alias("height"))`
- Python-heavy: `df.select(rx.rst_height("tile").alias("height"))` (heavy shim import)
- Scala: `rasters.select(rx.rst_height(col("tile")).alias("height"))`

`rasters` (SQL table) / `df` (Python DataFrame) / `rasters` (Scala DataFrame) all mean "the canonical sample loaded as a `tile` column," per the Conventions section — so the input cannot drift between tabs.

## The Conventions section (TOP of the Functions reference)

A single authored MDX section leading the function reference — after the page intro/setup, BEFORE the first family section (`## Accessor Functions`) — so every reader meets it before any individual function. It states, once, RasterX/pyrx-specifically:

1. **Canonical sample files** and what each demonstrates:
   - **single-band GeoTIFF** — `nyc_sentinel2_red.tif` — the DEFAULT for most functions.
   - **multiband GeoTIFF** — a NEW small committed fixture (see Fixtures) — for band-math, `rst_numbands`, `rst_bandmetadata`.
   - **DEM** — `srtm_n40w073.tif` — for terrain (slope/aspect/hillshade/…).
   - **NetCDF** — `nyc_climate.nc` (temperature/precipitation vars) — ONLY for multi-layer functions (`rst_subdatasets`, `rst_getsubdataset`).
2. **The `tile`-column convention:** across every example, `rasters` is a table (SQL) / a DataFrame (light Python, heavy Python, Scala) with a `tile` column sourced from the canonical file(s). `FROM rasters` / `df` means exactly that — stated here so no per-function boilerplate is needed.
3. **How to read the tabs:** SQL is the default tab; then Python-light, Python-heavy (blue), Scala (blue); each shows the same example in that tier's language.
4. **Output-note convention:** outputs render in each tier's natural form; where a form differs (binary geometry, subdataset/metadata maps) a shorthand + one-line note clarifies. So a difference is never left as a mystery.
5. **Per-function override rule:** if a function uses a NON-default fixture (multiband / DEM / NetCDF) or must show a fuller example (constructors), a short per-function note flags the exception against these defaults.

## Fixtures (canonical set)

| Fixture | Source | Used by |
|---|---|---|
| single-band GeoTIFF | `nyc_sentinel2_red.tif` (existing) | default: most accessors, tile-ops, transforms |
| **multiband GeoTIFF** | **NEW committed fixture** — a tiny real N-band GeoTIFF (e.g. red+NIR, ~2–3 bands) generated once and checked in under `sample-data/Volumes/...` | band-math (`rst_ndvi`/`evi`/`mapalgebra`/…), `rst_numbands`, `rst_bandmetadata` |
| DEM | `srtm_n40w073.tif` (existing) | terrain family |
| NetCDF | `nyc_climate.nc` (existing) | `rst_subdatasets`, `rst_getsubdataset` |
| aggregation set | a small set of `bench-corpus/rows/r*.tif` tiles (or several tiles of the canonical GeoTIFF) | `*_agg` (need multiple `tile` rows) |

**The multiband fixture does not exist yet** — no confirmed multiband sample was found (sentinel2 files are single-band `_red`). Constructing + committing it is an EXPLICIT EARLY TASK and a hard dependency for the band-math batch.

## Exceptions to bare-invocation (each flagged by a per-function note)

- **Constructors** (`rst_fromfile`, `rst_fromcontent`, `rst_frombands`, `rst_gridfrompoints`, `rst_dtmfromgeoms`): they PRODUCE a tile rather than consume one, so "invoke on `tile`" doesn't apply — show the fuller example (the load/build), consistent across their tabs. Sanctioned fuller-example fallback.
- **Multi-layer / NetCDF** (`rst_subdatasets`, `rst_getsubdataset`): bare invocation on `tile`, but the canonical NetCDF source, with a note ("uses `nyc_climate.nc`; subdatasets require a multi-layer format").
- **Fixture-specific families:** band-math/`numbands`/`bandmetadata` note the multiband fixture; terrain notes the DEM; `*_agg` note the multi-tile set (`GROUP BY` over several `tile` rows). Same invocation model, non-default source, flagged.

## Per-function shape

Heading → optional one-line per-function fixture note (only if non-default) → `<FunctionExamples>` 4-tab block (tier-idiomatic invocation on `tile`) → shared output (natural form + shorthand/note where forms differ). The doc-test backing each tab remains a REAL executable test that loads the conventional fixture and asserts the real value; the EXAMPLE SHOWN is the invocation, the TEST around it is real — both hold.

## Doc-test / shown-code relationship

The `*_python_light_example` / `*_python_heavy_example` / `*_scala_example` / `*_sql_example` functions still exist and still execute+assert (that's what earns the green code-indicators checkmark and keeps tests-are-the-source true). The refactor is that the SHOWN snippet is the bare invocation (the shared fixture load lives in a shared setup helper the example calls, not inline per function), so what renders in the tab is the consistent invocation while the test still runs end-to-end. Reuse/extend the existing per-family light files + heavy/scala example files; do NOT touch the mechanism.

## Review checks (every batch, incl. B–G and other packages)

Per function, the batch review MUST confirm:
- (a) all present tabs invoke THIS function (no wrong-function/aliased examples);
- (b) a real per-function SQL example exists (no raw/empty SQL tab — the missing-`*_sql_example` class);
- (c) output is non-degenerate and real (no `{}`/placeholder where a real value exists);
- (d) **ALL tabs use the same example** — same fixture + operation + args (the structural check that was missing);
- (e) any non-default fixture or fuller-example is noted per-function;
- (f) headings are alphabetical within their family section.

## Redoing Batch A

Batch A (commit `a758b027`) was built under the drifted model. Under this standard it is **re-authored, not patched**: write the Conventions section; unify all 34 accessor functions onto bare-invocation-on-conventional-`tile`; eliminate per-tab drift; apply the alphabetical-within-family heading order. The already-written doc-tests are salvage material (their values are real) but examples-shown + fixtures are unified. This is the first execution task of the corrected plan.

## Heading order

Alphabetical within each family section (`## Accessor Functions`, `## Aggregator Functions`, …); NOT one global A–Z (family grouping preserved). Combined headings (`rst_scalex / rst_scaley`) stay single blocks sorted by first name. The right-sidebar TOC auto-follows heading order.

## Out of scope

- The tabbing MECHANISM (FunctionExamples, generator, CSS) — reuse as-is.
- GridX/VectorX pages — later passes, but they INHERIT this corrected standard (Conventions section per page, one-example-per-function, the review checks).
- Product/binding code — docs-only; a genuinely-missing binding is a surfaced finding, never a fabricated example.

## Outcome

Every RasterX function renders as a 4-tab block whose tabs are unmistakably the SAME example (same fixture, operation, args), because the example is just the invocation on a conventionally-defined `tile` established once in a Conventions section at the top of the reference. Tier-natural output, with shorthand+notes only where a rendering genuinely differs. No reader is left guessing why tabs differ. Batch A is re-authored to this standard; B–G and other packages follow it from the start.
