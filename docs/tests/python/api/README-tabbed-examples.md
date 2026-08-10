# Authoring tabbed function examples (RasterX / GridX / VectorX)

This is the checked-in standard for the per-function **4-tab code examples** on the API docs
pages (`docs/docs/api/*-functions.mdx`): **SQL** (default) / **Python (light)** / **Python
(heavy)** (blue) / **Scala** (blue). Read this before authoring or reviewing any batch. It
exists because the standard was learned the slow way through review — follow it up-front so the
work is *standardizing*, not *figuring out how to standardize*.

Full design: `docs/superpowers/specs/2026-08-10-rasterx-tabbed-examples-corrected-design.md`.

## The one rule everything follows

**For each function there is ONE example, shared across all four tabs.** Same input fixture,
same operation, same argument values — expressed in each tier's language. The example IS the
tier-idiomatic *invocation of that function* on a conventionally-defined `tile`. Tabs must be
visibly the SAME example; a reader must never wonder whether a tab difference is a real tier
difference, a different chosen example, or an inaccurate doc.

## Conventions section (top of the page, authored once)

Every API page opens (after `## Tier availability`, before the first function family) with a
**Conventions** section stating, once: the canonical sample files; that `rasters` (SQL) / `df`
(Python & Scala) is a table/DataFrame with a `tile` column loaded from the sample; how to read
the tabs; and the output-note convention. Individual functions therefore show only the
**invocation** — never a per-function load.

## What each tab shows

- **The shown code invokes THE NAMED FUNCTION and nothing wraps it to change the result type.**
  NEVER wrap a tile-returning function in a derived accessor to manufacture a clean scalar
  (`rst_width(rst_clip(...))`, `rst_format(rst_fromfile(...))`) — that showcases the *wrong*
  function. `rst_clip`'s example shows `rst_clip` and *its* output.
- SQL tab: `SELECT gbx_rst_X(tile) AS ... FROM rasters`. Python: `df.select(rx.rst_X("tile"))`.
  Scala: `rasters.select(rx.rst_X(col("tile")))`.

## What the output shows (by return type)

- **Scalar-returning** (`rst_height`, `rst_srid`, `rst_numbands`, `rst_format`, …): the real
  scalar, **identical across all tiers** (same fixture → same value). No placeholders
  (`[<float>]`), no `{}` where a real value exists.
- **Tile/struct-returning** (`rst_clip`, `rst_resample`, `rst_transform`, the constructors,
  `rst_getsubdataset`, the `*_agg` family, …): the `_output` code block shows a **representative
  v2-Tile struct**, e.g. `{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}` — the
  light-tier virtual-tile shape (light defaults to **virtual tiles**, so its tile carries a
  populated `path`). Use that (or similar) for all tile output. Do **not** put a markdown link
  in the `_output` block — it is rendered inside a `<CodeBlock language="text">`, so markdown
  like `[Tile structure](./tile-structure)` renders LITERALLY, not as a link. The clickable
  "**v2 Tile**" pointer to `./tile-structure` lives ONCE in the page **Conventions** section
  (prose, where markdown renders), not per function. Do **not** use the OLD v1 3-field
  `{cellid, raster, metadata}` / `STRUCT<...>` form. Note: the light tab (virtual, `path`
  populated) and the heavy tab (materialized) tile shapes legitimately DIFFER per tier — that
  is correct per-tier rendering, not drift.
- **Geometry-returning** (`rst_boundingbox`): `...` + `(WKB binary)` shorthand + note.
- Never let SQL bleed into a Python/Scala tab; each tab is idiomatic to its language.

## Doc-tests are the source

Every tab is backed by a REAL executable doc-test that loads the canonical fixture (via the
shared helpers in `_fixtures.py`) and asserts the real value. The *shown* snippet is the bare
invocation; the *test* runs end-to-end. Naming the generator scans for, exactly:
`def <base>_python_light_example(spark)` / `def <base>_python_heavy_example(spark)` /
`val <base>_scala_example` / `def <base>_sql_example()` — each with a `<base>_..._example_output`
constant. **Every function needs a real per-function SQL example** or its SQL tab renders raw.

## Fixtures (canonical)

| Fixture | Path | Used by |
|---|---|---|
| single-band GeoTIFF | `nyc_sentinel2_red.tif` (/Volumes mount) | default (most functions) |
| multiband GeoTIFF | `src/test/resources/binary/geotiff-small/rgb_nir_small.tif` (committed — `sample-data/Volumes/...geobrix_samples/` is gitignored) | band-math, `rst_numbands`, `rst_bandmetadata` |
| DEM | `srtm_n40w073.tif` (/Volumes mount) | terrain |
| NetCDF | `src/test/resources/binary/netcdf-CMIP5/` (committed) | `rst_subdatasets`, `rst_getsubdataset` only |
| multi-tile set | a few `bench-corpus/rows/r*.tif` (or several tiles) | `*_agg` |

Shared loaders in `_fixtures.py`: `single_band_tile_df` / `multiband_tile_df` / `dem_tile_df` /
`netcdf_tile_df` (+ heavy equivalents). A non-default fixture (multiband/DEM/NetCDF) or a fuller
example (constructors, which *build* a tile) gets a short per-function note.

## Code-indicators checkmark

Every present tab must earn the green "🔗 Fully Validated" badge: `FunctionExamples` passes
`source`/`testFile`/`functionName`, and every example file lives under `docs/tests/…` — NEVER
under `integration/` or `tests-dbr/` (those downgrade the badge).

## Heading order

Alphabetical **within each family section** (`## Accessor Functions`, `## Aggregator Functions`,
…) — not one global A–Z; family grouping is preserved. The right-sidebar TOC follows heading
order.

## Review checklist (every batch)

Per function, confirm: (a) all present tabs invoke THIS function; (b) a real per-function SQL
example exists (no raw tab); (c) output is non-degenerate + real; (d) **all tabs are the SAME
example** (fixture + operation + args); (e) any non-default fixture / fuller example is noted;
(f) tile/struct outputs use the v2-Tile note (not v1 fields, not a wrapped accessor); (g)
headings alphabetical within family.
