# Rasterio Distributed — RasterX positioning page + cross-links

**Date:** 2026-07-28
**Status:** Approved (brainstorming)
**Branch:** `beta/0.4.0`

## Goal

Make deliberate "noise" about a factual, defensible claim: **RasterX's lightweight
(`pyrx`) tier is, in large part, distributed `rasterio` + a best-of-breed Python raster
stack** — run as Arrow UDFs / UDTFs across a Spark cluster, not a single-node
`rasterio.open` loop and not a reimplementation. Ship one positioning page that a
**rasterio user evaluating GeoBrix** reads to decide "yes, my workflow moves here," and
weave a short blurb + link into the highest-traffic entry points.

Honesty is load-bearing: the page must be trusted, so it states plainly what is
distributed today, what is heavyweight-only *for now* (roadmap-forward), and where output
**diverges** from rasterio/GDAL.

## Audience & framing

- **Primary audience:** rasterio users evaluating GeoBrix (positioning + migration bridge).
- **Honesty framing:** roadmap-forward, expressed in three buckets — distributed today /
  heavyweight-only for now / known behavior divergences.

## Factual basis (verified against code, 2026-07-28)

`pyrx` `[light]` deps (from `python/geobrix/pyproject.toml`): rasterio `>=1.3`, rio-tiler
`>=9.0,<9.3`, rio-cogeo `>=7,<8`, numpy, scipy `>=1.11`, numexpr, xarray-spatial `>=0.4,<1`,
pyproj, shapely `>=2`, pyogrio, h3, quadbin, scikit-image, pmtiles, morecantile, netcdf4.
Deliberately **no** `osgeo.gdal` — GDAL arrives via rasterio's bundled build.

Distribution mechanism (`pyrx/_udf.py`, `functions.py`): Arrow scalar `pandas_udf`
(per-tile), Arrow grouped-aggregate `pandas_udf` (merges), streaming `@udtf` (fan-out).
No driver-side `.rdd` / `_jvm` / `spark.conf.set`. Plus DataSource V2 readers/writers in
`ds/`.

Backing-library map (by `pyrx/core/` module): rasterio → I/O, warp, clip, resample, merge,
COG, metadata; NumPy(+numexpr) → band math, spectral indices, **terrain (Horn 3×3, pure
NumPy)**; scipy → focal (`ndimage.convolve`), proximity (`distance_transform_edt`), TIN
(Delaunay); rio-tiler → XYZ/web tiles; rio-cogeo → COG; scikit-image → contour; xarray-
spatial → **viewshed only**; shapely → geometry/rasterize/polygonize; pyproj → CRS scale;
h3/quadbin → grid cells.

### Honest gaps (three buckets)

1. **Distributed in pyrx today** — the coverage matrix (I/O & metadata, warp/reproject,
   clip/mask, resample, merge, COG, band math/indices, terrain, rasterize/polygonize,
   focal, proximity/contour/viewshed, tiling/XYZ, grid aggregation).
2. **Heavyweight-only for now** (framed as "runs in the GDAL tier; lightweight parity
   tracked"): OGR vector readers (`*_ogr`), `conforming` TIN mode (pyrx raises
   `NotImplementedError`; `constrained` works in both), advanced PMTiles DataSource writer
   options (the `gbx_pmtiles_agg` aggregate is in both tiers). Also: SQL default arguments
   are a heavyweight convenience (pyrx SQL requires explicit args).
3. **Known behavior divergences** (stated plainly + why, linking benchmarking/perf):
   `rst_color_relief` (GDAL DEMProcessing vs NumPy `np.interp`; no `default` keyword in
   pyrx), `rst_convolve` / `rst_derivedband` (edge handling: GDAL halo vs NumPy
   `pad(mode='edge')`), `rst_resample` (NoData/edge boundary pixels), `rst_contour`
   (`gdal.ContourGenerateEx` vs `skimage.measure.find_contours`), `rst_viewshed`
   (`gdal.ViewshedGenerate` vs `xrspatial.viewshed` — different semantics, xrspatial errors
   on off-grid observer where heavy clamps). Also: rasterio's bundled GDAL has a narrower
   driver set than the heavyweight custom build.

## Deliverables

### 1. New page — `docs/docs/api/rasterio-distributed.mdx`

Sidebar-wired under **Functions → RasterX** (child of the RasterX category in
`sidebars.js`, alongside `api/h3-raster-tessellation`). `sidebar_label: "Rasterio,
Distributed"`. Structure:

1. **Hook** — the claim, stated crisply with the stack named.
2. **Flagship side-by-sides (3, tested):** single-node **rasterio** snippet next to the
   distributed **pyrx** equivalent for **warp/reproject, clip, band math/NDVI**. Both
   sides imported from a tested example module via `CodeFromTest`.
3. **Coverage matrix** — rasterio/GDAL capability → pyrx function → backing library,
   grouped as above. Links to the existing tested function-reference pages for runnable
   detail (no per-row new tests).
4. **Honest gaps** — the three buckets above, roadmap-forward.
5. **Distribution mechanism** — brief: Arrow scalar / grouped-agg UDFs, streaming UDTFs,
   DataSource V2; no driver-side execution.

### 2. Tested example module (tests-are-the-doc-source)

- `docs/tests/python/api/rasterio_distributed_examples.py` — named snippet constants
  (`WARP_RASTERIO` / `WARP_PYRX`, `CLIP_*`, `NDVI_*`, `REGISTER`) + verifier functions.
- `docs/tests/python/api/test_rasterio_distributed_examples.py` — synthesizes tiny rasters,
  executes **both** the rasterio side and the pyrx side, and **asserts they agree** within
  a documented tolerance (byte- or value-level as appropriate per op). This proves the
  equivalence the page claims, not merely that pyrx runs.
- Note: `docs/tests/python/api/` is the SQL/api doc-test suite (run via
  `gbx:test:python-docs --suite api`, excluded from the default python-docs run) — place
  the test so it's picked up by that suite.

### 3. Cross-links (short blurb + link each)

- **Homepage** `docs/src/pages/index` (RasterX card): one line — "In large part,
  distributed rasterio + best-of-breed raster packages →".
- **`docs/docs/intro.mdx`**: a sentence in the existing two-tiers paragraph, linking the page.
- **`docs/docs/api/execution-tiers.mdx`**: a sentence near the "rasterio + NumPy" table row,
  linking the page.
- **`docs/docs/api/raster-functions.mdx`**: a "See also" link near the top.

### 4. Correctness fix folded in

`docs/docs/api/performance.mdx` currently attributes **terrain (slope, hillshade, aspect,
tri, tpi, roughness, viewshed)** to xarray-spatial. Code shows terrain is **pure NumPy
(Horn 3×3)**; only **viewshed** uses xarray-spatial. Correct that line so the new page and
`performance.mdx` agree and neither overclaims.

## Voice / constraints

- No internal planning vocab (no "wave N"); user-facing voice per CLAUDE.md.
- Justify by user utility, not Mosaic parity.
- Every new `.mdx` wired into `sidebars.js` in the same change (standing rule).
- Roadmap-forward claims must not imply a hard commitment/date; "for now" / "tracked", not
  "coming in X".

## Verification

1. New doc-test green in Docker (`gbx:test:python-docs --suite api` or targeted `--path`).
2. Docs static build clean — zero broken links/anchors (the build is the authority).
3. Browser screenshot pass on the new page + one cross-link site (agent uses a non-3000
   port, e.g. `--port 3001`).

## Out of scope

- No new pyrx functions or code changes (docs + tests + one attribution fix only).
- No push; lands on `beta/0.4.0` as its own commit(s), unpushed.
- Broader rasterio-parity engineering (closing the heavyweight-only gaps) is separate work.

## Landing

Own commit(s) on `beta/0.4.0`, stacked after the tier-tab color-cue commit. Unpushed until
the user says otherwise; suggest `/review` before any push.
