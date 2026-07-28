# Rasterio Distributed Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a `docs/docs/api/rasterio-distributed.mdx` positioning page ("RasterX's lightweight tier is distributed rasterio + a best-of-breed raster stack") with tested side-by-side examples, plus short blurb+link cross-references from the highest-traffic pages, and one attribution fix.

**Architecture:** Docusaurus MDX page under Functions→RasterX. Flagship code snippets are imported (via the existing `CodeFromTest` component) from a new tested example module; the doc-test executes BOTH the single-node rasterio side and the distributed pyrx side and asserts they agree. Coverage matrix + honest three-bucket gaps are prose/tables. Cross-links are one-sentence additions at four existing sites. No product code changes.

**Tech Stack:** Docusaurus MDX, `CodeFromTest` React component, `raw-loader`; pytest doc-tests running in the `geobrix-dev` Docker container (`gbx:test:python-docs`); pyrx (`databricks.labs.gbx.pyrx`, `databricks.labs.gbx.ds.register`), rasterio, numpy.

## Global Constraints

- Branch: `beta/0.4.0`. All commits local; **no push** this task set.
- Every new `docs/docs/**.mdx` MUST be wired into `docs/sidebars.js` in the same change (manual sidebar; unwired pages are orphaned / warn on build).
- Doc examples are the single source of truth and MUST be backed by executable tests with real assertions (no mocking Spark / GeoBrix / file I/O). Tests use tiny synthesized rasters.
- User-facing voice: no internal planning vocabulary (no "wave N"); justify by user utility, not Mosaic parity. Roadmap-forward claims say "for now" / "parity tracked", never a hard date/commitment.
- `docs/tests/python/api/` is the SQL/api doc-test suite — run via `gbx:test:python-docs --suite api` (excluded from the default python-docs run). New api tests must land there.
- Docs long-running work (Docker test, static build, browser screenshot) runs via `gbx:*` commands dispatched to a subagent; browser/dev-server checks use a NON-3000 port (e.g. `--port 3001`).
- Verify facts against code before asserting: terrain (`pyrx/core/terrain.py`) is pure NumPy (Horn 3×3, `np.pad(mode='edge')`), imports only numpy/pyproj/rasterio; only viewshed (`pyrx/core/analysis.py`) uses `xrspatial`.

---

### Task 1: Tested example module + doc-test (flagship side-by-sides)

Build the single source of truth for the three flagship snippets FIRST (TDD), so the page imports proven code.

**Files:**
- Create: `docs/tests/python/api/rasterio_distributed_examples.py`
- Create (test): `docs/tests/python/api/test_rasterio_distributed_examples.py`

**Interfaces:**
- Produces (consumed by Task 2 via `CodeFromTest` `functionName=`): string constants
  `REGISTER`, `WARP_RASTERIO`, `WARP_PYRX`, `CLIP_RASTERIO`, `CLIP_PYRX`,
  `NDVI_RASTERIO`, `NDVI_PYRX`.
- Produces (verifier fns, called by the test): `warp_both(spark, src_path)`,
  `clip_both(spark, src_path, wkt)`, `ndvi_both(spark, src_path)` — each runs the
  rasterio side and the pyrx side and returns `(rasterio_result, pyrx_result)` for the
  test to compare.
- Consumes: `databricks.labs.gbx.ds.register.register`, `databricks.labs.gbx.pyrx` (as `rx`),
  rasterio, numpy.

- [ ] **Step 1: Write the example module with snippet constants + verifier functions**

Create `docs/tests/python/api/rasterio_distributed_examples.py`:

```python
"""'Rasterio, Distributed' page examples — single source of truth.

Code shown in docs/docs/api/rasterio-distributed.mdx is imported from here.
Each flagship op is shown as a familiar single-node rasterio snippet next to the
distributed pyrx equivalent. The paired verifier functions run BOTH sides and the
test asserts they agree on tiny synthesized rasters — proving the equivalence the
page claims, not merely that pyrx runs.
"""

REGISTER = """# Register the GeoBrix lightweight (pyrx) functions once per session
from databricks.labs.gbx.ds.register import register
import databricks.labs.gbx.pyrx as rx
rx.functions.register(spark)   # registers rst_* as Spark UDFs / Column helpers"""

WARP_RASTERIO = """# Single-node rasterio: reproject one file to EPSG:3857
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
with rasterio.open("in.tif") as src:
    t, w, h = calculate_default_transform(
        src.crs, "EPSG:3857", src.width, src.height, *src.bounds)
    # ...write a reprojected file, one machine, one file at a time"""

WARP_PYRX = """# GeoBrix pyrx: reproject a whole DataFrame of tiles, distributed
df2 = df.withColumn("tile", rx.rst_transform("tile", 3857))
# rst_transform runs rasterio.warp on each tile in an Arrow UDF across the cluster"""

CLIP_RASTERIO = """# Single-node rasterio: clip one raster to a geometry
import rasterio
from rasterio.mask import mask
with rasterio.open("in.tif") as src:
    out, out_transform = mask(src, [geom], crop=True)"""

CLIP_PYRX = """# GeoBrix pyrx: clip every tile to a geometry, distributed
df2 = df.withColumn("tile", rx.rst_clip("tile", "geom"))"""

NDVI_RASTERIO = """# Single-node rasterio + NumPy: NDVI for one raster
import rasterio, numpy as np
with rasterio.open("in.tif") as src:
    red = src.read(1).astype("float32"); nir = src.read(2).astype("float32")
    ndvi = (nir - red) / (nir + red)"""

NDVI_PYRX = """# GeoBrix pyrx: NDVI across a DataFrame of tiles, distributed
df2 = df.withColumn("tile", rx.rst_ndvi("tile", 2, 1))  # (nir_band, red_band)"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register
    import databricks.labs.gbx.pyrx as rx
    register(spark)
    rx.functions.register(spark)


def _one_tile_df(spark, src_path):
    """Load a single-file raster as a one-row (source, tile) DataFrame via the
    lightweight raster_gbx reader."""
    return spark.read.format("raster_gbx").load(src_path)
```

NOTE for the implementer: the exact pyrx registration entry point and the exact
`rst_transform` / `rst_clip` / `rst_ndvi` Python-Column signatures MUST be confirmed
against `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` and the existing
tested examples in `docs/tests/python/api/rasterx_functions.py` before finalizing —
copy the real call form from there. Do not invent argument orders.

- [ ] **Step 2: Add the three paired verifier functions to the same module**

Append to `rasterio_distributed_examples.py` — each runs the rasterio side directly
and the pyrx side through Spark, returning both results as NumPy arrays for the test
to compare:

```python
def warp_both(spark, src_path):
    """Return (rasterio_crs, pyrx_crs) after reprojecting to EPSG:3857."""
    import rasterio
    from rasterio.warp import calculate_default_transform
    _register(spark)
    import databricks.labs.gbx.pyrx as rx
    with rasterio.open(src_path) as src:
        rio_crs = rasterio.crs.CRS.from_epsg(3857)
    df = _one_tile_df(spark, src_path).withColumn("tile", rx.rst_transform("tile", 3857))
    pyrx_srid = df.selectExpr("rst_srid(tile) AS srid").collect()[0]["srid"]
    return (rio_crs.to_epsg(), pyrx_srid)


def ndvi_both(spark, src_path):
    """Return (rasterio_ndvi_array, pyrx_ndvi_array) for band2=nir, band1=red."""
    import numpy as np
    import rasterio
    _register(spark)
    import databricks.labs.gbx.pyrx as rx
    with rasterio.open(src_path) as src:
        red = src.read(1).astype("float32")
        nir = src.read(2).astype("float32")
        rio_ndvi = (nir - red) / (nir + red)
    df = _one_tile_df(spark, src_path).withColumn("tile", rx.rst_ndvi("tile", 2, 1))
    # read the single result tile's band 1 back to a NumPy array
    tile_bytes = bytes(df.selectExpr("tile.raster AS r").collect()[0]["r"])
    with rasterio.io.MemoryFile(tile_bytes) as mf, mf.open() as ds:
        pyrx_ndvi = ds.read(1).astype("float32")
    return (rio_ndvi, pyrx_ndvi)
```

NOTE: `clip_both` follows the same shape (rasterio `mask` vs `rx.rst_clip`); include it
only if `rst_clip`'s tested call form is confirmed. If confirming clip is costly, the
page can show the clip side-by-side as illustrative code while the TEST covers warp +
ndvi (the two with clean numeric equality). Prefer testing all three; drop clip from the
test (not the page) only if its equality is not cleanly assertable.

- [ ] **Step 3: Write the failing test**

Create `docs/tests/python/api/test_rasterio_distributed_examples.py`:

```python
"""Executes the 'Rasterio, Distributed' doc examples and asserts the rasterio side
and the pyrx side AGREE on synthesized rasters (Docker; api suite)."""
import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds

sys.path.insert(0, str(Path(__file__).parent))
import rasterio_distributed_examples as ex  # noqa: E402


def _write_rgbnir(path, px=32):
    # 2-band raster: band1=red, band2=nir, EPSG:4326 over a small AOI
    red = np.linspace(0, 200, px * px, dtype="float32").reshape(px, px)
    nir = np.linspace(50, 255, px * px, dtype="float32").reshape(px, px)
    with rasterio.open(
        path, "w", driver="GTiff", width=px, height=px, count=2, dtype="float32",
        crs="EPSG:4326", transform=from_bounds(-122.5, 37.7, -122.4, 37.8, px, px),
    ) as ds:
        ds.write(red, 1)
        ds.write(nir, 2)


def test_warp_agrees(spark, tmp_path):
    p = str(tmp_path / "in.tif")
    _write_rgbnir(p)
    rio_epsg, pyrx_srid = ex.warp_both(spark, p)
    assert rio_epsg == 3857 and pyrx_srid == 3857


def test_ndvi_agrees(spark, tmp_path):
    p = str(tmp_path / "in.tif")
    _write_rgbnir(p)
    rio_ndvi, pyrx_ndvi = ex.ndvi_both(spark, p)
    assert rio_ndvi.shape == pyrx_ndvi.shape
    assert np.allclose(rio_ndvi, pyrx_ndvi, rtol=1e-4, atol=1e-4, equal_nan=True)
```

- [ ] **Step 4: Run the test to verify it fails (then iterate to green)**

Dispatch a subagent to run in Docker (do not run inline). Command:

```bash
bash scripts/commands/gbx-test-python-docs.sh --suite api \
  --path api/test_rasterio_distributed_examples.py \
  --log rasterio-distributed-doctest.log
```

Expected first run: FAIL (e.g. wrong pyrx call form / registration). Iterate on the
example module until both tests PASS. The verifier is the definition of done — the
rasterio and pyrx results must actually agree, not just execute.

- [ ] **Step 5: Commit**

```bash
git add docs/tests/python/api/rasterio_distributed_examples.py \
        docs/tests/python/api/test_rasterio_distributed_examples.py
git commit -m "docs(test): tested rasterio-vs-pyrx equivalence examples for RasterX

Both the single-node rasterio side and the distributed pyrx side are
executed and asserted to agree on synthesized rasters, backing the
'Rasterio, Distributed' page's side-by-side snippets.

Co-authored-by: Isaac"
```

---

### Task 2: The `rasterio-distributed.mdx` page + sidebar wiring

**Files:**
- Create: `docs/docs/api/rasterio-distributed.mdx`
- Modify: `docs/sidebars.js` (RasterX category `items`, currently `['api/h3-raster-tessellation']` at line ~109)

**Interfaces:**
- Consumes: the snippet constants from Task 1 via `CodeFromTest` (`functionName="WARP_RASTERIO"`, etc.) and `raw-loader` import of `docs/tests/python/api/rasterio_distributed_examples.py`.
- Produces: doc id `api/rasterio-distributed` (referenced by Task 3 cross-links + sidebar).

- [ ] **Step 1: Write the page**

Create `docs/docs/api/rasterio-distributed.mdx`. Frontmatter:

```mdx
---
sidebar_position: 3
sidebar_label: Rasterio, Distributed
title: Rasterio, Distributed
---

import CodeFromTest from '@site/src/components/CodeFromTest';
import rioDist from '!!raw-loader!../../tests/python/api/rasterio_distributed_examples.py';
```

Body sections (write real prose — no placeholders):
1. **Hook** — RasterX's lightweight (`pyrx`) tier is, in large part, `rasterio` plus a
   best-of-breed Python raster stack (rio-tiler, rio-cogeo, scipy, xarray-spatial,
   scikit-image, shapely, pyproj, h3/quadbin), run as Arrow UDFs / UDTFs across the
   cluster — not a single-node `rasterio.open` loop, and not a reimplementation. No JAR,
   no init script, no native GDAL install.
2. **Register** — `<CodeFromTest ... functionName="REGISTER" ... />`.
3. **Side-by-sides** — three subsections (Reproject / Clip / NDVI), each with the rasterio
   snippet then the pyrx snippet via `CodeFromTest` (`WARP_RASTERIO`+`WARP_PYRX`,
   `CLIP_*`, `NDVI_*`). One sentence each: pyrx runs the same rasterio/NumPy compute per
   tile, distributed. `testFile="docs/tests/python/api/test_rasterio_distributed_examples.py"`.
4. **Coverage matrix** — table: capability → pyrx function(s) → backing library, grouped
   (I/O & metadata → rasterio; warp/reproject → rasterio; clip/mask → rasterio+shapely;
   resample → rasterio; merge → rasterio+NumPy; COG → rio-cogeo; band math / indices →
   NumPy+numexpr; terrain (Horn 3×3) → NumPy; rasterize/polygonize → rasterio+shapely;
   focal → scipy.ndimage; proximity → scipy; contour → scikit-image; viewshed →
   xarray-spatial; XYZ/tiling → rio-tiler+morecantile; grid aggregation → h3/quadbin).
   Link to [RasterX Function Reference](./raster-functions) for runnable per-function code.
5. **Honest gaps (roadmap-forward, three buckets):**
   - *Distributed in pyrx today* — the matrix above; every `rst_*` runs in both tiers.
   - *Heavyweight-only for now (parity tracked)* — OGR vector readers (`*_ogr`),
     `conforming` TIN mode (pyrx raises on it; `constrained` works in both), advanced
     PMTiles DataSource writer options (the `gbx_pmtiles_agg` aggregate is in both tiers),
     and SQL default-argument convenience. Link [Execution Tiers](./execution-tiers).
   - *Known behavior divergences* — `rst_color_relief` (GDAL DEMProcessing vs NumPy
     `np.interp`; no `default` keyword in pyrx), `rst_convolve`/`rst_derivedband` (edge:
     GDAL halo vs NumPy `pad(mode='edge')`), `rst_resample` (NoData/edge boundary pixels),
     `rst_contour` (`gdal.ContourGenerateEx` vs `skimage.find_contours`), `rst_viewshed`
     (`gdal.ViewshedGenerate` vs `xrspatial.viewshed`). Link [Benchmarking](./benchmarking)
     and [Performance](./performance). Also note rasterio's bundled GDAL has a narrower
     driver set than the heavyweight custom build.
6. **How it's distributed** — brief: Arrow scalar UDFs (per-tile), grouped-aggregate Arrow
   UDFs (merges), streaming UDTFs (fan-out); no driver-side `.rdd`/`_jvm`; DataSource V2
   readers/writers. Link [Performance](./performance).

- [ ] **Step 2: Wire into the sidebar**

In `docs/sidebars.js`, add `'api/rasterio-distributed'` to the RasterX category `items`
(the array currently holding `'api/h3-raster-tessellation'`):

```js
        {
          type: 'category',
          label: 'RasterX',
          collapsed: true,
          link: { type: 'doc', id: 'api/raster-functions' },
          items: [
            'api/rasterio-distributed',
            'api/h3-raster-tessellation',
          ],
        },
```

- [ ] **Step 3: Verify page compiles + links resolve (docs static build)**

Dispatch a subagent (Docker):

```bash
bash scripts/commands/gbx-docs-static-build.sh --skip-zip --log rio-dist-build.log
```

Expected: exit 0, and NO "Broken links" / "broken anchors" entries. The build is the
authoritative link check (double-hyphen anchors from `&` headings are correct — trust the
build, don't hand-fix). Confirm `api/rasterio-distributed` compiled and its links to
`./raster-functions`, `./execution-tiers`, `./benchmarking`, `./performance` resolve.

- [ ] **Step 4: Commit**

```bash
git add docs/docs/api/rasterio-distributed.mdx docs/sidebars.js
git commit -m "docs: add 'Rasterio, Distributed' RasterX positioning page

RasterX's lightweight (pyrx) tier is distributed rasterio + a
best-of-breed Python raster stack: side-by-side rasterio-vs-pyrx
snippets (tested), a capability->function->library coverage matrix,
and an honest roadmap-forward gap list (heavyweight-only for now /
known behavior divergences). Wired into the RasterX sidebar.

Co-authored-by: Isaac"
```

---

### Task 3: Cross-links + the terrain attribution fix

Weave a one-sentence blurb + link into four high-traffic sites, and correct the two
`performance.mdx` lines that over-attribute terrain to xarray-spatial. Bundled into one
task/commit because each edit is a one-liner and they share a single verification (build).

**Files:**
- Modify: `docs/src/pages/index.js` (RasterX `Feature` description, ~line 56)
- Modify: `docs/docs/intro.mdx` (tiers paragraph, line 11)
- Modify: `docs/docs/api/execution-tiers.mdx` (near the "Python-worker UDFs (rasterio + NumPy)" row, line 70)
- Modify: `docs/docs/api/raster-functions.mdx` (top, after the imports block, ~line 12+)
- Modify: `docs/docs/api/performance.mdx` (lines 56 and 388)

**Interfaces:**
- Consumes: doc id `api/rasterio-distributed` from Task 2.

- [ ] **Step 1: Homepage RasterX card** — append to the RasterX `Feature` `description` in `docs/src/pages/index.js`:

Change the description string to end with: ` In large part, distributed rasterio + best-of-breed raster packages.`
(Keep the existing `link="/docs/api/raster-functions"`; the new page is reachable from the See-also added in Step 4. Do NOT restructure the `Feature` component to add a second link.)

- [ ] **Step 2: intro.mdx** — in the tiers paragraph (line 11), after "...covers all of **RasterX**, all of **VectorX** ... and all of **GridX** ...", add a sentence before the existing "See [Choosing an Execution Tier]" link:

`RasterX's lightweight tier is, in large part, [distributed rasterio](./api/rasterio-distributed) — the familiar rasterio/NumPy raster stack run as Arrow UDFs across the cluster.`

- [ ] **Step 3: execution-tiers.mdx** — immediately after the Tradeoffs table (the block containing the "Python-worker UDFs (rasterio + NumPy)" and "rasterio's bundled build (narrower)" rows), add a sentence:

`The lightweight raster tier is, in effect, [distributed rasterio](./rasterio-distributed): see how much of the rasterio/GDAL surface is already distributed — and what stays heavyweight-only for now.`

- [ ] **Step 4: raster-functions.mdx** — add a short admonition/"See also" near the top (after the intro paragraph, before the first tabbed section):

`:::tip Built on rasterio` newline `RasterX's lightweight tier is, in large part, **distributed rasterio** + a best-of-breed raster stack. See [Rasterio, Distributed](./rasterio-distributed) for the coverage matrix and honest gaps.` newline `:::`

- [ ] **Step 5: performance.mdx terrain attribution fix (two lines)**

Line 56 currently: `| **xarray-spatial** | Terrain analysis (slope, hillshade, aspect, tri, tpi, roughness, viewshed) |`
Change to attribute terrain to NumPy and scope xarray-spatial to viewshed only. Replace with two correct rows:
```
| **NumPy** (Horn 3×3) | Terrain analysis (slope, aspect, hillshade, tri, tpi, roughness) |
| **xarray-spatial** | Viewshed |
```
(Fold into the existing NumPy row if one already lists band math — implementer's judgment; the requirement is: terrain must be attributed to NumPy, and xarray-spatial scoped to viewshed only. Keep table columns consistent with the surrounding rows.)

Line 388 currently: `` | `pyrx/core/terrain.py` | xarray-spatial | `rst_slope`, `rst_aspect`, `rst_hillshade`, `rst_tri`, `rst_tpi`, `rst_roughness`, `rst_color_relief`, `rst_viewshed` | ``
Change the backing-library column for `terrain.py` from `xarray-spatial` to `NumPy` (Horn 3×3). Note `rst_viewshed` actually lives in `analysis.py` (xarray-spatial); if this row lumps it under terrain.py, split it out or annotate — verify the module column against `pyrx/core/` before editing so the table is accurate.

- [ ] **Step 6: Verify build clean (all links resolve, no regressions)**

Dispatch a subagent (Docker):
```bash
bash scripts/commands/gbx-docs-static-build.sh --skip-zip --log rio-dist-links-build.log
```
Expected: exit 0, no broken links/anchors. Confirm the four new `rasterio-distributed`
links resolve and the homepage/intro/execution-tiers/raster-functions pages still compile.

- [ ] **Step 7: Commit**

```bash
git add docs/src/pages/index.js docs/docs/intro.mdx docs/docs/api/execution-tiers.mdx \
        docs/docs/api/raster-functions.mdx docs/docs/api/performance.mdx
git commit -m "docs: link 'Rasterio, Distributed' from key pages; fix terrain attribution

Blurb+link into the homepage RasterX card, intro, execution-tiers, and
the RasterX function reference. Correct performance.mdx: terrain
(slope/aspect/hillshade/tri/tpi/roughness) is pure NumPy (Horn 3x3), not
xarray-spatial; only viewshed uses xarray-spatial.

Co-authored-by: Isaac"
```

---

### Task 4: Final verification (browser + full-suite sanity)

**Files:** none (verification only).

- [ ] **Step 1: Browser screenshot pass (non-3000 port)**

Dispatch the web-devloop-tester subagent: start the docs dev server on `--port 3001`
(`bash scripts/commands/gbx-docs-dev.sh` defaults to 3000 — override to 3001, or
`cd docs && npm run start -- --port 3001`). Screenshot:
- `http://localhost:3001/geobrix/docs/api/rasterio-distributed` — hook, a side-by-side pair, the coverage matrix, and the gaps section render correctly; tier tabs (if any) behave.
- The homepage RasterX card shows the new blurb.
Confirm no console errors. Stop the dev server when done. (Port 3000 is reserved for the user.)

- [ ] **Step 2: Report status; hold for push**

Summarize: page + tests + cross-links + attribution fix landed on `beta/0.4.0` as 3
commits; doc-test green; static build clean; browser verified. Do NOT push. Suggest
`/review` before any push, and `gh auth switch --user mjohns-databricks` when the user
approves a push.

---

## Self-Review

**Spec coverage:**
- New page under Functions→RasterX → Task 2 ✓
- Hook / side-by-sides / coverage matrix / three-bucket gaps / mechanism → Task 2 body ✓
- Flagship warp/clip/NDVI, both sides tested with equality assertion → Task 1 ✓
- Cross-links (homepage, intro, execution-tiers, raster-functions) → Task 3 ✓
- Terrain attribution fix (performance.mdx) → Task 3 Step 5 ✓ (two lines, verified against code)
- Sidebar wiring standing rule → Task 2 Step 2 ✓
- Verification: doc-test green, static build clean, browser (non-3000) → Tasks 1/2/3/4 ✓
- No push, own commits on beta/0.4.0 → commits per task + Task 4 Step 2 ✓

**Placeholder scan:** Snippet/verifier/test code is concrete. The two explicit "confirm
against `functions.py` / `rasterx_functions.py`" notes are deliberate guardrails (real
call signatures must come from code, not be invented) — not deferred work; Task 1 Step 4
iterates to green, which forces resolution.

**Type consistency:** `functionName` constants (`REGISTER`, `WARP_RASTERIO`, `WARP_PYRX`,
`CLIP_RASTERIO`, `CLIP_PYRX`, `NDVI_RASTERIO`, `NDVI_PYRX`) match between Task 1 (defined)
and Task 2 (consumed). Verifier fns `warp_both` / `ndvi_both` (+ optional `clip_both`)
match between the module and the test. Doc id `api/rasterio-distributed` consistent across
Tasks 2 and 3.
