# VizX static-map helper (`plot_static`) — Design

**Date:** 2026-06-24
**Branch:** off `beta/0.4.0` (single PR)
**Module:** `databricks.labs.gbx.vizx` (Python-only, tier-agnostic, `[vizx]` extra)

## Goal

Add one public helper, `plot_static`, that renders Spark- or GeoPandas-derived
vector geometries (or DGGS cells) over a tiled basemap as a **static**
matplotlib figure — a prettier alternative to plain `GeoDataFrame.plot()` (no
map context) and to folium `.explore()` (interactive, but renders a blank
*"Make this Notebook Trusted"* placeholder on GitHub and the docs site).

The name `plot_static` is deliberate: it signals the non-interactive
counterpart to `.explore()`, and it accepts **both** a GeoDataFrame and a Spark
DataFrame.

## Why this works on GitHub (the caching question)

GitHub never executes a notebook — it renders the **saved output cells**
committed in the `.ipynb`. When an executed notebook calls `plot_static`,
contextily fetches basemap tiles *at execution time* (on a cluster/laptop with
egress) and matplotlib rasterizes the basemap **into the output PNG**, which is
embedded in the notebook and committed. GitHub displays the baked pixels with
**zero network at render time**. This is the same model the existing static
`.plot()` cells use; we are only adding a basemap layer to the baked image.

Consequence: the basemap "just works" on GitHub as long as whoever *executes*
the notebook has egress. No basemap tiles are committed to the repo.

## Non-goals (v1)

- No interactive output (that remains `.explore()`).
- No committed/offline tile cache for no-egress executors (e.g. Docker
  doc-tests). The fallback below covers no-egress by rendering without a
  basemap.
- `grid_system` values `'quadbin'`, `'bng'`, `'custom'` are **forward-declared
  but not implemented** in v1 (fast-follow). `'custom'` is the trickiest:
  custom grids need their own cell→boundary resolver, currently heavy-only.

  **Fast-follow note (quadbin / bng):** no pure-Python cell→boundary port is
  needed — the light tier already ships driver-side scalar `_aswkb` impls that
  mirror what `_h3_boundary` does for h3:
  - quadbin: `databricks.labs.gbx.pygx._quadbin.as_wkb(cell: int) -> bytes`
  - bng: `databricks.labs.gbx.pygx._bng.cell_aswkb(cell_id: int) -> bytes`

  So each resolver is one `_GRID_DISPATCH` entry that, per collected cell id,
  calls the scalar `_aswkb` → WKB `bytes` → `parse_geom` (shapely). This runs
  **driver-side after the collect** (exactly like the h3 path), so it is
  unit-testable in the dev container with **no Spark-runtime / SQL-registration
  dependency**. (The columnar `quadbin_aswkb` / `bng_aswkb` SQL functions remain
  available as an in-Spark coercion alternative, but the scalar impls are
  preferred here for the same reason h3 uses the `h3` lib directly.)

  `'custom'` stays the trickiest: its scalar impl is
  `pygx._custom.cell_aswkb(conf: CustomGridConf, cell_id: int)` — it needs the
  grid configuration threaded through, so it requires either an extra param or
  resolving the conf from the frame, not just a cell-id column.

## Public API

```python
plot_static(
    data,                    # Spark DataFrame OR geopandas.GeoDataFrame
    *,
    geom_col=None,           # geometry/cell column; auto-detected if None
    grid_system=None,        # None | 'h3' | 'quadbin' | 'bng' | 'custom'  (v1: None, 'h3')
    column=None,             # attribute column → choropleth; None → single style
    cmap="viridis",
    legend=True,
    basemap=True,            # contextily tiles; graceful fallback if unreachable
    basemap_source=None,     # contextily provider; default CartoDB.Positron
    alpha=0.8,
    edgecolor="face",
    markersize=None,         # point layers
    title=None,
    fig_w=10, fig_h=10,
    max_rows=10_000,         # driver-collect guard (same convention as adapters)
    srid=None,               # CRS override for bare WKT/WKB (default: assume 4326)
    ax=None,                 # overlay onto an existing Axes
) -> "matplotlib.axes.Axes"
```

- **Returns** the `Axes` and renders inline. `pyplot.show()` is called **only
  when `plot_static` created the figure** (i.e. `ax is None`). Passing the
  returned `ax` back in composes overlays (cells choropleth → grid boundary →
  points) on one basemap. This replaces the notebooks'
  `gdf.plot(...)` + `grid_gdf.boundary.plot(ax=ax)` pair.
- Requires the `[vizx]` extra; guarded by `assert_viz_available()` (matplotlib +
  geopandas) as the other plotters are.

## Architecture / components

New file `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py` holding
`plot_static` plus private helpers. Exported from `vizx/__init__.py`.

### 1. Input resolution → GeoDataFrame

Private `_resolve_gdf(data, geom_col, grid_system, max_rows, srid)` returns an
EPSG:4326 GeoDataFrame:

- **`data` is a GeoDataFrame** → use as-is (its own CRS; reprojected later).
- **`data` is a Spark DataFrame:**
  - Resolve `geom_col`: explicit arg, else auto-detect — a native
    `GEOMETRY`/`GEOGRAPHY`-typed column first, else a column named
    `wkt`/`geometry`/`geom`, else (when `grid_system` is set) the lone
    remaining candidate. Ambiguous/none → `ValueError`.
  - **`grid_system is None`** (geometry) — branch on
    `df.schema[geom_col].dataType`:
    - native `GEOMETRY`/`GEOGRAPHY` → coerce **in Spark** with
      `expr("st_asbinary(<col>)")` to WKB bytes; resolve SRID via
      `expr("st_srid(<col>)")` (`GEOGRAPHY` ⇒ 4326); collect → `parse_geom`.
    - `BinaryType` → collect bytes → `parse_geom` (WKB/EWKB).
    - `StringType` → collect strings → `parse_geom` (WKT/EWKT, EWKT
      `SRID=...;` prefix honored).
    - any other type → `ValueError` naming the dtype and suggesting
      `st_asbinary` / `st_astext`.
  - **`grid_system` set** (cells) — dispatch table
    `{'h3': _h3_boundaries, 'quadbin': _nyi, 'bng': _nyi, 'custom': _nyi}`:
    - `'h3'` (v1): each cell id may be a **string** h3 index or a **long**
      bigint; longs are converted via `h3.int_to_str`, then
      `h3.cell_to_boundary` → shapely `Polygon` (lng, lat order). CRS 4326.
    - `'quadbin'`/`'bng'`/`'custom'`: `NotImplementedError(
      "grid_system='<x>' is a planned fast-follow; not supported yet")`.
  - **Collect guard**: same truncate-and-warn at `max_rows` as
    `as_gdf`/`cells_as_gdf` (`max_rows=None` opts out).

Reuses the shared decoder `databricks.labs.gbx._geom.parse_geom`, so
`plot_static` accepts exactly the same geometry encodings as every other
`gbx_st_*` function (geometry-input-consistency rule). The `grid_system`
dispatch table is the single seam where quadbin/bng/custom slot in later
without reshaping the API.

### 2. CRS + render

- When `basemap=True`, reproject the resolved gdf to **EPSG:3857** (contextily
  requires Web Mercator).
- Draw:
  `gdf.plot(column=column, cmap=cmap, legend=legend, alpha=alpha,
  edgecolor=edgecolor, markersize=markersize, ax=ax)`.
- Axis ticks off; set `title` if given.

### 3. Basemap (with graceful fallback)

- Lazy `import contextily as cx` **inside** the `basemap` branch.
- `cx.add_basemap(ax, source=basemap_source or cx.providers.CartoDB.Positron,
  crs=gdf.crs)`.
- Wrap in `try/except Exception`: on **any** failure (no egress, HTTP error,
  contextily not installed) → `warnings.warn(...)` and continue **without** the
  basemap. The figure still renders and the PNG still bakes — so a locked-down
  cluster degrades to a basemap-less map rather than erroring.
- `basemap=False` skips tiles entirely (deterministic, no network).
- Default provider: **CartoDB.Positron** (light, clean under viridis).

## Dependency

`contextily` is the new package. Two layers, per the project's supply-chain
pinning rule (execution environments are exact-version **and** hash-pinned;
range pins are only for the published library extra):

- **Published `[vizx]` extra** (`python/geobrix/pyproject.toml`): add a range
  pin `contextily>=1.5,<2` (library convention, alongside
  matplotlib/geopandas/folium/mapclassify).
- **Execution-env lock files** (local **and** GitHub CI): regenerate with
  `contextily` and **all its transitive deps** exact-version + `--hash=sha256`
  pinned (`--require-hashes` style), the same way every other package in these
  files is pinned:
  - `python/geobrix/requirements-pyrx-ci.txt` — the light-tier GitHub CI lane
    that runs the vizx tests (already carries geopandas/matplotlib/folium/
    mapclassify; `contextily` must be added so the basemap-fallback test can
    import and monkeypatch it).
  - `python/geobrix/requirements-dev-container.txt` — the local dev container.
  - Regenerate via the repo's pip-compile path (do not hand-edit hashes), then
    verify the lock installs cleanly in a fresh venv (a dir added to CI needs
    *all* its non-stubbed third-party imports present in the lock, not just
    what the full dev extras happen to provide).

`contextily` is lazy-imported only in the basemap branch; its absence at
runtime is still handled by the warn-and-fallback path (not a hard error).
`assert_viz_available` continues to guard matplotlib + geopandas only.

## Testing (TDD)

`python/geobrix/test/vizx/test_static_map.py` — runs in the dev container with
no Databricks runtime and no network:

1. WKT-string Spark DF → expected polygon geometry; returns an `Axes`; one
   figure produced.
2. WKB-binary Spark DF → geometry identical to the WKT case (round-trip via
   `parse_geom`).
3. `grid_system='h3'` with **string** ids and with **long** ids → both produce
   identical cell-boundary polygons.
4. `column=` → choropleth path renders with a legend.
5. `ax=` overlay → a second `plot_static` call draws onto the same `Axes` and
   returns that same object.
6. `max_rows` smaller than input → truncation `UserWarning`.
7. Unknown column dtype → `ValueError`; `grid_system` in
   `{'quadbin','bng','custom'}` → `NotImplementedError`.
8. **Basemap fallback**: monkeypatch `contextily.add_basemap` to raise →
   assert a warning is emitted and a figure is still produced.

Real tile fetching is **not** exercised in CI (no egress); it is validated
manually by running an example notebook. The native `GEOMETRY`/`GEOGRAPHY`
coercion path (`st_asbinary`/`st_srid`) only runs where those built-ins exist
(Databricks/Serverless); unit tests cover the string/binary/h3 routing
directly. Headless rendering uses the `Agg` backend already established in the
vizx test suite.

## Docs + notebook adoption

- New `plot_static` section in `docs/docs/api/vizx.mdx`: signature, the
  supported-encodings table (WKT, EWKT, WKB, EWKB, native GEOMETRY, native
  GEOGRAPHY, H3 cell ids), `grid_system` (with the fast-follow note), the
  basemap graceful-fallback note, and an overlay example.
- Adopt in the **h3-rasterize** notebook (replace the `.plot()` + boundary pair
  with `plot_static(..., grid_system='h3', column='band_level')` over a
  basemap) and **eo-series 01 / 03** cell maps, so the committed output PNGs
  gain a basemap. xView object footprints optional.

## Delivery

- Lands on **PR #45** (`refactor/vizx-rebrand`), per standing guidance to route
  this batch of vizx work through that PR. Commit locally as work progresses;
  **push only on the user's go** (each push triggers a CI build).
- After merge, fast-follow tasks: implement `grid_system` `'quadbin'`, `'bng'`,
  and `'custom'` cell→boundary resolvers in the dispatch table.
