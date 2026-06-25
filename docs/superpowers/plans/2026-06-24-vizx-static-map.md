# VizX static-map helper (`plot_static`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `databricks.labs.gbx.vizx.plot_static`, a static (non-interactive) map renderer that draws Spark- or GeoPandas-derived geometries / H3 cells over a contextily basemap, baked into a GitHub-renderable PNG.

**Architecture:** One new file `vizx/_static_map.py` exposing `plot_static`, built on three private helpers — a pure `_geom_strategy(dtype)` decode-strategy chooser, `_resolve_gdf(...)` (Spark/GeoPandas → EPSG:4326 GeoDataFrame, reusing the shared `parse_geom`), and a `grid_system` dispatch table (h3 implemented; quadbin/bng/custom forward-declared). The renderer reprojects to Web Mercator, plots with GeoPandas, and overlays a contextily basemap inside a try/except that degrades to no-basemap on any failure.

**Tech Stack:** Python 3.12, geopandas, shapely, matplotlib, contextily, h3, pyspark (local mode in tests). Lock files are uv-compiled with `--generate-hashes`.

## Global Constraints

- Module is `databricks.labs.gbx.vizx`; public function name is exactly `plot_static`. No aliases (beta no-aliases policy).
- Requires the `[vizx]` extra; package code calls `assert_viz_available()` before importing matplotlib/geopandas (lazy imports inside functions only).
- Geometry inputs must accept the same encodings as every other `gbx_st_*` function — reuse `databricks.labs.gbx._geom.parse_geom` for the decode (WKB/EWKB/WKT/EWKT). Do not write a second decoder.
- `grid_system` vocabulary is exactly `None | 'h3' | 'quadbin' | 'bng' | 'custom'`. v1 implements `'h3'`; `'quadbin'`, `'bng'`, `'custom'` raise `NotImplementedError` ("planned fast-follow").
- Basemap uses contextily with default provider `CartoDB.Positron`; any basemap failure (no egress, HTTP error, missing dep) degrades to a `warnings.warn` + render-without-basemap. Never a hard error.
- Supply-chain pinning: the published `[vizx]` extra uses a range pin (`contextily>=1.5,<2`); the execution-env lock files (`requirements-pyrx-ci.txt`, `requirements-dev-container.txt`) pin `contextily` and all transitive deps exact-version + `--hash=sha256` (regenerated via `uv pip compile --generate-hashes`, never hand-edited).
- All Maven/test/lint work runs inside the `geobrix-dev` Docker container. Tests run via `bash scripts/commands/gbx-test-python.sh --path <path>`; lint via `bash scripts/commands/gbx-lint-python.sh --check`.
- Lands on PR #45 (`refactor/vizx-rebrand`). Commit locally; **do not push** (each push triggers CI) — the controller pushes only on the user's explicit go.
- Black/isort/flake8 must pass (CI gate). Use in-container `black` (host black may differ).

---

### Task 1: Add `contextily` dependency + regenerate hash-pinned locks

**Files:**
- Modify: `python/geobrix/pyproject.toml` (the `vizx = [...]` extra, ~line 133)
- Modify: `python/geobrix/requirements-pyrx-ci.in` (the `[vizx]` visualization block, ~line with `mapclassify==2.10.0`)
- Modify: `python/geobrix/requirements-dev-container.in` (the geospatial dev stack block)
- Regenerate: `python/geobrix/requirements-pyrx-ci.txt`, `python/geobrix/requirements-dev-container.txt`

**Interfaces:**
- Consumes: nothing.
- Produces: `contextily` importable in the dev container and pinned in both locks.

- [ ] **Step 1: Add the range pin to the published extra**

In `python/geobrix/pyproject.toml`, the `vizx` extra becomes:

```toml
vizx = [
    "matplotlib>=3.7,<4",
    "geopandas>=1.0,<2",
    "folium>=0.16,<1",
    "mapclassify>=2.6,<3",
    "contextily>=1.5,<2",
]
```

- [ ] **Step 2: Add the exact pin to both `.in` source files**

In `python/geobrix/requirements-pyrx-ci.in`, inside the `# --- visualization ([vizx] extra) ...` block, append after `mapclassify==2.10.0`:

```
contextily==1.6.2
```

In `python/geobrix/requirements-dev-container.in`, inside the `# --- geospatial dev stack (not in DBR) ---` block, append after `pyproj==3.7.2`:

```
contextily==1.6.2
```

(If the corp PyPI proxy does not have `1.6.2`, use the latest available `1.x` and keep the `>=1.5,<2` range in the extra consistent.)

- [ ] **Step 3: Regenerate both hash-pinned locks (in the container, proxy available)**

Run:

```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix/python/geobrix && uv pip compile --generate-hashes --python-version 3.12 --output-file requirements-pyrx-ci.txt requirements-pyrx-ci.in && uv pip compile --generate-hashes --python-version 3.12 --output-file requirements-dev-container.txt requirements-dev-container.in"
```

Expected: both `.txt` files now contain a hash-pinned `contextily==1.6.2 \` block plus any new transitive deps (e.g. `xyzservices`, `mercantile`, `geographiclib`/`geopy`, `joblib`), each with `--hash=sha256:` lines.

- [ ] **Step 4: Verify `contextily` is pinned + transitive deps captured**

Run:

```bash
grep -n "^contextily==" python/geobrix/requirements-pyrx-ci.txt python/geobrix/requirements-dev-container.txt
```

Expected: one match in each file, immediately followed by `--hash=sha256:` lines.

- [ ] **Step 5: Install into the running dev container so test tasks can import it**

Run:

```bash
bash scripts/commands/gbx-docker-exec.sh "pip install --require-hashes -r /root/geobrix/python/geobrix/requirements-dev-container.txt && python -c 'import contextily; print(contextily.__version__)'"
```

Expected: the install succeeds (clean-resolve from the hashed lock) and prints `1.6.2`.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/pyproject.toml python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-dev-container.in python/geobrix/requirements-pyrx-ci.txt python/geobrix/requirements-dev-container.txt
git commit -m "build(vizx): add contextily dep, hash-pin in CI + dev-container locks"
```

---

### Task 2: `_static_map.py` — geometry-path resolution (`grid_system=None`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py`
- Test: `python/geobrix/test/vizx/test_static_map.py`

**Interfaces:**
- Consumes: `databricks.labs.gbx._geom.parse_geom(x) -> shapely geometry | None`.
- Produces:
  - `_geom_strategy(dtype) -> 'native' | 'binary' | 'string'` (raises `ValueError` otherwise); `dtype` is a `pyspark.sql.types.DataType`.
  - `_detect_geom_col(df, grid_system) -> str`.
  - `_collect_limited(df, max_rows) -> pandas.DataFrame` (truncate-and-warn).
  - `_resolve_gdf(data, geom_col, grid_system, max_rows, srid) -> geopandas.GeoDataFrame` (EPSG:4326-or-`srid`). For this task only the `grid_system is None` and GeoDataFrame-passthrough paths are live; the cell path is added in Task 3.

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/vizx/test_static_map.py`:

```python
import logging
import warnings

import pytest
from pyspark.sql.types import BinaryType, LongType, StringType


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-static-map-tests")
        .getOrCreate()
    )
    yield s


# --- _geom_strategy (pure, no Spark) ---


def test_geom_strategy_string_binary_native_and_error():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(StringType()) == "string"
    assert sm._geom_strategy(BinaryType()) == "binary"
    assert sm._geom_strategy(LongType()) is None or True  # placeholder; replaced below


def test_geom_strategy_rejects_unsupported():
    from databricks.labs.gbx.vizx import _static_map as sm

    with pytest.raises(ValueError):
        sm._geom_strategy(LongType())


class _FakeGeoType:
    # mimics a Databricks GEOMETRY/GEOGRAPHY dataType for routing tests
    def __init__(self, name):
        self._name = name

    def typeName(self):
        return self._name

    def simpleString(self):
        return self._name


def test_geom_strategy_native_for_geometry_and_geography():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(_FakeGeoType("geometry")) == "native"
    assert sm._geom_strategy(_FakeGeoType("geography")) == "native"


# --- _resolve_gdf geometry path ---


def test_resolve_gdf_wkt_string(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = sm._resolve_gdf(df, None, None, 10_000, None)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"]
    assert "wkt" not in gdf.columns
    assert [g.x for g in gdf.geometry] == [1.0, 3.0]


def test_resolve_gdf_wkb_matches_wkt(spark):
    import shapely

    from databricks.labs.gbx.vizx import _static_map as sm

    wkb = bytearray(shapely.to_wkb(shapely.from_wkt("POINT (5 6)")))
    df = spark.createDataFrame([(wkb,)], ["geometry"])
    gdf = sm._resolve_gdf(df, None, None, 10_000, None)
    assert (gdf.geometry.iloc[0].x, gdf.geometry.iloc[0].y) == (5.0, 6.0)


def test_resolve_gdf_passes_through_geodataframe():
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx import _static_map as sm

    g = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0, 0)], crs=4326)
    assert sm._resolve_gdf(g, None, None, 10_000, None) is g


def test_resolve_gdf_unknown_column_type_raises(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["geometry"])  # LongType, no grid_system
    with pytest.raises(ValueError):
        sm._resolve_gdf(df, None, None, 10_000, None)


def test_resolve_gdf_truncates_and_warns(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.range(5).selectExpr("concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gdf = sm._resolve_gdf(df, None, None, 2, None)
    assert len(gdf) == 2
    assert any("max_rows" in str(w.message) for w in caught)
```

Delete the placeholder line in `test_geom_strategy_string_binary_native_and_error` (the `LongType() is None or True`); keep only the `StringType`/`BinaryType` asserts there:

```python
def test_geom_strategy_string_binary_native_and_error():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(StringType()) == "string"
    assert sm._geom_strategy(BinaryType()) == "binary"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: FAIL — `ModuleNotFoundError: ... _static_map` / `AttributeError: _geom_strategy`.

- [ ] **Step 3: Implement the geometry path**

Create `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py`:

```python
"""Static (non-interactive) map rendering for gbx.vizx.

plot_static renders Spark- or GeoPandas-derived geometries / DGGS cells over a
contextily basemap as a static matplotlib figure -- the GitHub-renderable
counterpart to GeoDataFrame.explore(). Requires the [vizx] extra.
"""

import warnings

_GEOM_COL_CANDIDATES = ("wkt", "geometry", "geom", "ewkt", "wkb", "ewkb")
_CELL_COL_CANDIDATES = ("cellid", "cell", "cell_id", "h3", "quadbin", "bng", "index")


def _geom_strategy(dtype):
    """Decode strategy for a Spark geometry column's dataType.

    Returns 'native' (Databricks GEOMETRY/GEOGRAPHY -> st_asbinary in Spark),
    'binary' (WKB/EWKB), or 'string' (WKT/EWKT). Raises ValueError otherwise.
    """
    name = dtype.typeName().lower()
    simple = dtype.simpleString().lower()
    if "geometry" in name or "geography" in name or "geometry" in simple or "geography" in simple:
        return "native"
    if name == "binary":
        return "binary"
    if name == "string":
        return "string"
    raise ValueError(
        f"plot_static: geometry column has unsupported type {dtype.simpleString()!r}; "
        "coerce it to WKB/WKT first (e.g. st_asbinary(col) / st_astext(col)), or "
        "pass grid_system= for DGGS cell ids."
    )


def _detect_geom_col(df, grid_system):
    """Auto-detect the geometry/cell column name. Raise ValueError if ambiguous."""
    cols = df.columns
    lower = {c.lower(): c for c in cols}
    if grid_system is not None:
        for cand in _CELL_COL_CANDIDATES:
            if cand in lower:
                return lower[cand]
        if len(cols) == 1:
            return cols[0]
        raise ValueError(
            "plot_static: could not auto-detect the cell-id column; pass "
            f"geom_col= explicitly (columns: {cols})."
        )
    for f in df.schema.fields:
        s = f.dataType.simpleString().lower()
        if "geometry" in s or "geography" in s:
            return f.name
    for cand in _GEOM_COL_CANDIDATES:
        if cand in lower:
            return lower[cand]
    raise ValueError(
        "plot_static: could not auto-detect the geometry column; pass geom_col= "
        f"explicitly (columns: {cols})."
    )


def _collect_limited(df, max_rows):
    """Collect a Spark DataFrame to pandas with a truncate-and-warn row guard."""
    if max_rows is None:
        return df.toPandas()
    pdf = df.limit(max_rows + 1).toPandas()
    if len(pdf) > max_rows:
        pdf = pdf.iloc[:max_rows]
        warnings.warn(
            f"plot_static: output truncated to max_rows={max_rows} for driver-side "
            "viz; pass max_rows=None to collect all rows.",
            stacklevel=2,
        )
    return pdf


def _resolve_gdf(data, geom_col, grid_system, max_rows, srid):
    """Spark DataFrame or GeoDataFrame -> geopandas.GeoDataFrame (EPSG:4326 or srid)."""
    import geopandas as gpd

    if isinstance(data, gpd.GeoDataFrame):
        return data

    col = geom_col or _detect_geom_col(data, grid_system)

    if grid_system is not None:
        return _resolve_cells(data, col, grid_system, max_rows)  # added in Task 3

    from databricks.labs.gbx._geom import parse_geom

    field = data.schema[col]
    strategy = _geom_strategy(field.dataType)
    work = data
    if strategy == "native":
        from pyspark.sql.functions import expr

        work = data.withColumn(col, expr(f"st_asbinary(`{col}`)"))
        if srid is None and "geography" in field.dataType.simpleString().lower():
            srid = 4326

    pdf = _collect_limited(work, max_rows)
    geoms = [parse_geom(v) for v in pdf[col]]
    pdf = pdf.drop(columns=[col])
    return gpd.GeoDataFrame(pdf, geometry=geoms, crs=(srid or 4326))
```

Note: `_resolve_cells` is referenced but not yet defined — Task 2's tests never set `grid_system`, so that branch is not exercised here. Task 3 adds `_resolve_cells`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: PASS (all Task-2 tests green).

- [ ] **Step 5: Lint**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py && isort python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py"`
Then: `bash scripts/commands/gbx-lint-python.sh --check`
Expected: lint passes.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py
git commit -m "feat(vizx): _static_map geometry-path resolution (parse_geom reuse)"
```

---

### Task 3: `grid_system` cell dispatch (h3 implemented; quadbin/bng/custom NYI)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py`
- Test: `python/geobrix/test/vizx/test_static_map.py`

**Interfaces:**
- Consumes: `_collect_limited`, `_detect_geom_col` (Task 2).
- Produces: `_resolve_cells(data, col, grid_system, max_rows) -> geopandas.GeoDataFrame` and module-level `_GRID_DISPATCH` dict; the `'h3'` resolver accepts string h3 indices and long bigints.

- [ ] **Step 1: Write the failing tests**

Append to `python/geobrix/test/vizx/test_static_map.py`:

```python
def _ny_hex_string():
    import h3

    return h3.latlng_to_cell(40.7, -74.0, 9)  # string h3 index


def test_resolve_cells_h3_string_and_long_match(spark):
    import h3

    from databricks.labs.gbx.vizx import _static_map as sm

    s = _ny_hex_string()
    as_long = h3.str_to_int(s)

    df_str = spark.createDataFrame([(s,)], ["cellid"])
    df_long = spark.createDataFrame([(as_long,)], ["cellid"])

    g_str = sm._resolve_gdf(df_str, None, "h3", 10_000, None)
    g_long = sm._resolve_gdf(df_long, None, "h3", 10_000, None)

    assert g_str.crs.to_epsg() == 4326
    # identical boundary polygon from either id form
    assert g_str.geometry.iloc[0].equals(g_long.geometry.iloc[0])


def test_resolve_cells_carries_attribute_columns(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    s = _ny_hex_string()
    df = spark.createDataFrame([(s, 7)], ["cellid", "count"])
    gdf = sm._resolve_gdf(df, "cellid", "h3", 10_000, None)
    assert list(gdf["count"]) == [7]
    assert "cellid" not in gdf.columns


@pytest.mark.parametrize("gs", ["quadbin", "bng", "custom"])
def test_resolve_cells_fast_follow_not_implemented(spark, gs):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["cellid"])
    with pytest.raises(NotImplementedError):
        sm._resolve_gdf(df, "cellid", gs, 10_000, None)


def test_resolve_cells_unknown_grid_system_raises(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["cellid"])
    with pytest.raises(ValueError):
        sm._resolve_gdf(df, "cellid", "geohash", 10_000, None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: FAIL — `NameError: _resolve_cells` / `AttributeError`.

- [ ] **Step 3: Implement the cell dispatch**

Append to `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py` (after `_collect_limited`, before `_resolve_gdf`):

```python
def _h3_boundary(cell):
    import h3
    from shapely.geometry import Polygon

    idx = cell if isinstance(cell, str) else h3.int_to_str(int(cell))
    ring = h3.cell_to_boundary(idx)  # (lat, lng) pairs in h3 v4
    return Polygon([(lng, lat) for lat, lng in ring])


def _h3_boundaries(values):
    return [_h3_boundary(c) for c in values]


def _nyi(name):
    def _raise(_values):
        raise NotImplementedError(
            f"plot_static: grid_system={name!r} is a planned fast-follow; "
            "not supported yet."
        )

    return _raise


_GRID_DISPATCH = {
    "h3": _h3_boundaries,
    "quadbin": _nyi("quadbin"),
    "bng": _nyi("bng"),
    "custom": _nyi("custom"),
}


def _resolve_cells(data, col, grid_system, max_rows):
    """DGGS cell-id column -> boundary-polygon GeoDataFrame (EPSG:4326)."""
    import geopandas as gpd

    if grid_system not in _GRID_DISPATCH:
        raise ValueError(
            f"plot_static: grid_system={grid_system!r} is not one of "
            f"{sorted(_GRID_DISPATCH)} or None."
        )
    pdf = _collect_limited(data, max_rows)
    geometry = _GRID_DISPATCH[grid_system](pdf[col].tolist())
    return gpd.GeoDataFrame(pdf.drop(columns=[col]), geometry=geometry, crs=4326)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: PASS (all Task-2 and Task-3 tests green).

- [ ] **Step 5: Lint**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py && isort python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py"`
Then: `bash scripts/commands/gbx-lint-python.sh --check`
Expected: lint passes.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_map.py
git commit -m "feat(vizx): plot_static h3 cell dispatch; quadbin/bng/custom forward-declared"
```

---

### Task 4: `plot_static` public renderer (basemap + fallback + overlay) and export

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py`
- Test: `python/geobrix/test/vizx/test_static_map.py`

**Interfaces:**
- Consumes: `_resolve_gdf` (Tasks 2-3), `assert_viz_available` from `vizx/_env.py`.
- Produces: `plot_static(data, *, geom_col=None, grid_system=None, column=None, cmap="viridis", legend=True, basemap=True, basemap_source=None, alpha=0.8, edgecolor="face", markersize=None, title=None, fig_w=10, fig_h=10, max_rows=10_000, srid=None, ax=None) -> matplotlib.axes.Axes`. Exported from `databricks.labs.gbx.vizx`.

- [ ] **Step 1: Write the failing tests**

Append to `python/geobrix/test/vizx/test_static_map.py` (and add the headless backend lines at the very top of the file, mirroring `test_raster.py`):

At the **top** of the file, immediately after `import warnings`, add:

```python
import matplotlib

matplotlib.use("Agg")  # headless: no display needed
import matplotlib.pyplot as plt  # noqa: E402
```

New tests appended at the end:

```python
def test_plot_static_returns_axes_and_one_figure(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    ax = plot_static(df, basemap=False)
    assert ax is not None
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_static_choropleth_column_with_legend(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame(
        [("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 3)], ["wkt", "v"]
    )
    ax = plot_static(df, column="v", basemap=False)
    assert ax.get_figure() is not None
    plt.close("all")


def test_plot_static_overlay_reuses_axes(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df1 = spark.createDataFrame([("POINT (1 1)",)], ["wkt"])
    df2 = spark.createDataFrame([("POINT (2 2)",)], ["wkt"])
    ax = plot_static(df1, basemap=False)
    ax2 = plot_static(df2, basemap=False, ax=ax)
    assert ax2 is ax
    assert len(plt.get_fignums()) == 1  # no new figure created for the overlay
    plt.close("all")


def test_plot_static_basemap_fallback_warns(spark, monkeypatch):
    import contextily

    from databricks.labs.gbx.vizx import plot_static

    def _boom(*a, **k):
        raise RuntimeError("no egress")

    monkeypatch.setattr(contextily, "add_basemap", _boom)
    plt.close("all")
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ax = plot_static(df, basemap=True)
    assert ax is not None
    assert len(plt.get_fignums()) == 1  # figure still produced
    assert any("basemap unavailable" in str(w.message) for w in caught)
    plt.close("all")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: FAIL — `ImportError: cannot import name 'plot_static'`.

- [ ] **Step 3: Implement `plot_static`**

Append to `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py` (at the end):

```python
def plot_static(
    data,
    *,
    geom_col=None,
    grid_system=None,
    column=None,
    cmap="viridis",
    legend=True,
    basemap=True,
    basemap_source=None,
    alpha=0.8,
    edgecolor="face",
    markersize=None,
    title=None,
    fig_w=10,
    fig_h=10,
    max_rows=10_000,
    srid=None,
    ax=None,
):
    """Render geometries / DGGS cells over a basemap as a static figure.

    ``data`` is a Spark DataFrame or a geopandas.GeoDataFrame. Geometry columns
    accept WKT/EWKT/WKB/EWKB and native GEOMETRY/GEOGRAPHY (decoded via the
    shared parse_geom); set ``grid_system`` ('h3' in v1) to treat the column as
    DGGS cell ids (string or long). The contextily basemap is rendered when
    ``basemap=True``; any failure (no egress / missing dep) degrades to a
    warning and a basemap-less render. Returns the matplotlib Axes; pass it back
    via ``ax=`` to overlay layers. Requires the [vizx] extra.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    import matplotlib.pyplot as plt

    gdf = _resolve_gdf(data, geom_col, grid_system, max_rows, srid)

    created = ax is None
    if created:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))

    plot_gdf = gdf.to_crs(3857) if basemap else gdf

    kwargs = {"ax": ax, "alpha": alpha, "edgecolor": edgecolor, "cmap": cmap}
    if column is not None:
        kwargs["column"] = column
        kwargs["legend"] = legend
    if markersize is not None:
        kwargs["markersize"] = markersize
    plot_gdf.plot(**kwargs)

    if basemap:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
            cx.add_basemap(ax, source=source, crs=plot_gdf.crs)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress/missing -> fallback
            warnings.warn(
                f"plot_static: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap. Ensure network egress to the tile "
                "server at execution time for the basemap to bake into the output.",
                stacklevel=2,
            )

    if title:
        ax.set_title(title)
    ax.set_axis_off()

    if created:
        plt.show()
    return ax
```

- [ ] **Step 4: Export from the package**

Edit `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py`:

```python
from databricks.labs.gbx.vizx._raster import plot_file, plot_mask_layers, plot_raster
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._vector import as_gdf, cells_as_gdf, grid_as_gdf

__all__ = [
    "plot_raster",
    "plot_file",
    "plot_mask_layers",
    "plot_static",
    "as_gdf",
    "cells_as_gdf",
    "grid_as_gdf",
]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py`
Expected: PASS (full file green).

- [ ] **Step 6: Run the whole vizx suite (no regressions)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/`
Expected: PASS.

- [ ] **Step 7: Lint**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/src/databricks/labs/gbx/vizx/__init__.py python/geobrix/test/vizx/test_static_map.py && isort python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/src/databricks/labs/gbx/vizx/__init__.py python/geobrix/test/vizx/test_static_map.py"`
Then: `bash scripts/commands/gbx-lint-python.sh --check`
Expected: lint passes.

- [ ] **Step 8: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/src/databricks/labs/gbx/vizx/__init__.py python/geobrix/test/vizx/test_static_map.py
git commit -m "feat(vizx): plot_static renderer with contextily basemap + fallback"
```

---

### Task 5: Document `plot_static` in `vizx.mdx`

**Files:**
- Modify: `docs/docs/api/vizx.mdx`

**Interfaces:**
- Consumes: the `plot_static` signature from Task 4. Produces: no code.

- [ ] **Step 1: Add the `plot_static` section**

In `docs/docs/api/vizx.mdx`, after the `### grid_as_gdf` block (ends near the worked-example note before `## Escape hatches`), insert a new top-level section:

````markdown
## Static maps

`plot_static` renders Spark- or GeoPandas-derived geometries (or H3 cells) over
a basemap as a **static** matplotlib figure — the GitHub-renderable counterpart
to `GeoDataFrame.explore()` (whose Leaflet/folium output renders a blank
*"Make this Notebook Trusted"* placeholder on GitHub and the docs site).

The basemap is fetched from a web tile server (via `contextily`) **at execution
time** and rasterized into the figure, so it bakes into the committed notebook
output PNG — GitHub then displays it with no network. If the executing
environment has no egress, the map renders without a basemap and a warning is
emitted (never a hard error).

### `plot_static`

```python
plot_static(
    data, *, geom_col=None, grid_system=None, column=None, cmap="viridis",
    legend=True, basemap=True, basemap_source=None, alpha=0.8, edgecolor="face",
    markersize=None, title=None, fig_w=10, fig_h=10, max_rows=10_000,
    srid=None, ax=None,
)
```

`data` is a Spark DataFrame **or** a `geopandas.GeoDataFrame`. Returns the
matplotlib `Axes`; pass it back via `ax=` to overlay layers on one map.

**Geometry columns** accept the same encodings as every other `gbx_st_*`
function — WKT, EWKT, WKB, EWKB, and native `GEOMETRY` / `GEOGRAPHY` (coerced
in-Spark via `st_asbinary`). Set **`grid_system`** to treat the column as DGGS
cell ids instead:

| `grid_system` | Behaviour |
|---|---|
| `None` (default) | Column is a geometry encoding (WKT/EWKT/WKB/EWKB/`GEOMETRY`/`GEOGRAPHY`). |
| `'h3'` | Column holds H3 cell ids (string index **or** bigint); rendered as cell-boundary polygons. |
| `'quadbin'`, `'bng'`, `'custom'` | Planned — currently raise `NotImplementedError`. |

```python
from databricks.labs.gbx.vizx import plot_static

# H3 choropleth over a basemap, then overlay the shared-canvas boundary:
ax = plot_static(cells_df, grid_system="h3", column="count", title="Coverage")
plot_static(grid_boundary_df, basemap=False, ax=ax, edgecolor="red")
```

`basemap_source` overrides the default `contextily.providers.CartoDB.Positron`;
`basemap=False` skips tiles entirely (deterministic, no network).
````

- [ ] **Step 2: Verify no internal-vocabulary leak (QC gate)**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/vizx.mdx`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add docs/docs/api/vizx.mdx
git commit -m "docs(vizx): document plot_static static-map helper"
```

---

### Task 6: Adopt `plot_static` in the h3-rasterize notebook

**Files:**
- Modify: `notebooks/examples/h3-rasterize/h3_rasterize_isobands.ipynb`

**Interfaces:**
- Consumes: `plot_static` (Task 4). Produces: no code (notebook edit only).

**Note:** the deliverable is the edited **code cells**. The committed output PNGs (with a baked basemap) are refreshed when the user re-executes the notebook on a cluster with egress — not in this task. Do not attempt to execute the notebook here.

- [ ] **Step 1: Locate the static-plot cell**

Run: `python3 -c "import json; nb=json.load(open('notebooks/examples/h3-rasterize/h3_rasterize_isobands.ipynb')); [print(i, repr(''.join(c['source'])[:120])) for i,c in enumerate(nb['cells']) if c['cell_type']=='code' and 'bands_gdf.plot' in ''.join(c['source'])]"`
Expected: prints the index of the cell containing `bands_gdf.plot(column="band_level" ...)` + `grid_gdf.boundary.plot(...)`.

- [ ] **Step 2: Replace the plot cell body with `plot_static`**

Using a small Python script (so JSON stays valid), set that code cell's source to:

```python
# Static map: H3 isobands as a choropleth over a basemap, with the shared
# canvas boundary overlaid. Renders on GitHub (static PNG). For an interactive
# pan/zoom version in Databricks, use bands_gdf.explore(...) / grid_gdf.explore(m=...).
from databricks.labs.gbx.vizx import plot_static

ax = plot_static(
    bands_gdf,
    column="band_level",
    cmap="viridis",
    title="DEM isobands (H3)",
)
plot_static(grid_gdf, basemap=False, ax=ax, edgecolor="red", alpha=1.0)
```

Here `bands_gdf` / `grid_gdf` are the existing GeoDataFrames already built earlier in the notebook (from `cells_as_gdf` / `grid_as_gdf`). If they are still Spark DataFrames at that point, pass the Spark frames with `grid_system="h3"` instead — verify which by reading the cell that defines `bands_gdf`.

Use this script form to edit (preserves notebook JSON):

```bash
python3 - <<'PY'
import json
f = "notebooks/examples/h3-rasterize/h3_rasterize_isobands.ipynb"
nb = json.load(open(f))
NEW = '''# Static map: H3 isobands as a choropleth over a basemap, with the shared
# canvas boundary overlaid. Renders on GitHub (static PNG). For an interactive
# pan/zoom version in Databricks, use bands_gdf.explore(...) / grid_gdf.explore(m=...).
from databricks.labs.gbx.vizx import plot_static

ax = plot_static(
    bands_gdf,
    column="band_level",
    cmap="viridis",
    title="DEM isobands (H3)",
)
plot_static(grid_gdf, basemap=False, ax=ax, edgecolor="red", alpha=1.0)
'''
IDX = None  # set to the cell index found in Step 1
assert IDX is not None, "set IDX from Step 1"
nb["cells"][IDX]["source"] = NEW.splitlines(keepends=True)
nb["cells"][IDX]["outputs"] = []
nb["cells"][IDX]["execution_count"] = None
json.dump(nb, open(f, "w"), indent=1)
open(f, "a").write("\n")
print("updated cell", IDX)
PY
```

- [ ] **Step 3: Verify the notebook still parses + the static-render note still holds**

Run: `python3 -c "import json; json.load(open('notebooks/examples/h3-rasterize/h3_rasterize_isobands.ipynb')); print('ok')"`
Expected: `ok`.

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/` (sanity: code import path unchanged)
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add "notebooks/examples/h3-rasterize/h3_rasterize_isobands.ipynb"
git commit -m "docs(notebooks): adopt vizx.plot_static in h3-rasterize map cell"
```

---

## Out of scope / fast-follow

- `grid_system` `'quadbin'`, `'bng'`, `'custom'` cell→boundary resolvers (the dispatch seam is in place; each is one entry in `_GRID_DISPATCH` plus a resolver + tests).
- eo-series 01/03 cell-map adoption (optional; mirror Task 6 once h3-rasterize is validated by a real re-run).
- An offline/committed tile cache for no-egress executors (explicitly out of v1 per the spec).

## Self-Review

**Spec coverage:** plot_static signature + return-Axes-overlay (Task 4) ✓; Spark+GeoPandas input (Tasks 2,4) ✓; geometry encodings via parse_geom incl. native GEOMETRY/GEOGRAPHY (Task 2) ✓; grid_system h3 + forward-declared NYI (Task 3) ✓; contextily basemap + graceful fallback + 3857 reproject + CartoDB.Positron default (Task 4) ✓; contextily in [vizx] range pin + exact+hash locks both files (Task 1) ✓; tests enumerated in the spec all map to Task 2/3/4 tests ✓; docs (Task 5) ✓; notebook adoption (Task 6) ✓; PR #45 + no-push (Global Constraints) ✓.

**Placeholder scan:** none — every code step has complete code; the only deliberate IDX placeholder in Task 6 Step 2 is guarded by an `assert IDX is not None` and instructed in Step 1.

**Type consistency:** `_resolve_gdf(data, geom_col, grid_system, max_rows, srid)` signature identical across Tasks 2-4; `_resolve_cells` / `_GRID_DISPATCH` names consistent Task 3↔used nowhere else; `plot_static` keyword names identical between Task 4 impl, tests, docs (Task 5), and notebook usage (Task 6).
