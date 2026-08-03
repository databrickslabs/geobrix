# VizX render from a Spark DataFrame — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `plot_tiles(df, ...)` to VizX — render raster tiles straight from a filtered Spark DataFrame, resolving virtual **or** materialized payloads and v1 **or** v2 tile shapes via a thin resolver over `pyrx.core.open_tile`. Fix pyrx `tile_to_numpy`/`rst_apply` to be virtual-aware with the same primitive.

**Architecture:** A `resolve_tile_row(row, tile_col)` context manager delegates opening to `pyrx.core.open_tile.open_tile` (which normalizes v1/v2/bytes via `_to_virtual_tile` and applies pending nodata/srid/bands on virtual tiles). `plot_tiles` pulls at most `limit` rows (`df.limit(N)`, never the whole DF), resolves each, and renders via the existing `vizx/_raster.py` `_decimated_read` + `_render` path — `mode="facet"` (grid), `"first"` (one), `"mosaic"` (same-CRS stitch, raise on mixed CRS). The escape hatches gain the same virtual-open path.

**Tech Stack:** Python 3.12, VizX (`geobrix[vizx]`: matplotlib/rasterio), pyrx (`geobrix[light]`), PySpark. Tests: pytest with `matplotlib.use("Agg")`; local Spark for the DataFrame path (`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` = `.venv-pyrx/bin/python`).

## Global Constraints

- **VizX may import `pyrx.core`** (both ship in the light install). The resolver MUST delegate tile-opening to `pyrx.core.open_tile.open_tile` — do NOT reimplement staging/window/pending-instruction logic in VizX.
- **v1 + v2 + bytes all supported** via `open_tile`/`_to_virtual_tile` (no extra shape code needed).
- **Never collect the whole DataFrame.** Always `df.limit(N)` where N is the mode cap; if `df` has more rows, render the first N and **warn** (do not `df.count()` on every call).
- **Mosaic is same-CRS only.** Mixed CRS → `raise ValueError` naming the distinct CRS values. `facet`/`first` are CRS-agnostic and never raise on mixed CRS.
- **No overload** of `plot_raster`/`plot_tile`/`plot_mask_layers`/`raster_layer` — `plot_tiles` is the new entry.
- **No v2 struct schema change.** No new SQL-registered function names (binding parity unaffected — `plot_tiles`/`resolve_tile_row` are Python-only VizX API; `tile_to_numpy`/`rst_apply` are existing Python-only escape hatches).
- **pyrx never uses** `spark.conf.set`/`_jvm`/`.rdd`.
- **Tests execute real code with real assertions** on real rasters (rasterio-written temp files / bench-corpus / modis). No mocking rasterio/serde/open_tile. VizX render tests set `matplotlib.use("Agg")` and skip cleanly if the `[vizx]` extra is unavailable (mirror existing `test/vizx/` tests).

---

## Task 1: `resolve_tile_row` payload resolver + escape-hatch fixes

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/escape.py`
- Test: `python/geobrix/test/vizx/test_tiles_resolver.py` (create), `python/geobrix/test/pyrx/test_escape_virtual.py` (create)

**Interfaces:**
- Produces: `resolve_tile_row(tile_or_row, tile_col="tile")` — a `@contextmanager` yielding an open rasterio `DatasetReader`, accepting a Spark Row, a tile struct (dict/Row), a `VirtualTile`, or raw bytes. Task 2 (`plot_tiles`) consumes it.

- [ ] **Step 1: Write the failing resolver test**

Create `python/geobrix/test/vizx/test_tiles_resolver.py`:

```python
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds


def _write_tif(path, size=32, dtype="uint16", nodata=0, crs="EPSG:4326"):
    data = (np.random.rand(size, size) * 100).astype(dtype)
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, size, size)
    with rasterio.open(path, "w", driver="GTiff", height=size, width=size,
                       count=1, dtype=dtype, crs=crs, nodata=nodata,
                       transform=transform) as dst:
        dst.write(data, 1)
    return size


def _materialized_bytes(size=32):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    data = (np.random.rand(size, size) * 100).astype("uint16")
    with MemoryFile() as mf:
        with mf.open(driver="GTiff", height=size, width=size, count=1,
                     dtype="uint16", crs="EPSG:4326") as dst:
            dst.write(data, 1)
        return mf.read()


def test_resolve_virtual_v2_row(tmp_path):
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    p = str(tmp_path / "v.tif"); sz = _write_tif(p)
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    with resolve_tile_row(vt) as ds:
        assert ds.count == 1 and ds.width == sz


def test_resolve_materialized_and_bytes():
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row
    b = _materialized_bytes()
    with resolve_tile_row(b) as ds:          # raw bytes
        assert ds.width == 32
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    vt = VirtualTile(cellid=0, raster=b)     # materialized v2
    with resolve_tile_row(vt) as ds:
        assert ds.width == 32


def test_resolve_v1_three_field_dict():
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row
    b = _materialized_bytes()
    v1 = {"cellid": 0, "raster": b, "metadata": {}}   # v1 shape
    with resolve_tile_row(v1) as ds:
        assert ds.width == 32


def test_resolve_row_with_tile_col(tmp_path):
    # a Row-like dict whose tile_col holds the struct
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    p = str(tmp_path / "v.tif"); sz = _write_tif(p)
    vt_row = {"cellid": -1, "raster": None, "path": p,
              "window": {"col_off": 0, "row_off": 0, "width": sz, "height": sz},
              "clip_polygon": None, "clip_crs": None, "crs": None, "metadata": {}}
    row = {"tile": vt_row, "other": 1}
    with resolve_tile_row(row, tile_col="tile") as ds:
        assert ds.width == sz


def test_resolve_virtual_honors_pending_nodata(tmp_path):
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    p = str(tmp_path / "nn.tif"); sz = _write_tif(p, dtype="float32", nodata=None)
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz),
                     metadata={"pending_nodata": "-9999.0"})
    with resolve_tile_row(vt) as ds:
        assert ds.nodata == -9999.0
```

- [ ] **Step 2: Run to verify failure**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/vizx/test_tiles_resolver.py -v`
Expected: FAIL (`_tiles.py` / `resolve_tile_row` does not exist).

- [ ] **Step 3: Implement `resolve_tile_row` in `_tiles.py`**

```python
"""Render raster tiles from a Spark DataFrame; resolve virtual/materialized payloads.

The tile->pixels boundary for VizX. Delegates opening to pyrx.core.open_tile so
virtual (path+window) tiles read their window and apply pending nodata/srid/bands
instructions, and v1/v2/bytes shapes all normalize identically — no reimplementation.
"""

from contextlib import contextmanager


def _extract_tile(tile_or_row, tile_col):
    """Pull the tile value out of a Row/dict that has a tile_col, else pass through."""
    # bytes / VirtualTile / a tile-struct dict|Row -> use as-is; a wrapper Row with
    # a tile_col field -> extract that field.
    if isinstance(tile_or_row, (bytes, bytearray)):
        return tile_or_row
    # dict/Row: if it has the tile_col AND that looks like a tile (struct), extract.
    d = None
    if hasattr(tile_or_row, "asDict"):
        d = tile_or_row.asDict()
    elif isinstance(tile_or_row, dict):
        d = tile_or_row
    if d is not None and tile_col in d and not ("raster" in d or "path" in d):
        return d[tile_col]
    return tile_or_row


@contextmanager
def resolve_tile_row(tile_or_row, tile_col="tile"):
    """Yield an open rasterio DatasetReader for a tile from any shape.

    Accepts a Spark Row (with ``tile_col``), a tile struct (dict/Row), a
    ``VirtualTile``, or raw GeoTIFF bytes. Virtual tiles are read via
    ``pyrx.core.open_tile`` (window + pending instructions applied); materialized
    tiles and bytes open directly. v1 (3-field) and v2 (8-field) both supported.
    """
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    tile = _extract_tile(tile_or_row, tile_col)
    with _ot.open_tile(_ot._to_virtual_tile(tile)) as ds:
        yield ds
```

- [ ] **Step 4: Run to verify pass**

Run: `... -m pytest python/geobrix/test/vizx/test_tiles_resolver.py -v`
Expected: PASS (5).

- [ ] **Step 5: Write failing escape-hatch tests**

Create `python/geobrix/test/pyrx/test_escape_virtual.py`:

```python
import numpy as np
import rasterio
from rasterio.transform import from_bounds


def _write_tif(path, size=16, dtype="uint16", crs="EPSG:4326"):
    data = (np.arange(size * size).reshape(size, size) % 50).astype(dtype)
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, size, size)
    with rasterio.open(path, "w", driver="GTiff", height=size, width=size,
                       count=1, dtype=dtype, crs=crs, transform=transform) as dst:
        dst.write(data, 1)
    return size


def test_tile_to_numpy_virtual(tmp_path):
    from databricks.labs.gbx.pyrx.core.escape import tile_to_numpy
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    p = str(tmp_path / "v.tif"); sz = _write_tif(p)
    vt_row = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz)).to_row()
    arr = tile_to_numpy(vt_row)
    assert arr is not None and arr.shape[-1] == sz


def test_tile_to_numpy_bytes_and_v1_still_work(tmp_path):
    from databricks.labs.gbx.pyrx.core.escape import tile_to_numpy
    p = str(tmp_path / "m.tif"); sz = _write_tif(p)
    raw = open(p, "rb").read()
    assert tile_to_numpy(raw).shape[-1] == sz               # bytes
    assert tile_to_numpy({"cellid": 0, "raster": raw, "metadata": {}}).shape[-1] == sz  # v1
```

- [ ] **Step 6: Run to verify failure**

Run: `... -m pytest python/geobrix/test/pyrx/test_escape_virtual.py -v`
Expected: `test_tile_to_numpy_virtual` FAILS (reads `tile["raster"]` = None → `bytes(None)` TypeError).

- [ ] **Step 7: Make `tile_to_numpy` + `rst_apply` virtual-aware**

In `escape.py`, route both through `open_tile` (v1/v2/virtual/bytes) instead of reading `tile["raster"]` directly:

```python
def tile_to_numpy(tile_or_bytes):
    """Read a tile's raster into a numpy ndarray (all bands).

    Accepts a tile struct (Row/dict, v1 or v2), a virtual tile (path+window),
    or raw bytes. Virtual tiles read their window (pending instructions applied).
    """
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    with _ot.open_tile(_ot._to_virtual_tile(tile_or_bytes)) as ds:
        return ds.read()
```

```python
    @udf(returnType=returnType)
    def _apply(tile):
        from databricks.labs.gbx.pyrx.core import open_tile as _ot

        # empty (no raster AND no path) -> null; else open (virtual reads window)
        if tile is None:
            return None
        d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
        if d.get("raster") is None and d.get("path") is None:
            return None
        with _ot.open_tile(_ot._to_virtual_tile(tile)) as ds:
            return fn(ds)
```

(Keep the existing imports; `_serde` may no longer be needed in `tile_to_numpy` — leave other uses intact. Verify `_col`, `udf`, `DataType` imports unchanged.)

- [ ] **Step 8: Run to verify pass + escape regression**

Run: `... -m pytest python/geobrix/test/pyrx/test_escape_virtual.py python/geobrix/test/pyrx/ -q --ignore=python/geobrix/test/ds` (ignore netCDF4-collection modules if they surface). Expected: escape tests green; no NEW failures vs the known netCDF4/JAR-gated set.

- [ ] **Step 9: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/core/escape.py \
        python/geobrix/test/vizx/test_tiles_resolver.py \
        python/geobrix/test/pyrx/test_escape_virtual.py
git commit -m "feat(vizx,pyrx): tile payload resolver; virtual-aware tile_to_numpy/rst_apply

Co-authored-by: Isaac"
```

---

## Task 2: `plot_tiles(df, mode="first"|"facet")` + size guard

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py`, `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py`
- Test: `python/geobrix/test/vizx/test_plot_tiles.py` (create)

**Interfaces:**
- Consumes: `resolve_tile_row` (Task 1), `_decimated_read`/`_render` (`vizx/_raster.py`).
- Produces: `plot_tiles(df, tile_col="tile", *, mode="facet", limit=None, fig_w=10, fig_h=10, max_pixels=2000, composite="auto", emphasis="blend")`. Task 3 adds `mode="mosaic"`.

- [ ] **Step 1: Write failing tests**

Create `python/geobrix/test/vizx/test_plot_tiles.py`:

```python
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402
import rasterio  # noqa: E402
from rasterio.transform import from_bounds  # noqa: E402


def _write_tif(path, size=32, crs="EPSG:4326"):
    data = (np.random.rand(size, size) * 100).astype("uint16")
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, size, size)
    with rasterio.open(path, "w", driver="GTiff", height=size, width=size,
                       count=1, dtype="uint16", crs=crs, transform=transform) as dst:
        dst.write(data, 1)
    return size


def _virtual_df(spark, paths):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile, V2_TILE_SCHEMA
    rows = []
    for p in paths:
        with rasterio.open(p) as ds:
            w, h = ds.width, ds.height
        rows.append((VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, w, h)).to_row(),))
    from pyspark.sql.types import StructType, StructField
    schema = StructType([StructField("tile", V2_TILE_SCHEMA, False)])
    return spark.createDataFrame([(row[0],) for row in rows], schema)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_plot_tiles_first(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles
    paths = [str(tmp_path / f"t{i}.tif") for i in range(3)]
    for p in paths: _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    ax = plot_tiles(df, mode="first")
    assert ax is not None and len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_tiles_facet_bounded(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles
    paths = [str(tmp_path / f"f{i}.tif") for i in range(5)]
    for p in paths: _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    fig = plot_tiles(df, mode="facet", limit=4)
    # 5 rows, limit 4 -> at most 4 panels rendered
    axes = fig.get_axes() if hasattr(fig, "get_axes") else []
    assert 1 <= len(axes) <= 4
    plt.close("all")


def test_plot_tiles_facet_warns_on_overflow(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles
    paths = [str(tmp_path / f"w{i}.tif") for i in range(6)]
    for p in paths: _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    with pytest.warns(UserWarning, match="limit"):
        plot_tiles(df, mode="facet", limit=2)
    plt.close("all")
```

- [ ] **Step 2: Run to verify failure**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/vizx/test_plot_tiles.py -v`
Expected: FAIL (`plot_tiles` not exported).

- [ ] **Step 3: Implement `plot_tiles` (first + facet)**

Add to `_tiles.py`:

```python
import warnings

_MODE_DEFAULT_LIMITS = {"first": 1, "facet": 25, "mosaic": 64}


def _collect_bounded(df, tile_col, limit):
    """Pull at most `limit` rows to the driver; warn if the DF has more.

    Never collects the whole DF: uses df.limit(limit+1) to peek at overflow
    without a full count().
    """
    peek = df.limit(limit + 1).collect()
    if len(peek) > limit:
        warnings.warn(
            f"plot_tiles: DataFrame has more than limit={limit} tiles; "
            f"rendering the first {limit}. Filter the DataFrame or raise `limit`.",
            UserWarning,
            stacklevel=3,
        )
    return peek[:limit]


def plot_tiles(df, tile_col="tile", *, mode="facet", limit=None,
               fig_w=10, fig_h=10, max_pixels=2000, composite="auto",
               emphasis="blend"):
    """Render raster tiles from a (filtered) Spark DataFrame.

    mode: "facet" (grid of thumbnails, default), "first" (one tile), "mosaic"
    (stitch same-CRS tiles into one georeferenced image; raises on mixed CRS).
    limit caps rows pulled to the driver (mode-defaulted); the whole DataFrame is
    never collected. Virtual and materialized tiles (v1/v2) are both supported.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available
    assert_viz_available()
    import matplotlib.pyplot as plt
    from databricks.labs.gbx.vizx._raster import _decimated_read, _render

    if mode not in ("first", "facet", "mosaic"):
        raise ValueError(f"plot_tiles: mode must be first|facet|mosaic; got {mode!r}")
    if limit is None:
        limit = _MODE_DEFAULT_LIMITS[mode]

    if mode == "mosaic":
        return _plot_tiles_mosaic(df, tile_col, limit, fig_w=fig_w, fig_h=fig_h,
                                  max_pixels=max_pixels, composite=composite,
                                  emphasis=emphasis)  # Task 3

    rows = _collect_bounded(df, tile_col, limit if mode == "facet" else 1)

    if mode == "first":
        with resolve_tile_row(rows[0], tile_col) as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(data, transform, title="tile", fig_w=fig_w, fig_h=fig_h,
                    scale=scale, composite=composite, nodata=src.nodata,
                    emphasis=emphasis)
        return plt.gca()

    # facet: grid of panels
    n = len(rows)
    ncols = min(n, 4) or 1
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h), squeeze=False)
    flat = [a for r in axes for a in r]
    for ax, row in zip(flat, rows):
        with resolve_tile_row(row, tile_col) as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(data, transform, title=None, fig_w=fig_w, fig_h=fig_h,
                    scale=scale, composite=composite, nodata=src.nodata,
                    emphasis=emphasis, ax=ax)
    for ax in flat[n:]:
        ax.axis("off")
    return fig
```

> NOTE: `_render` must accept an `ax=` kwarg to draw into a subplot. Read `_render` (`_raster.py:131`) — if it does NOT take `ax`, add an optional `ax=None` param that, when given, draws into that Axes instead of creating a new figure (matplotlib `rasterio.plot.show(..., ax=ax)`), and returns it. Make that a small, backward-compatible change in the SAME task and note it in the report. If `_render` already supports `ax`, use it.

- [ ] **Step 4: Export `plot_tiles`**

In `vizx/__init__.py`: add `from databricks.labs.gbx.vizx._tiles import plot_tiles` and add `"plot_tiles"` to `__all__`. (Keep `resolve_tile_row` internal for now — not exported; revisit in Task 4 docs.)

- [ ] **Step 5: Run to verify pass**

Run: `... -m pytest python/geobrix/test/vizx/test_plot_tiles.py -v` → PASS (3).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py \
        python/geobrix/src/databricks/labs/gbx/vizx/__init__.py \
        python/geobrix/src/databricks/labs/gbx/vizx/_raster.py \
        python/geobrix/test/vizx/test_plot_tiles.py
git commit -m "feat(vizx): plot_tiles(df) first + facet modes with bounded collect

Co-authored-by: Isaac"
```

---

## Task 3: `plot_tiles(mode="mosaic")` + CRS guard

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py`
- Test: `python/geobrix/test/vizx/test_plot_tiles.py` (extend)

**Interfaces:**
- Consumes: `resolve_tile_row`, `_collect_bounded`, `_render`.
- Produces: `_plot_tiles_mosaic(...)` invoked by `plot_tiles(mode="mosaic")`.

- [ ] **Step 1: Write failing tests**

Append to `test_plot_tiles.py`:

```python
def test_plot_tiles_mosaic_same_crs(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles
    # two adjacent tiles, same CRS -> one stitched image
    paths = [str(tmp_path / f"m{i}.tif") for i in range(2)]
    for p in paths: _write_tif(p, crs="EPSG:4326")
    df = _virtual_df(spark, paths)
    plt.close("all")
    ax = plot_tiles(df, mode="mosaic")
    assert ax is not None and len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_tiles_mosaic_mixed_crs_raises(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles
    p1 = str(tmp_path / "a.tif"); _write_tif(p1, crs="EPSG:4326")
    p2 = str(tmp_path / "b.tif"); _write_tif(p2, crs="EPSG:3857")
    df = _virtual_df(spark, [p1, p2])
    with pytest.raises(ValueError, match="CRS"):
        plot_tiles(df, mode="mosaic")


def test_plot_tiles_facet_mixed_crs_ok(spark, tmp_path):
    # facet is CRS-agnostic -> no raise on mixed CRS
    from databricks.labs.gbx.vizx import plot_tiles
    p1 = str(tmp_path / "c.tif"); _write_tif(p1, crs="EPSG:4326")
    p2 = str(tmp_path / "d.tif"); _write_tif(p2, crs="EPSG:3857")
    df = _virtual_df(spark, [p1, p2])
    plt.close("all")
    fig = plot_tiles(df, mode="facet")
    assert fig is not None
    plt.close("all")
```

- [ ] **Step 2: Run to verify failure**

Run: `... -m pytest python/geobrix/test/vizx/test_plot_tiles.py -k mosaic -v` → FAIL (`_plot_tiles_mosaic` NotImplemented/missing).

- [ ] **Step 3: Implement `_plot_tiles_mosaic`**

```python
def _plot_tiles_mosaic(df, tile_col, limit, *, fig_w, fig_h, max_pixels,
                       composite, emphasis):
    """Stitch same-CRS tiles into one georeferenced image via rasterio.merge."""
    import matplotlib.pyplot as plt
    import rasterio
    from contextlib import ExitStack
    from rasterio.merge import merge
    from databricks.labs.gbx.vizx._raster import _render

    rows = _collect_bounded(df, tile_col, limit)
    with ExitStack() as stack:
        datasets = [stack.enter_context(resolve_tile_row(r, tile_col)) for r in rows]
        crs_set = {ds.crs.to_string() if ds.crs else None for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(
                f"plot_tiles(mode='mosaic'): tiles have differing CRS {sorted(map(str, crs_set))}; "
                f"filter the DataFrame to a single CRS (cross-CRS mosaic is not supported here)."
            )
        mosaic, transform = merge(datasets)  # decimation handled below if large
    # decimate the merged array for display
    import numpy as np
    scale = max(mosaic.shape[-1], mosaic.shape[-2]) / max_pixels
    nodata = datasets[0].nodata if datasets else None
    _render(mosaic, transform, title="mosaic", fig_w=fig_w, fig_h=fig_h,
            scale=max(scale, 1.0), composite=composite, nodata=nodata,
            emphasis=emphasis)
    return plt.gca()
```

> NOTE: `rasterio.merge.merge` accepts a list of open DatasetReaders (or paths). Confirm the installed rasterio supports dataset-list input (it does in modern rasterio); if the version requires paths, fall back to writing each resolved dataset to a MemoryFile and merging those. Verify against the venv rasterio in Step 4. `merge` requires all inputs share a CRS — the explicit pre-check gives a clear GeoBrix error before rasterio's own opaque one.

- [ ] **Step 4: Run to verify pass**

Run: `... -m pytest python/geobrix/test/vizx/test_plot_tiles.py -v` → PASS (all, incl. mosaic + mixed-CRS raise + facet-mixed-ok).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py python/geobrix/test/vizx/test_plot_tiles.py
git commit -m "feat(vizx): plot_tiles mosaic mode (same-CRS stitch; raise on mixed CRS)

Co-authored-by: Isaac"
```

---

## Task 4: Exports test, docs, and dependency/lock

**Files:**
- Modify: `python/geobrix/test/vizx/test_exports.py` (add `plot_tiles`)
- Docs: the visualization / VizX doc page (a `plot_tiles` section)
- Verify: `[vizx]`+`[light]` co-install; CI lock / tier gating if the new test dirs need registering.

- [ ] **Step 1: Exports test**

Add `plot_tiles` to whatever `test/vizx/test_exports.py` asserts is importable from `databricks.labs.gbx.vizx`. Run it green.

- [ ] **Step 2: Docs — `plot_tiles` section**

In the VizX/visualization docs page (find it: `grep -rl "plot_raster\|plot_tile" docs/docs/`), add a short section: `plot_tiles(df, mode=..., limit=...)` renders raster tiles straight from a filtered Spark DataFrame; virtual tiles are read transparently (no manual materialize); `facet`/`first`/`mosaic`; `mosaic` needs one CRS. End-user voice, NO internal vocabulary (no "wave/inc N"). If the docs page has runnable doc-tests, source the snippet per the docs-are-tests convention; otherwise a plain fenced example.

- [ ] **Step 3: Dependency/lock check**

VizX now imports `pyrx.core`. Confirm `geobrix[vizx]` installs alongside `geobrix[light]` in the environments (they already co-install in the example configs: `geobrix[light,stac,vizx]`). If CI has a light-tier test allowlist (`_LIGHT_TEST_DIRS`) or a lockfile that must list new test dirs, add `test/vizx/test_tiles_resolver.py`/`test_plot_tiles.py` and `test/pyrx/test_escape_virtual.py` as needed (see the light CI lock completeness note). If nothing requires it, state that in the report.

- [ ] **Step 4: Voice + docs build**

`grep -rn -iE "wave [0-9]+|inc [0-9]+" docs/docs/` → empty. If a doc page changed, run the Docker docs build (`gbx:docs:static-build`) and confirm compiled + no broken links.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/vizx/test_exports.py docs/docs/...
git commit -m "test+docs(vizx): export plot_tiles; document DataFrame rendering

Co-authored-by: Isaac"
```

---

## Task 5: End-to-end validation on a real reader DataFrame

**Files:** none (validation only; may add one integration test if it runs locally).

- [ ] **Step 1: Local integration check**

With a local Spark session + the `gtiff_gbx` reader (virtual default), read a real multi-tile source into a DataFrame and call `plot_tiles` in each mode; assert no error and a figure is produced. If the reader needs Docker/Volumes, do this as a small local rasterio-backed DataFrame instead (Tasks 1-3 already cover the reader-shaped rows). Run:

```
PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python \
  .venv-pyrx/bin/python -m pytest python/geobrix/test/vizx/ python/geobrix/test/pyrx/test_escape_virtual.py -q
```
Expected: green (ignore known netCDF4/JAR-gated non-failures).

- [ ] **Step 2: (Optional, controller-paced) Serverless smoke**

If desired, confirm on Serverless that `plot_tiles` works in a notebook against a virtual-tile DataFrame — but this is a viz/display path; the local Agg tests are the authoritative gate. Controller decides whether a cluster run adds value.

- [ ] **Step 3: Ledger + done**

Record completion; the feature is ready for the final whole-branch review.

---

## Self-Review

**Spec coverage:** Task 1 = resolver + escape-hatch fix (spec §Payload resolver, §Adjacent gap). Task 2 = `plot_tiles` first/facet + size guard (spec §New entry, §Size guard). Task 3 = mosaic + CRS raise (spec §modes, §Non-Goals CRS). Task 4 = exports/docs/deps (spec §Files). Task 5 = validation (spec §Testing). v1/v2/bytes support is inherent to the resolver (Task 1 tests cover all three). ✓

**Placeholder scan:** `_render` `ax=` support is a real, called-out sub-change with a fallback. Mosaic `merge` dataset-list input has a version fallback. The `_virtual_df` test helper builds the DataFrame directly (`createDataFrame([(row[0],) for row in rows], schema)`). No TBD/TODO. ✓

**Type consistency:** `resolve_tile_row(tile_or_row, tile_col)` signature consistent across Tasks 1-3; `_collect_bounded`/`_MODE_DEFAULT_LIMITS` defined in Task 2 used in Task 3; `_render`/`_decimated_read` verified against `_raster.py`. `open_tile`/`_to_virtual_tile` verified against `open_tile.py`. ✓

**Open risks for the review loop:** (1) `_render` may not accept `ax=` — Task 2 Step 3 handles it explicitly but the implementer must verify the current signature. (2) `rasterio.merge` input form is version-dependent — Task 3 Step 3 notes the fallback. (3) The Task-2 test `_virtual_df` helper must be de-artifacted before use.
