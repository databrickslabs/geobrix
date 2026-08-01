# Reader `clipPolygons` / `windows` Selection (drop bbox) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the light `raster_gbx` reader's single `bbox`/`bboxCrs` option with `clipPolygons` (single or list of geometries) and `windows` (single or list of pixel windows), mutually exclusive, both working in materialized and virtual modes.

**Architecture:** `raster_gbx` is a Spark Python DataSource V2 (`RasterGbxReader`/`RasterGbxDataSource` in `ds/raster.py`) emitting `(source, tile<v2>)` via `_v2_tile_row` (Inc 2). This increment adds a `window_for_geom` helper, replaces option parsing + the planning `bbox` branch with `clipPolygons`/`windows` branches, and honors the clip in emission: materialized tiles are pre-clipped (Choice 2) via the existing `_clip.clip_dataset`; virtual tiles carry the clip as instructions for `open_tile`.

**Tech Stack:** Python 3.12, PySpark Python DataSource V2, rasterio ≥1.3, shapely, pytest. Tests run in the `geobrix-dev` Docker container via `gbx:test:pyrx`. Serverless via `.venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py` (oauth-fe, env v5).

## Global Constraints

- **Light tier only:** rasterio/shapely only, NO `osgeo.gdal`; NO `spark.conf.set`/`_jvm`/`.rdd` in library code.
- **Exploratory / non-wired:** reader options only. NO catalog / `registered_functions.txt` / `function-info.json` / bindings changes.
- **DRY / reuse:** `pyrx._geom.parse_geom` (WKB/EWKB/WKT/EWKT), `pyrx.core._clip.clip_dataset` (mask + cutline reproject; returns clipped GTiff bytes or None on non-overlap), `_v2_tile_row` (Inc 2 assembly), `_get_or_stage_file`. Do NOT reimplement clip/mask/reproject.
- **CRS precedence (UNIFIED everywhere — reader, `_clip`, tile field):** embedded EWKB/EWKT SRID (>0) → `clipCrs` → raster CRS. `clip_crs` applies ONLY to geometries with no embedded SRID (plain WKB/WKT). Task 1 aligns `_clip.clip_dataset` to this (it previously overrode embedded SRID with `clip_crs`). So the reader can pass its `clipCrs` straight through to `_clip` and both do the same thing; no per-layer contradiction.
- **Reference-vs-instruction:** materialized tile (`raster` non-null) → `window`/`clip_polygon`/`clip_crs` are provenance of what was ALREADY applied. Virtual tile (`raster` null) → they are pending instructions.
- **Skip semantics:** polygon envelope disjoint from raster → no tile; clip masking out all window pixels (`clip_dataset` returns None) → no tile; `windows` entry fully outside extent → no tile; partly-outside window → clip to extent.
- **Mutually exclusive:** `clipPolygons` and `windows` both set → `ValueError` at option parse. Neither → whole-file (Inc 2, unchanged).
- **Plural option names:** `clipPolygons`, `windows` (documented "single value or list"); `clipCrs` singular.
- **Breaking:** `bbox`/`bboxCrs` options and their `_read_legacy` branch are REMOVED. Migrate `test_raster_bbox.py`.
- Emitted rows are positional 8-tuples in `V2_TILE_SCHEMA` order via `_v2_tile_row`. `window` is `(col,row,w,h)` or None.
- Test dir: `python/geobrix/test/ds/`. Run: `bash scripts/commands/gbx-test-pyrx.sh --path <file> --log <name>.log` (Docker; dispatch as a Task). `gbx:test:pyrx` now accepts repeated `--path`.
- **Lint with the CI-pinned tools:** format via `.venv-pyrx/bin/isort` + `.venv-pyrx/bin/black --line-length 88` (or in-container), NEVER an ad-hoc black. Verify with Docker `gbx:lint:python --check` before declaring a task done.
- Commits: body ends `Co-authored-by: Isaac`. `gh auth switch --user mjohns-databricks` before any push.
- Anchors in `ds/raster.py` (current): `_plan_partitions_for_file` signature line 326, `bbox` planning branch 359-383, `RasterGbxReader.__init__` bbox parse 473-487 + `emit_virtual` 488, `partitions()` passes bbox 502-508, `read()` virtual branch 538-558 / passthrough 563 / encode 590-612, `_read_legacy` bbox branch 623-648. `_window.window_for_bbox` in `ds/_window.py`. `_clip.clip_dataset(ds, clip_polygon: bytes, clip_crs) -> Optional[bytes]`.

---

### Task 1: align `_clip.clip_dataset` CRS rule (embedded SRID wins over clip_crs)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py`
- Test: `python/geobrix/test/pyrx/test_core_virtual_clip.py` (add a case)

**Interfaces:**
- Consumes: `parse_geom`, shapely, `edit.clip_to_geom` (unchanged).
- Produces: `clip_dataset(ds, clip_polygon, clip_crs)` unchanged signature/return, but `clip_crs` is now applied ONLY when the parsed geometry has no embedded SRID (`shapely.get_srid(geom) <= 0`). An EWKB/EWKT with SRID > 0 keeps its own SRID even if `clip_crs` is passed.

**Design note:** current body does `if clip_crs: geom = set_srid(geom, code)` unconditionally ("authoritative override"). Change to only `set_srid` when the geom lacks an SRID. All existing tests pass plain WKB (SRID 0), so they still reproject via `clip_crs` — behavior unchanged for them; only the EWKB-with-SRID + clip_crs combination changes (now embedded SRID wins).

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/pyrx/test_core_virtual_clip.py
import shapely
from shapely import set_srid


def test_embedded_srid_wins_over_clip_crs():
    # UTM raster; polygon carries embedded SRID 4326 (lon/lat covering the raster).
    # A mismatched clip_crs="EPSG:3857" must NOT override the embedded 4326 —
    # the clip still succeeds because the true CRS (4326) is used.
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from rasterio.warp import transform_bounds
    from shapely.geometry import box

    tr = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    prof = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                crs="EPSG:32633", transform=tr, nodata=-9999.0)
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
        utm_bytes = mf.read()

    minx, miny, maxx, maxy = transform_bounds("EPSG:32633", "EPSG:4326",
                                              500000, 4999200, 500800, 5000000)
    g = set_srid(box(minx, miny, maxx, maxy), 4326)
    ewkb = shapely.wkb.dumps(g, include_srid=True)

    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import _clip
    with _serde.open_tile(utm_bytes) as ds:
        # clip_crs deliberately WRONG (3857); embedded 4326 must win -> clip succeeds
        out = _clip.clip_dataset(ds, ewkb, clip_crs="EPSG:3857")
    assert out is not None
    with _serde.open_tile(out) as ds2:
        assert ds2.crs.to_epsg() == 32633  # raster CRS unchanged
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_clip.py --log clipcrs.log`
Expected: FAIL — current code overrides embedded 4326 with 3857 → `edit.clip_to_geom` reprojects from the wrong CRS → non-overlap → None (or wrong clip).

- [ ] **Step 3: Implement**

In `_clip.clip_dataset`, replace the unconditional override:

```python
    geom = parse_geom(clip_polygon)
    if geom is None:
        return None
    # clip_crs applies ONLY when the geometry carries no embedded SRID.
    # embedded SRID (>0) -> clip_crs -> raster CRS (edit.clip_to_geom's own fallback).
    if clip_crs and shapely.get_srid(geom) <= 0:
        code = _epsg_int(clip_crs)
        if code is not None:
            geom = shapely.set_srid(geom, code)
    return edit.clip_to_geom(ds, geom)
```

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_virtual_clip.py --log clipcrs.log`
Expected: PASS (the new case + all prior clip tests — they use plain WKB so are unaffected).

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py python/geobrix/test/pyrx/test_core_virtual_clip.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py python/geobrix/test/pyrx/test_core_virtual_clip.py
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/_clip.py python/geobrix/test/pyrx/test_core_virtual_clip.py
git commit -m "fix(pyrx): clip_crs applies only when geom lacks embedded SRID

Unifies CRS precedence to embedded SRID -> clip_crs -> raster CRS
everywhere (reader + _clip). Previously clip_dataset overrode an
embedded EWKB SRID with clip_crs; now an EWKB/EWKT with SRID>0 keeps
it. Plain WKB (all existing tests) is unaffected.

Co-authored-by: Isaac"
```

---

### Task 2: `window_for_geom` — envelope window for an arbitrary geometry

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_window.py`
- Test: `python/geobrix/test/ds/test_window.py` (add cases)

**Interfaces:**
- Consumes: shapely, rasterio, `pyrx._geom.parse_geom`.
- Produces: `window_for_geom(src, geom, geom_crs=None) -> Optional[Window]` — `geom` is a shapely geometry already in `geom_crs` (or raster CRS if `geom_crs` is None); reproject its bounds `geom_crs → src.crs`, compute the integer pixel envelope (`from_bounds` → floor/ceil), intersect with `(0,0,W,H)`, return None if disjoint. Same clip-safe contract as `window_for_bbox`.

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/ds/test_window.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.ds._window import window_for_geom


def _ds(width=1000, height=1000, epsg=4326):
    # origin (10,50), 0.001 deg pixels
    prof = dict(driver="GTiff", width=width, height=height, count=1,
                dtype="float32", crs=f"EPSG:{epsg}",
                transform=from_origin(10.0, 50.0, 0.001, 0.001), nodata=-9999.0)
    mf = MemoryFile()
    with mf.open(**prof) as ds:
        ds.write(np.zeros((height, width), "float32"), 1)
    return mf.open()


def test_window_for_geom_same_crs_envelope():
    ds = _ds()
    # a box over cols 100..300, rows 50..250 in pixel space
    minx = 10.0 + 100 * 0.001
    maxx = 10.0 + 300 * 0.001
    maxy = 50.0 - 50 * 0.001
    miny = 50.0 - 250 * 0.001
    win = window_for_geom(ds, box(minx, miny, maxx, maxy), geom_crs="EPSG:4326")
    assert win is not None
    assert (int(win.col_off), int(win.row_off)) == (100, 50)
    assert int(win.width) == 200 and int(win.height) == 200


def test_window_for_geom_overhang_clips_to_extent():
    ds = _ds(width=200, height=200)
    # box extends beyond the raster on the +x/+y side
    win = window_for_geom(ds, box(10.0 + 100 * 0.001, 50.0 - 500 * 0.001,
                                  10.0 + 900 * 0.001, 50.0), geom_crs="EPSG:4326")
    assert win is not None
    assert int(win.col_off) == 100 and int(win.width) == 100  # clipped to 200-wide raster


def test_window_for_geom_disjoint_returns_none():
    ds = _ds()
    win = window_for_geom(ds, box(100.0, 10.0, 101.0, 11.0), geom_crs="EPSG:4326")
    assert win is None


def test_window_for_geom_reprojects_bounds():
    # UTM raster; geom given in 4326 must reproject and cover it
    from rasterio.warp import transform_bounds
    prof = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                crs="EPSG:32633", transform=from_origin(500000.0, 5000000.0, 100.0, 100.0),
                nodata=-9999.0)
    mf = MemoryFile()
    with mf.open(**prof) as d:
        d.write(np.zeros((8, 8), "float32"), 1)
    ds = mf.open()
    minx, miny, maxx, maxy = transform_bounds("EPSG:32633", "EPSG:4326",
                                              500000, 4999200, 500800, 5000000)
    win = window_for_geom(ds, box(minx, miny, maxx, maxy), geom_crs="EPSG:4326")
    assert win is not None and int(win.width) > 0 and int(win.height) > 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_window.py --log win.log`
Expected: FAIL — `ImportError: cannot import name 'window_for_geom'`.

- [ ] **Step 3: Implement**

Add to `ds/_window.py`:

```python
def window_for_geom(src, geom, geom_crs: Optional[str] = None) -> Optional[Window]:
    """Clipped, integer, in-bounds Window of ``src`` covering ``geom``'s envelope.

    ``geom`` is a shapely geometry in ``geom_crs`` (or ``src.crs`` if None). The
    geometry's bounds are reprojected to ``src.crs`` and turned into a whole-pixel
    window clipped to the dataset; returns None if it does not overlap. Same
    clip-safe contract as ``window_for_bbox`` (window and window_transform agree).
    """
    minx, miny, maxx, maxy = geom.bounds
    return window_for_bbox(src, (minx, miny, maxx, maxy), geom_crs)
```

(Delegates to `window_for_bbox` on the geometry's bounds — reuse, no new envelope math.)

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_window.py --log win.log`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/ds/_window.py python/geobrix/test/ds/test_window.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/ds/_window.py python/geobrix/test/ds/test_window.py
git add python/geobrix/src/databricks/labs/gbx/ds/_window.py python/geobrix/test/ds/test_window.py
git commit -m "feat(ds): window_for_geom envelope window for arbitrary geometry

Generalizes window_for_bbox to a shapely geometry's bounds (reproject
+ whole-pixel envelope clipped to extent, None on disjoint). Reused by
the clipPolygons reader selection.

Co-authored-by: Isaac"
```

---

### Task 3: option parsing — `clipPolygons` / `windows` / `clipCrs`, drop bbox

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`RasterGbxReader.__init__`)
- Test: `python/geobrix/test/ds/test_raster_options.py`

**Interfaces:**
- Consumes: nothing new (string/list option parsing).
- Produces: `RasterGbxReader` parses `clipPolygons` (single geometry-input or list → `self.clip_polygons: list`), `windows` (single `(c,r,w,h)` or list → `self.windows: list`), `clipCrs` (`self.clip_crs: Optional[str]`); raises `ValueError` if both `clip_polygons` and `windows` are non-empty; `bbox`/`bboxCrs` removed. A geometry-input is bytes (WKB/EWKB) or str (WKT/EWKT); a window is a 4-int tuple/list.

**Design note:** options arrive as strings from Spark `.option()`, but programmatic callers may pass Python lists via the DataSource options dict. Normalize: if the value is a single geometry (bytes/str not looking like a list) wrap in `[v]`; if a list, use as-is. For `windows`, accept a `(c,r,w,h)` tuple/list → `[tuple]`, or a list of them. Keep the normalizer small and covered by tests. (Spark `.option` string-encoding of lists is out of scope here — tests exercise the Python-list and single-value forms the reader receives; the Serverless leg confirms the real call path.)

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_options.py
"""Reader option parsing: clipPolygons/windows/clipCrs normalization + exclusion."""
import pytest
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxReader


def _wkb():
    return shapely.wkb.dumps(box(0, 0, 1, 1))


def test_single_clip_polygon_normalizes_to_list():
    r = RasterGbxReader({"path": "/x", "clipPolygons": _wkb()})
    assert isinstance(r.clip_polygons, list) and len(r.clip_polygons) == 1
    assert r.windows == []


def test_list_clip_polygons():
    r = RasterGbxReader({"path": "/x", "clipPolygons": [_wkb(), _wkb()]})
    assert len(r.clip_polygons) == 2


def test_single_window_normalizes_to_list():
    r = RasterGbxReader({"path": "/x", "windows": (0, 0, 256, 256)})
    assert r.windows == [(0, 0, 256, 256)]
    assert r.clip_polygons == []


def test_list_windows():
    r = RasterGbxReader({"path": "/x", "windows": [(0, 0, 256, 256), (256, 0, 256, 256)]})
    assert len(r.windows) == 2


def test_clip_crs_parsed():
    r = RasterGbxReader({"path": "/x", "clipPolygons": _wkb(), "clipCrs": "EPSG:4326"})
    assert r.clip_crs == "EPSG:4326"


def test_clip_and_windows_mutually_exclusive():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "clipPolygons": _wkb(), "windows": (0, 0, 8, 8)})


def test_no_bbox_option_attribute():
    r = RasterGbxReader({"path": "/x"})
    assert r.clip_polygons == [] and r.windows == [] and r.clip_crs is None
    assert not hasattr(r, "bbox")
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_options.py --log opt.log`
Expected: FAIL — reader still has `bbox`, no `clip_polygons`.

- [ ] **Step 3: Implement**

In `RasterGbxReader.__init__`, replace the bbox block (lines 473-487) with:

```python
        # AOI selection (mutually exclusive): clipPolygons (arbitrary geometry,
        # single or list) OR windows (pixel (col,row,w,h), single or list).
        self.clip_polygons = _as_geom_list(options.get("clipPolygons"))
        self.windows = _as_window_list(options.get("windows"))
        self.clip_crs = options.get("clipCrs")
        if self.clip_polygons and self.windows:
            raise ValueError(
                "raster_gbx: 'clipPolygons' and 'windows' are mutually exclusive; "
                "supply one, not both."
            )
```

Add module-level helpers near the reader:

```python
def _as_geom_list(val) -> list:
    """Normalize a clipPolygons option to a list of geometry inputs (bytes/str)."""
    if val is None or val == "":
        return []
    if isinstance(val, (bytes, bytearray, str)):
        return [val]
    return list(val)  # already a sequence of geometry inputs


def _as_window_list(val) -> list:
    """Normalize a windows option to a list of (col,row,w,h) int tuples."""
    if val is None or val == "":
        return []
    # single (c,r,w,h)?
    if (isinstance(val, (tuple, list)) and len(val) == 4
            and all(isinstance(v, (int, float)) for v in val)):
        return [tuple(int(v) for v in val)]
    return [tuple(int(v) for v in w) for w in val]  # list of windows
```

Delete `self.bbox`/`self.bbox_crs`. (Follow-on tasks fix `partitions()`/planning/`read` which still reference them — the suite will be red until Task 4/5; that is expected within this plan's sequence.)

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_options.py --log opt.log`
Expected: PASS (7 tests). Other reader tests may now error on the missing `self.bbox` — that is fixed in Task 4.

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
git commit -m "feat(ds): parse clipPolygons/windows/clipCrs, drop bbox options

clipPolygons + windows normalize single-or-list; mutually exclusive
(ValueError if both). clipCrs optional. bbox/bboxCrs removed. Planning
and read still reference the old fields (fixed in the next tasks).

Co-authored-by: Isaac"
```

---

### Task 4: planning branches — clipPolygons → envelope partitions, windows → window partitions

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`_plan_partitions_for_file`, `partitions()`)
- Test: `python/geobrix/test/ds/test_raster_plan_select.py`

**Interfaces:**
- Consumes: `window_for_geom` (Task 2), `parse_geom`, shapely, `self.clip_polygons`/`windows`/`clip_crs` (Task 3).
- Produces: `_plan_partitions_for_file(file_path, budget_bytes, *, clip_polygons, clip_crs, windows, emit_virtual=False)` — replaces `bbox`/`bbox_crs` params. Branch order after `emit_virtual`: clipPolygons → per-polygon `_TilePartition(window=envelope, clip_polygon=<wkb>, clip_crs=<resolved crs str or None>)` (skip disjoint); windows → per-window `_TilePartition(window=clipped, clip_polygon=None)` (skip fully-outside); neither → normal whole/split path.
- Per-polygon CRS resolution helper `_resolve_clip_crs(geom, reader_clip_crs) -> Optional[str]`: if `shapely.get_srid(geom) > 0` → `f"EPSG:{srid}"`; elif reader_clip_crs → reader_clip_crs; else None (raster CRS).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_plan_select.py
"""Driver-side planning for clipPolygons / windows selection."""
import numpy as np
import rasterio
import shapely.wkb
from rasterio.transform import from_origin
from shapely import set_srid
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import _plan_partitions_for_file


def _write(tmp_path, width=1000, height=1000):
    p = str(tmp_path / "r.tif")
    prof = dict(driver="GTiff", width=width, height=height, count=1, dtype="float32",
                crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.001, 0.001),
                nodata=-9999.0)
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.zeros((height, width), "float32"), 1)
    return p


def _boxwkb(c0, r0, c1, r1):
    return shapely.wkb.dumps(box(10.0 + c0 * 0.001, 50.0 - r1 * 0.001,
                                 10.0 + c1 * 0.001, 50.0 - r0 * 0.001))


def test_clip_polygons_one_partition_each(tmp_path):
    p = _write(tmp_path)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[_boxwkb(100, 50, 300, 250), _boxwkb(400, 400, 500, 500)],
        clip_crs="EPSG:4326", windows=[])
    assert len(parts) == 2
    assert parts[0].window == (100, 50, 200, 200)
    assert parts[0].clip_polygon is not None and parts[0].clip_crs == "EPSG:4326"


def test_clip_polygon_disjoint_skipped(tmp_path):
    p = _write(tmp_path)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[shapely.wkb.dumps(box(100.0, 10.0, 101.0, 11.0))],
        clip_crs="EPSG:4326", windows=[])
    assert parts == []


def test_windows_one_partition_each_partial_clipped(tmp_path):
    p = _write(tmp_path, width=200, height=200)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[], clip_crs=None,
        windows=[(0, 0, 256, 256), (50, 50, 100, 100)])
    # first window overhangs 200x200 -> clipped to (0,0,200,200); second fits
    assert len(parts) == 2
    assert parts[0].window == (0, 0, 200, 200)
    assert parts[1].window == (50, 50, 100, 100)
    assert all(p.clip_polygon is None for p in parts)


def test_window_fully_outside_skipped(tmp_path):
    p = _write(tmp_path, width=200, height=200)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[], clip_crs=None, windows=[(500, 500, 10, 10)])
    assert parts == []


def test_embedded_srid_overrides_clip_crs(tmp_path):
    p = _write(tmp_path)
    g = set_srid(box(10.1, 49.8, 10.2, 49.9), 4326)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[shapely.wkb.dumps(g, include_srid=True)],
        clip_crs="EPSG:3857", windows=[])  # clipCrs is 3857 but embedded is 4326
    assert len(parts) == 1
    assert parts[0].clip_crs == "EPSG:4326"  # embedded SRID wins
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_plan_select.py --log plan.log`
Expected: FAIL — signature still takes `bbox`/`bbox_crs`.

- [ ] **Step 3: Implement**

Change `_plan_partitions_for_file` signature to `(file_path, budget_bytes, *, clip_polygons=(), clip_crs=None, windows=(), emit_virtual=False)`. After the `emit_virtual` short-circuit, replace the `bbox` branch with:

```python
    # clipPolygons: one tile per polygon whose envelope intersects the raster.
    if clip_polygons:
        from databricks.labs.gbx.ds._window import window_for_geom
        from databricks.labs.gbx._geom import parse_geom
        import shapely

        parts = []
        with rasterio.open(file_path) as ds:
            for raw in clip_polygons:
                geom = parse_geom(raw)
                if geom is None:
                    raise ValueError(f"raster_gbx: unparseable clipPolygons entry: {raw!r}")
                resolved = _resolve_clip_crs(geom, clip_crs)  # embedded SRID -> clipCrs -> None
                win = window_for_geom(ds, geom, geom_crs=resolved)
                if win is None:
                    continue  # envelope disjoint -> no tile
                parts.append(_TilePartition(
                    file_path=file_path,
                    window=(int(win.col_off), int(win.row_off),
                            int(win.width), int(win.height)),
                    is_passthrough=False, is_whole=True, emit_fmt="gtiff",
                    clip_polygon=(raw if isinstance(raw, (bytes, bytearray))
                                  else shapely.wkb.dumps(geom)),
                    clip_crs=resolved,
                ))
        return parts

    # windows: one tile per pixel window, clipped to extent (skip fully-outside).
    if windows:
        from rasterio.windows import Window as _W
        parts = []
        with rasterio.open(file_path) as ds:
            full = _W(0, 0, ds.width, ds.height)
            for c, r, w, h in windows:
                try:
                    iw = _W(c, r, w, h).intersection(full)
                except Exception:
                    continue  # disjoint
                if iw.width < 1 or iw.height < 1:
                    continue
                parts.append(_TilePartition(
                    file_path=file_path,
                    window=(int(iw.col_off), int(iw.row_off),
                            int(iw.width), int(iw.height)),
                    is_passthrough=False, is_whole=True, emit_fmt="gtiff",
                ))
        return parts
```

Add the resolver near the planning fn:

```python
def _resolve_clip_crs(geom, reader_clip_crs):
    """Reader CRS precedence: embedded SRID > reader clipCrs > raster CRS (None)."""
    import shapely
    srid = shapely.get_srid(geom)
    if srid and srid > 0:
        return f"EPSG:{srid}"
    return reader_clip_crs or None
```

Update `partitions()` (lines 502-508) to pass `clip_polygons=self.clip_polygons, clip_crs=self.clip_crs, windows=self.windows, emit_virtual=self.emit_virtual` (drop bbox args).

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_plan_select.py --log plan.log`
Expected: PASS (5 tests).

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
git commit -m "feat(ds): plan clipPolygons/windows partitions (drop bbox branch)

Per-polygon envelope partitions (embedded SRID > clipCrs > raster,
disjoint skipped) and per-window partitions (clipped to extent,
fully-outside skipped). partitions() passes the new selection args.

Co-authored-by: Isaac"
```

---

### Task 5: emission — materialized pre-clip (Choice 2) + virtual instructions; drop legacy bbox

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`read()` encode branch, `_read_legacy` bbox branch removal)
- Test: `python/geobrix/test/ds/test_raster_clip.py` (new; replaces the bbox datasource tests)

**Interfaces:**
- Consumes: `_clip.clip_dataset`, `_v2_tile_row`, `_get_or_stage_file`, `open_tile`/`VirtualTile` (for the round-trip test), `_encode`.
- Produces: `read()` — when `partition.clip_polygon` is set and NOT virtual: stage → read the envelope window → `_clip.clip_dataset` to pre-clip → if None (all-nodata/non-overlap) emit nothing → else v2 tile with `raster`=clipped bytes and `clip_polygon`/`clip_crs` reference. Virtual clip tiles carry `clip_polygon`/`clip_crs` in the row (raster null). Non-clip windows unchanged. `_read_legacy` bbox branch removed.

**Design note (reuse + the skip):** the materialized-clip read = read the envelope window into a windowed dataset, then `clip_dataset`. The cleanest reuse is to build a `VirtualTile(path, window, clip_polygon, clip_crs)` and call `open_tile` (Inc 1) to get the clipped dataset, then re-encode to bytes — BUT `open_tile` yields an empty 1×1 on disjoint clip rather than None, which conflicts with the skip-empty rule. So instead: stage + `rasterio.open`, read the window into a `MemoryFile` dataset, call `_clip.clip_dataset(win_ds, clip_polygon, clip_crs)`; a None return → skip (yield nothing). This keeps the skip contract explicit. The virtual branch (Task-2/Inc-2) must also thread `clip_polygon`/`clip_crs` into `_v2_tile_row`.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_clip.py
"""clipPolygons emission: materialized pre-clip (Choice 2) + virtual instructions."""
import numpy as np
import rasterio
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _write(tmp_path, width=512, height=512):
    p = str(tmp_path / "r.tif")
    prof = dict(driver="GTiff", width=width, height=height, count=1, dtype="float32",
                crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.001, 0.001),
                nodata=-9999.0)
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(width * height, dtype="float32").reshape(height, width), 1)
    return p


def _tri_wkb():
    # a triangle inside cols 50..250, rows 50..250 (non-rectangular -> real mask)
    from shapely.geometry import Polygon
    pts = [(10.0 + 50 * 0.001, 50.0 - 250 * 0.001),
           (10.0 + 250 * 0.001, 50.0 - 250 * 0.001),
           (10.0 + 150 * 0.001, 50.0 - 50 * 0.001)]
    return shapely.wkb.dumps(Polygon(pts))


def test_materialized_clip_is_preclipped_with_nodata(spark, tmp_path):
    p = _write(tmp_path)
    df = (spark.read.format("raster_gbx")
          .option("clipPolygons", _tri_wkb()).option("clipCrs", "EPSG:4326").load(p))
    rows = df.collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    assert t["raster"] is not None                 # materialized
    assert t["clip_polygon"] is not None            # reference to applied clip
    with MemoryFile(bytes(t["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)
        nod = ds.nodata
    # triangle mask -> some interior real pixels AND some nodata corners present
    assert np.any(arr == nod) and np.any(arr != nod)


def test_virtual_clip_carries_instructions_and_round_trips(spark, tmp_path):
    p = _write(tmp_path)
    wkb = _tri_wkb()
    df = (spark.read.format("raster_gbx").option("virtualTiles", "true")
          .option("clipPolygons", wkb).option("clipCrs", "EPSG:4326").load(p))
    rows = df.collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    assert t["raster"] is None                      # virtual: instructions, not applied
    assert t["clip_polygon"] is not None and t["clip_crs"] == "EPSG:4326"
    # round-trip: open_tile applies the clip -> same as a direct mask of the window
    tile = VirtualTile.from_row(t)
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
        gnod = ds.nodata
    assert np.any(got == gnod) and np.any(got != gnod)  # triangle masked
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_clip.py --log clip.log`
Expected: FAIL — clip not applied in read (materialized tile not pre-clipped) / virtual clip fields not populated.

- [ ] **Step 3: Implement**

(a) Virtual branch (read, ~line 538): thread clip into the row —
```python
            yield (source, _v2_tile_row(
                _encode.CELLID_FRESH, None, path=partition.file_path,
                window=partition.window, metadata=meta,
                clip_polygon=partition.clip_polygon, clip_crs=partition.clip_crs))
```

(b) Materialized clip: in `read()`, BEFORE the generic encode branch, add — when `partition.clip_polygon is not None` (and not virtual/passthrough):
```python
        if partition.clip_polygon is not None:
            from databricks.labs.gbx.pyrx.core import _clip
            local_path = _get_or_stage_file(partition.file_path)
            col, row, w, h = partition.window
            with rasterio.Env(GDAL_CACHEMAX=128):
                with rasterio.open(local_path) as ds:
                    _cellid, win_bytes, meta = _encode.encode_tile(
                        ds, window=partition.window,
                        source_path=partition.file_path,
                        all_parents=partition.all_parents, tile_format="gtiff")
            with MemoryFile(win_bytes) as mf, mf.open() as wds:
                clipped = _clip.clip_dataset(wds, partition.clip_polygon, partition.clip_crs)
            if clipped is None:
                return  # all-nodata / non-overlap -> skip (no tile)
            yield (source, _v2_tile_row(
                _encode.CELLID_FRESH, clipped, path=partition.file_path,
                window=partition.window, metadata=meta,
                clip_polygon=partition.clip_polygon, clip_crs=partition.clip_crs))
            return
```
(`_encode.metadata_for` does NOT exist — reuse the `meta` returned by the `encode_tile` call above [it returns `(cellid, raster_bytes, meta)`], which is the correct provenance for this source/window. `raster` bytes = the clipped GTiff from `clip_dataset`. `MemoryFile` is imported at top of `raster.py` or import locally.)

(c) `_read_legacy`: delete the `self.bbox` branch (lines 623-648) entirely; the legacy path keeps only its whole/split behavior. If any legacy test depended on bbox, it is migrated in Task 6.

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_clip.py --log clip.log`
Expected: PASS (2 tests). If `clip_dataset` returns a non-None but all-nodata array for a degenerate triangle, adjust the test geometry so interior real pixels exist (the invariant: mask present + real pixels present).

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort <changed files>
.venv-pyrx/bin/black --line-length 88 <changed files>
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_clip.py
git commit -m "feat(ds): clip emission — materialized pre-clip + virtual instructions

Materialized clipPolygons tiles are pre-masked via _clip.clip_dataset
(Choice 2; all-nodata/non-overlap -> skip), clip fields as reference.
Virtual clip tiles carry clip_polygon/clip_crs as instructions that
open_tile applies. Legacy bbox read path removed.

Co-authored-by: Isaac"
```

---

### Task 6: migrate bbox tests + full regression + Serverless

**Files:**
- Modify/Remove: `python/geobrix/test/ds/test_raster_bbox.py` → migrate to `test_raster_clip.py` equivalents (box-as-clipPolygon) or delete bbox-only cases; sweep `test_raster_datasource.py` for any `bbox` references.
- Serverless notebook: `prompts/features/2026-08-01-reader-clip-select-serverless.py` (gitignored scratch).

**Interfaces:** consumes the shipped reader. No new source.

- [ ] **Step 1: Migrate/remove bbox tests**

`test_raster_bbox.py` exercised `.option("bbox",...)`. For each meaningful case (windows-to-AOI, overhang-clips, non-overlap-skipped), re-express with `.option("clipPolygons", <box wkb>)` in `test_raster_clip.py` (a box IS its own envelope, so the window equals the old bbox window; materialized pre-clip of a box = the full window since a rectangle masks nothing out). Delete `test_raster_bbox.py`. Grep `test_raster_datasource.py` for `bbox` and remove/convert.

- [ ] **Step 2: Scoped regression (raster ds, exclude netcdf)**

Run (netcdf collection errors are a PRE-EXISTING missing-dep env issue — scope around them):
```
bash scripts/commands/gbx-test-pyrx.sh \
  --path python/geobrix/test/ds/test_raster_datasource.py \
  --path python/geobrix/test/ds/test_raster_clip.py \
  --path python/geobrix/test/ds/test_raster_virtual.py \
  --path python/geobrix/test/ds/test_raster_v2_row.py \
  --path python/geobrix/test/ds/test_raster_options.py \
  --path python/geobrix/test/ds/test_raster_plan_select.py \
  --path python/geobrix/test/ds/test_raster_large.py \
  --path python/geobrix/test/ds/test_encode.py \
  --path python/geobrix/test/ds/test_window.py \
  --log ds-clip-regress.log
```
Expected: all pass. Fix any bbox-referencing straggler.

- [ ] **Step 3: Docker lint gate**

`bash scripts/commands/gbx-lint-python.sh --check` — confirm the files changed in Inc 2.5 are clean (ignore pre-existing unrelated failures if any; grep the output for the changed files). If dirty, reformat in-container (`docker exec geobrix-dev ... isort/black`) and amend.

- [ ] **Step 4: Serverless proof (fire directly)**

Rebuild+restage the wheel (`GBX_BUNDLE_SKIP_JAR_UPLOAD=1 .venv-pyrx/bin/python notebooks/tests/push_wheel_to_volume.py`), keep only current 0.4.4 wheel/JAR. Author `prompts/features/2026-08-01-reader-clip-select-serverless.py`: read the Volume corpus with `.option("clipPolygons", [box1_ewkb_4326, box2_wkb]).option("clipCrs","EPSG:4326")` (mixed CRS) in BOTH materialized and virtual modes; assert per-file tile counts, materialized pre-clip (nodata present), virtual raster-null + clip fields; a `windows=[...]` read; worker-side `open_tile` on the virtual rows. Self-report JSON via `dbutils.notebook.exit`. Build `.ipynb`, fire via the runner (`--wheel <gdal_artifacts path> --extras light --profile oauth-fe`). Verify `all_ok=true`, rows>0. Paste run URL + JSON into a RESULTS section.

- [ ] **Step 5: Commit any test migration**

```bash
git add python/geobrix/test/ds/  # deletions + migrations
git commit -m "test(ds): migrate bbox tests to clipPolygons; drop test_raster_bbox

Re-express the bbox AOI cases as box-geometry clipPolygons (a box is its
own envelope) and remove the bbox-specific tests. Full raster ds suite
green; Serverless clip/window selection verified on real /Volumes.

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- Unified CRS precedence (embedded SRID → clipCrs → raster) in `_clip` → Task 1. ✓
- Drop bbox/bboxCrs → Task 3 (parse) + Task 4 (planning) + Task 5 (legacy). ✓
- clipPolygons single/list → Task 3 normalize + Task 4 plan + Task 5 emit. ✓
- clipCrs + per-polygon precedence → Task 4 `_resolve_clip_crs` (records the resolved CRS on the tile for provenance/virtual-instruction). ✓
- windows single/list, clip-partial/skip-outside → Task 4. ✓
- Mutually exclusive → Task 3. ✓
- Materialized pre-clip (Choice 2) + skip all-nodata → Task 5. ✓
- Virtual clip = instructions + round-trip via open_tile → Task 5. ✓
- Reference-vs-instruction principle → realized by Task 5 (materialized applies, virtual defers). ✓
- window_for_geom envelope → Task 2. ✓
- Migrate test_raster_bbox → Task 6. ✓
- Serverless (mixed CRS, both tiers, worker-side) → Task 6. ✓
- Non-wired → no task touches registration files. ✓

**2. Placeholder scan:** No TBD/TODO. Task 5's metadata note now reuses the real `encode_tile` `meta` return (verified `_encode.metadata_for` does not exist). Test-geometry adjustment notes (Task 5 Step 4) are real guidance preserving a stated invariant.

**3. Type consistency:** `_plan_partitions_for_file(..., *, clip_polygons, clip_crs, windows, emit_virtual)` signature consistent between Task 4 def and `partitions()` call. `_TilePartition(clip_polygon=, clip_crs=)` fields exist from Inc 2, populated in Task 4, consumed in Task 5. `_v2_tile_row(..., clip_polygon=, clip_crs=)` matches Inc-2 signature. `window_for_geom(src, geom, geom_crs=None)` matches Task 2 def and Task 4 call. `_resolve_clip_crs(geom, reader_clip_crs) -> Optional[str]` consistent. `clip_dataset(ds, clip_polygon, clip_crs) -> Optional[bytes]` signature unchanged; Task 1 only changes its internal SRID rule. Note: with the unified rule, the reader could pass `clipCrs` straight to `_clip`, but it still calls `_resolve_clip_crs` to STAMP the resolved CRS onto the tile's `clip_crs` field (provenance for materialized, instruction for virtual) — the resolver's role is recording, not avoiding a contradiction.

## Deferred (tracked, not built here)

- overlapPercent + regular x,y tiling-size (Inc 3); functions-virtual-aware (Inc 4); heavy-tier v2.
- ds/writer.py virtual raster=None guard; COG "format" metadata reads "gtiff"; materialize_to_bytes clean-profile; dedup _epsg_of/_epsg_int; non-EPSG (WKT2/PROJ) CRS.
