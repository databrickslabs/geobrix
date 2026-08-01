# Reader `tileSize` grid + `overlapPercent` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a regular fixed-size tiling grid (`tileSize`) and an overlap modifier (`overlapPercent`) to the light `raster_gbx` reader, working in both materialized and virtual modes, reusing the existing `_overlap_steps` overlap math.

**Architecture:** `raster_gbx` (`ds/raster.py`) plans windows at the driver in `_plan_partitions_for_file`, then emits materialized (encoded bytes) or virtual (bytes-free path+window) v2 tiles. Inc 3 adds a pure `plan_grid_windows` enumerator in `pyrx/core/tiling.py` (windows, not bytes), a new `tileSize` planning branch, and a driver-side ~2GB cell guard applied to materialized tiles only. `open_tile` needs no change — a `tileSize` tile is a windowed tile with no clip.

**Tech Stack:** Python 3.12, PySpark Python DataSource V2, rasterio ≥1.3, pytest. Tests run in the `geobrix-dev` Docker container via `gbx:test:pyrx`. Serverless via `.venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py` (oauth-fe, env v5).

## Global Constraints

- **Light tier only:** rasterio only, NO osgeo.gdal; NO spark.conf.set/_jvm/.rdd in library code.
- **Exploratory / non-wired:** reader options only. NO registered_functions.txt / function-info.json / bindings changes.
- **DRY / reuse:** `pyrx.core.tiling._overlap_steps` (overlap math — `overlap_px = ceil(tile_dim·pct/100)`, `step = max(1, tile_dim − overlap_px)`), `_TilePartition` / `_v2_tile_row` (Inc 2), the existing guard `_estimate_tile_bytes` + `_MAX_TILE_BYTES` (=1932735283, ~1.8 GiB) in `ds/raster.py`, `emit_virtual` threading (Inc 2). Do NOT reinvent overlap stepping or the guard.
- **Overlap is `tileSize`-only.** Not wired into splitStrategy/plan_layout this increment. `overlapPercent>0` without `tileSize` → clear ValueError.
- **Selection exclusivity:** at most one of `clipPolygons` / `windows` / `tileSize` — else ValueError. None → whole-file/splitStrategy (unchanged).
- **Guard is materialized-only, at plan time.** In the `tileSize` planning branch, when `emit_virtual` is False, check the nominal decoded cell `tile_w * tile_h * bands * itemsize` against `_MAX_TILE_BYTES` and raise a clear error naming `tileSize`. When `emit_virtual` is True, DO NOT guard (bytes-free). Overlap changes stride, not tile size, so the nominal cell is `tile_w × tile_h`.
- **Emission is unchanged** from Inc 2/2.5: a `tileSize` tile is a windowed tile with `clip_polygon=None` — it flows the existing materialized-encode / virtual paths. No `read()` change needed beyond what's already there.
- `tileSize` over `.option()` is the string `"w,h"` (bare `"n"` → `(n,n)`). `overlapPercent` is an int string, default `"0"`, valid `0..99`.
- Test dir: `python/geobrix/test/ds/` and `python/geobrix/test/pyrx/`. Run: `bash scripts/commands/gbx-test-pyrx.sh --path <path> --log <name>.log` (Docker; dispatch as a Task; repeated `--path` supported). Doc tests: `gbx:test:python-docs --path <rel-to-docs/tests/python> --skip-build`.
- **Lint with CI-pinned tools:** `.venv-pyrx/bin/isort` + `.venv-pyrx/bin/black --line-length 88` (or in-container), verify Docker `gbx:lint:python --check` for changed files before declaring a task done. NEVER an ad-hoc black.
- Commits: body ends `Co-authored-by: Isaac`. `gh auth switch --user mjohns-databricks` before any push.
- Anchors: `tiling.py` — `_overlap_steps` L67-74, `_iter_window_tiles` L30-51 (the stepping loop to mirror), `math` already imported. `ds/raster.py` — `_MAX_TILE_BYTES` L84, `_estimate_tile_bytes` L153, `_numpy_itemsize` L144; `RasterGbxReader.__init__` option block L636-644; `_plan_partitions_for_file` windows branch L464-491 (mirror for tileSize); `partitions()` call L657-666; materialized encode in `read()` L791-813.

---

### Task 1: `plan_grid_windows` — pure window enumerator with overlap

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/tiling.py`
- Test: `python/geobrix/test/pyrx/test_core_tiling.py` (add cases)

**Interfaces:**
- Consumes: `_overlap_steps` (existing), stdlib.
- Produces: `plan_grid_windows(width, height, tile_w, tile_h, overlap=0) -> list[tuple[int,int,int,int]]` — regular grid of `(col_off, row_off, w, h)` windows over `width×height`, stepping by the `_overlap_steps` stride, each window clamped to the extent (edge tiles smaller). Pure: no dataset, no bytes. Same origins as `_iter_window_tiles`/`to_overlapping_tiles` for the same args.

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/pyrx/test_core_tiling.py
from databricks.labs.gbx.pyrx.core.tiling import plan_grid_windows


def test_plan_grid_windows_no_overlap_exact_grid():
    # 512x512 into 256x256, no overlap -> 4 cells at (0,0),(256,0),(0,256),(256,256)
    wins = plan_grid_windows(512, 512, 256, 256, 0)
    assert sorted(wins) == [
        (0, 0, 256, 256), (0, 256, 256, 256),
        (256, 0, 256, 256), (256, 256, 256, 256),
    ]


def test_plan_grid_windows_overlap_matches_step_contract():
    # 512x512, tile 256, overlap 25% -> overlap_px=64, step=192.
    # col/row offsets: 0,192,384 (384+256 clamps to 512 -> w=128). 3x3 = 9 windows.
    wins = plan_grid_windows(512, 512, 256, 256, 25)
    offs = sorted({(c, r) for c, r, _, _ in wins})
    assert offs == [(c, r) for r in (0, 192, 384) for c in (0, 192, 384)]
    assert len(wins) == 9
    # clamped edge window at col 384 has width 512-384=128
    edge = [w for c, r, w, h in wins if c == 384]
    assert all(w == 128 for w in edge)


def test_plan_grid_windows_tile_larger_than_raster_single_clamped():
    wins = plan_grid_windows(300, 200, 512, 512, 0)
    assert wins == [(0, 0, 300, 200)]


def test_plan_grid_windows_complete_coverage():
    # every source pixel is covered by at least one window
    import numpy as np
    W, H = 100, 80
    covered = np.zeros((H, W), dtype=bool)
    for c, r, w, h in plan_grid_windows(W, H, 32, 32, 10):
        covered[r:r + h, c:c + w] = True
    assert covered.all()
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_tiling.py --log grid.log`
Expected: FAIL — `ImportError: cannot import name 'plan_grid_windows'`.

- [ ] **Step 3: Implement**

Add to `pyrx/core/tiling.py` (near `_overlap_steps`):

```python
def plan_grid_windows(width, height, tile_width, tile_height, overlap=0):
    """Enumerate a regular grid of (col_off, row_off, w, h) windows over
    ``width x height``, stepping by the overlap-adjusted stride and clamping
    each window to the extent. Pure window planning — no dataset, no bytes.
    Overlap semantics match ``_iter_window_tiles`` / ``rst_tooverlappingtiles``.
    """
    tw, th, step_x, step_y = _overlap_steps(tile_width, tile_height, overlap)
    windows = []
    row = 0
    while row < height:
        col = 0
        while col < width:
            w = min(tw, width - col)
            h = min(th, height - row)
            if w > 0 and h > 0:
                windows.append((col, row, w, h))
            col += step_x
        row += step_y
    return windows
```

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_tiling.py --log grid.log`
Expected: PASS (4 new + existing tiling tests).

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/pyrx/core/tiling.py python/geobrix/test/pyrx/test_core_tiling.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/pyrx/core/tiling.py python/geobrix/test/pyrx/test_core_tiling.py
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/tiling.py python/geobrix/test/pyrx/test_core_tiling.py
git commit -m "feat(pyrx): plan_grid_windows pure enumerator with overlap

Regular fixed-size grid of (col,row,w,h) windows over a raster, stepping
by the existing _overlap_steps stride and clamping to extent. Pure (no
dataset/bytes) so the reader can plan windows at the driver for both
materialized and virtual tiles. Overlap semantics match rst_tooverlappingtiles.

Co-authored-by: Isaac"
```

---

### Task 2: option parsing — `tileSize` + `overlapPercent`, mutual-exclusion

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`RasterGbxReader.__init__`, add `_as_tile_size`)
- Test: `python/geobrix/test/ds/test_raster_options.py`

**Interfaces:**
- Produces: `RasterGbxReader` parses `self.tile_size: Optional[Tuple[int,int]]` (from `"w,h"` / bare `"n"`) and `self.overlap_percent: int`. Raises `ValueError` if more than one of `clip_polygons`/`windows`/`tile_size` is set, or if `overlap_percent>0` without `tile_size`, or on malformed `tileSize` / out-of-range `overlapPercent`. `_as_tile_size(val) -> Optional[Tuple[int,int]]` module helper.

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/ds/test_raster_options.py
def test_tile_size_wh_string():
    r = RasterGbxReader({"path": "/x", "tileSize": "512,256"})
    assert r.tile_size == (512, 256)
    assert r.overlap_percent == 0


def test_tile_size_square_shorthand():
    r = RasterGbxReader({"path": "/x", "tileSize": "512"})
    assert r.tile_size == (512, 512)


def test_overlap_percent_parsed():
    r = RasterGbxReader({"path": "/x", "tileSize": "256", "overlapPercent": "25"})
    assert r.overlap_percent == 25


def test_tile_size_mutually_exclusive_with_windows():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "tileSize": "256", "windows": "[0,0,8,8]"})


def test_tile_size_mutually_exclusive_with_clip_polygons():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "tileSize": "256", "clipPolygons": _WKT1})


def test_overlap_without_tile_size_raises():
    with pytest.raises(ValueError, match="overlapPercent"):
        RasterGbxReader({"path": "/x", "overlapPercent": "25"})


def test_bad_tile_size_raises():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "tileSize": "abc"})


def test_overlap_out_of_range_raises():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "tileSize": "256", "overlapPercent": "150"})


def test_no_tile_size_default():
    r = RasterGbxReader({"path": "/x"})
    assert r.tile_size is None and r.overlap_percent == 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_options.py --log topt.log`
Expected: FAIL — no `tile_size` attribute.

- [ ] **Step 3: Implement**

Add the module helper near `_as_window_list`:

```python
def _as_tile_size(val):
    """Parse a tileSize option ("w,h" or bare "n" -> (n,n)) to (w,h) or None."""
    if val is None or val == "":
        return None
    if isinstance(val, (tuple, list)) and len(val) == 2:
        w, h = int(val[0]), int(val[1])
    else:
        parts = [p for p in str(val).split(",") if p.strip() != ""]
        if len(parts) == 1:
            w = h = int(parts[0])
        elif len(parts) == 2:
            w, h = int(parts[0]), int(parts[1])
        else:
            raise ValueError(
                f"raster_gbx: 'tileSize' must be 'w,h' or a single int; got {val!r}"
            )
    if w <= 0 or h <= 0:
        raise ValueError(f"raster_gbx: 'tileSize' must be positive; got {val!r}")
    return (w, h)
```

(The `int(...)` calls raise `ValueError` on non-numeric like `"abc"` — that satisfies the bad-tileSize test.)

In `RasterGbxReader.__init__`, after the clip/windows block and its mutual-exclusion check, add:

```python
        self.tile_size = _as_tile_size(options.get("tileSize"))
        self.overlap_percent = int(options.get("overlapPercent", "0"))
        if not (0 <= self.overlap_percent < 100):
            raise ValueError(
                f"raster_gbx: 'overlapPercent' must be 0..99; got {self.overlap_percent}"
            )
        _selectors = [bool(self.clip_polygons), bool(self.windows), bool(self.tile_size)]
        if sum(_selectors) > 1:
            raise ValueError(
                "raster_gbx: 'clipPolygons', 'windows', and 'tileSize' are mutually "
                "exclusive; supply at most one."
            )
        if self.overlap_percent > 0 and not self.tile_size:
            raise ValueError(
                "raster_gbx: 'overlapPercent' requires 'tileSize' (it modifies the "
                "regular tiling grid only)."
            )
```

Replace the old two-way `if self.clip_polygons and self.windows:` check with the `sum(_selectors) > 1` form (it subsumes it).

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_options.py --log topt.log`
Expected: PASS (new + existing option tests). Other reader tests may error until Task 3 wires planning — expected within this plan.

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_options.py
git commit -m "feat(ds): parse tileSize/overlapPercent, extend mutual-exclusion

tileSize ('w,h' or bare 'n'->square) + overlapPercent (0..99, tileSize-only)
options. Selection exclusivity now covers clipPolygons/windows/tileSize
(at most one); overlapPercent without tileSize raises. Planning branch
wired next.

Co-authored-by: Isaac"
```

---

### Task 3: planning branch — `tileSize` → grid windows + materialized guard

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`_plan_partitions_for_file`, `partitions()`)
- Test: `python/geobrix/test/ds/test_raster_plan_select.py`

**Interfaces:**
- Consumes: `plan_grid_windows` (Task 1), `self.tile_size`/`self.overlap_percent` (Task 2), `_estimate_tile_bytes`/`_MAX_TILE_BYTES`, `_numpy_itemsize`.
- Produces: `_plan_partitions_for_file(..., *, ..., tile_size=None, overlap_percent=0, ...)` — new branch after the `windows` branch: if `tile_size` set → open header for `(W,H,bands,dtype)`, when `not emit_virtual` guard the nominal cell (`tw*th*bands*itemsize > _MAX_TILE_BYTES` → clear ValueError naming tileSize), then `plan_grid_windows(W,H,tw,th,overlap)` → one `_TilePartition(window=w, clip_polygon=None)` per window (threading `emit_virtual`). `partitions()` passes `tile_size`/`overlap_percent`.

- [ ] **Step 1: Write the failing test**

```python
# add to python/geobrix/test/ds/test_raster_plan_select.py
def test_tile_size_plans_grid(tmp_path):
    p = _write(tmp_path, width=512, height=512)
    parts = _plan_partitions_for_file(p, 0, tile_size=(256, 256), overlap_percent=0)
    assert len(parts) == 4
    assert (0, 0, 256, 256) in [pt.window for pt in parts]
    assert all(pt.clip_polygon is None for pt in parts)


def test_tile_size_overlap_changes_count(tmp_path):
    p = _write(tmp_path, width=512, height=512)
    parts = _plan_partitions_for_file(p, 0, tile_size=(256, 256), overlap_percent=25)
    assert len(parts) == 9  # 3x3 with step 192


def test_tile_size_larger_than_raster_single(tmp_path):
    p = _write(tmp_path, width=300, height=200)
    parts = _plan_partitions_for_file(p, 0, tile_size=(512, 512), overlap_percent=0)
    assert len(parts) == 1
    assert parts[0].window == (0, 0, 300, 200)


def test_tile_size_materialized_guard_raises(tmp_path):
    # a 40000x40000 float32 cell would be ~6.4 GB > ~1.8 GiB guard
    p = _write(tmp_path, width=50000, height=50000)  # header only; not materialized
    with pytest.raises(ValueError, match="tileSize"):
        _plan_partitions_for_file(p, 0, tile_size=(40000, 40000), overlap_percent=0,
                                  emit_virtual=False)


def test_tile_size_virtual_no_guard(tmp_path):
    # same oversized tile is fine in virtual mode (bytes-free)
    p = _write(tmp_path, width=50000, height=50000)
    parts = _plan_partitions_for_file(p, 0, tile_size=(40000, 40000), overlap_percent=0,
                                      emit_virtual=True)
    assert len(parts) >= 1  # no raise
```

Note: `_write` here must create a header of the requested size cheaply. If the existing `_write` in this file materializes full pixels (slow/large for 50000²), add a `_write_header(tmp_path, width, height)` helper that writes a sparse/tiled GTiff header without a giant array (or use a small `blockxsize` and write nodata) — the guard reads only `ds.width/height/count/dtypes`, so the file just needs those dimensions. The implementer picks the cheapest way to get a 50000²-dimensioned readable header.

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_plan_select.py --log tplan.log`
Expected: FAIL — signature has no `tile_size`.

- [ ] **Step 3: Implement**

Add `tile_size`/`overlap_percent` to `_plan_partitions_for_file`'s keyword-only params. After the `windows` branch (before the normal/split path), add:

```python
    if tile_size:
        from databricks.labs.gbx.pyrx.core.tiling import plan_grid_windows

        tw, th = int(tile_size[0]), int(tile_size[1])
        with rasterio.open(file_path) as ds:
            W, H = ds.width, ds.height
            bands = ds.count
            itemsize = _numpy_itemsize(ds.dtypes[0])
        if not emit_virtual:
            # materialized-only guard: the nominal cell must fit the ~2GB Spark
            # cell limit (overlap changes stride, not tile size). Virtual tiles
            # carry no bytes -> no guard here (fires later at materialize time).
            cell = tw * th * max(bands, 1) * itemsize
            if cell > _MAX_TILE_BYTES:
                raise ValueError(
                    f"raster_gbx: tileSize {tw}x{th} materialized cell is "
                    f"~{cell // (1024 * 1024)} MB, exceeding the ~2 GB Spark cell "
                    f"limit; use a smaller tileSize or virtualTiles=true."
                )
        return [
            _TilePartition(
                file_path=file_path,
                window=(c, r, w, h),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=emit_virtual,
            )
            for c, r, w, h in plan_grid_windows(W, H, tw, th, overlap_percent)
        ]
```

Update `partitions()` to pass `tile_size=self.tile_size, overlap_percent=self.overlap_percent`.

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_plan_select.py --log tplan.log`
Expected: PASS (5 new).

- [ ] **Step 5: Lint + commit**

```bash
.venv-pyrx/bin/isort python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
.venv-pyrx/bin/black --line-length 88 python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_plan_select.py
git commit -m "feat(ds): tileSize planning branch + materialized-only cell guard

tileSize -> plan_grid_windows grid partitions (threading emit_virtual);
a driver-side ~2GB cell guard applies only when materializing (virtual
tiles are bytes-free -> no guard). partitions() passes tile_size/overlap.

Co-authored-by: Isaac"
```

---

### Task 4: emission round-trip tests (materialized + virtual)

**Files:**
- Test: `python/geobrix/test/ds/test_raster_tilesize.py` (new)

**Interfaces:** consumes the shipped reader + `open_tile`/`VirtualTile`. No new source (emission is the existing windowed path; a tileSize tile is a windowed tile with no clip).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_tilesize.py
"""tileSize grid emission: materialized per-cell bytes + virtual round-trip."""
import numpy as np
import rasterio
from rasterio.transform import from_origin

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


def test_materialized_tilesize_grid(spark, tmp_path):
    p = _write(tmp_path, 512, 512)
    rows = (spark.read.format("raster_gbx").option("tileSize", "256,256")
            .load(p)).collect()
    assert len(rows) == 4
    for r in rows:
        assert r["tile"]["raster"] is not None
        assert r["tile"]["clip_polygon"] is None
        assert r["tile"]["window"]["width"] == 256


def test_materialized_tilesize_pixels_match_source(spark, tmp_path):
    p = _write(tmp_path, 512, 512)
    rows = (spark.read.format("raster_gbx").option("tileSize", "256,256")
            .load(p)).collect()
    with rasterio.open(p) as ds:
        full = ds.read(1)
    from rasterio.io import MemoryFile
    for r in rows:
        w = r["tile"]["window"]
        with MemoryFile(bytes(r["tile"]["raster"])) as mf, mf.open() as t:
            got = t.read(1)
        exp = full[w["row_off"]:w["row_off"] + w["height"],
                   w["col_off"]:w["col_off"] + w["width"]]
        assert np.array_equal(got, exp)


def test_virtual_tilesize_round_trips(spark, tmp_path):
    p = _write(tmp_path, 512, 512)
    rows = (spark.read.format("raster_gbx").option("virtualTiles", "true")
            .option("tileSize", "256,256").load(p)).collect()
    assert len(rows) == 4
    with rasterio.open(p) as ds:
        full = ds.read(1)
    for r in rows:
        assert r["tile"]["raster"] is None
        tile = VirtualTile.from_row(r["tile"])
        with ot.open_tile(tile) as t:
            got = t.read(1)
        w = r["tile"]["window"]
        exp = full[w["row_off"]:w["row_off"] + w["height"],
                   w["col_off"]:w["col_off"] + w["width"]]
        assert np.array_equal(got, exp)


def test_overlap_grid_count(spark, tmp_path):
    p = _write(tmp_path, 512, 512)
    rows = (spark.read.format("raster_gbx").option("tileSize", "256,256")
            .option("overlapPercent", "25").load(p)).collect()
    assert len(rows) == 9
```

- [ ] **Step 2: Run to verify it fails, then pass**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_tilesize.py --log ttile.log`
Expected: initially may fail only if a wiring gap remains; if all pass immediately that confirms Tasks 1-3 composed correctly (this task is the integration proof). If a materialized tile's window read differs from the source slice, investigate the encode path (do NOT weaken the array_equal invariant).

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/ds/test_raster_tilesize.py
git commit -m "test(ds): tileSize grid emission — materialized + virtual round-trip

Materialized tileSize tiles carry per-cell bytes matching the source
slice; virtual tileSize tiles are bytes-free and round-trip through
open_tile to the same pixels; overlapPercent changes the grid count.

Co-authored-by: Isaac"
```

---

### Task 5: docs + full regression + Serverless

**Files:**
- Modify: `docs/docs/readers/raster.mdx` (+ geotiff/cog option tables)
- Serverless notebook: `prompts/features/2026-08-01-reader-tilesize-serverless.py` (gitignored scratch)

- [ ] **Step 1: Docs**

Add `tileSize` and `overlapPercent` rows to the options table in `docs/docs/readers/raster.mdx`, and to the geotiff/cog option tables (brief rows referencing raster options). In raster.mdx include a short "why overlap" note: a feature straddling a tile seam appears whole in at least one tile. State: `tileSize` is mutually exclusive with `clipPolygons`/`windows`; `overlapPercent` modifies `tileSize` only; materialized cells are guarded to ~2 GB, virtual tiles are not. If a doc example snippet is added, it must live in the doc-test source (`docs/tests/python/readers/...`) per the docs-are-tests rule and be exercised — but a table-row + prose addition needs no new executable example.

- [ ] **Step 2: Scoped regression (raster ds, exclude netcdf)**

Run:
```
bash scripts/commands/gbx-test-pyrx.sh \
  --path python/geobrix/test/ds/test_raster_datasource.py \
  --path python/geobrix/test/ds/test_raster_clip.py \
  --path python/geobrix/test/ds/test_raster_virtual.py \
  --path python/geobrix/test/ds/test_raster_v2_row.py \
  --path python/geobrix/test/ds/test_raster_options.py \
  --path python/geobrix/test/ds/test_raster_plan_select.py \
  --path python/geobrix/test/ds/test_raster_tilesize.py \
  --path python/geobrix/test/ds/test_raster_large.py \
  --path python/geobrix/test/ds/test_encode.py \
  --path python/geobrix/test/ds/test_window.py \
  --log ds-tilesize-regress.log
```
Then `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_core_tiling.py --log tiling-regress.log`. All pass. (netcdf collection errors are a PRE-EXISTING missing-dep, not this branch.)

- [ ] **Step 3: Docker lint gate**

`bash scripts/commands/gbx-lint-python.sh --check` — confirm the changed files are clean (grep the output for them; if dirty, reformat in-container and amend).

- [ ] **Step 4: Serverless proof (fire directly)**

Rebuild+restage the wheel (`GBX_BUNDLE_SKIP_JAR_UPLOAD=1 .venv-pyrx/bin/python notebooks/tests/push_wheel_to_volume.py`; keep only current 0.4.4 wheel/JAR). Author `prompts/features/2026-08-01-reader-tilesize-serverless.py`: read the Volume corpus with `.option("tileSize","256,256")` and `.option("overlapPercent","25")` in BOTH materialized and virtual modes; assert grid cell count (no overlap vs 25%), materialized per-cell bytes present, virtual raster-null + worker `open_tile` round-trip. Self-report JSON via `dbutils.notebook.exit`. Build `.ipynb` (drop %pip/docstring cells), fire via the runner (`--wheel <gdal_artifacts path> --extras light --profile oauth-fe`). Verify `all_ok=true`, rows>0, paste run URL + JSON into a RESULTS section.

- [ ] **Step 5: Commit docs**

```bash
git add docs/docs/readers/raster.mdx docs/docs/readers/geotiff.mdx docs/docs/readers/cog.mdx
git commit -m "docs: document tileSize + overlapPercent reader options

Adds the regular-grid tiling options with the feature-straddling-a-seam
overlap rationale and the materialized-only cell-guard note. Serverless
tileSize/overlap proof green on real /Volumes.

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- `plan_grid_windows` pure enumerator w/ overlap → Task 1. ✓
- `tileSize` `"w,h"`/bare-`n` parse + `overlapPercent` + mutual-exclusion + overlap-requires-tileSize → Task 2. ✓
- tileSize planning branch (both tiers) → Task 3. ✓
- Materialized-only ~2GB guard at plan time; virtual unguarded → Task 3 (+ tests). ✓
- Overlap = tileSize-only (not splitStrategy) → enforced in Task 2 (error) + only tileSize branch uses overlap. ✓
- Materialized per-cell bytes == source slice; virtual round-trip via open_tile → Task 4. ✓
- overlap changes grid count → Task 1 + Task 4. ✓
- tile>raster → single clamped window → Task 1 + Task 3. ✓
- Docs (raster/geotiff/cog + why-overlap) → Task 5. ✓
- Serverless both tiers → Task 5. ✓
- Non-wired (no registration) → no task touches registration files. ✓

**2. Placeholder scan:** No TBD/TODO. Task 3's `_write_header` note is a concrete instruction (cheap large-dimension header) with a defined purpose. Task 4 Step 2's "if all pass immediately that's the integration proof" is real guidance, not a stub.

**3. Type consistency:** `plan_grid_windows(width, height, tile_w, tile_h, overlap=0) -> list[(c,r,w,h)]` consistent Task 1↔3. `_as_tile_size(val) -> Optional[(w,h)]` consistent Task 2↔3. `_plan_partitions_for_file(..., tile_size=None, overlap_percent=0)` consistent Task 3 def↔`partitions()` call. `_TilePartition(window=, clip_polygon=None, emit_virtual=)` matches Inc-2 fields. Guard uses real `_MAX_TILE_BYTES`/`_numpy_itemsize`. `_overlap_steps` reused, not reimplemented.

## Deferred (tracked, not built here)

- `overlapPercent` in the budget-driven splitStrategy/plan_layout path (`reader-overlap-percent-option` memory).
- Heavy tier.
- Carried: writer virtual raster=None guard; COG `format` metadata "gtiff"; materialize_to_bytes clean-profile; dedup `_epsg_of`/`_epsg_int`; non-EPSG CRS.
