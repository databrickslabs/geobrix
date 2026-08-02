# Functions Virtual-Aware + Force-Output + Writer Round-Trip (Increment 4) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every light-tier `rst_*` function consume virtual OR materialized tiles through one shared front-door; let header-only accessors answer without materializing; add `virtualize_dir`/`virtualize_prefix`/`materialize` force-output params to tile-returning functions; make the writers dual-accept v1/v2 input and auto-materialize virtual tiles (fixing the pre-existing v2 read→write regression).

**Architecture:** Three shared helpers in `pyrx/core/open_tile.py` + `accessors.py` (`_open`, `open_header`, `shape_output`) reused by ~55 UDF wrappers in `functions.py` and the writers. Two phases: **A** builds + proves the helpers on one function per family (incl. Serverless); **B** applies the mechanical `_serde.open_tile → _open` swap across the full catalog. Operation logic is never touched — only how the dataset is opened and how the result is shaped.

**Tech Stack:** Python 3.12, rasterio ≥1.3, PySpark UDFs/DataSource V2, pytest. Tests in the `geobrix-dev` Docker container via `gbx:test:pyrx`. Serverless via `.venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py` (oauth-fe, env v5).

## Global Constraints

- **Light tier only:** rasterio only, NO `osgeo.gdal`; NO `spark.conf.set`/`_jvm`/`.rdd` in library code.
- **Exploratory / non-wired:** NO new registered functions; params are OPTIONAL args on existing functions. NO `registered_functions.txt` / `function-info.json` / bindings **additions** (existing entries unchanged). Binding-parity stays green.
- **DRY / reuse (do NOT reinvent):** Inc-1 `open_tile(VirtualTile)`, `materialize_to_bytes(VirtualTile)`, `materialize(VirtualTile)` in `pyrx/core/open_tile.py`; `VirtualTile.from_row`/`from_v1`; `preparer._stage_local_if_needed`; the writers' existing `_write.tile_to_bytes` / `analysis.cog_convert_file`. Operation cores (terrain/focal/mapalgebra/…) are untouched.
- **Operation bodies unchanged:** a function's math never changes — only `with _serde.open_tile(bytes(tile["raster"])) as ds:` → `with _open(tile) as ds:`.
- **Force-output semantics:** `virtualize_dir=<durable path>` (+ optional `virtualize_prefix`) → write bytes to `dir/[<prefix>_]<cellid>_<col>_<row>_<w>_<h>.tif` (overwrite), return light virtual tile; no-op if already virtual. `materialize=True|False` → ensure bytes; no-op if already materialized. neither → auto (deferrable ops stay virtual if input virtual; pixel ops materialize). `virtualize_dir`+`materialize=True` → `ValueError`. `virtualize_dir` must be durable (Volume); FUSE-safe local-temp→copy write.
- **`virtualize_dir` virtualizes the RESULT not the computation:** a pixel op still computes; virtualizing writes the computed bytes to the path and returns the light row.
- **Header-only accessors** (answer from `path` header, no `.read()`): width, height, numbands, srid, scalex/y, upperleftx/y, rotation, skewx/y, boundingbox, type, getnodata, format, metadata, georeference, bandmetadata, subdatasets. **Pixel accessors** (materialize window): avg, minimum, maximum, median, pixelcount, summary, histogram, sample, isempty, getsubdataset.
- **Writers emit FORMAT FILES (GTiff/COG), never "a tile."** Change is INPUT acceptance only: dual-accept v1 or v2 envelope; internally normalize each tile → `VirtualTile` to obtain bytes; virtual → auto-materialize; `cog_gbx` whole-file virtual (window==full extent AND clip_polygon is None) → `cog_convert_file(tile.path)` path-direct (no bytes); windowed/clipped virtual or materialized → materialize then convert.
- **Materialize is the light→heavy bridge:** heavy has none of these params; document on every light rst_* + Light-vs-Heavy page.
- Tests: `python/geobrix/test/pyrx/` (function/accessor tests) and `python/geobrix/test/ds/` (writer tests). Run `bash scripts/commands/gbx-test-pyrx.sh --path <p> --log <n>.log` (Docker; dispatch as Task; repeated --path ok, path is repo-relative).
- **Lint with CI-pinned tools:** `.venv-pyrx/bin/isort` + `.venv-pyrx/bin/black --line-length 88` (or in-container); verify Docker `gbx:lint:python --check` on changed files before task done. When REMOVING/adding an option, grep tests by BOTH `.option(` and attribute/`"name"` forms; run the full non-netcdf ds/pyrx suite before push (netcdf collection-aborts are a pre-existing env gap — work around with an explicit file list, don't shrink coverage).
- Commits: body ends `Co-authored-by: Isaac`. Push is the USER's call — never auto-push.
- Anchors: `open_tile.py` — `open_tile` L109, `materialize_to_bytes` L153, `materialize` L179. `accessors.py` — accessors take an OPEN `ds` (L28+); header/pixel split is in the WRAPPER, not the accessor body. `functions.py` — 55 `_serde.open_tile(...)` call sites; individual `_*_udf` closures (e.g. `_georeference_udf` L208, `_summary_udf` L241); `build_tile` used for returns; `_tile_raster_bytes` L3342. `writer.py` — `assert_write_schema` L29 (calls `reader_schema()` = v1), `write` L93, `bytes(tile["raster"])` L99. `cog_writer.py` — `assert_path_schema`, driverMode gather, `cog_convert_file`. `raster.py` — `reader_schema()` v1, `reader_schema_v2()` v2.

---

## PHASE A — front-door + helpers + writers, proven per family

### Task 1: `_open(tile)` consume adapter + `_open_all(tiles)`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py`
- Test: `python/geobrix/test/pyrx/test_core_open_adapter.py`

**Interfaces:**
- Consumes: `open_tile`, `VirtualTile` (from_row/from_v1).
- Produces:
  - `@contextmanager _open(tile) -> DatasetReader` — accepts a tile as: raw bytes; a v1 dict/Row `{cellid, raster, metadata}`; a v2 dict/Row (8-field); or a `VirtualTile`. Normalizes to a `VirtualTile` and delegates to `open_tile`. (bytes → `VirtualTile(cellid=0, raster=bytes)`; dict/Row with `raster` and no `path` → `from_v1`; dict/Row with `path` → `from_row`; `VirtualTile` → passthrough.)
  - `_open_all(tiles) -> contextmanager yielding list[DatasetReader]` — maps `_open` over a list under one ExitStack (for agg/mapalgebra).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_open_adapter.py
"""_open adapter: v1 struct / v2 materialized / v2 virtual / raw bytes / VirtualTile."""
import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from .conftest import make_geotiff_bytes  # existing fixture (v1 bytes)


def test_open_raw_bytes():
    with ot._open(make_geotiff_bytes(width=4, height=3)) as ds:
        assert ds.width == 4 and ds.height == 3


def test_open_v1_struct():
    tile = {"cellid": 0, "raster": make_geotiff_bytes(width=5, height=2), "metadata": {}}
    with ot._open(tile) as ds:
        assert ds.width == 5 and ds.height == 2


def test_open_v2_materialized_struct():
    tile = {"cellid": 0, "raster": make_geotiff_bytes(width=6, height=6),
            "path": None, "window": None, "clip_polygon": None,
            "clip_crs": None, "crs": None, "metadata": {}}
    with ot._open(tile) as ds:
        assert ds.width == 6

def test_open_v2_virtual_struct(tmp_path):
    p = str(tmp_path / "r.tif")
    prof = dict(driver="GTiff", width=512, height=512, count=1, dtype="float32",
                crs="EPSG:4326",
                transform=rasterio.transform.from_origin(10, 50, 0.001, 0.001),
                nodata=-9999.0)
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(512 * 512, dtype="float32").reshape(512, 512), 1)
    tile = {"cellid": 0, "raster": None, "path": p,
            "window": {"col_off": 0, "row_off": 0, "width": 256, "height": 256},
            "clip_polygon": None, "clip_crs": None, "crs": None, "metadata": {}}
    with ot._open(tile) as ds:
        assert (ds.width, ds.height) == (256, 256)


def test_open_virtualtile_passthrough():
    vt = VirtualTile(cellid=0, raster=make_geotiff_bytes(width=3, height=3))
    with ot._open(vt) as ds:
        assert ds.width == 3


def test_open_all_list(tmp_path):
    tiles = [{"cellid": 0, "raster": make_geotiff_bytes(width=w, height=2), "metadata": {}}
             for w in (2, 3)]
    with ot._open_all(tiles) as dss:
        assert [d.width for d in dss] == [2, 3]
```

- [ ] **Step 2: Run to verify it fails** — `--path python/geobrix/test/pyrx/test_core_open_adapter.py`, expect ImportError `_open`.

- [ ] **Step 3: Implement**

```python
# in open_tile.py
from contextlib import ExitStack

def _to_virtual_tile(tile) -> VirtualTile:
    if isinstance(tile, VirtualTile):
        return tile
    if isinstance(tile, (bytes, bytearray)):
        return VirtualTile(cellid=0, raster=bytes(tile))
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    if "path" in d or "window" in d:  # v2 shape
        return VirtualTile.from_row(d)
    return VirtualTile.from_v1(d.get("cellid", 0), d["raster"], d.get("metadata"))


@contextmanager
def _open(tile):
    with open_tile(_to_virtual_tile(tile)) as ds:
        yield ds


@contextmanager
def _open_all(tiles):
    with ExitStack() as stack:
        yield [stack.enter_context(open_tile(_to_virtual_tile(t))) for t in tiles]
```

- [ ] **Step 4: Run to verify pass** (6 tests). — [ ] **Step 5: Lint + commit** (`feat(pyrx): _open/_open_all tile adapter (v1/v2/virtual/bytes → dataset)`).

---

### Task 2: `open_header(tile)` + header-only accessor path

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` (add `open_header`)
- Test: `python/geobrix/test/pyrx/test_core_open_header.py`

**Interfaces:**
- Produces: `@contextmanager open_header(tile) -> DatasetReader` — bytes/materialized → open bytes (as `_open`); virtual (raster None) → `rasterio.open(_stage_local_if_needed(path))` yielding the dataset **without reading pixels** (header/profile/transform available; no `.read()`). Callers that only need metadata use this; it never materializes a window.

- [ ] **Step 1: Write the failing test** — a virtual tile over a large raster: `open_header` yields a ds whose `.width/.height/.crs/.transform` are correct, and assert **no pixel read happened** (patch `rasterio.DatasetReader.read` with a spy / or assert the staged file is opened but the window is never sliced — simplest: spy on the ds.read via `unittest.mock.patch.object` and assert not called after computing width/srid/bounds).

```python
# python/geobrix/test/pyrx/test_core_open_header.py
import numpy as np, rasterio
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import accessors


def _big_virtual(tmp_path):
    p = str(tmp_path / "big.tif")
    prof = dict(driver="GTiff", width=2048, height=2048, count=1, dtype="float32",
                crs="EPSG:4326", transform=from_origin(10, 50, 0.001, 0.001), nodata=-9999.0)
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.zeros((2048, 2048), "float32"), 1)
    return {"cellid": 0, "raster": None, "path": p,
            "window": {"col_off": 0, "row_off": 0, "width": 2048, "height": 2048},
            "clip_polygon": None, "clip_crs": None, "crs": None, "metadata": {}}


def test_open_header_metadata_without_read(tmp_path):
    tile = _big_virtual(tmp_path)
    with ot.open_header(tile) as ds:
        # spy: no pixel read for header accessors
        orig = ds.read
        calls = []
        ds.read = lambda *a, **k: (calls.append(1), orig(*a, **k))[1]
        w = accessors.width(ds)
        s = accessors.srid(ds)
        bb = accessors.boundingbox(ds)
        assert w == 2048 and s == 4326 and bb is not None
        assert calls == []  # header-only: no pixels read
```

- [ ] **Step 2-4:** fail → implement `open_header` (mirror `_open` but for virtual path use a plain `rasterio.open(staged)` held on an ExitStack, cleanup temp on exit; do NOT call the window-read helper) → pass.
- [ ] **Step 5: Lint + commit** (`feat(pyrx): open_header — metadata from a virtual tile without materializing`).

---

### Task 3: `shape_output(...)` force-output helper

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py`
- Test: `python/geobrix/test/pyrx/test_core_shape_output.py`

**Interfaces:**
- Produces: `shape_output(tile, *, virtualize_dir=None, virtualize_prefix=None, materialize=None) -> VirtualTile` — applied to a produced tile (a `VirtualTile`, or a v1/v2 struct normalized via `_to_virtual_tile`). Rules per Global Constraints. `virtualize_dir` write uses provenance filename `[<prefix>_]<cellid>_<col>_<row>_<w>_<h>.tif` (window = full extent if the tile's window is None, i.e. `(0,0,W,H)` from the bytes), FUSE-safe (write local temp → `shutil.copyfile` to dir), overwrite.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyrx/test_core_shape_output.py
import os, numpy as np, rasterio
import pytest
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from .conftest import make_geotiff_bytes


def test_materialize_true_forces_bytes(tmp_path):
    p = str(tmp_path / "r.tif")  # make a virtual tile
    prof = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                crs="EPSG:4326", transform=rasterio.transform.from_origin(10,50,0.001,0.001), nodata=-9999.0)
    with rasterio.open(p,"w",**prof) as ds: ds.write(np.arange(64,dtype="float32").reshape(8,8),1)
    vt = VirtualTile(cellid=7, path=p, window=(0,0,8,8))
    out = ot.shape_output(vt, materialize=True)
    assert out.raster is not None and not out.is_virtual()


def test_materialize_noop_on_materialized():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    out = ot.shape_output(vt, materialize=True)
    assert out.raster == vt.raster


def test_virtualize_dir_writes_and_returns_virtual(tmp_path):
    vt = VirtualTile(cellid=3, raster=make_geotiff_bytes(width=4, height=4))
    out = ot.shape_output(vt, virtualize_dir=str(tmp_path))
    assert out.raster is None and out.path is not None
    assert os.path.exists(out.path)
    # round-trips
    with ot.open_tile(out) as ds:
        assert ds.width == 4


def test_virtualize_prefix_in_name(tmp_path):
    vt = VirtualTile(cellid=9, raster=make_geotiff_bytes())
    out = ot.shape_output(vt, virtualize_dir=str(tmp_path), virtualize_prefix="run1")
    assert os.path.basename(out.path).startswith("run1_")


def test_conflict_raises():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    with pytest.raises(ValueError):
        ot.shape_output(vt, virtualize_dir="/x", materialize=True)


def test_auto_noop():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    assert ot.shape_output(vt) is vt
```

- [ ] **Step 2-4:** fail → implement → pass (6 tests). Window for the filename: if `tile.window` set use it, else read `(W,H)` from the bytes and use `(0,0,W,H)`.
- [ ] **Step 5: Lint + commit** (`feat(pyrx): shape_output — virtualize_dir/prefix/materialize force-output`).

---

### Task 4: writers dual-accept v1/v2 + auto-materialize virtual (gtiff/raster)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/writer.py`
- Test: `python/geobrix/test/ds/test_writer_virtual.py` + regression in `test_writer.py`

**Interfaces:**
- `assert_write_schema` accepts v1 (`reader_schema()`) OR v2 (`reader_schema_v2()`) envelope. `write()` normalizes each `row["tile"]` → `VirtualTile` (`_to_virtual_tile`); if virtual → `materialize_to_bytes` → `raster_bytes = out.raster`; else `bytes(tile["raster"])`; then existing `tile_to_bytes` path unchanged.

- [ ] **Step 1: failing tests** — (a) v1 round-trip still writes (regression); (b) a v2 materialized DataFrame writes; (c) a **virtual** tile DataFrame (raster=None, path+window) writes files whose bytes equal the source window; (d) `assert_write_schema` accepts both v1 and v2, rejects a 3-field-wrong schema. Use `spark.read.format("raster_gbx").option("virtualTiles","true")` to produce virtual rows, then `.write.format("gtiff_gbx")`.
- [ ] **Step 2-4:** fail (v2 rejected / bytes(None) crash) → implement dual-accept + normalize+materialize → pass.
- [ ] **Step 5: commit** (`fix(ds): writers dual-accept v1/v2, auto-materialize virtual tiles`).

---

### Task 5: cog_gbx writer — path-direct convert for whole-file virtual tiles

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/cog_writer.py`
- Test: `python/geobrix/test/ds/test_cog_writer_virtual.py`

**Interfaces:**
- When the input is a v2 `tile` struct: extract `tile.path`. **Whole-file virtual** (`raster is None` AND window == full extent AND `clip_polygon is None`) → `cog_convert_file(local(path), out)` directly (no bytes). **Windowed/clipped virtual** → `materialize_to_bytes` then convert the bytes. **Materialized** → convert bytes (as today). Keep the existing top-level-`path` (file_gbx) input working (detect which schema).

- [ ] **Step 1: failing tests** — (a) whole-file virtual tile → COG written, is_cog, path-direct taken (spy that `materialize_to_bytes` NOT called); (b) windowed virtual tile → COG written, materialize path taken (only the window's extent in the output); (c) existing file_gbx top-level-path input still writes. 
- [ ] **Step 2-4:** fail → implement the branch → pass.
- [ ] **Step 5: commit** (`feat(ds): cog_gbx path-direct converts whole-file virtual tiles`).

---

### Task 6: wire the force-output params + header/pixel split into ONE representative function per family; Serverless proof

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (ONE per family: `rst_width` [header accessor], `rst_avg` [pixel accessor], `rst_clip` [deferrable-ish tile op], `rst_slope` [pixel tile op], `rst_merge` [agg], `rst_retile` [UDTF])
- Test: `python/geobrix/test/pyrx/test_virtual_aware_family.py`
- Serverless: `prompts/features/2026-08-01-functions-virtual-aware-serverless.py`

**Interfaces:**
- The 6 representatives: accessor wrappers use `open_header` (rst_width) vs `_open` (rst_avg); tile-op wrappers use `_open` for input and `shape_output(result, virtualize_dir=, virtualize_prefix=, materialize=)` at return, gaining the 3 optional params in their signature (Python API + SQL registration signature). agg uses `_open_all`; UDTF uses `_open`.

- [ ] **Step 1: failing tests** — for each representative on a VIRTUAL input tile: rst_width header-only (no read); rst_avg materializes + correct; rst_slope(virtual) auto-materializes result; rst_slope(virtual, virtualize_dir=tmp) returns light row round-tripping to the slope result; rst_slope(virtual, materialize=True) bytes; conflict errors; rst_clip(virtual) stays virtual (clip_polygon set, raster None) under auto; rst_merge/rst_retile consume virtual inputs.
- [ ] **Step 2-4:** fail → wire the 6 → pass (Docker).
- [ ] **Step 5:** rebuild+stage wheel; author + fire Serverless notebook (read virtual → rst_slope(virtualize_dir=Volume) → write cog_gbx; assert written COG round-trips, worker-side). Verify all_ok. Commit the family wiring (`feat(pyrx): virtual-aware + force-output on representative rst_* per family`).

---

## PHASE B — full-catalog mechanical sweep (after Phase A green)

### Task 6.5: `open_header` returns WINDOW dims for a windowed virtual tile (prereq for the accessor sweep)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` (`open_header`)
- Test: `python/geobrix/test/pyrx/test_core_open_header.py` (add windowed cases)

**Why:** Task 6's review surfaced that `open_header` opens the FULL source for a virtual tile, so a header accessor (rst_width/height/boundingbox/scale/origin) on a SUB-WINDOWED virtual tile returns the full-source dims, not the window's — inconsistent with the pixel path (`_open`, which respects the window) and with the materialized-equivalent tile. Task 7 wires ALL header accessors, so this must be consistent FIRST. Decision (user): a header accessor on a windowed virtual tile returns the WINDOW's dims/extent (== materialized-equivalent), still header-only (no pixel read).

**Interfaces:** `open_header(tile)` unchanged signature. For a virtual tile WITH a sub-window (window present AND not full-extent), yield a dataset whose reported width/height/transform/bounds reflect the WINDOW — derived from the window offset/size + the source's transform (a `rasterio.vrt.WarpedVRT`-free, read-free adjustment: open the source header, compute `window_transform`, and present window dims). Simplest robust impl: open the source header (as today), then wrap so `.width/.height` = window w/h and `.transform` = `src.window_transform(Window(*window))` and `.bounds` follow — WITHOUT reading pixels. If a clean wrapper is hard, an acceptable alternative that stays header-only: open the source, and have the accessor-facing values computed from window+src.transform via a tiny shim object exposing `.width/.height/.count/.crs/.transform/.bounds/.profile/.nodata/.dtypes` (a "header view"), never `.read`. Whole-file virtual (window None or == full extent) and materialized/bytes tiles: unchanged.

- [ ] **Step 1: Write the failing test** — extend `test_core_open_header.py`:
  - `test_open_header_windowed_virtual_reports_window_dims`: a virtual tile over a 2048×2048 source with window `(100, 50, 512, 384)` → `open_header` yields ds with `width==512, height==384`, and `transform`/`bounds` equal `src.window_transform` of that window; assert `.read` NOT called (class-level patch). This is the discriminating test — current code returns 2048 and FAILS.
  - keep the existing whole-file test (window full → still full dims), and the bytes test.

- [ ] **Step 2: Run → FAIL** (`--path python/geobrix/test/pyrx/test_core_open_header.py`), current returns 2048 for the windowed case.

- [ ] **Step 3: Implement** the window-view in `open_header` (header-only; no `.read()`), per Interfaces. Ensure `boundingbox`/`scalex`/`upperleftx` accessors (which read `.transform`/`.bounds`) get window-correct values.

- [ ] **Step 4: Run → PASS.** Also run `test_virtual_aware_family.py` (rst_width family test) to confirm no regression.

- [ ] **Step 5: Lint + commit** (`feat(pyrx): open_header reports window dims for a windowed virtual tile`).

---

### Task 7: swap all remaining `_serde.open_tile → _open` + header/pixel accessor split + params passthrough

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (remaining ~49 `_serde.open_tile` sites), core agg/gridagg/cellraster/tessellate/tiling reducers that open tiles, and any core module that opens bytes directly.
- Test: full non-netcdf pyrx + ds suites (regression) + a coverage test asserting no `_serde.open_tile(bytes(tile[...]))` pattern remains in functions.py (grep-based guard test).

**Interfaces:** identical helpers from Phase A; this task is pure repetition — no new interfaces.

- [ ] **Step 1: mechanical swap** — replace every remaining `with _serde.open_tile(bytes(tile["raster"])) as ds:` (and the multi-tile `[bytes(t["raster"]) ...]` forms) with `_open(tile)` / `_open_all(tiles)`. Accessor wrappers: header-only ones → `open_header`, pixel ones → `_open` (per the Global-Constraints lists). Tile-returning wrappers: add the 3 force-output params + `shape_output` at return. Do it module-by-module (accessors block, terrain block, focal, mapalgebra, indices, resample, edit, agg, tessellate, xyz, features, tin) so each is independently reviewable.

- [ ] **Step 2: guard test** — add `test_no_v1_open_tile_pattern.py`: grep functions.py source for `_serde.open_tile(` and assert 0 remaining (or an allowlist of legitimate v1-bytes constructors like `rst_fromcontent`). Prevents backsliding.

- [ ] **Step 3: regression** — run the full non-netcdf pyrx suite (`test/pyrx/` explicit file list excluding any netcdf) + ds writer/raster suites. All green. Fix any function whose op body assumed `tile["raster"]` bytes directly (should be none — all go through the wrapper).

- [ ] **Step 4: lint gate** — Docker `gbx:lint:python --check` on all changed files; reformat in-container if needed.

- [ ] **Step 5: commit** (`feat(pyrx): catalog-wide swap to _open front-door + header/pixel + force-output`). Large commit is fine (mechanical); note it in the message.

---

### Task 8: docs — per-function override note, Light-vs-Heavy blurb, Virtual Tiles content, advice taxonomy

**Files:**
- Modify: light `rst_*` function docs (the doc-test-sourced pages + function-info descriptions), `docs/docs/` Light-vs-Heavy page, the queued Virtual Tiles page stub.
- Doc examples live in doc-test source and execute.

- [ ] **Step 1** — add a shared "Virtual-tile overrides" note (virtualize_dir / virtualize_prefix / materialize; light-only; heavy has none; materialize = light→heavy bridge) to the tile-returning function docs (a reusable MDX snippet/partial imported into each, to avoid 46× copy-paste — DRY in docs too).
- [ ] **Step 2** — Light-vs-Heavy page: a blurb that light tiles can be virtual (bytes-free) and must be materialized before heavy consumes them; heavy has none of the override params.
- [ ] **Step 3** — Virtual Tiles page (queued capstone): if building now, add the advice taxonomy (metadata accessors free / pixel accessors materialize / deferrable ops stay virtual / pixel ops materialize / writers are a materialize boundary / crossing-to-heavy materializes). If deferring the full page, at least land the taxonomy section + link from Large Rasters. (Coordinate with the queued virtual-tiles-docs-page + hero-diagram capstone — this task delivers the CONTENT; the page polish + diagram remain the capstone.)
- [ ] **Step 4** — verify any doc-test snippets execute (`gbx:test:python-docs --path <rel> --skip-build`); QC wave-leak/internals check clean.
- [ ] **Step 5: commit** (`docs: virtual-tile override note per rst_* + Light-vs-Heavy + advice taxonomy`).

---

## Self-Review

**1. Spec coverage:**
- Consume-virtual everywhere → Task 1 (`_open`/`_open_all`) + Task 6 (family) + Task 7 (sweep). ✓
- Header-only accessors → Task 2 (`open_header`) + Task 6 (rst_width) + Task 7 (all accessors split). ✓
- Force-output params → Task 3 (`shape_output`) + Task 6 (rst_slope etc.) + Task 7 (all tile ops). ✓
- Writer dual-accept + auto-materialize + fix v2 regression → Task 4. ✓
- cog_gbx path-direct whole-file virtual → Task 5. ✓
- Two phases (A proof / B sweep) → Tasks 1-6 (A) / 7-8 (B). ✓
- Docs (per-fn note, Light-vs-Heavy, Virtual Tiles content, taxonomy) → Task 8. ✓
- Materialize = light→heavy bridge → documented Task 8; enforced by heavy having no params (no code here). ✓
- Serverless proof → Task 6. ✓
- Non-wired (no registration additions) → Global Constraints; params are optional args. ✓

**2. Placeholder scan:** No TBD/TODO. Task 7 is intentionally a bounded mechanical repetition of the Phase-A pattern (not a placeholder — the pattern is fully specified in Tasks 1-6). Task 8's "build now vs defer full page" is a real branch with a defined minimum (land the taxonomy + link).

**3. Type consistency:** `_open(tile)`/`_open_all(tiles)` (Task 1) used in 6/7. `open_header(tile)` (Task 2) used by header accessors (6/7). `shape_output(tile, *, virtualize_dir, virtualize_prefix, materialize) -> VirtualTile` (Task 3) used by tile-returning wrappers (6/7). `_to_virtual_tile` shared normalizer (Task 1) reused by writers (Task 4). `materialize_to_bytes`/`open_tile` reused, not redefined. Header-only vs pixel accessor lists are fixed in Global Constraints and applied identically in Task 6 + Task 7.

## Deferred (tracked, not built here)

- Heavy-tier v2 handling.
- `rst_transform` lazy-warp internals (Inc 5) — inherits the force-output params.
- Virtual Tiles page polish + hero diagram (capstone; Task 8 delivers the content/taxonomy).
- Carried: COG `format` metadata "gtiff"; `materialize_to_bytes` clean-profile; dedup `_epsg_of`/`_epsg_int`; non-EPSG CRS.
