# Virtual-Tile Reader Increment 2 — `virtualTiles` Emit Mode + v2 Cutover — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the light-tier `raster_gbx` reader emit the v2 tile struct always, add a `virtualTiles` option that produces bytes-free `(path, whole-file-window)` tiles, and keep the default materialized path byte-identical (now widened to v2).

**Architecture:** `raster_gbx` is a Spark Python DataSource V2 (`RasterGbxDataSource`/`RasterGbxReader` in `ds/raster.py`) emitting positional rows `(source, tile)`. Today `tile` is v1 `_serde.TILE_SCHEMA` `(cellid, raster, metadata)`. This increment swaps the emitted schema to `V2_TILE_SCHEMA` (8 fields, `raster` nullable) from `pyrx/core/virtual_tile.py`, routes both the normal and legacy read paths through one `_v2_tile_row` builder, and adds a `virtualTiles` planning+emission branch that skips staging/encode entirely.

**Tech Stack:** Python 3.12, PySpark Python DataSource V2, rasterio ≥1.3, pytest. Tests run in the `geobrix-dev` Docker container via `gbx:test:pyrx`. Serverless leg via `.venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py` (oauth-fe, env v5).

## Global Constraints

- **Light tier only:** pyrx/ds is JAR-free. NO `osgeo.gdal` import (rasterio only). NO `spark.conf.set` / `_jvm` / `.rdd` in library code.
- **Exploratory / non-wired:** reader OPTION only. NO catalog registration, NO `registered_functions.txt` / `function-info.json` / bindings changes. Keeps binding-parity + QC green.
- **v2 struct is the single source:** import `V2_TILE_SCHEMA` and (where useful) `VirtualTile` from `databricks.labs.gbx.pyrx.core.virtual_tile`. Do NOT redefine the struct in `ds/`.
- **Emitted rows are POSITIONAL tuples** matching schema field order. Outer envelope: `(source: str, tile: <struct>)`. v2 `tile` tuple order = `(cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)` where `window` is `(col_off,row_off,width,height)` or `None`. Getting the order wrong = silent wrong-column data.
- **Default (materialized) pixels must stay byte-identical** to the pre-cutover reader; passthrough parity preserved. Only the row SHAPE widens (raster still populated).
- **Virtual mode invariants:** `raster=None`, `path` set, `window=(0,0,W,H)` (never None). No staging, no `encode_tile`, no `plan_layout`, no bbox split. One tile per file.
- Test dir: `python/geobrix/test/ds/` (existing reader tests live here). Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/<file> --log <name>.log` (Docker; dispatch as a Task, don't block).
- Commits: end body with `Co-authored-by: Isaac`. `gh auth switch --user mjohns-databricks` before any push.
- Reference anchors (pre-change line numbers in `ds/raster.py`): `reader_schema()` 149-156; `_TilePartition` 175-230; `_plan_partitions_for_file` 278-392; `RasterGbxReader.__init__` 396-421; `partitions()` 423-442; `read()` 444-500; `_read_legacy()` 505-624; `RasterGbxDataSource.schema()` 632-633. `CELLID_FRESH=-1` in `ds/_encode.py:19`.

---

### Task 1: v2 row builder + `reader_schema_v2` (shared assembly point)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (add `reader_schema_v2`, `_v2_tile_row`)
- Test: `python/geobrix/test/ds/test_raster_v2_row.py`

**Interfaces:**
- Consumes: `V2_TILE_SCHEMA` from `databricks.labs.gbx.pyrx.core.virtual_tile`.
- Produces:
  - `reader_schema_v2() -> StructType` — `(source: string non-null, tile: V2_TILE_SCHEMA non-null)`.
  - `_v2_tile_row(cellid, raster, path, window, metadata, clip_polygon=None, clip_crs=None, crs=None) -> tuple` — returns the 8-tuple in `V2_TILE_SCHEMA` field order; `window` (a 4-tuple `(col,row,w,h)` or None) is serialized to the nested dict `{"col_off","row_off","width","height"}` or None.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_v2_row.py
"""v2 reader schema + row builder: single struct-assembly point for the reader."""
from databricks.labs.gbx.ds.raster import reader_schema_v2, _v2_tile_row
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA


def test_reader_schema_v2_is_source_plus_v2_tile():
    sch = reader_schema_v2()
    assert [f.name for f in sch.fields] == ["source", "tile"]
    assert sch["tile"].dataType == V2_TILE_SCHEMA
    assert sch["tile"].dataType["raster"].nullable is True


def test_v2_row_materialized_shape_and_order():
    row = _v2_tile_row(
        cellid=-1, raster=b"abc", path="/v/x.tif", window=(1, 2, 300, 400),
        metadata={"driver": "GTiff"},
    )
    # 8-tuple in V2_TILE_SCHEMA field order
    assert row[0] == -1                      # cellid
    assert row[1] == b"abc"                  # raster
    assert row[2] == "/v/x.tif"              # path
    assert row[3] == {"col_off": 1, "row_off": 2, "width": 300, "height": 400}
    assert row[4] is None and row[5] is None and row[6] is None  # clip_polygon/clip_crs/crs
    assert row[7] == {"driver": "GTiff"}     # metadata


def test_v2_row_virtual_null_raster_and_window_dict():
    row = _v2_tile_row(
        cellid=-1, raster=None, path="/v/x.tif", window=(0, 0, 512, 512),
        metadata={"format": "cog"},
    )
    assert row[1] is None                    # raster null (virtual)
    assert row[3] == {"col_off": 0, "row_off": 0, "width": 512, "height": 512}


def test_v2_row_none_window_serializes_to_none():
    row = _v2_tile_row(cellid=-1, raster=b"abc", path="/v/x.tif", window=None,
                       metadata={})
    assert row[3] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_v2_row.py --log v2row.log`
Expected: FAIL — `ImportError: cannot import name 'reader_schema_v2'`.

- [ ] **Step 3: Write minimal implementation**

Add to `ds/raster.py` (near `reader_schema`, ~line 156):

```python
def reader_schema_v2() -> StructType:
    """(source, tile) — tile is the v2 VirtualTile struct (raster nullable)."""
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("tile", V2_TILE_SCHEMA, nullable=False),
        ]
    )


def _v2_tile_row(
    cellid,
    raster,
    path,
    window,
    metadata,
    clip_polygon=None,
    clip_crs=None,
    crs=None,
) -> tuple:
    """Assemble one v2 tile tuple in V2_TILE_SCHEMA field order.

    ``window`` is a (col_off, row_off, width, height) tuple or None; it is
    serialized to the nested struct dict Spark expects (or None). This is the
    SINGLE place the reader assembles a v2 tile — both the virtual and
    materialized paths route through it.
    """
    win = None
    if window is not None:
        c, r, w, h = window
        win = {"col_off": int(c), "row_off": int(r), "width": int(w), "height": int(h)}
    return (
        int(cellid),
        raster,
        path,
        win,
        clip_polygon,
        clip_crs,
        crs,
        metadata,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_v2_row.py --log v2row.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_v2_row.py
git commit -m "feat(ds): v2 reader schema + single-point v2 tile row builder

reader_schema_v2 (source, V2_TILE_SCHEMA) and _v2_tile_row assemble the
8-field v2 tile in schema order (window->nested dict or None). Single
assembly point for the upcoming reader cutover; not wired into the
reader yet.

Co-authored-by: Isaac"
```

---

### Task 2: Cut the materialized reader paths over to v2

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`RasterGbxDataSource.schema`, `read`, `_read_legacy`)
- Test: `python/geobrix/test/ds/test_raster_datasource.py` (migrate assertions), `python/geobrix/test/ds/test_raster_bbox.py` (migrate)

**Interfaces:**
- Consumes: `_v2_tile_row`, `reader_schema_v2` (Task 1).
- Produces: reader emits `(source, <8-tuple v2 tile>)` from every non-virtual path; `RasterGbxDataSource.schema()` returns `reader_schema_v2()`. Pixels/metadata unchanged; only row shape widens (raster still populated, `path`=source, `window`=partition.window or `(0,0,W,H)` for passthrough).

**Design note:** every place that currently does `yield (source, (cellid, raster_bytes, meta))` becomes `yield (source, _v2_tile_row(cellid, raster_bytes, path=partition.file_path, window=<win>, metadata=meta))`. For passthrough, `window=(0,0,width,height)` (the whole-file window, since v1 used None there). For `encode_tile`, `window=partition.window`. In `_read_legacy`, use the window each yield corresponds to (bbox window; `(0,0,W,H)` for passthrough; `(col,row,w,h)` per split tile).

- [ ] **Step 1: Write the failing test (migrate + assert v2)**

Update `test_raster_datasource.py` — change the schema assertion and add v2 field checks. Replace the `test_schema_matches_tile_schema` body and add coverage:

```python
# in test_raster_datasource.py — migrate schema assertion to v2
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

def test_schema_is_v2_tile():
    schema = RasterGbxDataSource(options={"path": "x"}).schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == V2_TILE_SCHEMA

def test_materialized_row_is_v2_with_populated_raster(spark, tmp_path):
    f = _write_sample_gtiff(tmp_path)  # existing helper in this file
    row = spark.read.format("raster_gbx").load(str(f)).collect()[0]
    tile = row["tile"]
    assert tile["cellid"] == -1
    assert tile["raster"] is not None          # materialized: bytes present
    assert tile["path"] is not None            # provenance populated
    # whole-file GTiff passthrough -> window is the whole file
    assert tile["window"]["width"] > 0 and tile["window"]["height"] > 0
    assert tile["clip_polygon"] is None and tile["crs"] is None
    # pixels still decode
    from rasterio.io import MemoryFile
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as out:
        assert out.width > 0
```

Migrate any other assertion in the file that used `_serde.TILE_SCHEMA` or assumed a 3-field tile (they index by name like `row["tile"]["cellid"]`, which still works — only the schema-equality assert and any positional/`len` checks need changing). Do the same sweep in `test_raster_bbox.py`.

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_datasource.py --log dsmig.log`
Expected: FAIL — schema is still v1 (`reader_schema()`), new assertions fail.

- [ ] **Step 3: Implement the cutover**

In `ds/raster.py`:

(a) `RasterGbxDataSource.schema()` → `return reader_schema_v2()`.

(b) `read()` passthrough branch — replace the yield:
```python
            yield (
                source,
                _v2_tile_row(cellid, raster_bytes, path=partition.file_path,
                             window=(0, 0, width, height), metadata=meta),
            )
```

(c) `read()` windowed/encode branch — replace the yield:
```python
        yield (
            source,
            _v2_tile_row(cellid, raster_bytes, path=partition.file_path,
                         window=partition.window, metadata=meta),
        )
```

(d) `_read_legacy()` — each of its three `yield (source, (cellid, raster_bytes, meta))` sites becomes `_v2_tile_row(...)` with the matching window: bbox yield → the computed bbox window tuple; passthrough yield → `(0,0,width,height)`; split-tile yield → `(col,row,w,h)`.

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_datasource.py --path python/geobrix/test/ds/test_raster_bbox.py --log dsmig.log`
Expected: PASS. If a passthrough-parity test compares raw bytes, it still holds (raster bytes unchanged). If any test asserted exactly 3 tile subfields, update to the v2 field set.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_datasource.py \
        python/geobrix/test/ds/test_raster_bbox.py
git commit -m "feat(ds): cut raster_gbx reader output over to v2 tile struct

schema() now returns reader_schema_v2; every materialized/legacy read
path emits via _v2_tile_row (raster populated + path/window provenance,
new fields null). Pixels byte-identical; only the row shape widens.
Reader tests migrated to the v2 struct (by field name).

Co-authored-by: Isaac"
```

---

### Task 3: `virtualTiles` option — planning + bytes-free emission

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`_TilePartition` field, `RasterGbxReader.__init__`, `_plan_partitions_for_file`, `partitions`, `read`)
- Test: `python/geobrix/test/ds/test_raster_virtual.py`

**Interfaces:**
- Consumes: `_v2_tile_row` (Task 1), the v2-emitting `read` (Task 2), `_layouts` fixture pattern.
- Produces: `.option("virtualTiles","true")` → one bytes-free row per file: `raster=None`, `path`=source, `window=(0,0,W,H)`, metadata = header-subset (`driver`, `format`, `width`, `height`, `count`, `sourcePath`). `_TilePartition` gains `emit_virtual: bool = False`.

**Design note:** virtual planning is a short-circuit BEFORE the bbox/normal branches in `_plan_partitions_for_file` (whole-file only this increment — bbox+virtual deferred). It opens the header once for `(W,H)`. The `read` virtual branch does NOT stage or call `_encode` — it builds the row directly from `partition.window` and a lightweight header read (or carries dims from planning via metadata; prefer reading dims in planning and passing them through metadata to avoid a second open on the worker — but a header open on the worker is acceptable and simpler; choose the header-open-on-worker approach for a self-contained `read`).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_raster_virtual.py
"""virtualTiles emit mode: bytes-free (path, whole-file window) tiles that
round-trip through open_tile to the correct pixels.
"""
import numpy as np
import rasterio

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.test.pyrx import _layouts  # 3-layout fixture (Inc 1)


def _write3(tmp_path):
    return {
        "cog": _layouts.write_cog(str(tmp_path / "a.cog.tif"), 512, 512, 256),
        "tiled": _layouts.write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), 512, 512, 256),
        "striped": _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), 512, 512),
    }


def test_virtual_emits_bytes_free_rows(spark, tmp_path):
    d = _write3(tmp_path)
    df = (spark.read.format("raster_gbx")
          .option("virtualTiles", "true")
          .load(str(tmp_path)))
    assert df.schema["tile"].dataType == V2_TILE_SCHEMA
    rows = df.collect()
    assert len(rows) == 3  # one per file
    for r in rows:
        t = r["tile"]
        assert t["raster"] is None                      # bytes-free
        assert t["path"] is not None
        assert t["window"]["col_off"] == 0 and t["window"]["row_off"] == 0
        assert t["window"]["width"] == 512 and t["window"]["height"] == 512
        assert t["metadata"] and "width" in t["metadata"]


def test_virtual_row_round_trips_through_open_tile(spark, tmp_path):
    d = _write3(tmp_path)
    rows = (spark.read.format("raster_gbx")
            .option("virtualTiles", "true")
            .load(d["tiled"])).collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    tile = VirtualTile.from_row(t)                       # reader row -> VirtualTile
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
    with rasterio.open(d["tiled"]) as ds:
        exp = ds.read(1)
    assert np.array_equal(got, exp)                      # whole-file window == full read
```

Note on the `_layouts` import path: it currently lives at `python/geobrix/test/pyrx/_layouts.py`. If `test/ds/` cannot import it as `databricks.labs.gbx.test.pyrx._layouts`, add a tiny local corpus helper in `test/ds/` or import via a relative path fixture — the implementer resolves the exact import that works in the container (the assertion content is what matters).

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_virtual.py --log virt.log`
Expected: FAIL — `virtualTiles` option ignored (rows have raster bytes, or schema mismatch).

- [ ] **Step 3: Implement**

(a) `_TilePartition.__slots__` += `"emit_virtual"`; `__init__` gains `emit_virtual: bool = False` and `self.emit_virtual = emit_virtual`.

(b) `RasterGbxReader.__init__`: `self.emit_virtual = str(options.get("virtualTiles", "false")).lower() == "true"`.

(c) `_plan_partitions_for_file` gains a param `emit_virtual: bool = False`; at the TOP of the function (before bbox/normal branches):
```python
    if emit_virtual:
        import rasterio
        with rasterio.open(file_path) as ds:
            width, height = ds.width, ds.height
        return [
            _TilePartition(
                file_path=file_path,
                window=(0, 0, width, height),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=True,
            )
        ]
```

(d) `partitions()`: pass `emit_virtual=self.emit_virtual` into `_plan_partitions_for_file(...)`.

(e) `read()`: add a branch BEFORE the passthrough branch:
```python
        if getattr(partition, "emit_virtual", False):
            import rasterio
            with rasterio.open(partition.file_path) as ds:
                meta = {
                    "sourcePath": partition.file_path,
                    "driver": ds.driver,
                    "format": ("cog" if ds.driver == "COG" else "gtiff"),
                    "width": str(ds.width),
                    "height": str(ds.height),
                    "count": str(ds.count),
                }
            yield (
                source,
                _v2_tile_row(_encode.CELLID_FRESH, None, path=partition.file_path,
                             window=partition.window, metadata=meta),
            )
            return
```

- [ ] **Step 4: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/test_raster_virtual.py --log virt.log`
Expected: PASS (2 tests). If `VirtualTile.from_row` needs the metadata-map value types coerced, confirm the row's `window` nested struct maps back to the 4-tuple (from_row already handles the nested dict/Row).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py \
        python/geobrix/test/ds/test_raster_virtual.py
git commit -m "feat(ds): virtualTiles reader option emits bytes-free tiles

.option('virtualTiles','true') emits one virtual tile per file (raster
null, path set, window=(0,0,W,H)) with no staging/encode. Round-trips
through open_tile to the correct pixels. Whole-file window only;
taxonomy + bbox-virtual deferred to Inc 3.

Co-authored-by: Isaac"
```

---

### Task 4: Full-suite regression + Serverless proof

**Files:**
- Serverless notebook: `prompts/features/2026-07-31-virtual-tile-reader-serverless.py` (gitignored scratch)

**Interfaces:** consumes the shipped reader. No new source.

- [ ] **Step 1: Run the full ds + pyrx suites (regression for the cutover)**

Run (Docker, dispatch as Task):
`bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/ds/ --log ds-suite.log`
then `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/ --log pyrx-suite.log`
Expected: all pass. The v2 cutover touches every reader consumer — this confirms no downstream (e.g. functions reading reader output, doc tests) broke. If a failure surfaces in a consumer that indexed the v1 tile positionally, fix that consumer to use v2 field names (in scope — it's fallout of the cutover) and note it.

- [ ] **Step 2: Author the Serverless notebook**

Write `prompts/features/2026-07-31-virtual-tile-reader-serverless.py` (Databricks source; the runner strips %pip). Cells:
1. Corpus already staged at `/Volumes/geospatial_docs/geobrix/sample-data/large-raster/virtual-tile` (from Inc-1 rounds); if absent, regenerate via the local-temp→shutil.copyfile pattern.
2. `df = spark.read.format("raster_gbx").option("virtualTiles","true").load(CORPUS)` → assert `df.schema["tile"].dataType` is the v2 struct and rows are bytes-free (`raster is None`).
3. mapInPandas over the collected virtual rows: `VirtualTile.from_row(tile)` → `open_tile` on the WORKER → read band 1, assert shape == (H,W) of the file; self-report per-file status.
4. `dbutils.notebook.exit(json.dumps(summary))` with `all_ok`, counts, per-file results.

- [ ] **Step 3: Build the .ipynb + fire the job**

```bash
# build ipynb (drop %pip/docstring cells) then fire:
.venv-pyrx/bin/python -c "import nbformat,pathlib; from nbformat.v4 import new_code_cell,new_notebook; \
src=pathlib.Path('prompts/features/2026-07-31-virtual-tile-reader-serverless.py'); \
cells=[new_code_cell(c.strip('\n')) for c in src.read_text().split('# COMMAND ----------') \
if c.strip() and '%pip' not in c.lower() and not (c.lstrip().startswith(chr(34)*3))]; \
nb=new_notebook(cells=cells); nb.metadata['language_info']={'name':'python'}; \
nbformat.write(nb,str(src.with_suffix('.ipynb'))); print('wrote', src.with_suffix('.ipynb'))"

DATABRICKS_CONFIG_PROFILE=oauth-fe .venv-pyrx/bin/python notebooks/tests/run_notebooks_serverless.py \
  --notebook prompts/features/2026-07-31-virtual-tile-reader-serverless.ipynb \
  --wheel /Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/geobrix-0.4.4-py3-none-any.whl \
  --extras light --profile oauth-fe
```
NOTE: rebuild + restage the wheel FIRST (`GBX_BUNDLE_SKIP_JAR_UPLOAD=1 .venv-pyrx/bin/python notebooks/tests/push_wheel_to_volume.py`) so the reader change is in the wheel the workers install. Keep only the current 0.4.4 wheel/JAR in the artifact dir.

- [ ] **Step 4: Verify + record**

Confirm `all_ok=true`, rows>0, per-file `open_tile` succeeded on workers reading the reader-emitted virtual rows from real `/Volumes`. Paste the run URL + JSON into a RESULTS section in the notebook. No commit needed (gitignored).

- [ ] **Step 5: Commit any consumer fixes from Step 1** (if the regression surfaced downstream v1-indexing) in a focused commit; otherwise nothing to commit here.

---

## Self-Review

**1. Spec coverage:**
- Reader emits V2_TILE_SCHEMA always → Task 1 (schema/builder) + Task 2 (schema() cutover). ✓
- `virtualTiles` option, bytes-free, one whole-file tile/file → Task 3. ✓
- Materialized path widened to v2, pixels byte-identical → Task 2. ✓
- Round-trip reader → open_tile → Task 3 test + Task 4 Serverless. ✓
- v1→v2 test migration → Task 2. ✓
- Passthrough collapses into whole-file virtual tile → Task 3 (no passthrough in virtual branch). ✓
- Empty dir → empty DataFrame with v2 schema → covered by schema() returning v2 unconditionally (Task 2); existing bbox no-overlap test in test_raster_bbox.py exercises the empty path. ✓
- Serverless proof (reader-sourced, worker-side) → Task 4. ✓
- Non-wired (no registration) → Global Constraints; no task touches registration files. ✓
- Heavy-tier v2 handling explicitly deferred → not a task (spec non-goal). ✓

**2. Placeholder scan:** No TBD/TODO. Every code step has real code. Task 3's `_layouts` import caveat and Task 4's consumer-fix contingency are real instructions with a defined resolution, not placeholders.

**3. Type consistency:** `_v2_tile_row(cellid, raster, path, window, metadata, clip_polygon=None, clip_crs=None, crs=None)` signature identical across Tasks 1/2/3. Row order = V2_TILE_SCHEMA field order `(cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)` used consistently. `window` param is always the 4-tuple-or-None; `_v2_tile_row` is the only place it becomes a dict. `emit_virtual` field/param/option threaded consistently (Task 3). `reader_schema_v2` used by `schema()` (Task 2) and asserted in tests (Tasks 1/3).

## Deferred (tracked, not built here)

- Heavy-tier v2-struct handling (Scala) — required before heavy consumes the now-default v2 reader output. Gated on heavy-tier parity.
- Window taxonomy: overlapPercent / user-bounds / x,y tiling / overview `z`; bbox-driven virtual windows (Inc 3).
- Functions routed through `open_tile` catalog-wide (Inc 4).
- Carried from Inc 1: `materialize_to_bytes` clean-profile fix; dedup `_epsg_of`/`_epsg_int`; non-EPSG CRS handling.
