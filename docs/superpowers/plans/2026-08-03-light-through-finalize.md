# Light-Through-Finalize Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make cheap pure-read raster ops (`rst_initnodata`, `rst_setsrid`, `rst_band`) record a *pending instruction* on a virtual tile and stay virtual (no pixel read, no new backing file); apply them together at the next `open_tile`. Fix `rst_memsize` to work on virtual tiles. Then wire eo-series `finalize_tiled_band_tbl` to stay light through to tessellation.

**Architecture:** Pending instructions ride in the existing `metadata` map (`pending_nodata` / `pending_srid` / `pending_bands`) — no v2 struct schema change. `core/open_tile.py` applies them to the rasterio read in a defined pipeline order (band-select → nodata → setsrid → window → clip → reproject). The three `rst_*` functions branch on input state: virtual input → append metadata key + return still-virtual v2 tile; materialized input → apply to bytes eagerly (today's behavior) but emit v2. Opt-out is the existing `materialize=True`.

**Tech Stack:** Python 3.12, pyrx (pure-Python/PySpark lightweight tier), rasterio, PySpark DataSource V2. Tests via pytest (venv `.venv-pyrx` locally with `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` set to the venv python; Docker for doc/full suites). Serverless validation via `gbx:test:notebooks-serverless` with `--set-var`/`--dep-notebook`.

## Global Constraints

- **No v2 struct schema change.** Pending instructions ride in `metadata: map<string,string>`. Key names: `pending_nodata`, `pending_srid`, `pending_bands` (exact literals).
- **v2 output everywhere.** `rst_initnodata`, `rst_setsrid`, `rst_band` must emit `V2_TILE_SCHEMA` in ALL code paths (they currently drop to `_serde.TILE_SCHEMA` / v1 3-field on the default path).
- **The one rule:** virtual input (`raster is None`, `path` set) → record pending key, stay virtual; materialized input (`raster` bytes present) → apply eagerly to bytes. Keyed on `VirtualTile.is_virtual()`.
- **Apply-at-open order (fixed):** band-select → nodata → setsrid → window → clip → reproject. Order-independent of call order.
- **Opt-out = existing force-output params.** `materialize=True` applies all pending now + bakes bytes; `virtualize_dir=<path>` applies now + writes a new backing file. No new API surface.
- **Pending keys are consumed when a materialized (bytes) tile ROW is produced.** "Materialization" here means emitting a tile **row that carries `raster` bytes** (a heavy/materialized tile) — NOT the transient `open_tile` read that yields an in-memory rasterio dataset for an accessor. Whenever a UDF returns a tile row with `raster` set (the eager materialized branch; `materialize=True`; `virtualize_dir` writing a new backing file then returning its ref; `materialize_to_bytes`), the `pending_*` keys that were baked into those bytes MUST be **removed** from that output row's `metadata`. A materialized (bytes) tile carries NO `pending_*` keys. This prevents double-application on a later open and keeps a materialized tile's metadata honest (provenance, not pending instructions). A virtual tile that merely records/accumulates an instruction keeps its keys; `open_tile` reading a window to answer an accessor does NOT strip keys from the (unchanged) virtual row.
- **setsrid is RELABEL only** (assign EPSG, no reproject). `rst_transform`/warp stays a materializing op. Broader non-EPSG/CRS-string handling is out of scope (separate Spec B).
- **Heavy tier untouched.** Pending instructions are a lightweight-tier concept.
- **pyrx never uses `spark.conf.set`/`_jvm`/`.rdd`.** (Standing pyrx constraint.)
- **Binding parity:** these three functions already exist in all bindings; no new registered function names are added, so `registered_functions.txt` / `function-info.json` are unchanged. Do NOT add new function names.
- **Tests execute real code with real assertions** on real rasters (`target/test-classes/modis/*.TIF` locally, or `sample-data/.../bench-corpus/rows/*.tif`). No mocking of rasterio/serde.

---

## Task 1: Pending-instruction metadata helpers + apply-at-open

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py`
- Test: `python/geobrix/test/pyrx/test_pending_instructions.py` (create)

**Interfaces:**
- Produces: module-level constants `PENDING_NODATA = "pending_nodata"`, `PENDING_SRID = "pending_srid"`, `PENDING_BANDS = "pending_bands"` in `open_tile.py`; a helper `_apply_pending(ds_or_bytes, metadata) -> bytes|dataset` that applies recorded instructions in fixed order at read time. Tasks 2-4 write these keys; this task consumes them.

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/test_pending_instructions.py`:

```python
import glob
import numpy as np
import rasterio
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _a_tif():
    # any real multi-band tif; bench-corpus rows are 4-band EPSG:4326
    for c in sorted(glob.glob("sample-data/Volumes/main/default/bench-corpus/rows/*.tif")):
        return c
    return sorted(glob.glob("target/test-classes/modis/*_B02.TIF"))[0]


def _virtual(tif, **meta):
    with rasterio.open(tif) as ds:
        w, h = ds.width, ds.height
    return VirtualTile(cellid=-1, raster=None, path=tif,
                       window=(0, 0, w, h), metadata=dict(meta))


def test_pending_nodata_applied_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.nodata == -9999.0


def test_pending_srid_relabels_crs_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_srid="3857")
    with ot.open_tile(vt) as ds:
        assert ds.crs.to_epsg() == 3857


def test_pending_bands_selects_bands_at_open():
    tif = _a_tif()
    with rasterio.open(tif) as ds:
        assert ds.count >= 2, "test needs a multi-band source"
    vt = _virtual(tif, pending_bands="1")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1


def test_pending_apply_order_band_then_nodata():
    # band-select THEN nodata: result is single-band with nodata set
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1
        assert ds.nodata == -9999.0


def test_no_pending_keys_is_noop():
    tif = _a_tif()
    vt = _virtual(tif)
    with rasterio.open(tif) as src:
        want_bands = src.count
    with ot.open_tile(vt) as ds:
        assert ds.count == want_bands
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -v`
Expected: FAIL (open_tile ignores the pending_* keys; nodata/crs/count unchanged).

- [ ] **Step 3: Implement apply-at-open in `open_tile.py`**

Add near the top of `open_tile.py` (after imports):

```python
PENDING_NODATA = "pending_nodata"
PENDING_SRID = "pending_srid"
PENDING_BANDS = "pending_bands"


_PENDING_KEYS = (PENDING_NODATA, PENDING_SRID, PENDING_BANDS)


def _parse_pending(metadata):
    """Return (bands|None, nodata|None, srid|None) from a tile metadata map."""
    md = metadata or {}
    bands = None
    if md.get(PENDING_BANDS):
        bands = [int(b) for b in str(md[PENDING_BANDS]).split(",") if b.strip()]
    nodata = float(md[PENDING_NODATA]) if md.get(PENDING_NODATA) not in (None, "") else None
    srid = int(md[PENDING_SRID]) if md.get(PENDING_SRID) not in (None, "") else None
    return bands, nodata, srid


def _without_pending(metadata):
    """Metadata map with all pending_* keys removed (consumed on materialization)."""
    return {k: v for k, v in (metadata or {}).items() if k not in _PENDING_KEYS}
```

Modify `_window_dataset_bytes` to accept and apply the pending instructions in order (band-select → nodata → setsrid), building the output profile accordingly:

```python
def _window_dataset_bytes(src, window: Window, pending=(None, None, None)) -> bytes:
    """Read one window into standalone GTiff bytes, applying pending instructions.

    pending = (bands|None, nodata|None, srid|None); applied in fixed order:
    band-select (which bands to read) -> nodata (profile) -> setsrid (crs relabel).
    """
    import rasterio.crs
    bands, nodata, srid = pending
    indexes = bands if bands else None  # rasterio: 1-based band list or None=all
    data = src.read(window=window, indexes=indexes)
    if data.ndim == 2:  # single-band read collapses a dim
        data = data[np.newaxis, :, :]
    profile = src.profile.copy()
    count = len(bands) if bands else src.count
    profile.update(
        driver="GTiff",
        height=int(window.height),
        width=int(window.width),
        count=count,
        transform=src.window_transform(window),
    )
    if nodata is not None:
        profile["nodata"] = nodata
    if srid is not None:
        profile["crs"] = rasterio.crs.CRS.from_epsg(srid)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()
```

In `open_tile` (the virtual branch), parse pending keys and thread them through. Replace the window-read block:

```python
        c, r, w, h = tile.window
        window = Window(c, r, w, h)
        pending = _parse_pending(tile.metadata)
        with rasterio.open(local_path) as src:
            src_epsg = src.crs.to_epsg() if src.crs else None
            want = _epsg_of(tile.crs) if tile.crs else None
            if want is not None and want != src_epsg:
                tile_bytes = _warp_window_bytes(src, window, want, pending=pending)
            else:
                tile_bytes = _window_dataset_bytes(src, window, pending=pending)
```

And thread `pending` through `_warp_window_bytes` (pass to its internal `_window_dataset_bytes` call):

```python
def _warp_window_bytes(src, window: Window, want_epsg: int, pending=(None, None, None)) -> bytes:
    win_bytes = _window_dataset_bytes(src, window, pending=pending)
    with MemoryFile(win_bytes) as mf, mf.open() as wds:
        with WarpedVRT(wds, crs=f"EPSG:{want_epsg}") as vrt:
            prof = vrt.profile.copy()
            prof.update(driver="GTiff")
            data = vrt.read()
            with MemoryFile() as out_mf:
                with out_mf.open(**prof) as dst:
                    dst.write(data)
                return out_mf.read()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -v`
Expected: PASS (all 5).

- [ ] **Step 5: Apply pending instructions in `open_header` too**

`open_header` (used by header-only accessors) must reflect band-select (`count`) and setsrid (`crs`) so `rst_numbands`/`rst_srid` are correct on a tile with pending instructions. Read `open_header` (lines ~266-309); the `_WindowedHeader.profile`/`count`/`crs` view must honor `_parse_pending(tile.metadata)`: override `count` to `len(bands)` when `pending_bands` set, and `crs` to `EPSG:srid` when `pending_srid` set. Add a test:

```python
def test_open_header_reflects_pending_bands_and_srid():
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_srid="3857")
    with ot.open_header(vt) as ds:
        assert ds.count == 1
        assert ds.crs.to_epsg() == 3857
```

Run the test; implement the `open_header` override until it passes.

- [ ] **Step 6: Strip pending keys when materializing to bytes (`shape_output` / `materialize_to_bytes`)**

The key place a virtual tile with pending keys crosses to bytes is `shape_output` (`materialize=True` and `virtualize_dir`) and `materialize_to_bytes`. Both currently read via `open_tile` (which now applies the pending instructions) and then return a `VirtualTile` — but they COPY the source metadata forward, which would leave stale `pending_*` keys on a now-materialized tile. Fix both so the produced-bytes tile carries `_without_pending(...)` metadata.

In `open_tile.py`, `materialize_to_bytes` (~line 311): when it builds the materialized `VirtualTile`, set `metadata=_without_pending(vt.metadata)`.

In `shape_output` (~line 343): the `virtualize_dir` branch that writes bytes and returns a `VirtualTile` (lines ~420-431) already builds `meta = dict(vt.metadata or {})` — change to `meta = _without_pending(vt.metadata)` before adding `meta["shape_output"]="virtualized"`. (Note: the `virtualize_dir` branch's "already virtual → return as-is" early return is for a tile whose `raster` is None with NO produced bytes; leave that path's keys intact — it's still virtual.)

> Subtlety: `shape_output`/`materialize_to_bytes` call `open_tile(vt)` to get pixels, and `open_tile` applies the pending instructions to those pixels. So the produced bytes already honor nodata/srid/bands; stripping the keys afterward is correct (baked in, no longer pending). Import `_without_pending` is same-module.

Add a test:

```python
def test_materialize_strips_pending_keys():
    from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes, PENDING_NODATA
    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999", pending_srid="3857")
    mat = materialize_to_bytes(vt)
    assert mat.raster is not None
    assert PENDING_NODATA not in (mat.metadata or {})
    assert "pending_srid" not in (mat.metadata or {})
    # and the bytes actually honor the instructions
    import rasterio, io
    from rasterio.io import MemoryFile
    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 3857
```

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k "materialize_strips" -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py python/geobrix/test/pyrx/test_pending_instructions.py
git commit -m "feat(pyrx): apply pending nodata/srid/bands at open_tile; strip keys on materialize

Co-authored-by: Isaac"
```

---

## Task 2: `rst_initnodata` records nodata on virtual, v2 everywhere

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (`_init_nodata_udf`, `_init_nodata_v2_udf`, `rst_initnodata` ~lines 1215-1239, 1293-1311)
- Test: `python/geobrix/test/pyrx/test_pending_instructions.py` (extend)

**Interfaces:**
- Consumes: `PENDING_NODATA` key + apply-at-open from Task 1.
- Produces: `rst_initnodata` default path emits v2; on virtual input records `metadata['pending_nodata']` and stays virtual.

- [ ] **Step 1: Write the failing test**

Append to the test file:

```python
from pyspark.sql import SparkSession
from databricks.labs.gbx.pyrx import functions as rx
from databricks.labs.gbx.ds.register import register


def _spark():
    return (SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false").getOrCreate())


def _read_virtual_df(spark, tif):
    register(spark)
    return (spark.read.format("gtiff_gbx").option("driver", "GTiff")
            .option("filterRegex", r".*\.(tif|TIF)$").load(tif))


def test_initnodata_virtual_stays_virtual_records_key():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile"))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None, "virtual tile must stay virtual (no bytes)"
    assert row["path"] is not None, "path reference preserved"
    assert row["metadata"]["pending_nodata"] == "-9999.0" or \
        row["metadata"]["pending_nodata"] == "-9999"


def test_initnodata_virtual_emits_v2_struct():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile"))
    fields = [f.name for f in out.schema["tile"].dataType.fields]
    assert "path" in fields and "window" in fields  # v2, not v1 3-field


def test_initnodata_materialize_true_bakes_bytes():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile", materialize=True))
    row = out.select("tile.raster").first()
    assert row["raster"] is not None and len(row["raster"]) > 0
```

- [ ] **Step 2: Run to verify failure**

Run: `PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k initnodata -v`
Expected: FAIL — today `_init_nodata_udf` returns v1 (no `path` field / materializes).

- [ ] **Step 3: Implement virtual-recording default path**

Change the default `rst_initnodata` (no force-output args) to emit v2 and record-when-virtual. Replace `_init_nodata_udf` (currently `@f.udf(_serde.TILE_SCHEMA)`) with a v2 UDF that branches:

```python
@f.udf(V2_TILE_SCHEMA)
def _init_nodata_udf(tile):
    if _tile_is_empty(tile):
        return None
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        # record pending instruction; stay virtual (no pixel read)
        md = dict(vt.metadata or {})
        md.setdefault(ot.PENDING_NODATA, str(edit._DEFAULT_NODATA))
        vt.metadata = md
        return vt.to_row()
    # materialized: apply eagerly to bytes (today's behavior), emit v2
    new_bytes = _init_nodata_bytes(tile)
    return VirtualTile(cellid=_tile_cellid(tile), raster=new_bytes,
                       metadata=dict(vt.metadata or {})).to_row()
```

Ensure imports at top of functions.py include `from databricks.labs.gbx.pyrx.core import edit` and `from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile` (verify; `ot` and `edit` are already imported — grep first). `rst_initnodata`'s dispatch (the `if _force_output_requested(...)` branch) stays; only the default `_init_nodata_udf` changes.

The materialized branch above builds the output `VirtualTile` from an already-materialized INPUT — which carries no `pending_*` keys (invariant), so `dict(vt.metadata or {})` is already pending-free. No extra strip needed on this branch. The `pending_*`-clearing on the force-output paths (`materialize=True`/`virtualize_dir`) is handled centrally in `_shaped_result_row` — see Step 3b.

- [ ] **Step 3b: Strip pending keys in `_shaped_result_row` (force-output paths)**

`_shaped_result_row` (functions.py ~209) wraps produced bytes as `VirtualTile(cellid=..., raster=bytes(new_bytes))` then calls `ot.shape_output(...)`. For `rst_initnodata(..., materialize=True)` / `virtualize_dir` on a VIRTUAL input, `new_bytes` came from `_init_nodata_bytes(tile)` which reads via `open_tile` — applying the input's pending keys into the bytes. The freshly-built `VirtualTile` starts with empty metadata (no keys), so `_shaped_result_row` is already pending-free for the produced tile. VERIFY this by reading `_shaped_result_row` (it builds `vt = VirtualTile(cellid=..., raster=bytes(new_bytes))` with default empty metadata). If it does NOT copy source metadata, no change needed here; add a one-line comment noting pending keys are consumed because the produced tile starts fresh. If it DOES copy source metadata in a variant, apply `_without_pending`. (Task 1 Step 6 handles the `open_tile`→`shape_output`/`materialize_to_bytes` path that DOES carry metadata forward.)

> Note: if `pending_nodata` is already present (a prior initnodata), `setdefault` preserves it — idempotent. If the source already has a nodata value, the pending key still standardizes to `-9999` on the NEXT open; that matches today's "preserve if set, else default" only for materialized tiles. Design choice per spec: virtual records the default; a source-nodata-preserving variant is out of scope.

- [ ] **Step 4: Run to verify pass**

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k initnodata -v`
Expected: PASS (3).

- [ ] **Step 5: Run the ds + pyrx accessor regression**

Run: `... -m pytest python/geobrix/test/pyrx/ python/geobrix/test/ds/ -q` (locally, venv). Fix any test that assumed `rst_initnodata` returns v1 3-field (update to v2 field set by name). Expected: green.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_pending_instructions.py
git commit -m "feat(pyrx): rst_initnodata records pending nodata on virtual tiles, emits v2

Co-authored-by: Isaac"
```

---

## Task 3: `rst_setsrid` and `rst_band` record on virtual, v2 everywhere

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (`_setsrid_udf` ~1344, `_band_udf` ~1370)
- Test: `python/geobrix/test/pyrx/test_pending_instructions.py` (extend)

**Interfaces:**
- Consumes: `PENDING_SRID` / `PENDING_BANDS` apply-at-open from Task 1.
- Produces: `rst_setsrid` / `rst_band` default paths emit v2; virtual input records the key + stays virtual.

- [ ] **Step 1: Write failing tests**

```python
def test_setsrid_virtual_records_key_stays_virtual():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_setsrid("tile", 3857))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None and row["path"] is not None
    assert row["metadata"]["pending_srid"] == "3857"
    # reading it applies the relabel
    row2 = out.select(rx.rst_srid("tile").alias("s")).first()
    assert row2["s"] == 3857


def test_band_virtual_records_key_stays_virtual():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_band("tile", 1))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None and row["path"] is not None
    assert row["metadata"]["pending_bands"] == "1"
    row2 = out.select(rx.rst_numbands("tile").alias("n")).first()
    assert row2["n"] == 1


def test_setsrid_band_v2_struct():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    for col in (rx.rst_setsrid("tile", 3857), rx.rst_band("tile", 1)):
        fields = [f.name for f in df.withColumn("tile", col).schema["tile"].dataType.fields]
        assert "path" in fields and "window" in fields
```

- [ ] **Step 2: Run to verify failure**

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k "setsrid or band" -v`
Expected: FAIL (v1 3-field / materializes).

- [ ] **Step 3: Implement**

Replace `_setsrid_udf` and `_band_udf` with v2, record-when-virtual variants (mirror Task 2's pattern). Band index is stored as-is (1-based) in `pending_bands`; validate `band_index >= 1` before recording (raise `ValueError` on `<1`, matching `edit.band`'s range guard intent — full upper-bound range check still happens at read via `edit.band`/rasterio):

```python
@f.udf(V2_TILE_SCHEMA)
def _setsrid_udf(tile, srid):
    if _tile_is_empty(tile) or srid is None:
        return None
    vt = ot._to_virtual_tile(tile)
    s = int(srid)
    if s <= 0:
        raise ValueError(f"rst_setsrid requires a positive EPSG code; got {s}")
    if vt.is_virtual():
        md = dict(vt.metadata or {}); md[ot.PENDING_SRID] = str(s); vt.metadata = md
        return vt.to_row()
    new_bytes = _setsrid_bytes(tile, s)
    return VirtualTile(cellid=_tile_cellid(tile), raster=new_bytes,
                       metadata=dict(vt.metadata or {})).to_row()


@f.udf(V2_TILE_SCHEMA)
def _band_udf(tile, band_index):
    if _tile_is_empty(tile) or band_index is None:
        return None
    vt = ot._to_virtual_tile(tile)
    b = int(band_index)
    if b < 1:
        raise ValueError(f"rst_band: band_index {b} out of range (>=1)")
    if vt.is_virtual():
        md = dict(vt.metadata or {}); md[ot.PENDING_BANDS] = str(b); vt.metadata = md
        return vt.to_row()
    new_bytes = _band_bytes(tile, b)
    return VirtualTile(cellid=_tile_cellid(tile), raster=new_bytes,
                       metadata=dict(vt.metadata or {})).to_row()
```

> Note on `pending_bands` accumulation: this spec supports a single band-select recorded as one index (matching `rst_band`'s single-band semantics). Multiple stacked `rst_band` calls overwrite the key (last wins) — document as a known limit; multi-band subset selection is a future extension of the same mechanism.

- [ ] **Step 4: Run to verify pass**

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k "setsrid or band" -v`
Expected: PASS (3).

- [ ] **Step 5: Regression**

Run: `... -m pytest python/geobrix/test/pyrx/ python/geobrix/test/ds/ -q`. Fix v1-shape assumptions. Expected: green.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_pending_instructions.py
git commit -m "feat(pyrx): rst_setsrid/rst_band record pending instructions on virtual tiles, emit v2

Co-authored-by: Isaac"
```

---

## Task 4: `rst_memsize` virtual-aware

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (`_memsize_udf` ~350, `_memsize_struct_udf` ~360, `rst_memsize` ~4020)
- Test: `python/geobrix/test/pyrx/test_pending_instructions.py` (extend)

**Interfaces:**
- Consumes: `open_header` from open_tile (already virtual-aware).
- Produces: `rst_memsize` returns a non-null estimate on a virtual tile.

- [ ] **Step 1: Write failing test**

```python
def test_memsize_virtual_not_null():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    v = df.select(rx.rst_memsize("tile").alias("m")).first()["m"]
    assert v is not None and v > 0
```

- [ ] **Step 2: Run to verify failure**

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k memsize -v`
Expected: FAIL (None — reads null raster field).

- [ ] **Step 3: Implement**

`rst_memsize` mirrors heavyweight "in-memory buffer length". For a virtual tile there are no bytes yet, so return the decoded window size estimate `count * width * height * itemsize` via `open_header` (no pixel read). Change `rst_memsize` to route through a struct-accepting UDF that is virtual-aware:

```python
@f.udf(LongType())
def _memsize_struct_udf(tile):
    if _tile_is_empty(tile):
        return None
    vt = ot._to_virtual_tile(tile)
    if not vt.is_virtual():
        return int(len(bytes(vt.raster)))
    # virtual: estimate decoded window footprint from the header (no pixel read)
    from databricks.labs.gbx.pyrx import _env
    _env.configure_gdal_env()
    with ot.open_header(tile) as ds:
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)
```

And point `rst_memsize` at the struct UDF (it currently uses `_memsize_udf(_raster_field(...))`):

```python
def rst_memsize(tile: ColLike) -> Column:
    """Serialized size for a materialized tile (buffer length); for a virtual tile,
    the estimated decoded window footprint (count*w*h*itemsize). LONG."""
    return _memsize_struct_udf(_col(tile))
```

Add `import numpy as np` to functions.py if not present (grep first).

> Note: this changes `rst_memsize` semantics for virtual tiles from "encoded bytes" to "decoded estimate". That is the only sensible answer with no bytes in hand, and `finalize_tiled_band_tbl` uses it for a rough size column, not an exact byte count. Document in the docstring (done above).

- [ ] **Step 4: Run to verify pass**

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k memsize -v`
Expected: PASS. Also confirm materialized path unchanged (add an assertion reading a `virtualTiles=false` df if a materialized fixture is handy, else covered by existing ds tests).

- [ ] **Step 5: Regression**

Run: `... -m pytest python/geobrix/test/pyrx/ python/geobrix/test/ds/ -q`. Expected: green.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_pending_instructions.py
git commit -m "fix(pyrx): rst_memsize returns decoded-window estimate for virtual tiles

Co-authored-by: Isaac"
```

---

## Task 5: Multi-instruction accumulation + docs

**Files:**
- Test: `python/geobrix/test/pyrx/test_pending_instructions.py` (extend)
- Modify: `docs/docs/api/virtual-tiles.mdx`, `docs/docs/api/execution-tiers.mdx` (the virtual↔materialized advice section)

**Interfaces:** none new; validates Tasks 1-4 compose.

- [ ] **Step 1: Accumulation test**

```python
def test_accumulated_instructions_apply_in_order():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = (df.withColumn("tile", rx.rst_band("tile", 1))
             .withColumn("tile", rx.rst_initnodata("tile"))
             .withColumn("tile", rx.rst_setsrid("tile", 3857)))
    row = out.select("tile.raster", "tile.metadata").first()
    assert row["raster"] is None  # still virtual after 3 ops
    md = row["metadata"]
    assert md["pending_bands"] == "1"
    assert md["pending_nodata"].startswith("-9999")
    assert md["pending_srid"] == "3857"
    # materialize once: all three apply
    m = df.withColumn("tile", rx.rst_band("tile", 1)) \
          .withColumn("tile", rx.rst_setsrid("tile", 3857)) \
          .withColumn("tile", rx.rst_initnodata("tile", materialize=True))
    r = m.select(rx.rst_numbands("tile").alias("n"),
                 rx.rst_srid("tile").alias("s"),
                 "tile.raster", "tile.metadata").first()
    assert r["n"] == 1 and r["s"] == 3857
    assert r["raster"] is not None  # materialized
    # pending keys consumed on materialization to bytes
    md = r["metadata"] or {}
    assert "pending_bands" not in md and "pending_srid" not in md and "pending_nodata" not in md
```

- [ ] **Step 2: Run — verify pass** (mechanism already built in Tasks 1-4)

Run: `... -m pytest python/geobrix/test/pyrx/test_pending_instructions.py -k accumulated -v`
Expected: PASS. If order-dependence surfaces, fix in `_parse_pending`/apply order (Task 1), not here.

- [ ] **Step 3: Doc — virtual-tiles.mdx "Operating on tiles" section**

In `docs/docs/api/virtual-tiles.mdx`, under "Operating on tiles: your choice of output", add a short paragraph:

```markdown
### Instructions that stay virtual

A few cheap, common operations record an **instruction** on a virtual tile instead
of reading pixels — the tile stays bytes-free and the instruction is applied on the
next read (alongside the window and any clip/reproject):

- `rst_initnodata` — set the NoData value
- `rst_setsrid` — relabel the CRS (assign an EPSG code; not a reproject)
- `rst_band` — select a band

They accumulate: chain them on a virtual tile and all apply together when the tile is
finally read (e.g. at tessellation). Pass `materialize=True` (or `virtualize_dir`) to
apply them immediately and produce bytes.
```

- [ ] **Step 4: Doc — execution-tiers.mdx advice**

In the `#virtual-materialized-advice` section, add `rst_initnodata`, `rst_setsrid`, `rst_band` to the list of operations that stay virtual by default.

- [ ] **Step 5: Voice check + docs build**

Run: `grep -rn -iE "wave [0-9]+|inc [0-9]+" docs/docs/` → empty.
Run (Docker): `bash scripts/commands/gbx-docs-static-build.sh --log lf-build.log` → "Compiled successfully" + "Generated static files", no broken-link errors.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/test/pyrx/test_pending_instructions.py docs/docs/api/virtual-tiles.mdx docs/docs/api/execution-tiers.mdx
git commit -m "test+docs(pyrx): pending-instruction accumulation + virtual-tiles guidance

Co-authored-by: Isaac"
```

---

## Task 6: Wire eo-series finalize to stay virtual + Serverless validation

**Files:**
- Modify: `notebooks/examples/eo-series/config_nb.ipynb` (`finalize_tiled_band_tbl`, cell ~20)
- (No further code changes; consumes Tasks 1-4.)

**Interfaces:** consumes the light-through-finalize behavior end-to-end.

- [ ] **Step 1: Confirm finalize now stays virtual (local reasoning)**

`finalize_tiled_band_tbl` currently does `.withColumn("tile", rx.rst_initnodata("tile"))`. With Task 2, on a virtual input this records `pending_nodata` and stays virtual — no code change to the call is strictly required. The `rst_memsize`/`rst_boundingbox`/`rst_srid` columns now work on virtual tiles (Task 4 + existing header accessors). So the `band_*_tile` Delta table will store **virtual** rows (bytes-free path+window refs + pending nodata), and tessellation (`gen_tessellate_tiled_band` → `open_tile`) materializes at read.

Update the helper's comments/docstring (they were adjusted earlier) to state that `rst_initnodata` here records a pending instruction and the tile stays virtual through to tessellation. No behavior code change unless Step 3 finds a break.

- [ ] **Step 2: Rebuild + stage the wheel (carries Tasks 1-4)**

```bash
export DATABRICKS_CONFIG_PROFILE=oauth-fe
cd python/geobrix && python3 -m build && cd ../..
databricks fs cp python/geobrix/dist/geobrix-0.5.0-py3-none-any.whl \
  dbfs:/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.5.0-py3-none-any.whl --overwrite
# verify bytes landed (compare local vs remote size)
```

- [ ] **Step 3: Serverless run with forced compute**

```bash
export DATABRICKS_CONFIG_PROFILE=oauth-fe
bash scripts/commands/gbx-test-notebooks-serverless.sh \
  --notebook 'notebooks/examples/eo-series/03. Gridded EO Data.ipynb' \
  --dep-notebook 'notebooks/examples/eo-series/config_nb.ipynb' \
  --set-var FORCE_REBUILD=True \
  --extras light,stac,vizx --extra-deps rich \
  --wheel /Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.5.0-py3-none-any.whl \
  --log lf-eoseries03.log
```
Expected: `ALL NOTEBOOKS SUCCEEDED`. If a cell breaks because a still-virtual tile hits a bytes-expecting escape hatch (`tile_to_numpy`/`rst_apply`/`plot_raster(tile["raster"])`), add the minimal `materialize=True` at that exact call (per spec: those are opt-outs).

- [ ] **Step 4: Verify the tile table is actually virtual (the whole point)**

Add a one-off check cell OR query post-run: read one row of `band_b02_tile` and assert `tile.raster IS NULL` and `tile.path IS NOT NULL` and `tile.metadata['pending_nodata']` is set. Record the run page URL. This is the proof that finalize stayed light and materialization moved to tessellation.

- [ ] **Step 5: Commit**

```bash
git add "notebooks/examples/eo-series/config_nb.ipynb"
git commit -m "examples(eo-series): finalize stays virtual; materialize at tessellation

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:** Task 1 = apply-at-open mechanism (spec §Apply-at-open). Tasks 2-3 = the three members record-when-virtual + v2 (spec §Members, §The one rule). Task 4 = rst_memsize fix (spec §Also fixed). Task 5 = accumulation + docs (spec §Testing, doc surfaces). Task 6 = eo-series light-through-finalize end-to-end (spec §Testing end-to-end). CRS non-EPSG breadth deferred to Spec B (spec §Non-Goals) — not in any task. ✓

**Placeholder scan:** all code steps carry real code; exact metadata key literals fixed in Global Constraints. No TBD/TODO. ✓

**Type consistency:** `PENDING_NODATA/SRID/BANDS` defined in Task 1, used verbatim in 2-4. `V2_TILE_SCHEMA`, `VirtualTile.to_row()`, `ot._to_virtual_tile`, `ot.open_header`, `edit._DEFAULT_NODATA` all verified against current source. `_parse_pending` returns `(bands, nodata, srid)` used consistently. ✓

**Pending-key consumption (added):** the invariant "materialization to bytes strips `pending_*` keys" is covered by Task 1 Step 6 (`materialize_to_bytes`/`shape_output`), Task 2 Step 3b (`_shaped_result_row` verification), and asserted in Task 5's accumulation test. `_without_pending` helper defined in Task 1. ✓

**Open risk flagged for review loop:** `_window_dataset_bytes` single-band `data[np.newaxis]` reshape and `count` recomputation must be verified against a real multi-band read (Task 1 Step 4 covers it). `open_header` override (Task 1 Step 5) touches `_WindowedHeader` — implementer reads lines 206-259 first. `_shaped_result_row`'s exact metadata handling (Task 2 Step 3b) must be read before deciding whether an extra strip is needed there.
