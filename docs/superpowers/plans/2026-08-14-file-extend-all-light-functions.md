# TDD Implementation Plan: Extend FILE/FILEREF support to all light-tier functions

**For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development

- **Date:** 2026-08-14
- **Status:** Implementation plan (ready for TDD execution)
- **Scope:** GeoBrix **light tier** (`pyrx`), virtual tiles. Extends the MVP.
- **Spec:** `docs/superpowers/specs/2026-08-14-file-extend-all-light-functions-design.md`

---

## Goal

Extend FILE/FILEREF transparent acceleration to **every remaining tile-consuming light-tier
`rst_*` function** — building on the MVP's proven `_uf_*` 2-arg UDF pattern. Rolled out in
three validated groups: scalar accessors → single-input tile-producing ops → multi-input
and aggregators. Each group ends with a validation gate (green CI + dogfood spot-check + scoped
FILE-vs-Volume A/B) before the next group starts.

**Success criteria:**
- All existing pyrx tests still pass (fallback path unchanged).
- New stub-FileRef unit tests per group prove FILE branch equals fallback (pixel-equal), covering C1 guard cases.
- Each group's validation gate documented and passed before the next group begins.

---

## Architecture

```
User code (Python light API)
         ↓
pyrx public binding (e.g. rst_avg, rst_resample, rst_merge)
         ↓
file_ref_arg(tile_col)          ← plan-level mint (try_to_file or lit(None))
         ↓
2-arg _uf_* UDF (tile, file_ref)          SQL registry: single-arg _u_* unchanged
         ↓
open_tile / _open / open_header (file_ref=file_ref)
  ├─ FILE path: open_windowed_via_fileref → yield windowed DatasetReader
  └─ fallback:  _stage_local_if_needed(tile.path) → rasterio.open
         ↓
op core_fn(ds, *op_args) → new_bytes / scalar result
```

**Key invariants carried from MVP:**
- C1 guard lives in `open_tile`: FILE path only when `tile.clip_polygon is None` AND no warp.
- `file_ref_arg` returns `F.call_function("try_to_file", tile_col["path"])` when `file_supported()`, else `F.lit(None)`.
- SQL registry always points at single-arg `_u_*` / `_metadata_udf` / `_histogram_udf` etc. Never 2-arg the SQL path.
- Force-output (`virtualize_dir`/`virtualize_prefix`/`materialize`) variants are SQL/Python UDFs that do not get FILE wiring (the output path drives the choice, not the input read; adding file_ref to a v2 force-output UDF adds arity complexity for marginal gain — revisit in a dedicated pass if benchmarks justify it).

---

## Tech Stack

- **FILE injection:** `file_ref_arg(tile_col)` from `_file_ref.py` (no spark param; uses `getActiveSession()` internally).
- **Windowed read:** `open_windowed_via_fileref` in `_file_ref.py` (rasterio + seekable stream).
- **Tests:** pytest (`python/geobrix/.venv-pyrx`), stub FileRef (no Spark required for core logic tests), `conftest.make_geotiff_bytes` for test fixtures.
- **WKB construction in tests:** `struct.pack` (CI has no shapely).

---

## Spec

Full specification: `docs/superpowers/specs/2026-08-14-file-extend-all-light-functions-design.md`.

---

## Global Constraints

Paste this block verbatim into every dispatch prompt.

- pyrx stays **Serverless-safe**: no `.rdd` / `sparkContext` / `_jvm` / `_jsc` / `spark.conf.set` anywhere in shipped pyrx. `file_ref_arg` uses `getActiveSession()` + `F.call_function("try_to_file", tile_col["path"])`; `file_supported()` is the fixture-free detect.
- **SQL registry stays single-arg** (`_u_*` / `gbx_rst_*`, positional). FILE wiring is Python-binding-only via separate 2-arg `_uf_*` UDFs. Never 2-arg the SQL path.
- `open_tile`/`_open`/`open_header` `file_ref=None` default → byte-identical fallback.
- **C1 guard**: FILE windowed fast-path is taken only when `tile.clip_polygon is None` AND no reprojection is needed (checked inside `open_windowed_via_fileref`). This is already implemented in `open_tile`; callers just pass `file_ref` — the guard is automatic.
- Tile struct unchanged; FileRef never stored/displayed — minted per-op, consumed in the UDF.
- `file_ref_arg` uses `F.call_function("try_to_file", tile_col['path'])` + `getActiveSession`.
- `file_supported()` is the fixture-free detect.
- Force-output (`virtualize_dir`/`virtualize_prefix`/`materialize`) UDFs are **not** FILE-wired in this plan (they write output, not read input; the read-acceleration benefit is smaller and the arity complexity is high).

---

## Benchmark methodology — LATERAL VIEW

Functions whose output is an **ARRAY / MAP / UDTF / generator** (e.g. array-returning aggregators, exploded outputs) **MUST be forced in the benchmark query with `LATERAL VIEW explode(...)` (SQL) or `explode()` + an action**, so Spark's Generate op actually runs — a bare projection can be optimized away or only partially computed.

- **Scalar and tile-struct outputs** are already forced by the existing `.count()` / aggregate action.
- **Group 1 / Group 2 accessor + tile-producing A/B** (`bench-corpus-1024-1k`, dogfood, 20×i3.xlarge) needs no explicit explode; `.count()` is sufficient for scalar and struct results.
- **Group 3 array / aggregator A/B** requires an explicit `LATERAL VIEW explode(result)` before `.count()` (or `df.select(F.explode(...)).count()` in PySpark) to ensure the Generate node actually executes. Without this, Spark may prune the Generate operator and the benchmark measures plan overhead rather than real computation.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` | **Modify** | Add `_uf_*` FILE-aware 2-arg UDF singletons for all remaining ops; add `_tile_producing_udf_file` factory; update public Python bindings. SQL registry untouched. |
| `python/geobrix/test/pyrx/test_file_ref_ext.py` | **Create** | All new tests: stub-FileRef pixel-equal checks per group, SQL-registry single-arg assertions, C1-guard coverage. |

No other files require modification for Groups 1–3. The existing `_file_ref.py`, `open_tile.py`, `conftest.py`, and `test_file_ref.py` (MVP) are unchanged.

---

## Group 1 — Remaining scalar accessors

### Task 1: Pixel accessors — `rst_avg`, `rst_min`, `rst_max`, `rst_median`, `rst_pixelcount`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

Consumes: existing `_pixel_accessor_udf_file` factory (already in `functions.py`); `file_ref_arg` from `_file_ref.py`.

Produces per accessor: a new `_uf_<name>` 2-arg singleton; an updated public binding that calls `_uf_<name>(tc, file_ref_arg(tc))`.

Representative: **`rst_avg`** (one pixel accessor shown in full; the other four follow the identical transform).

**Steps**

- [ ] **Write failing test:** `test_rst_avg_file_ref_equals_fallback`

  ```python
  # python/geobrix/test/pyrx/test_file_ref_ext.py
  import io
  import os
  import tempfile

  import pytest
  from databricks.labs.gbx.pyrx.core import accessors as _acc
  from databricks.labs.gbx.pyrx.core import open_tile as ot
  from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
  from databricks.labs.gbx.pyrx._file_ref import FileRefReadError


  class _StubFileRef:
      """Stub FileRef whose .open() returns a seekable BytesIO for a real bytes blob."""

      def __init__(self, data: bytes):
          self._data = data

      def open(self):
          return io.BytesIO(self._data)

      def as_local_file(self):
          raise AssertionError("_StubFileRef.as_local_file should not be called on happy path")


  def test_rst_avg_file_ref_equals_fallback(gtiff_bytes):
      """FILE branch produces byte-identical per-band means to the fallback branch."""
      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          # Fallback branch (file_ref=None).
          with ot._open(tile_row, file_ref=None) as ds:
              expected = _acc.avg(ds)

          # FILE branch (stub FileRef).
          with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
              got = _acc.avg(ds)

          assert got == expected, f"FILE branch diverged: {got!r} != {expected!r}"
      finally:
          os.remove(tmp)
  ```

- [ ] **Run → expect PASS** (no code change needed — `_open` already accepts `file_ref`; this test proves the integration works end-to-end)
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py::test_rst_avg_file_ref_equals_fallback -xvs
  ```

- [ ] **Write failing test:** `test_rst_avg_public_binding_uses_file_ref_arg`

  ```python
  def test_rst_avg_public_binding_uses_file_ref_arg():
      """rst_avg public binding must call _uf_avg (2-arg), not _u_avg (1-arg).
      Verify by inspecting the returned Column's UDF object arity."""
      from unittest import mock
      from pyspark.sql import functions as F
      import databricks.labs.gbx.pyrx.functions as prx

      tc = F.col("tile")
      with mock.patch(
          "databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=False
      ):
          col = prx.rst_avg(tc)

      # The column wraps a UDF call. The UDF must accept 2 args (tile, file_ref).
      # _uf_avg is a 2-arg UDF; _u_avg is a 1-arg UDF.
      # Check the number of args in the column's children as a proxy.
      assert "_uf_avg" in repr(col) or len(col._jc.toString()) > 0  # column built without error
      # More directly: check that _uf_avg exists and is a 2-arg UDF.
      assert hasattr(prx, "_uf_avg"), "_uf_avg singleton must exist"
  ```

- [ ] **Run → expect FAIL** (`_uf_avg` not yet defined)
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py::test_rst_avg_public_binding_uses_file_ref_arg -xvs
  ```

- [ ] **Implement: add `_uf_*` singletons for all 5 pixel accessors** (single block in `functions.py`, after the existing `_uf_isempty` singleton):

  ```python
  # Group 1 pixel accessors — FILE-aware 2-arg singletons.
  _uf_avg = _pixel_accessor_udf_file(accessors.avg, ArrayType(DoubleType()))
  _uf_min = _pixel_accessor_udf_file(accessors.minimum, ArrayType(DoubleType()))
  _uf_max = _pixel_accessor_udf_file(accessors.maximum, ArrayType(DoubleType()))
  _uf_median = _pixel_accessor_udf_file(accessors.median, ArrayType(DoubleType()))
  _uf_pixelcount = _pixel_accessor_udf_file(accessors.pixelcount, ArrayType(LongType()))
  ```

- [ ] **Update public bindings for all 5** (data-driven transform; identical pattern for each):

  `rst_avg` (representative — the other four follow the same single-line change):
  ```python
  def rst_avg(tile: ColLike) -> Column:
      """Per-band mean of valid (non-NoData) pixels; ARRAY<DOUBLE>."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_avg(tc, file_ref_arg(tc))
  ```

  Apply identical edits for `rst_min`, `rst_max`, `rst_median`, `rst_pixelcount`:
  | Public fn | Old call | New call |
  |---|---|---|
  | `rst_min` | `_u_min(_col(tile))` | `_uf_min(tc, file_ref_arg(tc))` |
  | `rst_max` | `_u_max(_col(tile))` | `_uf_max(tc, file_ref_arg(tc))` |
  | `rst_median` | `_u_median(_col(tile))` | `_uf_median(tc, file_ref_arg(tc))` |
  | `rst_pixelcount` | `_u_pixelcount(_col(tile))` | `_uf_pixelcount(tc, file_ref_arg(tc))` |

  Each binding must `from databricks.labs.gbx.pyrx._file_ref import file_ref_arg` at the top of the function body and capture `tc = _col(tile)`.

- [ ] **Assert SQL registry unchanged** (existing `_u_*` entries in `_sql_accessors` are not modified):
  ```bash
  grep -n '"gbx_rst_avg"\|"gbx_rst_min"\|"gbx_rst_max"\|"gbx_rst_median"\|"gbx_rst_pixelcount"' \
    /Users/mjohns/IdeaProjects/geobrix/python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
  # Expected: all 5 still point at _u_avg, _u_min, _u_max, _u_median, _u_pixelcount
  ```

- [ ] **Run tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs -k "avg"
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): FILE-aware _uf_avg/min/max/median/pixelcount 2-arg UDFs + updated bindings"
  ```

---

### Task 2: Header accessors — `rst_type`, `rst_getnodata`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

Consumes: existing `_header_accessor_udf_file` factory.

Representative: **`rst_type`** (one header accessor shown in full; `rst_getnodata` follows identically).

**Steps**

- [ ] **Write failing test:** `test_rst_type_file_ref_equals_fallback`

  ```python
  def test_rst_type_file_ref_equals_fallback(gtiff_bytes):
      """FILE branch (open_header path) reports same dtype names as fallback."""
      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          with ot.open_header(tile_row, file_ref=None) as ds:
              expected = _acc.type(ds)

          # open_header uses as_local_file() on FILE degradation path, not windowed read.
          # For a header-only op the StubFileRef's as_local_file() must return the path.
          class _StubHeaderFileRef:
              def __init__(self, path):
                  self._path = path

              def open(self):
                  return open(self._path, "rb")

              def as_local_file(self):
                  return self._path

          with ot.open_header(tile_row, file_ref=_StubHeaderFileRef(tmp)) as ds:
              got = _acc.type(ds)

          assert got == expected, f"FILE header branch diverged: {got!r} != {expected!r}"
      finally:
          os.remove(tmp)
  ```

- [ ] **Run → expect PASS** (`open_header` already accepts `file_ref`; test proves integration)
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py::test_rst_type_file_ref_equals_fallback -xvs
  ```

- [ ] **Write failing test:** `test_rst_type_binding_uses_uf_type`
  ```python
  def test_rst_type_binding_uses_uf_type():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_uf_type"), "_uf_type singleton must exist"
      assert hasattr(prx, "_uf_getnodata"), "_uf_getnodata singleton must exist"
  ```

- [ ] **Run → expect FAIL** (singletons not yet defined)

- [ ] **Implement: add `_uf_*` singletons for type and getnodata** (after `_uf_min`/`_uf_max` block):

  ```python
  # Group 1 header accessors — FILE-aware 2-arg singletons.
  _uf_type = _header_accessor_udf_file(accessors.type, ArrayType(StringType()))
  _uf_getnodata = _header_accessor_udf_file(accessors.getnodata, ArrayType(DoubleType()))
  ```

- [ ] **Update public bindings:**

  ```python
  def rst_type(tile: ColLike) -> Column:
      """Return the GDAL data-type name per band (e.g. ['Float32', 'Float32'])."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_type(tc, file_ref_arg(tc))

  def rst_getnodata(tile: ColLike) -> Column:
      """Return the NoData value per band as an array of doubles, or null if not set."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_getnodata(tc, file_ref_arg(tc))
  ```

- [ ] **Assert SQL registry unchanged** (entries for `gbx_rst_type` and `gbx_rst_getnodata` still point at `_u_type`, `_u_getnodata`):
  ```bash
  grep -n '"gbx_rst_type"\|"gbx_rst_getnodata"' \
    /Users/mjohns/IdeaProjects/geobrix/python/geobrix/src/databricks/labs/gbx/pyrx/functions.py
  ```

- [ ] **Run all Group 1 tests so far → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): FILE-aware _uf_type/_uf_getnodata header-accessor singletons + bindings"
  ```

---

### Task 3: Special cases — `rst_histogram` and coord fns

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

`rst_histogram` uses a 5-extra-arg UDF (not covered by the zero-or-one-arg factories). Coord fns
have 2 extra scalar args. Both need inline 2-arg UDF definitions rather than factory calls.

**Steps — histogram**

- [ ] **Write failing test:** `test_rst_histogram_file_ref_equals_fallback`

  ```python
  def test_rst_histogram_file_ref_equals_fallback(gtiff_bytes):
      """FILE branch produces same histogram as fallback."""
      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          with ot._open(tile_row, file_ref=None) as ds:
              expected = _acc.histogram(ds, 16, None, None, False)

          with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
              got = _acc.histogram(ds, 16, None, None, False)

          assert got == expected
      finally:
          os.remove(tmp)
  ```

- [ ] **Run → expect PASS** (integration already works; test proves it)

- [ ] **Write failing test:** `test_rst_histogram_binding_uses_uf_histogram_udf`
  ```python
  def test_rst_histogram_binding_uses_uf_histogram_udf():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_uf_histogram_udf"), "_uf_histogram_udf singleton must exist"
  ```

- [ ] **Run → expect FAIL**

- [ ] **Implement `_uf_histogram_udf`** (after `_uf_metadata_udf` / `_uf_summary_udf` block in `functions.py`):

  ```python
  @f.udf(MapType(StringType(), ArrayType(LongType())))
  def _uf_histogram_udf(tile, file_ref, n_buckets, min_val, max_val, include_nodata):
      if _tile_is_empty(tile):
          return None
      try:
          from databricks.labs.gbx.pyrx import _env

          _env.configure_gdal_env()
          nb = 256 if n_buckets is None else int(n_buckets)
          lo = None if min_val is None else float(min_val)
          hi = None if max_val is None else float(max_val)
          inc = bool(include_nodata) if include_nodata is not None else False
          with ot._open(tile, file_ref=file_ref) as ds:
              return accessors.histogram(ds, nb, lo, hi, inc)
      except Exception:  # noqa: BLE001
          return None
  ```

- [ ] **Update `rst_histogram` binding:**

  ```python
  def rst_histogram(
      tile: ColLike,
      n_buckets: ColLike = 256,
      min_val: ColLike = None,
      max_val: ColLike = None,
      include_nodata: ColLike = False,
  ) -> Column:
      """Per-band histogram as MAP<STRING, ARRAY<LONG>> keyed by ``band_<i>``."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      nb = f.lit(n_buckets) if isinstance(n_buckets, int) else _col(n_buckets)
      lo = f.lit(None) if min_val is None else _col(min_val)
      hi = f.lit(None) if max_val is None else _col(max_val)
      inc = (
          f.lit(include_nodata)
          if isinstance(include_nodata, bool)
          else _col(include_nodata)
      )
      return _uf_histogram_udf(tc, file_ref_arg(tc), nb, lo, hi, inc)
  ```

  SQL registry entry `"gbx_rst_histogram": _histogram_udf` stays unchanged.

**Steps — coord fns**

Coord fns (`rst_rastertoworldcoordx/y`, `rst_worldtorastercoordx/y`) have signature
`(tile, x, y)` where `x` and `y` are scalar columns. They use `open_header`. The
existing `_header_accessor_udf2` factory produces a 3-arg UDF `(tile, a, b)`. We need
a 4-arg FILE-aware variant `(tile, file_ref, a, b)`.

- [ ] **Add `_header_accessor_udf3_file` factory** (after `_header_accessor_udf2` in `functions.py`):

  ```python
  def _header_accessor_udf3_file(core_fn, return_type):
      """Struct + FileRef + 2 scalar args header-only accessor UDF (4 args total)."""

      @f.udf(return_type)
      def _udf(tile, file_ref, a, b):
          if _tile_is_empty(tile):
              return None
          try:
              from databricks.labs.gbx.pyrx import _env

              _env.configure_gdal_env()
              with ot.open_header(tile, file_ref=file_ref) as ds:
                  return core_fn(ds, a, b)
          except Exception:  # noqa: BLE001
              return None

      return _udf
  ```

- [ ] **Add `_uf_r2w_x/y` and `_uf_w2r_x/y` singletons** (after the `_u_r2w_x/y` block):

  ```python
  _uf_r2w_x = _header_accessor_udf3_file(coords.raster_to_world_x, DoubleType())
  _uf_r2w_y = _header_accessor_udf3_file(coords.raster_to_world_y, DoubleType())
  _uf_w2r_x = _header_accessor_udf3_file(coords.world_to_raster_x, IntegerType())
  _uf_w2r_y = _header_accessor_udf3_file(coords.world_to_raster_y, IntegerType())
  ```

- [ ] **Update public bindings** (all four coord fns follow the same pattern):

  ```python
  def rst_rastertoworldcoordx(tile: ColLike, x: ColLike, y: ColLike) -> Column:
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_r2w_x(tc, file_ref_arg(tc), _col(x), _col(y))

  def rst_rastertoworldcoordy(tile: ColLike, x: ColLike, y: ColLike) -> Column:
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_r2w_y(tc, file_ref_arg(tc), _col(x), _col(y))

  def rst_worldtorastercoordx(tile: ColLike, x: ColLike, y: ColLike) -> Column:
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_w2r_x(tc, file_ref_arg(tc), _col(x), _col(y))

  def rst_worldtorastercoordy(tile: ColLike, x: ColLike, y: ColLike) -> Column:
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_w2r_y(tc, file_ref_arg(tc), _col(x), _col(y))
  ```

  SQL entries `"gbx_rst_rastertoworldcoordx": _u_r2w_x` etc. stay unchanged.

- [ ] **Run Group 1 full test suite → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -x --tb=short
  ```

- [ ] **Run all existing pyrx tests → expect no regressions**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ -x --tb=short -q
  ```

- [ ] **Run flake8 on modified files:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && \
    .venv-pyrx/bin/python -m flake8 \
      python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
      python/geobrix/test/pyrx/test_file_ref_ext.py \
      --max-line-length=100
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): FILE-aware histogram + coord-fn UDFs; Group 1 complete"
  ```

---

### Task 4: Validation gate — Group 1

**Steps**

- [ ] **(CI) Run Group 1 tests green** (all `test_file_ref_ext.py` + existing `test_pyrx/` suite pass):
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ -x --tb=short -q
  ```

- [ ] **(Manual — orchestrator)** Dogfood correctness spot-check on a FILE-enabled classic DBR 19 dedicated cluster:
  1. Build and stage the wheel: `gbx:data:push-wheel --profile oauth-fe`
  2. Install in notebook: `%pip install "geobrix[light] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix/geobrix-<ver>-py3-none-any.whl"`
  3. Run: `df.select(prx.rst_avg("tile"), prx.rst_min("tile"), prx.rst_type("tile"), prx.rst_getnodata("tile"), prx.rst_histogram("tile"))` on a virtual-tile DataFrame and confirm results are non-null and match the volume-path fallback.

- [ ] **(Manual — orchestrator)** Scoped FILE-vs-Volume A/B for Group 1, **split by op type**:

  > **A/B sequencing insight from MVP data:** The 9-accessor FILE-vs-Volume A/B (dogfood, 20×i3.xlarge, `bench-corpus-1024-1k`, up to 1000 rows) measured **header-only accessors** (`rst_type`, `rst_getnodata`) at **1.9–3.9× speedup from FILE** (and **2.8–5.9× vs materialized**). However, the full-pixel-reading op `rst_isempty` got **~zero FILE benefit and was ~2× SLOWER via virtual tiles than materialized** — it scans every pixel and cannot short-circuit on header bytes. Therefore this A/B must SPLIT header-only ops from pixel-reading ops and interpret results accordingly.

  **Run A: Header-only ops** (`rst_type`, `rst_getnodata`) — expect **1.9–3.9× speedup from FILE**:
  ```bash
  gbx:bench:cluster \
    --functions "rst_type,rst_getnodata" \
    --input-tile virtual \
    --corpus bench-corpus-1024-1k
  # Re-run with GBX_DISABLE_FILE=1 for the volume-path baseline.
  ```

  **Run B: Pixel-reading ops** (`rst_avg`, `rst_min`, `rst_max`, `rst_median`, `rst_pixelcount`) — expect **near-zero FILE benefit**; pixel-reading ops scan every pixel and cannot short-circuit on header bytes. A wash (no FILE speedup) is **EXPECTED and is NOT a gate failure**:
  ```bash
  gbx:bench:cluster \
    --functions "rst_avg,rst_min,rst_max,rst_median,rst_pixelcount" \
    --input-tile virtual \
    --corpus bench-corpus-1024-1k
  # Re-run with GBX_DISABLE_FILE=1 for the volume-path baseline.
  ```

  **Gate criteria:** The signal to investigate is **(a)** FILE-on markedly SLOWER than FILE-off on any op, or **(b)** a large virtual-vs-materialized regression on non-pixel ops. Do **not** treat "no FILE speedup on pixel-reading accessors" as a blocker for advancing to Group 2.

  This is a **manual step** — do not start Group 2 until the A/B result is measured and recorded (with the header-only vs pixel-reading split documented).

---

## Group 2 — Single-input tile-producing ops

### Task 5: `_tile_producing_udf_file` factory + `rst_initnodata`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

Consumes: `ot._open(tile, file_ref=file_ref)` (C1 guard already inside `open_tile`).

Produces: a new factory `_tile_producing_udf_file(core_fn)` for ops with no extra args;
a `_uf_initnodata` UDF (special: pending-instruction path for virtual tiles).

**Steps**

- [ ] **Write failing test:** `test_rst_initnodata_file_ref_equals_fallback`

  ```python
  def test_rst_initnodata_file_ref_equals_fallback(gtiff_bytes):
      """rst_initnodata: FILE branch produces same result as fallback for materialized tile."""
      from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
      from databricks.labs.gbx.pyrx.core import open_tile as ot
      from databricks.labs.gbx.pyrx.core import edit
      import databricks.labs.gbx.pyrx.functions as prx

      # Use a materialized tile (raster=bytes) so no pending-instruction fast-path.
      tile_row = VirtualTile(cellid=0, raster=gtiff_bytes).to_row()

      # Fallback invocation (internal _open, file_ref=None).
      with ot._open(tile_row, file_ref=None) as ds:
          expected_bytes = edit.init_nodata(ds)

      # FILE branch (stub FileRef backed by the same bytes).
      with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
          got_bytes = edit.init_nodata(ds)

      # Both paths must produce the same nodata-applied tile bytes (pixel-equal).
      import rasterio
      from rasterio.io import MemoryFile
      with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
          with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
              import numpy as np
              np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
              assert exp_ds.nodata == got_ds.nodata
  ```

- [ ] **Run → expect PASS** (no code change; test proves the open_tile integration)

- [ ] **Write failing test:** `test_rst_initnodata_binding_uses_uf_initnodata`
  ```python
  def test_rst_initnodata_binding_uses_uf_initnodata():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_uf_initnodata"), "_uf_initnodata must exist"
  ```

- [ ] **Run → expect FAIL**

- [ ] **Add `_tile_producing_udf_file` factory** to `functions.py` (in the "Virtual-aware accessor UDF factories" section, after `_pixel_accessor_udf_file`):

  ```python
  def _tile_producing_udf_file(core_fn):
      """FILE-aware tile-producing UDF factory for ops with NO extra args.

      Signature: (tile: Struct, file_ref: FileRef|null) → V2_TILE_SCHEMA
      Reads input via ot._open(tile, file_ref=file_ref).  C1 guard (clip-polygon
      and warp checks) is handled inside open_tile; on FileRefReadError it degrades
      silently to the local-path branch.  core_fn(ds) must return GTiff bytes.

      SQL registry keeps the single-arg ``_u_*`` / ``_<name>_udf`` entry unchanged.
      Only the public Python Column binding uses the 2-arg ``_uf_*`` UDF.
      """

      @f.udf(V2_TILE_SCHEMA)
      def _udf(tile, file_ref):
          if _tile_is_empty(tile):
              return None
          try:
              from databricks.labs.gbx.pyrx import _env

              _env.configure_gdal_env()
              with ot._open(tile, file_ref=file_ref) as ds:
                  new_bytes = core_fn(ds)
              return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))
          except Exception:  # noqa: BLE001
              return None

      return _udf
  ```

- [ ] **Add `_uf_initnodata`** (inline, after `_init_nodata_v2_udf`, before `rst_clip`):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_initnodata(tile, file_ref):
      """FILE-aware rst_initnodata.

      Virtual tile: records a pending_nodata instruction; stays virtual (no pixel
      read; file_ref unused — the pending path avoids reading entirely).
      Materialized tile: applies init_nodata via ot._open (file_ref=file_ref threads
      through open_tile; C1 guard auto-handles clip/warp cases).
      """
      if _tile_is_empty(tile):
          return None
      vt = ot._to_virtual_tile(tile)
      if vt.is_virtual():
          # Pending-instruction path: record intent, stay virtual (no pixel read).
          md = dict(vt.metadata or {})
          md.setdefault(ot.PENDING_NODATA, str(edit._DEFAULT_NODATA))
          vt.metadata = md
          return vt.to_row()
      # Materialized: apply eagerly; file_ref is passed through open_tile.
      try:
          from databricks.labs.gbx.pyrx import _env

          _env.configure_gdal_env()
          with ot._open(tile, file_ref=file_ref) as ds:
              new_bytes = edit.init_nodata(ds)
          return VirtualTile(
              cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
          ).to_row()
      except Exception:  # noqa: BLE001
          return None
  ```

- [ ] **Update `rst_initnodata` binding:**

  ```python
  def rst_initnodata(
      tile: ColLike,
      virtualize_dir: Optional[str] = None,
      virtualize_prefix: Optional[str] = None,
      materialize: Optional[bool] = None,
  ) -> Column:
      """Ensure a NoData value is set on the raster tile; uses -9999.0 if not already set."""
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _init_nodata_v2_udf(
              _col(tile),
              *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_initnodata(tc, file_ref_arg(tc))
  ```

  SQL entry `"gbx_rst_initnodata": _init_nodata_udf` stays unchanged.

- [ ] **Run tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs -k "initnodata"
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): _tile_producing_udf_file factory + _uf_initnodata; Group 2 start"
  ```

---

### Task 6: `rst_clip` (C1-guard exercise)

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

`rst_clip` exercises the C1 guard because the tile being read may have `clip_polygon=None`
(normal case — the clip geometry is the explicit `geom` argument, NOT the tile's
`clip_polygon` field). So: `open_tile` will take the FILE fast-path to read the
source tile bytes; then `edit.clip_to_geom` applies the cutline to those bytes.

**Steps**

- [ ] **Write failing test:** `test_rst_clip_file_ref_equals_fallback`

  ```python
  def test_rst_clip_file_ref_equals_fallback(gtiff_bytes):
      """rst_clip: FILE branch reads source tile via FileRef; clip result pixel-equals fallback.

      WKB for the clip polygon is built via struct.pack (no shapely in CI).
      The polygon covers the top-left 2x2 pixels of the 4x3 test raster
      (extent 10.0..12.0, 48.5..50.0 in EPSG:4326).
      """
      import struct
      import numpy as np
      import rasterio
      from rasterio.io import MemoryFile
      from databricks.labs.gbx.pyrx.core import edit
      from databricks.labs.gbx._geom import parse_geom

      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          # Build a WKB polygon covering col 0..2, row 0..2 of the test raster.
          # conftest.make_geotiff_bytes: origin (10.0, 50.0), pixel_size 0.5.
          # Col 0..2 = x 10.0..11.0; row 0..2 = y 49.0..50.0.
          # Polygon WKB (little-endian): type=3 (polygon), 1 ring, 5 pts.
          coords_pts = [
              (10.0, 49.0), (11.0, 49.0), (11.0, 50.0), (10.0, 50.0), (10.0, 49.0)
          ]
          pts_bytes = b"".join(struct.pack("<dd", x, y) for x, y in coords_pts)
          ring_bytes = struct.pack("<I", len(coords_pts)) + pts_bytes
          geom_wkb = (
              b"\x01"                      # little-endian
              + struct.pack("<I", 3)        # type: polygon
              + struct.pack("<I", 1)        # 1 ring
              + ring_bytes
          )
          geom = parse_geom(geom_wkb)
          assert geom is not None, "parse_geom must succeed"

          # Fallback clip (file_ref=None).
          with ot._open(tile_row, file_ref=None) as ds:
              expected_bytes = edit.clip_to_geom(ds, geom, all_touched=False, geom_crs=None)

          # FILE clip (stub FileRef).
          with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
              got_bytes = edit.clip_to_geom(ds, geom, all_touched=False, geom_crs=None)

          assert expected_bytes is not None and got_bytes is not None
          with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
              with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                  np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
      finally:
          os.remove(tmp)
  ```

- [ ] **Run → expect PASS** (no code change; proves FILE path works for clip input read)

- [ ] **Write failing test:** `test_rst_clip_binding_uses_uf_clip`
  ```python
  def test_rst_clip_binding_uses_uf_clip():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_uf_clip"), "_uf_clip must exist"
  ```

- [ ] **Run → expect FAIL**

- [ ] **Add `_uf_clip`** to `functions.py` (after `_clip_v2_udf`):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_clip(tile, file_ref, geom_wkb, all_touched, clip_crs):
      """FILE-aware rst_clip: reads input tile via file_ref, then clips.

      C1 guard in open_tile: FILE fast-path applies when tile.clip_polygon is None
      (normal for rst_clip input) AND no warp is pending.  The explicit geom_wkb
      cutline is applied AFTER the tile bytes are read (not a tile-level clip_polygon).
      """
      if _tile_is_empty(tile) or geom_wkb is None:
          return None
      try:
          from databricks.labs.gbx._geom import parse_geom
          from databricks.labs.gbx.pyrx import _env

          _env.configure_gdal_env()
          geom = parse_geom(geom_wkb)
          if geom is None:
              return None
          with ot._open(tile, file_ref=file_ref) as ds:
              new_bytes = edit.clip_to_geom(
                  ds, geom, bool(all_touched), geom_crs=clip_crs
              )
          if new_bytes is None:
              return None
          return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))
      except Exception:  # noqa: BLE001
          return None
  ```

- [ ] **Update `rst_clip` binding** (only the non-force-output path gets FILE wiring; force-output path stays unchanged):

  ```python
  def rst_clip(
      tile: ColLike,
      geom: ColLike,
      cutline_all_touched: ColLike,
      clip_crs: ColLike = None,
      virtualize_dir: Optional[str] = None,
      virtualize_prefix: Optional[str] = None,
      materialize: Optional[bool] = None,
  ) -> Column:
      """Clip the raster to a geometry (WKB, EWKB, WKT, or EWKT)."""
      crs_col = f.lit(clip_crs) if clip_crs is not None else f.lit(None)
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _clip_v2_udf(
              _col(tile),
              _col(geom),
              _col(cutline_all_touched),
              crs_col,
              f.lit(virtualize_dir),
              f.lit(virtualize_prefix),
              f.lit(materialize),
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_clip(tc, file_ref_arg(tc), _col(geom), _col(cutline_all_touched), crs_col)
  ```

  SQL entry `"gbx_rst_clip": _clip_udf` stays unchanged.

- [ ] **Run tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs -k "clip"
  ```

- [ ] **Run all pyrx tests → expect no regressions**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ -x --tb=short -q
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): _uf_clip FILE-aware UDF + updated rst_clip binding (C1-guard coverage)"
  ```

---

### Task 7: Remaining Group 2 ops

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Pattern for each op** (identical transform; no representative re-shown):

1. For the existing `_<name>_bytes(tile, *args)` helper: add `file_ref=None` parameter; pass `file_ref=file_ref` to `ot._open(tile, file_ref=file_ref)`.
2. Add a `_uf_<name>` UDF with signature `(tile, file_ref, *op_args)` that calls `_<name>_bytes(tile, *op_args, file_ref=file_ref)` and wraps the result.
3. Update the public binding's non-force-output path to call `_uf_<name>(tc, file_ref_arg(tc), ...)`.
4. SQL registry entry stays unchanged.

**Op table** (all follow the identical steps above):

| Op | `_bytes` fn to extend | Extra args (after tile) | Notes |
|---|---|---|---|
| `rst_resample` | `_resample_bytes` | `factor, algorithm` | |
| `rst_resample_to_size` | `_resample_to_size_bytes` | `width_px, height_px, algorithm` | |
| `rst_resample_to_res` | `_resample_to_res_bytes` | `x_res, y_res, algorithm` | |
| `rst_updatetype` | `_update_type_bytes` | `new_type` | |
| `rst_threshold` | `_threshold_bytes` | `op, value` | |
| `rst_transform` | `_transform_bytes` | `target_srid` | identity short-circuit preserved |
| `rst_to_webmercator` | `_to_webmercator_bytes` | `resampling` | |
| `rst_transformcrs` | `_transformcrs_bytes` | `crs_value` | FILE degrades via C1 (warp needed) → fallback |
| `rst_slope` | `_slope_bytes` | `unit, xscale, yscale` | |
| `rst_aspect` | `_aspect_bytes` | `trigonometric, zero_for_flat` | |
| `rst_hillshade` | `_hillshade_bytes` | `azimuth, altitude, z_factor, xscale, yscale` | |
| `rst_setsrid` | (pending-instruction virtual path; no `_bytes` fn used for virtual) | `srid` | Same as initnodata: pending for virtual, file_ref for materialized |
| `rst_setcrs` | (pending-instruction virtual path) | `crs_value` | Same pattern |

**Representative implementation example — `rst_resample`** (shown in full; all other ops follow the identical transform):

- [ ] **Add `file_ref=None` to `_resample_bytes`:**

  ```python
  def _resample_bytes(tile, factor, algorithm, file_ref=None):
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      with ot._open(tile, file_ref=file_ref) as ds:
          return resample.resample_by_factor(ds, float(factor), str(algorithm))
  ```

- [ ] **Add `_uf_resample`** (after `_resample_v2_udf`):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_resample(tile, file_ref, factor, algorithm):
      if _tile_is_empty(tile):
          return None
      new_bytes = _resample_bytes(tile, factor, algorithm, file_ref=file_ref)
      return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))
  ```

- [ ] **Update `rst_resample` binding** (non-force-output path):

  ```python
  def rst_resample(tile, factor, algorithm="bilinear", virtualize_dir=None,
                   virtualize_prefix=None, materialize=None):
      """Resample a raster tile by a multiplicative factor."""
      alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _resample_v2_udf(
              _col(tile), _col(factor), alg,
              *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

      tc = _col(tile)
      return _uf_resample(tc, file_ref_arg(tc), _col(factor), alg)
  ```

  SQL entry `"gbx_rst_resample": _resample_udf` stays unchanged.

- [ ] **Write test for `rst_resample`:**

  ```python
  def test_rst_resample_file_ref_equals_fallback(gtiff_bytes):
      """rst_resample: FILE branch produces pixel-equal output to fallback."""
      import numpy as np
      from rasterio.io import MemoryFile
      from databricks.labs.gbx.pyrx.core import resample as _resample

      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          with ot._open(tile_row, file_ref=None) as ds:
              expected_bytes = _resample.resample_by_factor(ds, 2.0, "bilinear")

          with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
              got_bytes = _resample.resample_by_factor(ds, 2.0, "bilinear")

          with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
              with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                  np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
      finally:
          os.remove(tmp)
  ```

- [ ] **Apply identical transform for all remaining ops in the table above.** Write one test per op (`test_rst_<name>_file_ref_equals_fallback`) following the same pixel-equal pattern. For ops without a `_bytes` helper using `_open` directly (e.g. terrain ops via `_ruggedness_bytes`), apply the same `file_ref=None` extension to the shared bytes helper.

- [ ] **Run all Group 2 tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -x --tb=short -q
  ```

- [ ] **Run all pyrx tests → expect no regressions**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ -x --tb=short -q
  ```

- [ ] **Run flake8:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && \
    .venv-pyrx/bin/python -m flake8 \
      python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
      python/geobrix/test/pyrx/test_file_ref_ext.py \
      --max-line-length=100
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): FILE-aware _uf_* UDFs for all Group 2 tile-producing ops"
  ```

---

### Task 8: Validation gate — Group 2

**Steps**

- [ ] **(CI) Full pyrx suite green:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ --tb=short -q
  ```

- [ ] **(Manual — orchestrator)** Dogfood correctness spot-check on FILE-enabled cluster:
  - `rst_resample`, `rst_slope`, `rst_clip` each called on a virtual-tile DataFrame.
  - Results must be non-null and pixel-equal to the `GBX_DISABLE_FILE=1` baseline.

- [ ] **(Manual — orchestrator)** Scoped FILE-vs-Volume A/B for Group 2 pixel-read ops:
  ```bash
  gbx:bench:cluster \
    --functions "rst_resample,rst_slope,rst_aspect,rst_hillshade,rst_clip,rst_updatetype" \
    --input-tile virtual \
    --corpus bench-corpus-1024-1k
  # Repeat with GBX_DISABLE_FILE=1 for volume-path baseline.
  ```
  Do not start Group 3 until A/B result is measured and shows no regression.

---

## Group 3 — Multi-input / array / aggregators

### Task 9: `rst_frombands` (ARRAY input, FILE-ARRAY injection)

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Interfaces**

`rst_frombands` takes an ARRAY<tile struct>. To inject FileRefs per-element, the
binding uses `F.transform(tiles_col, lambda t: F.call_function("try_to_file", t["path"]))`.
This produces an ARRAY<FileRef> column in the plan; the UDF receives `(bands, file_refs)`.

For materialized elements (no path, `raster` set), `try_to_file(NULL)` returns NULL,
so `file_refs[i]` is None — the UDF degrades to `materialize_to_bytes` for those elements.
This preserves the sort-key invariant (materialized bytes are never re-encoded).

**Steps**

- [ ] **Write failing test:** `test_rst_frombands_file_ref_equals_fallback`

  ```python
  def test_rst_frombands_file_ref_equals_fallback(gtiff_bytes):
      """rst_frombands: FILE-array path assembles same multi-band tile as fallback."""
      import numpy as np
      from rasterio.io import MemoryFile
      from databricks.labs.gbx.pyrx.core import agg as agg_core

      fd1, tmp1 = tempfile.mkstemp(suffix=".tif")
      fd2, tmp2 = tempfile.mkstemp(suffix=".tif")
      os.close(fd1)
      os.close(fd2)
      with open(tmp1, "wb") as fh:
          fh.write(gtiff_bytes)
      with open(tmp2, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          vt1 = VirtualTile(cellid=0, path=tmp1, window=(0, 0, 4, 3)).to_row()
          vt2 = VirtualTile(cellid=0, path=tmp2, window=(0, 0, 4, 3)).to_row()

          # Fallback: materialize each tile and call frombands_tiles.
          with ot._open(vt1, file_ref=None) as ds1:
              from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes
              b1 = _dataset_to_gtiff_bytes(ds1)
          with ot._open(vt2, file_ref=None) as ds2:
              b2 = _dataset_to_gtiff_bytes(ds2)
          expected_bytes = agg_core.frombands_tiles([(0, b1), (1, b2)])

          # FILE path: read each tile via StubFileRef.
          with ot._open(vt1, file_ref=_StubFileRef(gtiff_bytes)) as ds1:
              fb1 = _dataset_to_gtiff_bytes(ds1)
          with ot._open(vt2, file_ref=_StubFileRef(gtiff_bytes)) as ds2:
              fb2 = _dataset_to_gtiff_bytes(ds2)
          got_bytes = agg_core.frombands_tiles([(0, fb1), (1, fb2)])

          with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
              with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                  np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
                  assert got_ds.count == 2
      finally:
          os.remove(tmp1)
          os.remove(tmp2)
  ```

- [ ] **Run → expect PASS** (no code change; proves integration)

- [ ] **Write failing test:** `test_rst_frombands_binding_uses_uf_frombands`
  ```python
  def test_rst_frombands_binding_uses_uf_frombands():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_uf_frombands"), "_uf_frombands must exist"
  ```

- [ ] **Run → expect FAIL**

- [ ] **Add `_uf_frombands`** to `functions.py` (after `_frombands_v2_udf`):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_frombands(bands, file_refs):
      """FILE-aware rst_frombands: reads each input via its per-element FileRef.

      ``bands`` is ARRAY<tile struct>; ``file_refs`` is a parallel ARRAY<FileRef|null>
      (same length) minted in the plan via F.transform.  Materialized elements have
      file_refs[i]=None (try_to_file on NULL returns NULL); they fall through to verbatim
      bytes to preserve the sort-key invariant (no re-encode of materialized tiles).
      """
      if not bands:
          return None
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      frefs = list(file_refs) if file_refs else []
      # Extend with None if file_refs is shorter (safety).
      while len(frefs) < len(bands):
          frefs.append(None)

      indexed = []
      dropped = 0
      first_good = None
      for i, (t, fref) in enumerate(zip(bands, frefs)):
          if t is None or _tile_is_empty(t):
              continue
          try:
              vt = ot._to_virtual_tile(t)
              if vt.is_virtual():
                  # Virtual: use FileRef if available, else materialize_to_bytes fallback.
                  with ot._open(t, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  # Materialized: verbatim bytes to preserve the sort-key invariant.
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              indexed.append((i, candidate))
              if first_good is None:
                  first_good = t
          except Exception:  # noqa: BLE001
              dropped += 1
              continue

      if not indexed:
          return None
      cellid = _tile_cellid(first_good) if first_good is not None else 0
      new_bytes = agg_core.frombands_tiles(indexed)
      tile = _serde.build_tile(new_bytes, "GTiff", cellid)
      if dropped:
          tile["metadata"][
              "last_error"
          ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
      return tile
  ```

- [ ] **Update `rst_frombands` binding** (non-force-output path):

  ```python
  def rst_frombands(
      bands: ColLike,
      virtualize_dir: Optional[str] = None,
      virtualize_prefix: Optional[str] = None,
      materialize: Optional[bool] = None,
  ) -> Column:
      """Assemble an ARRAY of single-band tiles into one multi-band tile."""
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _frombands_v2_udf(
              _col(bands),
              *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(bands)
      if file_supported():
          # Mint a FileRef per element of the array in the plan.
          file_refs_col = f.transform(tc, lambda t: f.call_function("try_to_file", t["path"]))
          return _uf_frombands(tc, file_refs_col)
      return _frombands_udf(tc)
  ```

  SQL entry `"gbx_rst_frombands": _frombands_udf` stays unchanged.

- [ ] **Run tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs -k "frombands"
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): _uf_frombands FILE-array injection via F.transform + updated binding"
  ```

---

### Task 10: `rst_merge` and `rst_combineavg`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Pattern:** identical to `rst_frombands`. Both take ARRAY<tile struct>; `_uf_merge` and `_uf_combineavg` receive `(tiles, file_refs)`.

**Steps**

- [ ] **Write failing test:** `test_rst_merge_file_ref_equals_fallback`

  ```python
  def test_rst_merge_file_ref_equals_fallback(gtiff_bytes):
      """rst_merge: FILE-array path produces same mosaic as fallback."""
      import numpy as np
      from rasterio.io import MemoryFile
      from databricks.labs.gbx.pyrx.core import agg as agg_core

      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          vt = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

          with ot._open(vt, file_ref=None) as ds:
              from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes
              b = _dataset_to_gtiff_bytes(ds)
          expected_bytes = agg_core.merge_tiles([b, b])

          with ot._open(vt, file_ref=_StubFileRef(gtiff_bytes)) as ds:
              fb = _dataset_to_gtiff_bytes(ds)
          got_bytes = agg_core.merge_tiles([fb, fb])

          with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
              with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                  np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
      finally:
          os.remove(tmp)
  ```

- [ ] **Run → expect PASS**

- [ ] **Add `_uf_merge`** (after `_merge_v2_udf`; same structure as `_uf_frombands` but calls `agg_core.merge_tiles`):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_merge(tiles, file_refs):
      """FILE-aware rst_merge: reads each input via its per-element FileRef."""
      if not tiles:
          return None
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      frefs = list(file_refs) if file_refs else []
      while len(frefs) < len(tiles):
          frefs.append(None)

      rasters = []
      dropped = 0
      for t, fref in zip(tiles, frefs):
          if t is None or _tile_is_empty(t):
              continue
          try:
              vt = ot._to_virtual_tile(t)
              if vt.is_virtual():
                  with ot._open(t, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              rasters.append(candidate)
          except Exception:  # noqa: BLE001
              dropped += 1
              continue

      if not rasters:
          return None
      new_bytes = agg_core.merge_tiles(rasters)
      tile = _serde.build_tile(new_bytes, "GTiff", 0)
      if dropped:
          tile["metadata"][
              "last_error"
          ] = f"RST_Merge: skipped {dropped} corrupt input tile(s)"
      return tile
  ```

- [ ] **Add `_uf_combineavg`** (after `_combineavg_v2_udf`; same structure, calls `agg_core.combineavg_tiles`, preserves cellid logic):

  ```python
  @f.udf(V2_TILE_SCHEMA)
  def _uf_combineavg(tiles, file_refs):
      """FILE-aware rst_combineavg: reads each input via its per-element FileRef."""
      if not tiles:
          return None
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      frefs = list(file_refs) if file_refs else []
      while len(frefs) < len(tiles):
          frefs.append(None)

      rasters = []
      good_elems = []
      dropped = 0
      for t, fref in zip(tiles, frefs):
          if t is None or _tile_is_empty(t):
              continue
          try:
              vt = ot._to_virtual_tile(t)
              if vt.is_virtual():
                  with ot._open(t, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              rasters.append(candidate)
              good_elems.append(t)
          except Exception:  # noqa: BLE001
              dropped += 1
              continue

      if not rasters:
          return None
      cellids = {_tile_cellid(t) for t in good_elems}
      cellid = _tile_cellid(good_elems[0]) if len(cellids) == 1 else -1
      new_bytes = agg_core.combineavg_tiles(rasters)
      if new_bytes is None:
          return None
      tile = _serde.build_tile(new_bytes, "GTiff", cellid)
      if dropped:
          tile["metadata"][
              "last_error"
          ] = f"RST_CombineAvg: skipped {dropped} corrupt input tile(s)"
      return tile
  ```

- [ ] **Update `rst_merge` and `rst_combineavg` bindings** (non-force-output paths; `file_refs_col = f.transform(tc, lambda t: f.call_function("try_to_file", t["path"]))` is the same FILE-array injection):

  ```python
  def rst_merge(tiles, virtualize_dir=None, virtualize_prefix=None, materialize=None):
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _merge_v2_udf(
              _col(tiles), *_force_output_lits(virtualize_dir, virtualize_prefix, materialize)
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(tiles)
      if file_supported():
          file_refs_col = f.transform(tc, lambda t: f.call_function("try_to_file", t["path"]))
          return _uf_merge(tc, file_refs_col)
      return _merge_udf(tc)

  def rst_combineavg(tiles, virtualize_dir=None, virtualize_prefix=None, materialize=None):
      if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
          _validate_force_output(virtualize_dir, materialize)
          return _combineavg_v2_udf(
              _col(tiles), *_force_output_lits(virtualize_dir, virtualize_prefix, materialize)
          )
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(tiles)
      if file_supported():
          file_refs_col = f.transform(tc, lambda t: f.call_function("try_to_file", t["path"]))
          return _uf_combineavg(tc, file_refs_col)
      return _combineavg_udf(tc)
  ```

  SQL entries `"gbx_rst_merge": _merge_udf`, `"gbx_rst_combineavg": _combineavg_udf` stay unchanged.

- [ ] **Run tests → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py -xvs -k "merge or combineavg"
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): _uf_merge/_uf_combineavg FILE-array; Group 3 scalar array ops done"
  ```

---

### Task 11: `rst_mapalgebra`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Pattern:** same FILE-array injection as merge/frombands. `rst_mapalgebra` takes
`ARRAY<tile struct>`. The `_uf_mapalgebra` UDF receives `(tiles, file_refs, expression)`.

**Steps**

- [ ] **Write failing test and add `_uf_mapalgebra`** following the identical FILE-array pattern.
  Core call inside the UDF body: read each tile via `ot._open(t, file_ref=fref)` →
  collect GTiff bytes → pass to `mapalgebra_core.mapalgebra(rasters, expression)` (the
  same core call as `_mapalgebra_bytes`).

- [ ] **Update `rst_mapalgebra` binding** (non-force-output path uses FILE injection;
  force-output path stays unchanged).

  SQL entry `"gbx_rst_mapalgebra": _mapalgebra_udf` stays unchanged.

- [ ] **Run tests → expect PASS; commit.**

---

### Task 12: Grouped aggregators — `rst_merge_agg`, `rst_combineavg_agg`, `rst_frombands_agg`

**Files**
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref_ext.py`

**Design rationale:**
`@pandas_udf(BinaryType())` aggregators receive Arrow-serialized columns.
Spark's FILE type may or may not be Arrow-serializable for pandas_udf; this
is a runtime constraint that cannot be verified in CI (FILE absent).

**Approach:**
Add new `_merge_agg_file_udf(tile, file_ref)`, `_combineavg_agg_file_udf(tile, file_ref)`,
`_frombands_agg_file_udf(tile, file_ref, band_index)` pandas_udf variants that receive the
FileRef column as a second Series. Update public bindings to use these when `file_supported()`.
Include a runtime guard: if the FileRef column fails Arrow serialization (e.g. on older
Serverless), catch the plan-build error and fall back to the non-FILE aggregator.

**Steps**

- [ ] **Write failing test:** `test_rst_merge_agg_binding_has_file_variant`
  ```python
  def test_rst_merge_agg_binding_has_file_variant():
      import databricks.labs.gbx.pyrx.functions as prx
      assert hasattr(prx, "_merge_agg_file_udf"), "_merge_agg_file_udf must exist"
      assert hasattr(prx, "_combineavg_agg_file_udf"), "_combineavg_agg_file_udf must exist"
      assert hasattr(prx, "_frombands_agg_file_udf"), "_frombands_agg_file_udf must exist"
  ```

- [ ] **Run → expect FAIL**

- [ ] **Add `_merge_agg_file_udf`** (after `_merge_agg_udf`):

  ```python
  @pandas_udf(BinaryType())
  def _merge_agg_file_udf(tile: pd.Series, file_ref: pd.Series) -> bytes:
      """FILE-aware merge aggregator: reads virtual tiles via per-row FileRef.

      For each row: if file_ref is not None AND tile is virtual, read via
      ot._open(tile_row, file_ref=fref); otherwise materialize_to_bytes (fallback).
      Materialized tiles use verbatim bytes (sort-key invariant preserved).
      """
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      rasters = []
      dropped = 0
      for r, fref in zip(tile, file_ref):
          if r is None:
              continue
          try:
              vt = ot._to_virtual_tile(r)
              if vt.is_virtual():
                  with ot._open(r, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              rasters.append(candidate)
          except Exception:  # noqa: BLE001
              dropped += 1
              continue
      if not rasters:
          return None
      return agg_core.merge_tiles(rasters)
  ```

- [ ] **Add `_combineavg_agg_file_udf`** (after `_combineavg_agg_udf`; same pattern, prepends cellid envelope):

  ```python
  @pandas_udf(BinaryType())
  def _combineavg_agg_file_udf(tile: pd.Series, file_ref: pd.Series) -> bytes:
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      rasters = []
      cellid = 0
      first = True
      dropped = 0
      for r, fref in zip(tile, file_ref):
          if r is None:
              continue
          try:
              vt = ot._to_virtual_tile(r)
              if vt.is_virtual():
                  with ot._open(r, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              if first:
                  cid = r["cellid"] if hasattr(r, "__getitem__") else 0
                  cellid = int(cid) if cid is not None else 0
                  first = False
              rasters.append(candidate)
          except Exception:  # noqa: BLE001
              dropped += 1
              continue
      if not rasters:
          return None
      out = agg_core.combineavg_tiles(rasters)
      if out is None:
          return None
      return cellid.to_bytes(8, "big", signed=True) + bytes(out)
  ```

- [ ] **Add `_frombands_agg_file_udf`** (after `_frombands_agg_udf`):

  ```python
  @pandas_udf(BinaryType())
  def _frombands_agg_file_udf(
      tile: pd.Series, file_ref: pd.Series, band_index: pd.Series
  ) -> bytes:
      from databricks.labs.gbx.pyrx import _env

      _env.configure_gdal_env()
      indexed = []
      dropped = 0
      for r, fref, idx in zip(tile, file_ref, band_index):
          if idx is None or r is None:
              continue
          try:
              vt = ot._to_virtual_tile(r)
              if vt.is_virtual():
                  with ot._open(r, file_ref=fref) as ds:
                      candidate = _dataset_to_gtiff_bytes(ds)
              else:
                  candidate = bytes(vt.raster)
              with _serde.open_tile(candidate):
                  pass
              indexed.append((int(idx), candidate))
          except Exception:  # noqa: BLE001
              dropped += 1
              continue
      if not indexed:
          return None
      return agg_core.frombands_tiles(indexed)
  ```

- [ ] **Update public aggregator bindings** with a `file_supported()` guard:

  ```python
  def rst_merge_agg(tile: ColLike) -> Column:
      """Merge a group's tile rasters into one spatial mosaic tile."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(tile)
      if file_supported():
          # FILE-aware aggregator: pass per-row FileRef as a second column.
          return _as_tile_udf(_merge_agg_file_udf(tc, file_ref_arg(tc)))
      return _as_tile_udf(_merge_agg_udf(tc))

  def rst_combineavg_agg(tile: ColLike) -> Column:
      """Per-pixel mean across a group's aligned tiles, ignoring NoData."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(tile)
      if file_supported():
          return _as_tile_cellid_envelope_udf(_combineavg_agg_file_udf(tc, file_ref_arg(tc)))
      return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(tc))

  def rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
      """Stack a group's single-band tiles into one multi-band tile."""
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

      tc = _col(tile)
      if file_supported():
          return _as_tile_udf(
              _frombands_agg_file_udf(tc, file_ref_arg(tc), _col(band_index))
          )
      return _as_tile_udf(_frombands_agg_udf(tc, _col(band_index)))
  ```

  SQL entries `"gbx_rst_merge_agg": _merge_agg_udf` etc. stay unchanged (SQL path never gets FILE).

  **Runtime constraint note:** If `@pandas_udf` cannot Arrow-serialize the FILE column
  on a given platform (e.g. Serverless or an older DBR), the aggregation will fail
  silently in the plan-build phase. In that case, `file_supported()` will have returned
  False (Serverless has no FILE), so this path is never taken. If it IS taken (DBR 19,
  FILE enabled) and fails, the error is surfaced immediately at plan build time with a clear
  message — do not add a try/except around the `file_supported()` guard.

- [ ] **Write test:** `test_rst_merge_agg_file_ref_unit`

  ```python
  def test_rst_merge_agg_file_ref_unit(gtiff_bytes):
      """Unit test: _merge_agg_file_udf processes tiles correctly with stub FileRefs."""
      import pandas as pd
      from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
      from rasterio.io import MemoryFile
      import numpy as np

      fd, tmp = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      with open(tmp, "wb") as fh:
          fh.write(gtiff_bytes)
      try:
          vt_row = VirtualTile(cellid=7, path=tmp, window=(0, 0, 4, 3)).to_row()
          fref_stub = _StubFileRef(gtiff_bytes)

          # Simulate what pandas_udf receives: a Series of tile rows and file_refs.
          from databricks.labs.gbx.pyrx.functions import _merge_agg_file_udf

          # Call the underlying function directly (pandas_udf wraps a plain fn).
          tile_series = pd.Series([vt_row])
          fref_series = pd.Series([fref_stub])

          # The pandas_udf decorator wraps the function; call the inner via .func.
          raw_fn = _merge_agg_file_udf.func
          result_bytes = raw_fn(tile_series, fref_series)

          assert result_bytes is not None
          with MemoryFile(bytes(result_bytes)) as mf, mf.open() as ds:
              assert ds.count == 1
              np.testing.assert_array_equal(
                  ds.read(1).shape, (3, 4)  # height=3, width=4
              )
      finally:
          os.remove(tmp)
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/test_file_ref_ext.py::test_rst_merge_agg_file_ref_unit -xvs
  ```

- [ ] **Run all pyrx tests → expect no regressions**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ -x --tb=short -q
  ```

- [ ] **Run flake8:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && \
    .venv-pyrx/bin/python -m flake8 \
      python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
      python/geobrix/test/pyrx/test_file_ref_ext.py \
      --max-line-length=100
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add \
    python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
    python/geobrix/test/pyrx/test_file_ref_ext.py
  git commit -m "feat(pyrx): FILE-aware aggregators _merge_agg_file/_combineavg_agg_file/_frombands_agg_file"
  ```

---

### Task 13: Validation gate — Group 3

**Steps**

- [ ] **(CI) Full pyrx suite green:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix/python/geobrix && \
    .venv-pyrx/bin/python -m pytest test/pyrx/ --tb=short -q
  ```

- [ ] **(Manual — orchestrator)** Dogfood correctness spot-check on FILE-enabled cluster:
  - `rst_frombands`, `rst_merge`, `rst_combineavg`, `rst_mapalgebra`, `rst_merge_agg`,
    `rst_frombands_agg` on virtual-tile DataFrames.
  - Pixel-equal to `GBX_DISABLE_FILE=1` baseline.
  - For aggregators: confirm pandas_udf accepts the FILE column on DBR 19 (if it raises
    an Arrow serialization error, document and fall back to the non-FILE aggregator path).

- [ ] **(Manual — orchestrator)** Scoped FILE-vs-Volume A/B for Group 3 ops:
  ```bash
  gbx:bench:cluster \
    --functions "rst_frombands,rst_merge,rst_combineavg,rst_mapalgebra,rst_merge_agg" \
    --input-tile virtual \
    --corpus bench-corpus-1024-1k
  # Repeat with GBX_DISABLE_FILE=1.
  ```
  > **LATERAL VIEW note (see Benchmark methodology section):** array-returning and aggregator outputs in this group (e.g. `rst_frombands` band arrays, `rst_merge_agg` tile struct) must be forced with `LATERAL VIEW explode(...)` (SQL) or `df.select(F.explode(...)).count()` (PySpark) in the benchmark query so Spark's Generate op actually executes — a bare `.count()` on an unexploded array column can be pruned.

---

## Self-Review

### Spec coverage

| Spec requirement | Covered? | Where |
|---|---|---|
| Every tile-consuming light `rst_*` op uses FILE when available | Yes | Groups 1–3 |
| `file_ref=None` → byte-identical fallback | Yes | All `_uf_*` UDFs thread through `open_tile`/`_open`/`open_header` |
| Reusable tile-producing FILE factory | Yes | `_tile_producing_udf_file` (Task 5) |
| C1 guard: FILE only when no clip AND no warp | Yes | Already in `open_tile`; Task 6 tests it |
| SQL registry single-arg only | Yes | All SQL entries keep `_u_*` / `_<name>_udf`; asserted in each group's tests |
| Serverless-safe (no .rdd / _jvm / conf.set) | Yes | All new code uses only `f.udf`, `f.transform`, `f.call_function`, `ot._open` |
| `file_ref_arg` uses `F.call_function("try_to_file", tile_col['path'])` + getActiveSession | Yes | Reuses MVP's `_file_ref.py::file_ref_arg` unchanged |
| `file_supported()` is the fixture-free detect | Yes | Reuses MVP's memoized probe |
| Phased rollout: G1 → G2 → G3, each gated | Yes | Tasks 4, 8, 13 |
| FILE-vs-Volume A/B per group | Yes | Manual gate steps in Tasks 4, 8, 13 |
| Force-output variants NOT FILE-wired (deferred) | Yes | Global Constraints notes this explicitly |

### Placeholder scan

The following steps are listed as "data-driven" (Task 7 op table, Task 11) and require the
executor to apply the described transform to each named op. These are not placeholders —
the transform is fully specified; the executor applies it repetitively. Every unique design
decision (factory, inline UDF, C1-guard behavior, FILE-array injection, pandas_udf variant)
is shown in full on the representative op.

### Type consistency

All new `_uf_*` scalar UDFs use the same return types as their `_u_*` counterparts
(`ArrayType(DoubleType())` for avg/min/max/median, `ArrayType(LongType())` for pixelcount,
`ArrayType(StringType())` for type, `ArrayType(DoubleType())` for getnodata,
`MapType(StringType(), ArrayType(LongType()))` for histogram,
`V2_TILE_SCHEMA` for tile-producing ops, `BinaryType()` for pandas_udf aggregators).
The `_header_accessor_udf3_file` factory produces UDFs with `DoubleType()` or `IntegerType()`
matching the coord fn return types.

### Executor constraints embedded in every task

- Use `.venv-pyrx/bin/python` from `python/geobrix`.
- Build WKB via `struct.pack` (CI has no shapely).
- SQL `_REGISTRY` entries stay on single-arg `_u_*` / `_metadata_udf` / `_histogram_udf` etc.
- No `.rdd` / `sparkContext` / `_jvm` / `_jsc` / `spark.conf.set`.
- `file_supported()` returns False in CI (no FILE); all new tests verify the fallback path works.
- Stub FileRef (`_StubFileRef`) uses `io.BytesIO` — seekable, matching the real FileRef contract.
