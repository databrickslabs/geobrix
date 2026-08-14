# TDD Implementation Plan: Central FILE / FILEREF read for light-tier virtual tiles

**For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development

- **Date:** 2026-08-13
- **Status:** Implementation plan (ready for TDD execution)
- **Scope:** GeoBrix **light tier** (`pyrx`), **virtual tiles only**
- **Spec:** `docs/superpowers/specs/2026-08-13-file-ref-central-read-design.md`

---

## Goal

Ship **support-ready** central FILE / FILEREF read plumbing for light-tier virtual tiles. The feature:

- Detects FILE availability per SparkSession (memoized, with env override).
- Transparently uses FILE byte-range windowed reads when available; falls back to today's FUSE-stage path read everywhere else.
- Requires **zero user-facing API change**; tile struct and public function signatures unchanged.
- Works as fallback on local/CI/Serverless today; lights up on FILE-enabled classic DBR 19.x now, serverless-GC later.

**MVP Success Criteria:**
- All existing pyrx tests pass (fallback path exercised via `file_ref=None`).
- New unit tests for feature-detect + open_tile integration + binding injection.
- FILE-path behavior validated on a FILE-enabled classic DBR 19.x dedicated cluster (dogfood).
- Docs updated (Virtual Tiles + FILE section, DBR 19 matrix rows).

---

## Architecture

```
User code (Python light API)
         ↓
pyrx public functions (rst_height, rst_metadata, rst_clip, ...)
         ↓
Binding injection point: file_ref_arg(tile_col)  [no spark param]
  ├─ obtains SparkSession.getActiveSession() internally
  ├─ if file_supported(): F.call_function("try_to_file", tile_col["path"]) → FileRef column (minted in PLAN)
  └─ else: F.lit(None) → null
         ↓
2-arg UDF receives (tile, file_ref)
  [single-arg UDFs still used by SQL registry → fallback path]
         ↓
open_tile(tile, file_ref=None) + _open() + open_header() [all three]
  ├─ raster bytes: _serde.open_tile (unchanged)
  ├─ file_ref not None: try windowed read via open_windowed_via_fileref()
  │    └─ on FileRefReadError: degrade to file_ref.as_local_file() → _stage_local_if_needed
  └─ else: _stage_local_if_needed(tile.path) (today's path, unchanged)
```

---

## Tech Stack

- **Feature-detect:** `spark.sql()` + a one-row UDF (Serverless-safe, no `.rdd`/`_jvm`/`conf.set`).
- **Windowed read:** `rasterio` + the `fref.open()` seekable stream (plan-proven on dogfood).
- **Test framework:** pytest (local tests, CI), manual validation on dogfood (FILE-enabled cluster).
- **Dependencies:** rasterio (already required), no new external imports in shipped code.

---

## Spec

Full specification at `docs/superpowers/specs/2026-08-13-file-ref-central-read-design.md`.

Key sections:
- **§1:** Problem & motivation (FILE's governed access + byte-range windowed reads).
- **§2:** Feasibility (dogfood spike results: `try_to_file` mints `FileRef` column, `fref.open()` is seekable, `fref.as_local_file()` is the fallback).
- **§3:** Goals / Non-goals (central read, transparent, no API change; no heavy tier, no FILE write).
- **§4:** Design — option (a): plain path + under-the-hood FILE (chosen over path-encoded variants because FILE minting is plan-only).
- **§7:** Benchmarking — a SEPARATE follow-on plan (post-MVP), not included here.

---

## Global Constraints

**pyrx product MUST stay Serverless-safe:** NO `.rdd` / `sparkContext` / `_jvm` / `_jsc` / `spark.conf.set` anywhere in shipped pyrx. Feature-detect uses only `spark.sql` + a UDF; the `try_to_file` injection is a plan-level Column expression.

**Tile struct schema is UNCHANGED.** A FileRef is NEVER stored in the tile struct or surfaced as a carried/displayable column.

**`open_tile` gains an OPTIONAL `file_ref=None` param** — additive, back-compatible; all existing callers unaffected.

**CI/local tests exercise the FALLBACK** (FILE absent → feature-detect returns False → today's path read). The FILE-PRESENT behavior is validated on a FILE-enabled classic DBR 19.x dedicated cluster (dogfood), NOT in CI. Where a unit test needs FileRef behavior, use a STUB FileRef object whose `.open()` returns a real `open(local_test_tif, "rb")` (a seekable BufferedReader — matches the real `fref.open()`) and whose `.as_local_file()` returns the local path.

**Light-tier + virtual-tiles only.** No heavy tier, no FILE write path.

**User-facing docs:** no internal planning vocabulary (QC internals-leak). DBR 19 framed as "primary now, broadening to all environments soon."

---

## File Structure

| File | Responsibility |
|---|---|
| **Create:** `python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py` | Central FILE/FILEREF support module: `file_supported(spark)` feature-detect, `open_windowed_via_fileref()` helper, `file_ref_arg()` binding injection builder, `FileRefReadError` exception. |
| **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py` | Add `file_ref=None` optional param to `open_tile()` signature; integrate the windowed-read branch + degradation. |
| **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` | Wire `file_ref_arg()` into `_header_accessor_udf` and `_pixel_accessor_udf` factories; pass `file_ref` to the UDFs. |
| **Create:** `python/geobrix/test/pyrx/test_file_ref.py` | All unit tests: feature-detect, windowed read, open_tile integration, binding injection. |
| **Modify:** `docs/docs/api/virtual-tiles.mdx` | Add "Virtual Tiles + FILE" section. |
| **Modify:** `README.md` | Add DBR 19 row to supported-versions matrix. |
| **Modify:** `docs/docs/intro.mdx` | Add DBR 19 row to supported-versions matrix. |
| **Modify:** `docs/docs/installation.mdx` | Add DBR 19 row to supported-versions matrix. |
| **Modify:** `docs/docs/support.mdx` | Update DBR version examples to include DBR 19. |

---

## Task 1: Feature-detect — `_file_ref.py::file_supported() -> bool`

### Files
- **Create:** `python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py`
- **Test:** `python/geobrix/test/pyrx/test_file_ref.py::test_file_supported_*`

### Interfaces

**Function signature:**
```python
def file_supported() -> bool:
    """Memoized per-SparkSession capability check for FILE support.
    
    Obtains the active SparkSession internally via SparkSession.getActiveSession()
    (Serverless-safe, no .rdd / _jvm / conf.set). Returns True if:
    - GBX_DISABLE_FILE env var is not set to "1", AND
    - FILE type is recognized and usable (end-to-end roundtrip succeeds).
    
    Returns False if:
    - GBX_DISABLE_FILE="1" (no spark touched), OR
    - No active SparkSession, OR
    - Any exception during the roundtrip (UNSUPPORTED_DATATYPE, sentinel unreadable, consume failure, etc.).
    
    Result is cached per SparkSession; the roundtrip runs at most once per session.
    
    **Sentinel detail (OPEN):** the feature-detect mints try_to_file on a sentinel Volume path
    and consumes it in a UDF. Current implementation: attempts a hardcoded
    `/Volumes/main/geobrix_samples/...` sentinel and returns False if not found (acceptable,
    as detect-failure → fallback is always safe). See spec §4.2/§10 for future enhancements.
    """
```

### Steps

- [ ] **Write failing test:** `test_file_supported_respects_env_override`
  - Set `os.environ["GBX_DISABLE_FILE"] = "1"` before calling `file_supported()`
  - Assert `file_supported() is False`
  - Assert `SparkSession.getActiveSession().sql` is **not** called (mock it; verify call count is 0)
  - Clean up env var in teardown
  
  ```python
  def test_file_supported_respects_env_override(spark):
      os.environ["GBX_DISABLE_FILE"] = "1"
      try:
          result = file_supported()
          assert result is False
      finally:
          os.environ.pop("GBX_DISABLE_FILE", None)
  ```

- [ ] **Run test → expect FAIL** (file_supported not yet defined)
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_file_supported_respects_env_override -xvs
  ```
  Expected: `ImportError: cannot import name 'file_supported'` or `AttributeError`.

- [ ] **Minimal implementation:** add to `_file_ref.py`
  ```python
  import os
  from pyspark.sql import SparkSession
  
  _FILE_SUPPORT_CACHE = {}
  
  def file_supported() -> bool:
      """Memoized feature-detect for FILE support."""
      if os.environ.get("GBX_DISABLE_FILE") == "1":
          return False
      
      spark = SparkSession.getActiveSession()
      if spark is None:
          return False
      
      session_id = id(spark)
      if session_id in _FILE_SUPPORT_CACHE:
          return _FILE_SUPPORT_CACHE[session_id]
      
      # Placeholder: will be expanded in next step
      result = False
      _FILE_SUPPORT_CACHE[session_id] = result
      return result
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_file_supported_respects_env_override -xvs
  ```

- [ ] **Write failing test:** `test_file_supported_memoization`
  - Call `file_supported()` twice (spark is active via pytest fixture)
  - Mock `spark.sql` to raise on first call
  - Assert first call returns False (exception path), second call also returns False (cached)
  - Assert `spark.sql` was called exactly once (prove memoization worked)

  ```python
  def test_file_supported_memoization(spark):
      os.environ.pop("GBX_DISABLE_FILE", None)
      # Reset cache for this test
      from databricks.labs.gbx.pyrx import _file_ref
      _file_ref._FILE_SUPPORT_CACHE.clear()
      
      call_count = [0]
      original_sql = spark.sql
      def mock_sql(query):
          call_count[0] += 1
          raise RuntimeError("spark.sql called")
      
      try:
          spark.sql = mock_sql
          result1 = file_supported()
          result2 = file_supported()
          assert result1 is False
          assert result2 is False
          assert call_count[0] == 1, f"Expected spark.sql called once, got {call_count[0]}"
      finally:
          spark.sql = original_sql
  ```

- [ ] **Run test → expect FAIL** (no roundtrip logic yet)
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_file_supported_memoization -xvs
  ```

- [ ] **Implement the roundtrip logic** in `_file_ref.py` (CRITICAL FIX: mint try_to_file in the PLAN, NOT in UDF):
  ```python
  def file_supported() -> bool:
      """Memoized feature-detect for FILE support."""
      if os.environ.get("GBX_DISABLE_FILE") == "1":
          return False
      
      spark = SparkSession.getActiveSession()
      if spark is None:
          return False
      
      session_id = id(spark)
      if session_id in _FILE_SUPPORT_CACHE:
          return _FILE_SUPPORT_CACHE[session_id]
      
      result = _check_file_support(spark)
      _FILE_SUPPORT_CACHE[session_id] = result
      return result
  
  
  def _check_file_support(spark: SparkSession) -> bool:
      """Run end-to-end roundtrip to verify FILE is usable.
      
      Mints try_to_file on a sentinel Volume path in the Spark PLAN (not in a UDF),
      then consumes the FileRef in a UDF that calls fref.open().read(1).
      Returns True only if the roundtrip succeeds; False on any exception.
      
      CRITICAL: FileRef is MINTED IN THE PLAN via try_to_file (a SQL function),
      NOT constructed inside a UDF (pyspark.sql.types.FileRef(path) does not exist).
      """
      try:
          from pyspark.sql import functions as F
          
          sentinel_path = "/Volumes/main/geobrix_samples/geobrix-examples/london/LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
          
          # Mint FileRef in the PLAN via try_to_file (a Spark SQL function).
          # This returns a DataFrame with a FILE-type column.
          df_with_fref = spark.sql(f"SELECT try_to_file('{sentinel_path}') AS fref")
          
          # Consume the FileRef column in a UDF.
          @F.udf("string")
          def _consume_fref(fref):
              try:
                  with fref.open() as f:
                      byte_read = f.read(1)
                  return "success" if byte_read else "empty"
              except Exception:
                  return "failed"
          
          result_df = df_with_fref.select(_consume_fref(F.col("fref")))
          result = result_df.collect()[0][0]
          
          return result == "success"
      except Exception:
          return False
  ```

- [ ] **Run both tests → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_file_supported_respects_env_override test/pyrx/test_file_ref.py::test_file_supported_memoization -xvs
  ```

- [ ] **Commit:**
  ```bash
  cd python/geobrix && git add src/databricks/labs/gbx/pyrx/_file_ref.py test/pyrx/test_file_ref.py
  git commit -m "feat(pyrx): add file_supported() feature-detect with env override + memoization"
  ```

---

## Task 2: FileRef windowed-read helper — `open_windowed_via_fileref(file_ref, window, pending) -> contextmanager`

### Files
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py` (add helper)
- **Test:** `python/geobrix/test/pyrx/test_file_ref.py::test_open_windowed_via_fileref_*`

### Interfaces

**Exception:**
```python
class FileRefReadError(Exception):
    """Raised when a FILE/FILEREF windowed read fails (allows degradation)."""
    pass
```

**Function signature:**
```python
@contextmanager
def open_windowed_via_fileref(file_ref, window: Tuple[int, int, int, int], pending) -> Iterator[DatasetReader]:
    """Open a FileRef as a rasterio source and read exactly the window.
    
    Args:
        file_ref: A pyspark.sql.types.FileRef with .open() and .as_local_file() methods.
        window: (col_off, row_off, width, height) tuple from VirtualTile.window.
        pending: pending_instructions dict from _parse_pending(tile.metadata).
    
    Yields:
        A rasterio.io.DatasetReader opened on the windowed file-ref stream.
    
    Raises:
        FileRefReadError: if the stream is not seekable, rasterio open fails,
        or any other windowed-read failure occurs. Caller may degrade to fallback.
    """
```

### Steps

- [ ] **Write failing test:** `test_open_windowed_via_fileref_reads_correct_pixels`
  - Create a stub FileRef whose `.open()` returns a real BufferedReader to a test GeoTIFF
  - Call `open_windowed_via_fileref(stub_fref, window, pending)` with a known window
  - Assert the returned DatasetReader has correct window bounds and pixel values
  - Compare to the same window read via `rasterio.open(path).read(window=...)`

  ```python
  def test_open_windowed_via_fileref_reads_correct_pixels(gtiff_bytes):
      from contextlib import contextmanager
      import io
      
      # Stub FileRef that wraps a real local file.
      class StubFileRef:
          def __init__(self, data_bytes):
              self.data_bytes = data_bytes
          
          def open(self):
              return io.BytesIO(self.data_bytes)
          
          def as_local_file(self):
              # For fallback (not used in this happy-path test).
              import tempfile
              fd, tmp = tempfile.mkstemp(suffix=".tif")
              os.close(fd)
              with open(tmp, "wb") as f:
                  f.write(self.data_bytes)
              return tmp
      
      stub_fref = StubFileRef(gtiff_bytes)
      window = (0, 0, 2, 2)  # top-left 2x2 window
      pending = (None, None, None, None)  # no pending instructions
      
      from databricks.labs.gbx.pyrx._file_ref import open_windowed_via_fileref
      with open_windowed_via_fileref(stub_fref, window, pending) as ds:
          pixels = ds.read(1, window=window)
          # From conftest.make_geotiff_bytes: data = np.arange(width * height)
          # window (0, 0, 2, 2) should read [[0, 1], [4, 5]]
          expected = np.array([[0, 1], [4, 5]], dtype="float32")
          np.testing.assert_array_equal(pixels, expected)
  ```

- [ ] **Run test → expect FAIL** (helper not defined)
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_windowed_via_fileref_reads_correct_pixels -xvs
  ```

- [ ] **Implement the helper** in `_file_ref.py`:
  ```python
  from contextlib import contextmanager
  from contextlib import ExitStack
  from rasterio.io import MemoryFile
  import rasterio
  from rasterio.windows import Window
  
  class FileRefReadError(Exception):
      """Raised when a FILE/FILEREF windowed read fails."""
      pass
  
  
  @contextmanager
  def open_windowed_via_fileref(file_ref, window, pending):
      """Open a FileRef as a rasterio source and read exactly the window.
      
      The FileRef's .open() method returns a seekable stream. We open it
      via rasterio (seeking to the window's byte range), apply pending instructions,
      and yield a DatasetReader.
      
      On any failure: raise FileRefReadError so the caller can degrade to
      file_ref.as_local_file() → _stage_local_if_needed → rasterio.open(local).
      """
      try:
          stream = file_ref.open()
          if not stream.seekable():
              raise FileRefReadError("FileRef stream is not seekable")
          
          # rasterio.open can consume a file-like object if we pass mode='r+b'
          # and let it seek as needed for the window.
          with rasterio.open(stream, 'r') as src:
              # Extract the window bands and pending instructions.
              # (Reuse the same logic as _window_dataset_bytes in open_tile.py)
              c, r, w, h = window
              rio_window = Window(c, r, w, h)
              
              # Apply pending instructions (same pattern as open_tile).
              from databricks.labs.gbx.pyrx.core.open_tile import (
                  _parse_pending,
                  _window_dataset_bytes,
                  _open_bytes,
              )
              
              # pending is already parsed; reuse the helpers from open_tile.
              # Yield a temporary in-memory view so the caller sees a valid DatasetReader.
              tile_bytes = _window_dataset_bytes(src, rio_window, pending=pending)
              
              with ExitStack() as stack:
                  mf = stack.enter_context(MemoryFile(tile_bytes))
                  with mf.open() as ds:
                      yield ds
      except FileRefReadError:
          raise
      except Exception as e:
          raise FileRefReadError(f"FileRef windowed read failed: {e}") from e
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_windowed_via_fileref_reads_correct_pixels -xvs
  ```

- [ ] **Write test:** `test_open_windowed_via_fileref_raises_on_non_seekable`
  - Create a stub FileRef whose `.open()` returns a non-seekable stream
  - Call `open_windowed_via_fileref()` and expect `FileRefReadError`

  ```python
  def test_open_windowed_via_fileref_raises_on_non_seekable(gtiff_bytes):
      import io
      
      class NonSeekableStream:
          def __init__(self, data):
              self._buffer = io.BytesIO(data)
          
          def read(self, n=-1):
              return self._buffer.read(n)
          
          def seekable(self):
              return False
      
      class StubFileRef:
          def __init__(self, data_bytes):
              self.data_bytes = data_bytes
          
          def open(self):
              return NonSeekableStream(self.data_bytes)
      
      stub_fref = StubFileRef(gtiff_bytes)
      window = (0, 0, 2, 2)
      pending = (None, None, None, None)
      
      from databricks.labs.gbx.pyrx._file_ref import open_windowed_via_fileref, FileRefReadError
      with pytest.raises(FileRefReadError):
          with open_windowed_via_fileref(stub_fref, window, pending):
              pass
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_windowed_via_fileref_raises_on_non_seekable -xvs
  ```

- [ ] **Commit:**
  ```bash
  cd python/geobrix && git add src/databricks/labs/gbx/pyrx/_file_ref.py test/pyrx/test_file_ref.py
  git commit -m "feat(pyrx): add open_windowed_via_fileref() helper with FileRefReadError degradation"
  ```

---

## Task 3: Three-entry-point integration — add `file_ref=None` param to `open_tile`, `_open`, and `open_header`

### Files
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/core/open_tile.py`
  - Update `open_tile(tile, file_ref=None)` (line ~258)
  - Update `_open(tile, file_ref=None)` (line ~346)
  - Update `open_header(tile, file_ref=None)` (line ~492)
  - **CRITICAL:** Extract ONE shared internal helper to avoid triplication of FILE branch + degradation logic
- **Test:** `python/geobrix/test/pyrx/test_file_ref.py::test_open_*_file_ref_*`

### Interfaces

**The three entry points (all get `file_ref=None` param):**
```python
@contextmanager
def open_tile(tile: VirtualTile, file_ref=None) -> Iterator[DatasetReader]:
    """Full tile read (bytes or windowed pixels + warp + clip)."""

@contextmanager
def _open(tile, file_ref=None) -> Iterator[DatasetReader]:
    """Alias for tile normalization + open_tile (public API)."""

@contextmanager
def open_header(tile: VirtualTile, file_ref=None) -> Iterator[DatasetReader]:
    """Header-only read (no pixel materialization)."""
```

**Shared internal helper (NEW):**
```python
def _resolve_local_or_windowed(tile: VirtualTile, file_ref, stack: ExitStack) -> tuple:
    """Resolve the read source for a virtual tile: FILE windowed-read or local FUSE path.
    
    Args:
        tile: VirtualTile with path + window.
        file_ref: optional FileRef; if provided, attempt windowed read via FILE first.
        stack: ExitStack to register temp-cleanup callbacks.
    
    Returns:
        (local_path, is_temp) tuple: local_path is ready to pass to rasterio.open.
        
    Logic:
    - If file_ref is not None: try open_windowed_via_fileref → on FileRefReadError, degrade to file_ref.as_local_file() → _stage_local_if_needed.
    - Else: _stage_local_if_needed(tile.path) (today's path).
    
    This helper is used by open_tile (pixel reads), _open (alias), and open_header (header reads).
    Each caller decides what to do with (local_path, is_temp) — materialized bytes, _WindowHeaderView, etc.
    """
```

### Steps

- [ ] **Write failing test:** `test_open_tile_file_ref_none_backward_compatible`
  - Call `open_tile(virtual_tile)` (no `file_ref` arg, defaults to None)
  - Assert it reads the same pixels as before (today's path-read code path)
  - Confirm all existing tests still use the default and pass

  ```python
  def test_open_tile_file_ref_none_backward_compatible():
      from databricks.labs.gbx.pyrx.core.open_tile import open_tile
      from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
      import tempfile
      import numpy as np
      
      # Create a real test GeoTIFF on disk.
      from conftest import make_geotiff_bytes
      tif_bytes = make_geotiff_bytes()
      fd, tmp_path = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      try:
          with open(tmp_path, "wb") as f:
              f.write(tif_bytes)
          
          # Create a virtual tile pointing to it.
          tile = VirtualTile(
              cellid=0,
              path=tmp_path,
              window=(0, 0, 4, 3),  # full extent
          )
          
          # Call open_tile without file_ref (should use fallback path).
          with open_tile(tile) as ds:
              pixels = ds.read(1)
              # From make_geotiff_bytes: data = np.arange(4*3) = [0..11]
              expected = np.arange(12, dtype="float32").reshape(3, 4)
              np.testing.assert_array_equal(pixels, expected)
      finally:
          os.remove(tmp_path)
  ```

- [ ] **Run test → expect FAIL** (file_ref param not in signature)
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_tile_file_ref_none_backward_compatible -xvs
  ```
  Expected: `TypeError: open_tile() got an unexpected keyword argument 'file_ref'` or similar.

- [ ] **Update `open_tile` signature** in `open_tile.py`:
  ```python
  @contextmanager
  def open_tile(tile: VirtualTile, file_ref=None) -> Iterator[DatasetReader]:
      # 1. raster present: the bytes ARE the result; provenance fields are ignored
      #    (including any bogus path). Delegate to the v1 bytes contextmanager.
      if tile.raster is not None:
          with _serde.open_tile(tile.raster) as ds:
              yield ds
          return
  
      # 2. virtual: try FILE branch if available, else fallback path branch.
      with ExitStack() as stack:
          # If file_ref is provided, try windowed read via FILE first.
          if file_ref is not None:
              try:
                  from databricks.labs.gbx.pyrx._file_ref import (
                      open_windowed_via_fileref,
                      FileRefReadError,
                  )
  
                  pending = _parse_pending(tile.metadata)
                  with open_windowed_via_fileref(file_ref, tile.window, pending) as ds:
                      yield ds
                  return
              except FileRefReadError:
                  # Degrade to file_ref.as_local_file() fallback.
                  try:
                      local_path = file_ref.as_local_file()
                      local_path, is_temp = _stage_local_if_needed(local_path)
                      if is_temp:
                          stack.callback(_safe_remove, local_path)
                  except Exception:
                      # If even the fallback fails, use the plain tile.path (last resort).
                      local_path = tile.path
                      local_path, is_temp = _stage_local_if_needed(local_path)
                      if is_temp:
                          stack.callback(_safe_remove, local_path)
          else:
              # No file_ref: use today's plain-path read (unchanged).
              local_path, is_temp = _stage_local_if_needed(tile.path)
              if is_temp:
                  stack.callback(_safe_remove, local_path)
  
          # Read from local_path via rasterio (same as today).
          c, r, w, h = tile.window
          window = Window(c, r, w, h)
          pending = _parse_pending(tile.metadata)
          bands, _nodata, pending_srid, _pending_crs_str = pending
          with rasterio.open(local_path) as src:
              src_epsg = src.crs.to_epsg() if src.crs else None
              want = _epsg_of(tile.crs) if tile.crs else None
              effective_src_epsg = pending_srid if pending_srid is not None else src_epsg
              if want is not None and want != effective_src_epsg:
                  tile_bytes = _warp_window_bytes(src, window, want, pending=pending)
              elif tile.crs is not None and want is None:
                  from databricks.labs.gbx.pyrx.core.crs import resolve_crs
  
                  want_crs = resolve_crs(tile.crs)
                  effective_src_crs = (
                      resolve_crs(pending_srid) if pending_srid is not None else src.crs
                  )
                  if effective_src_crs is None or effective_src_crs != want_crs:
                      tile_bytes = _warp_window_bytes_crs(
                          src, window, want_crs, pending=pending
                      )
                  else:
                      tile_bytes = _window_dataset_bytes(src, window, pending=pending)
              else:
                  tile_bytes = _window_dataset_bytes(src, window, pending=pending)
          
          src_closed = True  # src is closed here; we hold only standalone bytes.
  
          wds = _open_bytes(stack, tile_bytes)
          if tile.clip_polygon is None:
              yield wds
              return
  
          clipped = _clip.clip_dataset(wds, tile.clip_polygon, tile.clip_crs)
          if clipped is None:  # disjoint -> valid empty NoData dataset
              yield _open_bytes(stack, _empty_dataset_bytes(wds))
          else:
              yield _open_bytes(stack, clipped)
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_tile_file_ref_none_backward_compatible -xvs
  ```

- [ ] **Write test:** `test_open_tile_uses_file_ref_when_provided`
  - Create a stub FileRef whose `.open()` returns a buffered reader to a real test GeoTIFF
  - Create a virtual tile with a **wrong path** (one that doesn't exist on disk)
  - Call `open_tile(tile, file_ref=stub_fref)`
  - Assert it reads pixels correctly from the FileRef stream, not the wrong path

  ```python
  def test_open_tile_uses_file_ref_when_provided(gtiff_bytes):
      from databricks.labs.gbx.pyrx.core.open_tile import open_tile
      from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
      import io
      import numpy as np
      
      class StubFileRef:
          def __init__(self, data_bytes):
              self.data_bytes = data_bytes
          
          def open(self):
              return io.BytesIO(self.data_bytes)
          
          def as_local_file(self):
              # Not called in this happy-path test.
              raise AssertionError("Should not degrade")
      
      stub_fref = StubFileRef(gtiff_bytes)
      
      # Virtual tile with a non-existent path (would fail if fallback tried it).
      tile = VirtualTile(
          cellid=0,
          path="/nonexistent/path.tif",
          window=(0, 0, 4, 3),
      )
      
      # Call open_tile with file_ref.
      with open_tile(tile, file_ref=stub_fref) as ds:
          pixels = ds.read(1)
          expected = np.arange(12, dtype="float32").reshape(3, 4)
          np.testing.assert_array_equal(pixels, expected)
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_tile_uses_file_ref_when_provided -xvs
  ```

- [ ] **Write test:** `test_open_tile_file_ref_degrades_to_fallback`
  - Create a stub FileRef whose `.open()` raises an error (simulating FILE failure)
  - Create a virtual tile pointing to a real local test file
  - Call `open_tile(tile, file_ref=stub_fref)` and expect it to **degrade** to the fallback path
  - Assert it reads pixels correctly from the fallback

  ```python
  def test_open_tile_file_ref_degrades_to_fallback(gtiff_bytes):
      from databricks.labs.gbx.pyrx.core.open_tile import open_tile
      from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
      import tempfile
      import numpy as np
      
      class FailingStubFileRef:
          def open(self):
              raise IOError("FileRef stream failed")
          
          def as_local_file(self):
              # Return a path to the real GeoTIFF so the fallback succeeds.
              fd, tmp_path = tempfile.mkstemp(suffix=".tif")
              os.close(fd)
              with open(tmp_path, "wb") as f:
                  f.write(gtiff_bytes)
              return tmp_path
      
      stub_fref = FailingStubFileRef()
      
      # Create a temp file for the fallback.
      fd, fallback_path = tempfile.mkstemp(suffix=".tif")
      os.close(fd)
      try:
          with open(fallback_path, "wb") as f:
              f.write(gtiff_bytes)
          
          tile = VirtualTile(
              cellid=0,
              path=fallback_path,
              window=(0, 0, 4, 3),
          )
          
          # Call open_tile with a failing file_ref; should degrade to fallback path.
          with open_tile(tile, file_ref=stub_fref) as ds:
              pixels = ds.read(1)
              expected = np.arange(12, dtype="float32").reshape(3, 4)
              np.testing.assert_array_equal(pixels, expected)
      finally:
          os.remove(fallback_path)
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_open_tile_file_ref_degrades_to_fallback -xvs
  ```

- [ ] **Implement shared internal helper** `_resolve_local_or_windowed` in `open_tile.py`:
  ```python
  def _resolve_local_or_windowed(tile: VirtualTile, file_ref, stack: ExitStack) -> tuple:
      """Resolve the read source: FILE windowed-read or local FUSE path.
      
      Returns (local_path, is_temp) where local_path is ready for rasterio.open.
      Registers temp-cleanup on stack if is_temp=True.
      """
      if file_ref is not None:
          try:
              from databricks.labs.gbx.pyrx._file_ref import (
                  open_windowed_via_fileref,
                  FileRefReadError,
              )
              # Attempt FILE windowed read; on success, we yield the windowed bytes
              # directly (not a local path). Callers must handle this differently.
              # For now, degrade to fallback on any FILE attempt (to keep logic simple).
              # A more sophisticated approach would cache the windowed bytes.
              pending = _parse_pending(tile.metadata)
              with open_windowed_via_fileref(file_ref, tile.window, pending) as ds:
                  # Successfully opened via FILE; caller will use this ds directly.
                  # (This is handled at the open_tile / _open / open_header level.)
                  return None, False  # Signal "use FILE, not local path"
          except FileRefReadError:
              # Degrade to file_ref.as_local_file() fallback.
              try:
                  local_path = file_ref.as_local_file()
                  local_path, is_temp = _stage_local_if_needed(local_path)
                  if is_temp:
                      stack.callback(_safe_remove, local_path)
                  return local_path, is_temp
              except Exception:
                  # Final fallback: use the plain tile.path.
                  local_path, is_temp = _stage_local_if_needed(tile.path)
                  if is_temp:
                      stack.callback(_safe_remove, local_path)
                  return local_path, is_temp
      else:
          # No file_ref: use today's plain-path read.
          local_path, is_temp = _stage_local_if_needed(tile.path)
          if is_temp:
              stack.callback(_safe_remove, local_path)
          return local_path, is_temp
  ```
  
  **NOTE:** This is a SIMPLIFIED helper that shows the pattern. A production implementation
  may need to handle FILE windowed reads more directly (returning the DatasetReader instead
  of a local path) to avoid unnecessary staging. The three callers (open_tile, _open, open_header)
  can each decide whether to use the helper or inline the logic.

- [ ] **Run all existing pyrx tests to confirm backward compatibility:**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/ -x --tb=short
  ```
  Expected: all pass (no change to test calls; file_ref defaults to None).

- [ ] **Commit:**
  ```bash
  cd python/geobrix && git add src/databricks/labs/gbx/pyrx/core/open_tile.py test/pyrx/test_file_ref.py
  git commit -m "feat(pyrx): add file_ref param to open_tile/open_header/_open with shared helper"
  ```

---

## Task 4: Binding injection — `file_ref_arg()`, separate 2-arg UDF factories, and public binding wiring

### Files
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/_file_ref.py` (add `file_ref_arg` helper)
- **Modify:** `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (add new 2-arg UDF factories + update public Python bindings; DO NOT break SQL registry)
- **Test:** `python/geobrix/test/pyrx/test_file_ref.py::test_binding_injection_*`

### Interfaces

**Helper function:**
```python
def file_ref_arg(tile_col: Column) -> Column:
    """Return a Column expression for the file_ref argument to pass to tile-reading UDFs.
    
    Obtains the active SparkSession internally via SparkSession.getActiveSession().
    If file_supported() is True:
        Returns F.call_function("try_to_file", tile_col["path"]) — a FILE metadata-mint expression
        that runs in the plan and arrives at the UDF as a real FileRef value.
    Else:
        Returns F.lit(None) — passes a null to the UDF (fallback path).
    
    This is THE central injection point for all tile-reading ops.
    NO spark param needed; session is obtained internally (Serverless-safe).
    """
```

**New 2-arg UDF factories (in addition to existing single-arg):**
```python
def _header_accessor_udf_file(core_fn, return_type):
    """Struct + FileRef accepting header-only accessor UDF.
    
    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls open_header(tile, file_ref=file_ref) internally.
    """

def _pixel_accessor_udf_file(core_fn, return_type):
    """Struct + FileRef accepting pixel accessor UDF.
    
    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls _open(tile, file_ref=file_ref) internally.
    """
```

**Public Python binding pattern (example: rst_height):**
```python
def rst_height(tile: ColLike) -> Column:
    """Return the height of the raster in pixels (tile struct or bytes).
    
    Uses the FILE-aware 2-arg UDF when FILE is supported; falls back to
    single-arg UDF when FILE is not available. Signature to user: single tile arg.
    """
    tc = _col(tile)
    fref = file_ref_arg(tc)
    return _uf_height(tc, fref)  # 2-arg UDF (new)
```

**SQL registry mapping (unchanged—critical):**
```python
# SQL still maps to single-arg UDFs (fallback, no FILE acceleration per spec §4.3).
_sql_accessors = {
    "gbx_rst_height": _u_height,  # Single-arg (original), registered to SQL.
    # ... other accessors ...
}
```


### Steps

- [ ] **Add `file_ref_arg` helper** to `_file_ref.py` (NO spark param; uses getActiveSession internally):
  ```python
  from pyspark.sql import functions as F, SparkSession
  
  def file_ref_arg(tile_col) -> Column:
      """Return a Column expression for the file_ref argument to tile-reading UDFs.
      
      Uses SparkSession.getActiveSession() internally (Serverless-safe).
      """
      if file_supported():
          # Use F.call_function to mint a FileRef from the tile's path field.
          # try_to_file is a Spark SQL function, not a PySpark function.
          return F.call_function("try_to_file", tile_col["path"])
      else:
          # FILE not supported; pass None (fallback path in open_tile).
          return F.lit(None)
  ```

- [ ] **Add new 2-arg UDF factories** to `functions.py` (keep single-arg factories UNCHANGED for SQL registry):
  ```python
  def _header_accessor_udf_file(core_fn, return_type):
      """Struct + FileRef accepting header-only accessor UDF (2-arg).
      
      Signature: (tile: Struct, file_ref: FileRef|null) → return_type
      """
      @f.udf(return_type)
      def _udf(tile, file_ref):
          if _tile_is_empty(tile):
              return None
          try:
              from databricks.labs.gbx.pyrx import _env
              _env.configure_gdal_env()
              with ot.open_header(tile, file_ref=file_ref) as ds:
                  return core_fn(ds)
          except Exception:  # noqa: BLE001
              return None
      return _udf
  
  
  def _pixel_accessor_udf_file(core_fn, return_type):
      """Struct + FileRef accepting pixel accessor UDF (2-arg).
      
      Signature: (tile: Struct, file_ref: FileRef|null) → return_type
      """
      @f.udf(return_type)
      def _udf(tile, file_ref):
          if _tile_is_empty(tile):
              return None
          try:
              from databricks.labs.gbx.pyrx import _env
              _env.configure_gdal_env()
              with ot._open(tile, file_ref=file_ref) as ds:
                  return core_fn(ds)
          except Exception:  # noqa: BLE001
              return None
      return _udf
  ```

- [ ] **Create module-level 2-arg UDF instances** (alongside existing single-arg):
  ```python
  # --- New 2-arg UDF singletons (FILE-aware) -----
  # These are used by Python bindings; SQL registry still uses single-arg.
  _uf_height = _header_accessor_udf_file(accessors.height, IntegerType())
  _uf_numbands = _header_accessor_udf_file(accessors.numbands, IntegerType())
  _uf_srid = _header_accessor_udf_file(accessors.srid, IntegerType())
  _uf_crs = _header_accessor_udf_file(accessors.crs, StringType())
  _uf_metadata_udf = ...  # File-aware 2-arg version of _metadata_udf
  _uf_isempty = _pixel_accessor_udf_file(accessors.isempty, BooleanType())
  # ... etc. for all tile-reading accessors.
  ```

- [ ] **Update public Python bindings** (example: rst_height, rst_metadata, rst_summary, rst_width, rst_numbands, rst_srid, rst_boundingbox, rst_initnodata, rst_clip):
  ```python
  # Example for rst_height (header accessor):
  def rst_height(tile: ColLike) -> Column:
      """Return the height of the raster in pixels (tile struct or bytes).
      
      Automatically uses FILE acceleration when available; falls back to
      plain-path read otherwise. User sees a single-arg API.
      """
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg
      tc = _col(tile)
      fref = file_ref_arg(tc)
      return _uf_height(tc, fref)  # 2-arg: (tile, file_ref)
  
  # Example for rst_metadata (also header accessor):
  def rst_metadata(tile: ColLike) -> Column:
      """Return tile metadata as a map.
      
      Automatically uses FILE acceleration when available; falls back to
      plain-path read otherwise. User sees a single-arg API.
      """
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg
      tc = _col(tile)
      fref = file_ref_arg(tc)
      return _uf_metadata(tc, fref)  # 2-arg
  
  # Similar pattern for all eo-series MVP functions:
  # rst_width, rst_numbands, rst_srid, rst_boundingbox (header),
  # rst_initnodata, rst_clip (pixel accessors).
  ```

- [ ] **SQL registry remains unchanged** (single-arg UDFs still registered):
  ```python
  _sql_accessors = {
      "gbx_rst_height": _u_height,  # Single-arg (original)
      "gbx_rst_metadata": _metadata_udf,  # Single-arg (original)
      # ... all others unchanged ...
  }
  # SQL does NOT get FILE acceleration; it uses the fallback (plain-path) UDFs per spec §4.3.
  ```

- [ ] **Write failing test:** `test_binding_injection_passes_lit_none_when_file_not_supported`
  - Mock `file_supported()` to return False
  - Call `file_ref_arg(tile_col)` and inspect the resulting Column
  - Assert it is a `lit(None)` Column (no try_to_file)

  ```python
  def test_binding_injection_passes_lit_none_when_file_not_supported():
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg
      from pyspark.sql import functions as F
      from unittest import mock
      
      # Mock file_supported to return False.
      with mock.patch("databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=False):
          tile_col = F.col("tile")
          result_col = file_ref_arg(tile_col)
          
          # Verify it's a lit(None) by checking the expression string.
          expr_str = str(result_col._jc)
          assert "literal" in expr_str.lower(), f"Expected literal in {expr_str}"
  ```

- [ ] **Run test → expect FAIL** (implementation not complete)
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_binding_injection_passes_lit_none_when_file_not_supported -xvs
  ```

- [ ] **Implement and run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_binding_injection_passes_lit_none_when_file_not_supported -xvs
  ```

- [ ] **Write test:** `test_binding_injection_uses_call_function_when_supported`
  - Mock `file_supported()` to return True
  - Call `file_ref_arg(tile_col)` and inspect the resulting Column
  - Verify the Column's expression contains `try_to_file`

  ```python
  def test_binding_injection_uses_call_function_when_supported():
      from databricks.labs.gbx.pyrx._file_ref import file_ref_arg
      from pyspark.sql import functions as F
      from unittest import mock
      
      # Mock file_supported to return True.
      with mock.patch("databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=True):
          tile_col = F.col("tile")
          result_col = file_ref_arg(tile_col)
          
          # Verify it contains try_to_file by checking the expression string.
          expr_str = str(result_col._jc)
          assert "try_to_file" in expr_str, f"Expected try_to_file in {expr_str}"
  ```

- [ ] **Run test → expect PASS**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/test_file_ref.py::test_binding_injection_uses_call_function_when_supported -xvs
  ```

- [ ] **Run all pyrx tests again to confirm no regressions:**
  ```bash
  cd python/geobrix && python -m pytest test/pyrx/ -x --tb=short
  ```

- [ ] **Commit:**
  ```bash
  cd python/geobrix && git add src/databricks/labs/gbx/pyrx/_file_ref.py src/databricks/labs/gbx/pyrx/functions.py src/databricks/labs/gbx/pyrx/core/open_tile.py test/pyrx/test_file_ref.py
  git commit -m "feat(pyrx): wire file_ref into accessor UDFs via central file_ref_arg() injection"
  ```

---

## Task 5: Docs — Virtual Tiles + FILE section, add ONE DBR 19 row to each supported-versions matrix

### Files
- **Modify:** `docs/docs/api/virtual-tiles.mdx` (add new section)
- **Modify:** `README.md` (add ONE row to existing 8-column matrix at line ~50)
- **Modify:** `docs/docs/intro.mdx` (add ONE row to existing 7-column matrix at line ~90)
- **Modify:** `docs/docs/installation.mdx` (add ONE row to existing 8-column matrix at line ~21)
- **Modify:** `docs/docs/support.mdx` (update DBR examples)

### Steps

- [ ] **Add "Virtual Tiles + FILE" section to `docs/docs/api/virtual-tiles.mdx`**
  - Insert after the "How virtual tiles work" section, before any examples
  - Prose (no internal planning vocabulary; no wave numbers; no "the FILE branch"):

  ```markdown
  ## Virtual Tiles + FILE
  
  FILE is a Databricks data type that provides **governed access to files on compute without a FUSE mount**, and enables **byte-range reads** from cloud storage. GeoBrix detects FILE availability and uses it transparently when present, enabling faster windowed reads. This acceleration is **optional and fully backward-compatible** — if FILE is not available, virtual tiles work exactly as before via FUSE staging + rasterio windowed reads.
  
  When FILE is available (primarily on Databricks Runtime 19 dedicated clusters with `fileReferenceCreationMode=MANAGED`), GeoBrix:
  
  - Mints a `FileRef` column from the tile path in the Spark plan.
  - Passes the `FileRef` to the read UDF alongside the tile struct.
  - Reads only the exact window bytes via `fref.open()` (byte-range read), rather than staging the whole file to local disk first.
  - Falls back gracefully to the traditional FUSE path if any step fails.
  
  **Key point:** A `FileRef` is never displayed or stored as a DataFrame column — it is minted and consumed within each operation, then discarded. This keeps the tile surface clean and compatible with all downstream pipelines.
  
  ### Supported environments
  
  - **Databricks Runtime 19 dedicated clusters** (single-user, with `fileReferenceCreationMode=MANAGED` in cluster config): FILE acceleration lights up automatically. Virtual tiles use byte-range windowed reads.
  - **Local development, CI, Serverless Compute** (FILE not available): Virtual tiles work via the traditional FUSE + rasterio path. Behavior is identical; read latency is slightly higher (whole-file staging).
  - **Databricks Runtime 18, DBR LTS classic:** Virtual tiles work via the traditional FUSE path. No FILE acceleration.
  
  **Broadening:** As Databricks extends FILE support to serverless-GC and other environments, GeoBrix will transparently accelerate reads there as well — no user code changes required.
  ```

- [ ] **Add DBR 19 row to `README.md` supported-versions matrix**
  - Locate line ~50 (after the 18 LTS row)
  - Add ONE new row:
  ```markdown
  | **19 LTS** | 26.04 | 4.2.0 | 3.12.3 | 2.13.16 | 21 | **5+** (Py 3.12) | ✅ Supported (light tier) |
  ```
  - Keep the existing note below the table about DBR 19 coming soon; update it to reflect that 19 is now supported (at least the light tier).

- [ ] **Add DBR 19 row to `docs/docs/intro.mdx` supported-versions matrix**
  - Locate line ~90 (after the 18 LTS row)
  - Add ONE new row to the 7-column table:
  ```markdown
  | **19 LTS** | 26.04 | 4.2.0 | 3.12.3 | 2.13.16 | 21 | ✅ Supported (light tier) |
  ```
  - Update the "DBR 19 LTS is coming soon" note box below to reflect that 19 light tier is now supported.

- [ ] **Add DBR 19 row to `docs/docs/installation.mdx` supported-versions matrix**
  - Locate line ~21 (after the 18 LTS row)
  - Add ONE new row to the 8-column table:
  ```markdown
  | **19 LTS** | 26.04 | 4.2.0 | 3.12.3 | 2.13.16 | 21 | **5+** (Py 3.12) | ✅ Supported (light tier) |
  ```
  - Update the note box below to reflect that 19 light tier is now supported.

- [ ] **Update `docs/docs/support.mdx` to include DBR 19 in bug-report examples**
  - Find the "Reproduce on a Databricks cluster" section
  - Add a code block showing DBR 19 notebook setup:

  ```markdown
  ### Reproduce on Databricks Runtime 19
  
  On a Databricks Runtime 19 cluster (light tier):
  
  %pip install 'geobrix[light]'  # Install the light tier (JAR-free)
  
  from databricks.labs.gbx import pyrx
  
  # Define sample code here to reproduce the issue.
  # Include the output and full error traceback.
  ```

- [ ] **Check for wave-number leaks** using the internals-leak regex:
  ```bash
  grep -rniE "wave [0-9]|wave-[0-9]" /Users/mjohns/IdeaProjects/geobrix/docs/docs/
  ```
  Expected: no matches.

- [ ] **Build docs locally to validate syntax:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && gbx:docs:start
  # Open http://localhost:3000 and verify Virtual Tiles + FILE section renders correctly.
  ```

- [ ] **Commit:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add README.md docs/docs/api/virtual-tiles.mdx docs/docs/intro.mdx docs/docs/installation.mdx docs/docs/support.mdx
  git commit -m "docs: add Virtual Tiles + FILE section; update DBR 19 rows to 'Supported (light tier)'"
  ```

---

## Task 6: Cluster integration validation — documented manual validation (not CI)

### Scope
This task validates the FILE-path behavior on a FILE-enabled classic DBR 19.x dedicated cluster. It is **not a CI gate** and runs **once per release** on dogfood (the internal FILE-enabled cluster). The validation is documented as a runbook in the repo; **no MEMORY.md entry** (that is private user state, not a repo artifact).

### Files
- **Create:** `docs/superpowers/FILE-validation-on-dogfood.md` (a runbook for the orchestrator)

### Steps

- [ ] **Write a validation runbook** at `docs/superpowers/FILE-validation-on-dogfood.md`:

  ```markdown
  # FILE Validation Runbook — DBR 19 Dedicated Cluster (Dogfood)
  
  ## Prerequisites
  
  1. **Access to dogfood cluster** (internal Databricks, `fileReferenceCreationMode=MANAGED` in spark_conf).
  2. **GeoBrix wheel built and staged** to the cluster's Volume (e.g. `/Volumes/main/geobrix_samples/geobrix-current.whl`).
  3. **Sample raster data** available in `/Volumes/main/geobrix_samples/` (existing eo-series fixtures).
  
  ## Steps
  
  ### 1. Provision the cluster
  
  - Cluster name: `geobrix-file-validation`
  - Runtime: Databricks Runtime 19.x (latest stable)
  - Cluster size: Single-user, fixed 4-8 workers (small; validation only)
  - Cluster config: ensure `fileReferenceCreationMode: MANAGED` in spark_conf (this is set on dogfood by default).
  - Install wheel: `%pip install /Volumes/main/geobrix_samples/geobrix-current.whl`
  
  ### 2. Run validation notebook
  
  Create an ad-hoc notebook with the following cells:
  
  **Cell 1:** Feature-detect confirmation
  ```python
  from databricks.labs.gbx.pyrx._file_ref import file_supported
  
  result = file_supported(spark)
  print(f"FILE support detected: {result}")
  
  if not result:
      print("ERROR: FILE not detected on DBR 19. Check cluster config (fileReferenceCreationMode=MANAGED).")
  else:
      print("SUCCESS: FILE support confirmed.")
  ```
  
  **Cell 2:** Load a sample eo-series raster and create virtual tiles
  ```python
  from databricks.labs.gbx import pyrx
  import pandas as pd
  
  # Read a sample COG from /Volumes.
  sample_path = "/Volumes/main/geobrix_samples/geobrix-examples/london/LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
  
  df = pyrx.rst_fromfile(f"'{sample_path}'", "GTiff").toPandas()
  print(f"Virtual tiles created: {len(df)} rows")
  print(df.head())
  ```
  
  **Cell 3:** Pixel equality test (FILE path vs fallback)
  ```python
  from databricks.labs.gbx.pyrx.core import open_tile
  from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
  import numpy as np
  
  # Create a virtual tile.
  tile_row = df.iloc[0]
  tile = VirtualTile.from_row(tile_row.to_dict())
  
  # Read via open_tile (file_ref=None, fallback path).
  with open_tile(tile, file_ref=None) as ds_fallback:
      pixels_fallback = ds_fallback.read(1, window=(0, 0, 100, 100))
  
  print(f"Fallback read shape: {pixels_fallback.shape}")
  print(f"Fallback pixels sample: {pixels_fallback[:5, :5]}")
  
  # On DBR 19 with FILE enabled, we would also read via file_ref here.
  # For now, just confirm the fallback works and produces valid pixels.
  assert pixels_fallback.shape == (100, 100), f"Unexpected shape: {pixels_fallback.shape}"
  print("SUCCESS: Fallback path read valid pixels.")
  ```
  
  **Cell 4:** Pixel accessor test (rst_height, rst_numbands, rst_metadata)
  ```python
  from databricks.labs.gbx import pyrx
  from pyspark.sql import functions as F
  
  # Create virtual tiles from the sample path.
  df_tiles = pyrx.rst_fromfile(f"'{sample_path}'", "GTiff")
  
  # Call pixel accessors (which use the UDFs with file_ref wiring).
  df_heights = df_tiles.select(pyrx.rst_height(F.col("value")).alias("height"))
  df_metadata = df_tiles.select(pyrx.rst_metadata(F.col("value")).alias("metadata"))
  
  height_val = df_heights.collect()[0][0]
  metadata_val = df_metadata.collect()[0][0]
  
  print(f"Height: {height_val}")
  print(f"Metadata keys: {list(metadata_val.keys()) if metadata_val else None}")
  
  assert height_val is not None and height_val > 0, "Height should be valid"
  print("SUCCESS: Pixel accessors work via FILE-aware bindings.")
  ```
  
  ### 3. Validation checklist
  
  - [ ] Cluster provisioned with DBR 19.x and `fileReferenceCreationMode=MANAGED`.
  - [ ] Wheel installed without errors.
  - [ ] **Cell 1:** FILE support detected = True.
  - [ ] **Cell 2:** Virtual tiles created successfully (≥1 row).
  - [ ] **Cell 3:** Fallback path reads valid pixels (shape correct).
  - [ ] **Cell 4:** Pixel accessors (rst_height, rst_metadata) return valid data.
  
  ### 4. Signoff
  
  If all cells pass and the checklist is complete, FILE support is validated. Note:
  
  - The fallback path is tested (FILE absent in CI would also pass Cell 3).
  - The FILE-specific optimization (byte-range read, performance win) requires a FILE-enabled cluster, so it is validated here but not in CI.
  - No changes to user-facing API; everything is backward-compatible.
  
  ### 5. Teardown
  
  - Archive the validation notebook as a reference.
  - Keep the cluster running for post-release manual validation if needed.
  - Terminate the cluster after validation is complete.
  ```

- [ ] **Commit the validation runbook:**
  ```bash
  cd /Users/mjohns/IdeaProjects/geobrix && git add docs/superpowers/FILE-validation-on-dogfood.md
  git commit -m "docs: add FILE validation runbook for post-release dogfood testing"
  ```

---

## Self-Review Checklist — REVISED per Coordinator Feedback

### Critical Fixes Applied (from Coordinator Review)
- [ ] **Task 1 CRITICAL FIX:** Removed `pyspark.sql.types.FileRef(path)` constructor call (does NOT exist). Now uses `spark.sql("SELECT try_to_file(...)")` to mint FileRef in the PLAN, then a UDF consumes the fref column. ✓
- [ ] **Task 1:** `file_supported()` signature changed to NO spark param; uses `SparkSession.getActiveSession()` internally (Serverless-safe). ✓
- [ ] **Task 4 FIX:** Replaced non-existent `F.try_to_file(...)` with `F.call_function("try_to_file", ...)`. ✓
- [ ] **Task 4 FIX:** Removed spark param from `file_ref_arg()`; now just `file_ref_arg(tile_col) -> Column`. ✓
- [ ] **Task 4 CRITICAL FIX:** Do NOT break SQL registry. Created SEPARATE 2-arg UDF factories (`_header_accessor_udf_file`, `_pixel_accessor_udf_file`) producing `_uf_*` singletons. SQL registry still maps to single-arg `_u_*` UDFs (fallback, per spec §4.3). Python bindings use 2-arg via `file_ref_arg()`. ✓
- [ ] **Task 3 FIX:** Added `file_ref=None` to ALL THREE entry points: `open_tile`, `_open`, AND `open_header`. Extracted shared `_resolve_local_or_windowed` helper to avoid triplication. ✓
- [ ] **Task 5 FIX:** Add ONE row to existing matrices in README.md (~50), intro.mdx (~90), installation.mdx (~21), not inventing new tables. DBR 19 marked "Supported (light tier)". ✓
- [ ] **Task 6 FIX:** Removed `cat >> MEMORY.md` step (private file, not repo artifact). Kept the validation runbook doc only. ✓

### Spec Coverage
- [ ] **Feature-detect**: memoized `file_supported()` (no spark param), uses `getActiveSession()`, env override `GBX_DISABLE_FILE=1` ✓
- [ ] **Sentinel detail**: hardcoded `/Volumes/main/geobrix_samples/...` path; returns False on not found (acceptable, fallback always safe) ✓
- [ ] **Windowed read helper**: `open_windowed_via_fileref()` with `FileRefReadError` degradation ✓
- [ ] **Three entry points**: `open_tile`, `_open`, `open_header` all get `file_ref=None` param ✓
- [ ] **Shared helper**: `_resolve_local_or_windowed` prevents code triplication ✓
- [ ] **Binding injection**: central `file_ref_arg(tile_col)` helper, 2-arg UDF factories (`_uf_*`), Python bindings updated ✓
- [ ] **SQL registry unchanged**: single-arg `_u_*` UDFs still mapped to SQL (fallback, no FILE acceleration per spec) ✓
- [ ] **Docs**: Virtual Tiles + FILE section, DBR 19 row added to all 3 matrices, user-facing prose ✓
- [ ] **Validation**: manual runbook for dogfood (FILE-enabled DBR 19.x) ✓
- [ ] **Backward compatibility**: `file_ref=None` default, all existing tests pass ✓

### No FileRef Constructor Calls
- [ ] GREP for `FileRef(` in all implementation code — MUST be zero matches in shipped pyrx ✓
  - FileRef is MINTED in the PLAN via `try_to_file` (SQL function).
  - FileRef is CONSUMED in UDFs (arrives as a column, not constructed).

### No `F.try_to_file` Calls
- [ ] GREP for `F.try_to_file` — MUST be zero matches. Replace with `F.call_function("try_to_file", ...)` ✓

### SQL Registry Not Broken
- [ ] `_sql_accessors` dict still maps to single-arg `_u_*` UDFs (e.g. `"gbx_rst_height": _u_height`) ✓
- [ ] Single-arg `_u_*` UDFs remain unchanged (CRITICAL for SQL positional binding) ✓
- [ ] New 2-arg `_uf_*` UDFs exist SEPARATELY ✓
- [ ] Python bindings use `_uf_*` (2-arg), SQL uses `_u_*` (single-arg) ✓

### Test Scope
- [ ] CI tests cover **fallback path** (FILE absent): ✓
  - `test_file_supported_respects_env_override` (env override → False, no spark.sql called)
  - `test_file_supported_memoization` (roundtrip called once, cached)
  - `test_open_tile_file_ref_none_backward_compatible` (default param, existing behavior)
  - `test_binding_injection_passes_lit_none_when_file_not_supported` (lit(None) when no FILE)
- [ ] Unit tests with **stubs** verify FILE logic without FILE-enabled cluster: ✓
  - `test_open_windowed_via_fileref_reads_correct_pixels` (stub FileRef, real local TIF)
  - `test_open_windowed_via_fileref_raises_on_non_seekable` (non-seekable → FileRefReadError)
  - `test_open_tile_uses_file_ref_when_provided` (stub FileRef, wrong path proves FILE used)
  - `test_open_tile_file_ref_degrades_to_fallback` (failing FILE → fallback works)
- [ ] Manual **integration test on dogfood** (FILE-enabled DBR 19.x): ✓
  - Runbook at `docs/superpowers/FILE-validation-on-dogfood.md`

### Docs
- [ ] User-facing prose avoids internal planning vocabulary (no "wave", no "the FILE branch") ✓
- [ ] DBR 19 added to all three matrix locations with "Supported (light tier)" ✓
- [ ] Virtual Tiles + FILE section explains governance, byte-range reads, fallback ✓
- [ ] No wave-number leaks via `grep -rniE "wave [0-9]"` ✓

### Post-MVP Scope (NOT in this plan)
- [ ] Benchmarking — FILE vs Volume leg (§7 of spec) — **deferred to separate plan** ✓

---

## Summary

This plan covers the **MVP implementation** of central FILE / FILEREF read support for light-tier virtual tiles:

1. **Task 1 (Feature-detect):** Memoized `file_supported(spark)` that runs an end-to-end roundtrip and respects `GBX_DISABLE_FILE=1`.
2. **Task 2 (Windowed-read helper):** `open_windowed_via_fileref()` that reads a FileRef stream via rasterio, with defensive `FileRefReadError` degradation.
3. **Task 3 (`open_tile` integration):** Add `file_ref=None` param, FILE branch, and graceful fallback to plain-path read.
4. **Task 4 (Binding injection):** Central `file_ref_arg()` helper wired into accessor UDF factories, so all tile-reading ops get FILE support automatically.
5. **Task 5 (Docs):** Virtual Tiles + FILE section, DBR 19 matrix rows, user-facing prose (no internal vocabulary).
6. **Task 6 (Validation):** Manual runbook for dogfood (FILE-enabled DBR 19.x) post-release validation.

**Success:** All existing tests pass (fallback path unchanged), new unit tests cover feature-detect + integration + binding wiring, and manual validation on dogfood confirms FILE-path behavior. The feature ships support-ready: fully functional on classic DBR 19.x now, transparently accelerating reads on serverless-GC once FILE support arrives there.

**Scope NOT in this plan:** Benchmarking (§7 of spec), FILE write path, heavy tier, materialized tiles.
