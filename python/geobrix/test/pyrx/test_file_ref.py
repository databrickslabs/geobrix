"""Tests for pyrx._file_ref.file_supported() and open_windowed_via_fileref().

Covers:
  - GBX_DISABLE_FILE env override → False without touching Spark
  - Memoization: roundtrip attempted at most once per session
  - Any exception during roundtrip → False (exception swallowed, result cached)
  - open_windowed_via_fileref reads correct windowed pixels from a stub FileRef
  - open_windowed_via_fileref raises FileRefReadError on a non-seekable stream
"""

import io
import os
import tempfile

import numpy as np
import pytest

from databricks.labs.gbx.pyrx._file_ref import file_supported


def test_file_supported_respects_env_override(spark):
    """GBX_DISABLE_FILE=1 short-circuits before Spark is touched — spark.sql never called."""
    from databricks.labs.gbx.pyrx import _file_ref

    _file_ref._FILE_SUPPORT_CACHE.clear()
    os.environ["GBX_DISABLE_FILE"] = "1"
    call_count = [0]
    original_sql = spark.sql

    def counting_sql(query):
        call_count[0] += 1
        return original_sql(query)

    try:
        spark.sql = counting_sql
        result = file_supported()
        assert result is False
        assert call_count[0] == 0, f"Expected 0 spark.sql calls, got {call_count[0]}"
    finally:
        spark.sql = original_sql
        os.environ.pop("GBX_DISABLE_FILE", None)


def test_file_supported_memoization(spark):
    """Roundtrip runs exactly once; subsequent calls use the cached result."""
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


# ---------------------------------------------------------------------------
# StubFileRef helpers (used by both windowed-read tests)
# ---------------------------------------------------------------------------


class _StubFileRef:
    """Wraps raw GeoTIFF bytes; .open() returns a seekable BytesIO."""

    def __init__(self, data_bytes):
        self.data_bytes = data_bytes

    def open(self):
        return io.BytesIO(self.data_bytes)

    def as_local_file(self):
        fd, tmp = tempfile.mkstemp(suffix=".tif")
        os.close(fd)
        with open(tmp, "wb") as f:
            f.write(self.data_bytes)
        return tmp


class _NonSeekableStream:
    """Read-only, non-seekable wrapper around a BytesIO."""

    def __init__(self, data):
        self._buffer = io.BytesIO(data)

    def read(self, n=-1):
        return self._buffer.read(n)

    def seekable(self):
        return False


class _StubFileRefNonSeekable:
    """FileRef whose .open() returns a non-seekable stream."""

    def __init__(self, data_bytes):
        self.data_bytes = data_bytes

    def open(self):
        return _NonSeekableStream(self.data_bytes)


# ---------------------------------------------------------------------------
# Tests for open_windowed_via_fileref
# ---------------------------------------------------------------------------


def test_open_windowed_via_fileref_reads_correct_pixels(gtiff_bytes):
    """Happy path: windowed read via stub FileRef returns expected pixel values.

    gtiff_bytes is a 4×3 float32 GeoTIFF where data = np.arange(12).reshape(3, 4):
      Row 0: [0, 1, 2, 3]
      Row 1: [4, 5, 6, 7]
      Row 2: [8, 9, 10, 11]
    Window (col_off=0, row_off=0, width=2, height=2) → [[0, 1], [4, 5]].
    """
    from databricks.labs.gbx.pyrx._file_ref import open_windowed_via_fileref

    stub_fref = _StubFileRef(gtiff_bytes)
    window = (0, 0, 2, 2)  # (col_off, row_off, width, height)
    pending = (None, None, None, None)  # no pending instructions

    with open_windowed_via_fileref(stub_fref, window, pending) as ds:
        pixels = ds.read(1)
        expected = np.array([[0, 1], [4, 5]], dtype="float32")
        np.testing.assert_array_equal(pixels, expected)


def test_open_windowed_via_fileref_raises_on_non_seekable(gtiff_bytes):
    """Non-seekable FileRef stream → FileRefReadError is raised."""
    from databricks.labs.gbx.pyrx._file_ref import FileRefReadError, open_windowed_via_fileref

    stub_fref = _StubFileRefNonSeekable(gtiff_bytes)
    window = (0, 0, 2, 2)
    pending = (None, None, None, None)

    with pytest.raises(FileRefReadError):
        with open_windowed_via_fileref(stub_fref, window, pending):
            pass


# ---------------------------------------------------------------------------
# Tests for open_tile file_ref integration (Task 3)
# ---------------------------------------------------------------------------


def test_open_tile_file_ref_none_backward_compatible(gtiff_bytes):
    """open_tile(tile) with no file_ref arg uses today's path-read code path."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tif_bytes = gtiff_bytes
    fd, tmp_path = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp_path, "wb") as f:
            f.write(tif_bytes)

        tile = VirtualTile(
            cellid=0,
            path=tmp_path,
            window=(0, 0, 4, 3),
        )

        with open_tile(tile) as ds:
            pixels = ds.read(1)
            expected = np.arange(12, dtype="float32").reshape(3, 4)
            np.testing.assert_array_equal(pixels, expected)
    finally:
        os.remove(tmp_path)


def test_open_tile_uses_file_ref_when_provided(gtiff_bytes):
    """FILE branch: open_tile reads via FileRef even when tile.path is bogus."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    class StubFileRef:
        def __init__(self, data_bytes):
            self.data_bytes = data_bytes

        def open(self):
            return io.BytesIO(self.data_bytes)

        def as_local_file(self):
            raise AssertionError("Should not degrade — FILE branch should succeed")

    stub_fref = StubFileRef(gtiff_bytes)

    tile = VirtualTile(
        cellid=0,
        path="/nonexistent/path.tif",
        window=(0, 0, 4, 3),
    )

    with open_tile(tile, file_ref=stub_fref) as ds:
        pixels = ds.read(1)
        expected = np.arange(12, dtype="float32").reshape(3, 4)
        np.testing.assert_array_equal(pixels, expected)


def test_open_tile_file_ref_degrades_to_fallback(gtiff_bytes):
    """Degradation: failing FileRef.open() → falls back via as_local_file()."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    class FailingStubFileRef:
        def __init__(self, data_bytes):
            self._data_bytes = data_bytes

        def open(self):
            raise IOError("FileRef stream failed")

        def as_local_file(self):
            fd, tmp = tempfile.mkstemp(suffix=".tif")
            os.close(fd)
            with open(tmp, "wb") as f:
                f.write(self._data_bytes)
            return tmp

    stub_fref = FailingStubFileRef(gtiff_bytes)

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

        with open_tile(tile, file_ref=stub_fref) as ds:
            pixels = ds.read(1)
            expected = np.arange(12, dtype="float32").reshape(3, 4)
            np.testing.assert_array_equal(pixels, expected)
    finally:
        os.remove(fallback_path)


# ---------------------------------------------------------------------------
# Tests for file_ref_arg injection (Task 4)
# ---------------------------------------------------------------------------


def test_binding_injection_passes_lit_none_when_file_not_supported(spark):
    """file_ref_arg returns F.lit(None) when FILE is not supported.

    ``spark`` param ensures the JVM is running (needed for Column._jc).
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    with mock.patch(
        "databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=False
    ):
        tile_col = F.col("tile")
        result_col = file_ref_arg(tile_col)
        expr_str = str(result_col._jc)
        # F.lit(None) renders as 'NULL' in Spark; confirm no try_to_file expression.
        assert "try_to_file" not in expr_str, f"Unexpected try_to_file in {expr_str!r}"
        assert expr_str.upper() == "NULL", f"Expected NULL literal, got {expr_str!r}"


def test_binding_injection_uses_call_function_when_supported(spark):
    """file_ref_arg returns a try_to_file Column when FILE is supported.

    ``spark`` param ensures the JVM is running (needed for Column._jc).
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    with mock.patch(
        "databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=True
    ):
        tile_col = F.col("tile")
        result_col = file_ref_arg(tile_col)
        expr_str = str(result_col._jc)
        assert "try_to_file" in expr_str, f"Expected try_to_file in {expr_str!r}"


def test_binding_rewired_rst_height_returns_correct_value(spark, gtiff_bytes):
    """End-to-end: rewired rst_height returns correct pixel height via fallback.

    file_supported() returns False locally so file_ref_arg → F.lit(None) →
    the 2-arg UDF falls back to the plain-path open_header path.  Builds a
    materialized tile via rst_fromcontent (no JAR needed).
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx

    # Build a materialized tile from the gtiff_bytes fixture (4×3).
    df = (
        spark.createDataFrame([(gtiff_bytes,)], ["raster"])
        .select(prx.rst_fromcontent("raster", F.lit("GTiff")).alias("tile"))
    )
    result = df.select(prx.rst_height("tile")).collect()[0][0]
    assert result == 3, f"Expected height=3, got {result}"


def test_sql_registry_still_maps_to_single_arg_udfs():
    """SQL_REGISTRY entries for accessors point at single-arg ``_u_*`` UDFs.

    This confirms that the FILE-aware rewiring of the public Python bindings
    did NOT inadvertently redirect the SQL registry entries to the 2-arg ``_uf_*``
    UDFs.  SQL binds positionally, so a 2-arg UDF in the registry would cause
    incorrect or errored SQL calls.
    """
    from databricks.labs.gbx.pyrx.functions import (
        SQL_REGISTRY,
        _metadata_udf,
        _u_height,
        _u_numbands,
        _u_srid,
        _u_width,
    )

    assert SQL_REGISTRY["gbx_rst_height"] is _u_height, "gbx_rst_height must map to _u_height"
    assert SQL_REGISTRY["gbx_rst_numbands"] is _u_numbands, (
        "gbx_rst_numbands must map to _u_numbands"
    )
    assert SQL_REGISTRY["gbx_rst_srid"] is _u_srid, "gbx_rst_srid must map to _u_srid"
    assert SQL_REGISTRY["gbx_rst_width"] is _u_width, "gbx_rst_width must map to _u_width"
    assert SQL_REGISTRY["gbx_rst_metadata"] is _metadata_udf, (
        "gbx_rst_metadata must map to _metadata_udf (single-arg)"
    )
