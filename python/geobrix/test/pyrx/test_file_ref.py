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
