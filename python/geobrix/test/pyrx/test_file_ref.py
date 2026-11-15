"""Tests for the FILE reference: file_supported(), open_windowed_via_fileref(), file_ref_arg().

Covers:
  - GBX_DISABLE_FILE env override → False without touching Spark
  - Memoization: roundtrip attempted at most once per session
  - Any exception during roundtrip → False (exception swallowed, result cached)
  - open_windowed_via_fileref reads correct windowed pixels from a stub FileRef
  - open_windowed_via_fileref raises FileRefReadError on a non-seekable stream
  - caller-body exception propagates unchanged (not wrapped in FileRefReadError)
  - the FILE-reference Column a binding injects, per support, switch and cap

How ``open_tile`` routes a tile through a reference (a clip or a warp still applies, a failing
reference falls back, the switch moves nothing) is in ``test_file_ref_equivalence.py``.
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
    """Probe runs exactly once; subsequent calls use the cached result."""
    os.environ.pop("GBX_DISABLE_FILE", None)
    # Reset cache for this test
    from databricks.labs.gbx.pyrx import _file_ref

    _file_ref._FILE_SUPPORT_CACHE.clear()

    call_count = [0]
    original_sql = spark.sql

    def mock_sql(query):
        call_count[0] += 1
        raise RuntimeError("simulated UNSUPPORTED_DATATYPE")

    try:
        spark.sql = mock_sql
        result1 = file_supported()
        result2 = file_supported()
        assert result1 is False
        assert result2 is False
        assert (
            call_count[0] == 1
        ), f"Expected spark.sql called once, got {call_count[0]}"
    finally:
        spark.sql = original_sql


def test_file_supported_returns_true_when_probe_succeeds(spark):
    """Fixture-free probe: spark.sql().collect() succeeds → file_supported() returns True.

    Uses a mock so no real FILE env is required.  Confirms the success path of
    the plan-only probe yields True (old UDF-consume roundtrip always needed a
    real file and returned False locally).
    """
    from unittest.mock import MagicMock

    from databricks.labs.gbx.pyrx import _file_ref

    _file_ref._FILE_SUPPORT_CACHE.clear()
    os.environ.pop("GBX_DISABLE_FILE", None)

    original_sql = spark.sql
    call_count = [0]

    def mock_sql(query):
        call_count[0] += 1
        mock_df = MagicMock()
        mock_df.collect.return_value = [(True,)]
        return mock_df

    try:
        spark.sql = mock_sql
        result = file_supported()
        assert result is True, f"Expected True when probe succeeds, got {result}"
        assert call_count[0] == 1, f"Expected 1 spark.sql call, got {call_count[0]}"
    finally:
        spark.sql = original_sql
        # Belt-and-suspenders: clear the True entry so no downstream test sees
        # file_supported()→True via the cached value from this monkeypatch.
        _file_ref._FILE_SUPPORT_CACHE.clear()


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
    from databricks.labs.gbx.pyrx._file_ref import (
        FileRefReadError,
        open_windowed_via_fileref,
    )

    stub_fref = _StubFileRefNonSeekable(gtiff_bytes)
    window = (0, 0, 2, 2)
    pending = (None, None, None, None)

    with pytest.raises(FileRefReadError):
        with open_windowed_via_fileref(stub_fref, window, pending):
            pass


def test_open_windowed_via_fileref_windowless_full_extent(gtiff_bytes):
    """window=None resolves to the full source extent — same pixels as explicit (0,0,W,H).

    gtiff_bytes is 4×3 (width=4, height=3); window=None must read all 12 pixels.
    The 4-tuple path (explicit window) must be byte-identical — confirms no regression.
    """
    from databricks.labs.gbx.pyrx._file_ref import open_windowed_via_fileref

    stub_fref = _StubFileRef(gtiff_bytes)
    pending = (None, None, None, None)

    with open_windowed_via_fileref(stub_fref, (0, 0, 4, 3), pending) as ds_ex:
        pixels_ex = ds_ex.read(1)

    with open_windowed_via_fileref(stub_fref, None, pending) as ds_wl:
        pixels_wl = ds_wl.read(1)

    assert pixels_wl.shape == (3, 4), f"Expected (3,4) got {pixels_wl.shape}"
    np.testing.assert_array_equal(pixels_wl, pixels_ex)


# ---------------------------------------------------------------------------
# caller-body exception must propagate unchanged
# ---------------------------------------------------------------------------


def test_open_windowed_via_fileref_caller_exception_propagates(gtiff_bytes):
    """Exception raised in the caller's with-body must propagate unchanged.

    Before the fix, the yield was inside the broad try/except, so a caller-body
    exception would be caught and re-raised as FileRefReadError, which then caused
    a RuntimeError('generator didn't stop after throw()') at the contextmanager
    level.  After the fix the yield is outside the broad handler, so the ValueError
    must propagate directly.
    """
    from databricks.labs.gbx.pyrx._file_ref import open_windowed_via_fileref

    stub = _StubFileRef(gtiff_bytes)
    window = (0, 0, 4, 3)
    pending = (None, None, None, None)

    with pytest.raises(ValueError, match="caller boom"):
        with open_windowed_via_fileref(stub, window, pending) as ds:
            assert ds is not None  # generator did yield
            raise ValueError("caller boom")


def test_fuse_direct_switch_is_on_only_for_one(monkeypatch):
    """GBX_DISABLE_FUSE_DIRECT: unset is off, "1" turns the kill-switch on, every other value is off."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_direct_disabled

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
    assert not _fuse_direct_disabled()
    monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", "1")
    assert _fuse_direct_disabled()
    for val in ("0", "true", "yes", "on", "2", ""):
        monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", val)
        assert not _fuse_direct_disabled(), val


# supported, switch, stream cap, what the expression holds, what it must not
_ARG_ROWS = {
    "no FILE support": (False, None, None, ["NULL"], ["try_to_file", "CASE"]),
    # large whole-file /Volumes tiles go FUSE-direct (NULL), small and windowed ones keep the reference
    "size-gated default": (
        True,
        None,
        None,
        ["CASE", "try_to_file", "path_file_size", "window", "Volumes", ">"],
        [],
    ),
    "kill-switch is the bare stream": (
        True,
        "1",
        None,
        ["try_to_file"],
        ["CASE", "WHEN", "path_file_size"],
    ),
    "the cap is baked into the plan": (True, None, "12345", ["12345"], []),
}


@pytest.mark.parametrize("row", sorted(_ARG_ROWS), ids=str)
@pytest.mark.parametrize("as_name", [False, True], ids=["column", "bare-name"])
def test_file_ref_arg_expression(spark, monkeypatch, row, as_name):
    """The Column a binding injects as the FILE reference: its shape per switch, support and cap.
    A bare column name (what ``_col("tile")`` hands over) is coerced to a Column. try_to_file is a
    Databricks built-in, so the expression is judged by its text, not evaluated."""
    from unittest import mock

    from pyspark.sql import Column
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    supported, switch, cap, holds, lacks = _ARG_ROWS[row]
    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    if switch:
        monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", switch)
    if cap:
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", cap)
    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=supported
    ):
        col = file_ref_arg("tile" if as_name else F.col("tile"))
    assert isinstance(col, Column)
    text = str(col._jc)
    for want in holds:
        assert want.lower() in text.lower(), f"{row}: {want!r} missing from {text!r}"
    for bad in lacks:
        assert bad.lower() not in text.lower(), f"{row}: {bad!r} in {text!r}"


def test_file_ref_sql_follows_the_rule_of_file_ref_arg(monkeypatch):
    """The SQL form a FILE-reference wrapper sends: NULL without FILE, the bare mint with FUSE-direct off, else the
    mint with the same size-gated FUSE-direct rule and connect-aware cap as ``file_ref_arg``.
    """
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.manager import capabilities

    # the session lists a function that carries FILE (the `sql_file_type` capability has its own tests)
    monkeypatch.setattr(capabilities, "has", lambda *a, **k: True)
    monkeypatch.setattr(file_gbx, "file_supported", lambda spark=None: False)
    assert file_gbx.file_ref_sql("t") == "NULL"
    monkeypatch.setattr(file_gbx, "file_supported", lambda spark=None: True)
    monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", "1")
    assert file_gbx.file_ref_sql("t") == "try_to_file(t.path)"
    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT")
    cap = file_gbx._connect_aware_lru_sizing(None)[0]
    got = file_gbx.file_ref_sql("t")
    assert got.startswith(
        "CASE WHEN t.window IS NULL AND (startswith(t.path, '/Volumes')"
    )
    assert f"CAST(t.metadata['path_file_size'] AS BIGINT) > {cap} THEN NULL" in got
    assert got.endswith("ELSE try_to_file(t.path) END")
