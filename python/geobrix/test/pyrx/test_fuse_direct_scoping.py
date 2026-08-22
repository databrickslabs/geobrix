"""Tests for size-gated FUSE-direct default and kill-switch (GBX_DISABLE_FUSE_DIRECT).

Covers:
  - file_ref_arg expression structure: DEFAULT is size-gated CASE WHEN (window IS
    NULL + FUSE path + path_file_size > cap), KILL-SWITCH gives bare try_to_file.
  - Cap baking: the connect-aware stream cap from _connect_aware_lru_sizing is baked
    into the plan-level Column at driver-build time (honors GBX_STREAM_MAX_BYTES).
  - _fuse_direct_disabled() unit: False by default; True only when env=="1".
  - open_tile routing: whole-file tile (window=None) with file_ref=None reads via
    FUSE-direct local-path; tile with a present file_ref uses open_windowed_via_fileref.
  - Pixel equivalence: FUSE-direct (file_ref=None) == stream (file_ref present).
  - Materialized tile (raster set): unaffected regardless of env var.
  - CRITICAL SIZE GATE: the CASE WHEN condition includes the size > cap predicate, so
    small whole-file tiles (path_file_size <= cap) keep the FileRef and stream instead
    of taking the FUSE-direct path (small-file FUSE-direct regressed Serverless ~1.6x).
"""

import io
import os
import tempfile

import numpy as np

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# Stub helpers
# ---------------------------------------------------------------------------


class _StubStreamRef:
    """Stub FileRef whose .open() returns a seekable BytesIO of the provided bytes.

    Used to verify that open_windowed_via_fileref was invoked (tracks .open_called).
    """

    def __init__(self, data_bytes: bytes):
        self._data = data_bytes
        self.open_called = False

    def open(self):
        self.open_called = True
        return io.BytesIO(self._data)

    def as_local_file(self):
        raise AssertionError("as_local_file() must not be called on _StubStreamRef")


# ---------------------------------------------------------------------------
# B. _fuse_direct_disabled() unit tests
# ---------------------------------------------------------------------------


def test_fuse_direct_disabled_false_by_default(monkeypatch):
    """Returns False when GBX_DISABLE_FUSE_DIRECT is unset (scoped FUSE-direct is ON)."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_direct_disabled

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
    assert not _fuse_direct_disabled()


def test_fuse_direct_disabled_true_when_set_to_one(monkeypatch):
    """Returns True when GBX_DISABLE_FUSE_DIRECT=1 (kill-switch activated)."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_direct_disabled

    monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", "1")
    assert _fuse_direct_disabled()


def test_fuse_direct_disabled_false_for_zero(monkeypatch):
    """Returns False for GBX_DISABLE_FUSE_DIRECT=0 (kill-switch only activates on "1")."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_direct_disabled

    monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", "0")
    assert not _fuse_direct_disabled()


def test_fuse_direct_disabled_false_for_other_values(monkeypatch):
    """Returns False for arbitrary values like 'true', 'yes', etc."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_direct_disabled

    for val in ("true", "yes", "on", "2", ""):
        monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", val)
        assert not _fuse_direct_disabled(), f"Expected False for {val!r}"


# ---------------------------------------------------------------------------
# A. file_ref_arg expression structure
# ---------------------------------------------------------------------------


def test_file_ref_arg_default_is_size_gated_case_when(spark, monkeypatch):
    """DEFAULT (env unset): file_ref_arg returns a size-gated CASE WHEN expression.

    The WHEN condition must reference ALL THREE predicates:
      1. window IS NULL          — whole-file tiles only (windowed → stream)
      2. path starts with /Volumes or /dbfs — FUSE-accessible paths only
      3. path_file_size > cap    — large files only (small → stream, faster on Serverless)

    And the OTHERWISE branch must be try_to_file.

    A bare try_to_file would send ALL /Volumes tiles to FUSE-direct regardless of
    size, including small ones that are faster to stream on Serverless (~1.6x
    regression measured on-cluster) and windowed ones that need the FileRef for
    amortized opens (10-290x regression for striped rasters).
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)

    # Must be a CASE WHEN (not bare try_to_file)
    assert (
        "CASE" in expr_str.upper() or "WHEN" in expr_str.upper()
    ), f"Expected CASE WHEN expression for size-gated default, got: {expr_str!r}"
    # Must include try_to_file (in the OTHERWISE branch)
    assert (
        "try_to_file" in expr_str
    ), f"Expected try_to_file in CASE WHEN expression, got: {expr_str!r}"
    # Must include the size predicate: path_file_size metadata field
    assert (
        "path_file_size" in expr_str
    ), f"Expected path_file_size in size-gated expression, got: {expr_str!r}"
    # Must include a cast to long/bigint for the size comparison
    expr_lower = expr_str.lower()
    assert "bigint" in expr_lower or "long" in expr_lower or "cast" in expr_lower, (
        f"Expected cast to long/bigint in expression (for path_file_size comparison), "
        f"got: {expr_str!r}"
    )
    # Must include a > comparison (size > cap)
    assert (
        ">" in expr_str
    ), f"Expected '>' comparison in expression (path_file_size > cap), got: {expr_str!r}"
    # Must include the window IS NULL predicate
    assert (
        "window" in expr_lower
    ), f"Expected window IS NULL predicate in expression, got: {expr_str!r}"
    assert (
        "null" in expr_lower
    ), f"Expected null reference (window IS NULL) in expression, got: {expr_str!r}"
    # Must include the FUSE path prefix check (/Volumes)
    assert (
        "volumes" in expr_lower or "/Volumes" in expr_str
    ), f"Expected /Volumes path prefix check in expression, got: {expr_str!r}"


def test_file_ref_arg_cap_baking(spark, monkeypatch):
    """CAP BAKING: the connect-aware stream cap is baked into the Column at plan-build.

    With GBX_STREAM_MAX_BYTES=12345, file_ref_arg must produce a Column expression
    that literally contains 12345. This proves _connect_aware_lru_sizing is called
    on the driver at plan-build time and its result is embedded as a literal in the
    Spark plan — NOT deferred to the worker. On Serverless (Connect session), the
    cap would be 64 MiB; on Classic, 256 MiB. The env override wins in both cases.
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)

    assert "12345" in expr_str, (
        f"Cap from GBX_STREAM_MAX_BYTES must be baked as a literal into the plan-level "
        f"Column expression. Expected '12345' in expr, got: {expr_str!r}"
    )


def test_file_ref_arg_kill_switch_returns_bare_try_to_file(spark, monkeypatch):
    """KILL-SWITCH (GBX_DISABLE_FUSE_DIRECT=1): bare try_to_file, no CASE WHEN.

    The kill-switch forces the byte-range stream for EVERY tile (equivalent to
    the pre-size-gate behavior) to allow regression isolation. It must produce a
    bare try_to_file with no size gate, no window predicate, no path_file_size.
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", "1")

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)

    assert (
        "try_to_file" in expr_str
    ), f"Expected try_to_file in kill-switch expression, got: {expr_str!r}"
    assert (
        "CASE" not in expr_str.upper() and "WHEN" not in expr_str.upper()
    ), f"Kill-switch must produce bare try_to_file (no CASE WHEN), got: {expr_str!r}"
    # No size gate in kill-switch mode — path_file_size must not appear.
    assert "path_file_size" not in expr_str, (
        f"Kill-switch must not reference path_file_size (bare stream, no size gate), "
        f"got: {expr_str!r}"
    )


def test_file_ref_arg_file_not_supported_returns_null(spark, monkeypatch):
    """file_supported=False → file_ref_arg returns F.lit(None) (NULL literal).

    When FILE is not supported on the runtime, there is no try_to_file at all;
    the UDF receives file_ref=None and falls back to plain-path open.
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=False
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)

    assert (
        expr_str.upper() == "NULL"
    ), f"Expected NULL literal when FILE not supported, got: {expr_str!r}"
    assert (
        "try_to_file" not in expr_str
    ), f"Unexpected try_to_file when FILE not supported: {expr_str!r}"


def test_file_ref_arg_str_coercion_still_works(spark, monkeypatch):
    """file_ref_arg('tile') (str, not Column) must not raise TypeError.

    Callers use file_ref_arg(_col(tile)) where _col returns the bare str 'tile'.
    The coerce-to-Column guard must handle the str→Column conversion before the
    new whole_file / fuse_path subscript operations.
    """
    from unittest import mock

    from pyspark.sql import Column
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result_str = file_ref_arg("tile")
        assert isinstance(
            result_str, Column
        ), f"Expected Column, got {type(result_str)}"

        result_col = file_ref_arg(F.col("tile"))
        assert isinstance(
            result_col, Column
        ), f"Expected Column, got {type(result_col)}"


# ---------------------------------------------------------------------------
# C. open_tile routing (behavioral)
# ---------------------------------------------------------------------------


def test_whole_file_tile_without_file_ref_reads_local_path(gtiff_bytes):
    """Whole-file tile (window=None) with file_ref=None reads via FUSE-direct local path.

    This is the primary FUSE-direct path: file_ref_arg returns NULL for a whole-file
    /Volumes tile (on the driver), so the worker receives file_ref=None and opens the
    tile via tile.path directly. Pixels must match the expected ramp.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        tile = VirtualTile(cellid=0, path=tmp, window=None)

        with open_tile(tile, file_ref=None) as ds:
            pixels = ds.read(1)

        expected = np.arange(12, dtype="float32").reshape(3, 4)
        np.testing.assert_array_equal(pixels, expected)
    finally:
        os.remove(tmp)


def test_tile_with_file_ref_uses_open_windowed_via_fileref(gtiff_bytes):
    """Tile with a present file_ref always uses open_windowed_via_fileref (stream).

    This proves the revert: open_tile has NO env-var gate. A present file_ref
    always invokes the stream path, regardless of GBX_DISABLE_FUSE_DIRECT.
    The amortization benefit (one expensive .open() amortized across many windows
    from the same source) is preserved for windowed/striped tiles.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    stub = _StubStreamRef(gtiff_bytes)

    tile = VirtualTile(
        cellid=0,
        path="s3://my-bucket/prefix/tile.tif",
        window=(0, 0, 4, 3),
    )

    with open_tile(tile, file_ref=stub) as ds:
        pixels = ds.read(1)

    expected = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_array_equal(pixels, expected)
    assert (
        stub.open_called
    ), "open_windowed_via_fileref must invoke file_ref.open() when file_ref is present"


def test_pixel_equivalence_fuse_direct_vs_stream(gtiff_bytes):
    """FUSE-direct (file_ref=None) and stream (file_ref present) return identical pixels.

    Both paths must produce the same output for the same window, confirming no
    pixel divergence from the routing change.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        window = (0, 0, 2, 2)

        # FUSE-direct path: file_ref=None, open via tile.path
        tile_direct = VirtualTile(cellid=0, path=tmp, window=window)
        with open_tile(tile_direct, file_ref=None) as ds:
            pixels_direct = ds.read(1).copy()

        # Stream path: file_ref present, open via open_windowed_via_fileref
        stub = _StubStreamRef(gtiff_bytes)
        tile_stream = VirtualTile(cellid=0, path=tmp, window=window)
        with open_tile(tile_stream, file_ref=stub) as ds:
            pixels_stream = ds.read(1).copy()

        np.testing.assert_array_equal(
            pixels_direct,
            pixels_stream,
            err_msg="FUSE-direct and stream must return identical pixels",
        )
    finally:
        os.remove(tmp)


def test_materialized_tile_unaffected_by_env(gtiff_bytes, monkeypatch):
    """Materialized tile (raster set) is unaffected by GBX_DISABLE_FUSE_DIRECT.

    The first branch in open_tile (raster is not None) returns immediately;
    file_ref is never consulted and the env var has no effect.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    # Both with and without the kill-switch must produce correct pixels.
    for env_val in (None, "1"):
        if env_val is None:
            monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
        else:
            monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", env_val)

        tile = VirtualTile(cellid=0, raster=gtiff_bytes)
        with open_tile(tile) as ds:
            pixels = ds.read(1)

        expected = np.arange(12, dtype="float32").reshape(3, 4)
        np.testing.assert_array_equal(pixels, expected)


def test_open_tile_routing_depends_only_on_file_ref_presence(gtiff_bytes, monkeypatch):
    """open_tile routing depends only on whether file_ref is None, not on any env var.

    With file_ref=None: FUSE-direct local path regardless of GBX_DISABLE_FUSE_DIRECT.
    With file_ref present: stream path regardless of GBX_DISABLE_FUSE_DIRECT.

    This confirms the revert: the worker-side env-var gate (GBX_PREFER_FUSE_VOLUMES
    in the prior B1 commit) is gone — routing is now purely structural (None vs ref).
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        window = (0, 0, 4, 3)

        # file_ref=None: both env states give same FUSE-direct result
        for env_val in (None, "1"):
            if env_val is None:
                monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
            else:
                monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", env_val)

            tile = VirtualTile(cellid=0, path=tmp, window=window)
            with open_tile(tile, file_ref=None) as ds:
                pixels = ds.read(1)
            expected = np.arange(12, dtype="float32").reshape(3, 4)
            np.testing.assert_array_equal(pixels, expected)

        # file_ref present: stream is used regardless of env
        for env_val in (None, "1"):
            if env_val is None:
                monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
            else:
                monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", env_val)

            stub = _StubStreamRef(gtiff_bytes)
            tile = VirtualTile(cellid=0, path=tmp, window=window)
            with open_tile(tile, file_ref=stub) as ds:
                _ = ds.read(1)
            assert (
                stub.open_called
            ), f"file_ref.open() must be called when file_ref is present (env={env_val!r})"
    finally:
        os.remove(tmp)


# ---------------------------------------------------------------------------
# D. CRITICAL SCOPING TEST
# ---------------------------------------------------------------------------


def test_file_ref_arg_size_gate_all_three_conditions_required(spark, monkeypatch):
    """CRITICAL SIZE GATE: FUSE-direct requires ALL THREE conditions — size is the new gate.

    With DEFAULT env (GBX_DISABLE_FUSE_DIRECT unset), file_ref_arg returns a CASE WHEN:

        WHEN (tile.window IS NULL)                     -- whole-file only
          AND (path STARTS WITH /Volumes OR /dbfs)     -- FUSE-accessible only
          AND (path_file_size CAST AS LONG > cap)      -- LARGE files only
        THEN NULL                   ← FUSE-direct (lazy open, never materializes file)
        ELSE try_to_file(path)      ← stream (all other tiles)

    WHY THREE CONDITIONS MATTER:
    - Small files (path_file_size <= cap) CANNOT satisfy the third condition and
      MUST stream. Benchmarking showed small-file FUSE-direct regressed Serverless
      dir reads by ~1.6x — stream (bulk in-memory read + parse) is faster for small
      files on Serverless.
    - Large files (> cap) take FUSE-direct so they are opened lazily, never loading
      the whole file into the ~1 GB Serverless per-task memory budget.
    - A NULL or missing path_file_size makes the comparison NULL, which Spark's
      three-valued logic evaluates to FALSE in a WHEN condition — so tiles without
      a known size fall through to try_to_file (safe stream fallback).
    - Windowed tiles (window IS NOT NULL) cannot satisfy the first condition and
      always get the FileRef for amortized opens (one expensive .open() shared
      across all windows from the same source). Bypassing this would regress
      striped-raster reads 10-290x.

    This test asserts the STRUCTURE of the expression (not per-row evaluation —
    try_to_file is a Databricks built-in and is UNRESOLVED on OSS/Docker Spark).
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)

    # Must be a CASE WHEN (not a bare try_to_file)
    assert "CASE" in expr_str.upper() or "WHEN" in expr_str.upper(), (
        "SIZE GATE FAILURE: file_ref_arg must return a CASE WHEN in DEFAULT mode. "
        f"A bare try_to_file bypasses the gate and sends ALL tiles FUSE-direct. "
        f"Got: {expr_str!r}"
    )

    # The size predicate (path_file_size > cap) must appear in the WHEN condition.
    # Small whole-file tiles cannot satisfy this — they must stream.
    assert "path_file_size" in expr_str, (
        "SIZE GATE FAILURE: path_file_size missing from CASE WHEN condition. "
        "Without the size predicate, small files take FUSE-direct (regresses "
        f"Serverless ~1.6x). Got: {expr_str!r}"
    )

    # The window IS NULL predicate must appear in the WHEN condition.
    expr_lower = expr_str.lower()
    assert "window" in expr_lower, (
        f"SCOPING FAILURE: window IS NULL predicate missing from CASE WHEN condition. "
        f"Without it, even windowed tiles (window NOT NULL) would be FUSE-direct "
        f"(10-290x regression for striped rasters). Got: {expr_str!r}"
    )

    # try_to_file must appear in the OTHERWISE branch (all non-large / non-FUSE / windowed
    # tiles keep the FileRef for the governed byte-range stream).
    assert "try_to_file" in expr_str, (
        f"SIZE GATE FAILURE: try_to_file missing from CASE WHEN expression. "
        f"Non-large / windowed / remote tiles must receive the FileRef. "
        f"Got: {expr_str!r}"
    )
