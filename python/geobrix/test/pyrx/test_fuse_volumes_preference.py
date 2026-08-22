"""Tests for Approach-B FUSE-volumes optimisation (open-strategy decision).

Covers:
  - /Volumes path + GBX_PREFER_FUSE_VOLUMES=1 → FUSE-direct (as_local_file), NOT stream
  - Remote path (s3://) + GBX_PREFER_FUSE_VOLUMES=1 → stream (open_windowed_via_fileref)
  - FUSE preference absent (default) → stream used for all file_ref tiles
  - Pixel equivalence: FUSE-direct and stream-path return identical pixels for the same window
  - Materialized tile (raster set): unaffected by FUSE preference — no file_ref access
  - Byte-identical: 4-tuple windowed path unchanged (no regression)
  - file_ref_arg: FUSE path returns NULL when GBX_PREFER_FUSE_VOLUMES=1 and FILE supported;
    remote path returns try_to_file expression
"""

import io
import os
import tempfile

import numpy as np

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# StubFileRef helpers
# ---------------------------------------------------------------------------


class _StubFuseRef:
    """Stub FileRef for a FUSE-accessible source.

    Raises if .open() (stream) is called — verifies the stream is NOT used
    for FUSE paths.  .as_local_file() returns the real temp file path.
    """

    def __init__(self, local_path: str, *, allow_open: bool = False):
        self._local_path = local_path
        self._allow_open = allow_open
        self.open_called = False
        self.as_local_called = False

    def open(self):
        self.open_called = True
        if not self._allow_open:
            raise AssertionError(
                ".open() (stream) must NOT be called for /Volumes path"
            )
        with open(self._local_path, "rb") as f:
            return io.BytesIO(f.read())

    def as_local_file(self):
        self.as_local_called = True
        return self._local_path


class _StubStreamRef:
    """Stub FileRef for a remote source (stream only).

    .as_local_file() raises — verifies FUSE-fallback is NOT used for remote paths.
    """

    def __init__(self, data_bytes: bytes):
        self._data = data_bytes
        self.open_called = False

    def open(self):
        self.open_called = True
        return io.BytesIO(self._data)

    def as_local_file(self):
        raise AssertionError(
            ".as_local_file() must NOT be called for remote (s3://) path"
        )


# ---------------------------------------------------------------------------
# Open-strategy decision tests
# ---------------------------------------------------------------------------


def test_volumes_path_routes_to_fuse_direct_when_preferred(gtiff_bytes, monkeypatch):
    """/Volumes path + GBX_PREFER_FUSE_VOLUMES=1 → as_local_file() used, .open() NOT called.

    Approach B correctness: on Connect/Serverless the FUSE-direct path is ~100×
    faster than the byte-range stream, so /Volumes tiles must skip the stream.
    Pixel values must be identical to a direct windowed read.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        stub = _StubFuseRef(tmp)

        tile = VirtualTile(
            cellid=0,
            path="/Volumes/catalog/schema/volume/tile.tif",
            window=(0, 0, 4, 3),
        )

        with open_tile(tile, file_ref=stub) as ds:
            pixels = ds.read(1)

        expected = np.arange(12, dtype="float32").reshape(3, 4)
        np.testing.assert_array_equal(pixels, expected)

        assert not stub.open_called, ".open() (stream) must NOT be called for /Volumes"
        assert stub.as_local_called, ".as_local_file() must be called for /Volumes"
    finally:
        os.remove(tmp)


def test_dbfs_path_routes_to_fuse_direct_when_preferred(gtiff_bytes, monkeypatch):
    """/dbfs path + GBX_PREFER_FUSE_VOLUMES=1 → as_local_file() used, .open() NOT called."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        stub = _StubFuseRef(tmp)

        tile = VirtualTile(
            cellid=0,
            path="/dbfs/mnt/data/tile.tif",
            window=(0, 0, 4, 3),
        )

        with open_tile(tile, file_ref=stub) as ds:
            _ = ds.read(1)  # must not raise

        assert not stub.open_called, ".open() must NOT be called for /dbfs path"
        assert stub.as_local_called, ".as_local_file() must be called for /dbfs path"
    finally:
        os.remove(tmp)


def test_remote_path_routes_to_stream_when_preferred(gtiff_bytes, monkeypatch):
    """Remote (s3://) path + GBX_PREFER_FUSE_VOLUMES=1 → stream (.open()) used, not FUSE.

    Remote paths are not FUSE-mounted; the byte-range stream must be preserved.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

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
    assert stub.open_called, ".open() (stream) must be called for s3:// path"


def test_abfss_path_routes_to_stream_when_preferred(gtiff_bytes, monkeypatch):
    """Remote (abfss://) path + GBX_PREFER_FUSE_VOLUMES=1 → stream used."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    stub = _StubStreamRef(gtiff_bytes)

    tile = VirtualTile(
        cellid=0,
        path="abfss://container@account.dfs.core.windows.net/prefix/tile.tif",
        window=(0, 0, 4, 3),
    )

    with open_tile(tile, file_ref=stub) as ds:
        _ = ds.read(1)  # must not raise

    assert stub.open_called, ".open() (stream) must be called for abfss:// path"


def test_volumes_path_uses_stream_when_preference_absent(gtiff_bytes):
    """Without GBX_PREFER_FUSE_VOLUMES, /Volumes tiles use the stream (Classic behavior).

    Confirms Classic behavior unchanged: the env var must be explicitly set.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    # No monkeypatch.setenv — env var absent; Classic mode.
    os.environ.pop("GBX_PREFER_FUSE_VOLUMES", None)

    # Provide real bytes via a temp file for the stream path.
    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        # Redirect open() to return real bytes (stream path).
        real_data = gtiff_bytes

        class _ClassicRef:
            open_called = False
            as_local_called = False

            def open(self):
                _ClassicRef.open_called = True
                return io.BytesIO(real_data)

            def as_local_file(self):
                _ClassicRef.as_local_called = True
                return tmp

        ref = _ClassicRef()
        tile = VirtualTile(
            cellid=0,
            path="/Volumes/catalog/schema/volume/tile.tif",
            window=(0, 0, 4, 3),
        )

        with open_tile(tile, file_ref=ref) as ds:
            _ = ds.read(1)

        # Classic: stream path taken first (open_windowed_via_fileref uses .open())
        assert (
            ref.open_called
        ), "Classic: .open() (stream) must be called when pref absent"
    finally:
        os.remove(tmp)


# ---------------------------------------------------------------------------
# Pixel equivalence: FUSE-direct == stream for the same window
# ---------------------------------------------------------------------------


def test_fuse_direct_pixels_match_stream_path(gtiff_bytes, monkeypatch):
    """FUSE-direct and stream paths return byte-identical pixels for the same window.

    Uses the same source file for both reads; verifies no pixel divergence.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        window = (0, 0, 2, 2)

        # Stream path (GBX_PREFER_FUSE_VOLUMES NOT set → classic; remote s3 path for stream)
        os.environ.pop("GBX_PREFER_FUSE_VOLUMES", None)
        stream_ref = _StubStreamRef(gtiff_bytes)
        tile_remote = VirtualTile(cellid=0, path="s3://bucket/tile.tif", window=window)
        with open_tile(tile_remote, file_ref=stream_ref) as ds:
            pixels_stream = ds.read(1).copy()

        # FUSE path (GBX_PREFER_FUSE_VOLUMES=1; /Volumes path → as_local_file)
        monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")
        fuse_ref = _StubFuseRef(tmp)
        tile_fuse = VirtualTile(
            cellid=0,
            path="/Volumes/catalog/schema/volume/tile.tif",
            window=window,
        )
        with open_tile(tile_fuse, file_ref=fuse_ref) as ds:
            pixels_fuse = ds.read(1).copy()

        np.testing.assert_array_equal(
            pixels_stream,
            pixels_fuse,
            err_msg="FUSE-direct and stream must return identical pixels",
        )
    finally:
        os.remove(tmp)


def test_full_window_fuse_pixels_match_stream(gtiff_bytes, monkeypatch):
    """Full-window (window=None) FUSE-direct read matches full stream read."""
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        # Stream path (remote s3, no fuse pref)
        os.environ.pop("GBX_PREFER_FUSE_VOLUMES", None)
        stream_ref = _StubStreamRef(gtiff_bytes)
        tile_remote = VirtualTile(cellid=0, path="s3://bucket/tile.tif", window=None)
        with open_tile(tile_remote, file_ref=stream_ref) as ds:
            pixels_stream = ds.read(1).copy()

        # FUSE path (full window = None)
        monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")
        fuse_ref = _StubFuseRef(tmp)
        tile_fuse = VirtualTile(
            cellid=0, path="/Volumes/catalog/schema/volume/tile.tif", window=None
        )
        with open_tile(tile_fuse, file_ref=fuse_ref) as ds:
            pixels_fuse = ds.read(1).copy()

        np.testing.assert_array_equal(pixels_stream, pixels_fuse)
        assert pixels_fuse.shape == (3, 4), f"Expected (3,4) got {pixels_fuse.shape}"
    finally:
        os.remove(tmp)


# ---------------------------------------------------------------------------
# Materialized tile: unaffected by FUSE preference
# ---------------------------------------------------------------------------


def test_materialized_tile_unaffected_by_fuse_preference(gtiff_bytes, monkeypatch):
    """Materialized tile (raster set) ignores file_ref and GBX_PREFER_FUSE_VOLUMES.

    The first branch in open_tile (raster is not None) returns immediately;
    file_ref is never consulted and the FUSE preference has no effect.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    class _NeverCallRef:
        def open(self):
            raise AssertionError("file_ref.open() called on materialized tile")

        def as_local_file(self):
            raise AssertionError("file_ref.as_local_file() called on materialized tile")

    tile = VirtualTile(cellid=0, raster=gtiff_bytes)

    with open_tile(tile, file_ref=_NeverCallRef()) as ds:
        pixels = ds.read(1)

    expected = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_array_equal(pixels, expected)


# ---------------------------------------------------------------------------
# Byte-identical: 4-tuple windowed path unchanged
# ---------------------------------------------------------------------------


def test_windowed_path_byte_identical_without_file_ref(gtiff_bytes, monkeypatch):
    """4-tuple window without file_ref: behavior unchanged by GBX_PREFER_FUSE_VOLUMES.

    With file_ref=None the code goes through _stage_local_if_needed → FUSE-direct.
    Setting the preference must not change this — the result must be identical.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import open_tile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(gtiff_bytes)

        window = (1, 0, 3, 2)

        # Without preference
        os.environ.pop("GBX_PREFER_FUSE_VOLUMES", None)
        tile1 = VirtualTile(cellid=0, path=tmp, window=window)
        with open_tile(tile1) as ds:
            pixels_base = ds.read(1).copy()

        # With preference (file_ref=None, so no change expected)
        monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")
        tile2 = VirtualTile(cellid=0, path=tmp, window=window)
        with open_tile(tile2) as ds:
            pixels_pref = ds.read(1).copy()

        np.testing.assert_array_equal(pixels_base, pixels_pref)
    finally:
        os.remove(tmp)


# ---------------------------------------------------------------------------
# file_ref_arg: FUSE preference skips try_to_file for /Volumes, keeps for remote
# ---------------------------------------------------------------------------


def test_file_ref_arg_fuse_preferred_volumes_returns_null(spark, monkeypatch):
    """/Volumes path + GBX_PREFER_FUSE_VOLUMES=1 + FILE supported → NULL (no mint).

    Verifies the driver-side mint skip: on Connect/Serverless with GBX_PREFER_FUSE_VOLUMES,
    the plan-level try_to_file is skipped for FUSE paths so workers avoid the overhead.
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    # file_ref_arg is defined in file_gbx.py; mock its file_supported there.
    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        tile_col = F.col("tile")
        result = file_ref_arg(tile_col)
        expr_str = str(result._jc)
        # FUSE path → CASE WHEN conditional: WHEN path startsWith /Volumes THEN null
        # ELSE try_to_file(path) END.  Spark renders this with CASE/WHEN keywords.
        assert result is not None
        assert (
            "WHEN" in expr_str.upper() or "CASE" in expr_str.upper()
        ), f"Expected a CASE WHEN expression for FUSE-preferred path, got: {expr_str!r}"


def test_file_ref_arg_fuse_preferred_remote_keeps_try_to_file(spark, monkeypatch):
    """Remote column + GBX_PREFER_FUSE_VOLUMES=1 → try_to_file kept (CASE WHEN else branch).

    The CASE WHEN expression has try_to_file in the ELSE branch for non-FUSE paths.
    """
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    monkeypatch.setenv("GBX_PREFER_FUSE_VOLUMES", "1")

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)
        # The else branch must contain try_to_file for remote paths.
        assert (
            "try_to_file" in expr_str
        ), f"Expected try_to_file in CASE WHEN expression, got: {expr_str!r}"


def test_file_ref_arg_without_fuse_preference_unchanged(spark):
    """Without GBX_PREFER_FUSE_VOLUMES, file_ref_arg returns bare try_to_file (unchanged)."""
    from unittest import mock

    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    os.environ.pop("GBX_PREFER_FUSE_VOLUMES", None)

    with mock.patch(
        "databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True
    ):
        result = file_ref_arg(F.col("tile"))
        expr_str = str(result._jc)
        assert (
            "try_to_file" in expr_str
        ), f"Expected try_to_file in result, got: {expr_str!r}"
        # Without fuse preference: no CASE WHEN wrapping — bare try_to_file call.
        assert (
            "WHEN" not in expr_str.upper() and "CASE" not in expr_str.upper()
        ), f"Classic mode must not have CASE WHEN wrapping, got: {expr_str!r}"
