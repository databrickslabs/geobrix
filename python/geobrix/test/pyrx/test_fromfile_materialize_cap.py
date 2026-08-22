"""Tests for the rst_fromfile(materialize=True) Serverless-cap guard.

When materialize=True and the source's DECODED size exceeds the connect-aware
stream cap, rst_fromfile must raise StageTooLargeError with an actionable message
rather than silently attempting an OOM full read.

Two properties are asserted here:

1. Fix 2 — the guard keys on the DECODED size (count*width*height*itemsize read
   from the header), NOT the compressed on-disk size. A highly-compressible source
   (tiny compressed, large decoded) must be rejected because the full read below
   decodes the whole raster into RAM.

2. Fix 1d — the cap is DRIVER-captured. rst_fromfile bakes the connect-aware cap as
   an f.lit at Column-build time (on the driver) and passes it into _fromfile_impl
   as ``cap_bytes``. _fromfile_impl runs inside a UDF on a session-less Serverless
   worker, so when ``cap_bytes`` is provided it is used DIRECTLY (never re-resolved
   from a session, which would wrongly use the 256 MiB classic cap). The passed
   ``cap_bytes`` wins over env / session.

Covers:
  - decoded > cap + materialize=True → raises StageTooLargeError
  - message contains "materialize=True unsuccessful", "sizeInMB", "MiB", "decoded"
  - decoded <= cap + materialize=True → does NOT raise (returns a materialized tile)
  - explicit cap_bytes is used directly and wins over the env cap
  - highly-compressible source: compressed < cap < decoded → raises (decoded gate)
  - materialize=False / None (virtual default) → never raises regardless of size
"""

import os

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.file_gbx import StageTooLargeError

# ---------------------------------------------------------------------------
# Helpers — write REAL rasters so the header-based decoded-size gate can run.
# ---------------------------------------------------------------------------


def _write_raster(
    path: str,
    width: int,
    height: int,
    count: int = 1,
    dtype: str = "float32",
    compress: "str | None" = None,
) -> "tuple[int, int]":
    """Write a real all-zeros GTiff. Returns (compressed_on_disk, decoded_bytes)."""
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    if compress:
        profile["compress"] = compress
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.zeros((count, height, width), dtype=dtype))
    decoded = count * width * height * int(np.dtype(dtype).itemsize)
    return os.path.getsize(path), decoded


class TestFromfileMaterializeCap:
    """rst_fromfile(materialize=True) raises StageTooLargeError when decoded > cap."""

    def test_over_cap_raises_stage_too_large(self, tmp_path, monkeypatch):
        """decoded > cap (via env override) raises StageTooLargeError with the
        actionable message; the OOM full read is never attempted."""
        tif = tmp_path / "big.tif"
        # 1024x1024 uint16 → 2 MiB decoded.
        _, decoded = _write_raster(str(tif), 1024, 1024, dtype="uint16")

        cap = 512 * 1024  # 512 KiB < 2 MiB decoded
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(cap))

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        with pytest.raises(StageTooLargeError) as exc_info:
            _fromfile_impl(str(tif), "GTiff", True)

        msg = str(exc_info.value)
        assert "materialize=True unsuccessful" in msg, msg
        assert "sizeInMB" in msg, msg
        assert "MiB" in msg, msg
        assert "decoded" in msg, msg

    def test_gate_uses_decoded_not_compressed(self, tmp_path, monkeypatch):
        """A highly-compressible source (small compressed, large decoded) is rejected
        because the DECODED size exceeds the cap — even though the compressed on-disk
        size does not. Proves the gate keys on decoded size, not getsize()."""
        tif = tmp_path / "compressible.tif"
        compressed, decoded = _write_raster(
            str(tif), 1024, 1024, dtype="uint16", compress="deflate"
        )

        cap = 512 * 1024  # 512 KiB
        assert compressed < cap < decoded, (
            f"invalid setup: need compressed({compressed}) < cap({cap}) "
            f"< decoded({decoded})"
        )
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(cap))

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        with pytest.raises(StageTooLargeError):
            _fromfile_impl(str(tif), "GTiff", True)

    def test_under_cap_returns_materialized_tile(self, tmp_path, monkeypatch):
        """decoded <= cap → no StageTooLargeError; a materialized tile row returns."""
        tif = tmp_path / "small.tif"
        _write_raster(str(tif), 8, 6, dtype="float32")  # 192 bytes decoded

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(256 * 1024**2))  # 256 MiB

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        result = _fromfile_impl(str(tif), "GTiff", True)
        assert result is not None, "small materialize=True should return a tile row"
        # Materialized tile carries raster bytes (raster field non-null).
        assert result["raster"] is not None


class TestDriverCapturedCapBytes:
    """Fix 1d: an explicit cap_bytes is used DIRECTLY (never re-resolved) and wins."""

    def test_cap_bytes_used_directly_raises(self, tmp_path):
        """A small explicit cap_bytes makes a modest raster over-cap → raises,
        with NO env override in play (proves cap_bytes is used directly)."""
        tif = tmp_path / "r.tif"
        _, decoded = _write_raster(str(tif), 512, 512, dtype="uint16")  # 512 KiB

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        # cap_bytes below decoded → over cap → error.
        with pytest.raises(StageTooLargeError):
            _fromfile_impl(str(tif), "GTiff", True, cap_bytes=64 * 1024)  # 64 KiB

    def test_cap_bytes_wins_over_tiny_env(self, tmp_path, monkeypatch):
        """A tiny env cap would reject, but a large explicit cap_bytes wins → no
        raise. Proves cap_bytes takes precedence over env/session resolution."""
        tif = tmp_path / "r2.tif"
        _write_raster(str(tif), 8, 6, dtype="float32")  # 192 bytes decoded

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")  # would reject everything

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        # Large explicit cap → decoded (192 B) <= cap → no StageTooLargeError.
        result = _fromfile_impl(str(tif), "GTiff", True, cap_bytes=10 * 1024**3)
        assert result is not None
        assert result["raster"] is not None


class TestVirtualDefaultNeverRaises:
    """The virtual default (materialize=False/None) must never raise StageTooLargeError."""

    def test_virtual_false_never_raises(self, tmp_path, monkeypatch):
        """materialize=False (virtual path) opens the header only — no cap guard."""
        tif = tmp_path / "v.tif"
        _write_raster(str(tif), 1024, 1024, dtype="uint16")  # 2 MiB decoded

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")  # absurdly small

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        result = _fromfile_impl(str(tif), "GTiff", False)
        assert result is not None, "virtual tile should return a row"
        # Virtual tile: raster is None (bytes-free, points at the path).
        assert result["raster"] is None

    def test_virtual_none_never_raises(self, tmp_path, monkeypatch):
        """materialize=None (treated as False) skips the cap guard entirely."""
        tif = tmp_path / "v2.tif"
        _write_raster(str(tif), 1024, 1024, dtype="uint16")

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        result = _fromfile_impl(str(tif), "GTiff", None)
        assert result is not None
        assert result["raster"] is None
