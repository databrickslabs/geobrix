"""Tests for the rst_fromfile(materialize=True) Serverless-cap guard.

When materialize=True and the staged file exceeds the connect-aware stream cap,
rst_fromfile must raise StageTooLargeError with an actionable message rather
than silently attempting an OOM read.

Covers:
  - size > cap + materialize=True → raises StageTooLargeError
  - message contains "materialize=True unsuccessful" and "sizeInMB"
  - size <= cap + materialize=True → does NOT raise (normal execution)
  - materialize=False (virtual default) → never raises regardless of size
  - materialize=None (virtual default) → never raises regardless of size
"""

import os

import pytest

from databricks.labs.gbx.ds.file_gbx import StageTooLargeError


class TestFromfileMatializeCap:
    """rst_fromfile(materialize=True) raises StageTooLargeError when file exceeds cap."""

    def test_over_cap_raises_stage_too_large(self, tmp_path, monkeypatch):
        """size > cap raises StageTooLargeError immediately — no OOM read attempted."""
        # Create a tiny real file (content doesn't matter for the cap check)
        tif = tmp_path / "small.tif"
        tif.write_bytes(b"\x00" * 10)

        # Set a tiny cap (10 bytes) so our 10-byte file is > cap when we report 11
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "10")
        # Patch getsize so it reports a size over the cap
        monkeypatch.setattr(os.path, "getsize", lambda p: 11)
        # Patch _stage_local_if_needed to return the path as-is (not a Volumes path)
        monkeypatch.setattr(
            "databricks.labs.gbx.ds.file_gbx._stage_local_if_needed",
            lambda p: (str(p), False),
        )

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        # _fromfile_impl wraps errors with null-on-error — but we need to bypass
        # that to confirm StageTooLargeError is raised BEFORE the open() call.
        # We test via rst_fromfile which returns None on any exception via the UDF
        # boundary — instead test _fromfile_impl directly to inspect the error.
        with pytest.raises(StageTooLargeError) as exc_info:
            _fromfile_impl(str(tif), "GTiff", True)

        msg = str(exc_info.value)
        assert (
            "materialize=True unsuccessful" in msg
        ), f"Expected 'materialize=True unsuccessful' in: {msg!r}"
        assert "sizeInMB" in msg, f"Expected 'sizeInMB' in: {msg!r}"

    def test_error_message_contains_size_mib(self, tmp_path, monkeypatch):
        """Error message includes the MiB size of the file."""
        tif = tmp_path / "test.tif"
        tif.write_bytes(b"\x00" * 10)

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "10")
        monkeypatch.setattr(os.path, "getsize", lambda p: 11)
        monkeypatch.setattr(
            "databricks.labs.gbx.ds.file_gbx._stage_local_if_needed",
            lambda p: (str(p), False),
        )

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        with pytest.raises(StageTooLargeError) as exc_info:
            _fromfile_impl(str(tif), "GTiff", True)

        msg = str(exc_info.value)
        # Message should include a MiB value (e.g. "0.0 MiB") and sizeInMB param hint
        assert "MiB" in msg, f"Expected 'MiB' in: {msg!r}"

    def test_at_cap_does_not_raise(self, tmp_path, monkeypatch):
        """size == cap → 'stream' → no error raised (falls through to normal read)."""
        tif = tmp_path / "exact.tif"
        tif.write_bytes(b"\x00" * 10)

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        # Patch getsize to report size == cap (100 bytes)
        monkeypatch.setattr(os.path, "getsize", lambda p: 100)
        monkeypatch.setattr(
            "databricks.labs.gbx.ds.file_gbx._stage_local_if_needed",
            lambda p: (str(p), False),
        )

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        # Should NOT raise StageTooLargeError from the cap guard.
        # It may raise a different error (rasterio can't open the dummy file),
        # but that's fine — we just verify StageTooLargeError is NOT raised.
        try:
            _fromfile_impl(str(tif), "GTiff", True)
        except StageTooLargeError:
            pytest.fail("StageTooLargeError raised at cap — should only raise over cap")
        except Exception:
            pass  # other errors (rasterio open failure) are acceptable

    def test_below_cap_does_not_raise(self, tmp_path, monkeypatch):
        """size < cap → 'stream' → no StageTooLargeError raised."""
        tif = tmp_path / "small2.tif"
        tif.write_bytes(b"\x00" * 10)

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        monkeypatch.setattr(os.path, "getsize", lambda p: 50)
        monkeypatch.setattr(
            "databricks.labs.gbx.ds.file_gbx._stage_local_if_needed",
            lambda p: (str(p), False),
        )

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        try:
            _fromfile_impl(str(tif), "GTiff", True)
        except StageTooLargeError:
            pytest.fail(
                "StageTooLargeError raised below cap — should only raise over cap"
            )
        except Exception:
            pass  # rasterio open failure is fine


class TestVirtualDefaultNeverRaises:
    """The virtual default (materialize=False/None) must never raise StageTooLargeError."""

    def test_virtual_false_never_raises(self, tmp_path, monkeypatch):
        """materialize=False (virtual path) skips the cap guard entirely."""
        tif = tmp_path / "huge.tif"
        tif.write_bytes(b"\x00" * 10)

        # Set an absurdly small cap so any file would be "over cap"
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")
        monkeypatch.setattr(os.path, "getsize", lambda p: 1_000_000_000)

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        # Virtual path does NOT call _stage_local_if_needed — it opens the header only.
        # Should not raise StageTooLargeError.
        try:
            _fromfile_impl(str(tif), "GTiff", False)
        except StageTooLargeError:
            pytest.fail("StageTooLargeError raised on virtual (materialize=False) path")
        except Exception:
            pass  # rasterio open failure is acceptable

    def test_virtual_none_never_raises(self, tmp_path, monkeypatch):
        """materialize=None (treated as False) skips the cap guard entirely."""
        tif = tmp_path / "huge2.tif"
        tif.write_bytes(b"\x00" * 10)

        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")
        monkeypatch.setattr(os.path, "getsize", lambda p: 1_000_000_000)

        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        try:
            _fromfile_impl(str(tif), "GTiff", None)
        except StageTooLargeError:
            pytest.fail("StageTooLargeError raised on virtual (materialize=None) path")
        except Exception:
            pass  # rasterio open failure is acceptable
