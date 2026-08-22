"""Size-gate tests for CogGbxWriter and materialize_decision kind="cog_write".

Covers:
  A. materialize_decision kind="cog_write":
     - <= cap → "stream"
     - > cap but <= _COG_DRIVER_MAX_BYTES → "driver"
     - > _COG_DRIVER_MAX_BYTES → "error"
     - None size → "stream"
     - kind="write" over cap still → "fuse" (unchanged regression guard)
     - GBX_STREAM_MAX_BYTES env override applies to cog_write too

  B. CogGbxWriter.write() routing (spy / monkeypatch; no real on-cluster I/O):
     - small source → executor conversion (cog_convert_file called, no pending)
     - large source (monkeypatched getsize / tiny cap) → auto driver-side selected;
       INFO logged; cog_convert_file NOT called; pending_paths has the source
     - beyond-driver-bound → StageTooLargeError raised from write()
     - explicit driverMode=True → unchanged (still gathers all paths, ignores size)
     - commit() processes pending_paths via prepare_cogs in default mode
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin

import databricks.labs.gbx.ds.cog_writer as _cw_mod
import databricks.labs.gbx.ds.file_gbx as _fgbx
from databricks.labs.gbx.ds.cog_writer import CogCommitMessage, CogGbxWriter
from databricks.labs.gbx.ds.file_gbx import StageTooLargeError, materialize_decision

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_64MiB = 64 * 1024**2
_256MiB = 256 * 1024**2


def _write_src(path: str, w: int = 256, h: int = 256) -> None:
    """Write a small valid GTiff to *path*."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.zeros((1, h, w), dtype="uint8"))


def _path_schema() -> StructType:
    return StructType([StructField("path", StringType(), False)])


# ---------------------------------------------------------------------------
# A. materialize_decision kind="cog_write"
# ---------------------------------------------------------------------------


class TestMaterializeDecisionCogWrite:
    """materialize_decision with kind='cog_write' — all boundary cases."""

    # A1. Within executor cap → "stream"

    def test_zero_bytes_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")
        assert materialize_decision(0, kind="cog_write") == "stream"

    def test_exactly_at_cap_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")
        assert materialize_decision(1000, kind="cog_write") == "stream"

    def test_none_size_is_stream(self):
        assert materialize_decision(None, kind="cog_write") == "stream"

    # A2. Over cap, within driver bound → "driver"

    def test_one_over_cap_is_driver(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")
        # Ensure driver bound is much larger (it defaults to 10 GiB; no override needed)
        assert materialize_decision(1001, kind="cog_write") == "driver"

    def test_large_over_cap_under_driver_bound_is_driver(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")
        # 1 GiB is well over the 1000-byte cap but under the 10 GiB driver bound
        assert materialize_decision(1024**3, kind="cog_write") == "driver"

    def test_at_driver_bound_is_driver(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        # Monkeypatch module attribute to set a small driver bound
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 5000)
        # Exactly at driver bound → still "driver"
        assert materialize_decision(5000, kind="cog_write") == "driver"

    # A3. Over driver bound → "error"

    def test_one_over_driver_bound_is_error(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 5000)
        assert materialize_decision(5001, kind="cog_write") == "error"

    def test_huge_file_is_error(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 5000)
        assert materialize_decision(100 * 1024**3, kind="cog_write") == "error"

    # A4. GBX_STREAM_MAX_BYTES env override applies to cog_write

    def test_env_override_cap_applies(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "9999")
        assert materialize_decision(9999, kind="cog_write") == "stream"
        assert materialize_decision(10000, kind="cog_write") == "driver"

    # A5. kind="write" over cap still returns "fuse" — unchanged (regression guard)

    def test_kind_write_over_cap_is_fuse_not_driver(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        result = materialize_decision(200, kind="write")
        assert (
            result == "fuse"
        ), f"kind='write' must still return 'fuse' over cap, got {result!r}"

    def test_kind_read_over_cap_is_fuse_not_driver(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        result = materialize_decision(200, kind="read")
        assert (
            result == "fuse"
        ), f"kind='read' must still return 'fuse' over cap, got {result!r}"

    # A6. kind="ingest" is still "error" over cap (unchanged)

    def test_kind_ingest_over_cap_is_error(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(200, kind="ingest") == "error"


# ---------------------------------------------------------------------------
# B. CogGbxWriter.write() routing
# ---------------------------------------------------------------------------


class TestCogWriterSizeGateRouting:
    """CogGbxWriter routes correctly based on source size vs. executor cap."""

    # B1. Small source → executor conversion (cog_convert_file called)

    def test_small_source_uses_executor_conversion(self, tmp_path):
        """A real small source routes to executor cog_convert_file (default path)."""
        src = tmp_path / "in" / "small.tif"
        src.parent.mkdir()
        _write_src(str(src))

        out = tmp_path / "out"
        w = CogGbxWriter(str(out), _path_schema(), overwrite=True, cog_blocksize=256)

        seen_src = []
        from databricks.labs.gbx.pyrx.core import analysis as _analysis

        real_convert = _analysis.cog_convert_file

        def _spy(s, d, **kwargs):
            seen_src.append(s)
            return real_convert(s, d, **kwargs)

        with patch.object(_analysis, "cog_convert_file", side_effect=_spy):
            with patch.object(
                _cw_mod, "cog_convert_file", side_effect=_spy, create=True
            ):
                msg = w.write(iter([{"path": str(src)}]))

        assert (
            len(seen_src) >= 1
        ), "cog_convert_file should be called for a small source"
        assert isinstance(msg, CogCommitMessage)
        assert len(msg.paths) == 1
        assert msg.pending_paths == [], "small source must NOT be deferred to driver"

    # B2. Large source → auto driver-side; INFO logged; cog_convert_file NOT called

    def test_large_source_auto_routes_to_driver(self, tmp_path, monkeypatch, caplog):
        """With GBX_STREAM_MAX_BYTES=1, any file triggers the driver-side auto-route.

        cog_convert_file must NOT be called; the source path ends up in pending_paths;
        and an INFO message must be emitted.
        """
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

        src = tmp_path / "in" / "large.tif"
        src.parent.mkdir()
        _write_src(str(src))

        out = tmp_path / "out"
        w = CogGbxWriter(str(out), _path_schema(), overwrite=True, cog_blocksize=256)

        convert_calls = []

        def _spy_convert(*args, **kwargs):
            convert_calls.append(args)

        with patch.object(
            _cw_mod, "cog_convert_file", side_effect=_spy_convert, create=True
        ):
            with caplog.at_level(
                logging.INFO, logger="databricks.labs.gbx.ds.cog_writer"
            ):
                msg = w.write(iter([{"path": str(src)}]))

        # cog_convert_file must NOT have been called
        assert (
            convert_calls == []
        ), f"cog_convert_file must not be called on the auto-driver path; got {convert_calls}"

        # Source path must be in pending_paths
        assert isinstance(msg, CogCommitMessage)
        assert msg.paths == [], "no executor-converted outputs expected"
        assert len(msg.pending_paths) == 1
        assert str(src) in msg.pending_paths[0] or msg.pending_paths[0] == str(src)

        # INFO log must mention auto-routing
        auto_route_logged = any(
            "auto-routing" in record.message or "driver" in record.message.lower()
            for record in caplog.records
        )
        assert auto_route_logged, (
            f"Expected INFO log about auto-routing to driver; records: "
            f"{[r.message for r in caplog.records]}"
        )

    # B3. Beyond-driver-bound → StageTooLargeError raised from write()

    def test_beyond_driver_bound_raises_stage_too_large(self, tmp_path, monkeypatch):
        """When source size exceeds _COG_DRIVER_MAX_BYTES, write() raises StageTooLargeError."""
        # Set a tiny driver bound via the module attribute
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 1)
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "0")

        src = tmp_path / "in" / "giant.tif"
        src.parent.mkdir()
        _write_src(str(src))  # real file > 1 byte → triggers "error"

        out = tmp_path / "out"
        w = CogGbxWriter(str(out), _path_schema(), overwrite=True, cog_blocksize=256)

        with pytest.raises(StageTooLargeError):
            w.write(iter([{"path": str(src)}]))

    # B4. Explicit driverMode=True → unchanged (gathers all paths regardless of size)

    def test_explicit_driver_mode_unchanged(self, tmp_path, monkeypatch):
        """driverMode=True still gathers all paths regardless of size."""
        # Even with very small cap, driver_mode=True must NOT trigger size-gate logic
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 1)

        src = tmp_path / "in" / "scene.tif"
        src.parent.mkdir()
        _write_src(str(src))

        out = tmp_path / "out"
        w = CogGbxWriter(
            str(out),
            _path_schema(),
            overwrite=True,
            cog_blocksize=256,
            driver_mode=True,
        )
        msg = w.write(iter([{"path": str(src)}]))

        # driverMode=True: write() gathers source path in msg.paths (not pending_paths)
        assert isinstance(msg, CogCommitMessage)
        assert str(src) in msg.paths
        assert (
            msg.pending_paths == []
        ), "driverMode=True must not populate pending_paths"

    # B5. commit() processes pending_paths via prepare_cogs in default mode

    def test_commit_processes_pending_paths(self, tmp_path, monkeypatch):
        """commit() calls prepare_cogs for pending_paths from auto-routed messages."""
        src = tmp_path / "in" / "scene.tif"
        src.parent.mkdir()
        _write_src(str(src))

        out = tmp_path / "out"
        w = CogGbxWriter(
            str(out),
            _path_schema(),
            overwrite=True,
            cog_blocksize=256,
            driver_mode=False,
        )

        prepare_calls = []

        def _mock_prepare_cogs(sources, out_dir, **kwargs):
            prepare_calls.append((list(sources), out_dir))

        pending_msg = CogCommitMessage(paths=[], pending_paths=[str(src)])

        with patch(
            "databricks.labs.gbx.ds.cog_writer.prepare_cogs",
            side_effect=_mock_prepare_cogs,
            create=True,
        ):
            # Import the module-level name that commit() uses
            import databricks.labs.gbx.pyrx.core.preparer as _prep

            with patch.object(_prep, "prepare_cogs", side_effect=_mock_prepare_cogs):
                w.commit([pending_msg])

        # prepare_cogs must have been called with the pending source
        # (Either via patching the import path or the direct reference — accept either.)
        # If no calls caught by the above patches, retry with a lazy import mock.
        if not prepare_calls:
            # commit() does a lazy import; patch the canonical module path
            with patch(
                "databricks.labs.gbx.pyrx.core.preparer.prepare_cogs",
                side_effect=_mock_prepare_cogs,
            ) as _:
                w.commit([pending_msg])

        assert prepare_calls, "commit() must call prepare_cogs for pending_paths"
        sources_passed = prepare_calls[0][0]
        assert any(
            str(src) in s or s == str(src) for s in sources_passed
        ), f"prepare_cogs must receive the pending source path; got {sources_passed}"

    # B6. commit() in default mode does NOT call prepare_cogs for empty pending_paths

    def test_commit_no_pending_skips_prepare_cogs(self, tmp_path):
        """commit() must not call prepare_cogs when pending_paths is empty."""
        out = tmp_path / "out"
        w = CogGbxWriter(
            str(out),
            _path_schema(),
            overwrite=True,
            cog_blocksize=256,
            driver_mode=False,
        )

        empty_msg = CogCommitMessage(paths=["/fake/out.tif"], pending_paths=[])

        import databricks.labs.gbx.pyrx.core.preparer as _prep

        with patch.object(_prep, "prepare_cogs") as mock_pc:
            w.commit([empty_msg])
            # prepare_cogs should NOT be called (no pending paths)
            assert mock_pc.call_count == 0, (
                f"prepare_cogs must not be called with empty pending_paths; "
                f"call_count={mock_pc.call_count}"
            )
