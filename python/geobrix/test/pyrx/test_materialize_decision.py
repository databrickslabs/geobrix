"""Tests for materialize_decision — the shared connect-aware size-decision helper.

Covers:
  - size_bytes <= cap → "stream"
  - size_bytes > cap  → "fuse"
  - size_bytes is None → "stream" (safe default, matches NULL-size read gate)
  - GBX_STREAM_MAX_BYTES env override is respected (proves cap wiring)
  - classic-session cap: 256 MiB; a 200 MiB input is "stream" without override
  - kind values "read"/"write"/"ingest" all behave identically in this task
    (size-vs-cap only — no kind branching yet)
"""

import pytest

from databricks.labs.gbx.ds.file_gbx import materialize_decision

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_64MiB = 64 * 1024**2
_256MiB = 256 * 1024**2
_200MiB = 200 * 1024**2


# ---------------------------------------------------------------------------
# A. Basic stream / fuse boundary (no env override, no Spark session)
#    Without a spark session, _connect_aware_lru_sizing returns the classic
#    256 MiB cap.
# ---------------------------------------------------------------------------


class TestStreamFuseBoundary:
    """size_bytes vs the classic 256 MiB cap (no Spark session, no env override)."""

    def test_zero_bytes_is_stream(self):
        assert materialize_decision(0, kind="read") == "stream"

    def test_one_byte_is_stream(self):
        assert materialize_decision(1, kind="read") == "stream"

    def test_exactly_at_cap_is_stream(self):
        # <= cap → "stream"
        assert materialize_decision(_256MiB, kind="read") == "stream"

    def test_one_over_cap_is_fuse(self):
        # > cap → "fuse"
        assert materialize_decision(_256MiB + 1, kind="read") == "fuse"

    def test_large_file_is_fuse(self):
        assert materialize_decision(1024 * 1024**2, kind="read") == "fuse"

    def test_200mib_is_stream_with_classic_cap(self):
        # 200 MiB < 256 MiB → "stream" on classic (no session → classic fallback)
        assert materialize_decision(_200MiB, kind="read") == "stream"


# ---------------------------------------------------------------------------
# B. None size → always "stream" (safe default)
# ---------------------------------------------------------------------------


class TestNoneSize:
    def test_none_is_stream_for_read(self):
        assert materialize_decision(None, kind="read") == "stream"

    def test_none_is_stream_for_write(self):
        assert materialize_decision(None, kind="write") == "stream"

    def test_none_is_stream_for_ingest(self):
        assert materialize_decision(None, kind="ingest") == "stream"


# ---------------------------------------------------------------------------
# C. GBX_STREAM_MAX_BYTES env override wires the cap correctly
# ---------------------------------------------------------------------------


class TestEnvOverride:
    """Proves the env var reaches the cap used by materialize_decision."""

    def test_env_override_small_cap_below_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(12344, kind="read") == "stream"

    def test_env_override_small_cap_equal_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(12345, kind="read") == "stream"

    def test_env_override_small_cap_above_is_fuse(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(12346, kind="read") == "fuse"

    def test_env_override_applies_to_write_kind(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(12344, kind="write") == "stream"
        assert materialize_decision(12346, kind="write") == "fuse"

    def test_env_override_applies_to_ingest_kind(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(12344, kind="ingest") == "stream"
        assert materialize_decision(12346, kind="ingest") == "fuse"

    def test_none_size_is_stream_regardless_of_override(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(None, kind="read") == "stream"


# ---------------------------------------------------------------------------
# D. kind values are all identical in this task (size-vs-cap only)
# ---------------------------------------------------------------------------


class TestKindParity:
    """All kind values produce the same result for the same size."""

    @pytest.mark.parametrize("kind", ["read", "write", "ingest"])
    def test_below_cap_stream_for_all_kinds(self, kind):
        assert materialize_decision(1, kind=kind) == "stream"

    @pytest.mark.parametrize("kind", ["read", "write", "ingest"])
    def test_above_cap_fuse_for_all_kinds(self, kind, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(101, kind=kind) == "fuse"

    @pytest.mark.parametrize("kind", ["read", "write", "ingest"])
    def test_none_stream_for_all_kinds(self, kind):
        assert materialize_decision(None, kind=kind) == "stream"
