"""Tests for materialize_decision — the shared connect-aware size-decision helper.

Covers:
  - size_bytes <= cap → "stream"
  - size_bytes > cap  → "fuse"  (kind="read" or "write")
  - kind="ingest" + size > cap → "error"  (explicit materialize=True guard)
  - kind="ingest" + size <= cap → "stream"
  - size_bytes is None → "stream" (safe default, matches NULL-size read gate)
  - GBX_STREAM_MAX_BYTES env override is respected (proves cap wiring)
  - classic-session cap: 256 MiB; a 200 MiB input is "stream" without override
  - kind values "read"/"write" behave identically (fuse over cap)
  - kind="ingest" differs: "error" over cap, "stream" at/below cap
"""

import pytest

from databricks.labs.gbx.ds.file_gbx import materialize_decision, report_detected_cap

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
        # ingest over cap → "error" (explicit materialize guard), not "fuse"
        assert materialize_decision(12346, kind="ingest") == "error"

    def test_none_size_is_stream_regardless_of_override(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
        assert materialize_decision(None, kind="read") == "stream"


# ---------------------------------------------------------------------------
# D. kind="ingest" over cap → "error" (explicit materialize=True guard)
# ---------------------------------------------------------------------------


class TestIngestKind:
    """kind="ingest" diverges from "read"/"write": over cap returns "error", not "fuse"."""

    def test_ingest_at_cap_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(100, kind="ingest") == "stream"

    def test_ingest_below_cap_is_stream(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(99, kind="ingest") == "stream"

    def test_ingest_over_cap_is_error(self, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(101, kind="ingest") == "error"

    def test_ingest_large_file_is_error(self):
        # 1 GiB is well over the 256 MiB classic cap
        assert materialize_decision(1024 * 1024**2, kind="ingest") == "error"

    def test_ingest_none_is_stream(self):
        # None → "stream" for all kinds (safe default)
        assert materialize_decision(None, kind="ingest") == "stream"


# ---------------------------------------------------------------------------
# E. read/write over cap still → "fuse" (unchanged by ingest branch)
# ---------------------------------------------------------------------------


class TestReadWriteStillFuse:
    """kind="read" and "write" over cap remain "fuse" — ingest branch must not affect them."""

    @pytest.mark.parametrize("kind", ["read", "write"])
    def test_over_cap_is_fuse_not_error(self, kind, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(101, kind=kind) == "fuse"

    @pytest.mark.parametrize("kind", ["read", "write"])
    def test_below_cap_is_stream(self, kind, monkeypatch):
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "100")
        assert materialize_decision(99, kind=kind) == "stream"

    @pytest.mark.parametrize("kind", ["read", "write"])
    def test_none_is_stream(self, kind):
        assert materialize_decision(None, kind=kind) == "stream"


# ---------------------------------------------------------------------------
# F. report_detected_cap — returns the connect-aware cap used by materialize_decision
# ---------------------------------------------------------------------------


class TestCapBytesOverride:
    """cap_bytes is the DRIVER-captured cap: used DIRECTLY, no session resolution.

    This is the Serverless-safety fix: write gates run materialize_decision from a
    session-less worker, so they pass the driver-captured cap explicitly. When
    cap_bytes is given, the passed value must win regardless of session or env — it
    must NOT fall back to _connect_aware_lru_sizing (which would use the 256 MiB
    classic cap on a session-less worker).
    """

    _100MiB = 100 * 1024**2

    def test_cap_bytes_used_directly_read_over_cap_is_fuse(self):
        # 100 MiB with a 64 MiB driver cap → over cap → "fuse" for read.
        assert (
            materialize_decision(self._100MiB, kind="read", cap_bytes=_64MiB) == "fuse"
        )

    def test_cap_bytes_used_directly_write_over_cap_is_fuse(self):
        assert (
            materialize_decision(self._100MiB, kind="write", cap_bytes=_64MiB) == "fuse"
        )

    def test_cap_bytes_used_directly_ingest_over_cap_is_error(self):
        assert (
            materialize_decision(self._100MiB, kind="ingest", cap_bytes=_64MiB)
            == "error"
        )

    def test_cap_bytes_used_directly_cog_write_over_cap_is_driver(self):
        # Over the 64 MiB cap but under the 10 GiB driver bound → "driver".
        assert (
            materialize_decision(self._100MiB, kind="cog_write", cap_bytes=_64MiB)
            == "driver"
        )

    def test_cap_bytes_at_cap_is_stream(self):
        assert materialize_decision(_64MiB, kind="read", cap_bytes=_64MiB) == "stream"

    def test_cap_bytes_below_cap_is_stream(self):
        assert (
            materialize_decision(_64MiB - 1, kind="write", cap_bytes=_64MiB) == "stream"
        )

    def test_cap_bytes_wins_over_env_override(self, monkeypatch):
        # A huge env cap would say "stream", but the explicit tiny cap_bytes wins.
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(1024**3))  # 1 GiB env cap
        assert materialize_decision(self._100MiB, kind="read", cap_bytes=_64MiB) == (
            "fuse"
        ), "cap_bytes must be used directly, not re-resolved from env/session"

    def test_cap_bytes_none_falls_back_to_session_resolution(self):
        # cap_bytes=None → classic 256 MiB fallback (no session): 100 MiB → "stream".
        assert materialize_decision(self._100MiB, kind="read", cap_bytes=None) == (
            "stream"
        )

    def test_cap_bytes_none_is_stream(self):
        # None size short-circuits before cap resolution regardless of cap_bytes.
        assert materialize_decision(None, kind="read", cap_bytes=_64MiB) == "stream"


class TestReportDetectedCap:
    """report_detected_cap returns the same cap that materialize_decision uses.

    Without a real Spark Connect session (no Serverless in unit tests), the
    classic 256 MiB cap applies.  GBX_STREAM_MAX_BYTES override must win.
    """

    def test_returns_int(self):
        cap = report_detected_cap(spark=None)
        assert isinstance(cap, int)

    def test_default_classic_cap_is_256mib(self):
        # Without a Connect session, the cap must be the classic 256 MiB.
        cap = report_detected_cap(spark=None)
        assert cap == _256MiB, (
            f"Expected classic cap 256 MiB ({_256MiB:,} bytes), got {cap:,}. "
            "If this runs on Serverless (Connect session present), the expected value "
            "is 64 MiB; this test targets the non-Connect path."
        )

    def test_env_override_wins(self, monkeypatch):
        # GBX_STREAM_MAX_BYTES overrides the cap regardless of session type.
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1234567")
        cap = report_detected_cap(spark=None)
        assert cap == 1234567

    def test_env_override_64mib(self, monkeypatch):
        # Simulate the Connect/Serverless cap via env override.
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(_64MiB))
        cap = report_detected_cap(spark=None)
        assert cap == _64MiB

    def test_consistent_with_materialize_decision_boundary(self, monkeypatch):
        # report_detected_cap returns the EXACT threshold used by materialize_decision:
        # at cap → "stream"; cap+1 → "fuse".
        monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "99999")
        cap = report_detected_cap(spark=None)
        assert materialize_decision(cap, kind="read") == "stream"
        assert materialize_decision(cap + 1, kind="read") == "fuse"
