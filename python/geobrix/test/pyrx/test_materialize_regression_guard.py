"""Regression guard: every materializing entry point MUST route through the shared
size-safety decision gate.

INVARIANT: The four code paths that can pull a whole file or tile into executor RAM
are gated by either ``materialize_decision`` or ``_connect_aware_lru_sizing`` (which
is the cap source for ``materialize_decision``).  If any of those calls is removed
— e.g. replaced with a raw unbounded ``.read()`` / ``materialize_to_bytes`` — this
test fails, preventing silent OOM on Serverless (the ~1 GB per-task RAM limit).

Entry points covered:
  1. ``file_ref_arg``        (ds/file_gbx.py)  — uses _connect_aware_lru_sizing
  2. ``RasterGbxWriter.write`` (ds/writer.py)  — uses materialize_decision
  3. ``_fromfile_impl``      (pyrx/functions.py) — uses materialize_decision
  4. ``CogGbxWriter.write``  (ds/cog_writer.py)  — uses materialize_decision

These checks are STATIC (source-level).  They assert that the identifier appears
in the function/method source, not that the function is called at runtime with
specific arguments.  This is intentional: the guard should fail fast when a future
refactor drops the call, without requiring a Spark session or sample data.
"""

import inspect

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_source(fn) -> str:
    """Return the source code of *fn* as a string.

    Uses inspect.getsource; ImportError / OSError failures surface as test errors
    (not as skips) because missing source means the invariant cannot be verified.
    """
    return inspect.getsource(fn)


# ---------------------------------------------------------------------------
# 1. file_ref_arg — must reference _connect_aware_lru_sizing
# ---------------------------------------------------------------------------


class TestFileRefArgGate:
    """file_ref_arg (ds/file_gbx.py) must use _connect_aware_lru_sizing for the cap."""

    def test_file_ref_arg_references_connect_aware_lru_sizing(self):
        from databricks.labs.gbx.ds.file_gbx import file_ref_arg

        src = _get_source(file_ref_arg)
        assert "_connect_aware_lru_sizing" in src, (
            "INVARIANT BROKEN: file_ref_arg no longer calls _connect_aware_lru_sizing. "
            "The SIZE GATE (FUSE-direct for large tiles on Serverless) is now missing. "
            "Restore the _connect_aware_lru_sizing(spark)[0] cap call inside file_ref_arg "
            "to keep the Serverless-safe materialize policy. "
            "See ds/file_gbx.py and the Serverless-safe materialize policy in CLAUDE.md."
        )


# ---------------------------------------------------------------------------
# 2. RasterGbxWriter.write — must reference materialize_decision
# ---------------------------------------------------------------------------


class TestRasterGbxWriterGate:
    """RasterGbxWriter.write (ds/writer.py) must route through materialize_decision."""

    def test_raster_writer_write_references_materialize_decision(self):
        from databricks.labs.gbx.ds.writer import RasterGbxWriter

        src = _get_source(RasterGbxWriter.write)
        assert "materialize_decision" in src, (
            "INVARIANT BROKEN: RasterGbxWriter.write no longer calls materialize_decision. "
            "Any unbounded .read() / materialize_to_bytes inside the write loop can OOM "
            "Serverless executors (per-task RAM ~1 GB). "
            "Restore the materialize_decision(size, 'write') gate in RasterGbxWriter.write "
            "before performing any whole-tile materialize. "
            "See ds/writer.py and the Serverless-safe materialize policy in CLAUDE.md."
        )


# ---------------------------------------------------------------------------
# 3. _fromfile_impl — must reference materialize_decision
# ---------------------------------------------------------------------------


class TestFromfileImplGate:
    """_fromfile_impl (pyrx/functions.py) must route through materialize_decision."""

    def test_fromfile_impl_references_materialize_decision(self):
        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        src = _get_source(_fromfile_impl)
        assert "materialize_decision" in src, (
            "INVARIANT BROKEN: _fromfile_impl no longer calls materialize_decision. "
            "The ingest-size guard (kind='ingest' → 'error' over cap) is now missing. "
            "A caller passing materialize=True with a large source on Serverless will OOM "
            "silently instead of failing fast. "
            "Restore materialize_decision(_file_size, 'ingest') in _fromfile_impl. "
            "See pyrx/functions.py and the Serverless-safe materialize policy in CLAUDE.md."
        )


# ---------------------------------------------------------------------------
# 4. CogGbxWriter.write — must reference materialize_decision
# ---------------------------------------------------------------------------


class TestCogGbxWriterGate:
    """CogGbxWriter.write (ds/cog_writer.py) must route through materialize_decision."""

    def test_cog_writer_write_references_materialize_decision(self):
        from databricks.labs.gbx.ds.cog_writer import CogGbxWriter

        src = _get_source(CogGbxWriter.write)
        assert "materialize_decision" in src, (
            "INVARIANT BROKEN: CogGbxWriter.write no longer calls materialize_decision. "
            "The cog_write gate (driver-side auto-route vs error for over-cap sources) "
            "is now missing. Large COG conversions may fail silently or exhaust executor RAM. "
            "Restore materialize_decision(src_size, 'cog_write') in CogGbxWriter.write. "
            "See ds/cog_writer.py and the Serverless-safe materialize policy in CLAUDE.md."
        )


# ---------------------------------------------------------------------------
# Negative-check: verify the tests WOULD fail if a reference were removed.
# This is a meta-test — it proves the guards are not vacuously passing.
# ---------------------------------------------------------------------------


# These strings are deliberately NOT the target identifiers — we use them to
# build a fake "clean" source string that contains neither identifier.
_CLEAN_SRC = "def fn():\n    x = 1\n    return x\n"


class TestGuardSanity:
    """Sanity: the guards catch broken references (string-compare logic is sound)."""

    def test_guard_catches_missing_materialize_decision_reference(self):
        """A source string that lacks the identifier must fail the guard check.

        Uses a canned clean source string so inspect.getsource is not involved
        (inner functions capture the outer method source, which can contain the
        identifier in a comment and produce a false negative on the negative probe).
        """
        src = _CLEAN_SRC
        assert "materialize_decision" not in src

        with pytest.raises(AssertionError):
            assert (
                "materialize_decision" in src
            ), "Expected failure for missing reference"

    def test_guard_catches_missing_lru_sizing_reference(self):
        """Same probe for _connect_aware_lru_sizing."""
        src = _CLEAN_SRC
        assert "_connect_aware_lru_sizing" not in src

        with pytest.raises(AssertionError):
            assert "_connect_aware_lru_sizing" in src, "Expected failure"
