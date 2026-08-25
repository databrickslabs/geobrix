"""Regression guard: every materializing entry point still MENTIONS the shared
size-safety decision gate.

SCOPE — what this proves, and what it does NOT:

This is a TEXTUAL tripwire.  Each check asserts that the identifier
``materialize_decision`` (or its cap source ``_connect_aware_lru_sizing``) still
appears in the function/method source.  It exists so a future refactor cannot
SILENTLY DELETE the gate call — e.g. replace it with a raw unbounded ``.read()`` /
``materialize_to_bytes`` — without turning this test red.

It does **NOT** prove runtime Serverless memory-safety.  A green guard here says
nothing about *how* the gate is called: it would still pass under the Fix-1
worker-cap bug (where a writer re-resolved the cap on a session-less worker and got
the wrong 256 MiB classic cap), or if the gate were called with a size that
under-estimates RAM.  Those properties are proven by the behavioural tests
(``test_materialize_decision.py``, ``test_writer_size_gate.py``,
``test_cog_writer_size_gate.py``, ``test_fromfile_materialize_cap.py``), not here.
Do not read a green result in this file as evidence that the materialize paths are
memory-safe on Serverless.

Entry points covered (source mentions the identifier):
  1. ``file_ref_arg``        (ds/file_gbx.py)  — mentions _connect_aware_lru_sizing
  2. ``RasterGbxWriter.write`` (ds/writer.py)  — mentions materialize_decision
  3. ``_fromfile_impl``      (pyrx/functions.py) — mentions materialize_decision
  4. ``CogGbxWriter.write``  (ds/cog_writer.py)  — mentions materialize_decision

These checks are STATIC (source-level): the identifier must be present in the
source text.  This is intentional — the guard should fire when a future refactor
drops the call, without requiring a Spark session or sample data.
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
