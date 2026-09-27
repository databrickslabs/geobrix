"""Tests for pyrx.core.compression authority."""

import pytest

from databricks.labs.gbx.pyrx.core import compression as C


def test_predictor_for_dtype():
    assert C.predictor_for("float32") == 3
    assert C.predictor_for("float64") == 3
    assert C.predictor_for("int16") == 2
    assert C.predictor_for("uint16") == 2
    assert C.predictor_for("uint8") == 1
    assert C.predictor_for("int8") == 1


def test_auto_level_scales_down_with_size():
    # New ladder: ≤128 MiB → L6, ≤1 GiB → L3, >1 GiB → L1
    # Grounded by .superpowers/sdd/2026-09-10-grid-fidelity-stage3/followup-compression-matrix-report.md:
    # L6 is the universal knee; L9/L12 add nothing; L16 often regresses.
    small = C.auto_level(1 * 1024**2)  # 1 MiB -> L6 (<=128 MiB)
    mid = C.auto_level(200 * 1024**2)  # 200 MiB -> L3 (<=1 GiB)
    big = C.auto_level(2 * 1024**3)  # 2 GiB -> L1 (>1 GiB)
    assert small > mid > big  # monotonic non-increasing (strict across bands)
    assert big == 1  # >1 GiB OOM guard: L1 (was L6 before spike revision)


def test_creation_opts_auto_zstd_with_predictor():
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="GTiff"
    )
    assert o["compress"] == "zstd"
    assert o["predictor"] == "3"
    assert int(o["zstd_level"]) == C.auto_level(1 * 1024**2)


def test_creation_opts_explicit_codec_and_level():
    o = C.creation_opts("int16", compress="deflate", level=9)
    assert o["compress"] == "deflate"
    assert o["zlevel"] == "9"
    assert o["predictor"] == "2"


def test_creation_opts_explicit_predictor_override():
    o = C.creation_opts("float32", compress="zstd", level=9, predictor=1)
    assert o["predictor"] == "1"


def test_creation_opts_none():
    o = C.creation_opts("uint8", compress="none")
    assert o.get("compress") in (None, "none")  # no compression
    # a 'none' profile must not carry zstd_level/zlevel/predictor
    assert "zstd_level" not in o and "zlevel" not in o


def test_auto_plus_explicit_level_warns():
    with pytest.warns(UserWarning, match="auto"):
        C.creation_opts("int16", decoded_bytes=1024, compress="auto", level=22)


def test_auto_without_decoded_bytes_uses_balanced_default():
    o = C.creation_opts("int16", decoded_bytes=None, compress="auto")
    assert o["compress"] == "zstd"
    assert int(o["zstd_level"]) == C._AUTO_DEFAULT_LEVEL


def test_creation_opts_cog_driver_emits_level_not_zstd_level():
    """FIX 1: COG driver uses LEVEL (not zstd_level) for ZSTD level."""
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="COG"
    )
    assert o["compress"] == "zstd"
    assert "LEVEL" in o  # COG driver option
    assert "zstd_level" not in o  # GTiff-only option
    assert o["LEVEL"] == str(C.auto_level(1 * 1024**2))


def test_creation_opts_gtiff_driver_emits_zstd_level():
    """FIX 1: GTiff driver uses zstd_level (as before)."""
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="GTiff"
    )
    assert o["compress"] == "zstd"
    assert "zstd_level" in o  # GTiff option
    assert "LEVEL" not in o  # COG-only option
    assert o["zstd_level"] == str(C.auto_level(1 * 1024**2))


def test_creation_opts_cog_explicit_zstd_with_level():
    """FIX 1: COG with explicit ZSTD uses LEVEL."""
    o = C.creation_opts("int16", compress="zstd", level=18, driver="COG")
    assert o["compress"] == "zstd"
    assert o["LEVEL"] == "18"
    assert "zstd_level" not in o


def test_creation_opts_cog_deflate_uses_level():
    """FIX 1: COG DEFLATE also uses LEVEL."""
    o = C.creation_opts("int16", compress="deflate", level=9, driver="COG")
    assert o["compress"] == "deflate"
    assert "LEVEL" in o  # COG deflate uses LEVEL
    assert "zlevel" not in o  # GTiff-only option
    assert o["LEVEL"] == "9"


# ---------------------------------------------------------------------------
# New ladder exact values (L6 knee — grounded by compression-matrix spike)
# ---------------------------------------------------------------------------


def test_auto_default_level_is_6():
    """_AUTO_DEFAULT_LEVEL must be 6 (was 9; L6 is the universal knee)."""
    assert C._AUTO_DEFAULT_LEVEL == 6


def test_auto_level_new_ladder_boundaries():
    """Exact boundary values for the new _AUTO_LADDER."""
    # ≤128 MiB → L6
    assert C.auto_level(128 * 1024**2) == 6
    # just over 128 MiB → L3
    assert C.auto_level(128 * 1024**2 + 1) == 3
    # exactly at 1 GiB → L3
    assert C.auto_level(1 * 1024**3) == 3
    # just over 1 GiB → L1
    assert C.auto_level(1 * 1024**3 + 1) == 1
    # representative values
    assert C.auto_level(1 * 1024**2) == 6  # 1 MiB
    assert C.auto_level(200 * 1024**2) == 3  # 200 MiB
    assert C.auto_level(2 * 1024**3) == 1  # 2 GiB


# ---------------------------------------------------------------------------
# GBX_ZSTD_LEVEL knob
# ---------------------------------------------------------------------------


def test_gbx_zstd_level_fast(monkeypatch):
    """GBX_ZSTD_LEVEL=fast → L1 for auto path, overriding ladder."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "fast")
    assert C.auto_level(1 * 1024**2) == 1
    assert C.auto_level(2 * 1024**3) == 1  # size doesn't matter when knob set


def test_gbx_zstd_level_max(monkeypatch):
    """GBX_ZSTD_LEVEL=max → L9 for auto path, overriding ladder."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "max")
    assert C.auto_level(1 * 1024**2) == 9
    assert C.auto_level(2 * 1024**3) == 9


def test_gbx_zstd_level_explicit_integer(monkeypatch):
    """GBX_ZSTD_LEVEL=3 → fixed L3 regardless of decoded size."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "3")
    assert C.auto_level(1 * 1024**2) == 3
    assert C.auto_level(2 * 1024**3) == 3  # size doesn't matter when knob set


def test_gbx_zstd_level_default_keeps_ladder(monkeypatch):
    """GBX_ZSTD_LEVEL=default → size-adaptive ladder (same as unset)."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "default")
    assert C.auto_level(1 * 1024**2) == 6  # ≤128 MiB → L6
    assert C.auto_level(200 * 1024**2) == 3  # ≤1 GiB → L3
    assert C.auto_level(2 * 1024**3) == 1  # >1 GiB → L1


def test_gbx_zstd_level_unset_keeps_ladder(monkeypatch):
    """GBX_ZSTD_LEVEL unset → size-adaptive ladder."""
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)
    assert C.auto_level(1 * 1024**2) == 6  # ≤128 MiB → L6
    assert C.auto_level(200 * 1024**2) == 3  # ≤1 GiB → L3
    assert C.auto_level(2 * 1024**3) == 1  # >1 GiB → L1


def test_gbx_zstd_level_none_returns_default_level(monkeypatch):
    """GBX_ZSTD_LEVEL unset, decoded_bytes=None → _AUTO_DEFAULT_LEVEL."""
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)
    assert C.auto_level(None) == C._AUTO_DEFAULT_LEVEL


def test_gbx_zstd_level_invalid_out_of_range_warns_and_falls_back(monkeypatch):
    """GBX_ZSTD_LEVEL with out-of-range integer → warn + ladder fallback."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "99")
    with pytest.warns(UserWarning, match="GBX_ZSTD_LEVEL"):
        result = C.auto_level(1 * 1024**2)
    assert result == 6  # ladder: 1 MiB → L6


def test_gbx_zstd_level_invalid_non_integer_warns_and_falls_back(monkeypatch):
    """GBX_ZSTD_LEVEL with unrecognised string → warn + ladder fallback."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "foo")
    with pytest.warns(UserWarning, match="GBX_ZSTD_LEVEL"):
        result = C.auto_level(1 * 1024**2)
    assert result == 6  # ladder: 1 MiB → L6


def test_gbx_zstd_level_knob_case_insensitive(monkeypatch):
    """GBX_ZSTD_LEVEL is parsed case-insensitively."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "FAST")
    assert C.auto_level(1 * 1024**2) == 1

    monkeypatch.setenv("GBX_ZSTD_LEVEL", "MAX")
    assert C.auto_level(1 * 1024**2) == 9

    monkeypatch.setenv("GBX_ZSTD_LEVEL", "DEFAULT")
    assert C.auto_level(1 * 1024**2) == 6  # ladder for 1 MiB


def test_gbx_zstd_level_knob_does_not_affect_explicit_compress(monkeypatch):
    """creation_opts(compress='zstd', level=9) is unaffected by GBX_ZSTD_LEVEL.

    The knob overrides ONLY the compress='auto' path; explicit codec call sites
    already carry an explicit level and must honour it.
    """
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "fast")
    o = C.creation_opts("int16", compress="zstd", level=9)
    assert o["zstd_level"] == "9"  # explicit path ignores the env knob


def test_creation_opts_gtiff_deflate_uses_zlevel():
    """FIX 1: GTiff DEFLATE uses zlevel (as before)."""
    o = C.creation_opts("int16", compress="deflate", level=9, driver="GTiff")
    assert o["compress"] == "deflate"
    assert o["zlevel"] == "9"
    assert "LEVEL" not in o


# ---------------------------------------------------------------------------
# Missing matrix: explicit ZSTD predictor for float32/uint8; full LZW matrix
# (T3 completeness additions — these test canonical which is already correct;
# they are regression guards, not red→green drivers)
# ---------------------------------------------------------------------------


def test_creation_opts_explicit_zstd_float32_predictor_3():
    """Explicit compress='zstd' + float32 must derive predictor=3 from dtype.

    test_creation_opts_auto_zstd_with_predictor covers the auto path.  This covers
    the explicit codec path (different code branch in creation_opts).
    """
    o = C.creation_opts("float32", compress="zstd", level=9)
    assert o["compress"] == "zstd"
    assert o["predictor"] == "3"
    assert o["zstd_level"] == "9"


def test_creation_opts_explicit_zstd_uint8_predictor_1():
    """Explicit compress='zstd' + uint8 must derive predictor=1 (no predictor for byte data)."""
    o = C.creation_opts("uint8", compress="zstd", level=6)
    assert o["compress"] == "zstd"
    assert o["predictor"] == "1"


def test_creation_opts_explicit_deflate_uint8_predictor_1():
    """Explicit compress='deflate' + uint8 must derive predictor=1.

    DIV-2 guard: pre-fold _write.py used '3 if _is_float else 2', returning 2 for uint8.
    Canonical creation_opts must return predictor=1 (matching heavy OperatorOptions).
    """
    o = C.creation_opts("uint8", compress="deflate", level=6)
    assert o["compress"] == "deflate"
    assert o["predictor"] == "1"
    assert o["zlevel"] == "6"


def test_creation_opts_lzw_float32():
    """LZW + float32 must carry predictor=3 (floating-point horizontal differencing)."""
    o = C.creation_opts("float32", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "3"


def test_creation_opts_lzw_int16():
    """LZW + int16 must carry predictor=2 (integer horizontal differencing)."""
    o = C.creation_opts("int16", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "2"


def test_creation_opts_lzw_uint8_predictor_1():
    """LZW + uint8 must carry predictor=1 (no predictor for byte data)."""
    o = C.creation_opts("uint8", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "1"
