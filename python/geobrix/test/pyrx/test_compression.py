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
    small = C.auto_level(1 * 1024**2)  # 1 MiB -> L16
    mid = C.auto_level(200 * 1024**2)  # 200 MiB -> L9
    big = C.auto_level(2 * 1024**3)  # 2 GiB -> L6
    assert small > mid > big  # monotonic non-increasing (strict across bands)
    assert big <= 9 and big >= 6  # large payloads stay low (OOM guard)


def test_creation_opts_auto_zstd_with_predictor():
    o = C.creation_opts("float32", decoded_bytes=1 * 1024**2, compress="auto")
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
