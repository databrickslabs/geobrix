import pytest

from databricks.labs.gbx.pygx import _bng, _custom


def test_bng_parse_safe_none_on_bad_prefix():
    assert _bng.parse_safe("!!") is None


def test_bng_parse_safe_none_on_bad_body():
    assert _bng.parse_safe("TLxy") is None


def test_bng_parse_safe_value_on_valid():
    assert _bng.parse_safe("TL") is not None


def test_bng_parse_still_raises():
    with pytest.raises(Exception):
        _bng.parse("!!")


def _conf():
    return _custom.CustomGridConf(0, 1_000_000, 0, 1_000_000, 100_000, 10, 3)


def test_custom_point_or_none_none_on_out_of_bounds():
    assert _custom.point_to_cell_id_or_none(_conf(), -5.0, 500_000.0, 0) is None


def test_custom_point_or_none_none_on_nan():
    assert _custom.point_to_cell_id_or_none(_conf(), 500_000.0, float("nan"), 0) is None


def test_custom_point_or_none_raises_on_bad_resolution():
    with pytest.raises(ValueError):
        _custom.point_to_cell_id_or_none(_conf(), 500_000.0, 500_000.0, 99)
