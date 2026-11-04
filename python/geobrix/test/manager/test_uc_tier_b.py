"""The shape every UC raster tile handler (uc.tier_b) shares; each function's own rows are in its oracle table."""

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.uc import tier_b

MAN = manifest.load()
TIER_B = sorted(
    f.sql_name
    for f in MAN.functions.values()
    if f.levels.get("L4", {}).get("handler", "").startswith(f"{tier_b.__name__}:")
)


def test_every_manifest_row_has_its_handler_and_the_reverse():
    assert TIER_B and sorted(tier_b.HANDLERS) == TIER_B
    for name in TIER_B:
        ref = MAN.functions[name].levels["L4"]["handler"]
        assert manifest.resolve_ref(ref) is tier_b.HANDLERS[name], name


def _tile(tmp_path):
    return oracles.resolve_arg(oracles.Build("tile_4326"), str(tmp_path))


def test_a_tile_decoded_over_the_cap_is_refused_with_a_fix_naming_the_session_function(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        tier_b, "DECODED_CAP", 1024
    )  # the 64 x 48 float32 tile is 12 KiB decoded
    with pytest.raises(ValueError) as err:
        tier_b.HANDLERS["gbx_rst_width"](_tile(tmp_path))
    text = str(err.value)
    assert "the tile is 12 KiB decoded, over the 1 KiB" in text
    assert "Fix: run gbx_rst_width in a notebook or job session" in text


def test_the_cap_is_the_serverless_worker_cap():
    assert tier_b.DECODED_CAP == 64 * 1024 * 1024


def test_an_error_tile_is_null_and_a_null_argument_is_null(tmp_path):
    err = {"cellid": 0, "raster": None, "path": None, "metadata": {"last_error": "x"}}
    assert tier_b.HANDLERS["gbx_rst_width"](err) is None
    assert tier_b.HANDLERS["gbx_rst_bandmetadata"](_tile(tmp_path), None) is None


def test_a_band_outside_the_tile_raises_with_a_fix(tmp_path):
    with pytest.raises(
        ValueError, match=r"band 5 is outside.*Fix: pass a band from 1 to 1"
    ):
        tier_b.HANDLERS["gbx_rst_bandmetadata"](_tile(tmp_path), 5)
