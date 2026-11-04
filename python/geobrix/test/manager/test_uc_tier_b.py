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


# --- tile-returning functions ---------------------------------------------------------------------------------------
TILE_KEYS = {
    "cellid",
    "raster",
    "path",
    "path_mode",
    "window",
    "clip_polygon",
    "clip_crs",
    "crs",
    "metadata",
}


def test_a_tile_function_returns_the_dict_tile_with_the_srid_and_the_header_metadata(
    tmp_path,
):
    out = tier_b.HANDLERS["gbx_rst_setsrid"](_tile(tmp_path), 3857)
    assert set(out) == TILE_KEYS and out["path"] is None and out["window"] is None
    assert out["crs"] == "3857"
    assert out["metadata"] == {
        "driver": "GTiff",
        "width": "64",
        "height": "48",
        "count": "1",
    }


def test_transformcrs_builds_its_tile_like_the_other_tile_functions(tmp_path):
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.uc import handlers

    out = handlers.rst_transformcrs(_tile(tmp_path), "EPSG:3857")
    assert set(out) == TILE_KEYS and out["crs"] == "3857"
    with MemoryFile(bytes(out["raster"])) as mf, mf.open() as ds:
        assert out["metadata"] == {
            "driver": "GTiff",
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }


def test_an_error_tile_goes_through_a_tile_function_unchanged_whatever_the_arguments_are():
    err = {
        "cellid": 7,
        "raster": None,
        "path": None,
        "metadata": {"last_error": "upstream"},
    }
    for name, args in (("gbx_rst_setsrid", (-1,)), ("gbx_rst_resample", (0.0, "sinc"))):
        out = tier_b.HANDLERS[name](err, *args)
        assert set(out) == TILE_KEYS and out["raster"] is None and out["cellid"] == 7
        assert out["metadata"] == {"last_error": "upstream"}


def test_a_result_decoded_over_the_cap_is_refused_before_it_is_made(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        tier_b, "DECODED_CAP", 20 * 1024
    )  # the 12 KiB tile as Float64 is 24 KiB
    with pytest.raises(ValueError) as err:
        tier_b.HANDLERS["gbx_rst_updatetype"](_tile(tmp_path), "Float64")
    text = str(err.value)
    assert "the result would be 24 KiB decoded, over the 20 KiB" in text
    assert "Fix: run gbx_rst_updatetype in a notebook or job session" in text


def test_a_raster_a_kernel_made_over_the_cap_is_refused_not_truncated(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(tier_b, "DECODED_CAP", 1024)
    with pytest.raises(
        ValueError, match=r"the result would be 12 KiB.*Fix: run gbx_rst_x"
    ):
        tier_b.tile_row("gbx_rst_x", _tile(tmp_path), _tile(tmp_path)["raster"])


def test_a_wrong_argument_raises_and_a_null_argument_is_null(tmp_path):
    tile = _tile(tmp_path)
    assert tier_b.HANDLERS["gbx_rst_setsrid"](tile, None) is None
    assert tier_b.HANDLERS["gbx_rst_setsrid"](None, 3857) is None
    with pytest.raises(
        ValueError, match=r"band_index 5 is outside.*Fix: pass a band from 1 to 1"
    ):
        tier_b.HANDLERS["gbx_rst_band"](tile, 5)


# --- an ARRAY of tiles ----------------------------------------------------------------------------------------------
def _built(name, tmp_path):
    return oracles.resolve_arg(oracles.Build(name), str(tmp_path))


def test_an_array_function_skips_an_unreadable_member_and_says_how_many(tmp_path):
    stack = _built("aa_stack_corrupt", tmp_path)
    for name, op in (
        ("gbx_rst_combineavg", "RST_CombineAvg"),
        ("gbx_rst_combinemax", "RST_CombineMax"),
        ("gbx_rst_merge", "RST_Merge"),
        ("gbx_rst_frombands", "RST_FromBands"),
    ):
        out = tier_b.HANDLERS[name](stack)
        assert out["metadata"]["last_error"] == f"{op}: skipped 1 corrupt input tile(s)"
        assert out["raster"] is not None and out["metadata"]["driver"] == "GTiff"


def test_the_cell_id_of_an_array_result_is_the_sessions_rule_for_each_function(
    tmp_path,
):
    a, b, c = _built("aa_stack", tmp_path)
    same = [{**a, "cellid": 7}, {**b, "cellid": 7}]
    differ = [{**a, "cellid": 7}, {**b, "cellid": 9}]
    assert tier_b.HANDLERS["gbx_rst_combinesum"](same)["cellid"] == 7
    assert tier_b.HANDLERS["gbx_rst_combinesum"](differ)["cellid"] == -1
    assert tier_b.HANDLERS["gbx_rst_frombands"](differ)["cellid"] == 7
    assert tier_b.HANDLERS["gbx_rst_merge"](same)["cellid"] == 0
    assert tier_b.HANDLERS["gbx_rst_mapalgebra"](differ, "A + B")["cellid"] == 7


def test_a_null_array_or_expression_is_null_and_an_array_of_error_tiles_answers_the_first(
    tmp_path,
):
    err = {"cellid": 4, "raster": None, "path": None, "metadata": {"last_error": "x"}}
    for name in ("gbx_rst_combineavg", "gbx_rst_merge", "gbx_rst_frombands"):
        assert tier_b.HANDLERS[name](None) is None and tier_b.HANDLERS[name]([]) is None
        assert tier_b.HANDLERS[name]([None, None]) is None
        assert tier_b.HANDLERS[name]([None, err])["metadata"] == {"last_error": "x"}
    assert (
        tier_b.HANDLERS["gbx_rst_mapalgebra"](_built("aa_stack", tmp_path), None)
        is None
    )


@pytest.mark.parametrize(
    "name,route",
    [
        ("gbx_rst_combinesum", "gbx_rst_retile) so each result is smaller"),
        ("gbx_rst_combinesum", "combine_by_key("),
        ("gbx_rst_merge", 'merge_by_key(..., method="vrt")'),
        ("gbx_rst_frombands", "run gbx_rst_frombands in a notebook or job session"),
    ],
)
def test_a_member_decoded_over_the_cap_is_refused_with_the_route_of_the_session_function(
    tmp_path, monkeypatch, name, route
):
    monkeypatch.setattr(
        tier_b, "DECODED_CAP", 100
    )  # an 8 x 6 Float32 member is 192 bytes
    with pytest.raises(
        ValueError, match=r"the tile is 0 KiB decoded, over the 0 KiB"
    ) as err:
        tier_b.HANDLERS[name](_built("aa_stack", tmp_path))
    assert route in str(err.value) and "Fix: " in str(err.value)


def test_members_and_result_together_over_the_cap_are_refused_before_a_pixel_is_read(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        tier_b, "DECODED_CAP", 500
    )  # three 192-byte members and a 192-byte result are 768 bytes
    stack = _built("aa_stack", tmp_path)
    with pytest.raises(ValueError, match=r"together over the 0 KiB.*Fix: ") as err:
        tier_b.HANDLERS["gbx_rst_combineavg"](stack)
    assert "combine_by_key(" in str(err.value)
    with pytest.raises(ValueError, match=r"together over the 0 KiB.*Fix: "):
        tier_b.HANDLERS["gbx_rst_mapalgebra"](stack, "A + B")


def test_a_mosaic_of_tiles_in_two_crs_over_the_cap_names_the_grid_steps_before_the_vrt(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(tier_b, "DECODED_CAP", 300)
    with pytest.raises(ValueError) as err:
        tier_b.HANDLERS["gbx_rst_merge"](_built("aa_mixed_crs", tmp_path))
    text = str(err.value)
    assert "gbx_rst_transform" in text and text.index("gbx_rst_transform") < text.index(
        "mint_vrt("
    )


def test_a_mosaic_the_sandbox_would_have_to_stream_is_refused_with_the_vrt_route(
    tmp_path, monkeypatch
):
    # the members and the result fit the cap, but the merge holds several copies of the mosaic while it builds it
    monkeypatch.setattr(tier_b, "DECODED_CAP", 10 * 1024)
    with pytest.raises(
        ValueError, match=r"over this compute's memory budget.*mint_vrt\("
    ) as err:
        tier_b.HANDLERS["gbx_rst_merge"](_built("quad_tiles", tmp_path))
    assert "Fix: " in str(err.value)


def test_a_bad_map_algebra_expression_raises_and_a_virtual_member_refuses_the_array(
    tmp_path,
):
    stack = _built("aa_stack", tmp_path)
    with pytest.raises(
        ValueError, match=r"gbx_rst_mapalgebra: .*cannot be evaluated.*Fix: "
    ):
        tier_b.HANDLERS["gbx_rst_mapalgebra"](stack, "A + Z")
    with pytest.raises(
        ValueError, match=r"gbx_rst_mapalgebra: .*A_index=9 is out of range"
    ):
        tier_b.HANDLERS["gbx_rst_mapalgebra"](stack, '{"calc": "A", "A_index": 9}')
