"""The UC array forms (uc.forms): the shape every one shares; the rows of the public ones are in their oracle tables."""

import h3
import mapbox_vector_tile as mvt
import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench.oracles import UcGeometry
from databricks.labs.gbx.uc import forms
from databricks.labs.gbx.uc.handlers import _Refusal

MAN = manifest.load()


def _point(lon, lat, srid=4326):
    from shapely import to_wkb
    from shapely.geometry import Point

    return UcGeometry(to_wkb(Point(lon, lat)), srid)


def test_every_private_routine_and_form_has_its_manifest_handler():
    for name, pf in MAN.private.items():
        assert manifest.resolve_ref(pf.handler) is forms.HANDLERS[name], name
    assert (
        manifest.resolve_ref(
            MAN.functions["gbx_rst_h3_rasterize"].levels["L4"]["handler"]
        )
        is forms.HANDLERS["gbx_rst_h3_rasterize"]
    )


def test_the_pyramid_array_has_the_columns_of_the_table_function():
    tvf = MAN.functions["gbx_st_asmvt_pyramid"]
    rows = forms.st_asmvt_pyramid_cells(
        _point(100, 45), '{"name": "a"}', 0, 1, "layer", 4096
    )
    assert [tuple(r) for r in rows[:1]] == [tuple(n for n, _ in tvf.columns)]
    assert [(r["z"], r["x"], r["y"]) for r in rows] == [(0, 0, 0), (1, 1, 0)]
    assert mvt.decode(rows[0]["mvt_bytes"])["layer"]["features"][0]["properties"] == {
        "name": "a"
    }


def test_the_pyramid_refuses_other_srids_bad_attrs_and_an_oversized_result(monkeypatch):
    with pytest.raises(_Refusal, match="SRID 4326.*Fix:"):
        forms.st_asmvt_pyramid_cells(_point(100, 45, 3857), None, 0, 1, None, None)
    with pytest.raises(ValueError, match="JSON.*Fix:"):
        forms.st_asmvt_pyramid_cells(_point(100, 45), "{not json", 0, 1, None, None)
    with pytest.raises(ValueError, match="JSON object.*Fix:"):
        forms.st_asmvt_pyramid_cells(_point(100, 45), "[1]", 0, 1, None, None)
    assert forms.st_asmvt_pyramid_cells(None, None, 0, 1, None, None) is None
    monkeypatch.setattr(forms, "DECODED_CAP", 100)
    with pytest.raises(_Refusal, match="Fix: run gbx_st_asmvt_pyramid in a notebook"):
        forms.st_asmvt_pyramid_cells(_point(100, 45), None, 0, 1, None, None)


def test_a_struct_cast_to_text_is_named_as_such_not_reported_as_bad_json():
    for cast in ("{1}", "{a, 1}", " {name, 2} "):
        with pytest.raises(ValueError, match="STRUCT cast to text.*Fix: pass to_json"):
            forms.st_asmvt_pyramid_cells(_point(100, 45), cast, 0, 1, None, None)
    # text that merely is not JSON keeps the parse message
    with pytest.raises(ValueError, match="attrs is not JSON"):
        forms.st_asmvt_pyramid_cells(_point(100, 45), "{not json", 0, 1, None, None)


def test_tiles_of_one_tile_merge_into_one_layer_with_every_feature():
    a = forms.st_asmvt_pyramid_cells(_point(100, 45), '{"n": 1}', 0, 0, "roads", 4096)[
        0
    ]
    b = forms.st_asmvt_pyramid_cells(_point(101, 46), '{"n": 2}', 0, 0, "roads", 4096)[
        0
    ]
    merged = forms.st_asmvt_features(
        [a["mvt_bytes"], None, b["mvt_bytes"]], "roads", 4096
    )
    layer = mvt.decode(merged)["roads"]
    want = [f for t in (a, b) for f in mvt.decode(t["mvt_bytes"])["roads"]["features"]]
    key = lambda f: f["properties"]["n"]  # noqa: E731
    assert sorted(layer["features"], key=key) == sorted(want, key=key)
    assert forms.st_asmvt_features([], "roads", 4096) is None
    assert forms.st_asmvt_features(None, "roads", 4096) is None
    with pytest.raises(ValueError, match="layer 'roads'.*Fix:"):
        forms.st_asmvt_features([a["mvt_bytes"]], "other", 4096)


def test_merging_over_the_cap_is_refused_with_the_session_aggregate(monkeypatch):
    a = forms.st_asmvt_pyramid_cells(_point(100, 45), None, 0, 0, None, None)[0][
        "mvt_bytes"
    ]
    monkeypatch.setattr(forms, "DECODED_CAP", 10)
    with pytest.raises(_Refusal, match="Fix: run gbx_st_asmvt in a notebook"):
        forms.st_asmvt_features([a], "layer", 4096)


def test_cellfill_fills_a_null_cell_with_the_mean_of_its_neighbours():
    centre = h3.str_to_int(h3.latlng_to_cell(37.77, -122.42, 8))
    ring = [h3.str_to_int(c) for c in h3.grid_ring(h3.int_to_str(centre), 1)]
    cells = [{"cellid": centre, "value": None}] + [
        {"cellid": c, "value": float(i)} for i, c in enumerate(ring[:3])
    ]
    cells.append({"cellid": None, "value": 9.0})  # a NULL id is skipped
    got = {
        r["cellid"]: r["value"] for r in forms.h3_cellfill_cells(cells, 1, "mean", 2.0)
    }
    assert got[centre] == pytest.approx(1.0) and len(got) == 4
    assert all(
        got[c] == float(i) for i, c in enumerate(ring[:3])
    )  # a valid cell passes through
    # k = 0 fills nothing; an unknown method is a parameter error
    zero = {
        r["cellid"]: r["value"] for r in forms.h3_cellfill_cells(cells, 0, "mean", 2.0)
    }
    assert zero[centre] is None
    with pytest.raises(ValueError, match="method.*Fix:"):
        forms.h3_cellfill_cells(cells, 1, "median", 2.0)
    assert forms.h3_cellfill_cells([], 1, "mean", 2.0) is None
    assert forms.h3_cellfill_cells(None, 1, "mean", 2.0) is None


def test_the_cell_arrays_over_the_cap_are_refused_with_the_session_function(
    monkeypatch,
):
    monkeypatch.setattr(forms, "DECODED_CAP", 80)
    cells = [{"cellid": 612_000_000_000_000_000 + i, "value": 1.0} for i in range(5)]
    with pytest.raises(_Refusal, match="Fix: run gbx_h3_cellfill in a notebook"):
        forms.h3_cellfill_cells(cells, 1, "mean", 2.0)
    with pytest.raises(
        _Refusal, match="Fix: run gbx_rst_h3_rasterize_agg in a notebook"
    ):
        forms.rst_h3_rasterize(
            cells, 4326, None, None, None, None, None, None, None, "centroids", 0, None
        )


def test_the_filled_array_is_the_input_of_the_h3_rasterizer():
    centre = h3.str_to_int(h3.latlng_to_cell(37.77, -122.42, 8))
    ring = [h3.str_to_int(c) for c in h3.grid_ring(h3.int_to_str(centre), 1)]
    cells = [{"cellid": centre, "value": None}] + [
        {"cellid": c, "value": 2.0} for c in ring
    ]
    filled = forms.h3_cellfill_cells(cells, 1, "mean", 2.0)
    tiff = forms.rst_h3_rasterize(
        filled, 4326, None, None, None, None, None, None, None, "centroids", 0, None
    )
    from rasterio.io import MemoryFile

    with MemoryFile(tiff) as mf, mf.open() as ds:
        assert ds.count == 1 and ds.read(1).max() == pytest.approx(2.0)
