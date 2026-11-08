"""The DEM forms: each terrain function with a JVM companion against its light form, on one input, with equal output
checked by decoded pixels and the JVM form's error tiles counted by their `last_error` text.
"""

import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight

FNS = lf.DEM_FUNCTIONS


def _of(fn):
    return [f for f in lf.FORMS["DEM"] if f.fn == fn and f.input == "dem_1024_mem"]


def test_the_set_is_the_five_companions():
    assert FNS == (
        "gbx_rst_aspect",
        "gbx_rst_tri",
        "gbx_rst_tpi",
        "gbx_rst_roughness",
        "gbx_rst_color_relief",
    )
    from databricks.labs.gbx import manifest

    fns = manifest.load().functions
    for fn in FNS:
        assert fns[fn].levels["L1"]["companion"] == lf._DEM_COMPANION[fn]


@pytest.mark.parametrize("fn", FNS)
def test_each_function_has_its_jvm_form_a_light_form_and_a_floor_on_one_input(fn):
    got = {(f.level, f.variant) for f in _of(fn)}
    light = "c0@mem" if fn == "gbx_rst_color_relief" else "engine-c0@mem"
    assert {("L1", "today@mem"), ("L3", light), ("L3", "floor-c0@mem")} <= got
    assert len({f.variant for f in _of(fn)}) == len(_of(fn))


@pytest.mark.parametrize("fn", FNS)
def test_the_jvm_form_runs_on_dedicated_compute_only_under_a_private_name(fn):
    (l1,) = [f for f in _of(fn) if f.level == "L1"]
    assert l1.computes == ("dedicated",) and not l1.register
    assert lf.dem_l1_name(fn) in l1.sql and "setup_dem_l1" in l1.setup
    # an error tile under load is a note, not a lost row
    assert l1.allows_null


@pytest.mark.parametrize("fn", FNS)
def test_the_forms_are_compared_by_decoded_pixels_and_every_input_row_is_the_same(fn):
    for f in _of(fn):
        if f.variant.startswith("floor"):
            assert not f.vsql
            continue
        assert "gbx_bench_vfp_raster" in f.vsql
        assert f.vsql.format(src="t", uc="")  # braces balanced, only {src} left
        # the four distinct tiles through the function, not the eight lowest-hash copies of one of them
        assert "GROUP BY xxhash64(tile.raster)" in f.vsql and "LIMIT" not in f.vsql
        assert f.sql.format(src="t", uc="")


@pytest.mark.parametrize("fn", FNS)
def test_the_jvm_form_reports_its_error_tiles_by_text(fn):
    (l1,) = [f for f in _of(fn) if f.level == "L1"]
    assert "metadata['last_error']" in l1.nullsql and "GROUP BY" in l1.nullsql
    assert l1.nullsql.format(src="t", uc="")
    assert lf.dem_l1_name(fn) in l1.nullsql
    assert not [f for f in _of(fn) if f.level == "L3" and f.nullsql]


def test_colour_relief_reads_its_table_from_a_column_of_the_input():
    for f in _of("gbx_rst_color_relief"):
        if not f.variant.startswith("floor"):
            assert "(tile, ct)" in f.sql


def test_the_colour_table_is_written_once_and_named_in_the_input(tmp_path):
    got = lf._dem_color_table(str(tmp_path / "corpus"))
    assert set(got) == {"ct"}
    with open(got["ct"]) as fh:
        assert fh.read() == lf.DEM_COLOR_TABLE
    assert lf._dem_color_table(str(tmp_path / "corpus")) == got


def test_the_probe_forms_run_only_when_named_and_report_nulls():
    probes = [f for f in lf.FORMS["DEM"] if f.fn in lf.DEM_PROBE_FUNCTIONS]
    assert {f.fn for f in probes} == set(lf.DEM_PROBE_FUNCTIONS)
    for f in probes:
        assert f.opt_in and f.level == "L1" and f.allows_null and f.nullsql
        assert f.nullsql.format(src="t", uc="")
    assert not [f for f in lf.select_forms("DEM") if f.fn in lf.DEM_PROBE_FUNCTIONS]
    named = lf.select_forms("DEM", ("gbx_rst_width:nulls@mat",))
    assert [f.fn for f in named] == ["gbx_rst_width"]


def test_no_input_of_the_set_is_built_on_the_client_at_16x():
    assert preflight.client_cap_problems("DEM", (4, 16), forms=("engine-c0@mem",)) == []
    assert (
        preflight.client_cap_problems(
            "DEM", (4, 16), forms=tuple(f.variant for f in lf.FORMS["DEM"])
        )
        == []
    )
