"""The DEM forms: the terrain function TRI against its light forms, on one input, with equal output checked by decoded
pixels, and the JVM probes (the accessors and `fromcontent`) that report their error tiles by `last_error` text.
"""

import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight

FNS = lf.DEM_FUNCTIONS


def _of(fn):
    return [f for f in lf.FORMS["DEM"] if f.fn == fn and f.input == "dem_1024_mem"]


def test_the_set_is_tri_with_no_jvm_level():
    assert FNS == ("gbx_rst_tri",)
    from databricks.labs.gbx import manifest

    fns = manifest.load().functions
    # no terrain function has a JVM level left to measure
    for fn in (
        "gbx_rst_tri",
        "gbx_rst_aspect",
        "gbx_rst_tpi",
        "gbx_rst_roughness",
        "gbx_rst_color_relief",
        "gbx_rst_hillshade",
    ):
        assert "L1" not in fns[fn].levels


@pytest.mark.parametrize("fn", FNS)
def test_each_function_has_its_light_forms_and_a_floor_on_one_input(fn):
    got = {(f.level, f.variant) for f in _of(fn)}
    assert {
        ("L3", "engine-c0@mem"),
        ("L3", "engine-c1@mem"),
        ("L3", "floor-c0@mem"),
    } <= got
    assert not [f for f in _of(fn) if f.level == "L1"]
    assert len({f.variant for f in _of(fn)}) == len(_of(fn))


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


def test_the_probe_forms_run_only_when_named_and_report_nulls():
    probes = [f for f in lf.FORMS["DEM"] if f.opt_in and f.fn in lf.DEM_PROBE_FUNCTIONS]
    assert {f.variant for f in probes} == {"nulls-accessors@mat", "nulls@mem"}
    for f in probes:
        assert f.level == "L1" and f.allows_null and f.nullsql
        assert f.computes == ("dedicated",)
        assert f.nullsql.format(src="t", uc="")
        assert f.sql.format(src="t", uc="")
    assert not [f for f in lf.select_forms("DEM") if f.opt_in]
    named = lf.select_forms("DEM", ("gbx_rst_worldtorastercoordy:nulls-accessors@mat",))
    assert [f.variant for f in named] == ["nulls-accessors@mat"]


def test_the_accessor_probe_reads_the_jvm_kept_accessors_in_one_scan():
    (f,) = [f for f in lf.FORMS["DEM"] if f.variant == "nulls-accessors@mat"]
    from databricks.labs.gbx import manifest

    rows = manifest.load().functions
    for fn in lf.DEM_PROBE_ACCESSORS:
        assert (
            "L1" in rows[fn].levels
        ), fn  # a culled JVM level has no companion to install
        assert lf.dem_l1_name(fn) in f.sql and lf.dem_l1_name(fn) in f.nullsql
    assert f.sql.count("FROM {src}") == 1
    assert "tile.path" in f.nullsql  # which tiles


def test_no_input_of_the_set_is_built_on_the_client_at_16x():
    assert preflight.client_cap_problems("DEM", (4, 16), forms=("engine-c0@mem",)) == []
    assert (
        preflight.client_cap_problems(
            "DEM", (4, 16), forms=tuple(f.variant for f in lf.FORMS["DEM"])
        )
        == []
    )
