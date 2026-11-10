"""The HV forms: the five warps, fill-NoData, summary and proximity, each on one input per function with its light forms and a
floor; to_webmercator and resample_to_size, whose JVM companion is held for a decision, also carry it. Equal output is checked
by decoded values and the NULL outputs are reported by text."""

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight

FNS = lf.HV_FUNCTIONS
L1_FNS = lf.HV_L1_FUNCTIONS


def _of(fn):
    return [f for f in lf.FORMS["DEM"] if f.fn == fn]


def test_the_companions_are_the_manifests_l1_blocks_and_nothing_else_is_timed():
    fns = manifest.load().functions
    assert set(lf._HV_COMPANION) == set(L1_FNS) and len(FNS) == 8
    for fn in L1_FNS:
        assert fns[fn].levels["L1"]["companion"] == lf._HV_COMPANION[fn]
    # the six culled functions have no JVM level left to time
    for fn in set(FNS) - set(L1_FNS):
        assert "L1" not in fns[fn].levels, fn
    # clip, polygonize and viewshed have no JVM level to measure
    for fn in ("gbx_rst_clip", "gbx_rst_polygonize", "gbx_rst_viewshed"):
        assert "L1" not in fns[fn].levels
    assert not set(FNS) & set(lf.DEM_FUNCTIONS)


@pytest.mark.parametrize("fn", FNS)
def test_each_function_has_its_light_forms_a_floor_and_a_jvm_form_where_one_is_kept_on_one_input(
    fn,
):
    forms = _of(fn)
    assert len({f.input for f in forms}) == 1 and forms[0].input == lf._HV_INPUT[fn]
    assert len({f.variant for f in forms}) == len(forms)
    label = "mat" if fn in lf.HV_WARP_FUNCTIONS else "mem"
    light = {"gbx_rst_proximity": ("c0",), "gbx_rst_summary": ("engine-c0",)}.get(
        fn, ("engine-c0", "engine-c1")
    )
    today = ("today",) if fn in L1_FNS else ()
    assert {f.variant for f in forms} == {
        f"{v}@{label}" for v in (*today, *light, "floor-c0")
    }
    assert {f.level for f in forms if f.variant.startswith("today")} == (
        {"L1"} if today else set()
    )


def test_the_warps_read_the_corpus_tiles_and_the_rest_replicated_tiles():
    for fn in lf.HV_WARP_FUNCTIONS:
        assert lf._HV_INPUT[fn] == "tiles_mat"
    for fn in lf.HV_OTHER_FUNCTIONS:
        assert lf._HV_INPUT[fn].endswith("_mem")
        assert lf.BASE_ROWS[lf._HV_INPUT[fn]] == 128


@pytest.mark.parametrize("fn", L1_FNS)
def test_the_jvm_form_runs_on_dedicated_compute_only_under_a_private_name(fn):
    (l1,) = [f for f in _of(fn) if f.level == "L1"]
    assert l1.computes == ("dedicated",) and not l1.register
    assert lf.dem_l1_name(fn) in l1.sql and "setup_hv_l1" in l1.setup
    assert l1.allows_null  # an error tile under load is reported, not a lost row


@pytest.mark.parametrize("fn", FNS)
def test_every_form_but_the_floor_is_fingerprinted_by_value_and_reports_its_nulls(fn):
    for f in _of(fn):
        if f.variant.startswith("floor"):
            assert not f.vsql and not f.nullsql
            continue
        assert f.vsql.format(src="t", uc="") and f.nullsql.format(src="t", uc="")
        assert f.sql.format(src="t", uc="")
        assert ("gbx_bench_vfp_raster" in f.vsql) == (fn != "gbx_rst_summary")
    for l1 in [f for f in _of(fn) if f.level == "L1"]:
        assert "metadata['last_error']" in l1.nullsql


@pytest.mark.parametrize("fn", lf.HV_WARP_FUNCTIONS)
def test_the_warps_fingerprint_the_tiles_whose_path_hashes_to_a_bucket_not_a_sorted_top(
    fn,
):
    """A sort of the 16 MiB corpus rows took 15 to 20 minutes a form at 4000 tiles; the same tiles for every form."""
    for f in _of(fn):
        if f.vsql:
            assert "pmod(xxhash64(tile.path), 8) = 0" in f.vsql
            assert "ORDER BY" not in f.vsql and "LIMIT" not in f.vsql
        if f.nullsql:
            assert "tile.path" in f.nullsql


def test_resample_to_res_takes_the_pixel_size_of_the_tiles_own_crs():
    """The corpus holds degrees (4326) and metres: one fixed resolution would collapse or explode half of the tiles."""
    for f in _of("gbx_rst_resample_to_res"):
        if f.variant.startswith("floor"):
            continue
        assert f.sql.count("CASE WHEN tile.crs = 'EPSG:4326'") == 2


def test_proximity_has_sources_in_its_input():
    """A DEM tile's pixels are all non-zero, which would make every pixel a source and the form vacuous."""
    (f,) = [f for f in _of("gbx_rst_proximity") if f.variant == "c0@mem"]
    assert "'1,2'" in f.sql and f.input == "hv_classes_1024_mem"


def test_the_null_report_counts_distinct_texts_in_one_pass():
    q = lf._hv_nulls("fn(tile)", paths=True)
    assert q.count("FROM {src}") == 1 and "array_distinct" in q
    t = lf._hv_nulls("fn(tile)", text_output=True)
    assert "t IS NULL" in t and "last_error" not in t


def test_the_probe_and_the_other_dem_forms_are_untouched_by_the_hv_forms():
    probes = [f for f in lf.FORMS["DEM"] if f.opt_in]
    assert {f.variant for f in probes} == {"nulls-accessors@mat", "nulls@mem"}
    assert {f.fn for f in lf.FORMS["DEM"]} == {
        *lf.DEM_FUNCTIONS,
        *FNS,
        *lf.DEM_PROBE_FUNCTIONS,
    }


def test_no_input_of_the_hv_forms_is_built_on_the_client_at_16x():
    forms = tuple(f.variant for f in lf.FORMS["DEM"])
    assert preflight.client_cap_problems("DEM", (4, 16), forms=forms) == []
