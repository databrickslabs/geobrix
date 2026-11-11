"""The JV set: every function with a JVM level whose fate is not yet measured, at its JVM level against its light forms and a
floor, over one input per function, with a value fingerprint and a NULL report on every form but the floor.
"""

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import levels_jvm as jv
from databricks.labs.gbx.bench import local_timing as lt
from databricks.labs.gbx.bench import preflight
from databricks.labs.gbx.manager import levels

FNS = jv.FUNCTIONS
# the functions whose JVM level is the default with a dedicated 4x and 16x record and equal output (performance.mdx)
PROVEN = {
    "gbx_bng_cellfill", "gbx_bng_kring", "gbx_bng_tessellate", "gbx_bng_tessellateexplode",
    "gbx_rst_boundingbox", "gbx_rst_width", "gbx_rst_bng_rasterize_agg", "gbx_rst_fromcontent",
    "gbx_st_triangulate", "gbx_st_transformcrs",
}  # fmt: skip


def _of(fn):
    return [f for f in lf.FORMS[jv.MVP] if f.fn == fn]


def test_the_set_holds_every_jvm_level_not_proven_and_nothing_else():
    """From the code: every row with an L1 block, less the proven ones and TRI (the DEM set times it)."""
    rows = manifest.load().functions.values()
    want = {
        f.sql_name
        for f in rows
        if "L1" in f.levels and f.sql_name not in PROVEN | {"gbx_rst_tri"}
    }
    assert set(FNS) == want
    assert len(FNS) == len(set(FNS))


def test_the_set_covers_both_halves_of_the_list():
    rows = manifest.load().functions
    never = {fn for fn in FNS if "L1" not in levels.order(rows[fn])}
    first = {fn for fn in FNS if levels.order(rows[fn])[0] == "L1"}
    assert never | first == set(FNS)
    assert {"gbx_rst_band", "gbx_rst_setcrs"} <= never
    assert {"gbx_rst_crs", "gbx_bng_aswkb", "gbx_rst_h3_rasterize_agg"} <= first


@pytest.mark.parametrize("fn", FNS)
def test_each_function_has_its_jvm_form_its_light_forms_and_a_floor_last_on_one_input(
    fn,
):
    forms = _of(fn)
    spec = jv.spec_of(fn)
    assert {f.input for f in forms} == {spec.inp}
    names = [f.variant for f in forms]
    assert len(set(names)) == len(names)
    assert names[0] == f"l1@{spec.tag}" and forms[0].level == "L1"
    assert forms[0].computes == ("dedicated",)
    assert names[-1] == f"floor@{spec.tag}" and not forms[-1].register
    light = list(
        (manifest.load().functions[fn].levels["L3"].get("variants") or {})
    ) or ["l3"]
    assert names[1:-1] == [f"{v}@{spec.tag}" for v in light]
    assert all(f.level == "L3" for f in forms[1:])


@pytest.mark.parametrize("fn", FNS)
def test_every_form_but_the_floor_is_fingerprinted_by_value_and_reports_its_nulls(fn):
    forms = _of(fn)
    for f in forms[:-1]:
        assert f.vsql and f.vsql.format(src="t", uc="")
        assert f.sql.format(src="t", uc="")
        assert (
            f.allows_null
        )  # an error tile under load is a counted NULL, not a lost row
        if jv.spec_of(fn).shape != "gen":
            assert f.nullsql.format(src="t", uc="")
    assert not forms[-1].vsql and not forms[-1].nullsql
    assert forms[-1].sql.format(src="t", uc="")


@pytest.mark.parametrize("fn", FNS)
def test_the_levels_run_the_same_query_on_the_same_input(fn):
    """The JVM and light forms differ only in the level the SQL name is registered at."""
    forms = _of(fn)[:-1]
    assert len({f.sql for f in forms}) == 1 and len({f.vsql for f in forms}) == 1


def test_replicated_inputs_decode_each_distinct_tile_once():
    for s in jv.SPECS:
        if s.inp in jv._REPLICATED and s.value in ("raster", "png"):
            (f,) = [f for f in _of(s.fn) if f.level == "L1"]
            assert "GROUP BY" in f.vsql, s.fn


def test_the_custom_grid_is_a_column_not_a_call():
    """A `gbx_custom_grid(...)` call in the timed query would be timed with every form."""
    for s in jv.SPECS:
        if s.fn != "gbx_custom_grid":
            assert "gbx_custom_grid(" not in s.call, s.fn


@pytest.mark.parametrize("fn", FNS)
def test_each_function_has_a_local_set_that_compares_the_outputs(fn):
    spec = lt.SETS[lt.jv_set_name(fn)]
    assert spec.mvp == jv.MVP and spec.fn == fn and spec.compare_outputs
    assert spec.forms == jv.variants_of(fn)
    assert spec.forms[0].startswith("l1@") and spec.forms[-1].startswith("floor@")


def test_set_names_expand_by_list_by_input_and_all():
    assert lt.expand_sets("JV-rst_crs, JV-bng_aswkb") == ["JV-rst_crs", "JV-bng_aswkb"]
    assert len(lt.expand_sets("JV")) == len(FNS)
    dem = lt.expand_sets("JV@dem_1024_mem")
    assert dem and all(jv.spec_of("gbx_" + n[3:]).inp == "dem_1024_mem" for n in dem)


def test_output_flags_name_a_difference_and_a_missing_fingerprint():
    forms = ("l1@x", "c0@x", "floor@x")
    same = {("l1@x", 1): "v1:5", ("c0@x", 1): "v1:5"}
    assert lt.output_flags(same, forms=forms, ns=(1,)) == []
    diff = {("l1@x", 1): "v1:5", ("c0@x", 1): "v1:6"}
    assert "OUTPUTS DIFFER" in lt.output_flags(diff, forms=forms, ns=(1,))[0]
    missing = {("l1@x", 1): "123", ("c0@x", 1): "v1:6"}
    assert (
        "no value fingerprint for l1@x"
        in lt.output_flags(missing, forms=forms, ns=(1,))[0]
    )


def test_no_input_of_the_jv_forms_is_built_on_the_client_at_16x():
    forms = tuple(f.variant for f in lf.FORMS[jv.MVP])
    assert preflight.client_cap_problems(jv.MVP, (4, 16), forms=forms) == []


def test_the_run_parts_cover_every_function_once():
    flat = [fn for part in jv.PARTS.values() for fn in part]
    assert sorted(flat) == sorted(FNS)


def test_a_part_times_one_floor_per_input_and_every_form_of_its_functions():
    names = jv.part_forms(jv.PARTS["acc1"])
    assert [n for n in names if ":floor@" in n] == ["gbx_rst_crs:floor@n"]
    assert {n.split(":")[0] for n in names} == set(jv.PARTS["acc1"])
    assert all(lf.form_named(f, names) for fn in jv.PARTS["acc1"] for f in _of(fn)[:-1])
    assert not [
        n for n in jv.part_forms(jv.PARTS["acc1"], floors=False) if ":floor@" in n
    ]


def test_the_custom_aggregates_take_the_grid_as_a_constant():
    """The JVM custom aggregates evaluate the grid once, with no row: a grid column raises there (dedicated DBR 19)."""
    for fn in ("gbx_custom_cellfill", "gbx_rst_custom_rasterize_agg"):
        call = jv.spec_of(fn).call
        assert jv._G not in call and "named_struct('bound_x_min', 0L" in call, fn
    assert (
        jv._GRID_SQL.count("CAST(") == 8
    )  # the column form of the same grid (the scalars and explodes read it)
