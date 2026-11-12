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
    """From the code: every row with an L1 block, less the proven ones."""
    rows = manifest.load().functions.values()
    want = {f.sql_name for f in rows if "L1" in f.levels and f.sql_name not in PROVEN}
    assert set(FNS) == want
    assert len(FNS) == len(set(FNS))


def test_the_set_covers_the_jvm_levels_ordered_first_and_never():
    rows = manifest.load().functions
    never = {fn for fn in FNS if "L1" not in levels.order(rows[fn])}
    first = {fn for fn in FNS if levels.order(rows[fn])[0] == "L1"}
    assert never | first == set(FNS)
    # the last JVM level the level order never picked (tilexyz) runs first since its Arrow re-measure
    assert not never
    assert {
        "gbx_rst_numbands",
        "gbx_bng_aswkb",
        "gbx_rst_h3_rasterize_agg",
        "gbx_rst_tilexyz",
    } <= first


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
    assert lt.expand_sets("JV-rst_numbands, JV-bng_aswkb") == [
        "JV-rst_numbands",
        "JV-bng_aswkb",
    ]
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
    assert [n for n in names if ":floor@" in n] == ["gbx_rst_numbands:floor@n"]
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


# --- the cull check: the functions whose JVM level was removed, timed light only on the spec of their old JVM verdict ---
CULLED_TILE_OPS = (
    "gbx_rst_band",
    "gbx_rst_getsubdataset",
    "gbx_rst_setcrs",
    "gbx_rst_setsrid",
)
# the accessors culled after the Arrow re-measure (under 1.2x of the Arrow light form at 4x and 16x)
CULLED_ACCESSORS = tuple(
    f"gbx_rst_{n}"
    for n in (
        "crs", "format", "height", "memsize", "pixelheight", "pixelwidth", "rotation", "scalex", "scaley", "skewx",
        "skewy", "srid", "upperleftx", "rastertoworldcoordy",
    )
)  # fmt: skip
CULLED = CULLED_TILE_OPS + CULLED_ACCESSORS


@pytest.mark.parametrize("fn", CULLED)
def test_a_culled_function_keeps_its_light_forms_and_a_floor_with_no_jvm_form(fn):
    """Its L1 level is gone, so only the light forms (and the floor) are timed, to check the cull against the old L1."""
    row = manifest.load().functions[fn]
    assert "L1" not in row.levels
    assert fn not in FNS  # not a JVM level whose fate is open
    spec = jv.spec_of(fn)
    forms = _of(fn)
    light = list((row.levels["L3"].get("variants") or {})) or ["l3"]
    names = [f.variant for f in forms]
    assert names == [f"{v}@{spec.tag}" for v in light] + [f"floor@{spec.tag}"]
    assert all(f.level == "L3" for f in forms)
    assert all(f.vsql and f.nullsql for f in forms[:-1])
    assert jv.part_forms([fn]) == [f"{fn}:{n}" for n in names]


def test_the_cull_check_times_the_calls_and_inputs_of_the_old_jvm_verdict():
    """The same spec as the t25f re-measure (the old L1 medians it is compared with, cross-run)."""
    want = {
        "gbx_rst_band": ("dem_1024_mem", "{fn}(tile, 1)"),
        "gbx_rst_getsubdataset": ("jv_nc_512_mem", "{fn}(tile, 'Band2')"),
        "gbx_rst_setcrs": ("dem_1024_mem", "{fn}(tile, 'EPSG:32633')"),
        "gbx_rst_setsrid": ("dem_1024_mem", "{fn}(tile, 32633)"),
    }
    assert {fn: (jv.spec_of(fn).inp, jv.spec_of(fn).call) for fn in want} == want
    assert jv.BASE_ROWS["jv_nc_512_mem"] == 128
    assert "jv_nc_512_mem" in lf.INPUTS


def test_the_culled_accessors_keep_the_calls_and_inputs_of_their_arrow_re_measure():
    """The one-argument accessors over the corpus tiles, rastertoworldcoordy over the DEM tiles (its old JV spec)."""
    for fn in CULLED_ACCESSORS:
        s = jv.spec_of(fn)
        assert s.shape == "scalar" and s.floor_args == "tile", fn
        if fn == "gbx_rst_rastertoworldcoordy":
            assert (s.inp, s.call) == ("dem_1024_mem", "{fn}(tile, 500, 300)")
        else:
            assert (s.inp, s.call) == ("tiles_n", "{fn}(tile)"), fn


# --- the transport a light tile form took: the row's `path` says it -------------------------------------------------
_ACCESSORS = tuple(
    s.fn for s in jv.SPECS if s.fn.startswith("gbx_rst_") and s.shape == "scalar"
)
TILE_UDF_FNS = _ACCESSORS + CULLED + ("gbx_rst_tri",)


def test_the_tile_udf_functions_are_the_accessors_tilexyz_the_culls_and_tri():
    assert len(_ACCESSORS) == 6 and "gbx_rst_tilexyz" in _ACCESSORS


@pytest.mark.parametrize("fn", TILE_UDF_FNS)
def test_a_light_tile_form_records_the_transport_the_session_chose(fn, monkeypatch):
    from databricks.labs.gbx.bench import levels as bl
    from databricks.labs.gbx.manager import capabilities

    assert bl.PATH_OPS[(fn, "L3")] == "tile_udf"
    monkeypatch.setattr(capabilities, "path", lambda spark, op: f"{op}:arrow")
    form = next(
        f
        for f in lf.FORMS["DEM" if fn == "gbx_rst_tri" else jv.MVP]
        if f.fn == fn and f.level == "L3"
    )
    assert bl._path_of(object(), form) == "tile_udf:arrow"


@pytest.mark.parametrize("fn", [f for f in TILE_UDF_FNS if f != "gbx_rst_tri"])
def test_the_light_variant_behind_a_tile_udf_path_is_a_session_tile_udf(fn):
    """The label is the session's choice, so it is true only of a UDF whose transport the session chooses."""
    from databricks.labs.gbx._transport import SessionTileUdf
    from databricks.labs.gbx.manifest import resolve_ref

    variants = manifest.load().functions[fn].levels["L3"]["variants"]
    assert variants
    for ref in variants.values():
        obj = resolve_ref(ref)
        obj = getattr(
            obj, "_f", obj
        )  # the Column API wraps it to carry the session's read cap
        assert isinstance(obj, SessionTileUdf), (fn, ref)


def test_a_floor_form_records_no_transport(monkeypatch):
    """A floor is the bench's own pandas UDF, not the function's light UDF: no path label."""
    from databricks.labs.gbx.bench import levels as bl
    from databricks.labs.gbx.manager import capabilities

    monkeypatch.setattr(capabilities, "path", lambda spark, op: f"{op}:arrow")
    for fn, mvp in (
        ("gbx_rst_band", jv.MVP),
        ("gbx_rst_crs", jv.MVP),
        ("gbx_rst_tri", "DEM"),
    ):
        floor = [f for f in lf.FORMS[mvp] if f.fn == fn and "floor" in f.variant]
        assert floor and all(bl._path_of(object(), f) == "" for f in floor)


# --- the proven JVM defaults re-timed against today's light transport: their rows say which one they took ---------------
# (fn, the set that times it, the capability op that picks its light transport)
RETIMED = (
    ("gbx_rst_width", "raster-accessor", "tile_udf"),
    ("gbx_rst_boundingbox", "raster-accessor", "tile_udf"),
    ("gbx_rst_fromcontent", "raster-constructor", "tile_udf"),
    ("gbx_st_triangulate", "vector-generator", "tile_udtf"),
    ("gbx_st_interpolateelevationbbox", jv.MVP, "tile_udtf"),
)


@pytest.mark.parametrize("fn,mvp,op", RETIMED)
def test_a_retimed_light_form_records_the_transport_the_session_chose(
    fn, mvp, op, monkeypatch
):
    from databricks.labs.gbx.bench import levels as bl
    from databricks.labs.gbx.manager import capabilities

    assert bl.PATH_OPS[(fn, "L3")] == op
    monkeypatch.setattr(capabilities, "path", lambda spark, o: f"{o}:arrow")
    light = [
        f
        for f in lf.FORMS[mvp]
        if f.fn == fn and f.level == "L3" and "floor" not in f.variant
    ]
    assert light and all(bl._path_of(object(), f) == f"{op}:arrow" for f in light)


@pytest.mark.parametrize("fn", [fn for fn, _, op in RETIMED if op == "tile_udf"])
def test_a_retimed_tile_udf_is_a_session_tile_udf(fn):
    from databricks.labs.gbx._transport import SessionTileUdf
    from databricks.labs.gbx.manifest import resolve_ref

    for ref in manifest.load().functions[fn].levels["L3"]["variants"].values():
        obj = resolve_ref(ref)
        assert isinstance(getattr(obj, "_f", obj), SessionTileUdf), (fn, ref)


@pytest.mark.parametrize("fn", [fn for fn, _, op in RETIMED if op == "tile_udtf"])
def test_a_retimed_table_function_takes_the_session_udtf_transport(fn):
    """The manifest's `arrow` flag is what makes the registrar wrap the UDTF in the session's transport."""
    assert manifest.load().functions[fn].levels["L3"].get("arrow") is True
