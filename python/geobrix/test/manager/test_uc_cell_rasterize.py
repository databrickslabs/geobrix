"""The cell rasterizer twins on UC (gbx_rst_<grid>_rasterize): H3, BNG, quadbin and custom grid.

The rows are RASTER_AGGREGATE's (run on every handler by test_family_cross_tier.py); this file holds what the rows do
not say: the pre-reduction recipe (`max(value) GROUP BY cellid` ahead of the array) against the session twin, the
mechanisms behind the rows with a revert that turns them red, and the refusals only the UC form has.
"""

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracle_tables import raster_aggregate as ra
from databricks.labs.gbx.pyrx.core import cellraster as cr
from databricks.labs.gbx.uc import forms
from databricks.labs.gbx.uc.handlers import _Refusal

MAN = manifest.load()
TWINS = (
    "gbx_rst_h3_rasterize",
    "gbx_rst_bng_rasterize",
    "gbx_rst_quadbin_rasterize",
    "gbx_rst_custom_rasterize",
)
RECIPES = [
    c for c in oracles.TABLES["RASTER_AGGREGATE"] if c.sql is not None and c.fn in TWINS
]


def test_every_twin_is_a_uc_function_with_its_handler_and_a_binary_result():
    for name in TWINS:
        fn = MAN.functions[name]
        assert (
            manifest.resolve_ref(fn.levels["L4"]["handler"]) is forms.HANDLERS[name]
        ), name
        assert fn.returns_uc == "BINARY", name
        assert all(p.uc for p in fn.params), name


def test_a_recipe_row_exists_for_every_twin_and_both_group_shapes():
    assert {(c.fn, c.cid.split("pre_reduced_")[1]) for c in RECIPES} == {
        (fn, kind) for fn in TWINS for kind in ("dup_max", "nan_beside")
    }


@pytest.fixture(scope="module")
def session(spark):
    register(
        spark,
        only=list(TWINS),
        _levels={fn: ("L3", "c0") for fn in TWINS},
    )
    return spark


def test_the_pre_reduced_statement_burns_the_pixels_of_the_session_twin(
    session, tmp_path
):
    """The recipe reduces the group to one value per cell in SQL, then calls the twin: the pixels are the twin's rows'."""
    ran = [(c, oracles.run_recipe(session, c, "", str(tmp_path))) for c in RECIPES]
    assert [(c.cid, m) for c, m in ran if m] == []


def test_the_recipes_fail_when_a_nan_is_not_made_null_before_max(session, tmp_path):
    """Spark ranks NaN above every number, so a plain `max` lets a NaN beat a number the rasterizer would keep."""
    from dataclasses import replace

    guard = "max(CASE WHEN isnan(value) THEN NULL ELSE value END)"
    assert all(guard in c.sql for c in RECIPES)
    reverted = [replace(c, sql=c.sql.replace(guard, "max(value)")) for c in RECIPES]
    red = {
        c.cid
        for c in reverted
        if oracles.run_recipe(session, c, "", str(tmp_path)) is not None
    }
    assert red == {c.cid for c in RECIPES if c.cid.endswith("beside")}


def test_the_uc_rows_fail_when_a_repeated_cell_keeps_its_last_value(
    monkeypatch, tmp_path
):
    """The largest value of a repeated cell is the rule of `_burn_cells`: the last-wins revert turns the dup rows red."""

    def last_wins(cells, vals, key=None, typed=True):
        return {(c if key is None else key(c)): v for c, v in zip(cells, vals)}

    rows = [
        c
        for c in oracles.TABLES["RASTER_AGGREGATE"]
        if c.fn in TWINS and "uc" in c.surfaces and c.sql is None
    ]
    assert [m for _, m in oracles.run_handlers(rows, tmpdir=str(tmp_path)) if m] == []
    monkeypatch.setattr(cr, "max_value_per_cell", last_wins)
    red = {c.cid for c, m in oracles.run_handlers(rows, tmpdir=str(tmp_path)) if m}
    assert {"h3rt_dup_max", "qbrt_dup_max", "cgrt_dup_max", "brt_dup_max"} <= red


# --- the refusals only the UC form has -----------------------------------------------------------------------------
def _cells(n):
    return [{"cellid": 612_000_000_000_000_000 + i, "value": 1.0} for i in range(n)]


@pytest.mark.parametrize(
    "fn,args,agg",
    [
        (
            "rst_h3_rasterize",
            (4326, None, None, None, None, None, None, None, "centroids", 0, None),
            "gbx_rst_h3_rasterize_agg",
        ),
        (
            "rst_quadbin_rasterize",
            (4326, None, None, None, None, None, None, None, "centroids", 0, None),
            "gbx_rst_quadbin_rasterize_agg",
        ),
        (
            "rst_bng_rasterize",
            (27700, None, None, None, None, None, None, "centroids", 0, None),
            "gbx_rst_bng_rasterize_agg",
        ),
    ],
)
def test_an_array_over_the_cap_is_refused_with_the_aggregate(
    monkeypatch, fn, args, agg
):
    monkeypatch.setattr(forms, "DECODED_CAP", 80)
    cells = (
        [{"cellid": "TQ3080", "value": 1.0}] * 5
        if fn == "rst_bng_rasterize"
        else _cells(5)
    )
    with pytest.raises(_Refusal, match=f"Fix: run {agg} in a notebook"):
        getattr(forms, fn)(cells, *args)


def test_a_custom_array_over_the_cap_and_a_grid_without_a_crs(monkeypatch):
    grid = dict(ra.CUSTOM_GRID)
    canvas = (None,) * 7 + ("centroids", 0)
    ok = [{"cellid": ra.cg_id(530, 180, 0), "value": 1.0}]
    assert forms.rst_custom_rasterize(None, grid, 27700, *canvas) is None
    assert forms.rst_custom_rasterize(ok, None, 27700, *canvas) is None
    with pytest.raises(ValueError, match="no CRS.*Fix: supply out_srid"):
        forms.rst_custom_rasterize(ok, {**grid, "srid": -1}, None, *canvas)
    with pytest.raises(ValueError, match="not a custom grid.*Fix: build it"):
        forms.rst_custom_rasterize(ok, {"srid": 1}, 27700, *canvas)
    monkeypatch.setattr(forms, "DECODED_CAP", 80)
    with pytest.raises(
        _Refusal, match="Fix: run gbx_rst_custom_rasterize_agg in a notebook"
    ):
        forms.rst_custom_rasterize(ok * 5, grid, 27700, *canvas)


def test_the_bng_twin_refuses_an_out_crs_and_reads_no_out_srid():
    cells = [{"cellid": "TQ3080", "value": 2.0}]
    canvas = (1.0, 530000.0, 180000.0, 531000.0, 181000.0, 1, 1, "centroids", 0)
    with pytest.raises(ValueError, match="out_crs is not supported.*EPSG:27700"):
        forms.rst_bng_rasterize(cells, None, *canvas, "EPSG:3857")
    a = forms.rst_bng_rasterize(cells, None, *canvas, None)
    b = forms.rst_bng_rasterize(cells, 4326, *canvas, None)
    assert a == b and a is not None


def test_a_derived_canvas_over_the_cap_is_refused_before_it_is_allocated(monkeypatch):
    def fail(*_a, **_k):
        raise AssertionError("the burn ran: the canvas was not refused first")

    monkeypatch.setattr(cr, "cells_to_raster", fail)
    monkeypatch.setattr("databricks.labs.gbx.uc.tier_b.DECODED_CAP", 10_000)
    monkeypatch.setattr(forms, "DECODED_CAP", 10_000)
    far = [
        {"cellid": "TQ3080", "value": 1.0},
        {"cellid": "NT2573", "value": 1.0},
    ]
    with pytest.raises(_Refusal, match=r"grid is .*Fix: .*gbx_rst_bng_rasterize_agg"):
        forms.rst_bng_rasterize(
            far, 27700, 1.0, None, None, None, None, None, None, "centroids", 0, None
        )
