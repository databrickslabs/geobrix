"""The engine's table function (polygonize): how its eval takes arguments and what it gives for a NULL, error, refused or
failed call. The engine run is replaced by a stub, so every row runs on the host; the oracle rows
(SPEC_ANALYSIS, `gbx_rst_polygonize`) hold the polygons themselves.
"""

import inspect
import logging

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import l3, run, spec

MAN = manifest.load()
POLY = MAN.functions["gbx_rst_polygonize"]
TILE = {"cellid": 3, "raster": b"II*\x00in", "path": None, "metadata": {}}
ROWS = [(b"\x01", 1.0), (b"\x02", 2.0)]


@pytest.fixture(autouse=True)
def _fresh_log(monkeypatch):
    l3._logged.clear()
    monkeypatch.setattr(l3, "_setup", lambda grid_dirs: None)
    yield
    l3._logged.clear()


def _handler(arrow=False):
    return l3.udtf("gbx_rst_polygonize", arrow=arrow).func()


def test_the_eval_takes_the_tile_then_each_optional_argument_by_name():
    sig = inspect.signature(_handler().eval)
    assert list(sig.parameters) == ["tile", "band", "connectedness"]
    assert [p.default for p in list(sig.parameters.values())[1:]] == [None, None]


def test_it_returns_a_table_function_of_the_geometry_and_the_value():
    from pyspark.sql.types import BinaryType, DoubleType

    got = l3.udtf("gbx_rst_polygonize")
    assert [(f.name, f.dataType) for f in got.returnType.fields] == [
        ("geom_wkb", BinaryType()),
        ("value", DoubleType()),
    ]
    from pyspark.rdd import PythonEvalType

    assert got.evalType == PythonEvalType.SQL_TABLE_UDF
    arrow = l3.udtf("gbx_rst_polygonize", arrow=True)
    assert arrow.evalType == PythonEvalType.SQL_ARROW_TABLE_UDF


def test_a_scalar_row_and_a_raster_generator_are_refused():
    with pytest.raises(ValueError, match="generator row with a features output"):
        l3.udtf("gbx_rst_slope")
    with pytest.raises(ValueError, match="generator row with a features output"):
        l3.udtf("gbx_rst_maketiles")


def test_one_row_per_feature_and_an_argument_left_out_is_its_default(monkeypatch):
    seen = {}

    def fake_run(fn, rasters, values, *a, **k):
        seen.update(values)
        return run.Result(None, None, "cmd", features=list(ROWS))

    monkeypatch.setattr(run, "run", fake_run)
    assert list(_handler().eval(TILE)) == ROWS
    assert seen == {"band": 1, "connectedness": 4}
    list(_handler().eval(TILE, 2, 8))
    assert seen == {"band": 2, "connectedness": 8}
    list(_handler().eval(TILE, connectedness=8))  # `connectedness => 8`
    assert seen == {"band": 1, "connectedness": 8}
    list(
        _handler().eval(tile=TILE, band=3)
    )  # `tile => t, band => 3`: the tile by its name
    assert seen == {"band": 3, "connectedness": 4}


def test_an_unknown_named_argument_is_refused():
    with pytest.raises(TypeError, match="unknown argument 'width'"):
        list(_handler().eval(TILE, width=3))


@pytest.mark.parametrize(
    "tile", [None, {"cellid": 0, "raster": None, "path": None, "metadata": {}}]
)
def test_a_null_or_empty_tile_gives_no_rows_and_never_runs_the_engine(
    tile, monkeypatch
):
    monkeypatch.setattr(run, "run", lambda *a, **k: pytest.fail("the engine ran"))
    assert list(_handler().eval(tile)) == []


def test_an_error_tile_gives_no_rows(monkeypatch):
    err = {
        "cellid": 0,
        "raster": None,
        "path": None,
        "metadata": {"last_error": "boom", "error_message": "boom"},
    }
    monkeypatch.setattr(run, "run", lambda *a, **k: pytest.fail("the engine ran"))
    assert list(_handler().eval(err)) == []


def test_a_failed_call_gives_no_rows_and_logs_its_reason_once(monkeypatch, caplog):
    monkeypatch.setattr(
        run, "run", lambda *a, **k: run.Result(None, "GDAL said no", "cmd")
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        assert list(_handler().eval(TILE)) == []
        assert list(_handler().eval(TILE)) == []
    assert [r.getMessage() for r in caplog.records].count(
        "gbx_rst_polygonize: NULL, GDAL said no"
    ) == 1


def test_a_null_result_gives_no_rows(monkeypatch):
    monkeypatch.setattr(run, "run", lambda *a, **k: None)
    assert list(_handler().eval(TILE)) == []


def test_a_refused_argument_raises(monkeypatch):
    def refuse(*a, **k):
        raise spec.ParamError(
            "gbx_rst_polygonize: connectedness must be one of '4', '8'"
        )

    monkeypatch.setattr(run, "run", refuse)
    with pytest.raises(spec.ParamError, match="connectedness"):
        list(_handler().eval(TILE, 1, 6))


def test_a_refused_virtual_tile_gives_no_rows_and_a_missing_file_raises(monkeypatch):
    virtual = {"cellid": 0, "raster": None, "path": "/nope/x.tif", "metadata": {}}

    def corrupt(*a, **k):
        raise run.DataError("gbx_rst_polygonize: the virtual tile could not be read")

    monkeypatch.setattr(l3, "_virtual_raster", corrupt)
    assert list(_handler().eval(virtual)) == []

    def missing(*a, **k):
        raise run.MissingSourceError("gbx_rst_polygonize: the file is missing")

    monkeypatch.setattr(l3, "_virtual_raster", missing)
    with pytest.raises(run.MissingSourceError):
        list(_handler().eval(virtual))


def test_the_polygonize_row_is_the_engines_alone():
    from databricks.labs.gbx.manager import levels

    assert POLY.gdal.out_kind == "features" and POLY.kind == "generator"
    assert set(POLY.levels) == {"L3"} and POLY.levels["L3"].get("engine") == "gdal"
    assert levels.engine_only(POLY, "L3")
    assert levels.engine_form(POLY) == "engine-c0"
