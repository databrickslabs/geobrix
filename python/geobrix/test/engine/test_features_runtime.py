"""The features output of the GDAL spec engine (contour): the UDF's return type, how a result becomes SQL's value, the
row-wise branch that serves it, the layer reader's refusals, and the output-format guard.

Most rows replace the engine run with a stub and run on the host; the rows that run GDAL skip where ``osgeo`` is not
rasterio's GDAL (the SPEC_DEM rows hold the contour lines themselves).
"""

import logging
import types

import numpy as np
import pytest
from pyspark.sql.types import ArrayType, BinaryType, DoubleType, StructType

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import l3, run, spec
from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

MAN = manifest.load()
CONTOUR = MAN.functions["gbx_rst_contour"]
SLOPE = MAN.functions["gbx_rst_slope"]
TILE = {"cellid": 3, "raster": b"II*\x00in", "path": None, "metadata": {}}


@pytest.fixture(autouse=True)
def _fresh_log():
    l3._logged.clear()
    yield
    l3._logged.clear()


# --- the return type and the value ---------------------------------------------------------------------------------------
def test_a_features_row_returns_an_array_of_structs_and_a_raster_row_a_tile():
    got = l3.return_type(CONTOUR)
    assert isinstance(got, ArrayType) and isinstance(got.elementType, StructType)
    assert [(f.name, f.dataType) for f in got.elementType.fields] == [
        ("geom_wkb", BinaryType()),
        ("value", DoubleType()),
    ]
    assert l3.return_type(SLOPE) == V2_TILE_SCHEMA
    odd = types.SimpleNamespace(
        sql_name="gbx_x", gdal=types.SimpleNamespace(out_kind="vector")
    )
    with pytest.raises(ValueError, match="vector output is not built"):
        l3.return_type(odd)


def test_a_result_is_a_list_of_structs_and_a_failed_call_is_null_and_logged_once(
    caplog,
):
    assert l3._value(CONTOUR, None) is None
    ok = run.Result(None, None, "cmd", features=[(b"w1", 1.5), (b"w2", 2.5)])
    assert l3._value(CONTOUR, ok) == [
        {"geom_wkb": b"w1", "value": 1.5},
        {"geom_wkb": b"w2", "value": 2.5},
    ]
    assert l3._value(CONTOUR, run.Result(None, None, "cmd", features=[])) == []
    bad = run.Result(None, "GDAL failed", "cmd")
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        assert l3._value(CONTOUR, bad) is None and l3._value(CONTOUR, bad) is None
    assert [r.getMessage() for r in caplog.records] == [
        "gbx_rst_contour: NULL, GDAL failed"
    ]


def test_the_log_keeps_the_last_reasons_only(monkeypatch, caplog):
    monkeypatch.setattr(l3, "_LOGGED_MAX", 3)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        for i in range(5):
            l3._log_once("gbx_rst_contour", f"reason {i}")
        assert len(l3._logged) == 3
        assert ("gbx_rst_contour", "reason 0") not in l3._logged
        l3._log_once("gbx_rst_contour", "reason 4")  # still held: not logged again
        l3._log_once("gbx_rst_contour", "reason 0")  # evicted: logged again
    assert [r.getMessage() for r in caplog.records].count(
        "gbx_rst_contour: NULL, reason 0"
    ) == 2
    assert [r.getMessage() for r in caplog.records].count(
        "gbx_rst_contour: NULL, reason 4"
    ) == 1


# --- the row-wise UDF of a features row ------------------------------------------------------------------------------------
def _stub(monkeypatch, result):
    monkeypatch.setattr(l3, "_setup", lambda grid_dirs: None)
    monkeypatch.setattr(
        l3, "_inputs", lambda fn, first, *a, **k: ([b"II*\x00"], None, 3)
    )
    monkeypatch.setattr(
        run, "run", lambda fn, rasters, values, section=None, src_sizes=None: result
    )


def test_the_row_wise_udf_returns_the_features_of_the_run(monkeypatch):
    _stub(monkeypatch, run.Result(None, None, "cmd", features=[(b"w", 9.0)]))
    got = l3.c0("gbx_rst_contour").func(TILE, [9.0])
    assert got == [{"geom_wkb": b"w", "value": 9.0}]


def test_a_failed_run_a_refused_input_and_an_error_tile_are_null(monkeypatch):
    _stub(monkeypatch, run.Result(None, "GDAL failed", "cmd"))
    assert l3.c0("gbx_rst_contour").func(TILE, [9.0]) is None

    def refuse(fn, first, *a, **k):
        raise run.DataError("the tile is unreadable. Fix: pass a GeoTIFF tile")

    monkeypatch.setattr(l3, "_inputs", refuse)
    assert l3.c0("gbx_rst_contour").func(TILE, [9.0]) is None
    err = {
        "cellid": 1,
        "raster": None,
        "path": None,
        "metadata": {"error_message": "up"},
    }
    monkeypatch.setattr(l3, "_inputs", lambda fn, first, *a, **k: (None, err, None))
    assert (
        l3.c0("gbx_rst_contour").func(err, [9.0]) is None
    )  # a raster row would return the error tile as it is
    assert l3.c0("gbx_rst_slope").func(err, "degrees") == err


def test_a_features_row_has_no_batch_form():
    with pytest.raises(ValueError, match="no batch form; use c0"):
        l3.c1("gbx_rst_contour")


# --- the layer reader ----------------------------------------------------------------------------------------------------
class _Gdal:
    @staticmethod
    def GetLastErrorMsg():
        return ""


def _alg(layer_ds):
    return types.SimpleNamespace(Output=lambda: layer_ds)


def test_a_run_that_gives_no_output_layer_is_a_data_error():
    fields = CONTOUR.gdal.out_fields
    with pytest.raises(spec.DataError, match="no output layer"):
        run._features(_Gdal, _alg(None), fields)
    no_layer = types.SimpleNamespace(GetLayer=lambda i: None)
    with pytest.raises(spec.DataError, match="no output layer"):
        run._features(_Gdal, _alg(no_layer), fields)


# --- what runs GDAL ------------------------------------------------------------------------------------------------------
@pytest.fixture
def engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    return run


def _dem(nodata, fill=None):
    from databricks.labs.gbx.bench import oracles

    z = np.tile(np.arange(10, dtype="float32"), (10, 1))[np.newaxis]
    if fill is not None:
        z = np.full((1, 10, 10), fill, "float32")
    return oracles._gtiff(z, "EPSG:3857", nodata=nodata)


def test_a_tile_of_nothing_but_nodata_has_no_lines_not_a_null(engine):
    for nodata, fill in (
        (-9999.0, -9999.0),
        (None, -9999.0),
    ):  # declared, then the sentinel
        res = engine.run(
            CONTOUR,
            [_dem(nodata, fill)],
            {"levels": [], "interval": 1.0, "base": 0.0, "attr_field": "elev"},
        )
        assert res.error is None and res.features == []
        assert l3._value(CONTOUR, res) == []


def test_the_run_reads_the_lines_with_their_exact_levels(engine):
    res = engine.run(
        CONTOUR,
        [_dem(None)],
        {"levels": [1.5, 3.5], "interval": 0.0, "base": 0.0, "attr_field": "elev"},
    )
    assert sorted({row[1] for row in res.features}) == [1.5, 3.5]


def test_the_engine_refuses_an_output_format_it_does_not_write(engine, monkeypatch):
    monkeypatch.setitem(spec.OUTPUT_FORMAT_OF, "features", "GeoJSON")
    with pytest.raises(
        spec.ParamError, match="does not write MEM for a features output"
    ):
        engine.run(
            CONTOUR,
            [_dem(None)],
            {"levels": [1.5], "interval": 0.0, "base": 0.0, "attr_field": "elev"},
        )


def test_a_reason_that_recurs_is_kept_as_the_most_recent(monkeypatch, caplog):
    """A reason seen again moves to the end of the log's order, so the next eviction takes an older one."""
    monkeypatch.setattr(l3, "_LOGGED_MAX", 2)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        l3._log_once("gbx_rst_contour", "a")
        l3._log_once("gbx_rst_contour", "b")
        l3._log_once("gbx_rst_contour", "a")  # recurs: now the most recent
        l3._log_once("gbx_rst_contour", "c")  # evicts b, not a
        l3._log_once("gbx_rst_contour", "a")
        l3._log_once("gbx_rst_contour", "b")
    got = [r.getMessage().rsplit(", ", 1)[1] for r in caplog.records]
    assert got == ["a", "b", "c", "b"]
