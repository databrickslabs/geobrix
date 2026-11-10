"""The engine's table function (polygonize): how its eval takes arguments and what it gives for a NULL, error, refused or
failed call. The engine run is replaced by a stub, so those rows run on the host; the oracle rows
(SPEC_ANALYSIS, `gbx_rst_polygonize`) hold the polygons themselves. The rows at the end run the engine (they skip where
``osgeo`` is not rasterio's GDAL): the alpha band read a strip at a time, the budget and the stale-folder sweep.
"""

import inspect
import logging
import os

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


def test_a_stream_of_a_file_gdal_cannot_open_raises_a_data_error_not_a_runtime_error(
    tmp_path,
):
    """GDAL raises (exceptions are on in a worker) where the call itself saw a failing return."""
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    gdal = run._gdal()
    bad = tmp_path / "out.gpkg"
    bad.write_bytes(b"not a geopackage")
    stream = run.FeatureStream(gdal, str(bad), str(tmp_path), POLY.gdal.out_fields)
    with gdal.ExceptionMgr(useExceptions=True):
        with pytest.raises(run.DataError, match="could not be read back"):
            list(stream)


# --- the run (needs the engine) ------------------------------------------------------------------------------------------
@pytest.fixture
def engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    return run


def _tile(values, alpha=None) -> bytes:
    """A Byte tile of `values` (rows x cols) with no NoData, and when `alpha` is given a second band holding it, read as
    an alpha band (a third band of zeros sits between: GDAL derives no mask from alpha for 3 bands).
    """
    import numpy as np
    from rasterio.enums import ColorInterp
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    bands = [values] if alpha is None else [values, np.zeros_like(values), alpha]
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=values.shape[1],
            height=values.shape[0],
            count=len(bands),
            dtype="uint8",
            crs="EPSG:3857",
            transform=from_origin(0, values.shape[0], 1, 1),
        ) as dst:
            dst.write(np.stack(bands).astype("uint8"))
            if alpha is not None:
                dst.colorinterp = [
                    ColorInterp.gray,
                    ColorInterp.undefined,
                    ColorInterp.alpha,
                ]
        return mf.read()


def test_a_transparent_pixel_is_left_out_and_the_alpha_is_read_a_strip_at_a_time(
    engine, monkeypatch
):
    import numpy as np

    values = np.ones((32, 32), "uint8")
    values[:, 16:] = 2
    alpha = np.full((32, 32), 255, "uint8")
    alpha[:, 16:] = 0  # the 2s are transparent
    gdal = run._gdal()
    reads, read = [], gdal.Band.ReadRaster

    def recorded(self, xoff=0, yoff=0, xsize=None, ysize=None, *a, **k):
        reads.append(self.YSize if ysize is None else ysize)
        return read(self, xoff, yoff, xsize, ysize, *a, **k)

    monkeypatch.setattr(run, "_MASK_STRIP_BYTES", 8 * 32 * 4)  # four rows a strip
    monkeypatch.setattr(gdal.Band, "ReadRaster", recorded)
    res = run.run(POLY, [_tile(values, alpha)], {"band": 1, "connectedness": 4})
    assert res.error is None, res.error
    assert {row[1] for row in res.features} == {1.0}
    assert reads and max(reads) <= 4  # no read of the whole tile


def test_an_alpha_mask_gdal_cannot_make_is_an_error_tile(engine, monkeypatch):
    import numpy as np

    gdal = run._gdal()
    create = gdal.Driver.Create

    def refuse(self, name, *a, **k):
        return (
            None
            if str(name).endswith("alpha_mask.tif")
            else create(self, name, *a, **k)
        )

    monkeypatch.setattr(gdal.Driver, "Create", refuse)
    values = np.ones((32, 32), "uint8")
    res = run.run(POLY, [_tile(values, values * 255)], {"band": 1, "connectedness": 4})
    assert res.features is None and "the alpha mask could not be made" in res.error


def test_a_tile_whose_decoded_size_fits_the_budget_is_polygonized(engine, monkeypatch):
    """The output is features on local disk, not a raster: a tile is judged by its own decoded size, so one that fits
    the budget with little to spare is polygonized."""
    import numpy as np

    from databricks.labs.gbx.engine import argv

    values = np.ones((1024, 64), "uint8")
    values[8:, :] = 3
    props = [argv.RasterProps(4000, 4000, 1, "Byte")]
    assert argv.estimate_output(POLY, [], props) == 4000 * argv._STREAM_BYTES_PER_COLUMN
    # the tile's 64 KiB and its line buffers, where a raster of its pixels would be 64 KiB more
    monkeypatch.setattr(
        run, "decoded_budget", lambda: 1024 * 64 + 64 * argv._STREAM_BYTES_PER_COLUMN
    )
    res = run.run(POLY, [_tile(values)], {"band": 1, "connectedness": 4})
    assert res.error is None, res.error
    assert sorted(row[1] for row in res.features) == [1.0, 3.0]


def test_the_first_streamed_call_of_a_process_sweeps_the_stale_folders_once(
    monkeypatch, tmp_path
):
    """A worker ended with os._exit leaves its folder: the first streamed call of a process removes the old ones, and
    keeps a recent one (another call's)."""
    import os
    import time

    from databricks.labs.gbx.pyrx.core import local_temp

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(run, "_swept", False)
    stale, recent = tmp_path / "gbx_spec_stale", tmp_path / "gbx_spec_recent"
    stale.mkdir()
    recent.mkdir()
    old = time.time() - local_temp.DEFAULT_STALE_TTL_SECONDS - 60
    os.utime(stale, (old, old))
    sweeps, sweep = [], local_temp.gc_stale_local_temp
    monkeypatch.setattr(
        local_temp,
        "gc_stale_local_temp",
        lambda prefix, **kw: (sweeps.append(prefix), sweep(prefix, **kw)),
    )
    names = [run._output_name(POLY.gdal, "/vsimem/x") for _ in range(2)]
    assert sweeps == [run.SCRATCH_PREFIX]
    assert not stale.exists() and recent.exists()
    for out, scratch in names:
        assert out == f"{scratch}/out.gpkg" and os.path.isdir(scratch)


def test_a_feature_gdal_fails_to_read_mid_stream_is_a_data_error_after_the_rows_before_it(
    engine, monkeypatch
):
    import numpy as np

    values = np.ones((8, 8), "uint8")
    values[:, 4:] = 2
    res = run.run(POLY, [_tile(values)], {"band": 1, "connectedness": 4})
    assert res.error is None, res.error
    real = run._layer_rows

    def fails_after_one(lyr, fields, fix=None):
        yield next(real(lyr, fields, fix))
        raise RuntimeError("boom")

    monkeypatch.setattr(run, "_layer_rows", fails_after_one)
    got = []
    with pytest.raises(run.DataError, match="could not be read back"):
        for row in res.features:
            got.append(row)
    assert len(got) == 1


def _two_regions():
    """The real result of a tile of two regions: two features in the file GDAL wrote."""
    import numpy as np

    values = np.ones((8, 8), "uint8")
    values[:, 4:] = 2
    res = run.run(POLY, [_tile(values)], {"band": 1, "connectedness": 4})
    assert res.error is None, res.error
    return res


def _serve(monkeypatch, res):
    monkeypatch.setattr(run, "run", lambda *a, **k: res)
    return _handler()


def test_a_file_that_fails_its_check_before_the_first_row_gives_no_rows_and_logs(
    engine, monkeypatch, tmp_path, caplog
):
    (tmp_path / "out.gpkg").write_bytes(b"not a geopackage")
    stream = run.FeatureStream(
        run._gdal(), str(tmp_path / "out.gpkg"), str(tmp_path), POLY.gdal.out_fields
    )
    res = run.Result(None, None, "cmd", features=stream)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        assert list(_serve(monkeypatch, res).eval(TILE)) == []
    assert stream.expected is None and not tmp_path.exists()
    msgs = [r.getMessage() for r in caplog.records if "polygonize" in r.getMessage()]
    assert len(msgs) == 1 and "could not be read back" in msgs[0]
    assert "missing" not in msgs[0]


def test_a_file_with_no_readable_feature_count_gives_no_rows_and_logs(
    engine, monkeypatch, caplog
):
    res = _two_regions()
    scratch, gdal = res.features._scratch, res.features._gdal
    real = gdal.OpenEx

    class _NoCount:
        def __init__(self, lyr):
            self._lyr = lyr

        def GetFeatureCount(self):
            return -1

    class _Ds:
        def __init__(self, ds):
            self._ds = ds

        def GetLayer(self, i):
            return _NoCount(self._ds.GetLayer(i))

    monkeypatch.setattr(gdal, "OpenEx", lambda *a, **k: _Ds(real(*a, **k)))
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        assert list(_serve(monkeypatch, res).eval(TILE)) == []
    assert res.features.expected is None and not os.path.exists(scratch)
    assert any("no readable feature count" in r.getMessage() for r in caplog.records)


def test_a_read_that_fails_after_rows_went_out_raises_and_the_file_is_removed(
    engine, monkeypatch
):
    res = _two_regions()
    scratch, real = res.features._scratch, run._layer_rows

    def fails_after_one(lyr, fields, fix=None):
        yield next(real(lyr, fields, fix))
        raise RuntimeError("boom")

    monkeypatch.setattr(run, "_layer_rows", fails_after_one)
    got = []
    with pytest.raises(
        RuntimeError, match=r"gbx_rst_polygonize.*after 1 of 2 features.*retried"
    ):
        for row in _serve(monkeypatch, res).eval(TILE):
            got.append(row)
    assert len(got) == 1 and not os.path.exists(scratch)


def test_a_file_that_ends_short_of_its_feature_count_raises(engine, monkeypatch):
    res = _two_regions()
    scratch, real = res.features._scratch, run._layer_rows

    def stops_after_one(lyr, fields, fix=None):
        yield next(real(lyr, fields, fix))

    monkeypatch.setattr(run, "_layer_rows", stops_after_one)
    with pytest.raises(RuntimeError, match=r"after 1 of 2 features.*retried"):
        list(_serve(monkeypatch, res).eval(TILE))
    assert not os.path.exists(scratch)


def test_a_complete_read_gives_every_feature_and_removes_the_file(engine, monkeypatch):
    res = _two_regions()
    scratch = res.features._scratch
    rows = list(_serve(monkeypatch, res).eval(TILE))
    assert sorted(r[1] for r in rows) == [1.0, 2.0]
    assert (res.features.read, res.features.expected) == (2, 2)
    assert not os.path.exists(scratch)


def test_an_error_that_is_not_gdals_propagates_unchanged(monkeypatch):
    def buggy():
        yield ROWS[0]
        raise ValueError("a bug")

    monkeypatch.setattr(
        run, "run", lambda *a, **k: run.Result(None, None, "cmd", features=buggy())
    )
    with pytest.raises(ValueError, match="a bug"):
        list(_handler().eval(TILE))


def test_the_sweep_keeps_the_folder_of_a_live_process_however_old_and_removes_a_dead_ones(
    monkeypatch, tmp_path
):
    """A streamed folder is not written while it is read back: its age says nothing, its owner process does."""
    import os
    import subprocess
    import sys
    import time

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(run, "_swept", False)
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()
    live = tmp_path / f"{run.SCRATCH_PREFIX}{os.getppid()}_old"  # the parent is alive
    dead = tmp_path / f"{run.SCRATCH_PREFIX}{gone.pid}_old"
    live.mkdir()
    dead.mkdir()
    old = time.time() - 10 * 3600
    for d in (live, dead):
        os.utime(d, (old, old))
    out, scratch = run._output_name(POLY.gdal, "/vsimem/x")
    assert os.path.basename(scratch).startswith(f"{run.SCRATCH_PREFIX}{os.getpid()}_")
    assert live.exists() and not dead.exists() and os.path.isdir(scratch)
