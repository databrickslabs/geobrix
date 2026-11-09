"""The warp rows (transform, to_webmercator, resample, resample_to_size, resample_to_res) on the GDAL spec engine.

The binders and the output estimate need no GDAL and run on the host. The rows that run the engine (a tile already in
the target CRS, a virtual tile read at its target size, the per-call budget) skip where ``osgeo`` is not rasterio's
GDAL; the oracle rows of ``SPEC_WARP`` hold the rest.
"""

import numpy as np
import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import argv, l3, run, spec

M = manifest.load().functions
RS, RSZ, RSR = "gbx_rst_resample", "gbx_rst_resample_to_size", "gbx_rst_resample_to_res"
WM, TR = "gbx_rst_to_webmercator", "gbx_rst_transform"
PROPS = argv.RasterProps(64, 48, 1, "Float32")


def _build(fn, values, props=PROPS):
    return argv.build(M[fn], values, [props])


def _token(tokens, name):
    return next(t.split("=", 1)[1] for t in tokens if t.startswith(f"--{name}="))


# --- the binders ------------------------------------------------------------------------------------------------------
def test_a_null_argument_is_a_null_result_before_a_wrong_one_is_judged():
    assert _build(RS, {"factor": -1.0, "algorithm": None}) is None
    assert _build(RS, {"factor": None, "algorithm": "sinc"}) is None
    assert (
        _build(RSZ, {"width_px": 0, "height_px": None, "algorithm": "bilinear"}) is None
    )
    with pytest.raises(spec.ParamError, match="sinc"):
        _build(RS, {"factor": 0.5, "algorithm": "sinc"})


def test_nan_in_a_double_argument_reads_as_null_because_an_arrow_udf_hands_a_null_over_as_nan():
    assert _build(RS, {"factor": float("nan"), "algorithm": "bilinear"}) is None
    assert (
        _build(RSR, {"x_res": 1.0, "y_res": float("nan"), "algorithm": "near"}) is None
    )


@pytest.mark.parametrize(
    "fn,values,named",
    [
        (RS, {"factor": 0.0, "algorithm": "bilinear"}, "factor"),
        (RS, {"factor": float("inf"), "algorithm": "bilinear"}, "factor"),
        (RSZ, {"width_px": 0, "height_px": 2, "algorithm": "bilinear"}, "width_px"),
        (RSZ, {"width_px": 2, "height_px": -3, "algorithm": "bilinear"}, "height_px"),
        (RSR, {"x_res": -1.0, "y_res": 1.0, "algorithm": "bilinear"}, "x_res"),
        (RSR, {"x_res": 1.0, "y_res": 0.0, "algorithm": "bilinear"}, "y_res"),
    ],
)
def test_a_size_that_is_not_above_zero_names_the_member_and_its_value(
    fn, values, named
):
    with pytest.raises(spec.ParamError, match=f"{named} must be positive and finite"):
        _build(fn, values)


@pytest.mark.parametrize(
    "word,gdal_word",
    [
        ("near", "nearest"),
        ("NEAREST", "nearest"),
        ("CubicSpline", "cubicspline"),
        ("q3", "q3"),
    ],
)
def test_the_resampling_words_are_the_twelve_plus_nearest(word, gdal_word):
    assert (
        _token(_build(RS, {"factor": 0.5, "algorithm": word}), "resampling")
        == gdal_word
    )


@pytest.mark.parametrize("word", ["gauss", "sum", "rms", "sinc"])
def test_a_word_gdal_has_but_the_functions_do_not_take_is_refused(word):
    with pytest.raises(spec.ParamError, match=word):
        _build(RS, {"factor": 0.5, "algorithm": word})


def test_a_size_from_a_factor_rounds_half_to_even_and_is_at_least_one():
    half = _build(
        RS,
        {"factor": 0.5, "algorithm": "bilinear"},
        argv.RasterProps(65, 49, 1, "Float32"),
    )
    assert _token(half, "size") == "32,24"
    tiny = _build(RS, {"factor": 0.001, "algorithm": "bilinear"})
    assert _token(tiny, "size") == "1,1"


def test_a_size_is_a_share_of_the_windows_own_size_not_of_a_decimated_read():
    read = argv.RasterProps(32, 24, 1, "Float32", None, 64, 48)
    assert (
        _token(_build(RS, {"factor": 0.5, "algorithm": "bilinear"}, read), "size")
        == "32,24"
    )


# --- the output estimate ----------------------------------------------------------------------------------------------
def test_the_estimate_of_a_size_is_that_size_in_the_input_type():
    tokens = _build(RS, {"factor": 2.0, "algorithm": "bilinear"})
    assert argv.estimate_output(M[RS], tokens, [PROPS]) == 128 * 96 * 4


def test_the_estimate_of_a_resolution_is_the_extent_over_it():
    props = argv.RasterProps(64, 48, 2, "Float32", None, 0, 0, 0.01, 0.01)
    tokens = _build(
        RSR, {"x_res": 0.005, "y_res": 0.005, "algorithm": "bilinear"}, props
    )
    assert argv.estimate_output(M[RSR], tokens, [props]) == 128 * 96 * 2 * 4


def test_the_estimate_without_a_size_is_the_inputs_decoded_size():
    tokens = _build(TR, {"srid": 3857})
    assert argv.estimate_output(M[TR], tokens, [PROPS]) == 64 * 48 * 4


# --- the read at the target size ---------------------------------------------------------------------------------------
def test_a_virtual_tile_is_read_at_its_target_size_only_below_one_with_a_word_that_names_one():
    assert l3._decimation(M[RS], {"factor": 0.5, "algorithm": "average"})[0] == 0.5
    assert l3._decimation(M[RS], {"factor": 1.0, "algorithm": "average"}) is None
    assert l3._decimation(M[RS], {"factor": 2.0, "algorithm": "average"}) is None
    assert l3._decimation(M[RS], {"factor": 0.5, "algorithm": "sinc"}) is None
    assert l3._decimation(M[RS], {"factor": 0.5, "algorithm": None}) is None
    assert l3._decimation(M[RS], {"factor": -1.0, "algorithm": "near"}) is None
    assert l3._decimation(M[RS], None) is None
    assert (
        l3._decimation(M[RSZ], {"width_px": 2, "height_px": 2, "algorithm": "near"})
        is None
    )


# --- what runs the engine ---------------------------------------------------------------------------------------------
@pytest.fixture
def engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    return run


def _gtiff(w=64, h=48, epsg="EPSG:4326", nodata=-9999.0):
    from databricks.labs.gbx.bench import oracles

    z = (np.arange(w * h, dtype="float32").reshape(1, h, w) % 97).astype("float32")
    z[0, :4, :4] = nodata
    if epsg == "EPSG:4326":
        return oracles._gtiff(z, epsg, nodata=nodata, origin=(-1.0, 52.0), px=0.01)
    return oracles._gtiff(z, epsg, nodata=nodata)


def test_a_tile_already_in_the_target_crs_comes_back_as_its_own_bytes(engine):
    raw = _gtiff(epsg="EPSG:3857")
    for fn, values in ((TR, {"srid": 3857}), (WM, {"resampling": "bilinear"})):
        res = engine.run(M[fn], [raw], values)
        assert res.error is None and bytes(res.raster) == raw, fn


def test_a_tile_in_another_crs_is_warped(engine):
    raw = _gtiff()
    res = engine.run(M[TR], [raw], {"srid": 3857})
    assert res.error is None and bytes(res.raster) != raw


def test_a_reprojected_nodata_area_stays_nodata(engine):
    from rasterio.io import MemoryFile

    res = engine.run(M[TR], [_gtiff()], {"srid": 3857})
    with MemoryFile(bytes(res.raster)) as mf, mf.open() as ds:
        band = ds.read(1, masked=True)
        assert ds.nodata == -9999.0 and band.mask[:2, :2].all()
        assert (band.compressed() != 0).any()


def test_a_call_whose_input_and_output_pass_the_budget_is_an_error_tile_with_a_fix(
    engine, monkeypatch
):
    monkeypatch.setattr(run, "decoded_budget", lambda: 3 * 64 * 48 * 4)
    res = engine.run(M[RS], [_gtiff()], {"factor": 4.0, "algorithm": "bilinear"})
    assert res.raster is None and "Fix:" in res.error and "smaller output" in res.error
    ok = engine.run(M[RS], [_gtiff()], {"factor": 1.0, "algorithm": "bilinear"})
    assert (
        ok.error is None
    )  # input and an output of the same size: 2 x 12288 <= 3 x 12288


def test_a_virtual_tile_with_an_overview_is_read_at_its_target_size(engine, tmp_path):
    import rasterio
    from rasterio.enums import Resampling

    path = str(tmp_path / "ov.tif")
    z = (np.arange(64 * 64, dtype="float32") % 251).reshape(1, 64, 64)
    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype="float32",
        tiled=True,
        blockxsize=32,
        blockysize=32,
    )
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(z)
    with rasterio.open(path, "r+") as dst:
        dst.build_overviews([2], Resampling.average)
    tile = {"cellid": 1, "raster": None, "path": path, "window": None, "metadata": {}}
    sizes = []
    values = {"factor": 0.5, "algorithm": "average"}
    rasters, _, _ = l3._inputs(M[RS], tile, None, None, sizes, values)
    assert sizes == [(64, 64)]
    with rasterio.MemoryFile(rasters[0]) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (32, 32)
    res = engine.run(M[RS], rasters, values, src_sizes=sizes)
    with rasterio.MemoryFile(res.raster) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (32, 32)
    # a factor of 1 or more reads the whole window
    sizes = []
    rasters, _, _ = l3._inputs(
        M[RS], tile, None, None, sizes, {"factor": 2.0, "algorithm": "average"}
    )
    assert sizes == [None]
    with rasterio.MemoryFile(rasters[0]) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (64, 64)
