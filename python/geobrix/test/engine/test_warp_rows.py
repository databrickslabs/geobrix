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


def test_an_infinite_size_is_refused_by_name_not_as_an_overflow():
    with pytest.raises(spec.ParamError) as e:
        _build(RSZ, {"width_px": float("inf"), "height_px": 2, "algorithm": "near"})
    assert "width_px" in str(e.value)


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
    for word in (
        "max",
        "min",
        "med",
        "q1",
        "q3",
    ):  # a warp applies these, a read cannot
        assert l3._decimation(M[RS], {"factor": 0.5, "algorithm": word}) is None
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

    # a tile that arrives with a FILE reference is read at its target size as well

    class _Ref:
        def as_local_file(self):
            return path

    sizes = []
    rasters, _, _ = l3._inputs(M[RS], tile, None, _Ref(), sizes, values)
    assert sizes == [(64, 64)]
    with rasterio.MemoryFile(rasters[0]) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (32, 32)
    # a factor of 1 or more reads the whole window
    sizes = []
    rasters, _, _ = l3._inputs(
        M[RS], tile, None, None, sizes, {"factor": 2.0, "algorithm": "average"}
    )
    assert sizes == [None]
    with rasterio.MemoryFile(rasters[0]) as mf, mf.open() as ds:
        assert (ds.width, ds.height) == (64, 64)
    # a word only a warp applies reads the whole window; the reproject applies it
    sizes = []
    rasters, _, _ = l3._inputs(
        M[RS], tile, None, None, sizes, {"factor": 0.5, "algorithm": "max"}
    )
    assert sizes == [None]
    res = engine.run(M[RS], rasters, {"factor": 0.5, "algorithm": "max"})
    assert res.error is None, res.error


# --- a pixel no source pixel reaches is masked, never a valid 0 ----------------------------------------------------------
def _ramp(nodata=None, epsg="EPSG:4326", w=20, h=15, origin=(0.0, 60.0), px=1.0):
    """A one-band ramp 1..w*h (no 0) on a `px` degree grid: any 0 in a result is a fill, not a value."""
    from databricks.labs.gbx.bench import oracles

    z = np.arange(1, w * h + 1, dtype="float32").reshape(1, h, w)
    return oracles._gtiff(z, epsg, nodata=nodata, origin=origin, px=px)


def _open_result(res):
    from rasterio.io import MemoryFile

    assert res.error is None, res.error
    mf = MemoryFile(bytes(res.raster))
    return mf, mf.open()


def test_the_pixels_outside_a_shrunk_footprint_are_masked_when_no_nodata_is_declared(
    engine,
):
    # EPSG:4326 -> EPSG:3035 turns the rectangle: its corners leave the output's bounding box
    mf, ds = _open_result(engine.run(M[TR], [_ramp()], {"srid": 3035}))
    with mf, ds:
        assert ds.count == 1 and ds.nodata is None
        masks = ds.read_masks(1)
        assert (masks == 0).any() and (masks != 0).any()
        band = ds.read(1, masked=True)
        assert (band.compressed() >= 1).all()  # a valid pixel is a source value
        assert (band.data[masks == 0] == 0).all()  # the fill stays 0 under the mask
        assert ds.mask_flag_enums[0][0].name == "per_dataset"


def test_the_masked_footprint_pixels_are_the_ones_the_warp_does_not_reach(engine):
    from rasterio.crs import CRS
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    raw = _ramp()
    mf, ds = _open_result(engine.run(M[TR], [raw], {"srid": 3035}))
    with mf, ds:
        got = ds.read_masks(1) != 0
        w, h = ds.width, ds.height
    from rasterio.io import MemoryFile

    with MemoryFile(raw) as smf, smf.open() as src:
        transform, rw, rh = calculate_default_transform(
            src.crs, CRS.from_epsg(3035), src.width, src.height, *src.bounds
        )
        cover = np.zeros((rh, rw), "u1")
        reproject(
            np.full((src.height, src.width), 255, "u1"),
            cover,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=transform,
            dst_crs=CRS.from_epsg(3035),
            resampling=Resampling.nearest,
        )
    rows, cols = min(h, rh), min(w, rw)
    same = (got[:rows, :cols] == (cover[:rows, :cols] != 0)).mean()
    assert same > 0.98, same


def test_a_declared_nodata_source_is_reprojected_as_before_with_no_mask_band(engine):
    mf, ds = _open_result(engine.run(M[TR], [_ramp(nodata=-9999.0)], {"srid": 3035}))
    with mf, ds:
        assert ds.nodata == -9999.0
        assert ds.mask_flag_enums[0][0].name == "nodata"
        assert (ds.read(1) == -9999.0).any()  # the outside is the declared NoData


def test_a_resample_that_leaves_no_pixel_unreached_has_no_mask_band(engine):
    mf, ds = _open_result(
        engine.run(M[RS], [_ramp()], {"factor": 0.5, "algorithm": "near"})
    )
    with mf, ds:
        assert ds.mask_flag_enums[0][0].name == "all_valid"


def test_a_source_own_mask_and_the_footprint_are_both_masked(engine, tmp_path):
    import rasterio
    from rasterio.enums import MaskFlags
    from rasterio.transform import from_origin

    path = tmp_path / "own_mask.tif"
    z = (np.arange(300) % 250 + 1).astype("uint8").reshape(1, 15, 20)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=20,
        height=15,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 60.0, 1.0, 1.0),
    ) as dst:
        dst.write(z)
        m = np.full((15, 20), 255, "u1")
        m[:2, :2] = 0
        dst.write_mask(m)
    mf, ds = _open_result(engine.run(M[TR], [path.read_bytes()], {"srid": 3035}))
    with mf, ds:
        assert MaskFlags.per_dataset in ds.mask_flag_enums[0]
        masks = ds.read_masks(1)
        assert (masks == 0).sum() > 4  # the 2 x 2 source pixels and the corners outside
        assert (ds.read(1, masked=True).compressed() > 0).all()


def _gcp_tile(tmp_path):
    """A one-band tile with no NoData georeferenced by ground control points only (no geotransform)."""
    from osgeo import gdal, osr

    path = str(tmp_path / "gcp.tif")
    ds = gdal.GetDriverByName("GTiff").Create(path, 20, 15, 1, gdal.GDT_Float32)
    ds.GetRasterBand(1).WriteArray(np.arange(1, 301, dtype="float32").reshape(15, 20))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ds.SetGCPs(
        [
            gdal.GCP(0.0, 60.0, 0, 0, 0),
            gdal.GCP(20.0, 60.0, 0, 20, 0),
            gdal.GCP(0.0, 45.0, 0, 0, 15),
            gdal.GCP(20.0, 45.0, 0, 20, 15),
        ],
        srs.ExportToWkt(),
    )
    ds = None
    with open(path, "rb") as f:
        return f.read()


def test_a_gcp_georeferenced_tile_with_no_nodata_is_warped_with_its_footprint_masked(
    engine, tmp_path
):
    mf, ds = _open_result(engine.run(M[TR], [_gcp_tile(tmp_path)], {"srid": 3035}))
    with mf, ds:
        assert ds.crs is not None and ds.crs.to_epsg() == 3035
        masks = ds.read_masks(1)
        assert (masks == 0).any() and (masks != 0).any()
        assert (
            ds.read(1, masked=True).compressed() >= 1
        ).all()  # every 0 is a fill, under the mask
