"""The summary (a JSON output) and the fill (one run for each band) on the GDAL spec engine.

The oracle rows (SPEC_ANALYSIS) judge both against independent GDAL calls; these rows hold what they do not: the
shape of the engine's result, the commands it ran, a tile in another driver, an unreadable tile, and the fill's
promises (every band, no invented value, a pixel it cannot reach stays NoData). They skip where ``osgeo`` is not
rasterio's GDAL.
"""

import json
import logging

import numpy as np
import pytest
from pyspark.sql.types import StringType
from rasterio.io import MemoryFile

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import l3, run
from databricks.labs.gbx.engine.spec import ParamError

MAN = manifest.load()
SUMMARY, FILL = MAN.functions["gbx_rst_summary"], MAN.functions["gbx_rst_fillnodata"]
NODATA = -9999.0


@pytest.fixture(autouse=True)
def _engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    l3._logged.clear()


def _tif(bands, nodata=NODATA, crs="EPSG:3857"):
    return oracles._gtiff(np.asarray(bands, "float64"), crs, nodata=nodata)


def _read(raster):
    with MemoryFile(raster) as mf, mf.open() as ds:
        return ds.read(), ds.profile, ds.nodata


def _gradient(n=9):
    yy, xx = np.mgrid[0:n, 0:n].astype("float64")
    return 10.0 + xx + yy


# --- the summary --------------------------------------------------------------------------------------------------
def test_the_summary_is_gdals_info_json_of_the_input_with_statistics():
    z = np.arange(1, 13, dtype="float64").reshape(1, 3, 4)
    z[0, 0, 0] = NODATA
    res = run.run(SUMMARY, [_tif(z)], {})
    assert res.error is None and res.raster is None
    info = json.loads(res.text)
    assert info["driverShortName"] == "GTiff" and info["size"] == [4, 3]
    band = info["bands"][0]
    assert (band["minimum"], band["maximum"], band["mean"]) == (2.0, 12.0, 7.0)
    assert band["noDataValue"] == NODATA and band["type"] == "Float32"
    # the engine's temporary names never reach the text
    assert "/vsimem" not in res.text and info["description"] == "<input>"
    assert res.command == (
        "raster info --input=<input> --input-format=GTiff --stats --output-format=json"
    )


def test_the_summary_describes_a_tile_kept_in_another_driver_as_it_is():
    z = (np.arange(16, dtype="uint8").reshape(1, 4, 4) * 10).astype("uint8")
    with MemoryFile() as mf:
        with mf.open(driver="PNG", width=4, height=4, count=1, dtype="uint8") as ds:
            ds.write(z)
        png = mf.read()
    res = run.run(SUMMARY, [png], {})
    assert res.error is None
    info = json.loads(res.text)
    assert info["driverShortName"] == "PNG" and info["size"] == [4, 4]
    assert info["bands"][0]["maximum"] == 150.0
    assert "/vsimem" not in res.text and info["description"] == "<input>"
    assert "--input-format=PNG" in res.command


def test_a_container_tile_with_no_band_is_summarized_with_no_bands_and_a_fill_refuses_it():
    import pathlib

    nc = sorted(
        (
            pathlib.Path(__file__).resolve().parents[4]
            / "src/test/resources/binary/netcdf-coral"
        ).glob("*.nc")
    )[0].read_bytes()
    res = run.run(SUMMARY, [nc], {})
    assert res.error is None
    info = json.loads(res.text)
    assert info["driverShortName"] in ("netCDF", "HDF5") and info["bands"] == []
    refused = run.run(FILL, [nc], {"max_search_dist": 100.0, "smoothing_iter": 0})
    assert refused.raster is None and "no band" in refused.error


def test_the_summary_computes_its_statistics_from_the_pixels_not_from_stored_tags():
    z = np.arange(1, 13, dtype="float64").reshape(1, 3, 4)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff", width=4, height=3, count=1, dtype="float32", nodata=NODATA
        ) as ds:
            ds.write(z.astype("float32"))
            ds.update_tags(
                1,
                STATISTICS_MINIMUM="500",
                STATISTICS_MAXIMUM="900",
                STATISTICS_MEAN="700",
                STATISTICS_STDDEV="1",
            )
        stale = mf.read()
    res = run.run(SUMMARY, [stale], {})
    assert res.error is None
    band = json.loads(res.text)["bands"][0]
    assert (band["minimum"], band["maximum"], band["mean"]) == (1.0, 12.0, 6.5)


def test_a_driver_outside_the_allowlist_is_refused_on_the_summary_path():
    vrt = (
        b'<VRTDataset rasterXSize="2" rasterYSize="2"><VRTRasterBand dataType="Byte" band="1">'
        b"<SimpleSource><SourceFilename>/etc/hostname</SourceFilename></SimpleSource>"
        b"</VRTRasterBand></VRTDataset>"
    )
    res = run.run(SUMMARY, [vrt], {})
    assert res.text is None and res.raster is None and res.error


def test_a_band_with_no_valid_pixel_has_no_statistics_and_the_summary_is_still_given():
    res = run.run(SUMMARY, [_tif(np.full((1, 4, 4), NODATA))], {})
    assert res.error is None
    band = json.loads(res.text)["bands"][0]
    assert "minimum" not in band and band["noDataValue"] == NODATA


def test_an_unreadable_tile_is_an_error_and_a_null_logged_once(caplog):
    res = run.run(SUMMARY, [b"II*\x00not a tiff"], {})
    assert res.text is None and res.error
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.engine"):
        assert l3._value(SUMMARY, res) is None and l3._value(SUMMARY, res) is None
    assert len(caplog.records) == 1 and caplog.records[0].getMessage().startswith(
        "gbx_rst_summary: NULL, "
    )


def test_a_json_row_returns_a_string_and_a_result_is_its_text():
    assert l3.return_type(SUMMARY) == StringType()
    ok = run.Result(None, None, "cmd", text='{"size": [1, 1]}')
    assert l3._value(SUMMARY, ok) == '{"size": [1, 1]}'
    assert l3._value(SUMMARY, None) is None
    with pytest.raises(ValueError, match="no batch form"):
        l3.c1("gbx_rst_summary")


# --- the fill -----------------------------------------------------------------------------------------------------
def test_the_fill_works_where_the_process_temp_directory_is_unusable(monkeypatch):
    """GDAL's fill keeps its index and value files on disk, named from CPL_TMPDIR, else TMPDIR, else /tmp, with no check
    that the directory is usable. On a cluster that gave every row of a fill "Could not create Y index work file" (an
    error tile) while the same rows ran in the dev container. The engine names the canonical local temp for the call.
    """
    z = _gradient()[np.newaxis]
    z[0, 4, 4] = NODATA
    monkeypatch.setenv("TMPDIR", "/gbx-no-such-directory/tmp")
    monkeypatch.delenv("CPL_TMPDIR", raising=False)
    res = run.run(FILL, [_tif(z)], {"max_search_dist": 100.0, "smoothing_iter": 0})
    assert res.error is None, res.error
    out, _, _ = _read(res.raster)
    assert out[0, 4, 4] != NODATA


@pytest.mark.parametrize(
    "fn_name,values",
    [
        ("gbx_rst_fillnodata", {"max_search_dist": 100.0, "smoothing_iter": 0}),
        ("gbx_rst_tri", {}),
        ("gbx_rst_resample", {"factor": 0.5, "algorithm": "bilinear"}),
        ("gbx_rst_summary", {}),
    ],
)
def test_every_engine_algorithm_runs_with_the_local_temp_as_gdals_temp_directory(
    monkeypatch, fn_name, values
):
    """The temp directory is set for every engine call in the shared run path, not for the fill alone: any algorithm may
    keep work files on disk (a warp's, a viewshed's), and GDAL does not check the directory it is told.
    """
    from databricks.labs.gbx.pyrx.core.local_temp import local_temp_root

    seen = []
    real = run._execute

    def spy(gdal, *a, **kw):
        seen.append(gdal.GetThreadLocalConfigOption("CPL_TMPDIR"))
        return real(gdal, *a, **kw)

    monkeypatch.setattr(run, "_execute", spy)
    z = _gradient()[np.newaxis]
    res = run.run(MAN.functions[fn_name], [_tif(z)], dict(values))
    assert res.error is None, res.error
    assert seen and set(seen) == {local_temp_root()}


def test_every_band_is_filled_by_its_own_mask_and_the_rest_of_the_tile_is_the_inputs():
    z = np.stack([_gradient(), 100.0 + 2 * _gradient()])
    z[0, 4, 4] = NODATA
    z[1, 3:6, 3:6] = NODATA
    res = run.run(FILL, [_tif(z)], {"max_search_dist": 100.0, "smoothing_iter": 0})
    assert res.error is None
    out, profile, nodata = _read(res.raster)
    want, want_profile, _ = _read(_tif(z))
    assert out.shape == (2, 9, 9) and nodata == NODATA
    assert profile["dtype"] == want_profile["dtype"] == "float32"
    assert profile["transform"] == want_profile["transform"]
    assert profile["crs"] == want_profile["crs"]
    holes = z == NODATA
    assert not (out == NODATA).any()
    # a pixel that was valid is untouched
    assert np.array_equal(out[~holes], want[~holes])
    # a filled pixel is an interpolation: inside the range of the valid pixels of its own band
    for b in (0, 1):
        valid = z[b][~holes[b]]
        assert valid.min() <= out[b][holes[b]].min()
        assert out[b][holes[b]].max() <= valid.max()


def test_a_pixel_the_search_cannot_reach_stays_nodata_and_none_is_invented():
    z = _gradient()[np.newaxis]
    z[0, 3:6, 3:6] = NODATA  # the middle pixel is two pixels from the nearest valid one
    res = run.run(FILL, [_tif(z)], {"max_search_dist": 1, "smoothing_iter": 0})
    out = _read(res.raster)[0][0]
    assert out[4, 4] == NODATA
    assert (out[3:6, 3:6] != NODATA).sum() == 8
    assert not np.isnan(out).any()


def test_a_tile_that_declares_no_nodata_comes_back_unchanged():
    z = _gradient()[np.newaxis]
    z[0, 4, 4] = NODATA
    res = run.run(
        FILL, [_tif(z, nodata=None)], {"max_search_dist": 100.0, "smoothing_iter": 0}
    )
    out, _, nodata = _read(res.raster)
    assert nodata is None and out[0, 4, 4] == NODATA


def test_the_fill_keeps_the_type_of_a_byte_tile():
    z = np.full((1, 9, 9), 5.0)
    z[0, 4, 4] = 0.0
    raw = oracles._gtiff(z, "EPSG:3857", nodata=0.0, dtype="uint8")
    res = run.run(FILL, [raw], {"max_search_dist": 100.0, "smoothing_iter": 0})
    out, profile, nodata = _read(res.raster)
    assert profile["dtype"] == "uint8" and nodata == 0.0 and out[0, 4, 4] == 5


def test_the_fill_runs_the_algorithm_once_for_each_band_and_names_the_first_in_the_command():
    z = np.stack([_gradient()] * 3)
    z[:, 4, 4] = NODATA
    res = run.run(FILL, [_tif(z)], {"max_search_dist": 100.0, "smoothing_iter": 0})
    assert res.command.startswith(
        "raster fill-nodata --input=<input> --input-format=GTiff --band=1 --max-distance=100 --smoothing-iterations=0"
    )
    out = _read(res.raster)[0]
    assert out.shape[0] == 3 and not (out == NODATA).any()


def test_a_null_number_is_a_null_result_and_a_wrong_one_raises_naming_it():
    raw = _tif(_gradient()[np.newaxis])
    assert run.run(FILL, [raw], {"max_search_dist": None, "smoothing_iter": 0}) is None
    assert (
        run.run(FILL, [raw], {"max_search_dist": 5.0, "smoothing_iter": None}) is None
    )
    for bad in (2.5, -1, 3e9, float("inf"), "x"):
        with pytest.raises(ParamError, match=r"max_search_dist must be a whole number"):
            run.run(FILL, [raw], {"max_search_dist": bad, "smoothing_iter": 0})
    with pytest.raises(ParamError, match="smoothing_iter"):
        run.run(FILL, [raw], {"max_search_dist": 5.0, "smoothing_iter": -2})


def _masked_tile():
    """A tile with no NoData whose validity is its own mask band: a gradient warped to a rotated grid, whose corners are masked."""
    yy, xx = np.mgrid[0:64, 0:64].astype("float64")
    src = oracles._gtiff((10.0 + xx + yy)[np.newaxis], "EPSG:3857", nodata=None)
    warped = run.run(MAN.functions["gbx_rst_transform"], [src], {"srid": 32632})
    return warped.raster


def _mask_of(raster):
    with MemoryFile(raster) as mf, mf.open() as ds:
        return ds.read_masks(1) != 0, ds.read(1), ds.nodata


def test_a_pixel_the_fill_reaches_becomes_valid_in_a_tiles_own_mask_and_the_rest_stays_masked(
    monkeypatch,
):
    raw = _masked_tile()
    valid, _, nodata = _mask_of(raw)
    assert nodata is None and not valid.all()
    full = run.run(FILL, [raw], {"max_search_dist": 100.0, "smoothing_iter": 0})
    mask, data, _ = _mask_of(full.raster)
    assert mask.all() and np.isfinite(data).all()
    near = run.run(FILL, [raw], {"max_search_dist": 1.0, "smoothing_iter": 0})
    short, _, _ = _mask_of(near.raster)
    assert (valid <= short).all() and short.sum() > valid.sum() and not short.all()
    # without the mask step GDAL leaves the mask as the input had it: the fill would change no pixel's validity
    monkeypatch.setattr(run, "_unmask_filled", lambda *a, **k: None)
    plain = run.run(FILL, [raw], {"max_search_dist": 100.0, "smoothing_iter": 0})
    assert np.array_equal(_mask_of(plain.raster)[0], valid)


def test_a_masked_multi_band_tile_keeps_its_mask_through_the_per_band_fill():
    yy, xx = np.mgrid[0:64, 0:64].astype("float64")
    z = np.stack([10.0 + xx + yy, 200.0 - xx])
    src = oracles._gtiff(z, "EPSG:3857", nodata=None)
    raw = run.run(MAN.functions["gbx_rst_transform"], [src], {"srid": 32632}).raster
    valid, _, _ = _mask_of(raw)
    assert not valid.all()
    near = run.run(FILL, [raw], {"max_search_dist": 1.0, "smoothing_iter": 0})
    assert near.error is None
    short, _, _ = _mask_of(near.raster)
    assert (valid <= short).all() and short.sum() > valid.sum() and not short.all()
