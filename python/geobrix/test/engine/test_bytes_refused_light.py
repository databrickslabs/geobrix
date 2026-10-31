"""The light tier's tile-bytes reads refuse the formats that name other files.

A tile's bytes are data, and the remote-path gate never sees a VRT's sources, so `open_bytes` (the one way GeoBrix opens
raster bytes) refuses a driver of the manifest's `bytes_refused_light` class before it opens it. netCDF, HDF5, HFA,
JP2 and GPKG bytes are not in the class and open as before.

Rows: a VRT with a local source (its sentinel pixels must not appear), a VRT with a /vsicurl/ source (asserted on the
refusal message: a failed open logs no GDALOpen line), a label format, the formats that stay readable, the class
itself, and one real light function end to end.
"""

import io
import re

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")

from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx import _gdal_policy, manifest  # noqa: E402

SENTINEL = 7.0


def _vrt(source: str) -> bytes:
    return (
        '<VRTDataset rasterXSize="4" rasterYSize="4">'
        '<VRTRasterBand dataType="Float32" band="1"><SimpleSource>'
        f'<SourceFilename relativeToVRT="0">{source}</SourceFilename>'
        "<SourceBand>1</SourceBand></SimpleSource></VRTRasterBand></VRTDataset>"
    ).encode()


@pytest.fixture
def sentinel_tif(tmp_path):
    """A local GeoTIFF of one value (SENTINEL) that a VRT's source can name."""
    path = tmp_path / "sentinel.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:32631",
        transform=from_origin(0, 4, 1, 1),
    ) as w:
        w.write(np.full((1, 4, 4), SENTINEL, dtype="float32"))
    return path


def _refused(raw) -> str:
    with pytest.raises(_gdal_policy.DriverPolicyError) as raised:
        _gdal_policy.open_bytes(raw).close()
    return str(raised.value)


def _assert_refusal(msg: str, driver: str):
    assert driver in msg, msg
    assert _gdal_policy.REFUSED_PHRASE in msg, msg
    assert "Fix:" in msg and "GeoTIFF" in msg and "virtual tile" in msg, msg


def test_vrt_bytes_with_a_local_source_are_refused_before_the_source_is_read(
    sentinel_tif,
):
    msg = _refused(_vrt(str(sentinel_tif)))
    _assert_refusal(msg, "VRT")


def test_vrt_bytes_with_a_vsicurl_source_are_refused_by_message():
    msg = _refused(_vrt("/vsicurl/http://example.invalid/x.tif"))
    _assert_refusal(msg, "VRT")
    assert "CURL" not in msg and "example.invalid" not in msg, msg


def test_a_memoryfile_of_vrt_bytes_is_refused_too(sentinel_tif):
    with MemoryFile(_vrt(str(sentinel_tif))) as mf:
        msg = _refused(mf)
    _assert_refusal(msg, "VRT")


def test_a_label_format_is_refused():
    pds = (
        b"PDS_VERSION_ID = PDS3\r\nRECORD_TYPE = FIXED_LENGTH\r\nRECORD_BYTES = 4\r\n"
        b'^IMAGE = "data.img"\r\nOBJECT = IMAGE\r\n LINES = 4\r\n LINE_SAMPLES = 4\r\n'
        b" SAMPLE_BITS = 8\r\nEND_OBJECT = IMAGE\r\nEND\r\n"
    )
    _assert_refusal(_refused(pds), "PDS")


def test_a_caller_naming_a_refused_driver_is_refused_by_name(sentinel_tif):
    msg = _refused_by_driver_kw(_vrt(str(sentinel_tif)), "VRT")
    _assert_refusal(msg, "VRT")


def _refused_by_driver_kw(raw, driver) -> str:
    with pytest.raises(_gdal_policy.DriverPolicyError) as raised:
        _gdal_policy.open_bytes(raw, driver=driver).close()
    return str(raised.value)


def _netcdf_bytes() -> bytes:
    scipy_io = pytest.importorskip("scipy.io")
    buf = io.BytesIO()
    f = scipy_io.netcdf_file(buf, "w", version=2)
    f.createDimension("y", 4)
    f.createDimension("x", 4)
    v = f.createVariable("v", "f4", ("y", "x"))
    v[:] = np.arange(16, dtype="f4").reshape(4, 4)
    y = f.createVariable("y", "f8", ("y",))
    y[:] = [3.5, 2.5, 1.5, 0.5]
    y.units = "degrees_north"
    x = f.createVariable("x", "f8", ("x",))
    x[:] = [0.5, 1.5, 2.5, 3.5]
    x.units = "degrees_east"
    f.flush()
    return buf.getvalue()


def _memfile_bytes(driver: str) -> bytes:
    with MemoryFile() as mf:
        with mf.open(
            driver=driver,
            width=4,
            height=4,
            count=1,
            dtype="uint8",
            crs="EPSG:32631",
            transform=from_origin(0, 4, 1, 1),
        ) as w:
            w.write(np.arange(16, dtype="uint8").reshape(1, 4, 4))
        return mf.read()


def _gpkg_bytes(tmp_path) -> bytes:
    path = tmp_path / "a.gpkg"
    with rasterio.open(
        path,
        "w",
        driver="GPKG",
        width=4,
        height=4,
        count=1,
        dtype="uint8",
        crs="EPSG:32631",
        transform=from_origin(0, 4, 1, 1),
    ) as w:
        w.write(np.arange(16, dtype="uint8").reshape(1, 4, 4))
    return path.read_bytes()


@pytest.mark.parametrize("driver", ["netCDF", "HFA", "JP2OpenJPEG", "GPKG"])
def test_the_formats_kept_readable_still_open_from_bytes(driver, tmp_path):
    """One row per format the ruling keeps: the bytes open, and as the driver that wrote them."""
    try:
        raw = (
            _netcdf_bytes()
            if driver == "netCDF"
            else _gpkg_bytes(tmp_path) if driver == "GPKG" else _memfile_bytes(driver)
        )
        with MemoryFile(raw) as mf, mf.open() as plain:
            assert plain.driver == driver
    except pytest.skip.Exception:
        raise
    except (
        Exception
    ) as exc:  # noqa: BLE001 - the environment's GDAL lacks the format or cannot write it
        pytest.skip(f"this GDAL cannot make {driver} bytes here: {str(exc)[:80]}")
    with _gdal_policy.open_bytes(raw) as ds:
        assert ds.driver == driver
        assert ds.read().shape[-2:] == (4, 4)


def test_a_geotiff_opens_and_a_png_opens():
    for driver in ("GTiff", "PNG"):
        with _gdal_policy.open_bytes(_memfile_bytes(driver)) as ds:
            assert ds.driver == driver


def test_the_class_is_in_the_manifest_next_to_bytes_and_holds_what_the_ruling_names():
    man = manifest.load().gdal
    refused = set(man.bytes_refused_light_drivers)
    assert not refused & set(man.bytes_drivers)
    assert refused <= set(man.allowed_drivers)
    assert len(refused) == len(man.bytes_refused_light_drivers)
    named = {
        # names a file or dataset GDAL opens
        "VRT",
        "DERIVED",
        "DIMAP",
        "SAFE",
        "PDS4",
        "ECRGTOC",
        "RPFTOC",
        "TIL",
        "MEM",
        "NUMPY",
        # a label or header that names its data file, and the product directories
        "PDS",
        "ISIS2",
        "ISIS3",
        "VICAR",
        "ENVI",
        "EHdr",
        "GenBin",
        "ERS",
        "PAux",
        "MFF",
        "MFF2",
        "RST",
        "ILWIS",
        "SAGA",
        "ISCE",
        "ROI_PAC",
        "CEOS",
        "SAR_CEOS",
        "AirSAR",
        "RS2",
        "RCM",
        "TSX",
        "COSAR",
        "SENTINEL2",
        "AIG",
        "DOQ1",
        "DOQ2",
        "FAST",
        "ESAT",
    }
    assert refused == named, sorted(refused ^ named)
    # kept readable, by the ruling
    assert not refused & {"netCDF", "HDF5", "HFA", "JP2OpenJPEG", "GPKG"}


def test_the_class_validation_refuses_a_driver_that_is_not_allowed_and_a_repeat():
    raw = {
        "pin": "3.13.3",
        "raster_drivers": {"allowed": ["GTiff", "VRT"], "refused": []},
    }
    ok = dict(raw["raster_drivers"], bytes_refused_light=["VRT"])
    assert manifest._parse_gdal_section(
        dict(raw, raster_drivers=ok)
    ).bytes_refused_light_drivers == ("VRT",)
    for bad in (["NOPE"], ["VRT", "VRT"]):
        with pytest.raises(ValueError, match="bytes_refused_light"):
            manifest._parse_gdal_section(
                dict(
                    raw,
                    raster_drivers=dict(raw["raster_drivers"], bytes_refused_light=bad),
                )
            )


def test_a_real_light_function_gives_the_refusal_with_its_fix_on_vrt_bytes(
    sentinel_tif,
):
    """rst_rastertoworldcoord on a tile whose bytes are a VRT: the error carries the Fix, and no pixel of the
    sentinel file is read."""
    from databricks.labs.gbx.pyrx import functions as fns

    tile = {"raster": _vrt(str(sentinel_tif))}
    with pytest.raises(_gdal_policy.DriverPolicyError) as raised:
        fns._rastertoworldcoord_udf.func(tile, 1, 1)
    _assert_refusal(str(raised.value), "VRT")


def test_the_constructor_refuses_vrt_bytes_with_the_fix(sentinel_tif):
    from databricks.labs.gbx.pyrx import functions as fns

    with pytest.raises(_gdal_policy.DriverPolicyError) as raised:
        fns._fromcontent_udf.func(_vrt(str(sentinel_tif)), "VRT")
    _assert_refusal(str(raised.value), "VRT")


def test_a_virtual_tile_path_to_a_vrt_is_not_bytes_and_still_opens(
    sentinel_tif, tmp_path
):
    """The virtual-tile policy keeps judging paths: a VRT as a PATH is opened by open_data, not refused here."""
    vrt = tmp_path / "v.vrt"
    vrt.write_bytes(_vrt(str(sentinel_tif)))
    with _gdal_policy.open_data(str(vrt)) as ds:
        assert ds.driver == "VRT"
        assert float(ds.read()[0, 0, 0]) == SENTINEL


def test_the_refusal_names_no_planning_code():
    msg = _refused(_vrt("/vsicurl/http://example.invalid/x.tif"))
    assert not re.search(r"\b(FX|AP|OD|NP|PK)-\d+", msg), msg
