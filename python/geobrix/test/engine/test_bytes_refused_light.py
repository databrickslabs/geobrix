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


# ---- XML markers belong to the start of the document; known binary formats are never scanned

_XMP = b"<VRTDataset rasterXSize='4' rasterYSize='4'>"


def _png_with_text_chunk(text: bytes) -> bytes:
    """A real PNG whose tEXt chunk (right after IHDR, so inside the first 1024 bytes) holds ``text``."""
    import struct
    import zlib

    raw = _memfile_bytes("PNG")
    ihdr_end = 8 + 8 + 13 + 4  # signature, chunk header, IHDR data, its CRC
    body = b"Comment\x00" + text
    chunk = (
        struct.pack(">I", len(body))
        + b"tEXt"
        + body
        + struct.pack(">I", zlib.crc32(b"tEXt" + body))
    )
    return raw[:ihdr_end] + chunk + raw[ihdr_end:]


def test_a_png_that_carries_vrt_xml_in_a_text_chunk_opens_as_a_png():
    raw = _png_with_text_chunk(_XMP)
    assert _XMP in raw[:1024]
    with _gdal_policy.open_bytes(raw) as ds:
        assert ds.driver == "PNG"
        assert ds.read().shape[-2:] == (4, 4)


@pytest.fixture
def signatures_only(monkeypatch):
    """The path a process without the matching osgeo takes: no GDAL identify, only how the bytes start."""
    monkeypatch.setattr(_gdal_policy, "_identify_in_class", lambda head, names: None)


@pytest.mark.parametrize(
    "magic",
    [
        b"\x89PNG\r\n\x1a\n",
        b"\xff\xd8\xff\xe1",
        b"GIF89a",
        b"CDF\x01",
        b"CDF\x02",
        b"\x89HDF\r\n\x1a\n",
        b"II*\x00",
    ],
)
def test_binary_bytes_are_never_scanned_for_an_xml_marker(signatures_only, magic):
    head = magic + b"\x00" * 8 + _XMP + b"<Dimap_Document><Product_Observational"
    assert _gdal_policy._class_of_bytes(head, "partial") is None


@pytest.mark.parametrize(
    "head,driver",
    [
        (_XMP, "VRT"),
        (b"\xef\xbb\xbf  \n" + _XMP, "VRT"),
        (b'<?xml version="1.0" encoding="UTF-8"?>\n' + _XMP, "VRT"),
        (b"<!-- made by x -->\n<Dimap_Document>", "DIMAP"),
        (
            b'<?xml version="1.0"?>\n<?xml-model href="x.sch"?>\n<Product_Observational xmlns="u">',
            "PDS4",
        ),
    ],
)
def test_xml_formats_are_recognized_from_the_start_of_the_document(
    signatures_only, head, driver
):
    assert _gdal_policy._class_of_bytes(head, "partial") == driver


def test_a_marker_inside_text_that_is_not_xml_is_not_a_refusal(signatures_only):
    assert (
        _gdal_policy._class_of_bytes(b"notes: see " + _XMP + b" for details", "partial")
        is None
    )


def test_real_vrt_xml_with_a_prolog_is_still_refused(sentinel_tif):
    msg = _refused(b'\xef\xbb\xbf<?xml version="1.0"?>\n' + _vrt(str(sentinel_tif)))
    _assert_refusal(msg, "VRT")


# ---- bytes no allowed driver recognizes: a refused driver's own get the refusal with its Fix


def test_unrecognized_bytes_the_full_bytes_identify_as_a_class_driver_name_it(
    monkeypatch,
):
    """ENVI-like: the first 4096 bytes say nothing (identify wants a sibling header), the whole bytes do."""
    monkeypatch.setattr(_gdal_policy, "activate", lambda: {"mode": "full"})
    monkeypatch.setattr(
        _gdal_policy,
        "_identify_in_class",
        lambda head, names: "ENVI" if len(head) > 4096 else None,
    )
    msg = _refused(b"\x01\x02\x03 raw pixels " * 600)
    _assert_refusal(msg, "ENVI")


# ---- rst_asformat leaves the class out only where the policy refuses it


def test_asformat_leaves_the_refused_class_out_with_the_policy_on_and_in_with_it_off(
    monkeypatch,
):
    ops = pytest.importorskip("databricks.labs.gbx.pyrx.core.ops")
    writable_class = sorted(
        set(ops.rasterio.drivers.raster_driver_extensions().values())
        & set(_gdal_policy.bytes_refused_light_names())
    )
    if not writable_class:
        pytest.skip("this GDAL build has no writable driver of the refused class")
    monkeypatch.setattr(ops, "activate", lambda: {"mode": "full"})
    assert not set(writable_class) & ops._writable_drivers()
    monkeypatch.setattr(ops, "activate", lambda: {"mode": "off"})
    assert set(writable_class) <= ops._writable_drivers()
