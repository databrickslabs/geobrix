"""Unit tests for the writer's per-tile byte production (no Spark)."""

import logging

import numpy as np
import pytest
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds import _write


def _gtiff_bytes(width=4, height=3, dtype="float32"):
    from rasterio.transform import from_origin

    data = np.arange(width * height, dtype=dtype).reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def test_gtiff_target_is_verbatim():
    raw = _gtiff_bytes()
    meta = {"driver": "GTiff", "format": "GTiff", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver=None
    )
    assert out == raw


def test_force_gtiff_is_verbatim_even_if_metadata_says_otherwise():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG"}
    out = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver="GTiff"
    )
    assert out == raw


def test_a_zarr_tile_is_written_as_the_geotiff_its_bytes_are():
    """A Zarr tile names its source store in its metadata (driver Zarr) but carries GeoTIFF bytes,
    so it passes through verbatim; a driver the writer forces wins over that name."""
    raw = _gtiff_bytes()
    meta = {"driver": "Zarr", "format": "Zarr"}
    out = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver=None
    )
    assert out == raw
    forced = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver="COG"
    )
    assert forced != raw  # the forced COG is a re-encode, not the verbatim bytes


def test_non_gtiff_target_reencodes_same_pixels_with_tags():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(
        cellid=7, raster_bytes=raw, metadata=meta, force_driver=None
    )
    assert out != raw
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.driver in ("COG", "GTiff")
        arr = ds.read(1)
        tags = ds.tags()
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )
    assert tags.get("RASTERX_CELL") == "7"
    assert tags.get("RASTERX_driver") == "COG"


@pytest.mark.parametrize("driver", ["PNG", "JPEG"])
@pytest.mark.parametrize("compression", ["none", "DEFLATE"])
@pytest.mark.parametrize("source_codec", [None, "deflate"])
def test_a_png_or_jpeg_tile_is_not_handed_a_codec_option(
    caplog, driver, compression, source_codec
):
    """The writer's profile starts from a GTiff source (whose own codec it keeps) and adds the
    codec asked for: PNG and JPEG take neither, and GDAL logs each one it is handed."""
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 8.0, 1.0, 1.0),
    )
    if source_codec:
        profile["compress"] = source_codec
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.arange(192, dtype="uint8").reshape(3, 8, 8))
        raw = mf.read()
    with caplog.at_level(logging.WARNING, logger="rasterio._env"):
        out = _write.tile_to_bytes(
            cellid=1,
            raster_bytes=raw,
            metadata={"compression": compression},
            force_driver=driver,
        )
    assert out[:4] in (
        b"\x89PNG",
        b"\xff\xd8\xff\xe0",
        b"\xff\xd8\xff\xdb",
        b"\xff\xd8\xff\xee",
    )
    codec = [
        r.getMessage()
        for r in caplog.records
        if "CPLE_NotSupported" in r.getMessage()
        and any(k in r.getMessage() for k in ("COMPRESS", "PREDICTOR", "ZLEVEL"))
    ]
    assert codec == []


# --- alpha labels and the own mask of the re-encoded tile ----------------------------------


def _labelled_alpha(raster_bytes):
    from ..pyrx import _alpha_layouts as al

    return al.labelled_alpha(raster_bytes)


def test_non_gtiff_target_labels_alpha_where_its_source_does():
    """The re-encode keeps the alpha band of the tile and does not turn a 4th data band
    (RGB+NIR) into one: GDAL labels band 4 of a 4-band uint8 raster alpha when it creates one.
    """
    from ..pyrx import _alpha_layouts as al

    for name in al.LAYOUTS:
        out = _write.tile_to_bytes(
            cellid=1,
            raster_bytes=al.layout_bytes(name),
            metadata={"driver": "COG", "format": "COG", "compression": "DEFLATE"},
        )
        want = [len(al.LAYOUTS[name][0]) - 1] if al.LAYOUTS[name][1] else []
        assert _labelled_alpha(out) == want, name


def test_non_gtiff_target_keeps_the_tiles_own_mask_beside_an_alpha_band():
    """A COG re-encode carries the tile's own (per-dataset) mask band, with or without an
    alpha band: masked rows stay invalid."""
    from databricks.labs.gbx.pyrx.core import _nodata as nd

    from ..pyrx import _alpha_layouts as al

    masked, transparent = (3, 4), (8,)
    for name in ("rgb_nir", "rgba", "gray_x_alpha"):
        raw = al.row_coded(
            name, height=12, masked_rows=masked, transparent_rows=transparent
        )
        out = _write.tile_to_bytes(
            cellid=1,
            raster_bytes=raw,
            metadata={"driver": "COG", "format": "COG", "compression": "DEFLATE"},
        )
        with (
            MemoryFile(raw) as a,
            a.open() as src,
            MemoryFile(out) as b,
            b.open() as got,
        ):
            want = nd.valid_mask(src)
            assert (nd.valid_mask(got) == want).all(), name
            assert not want.all() and want.any()
