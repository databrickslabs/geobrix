"""Unit tests for the writer's per-tile byte production (no Spark)."""

import numpy as np
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
