"""The light tile encoders and writers carry a source's own (per-dataset) mask band and
its alpha label.

A masked source (JPEG/YCbCr imagery often has an internal mask band) came out of the
``raster_gbx`` tile encoder (``_encode.encode_tile``), the mosaic COG writer's tile window and
the compression re-encode of ``raster_gbx`` with every masked pixel valid, because the
pixels were rewritten without the mask. A 4-band ``uint8`` source whose band 4 is data
(RGB+NIR) also came out with GDAL's default alpha label on band 4.
"""

import os

import numpy as np
import pytest
import rasterio
from rasterio.enums import MaskFlags
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds import _encode
from databricks.labs.gbx.ds.writer import _apply_compression

from ..pyrx import _alpha_layouts as al
from .test_mosaic_write import _default_mosaic_opts, _make_writer

W, H = 64, 48
MASKED_ROWS = (10, 11, 30)


def _masked_bytes(nodata=None):
    """uint16 GTiff with an internal per-dataset mask hiding ``MASKED_ROWS``."""
    data = (np.arange(W * H, dtype="uint32").reshape(1, H, W) % 5000 + 1).astype(
        "uint16"
    )
    mask = np.full((H, W), 255, dtype="uint8")
    mask[list(MASKED_ROWS), :] = 0
    prof = dict(
        driver="GTiff",
        width=W,
        height=H,
        count=1,
        dtype="uint16",
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
        nodata=nodata,
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**prof) as dst:
                dst.write(data)
                dst.write_mask(mask)
            raw = mf.read()
    with MemoryFile(raw) as mf, mf.open() as chk:
        assert chk.mask_flag_enums[0] == [MaskFlags.per_dataset], "fixture needs a mask"
    return raw


def _valid(raster_bytes):
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return ds.read_masks(1) != 0


@pytest.mark.parametrize("tile_format", ["gtiff", "cog"])
@pytest.mark.parametrize("window", [(0, 0, W, H), (8, 5, 40, 30)])
def test_encode_tile_keeps_the_mask(tile_format, window):
    raw = _masked_bytes()
    col, row, w, h = window
    with MemoryFile(raw) as mf, mf.open() as ds:
        want = ds.read_masks(1, window=rasterio.windows.Window(*window)) != 0
        _, out, _ = _encode.encode_tile(
            ds, window, "/x/src.tif", "", tile_format=tile_format
        )
    assert not want.all(), "fixture: the window must hold masked pixels"
    got = _valid(out)
    assert got.shape == want.shape
    assert (got == want).all(), f"{int((got != want).sum())} pixels differ in validity"


@pytest.mark.parametrize("tile_format", ["gtiff", "cog"])
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_encode_tile_labels_alpha_where_its_source_does(tile_format, name):
    with MemoryFile(al.layout_bytes(name)) as mf, mf.open() as ds:
        _, out, _ = _encode.encode_tile(
            ds, (0, 0, 4, 4), "/x/src.tif", "", tile_format=tile_format
        )
        want = [ds.count - 1] if al.LAYOUTS[name][1] else []
    assert al.labelled_alpha(out) == want


def test_apply_compression_keeps_the_mask_and_the_alpha_label():
    out = _apply_compression(_masked_bytes(), "deflate", None, None)
    with MemoryFile(_masked_bytes()) as mf, mf.open() as ds:
        want = ds.read_masks(1) != 0
    assert (_valid(out) == want).all()
    rgb_nir = al.layout_bytes("rgb_nir")
    assert al.labelled_alpha(_apply_compression(rgb_nir, "deflate", None, None)) == []
    rgba = al.layout_bytes("rgba")
    assert al.labelled_alpha(_apply_compression(rgba, "deflate", None, None)) == [3]


def _write_masked_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(_masked_bytes())
    return path


@pytest.mark.parametrize("tile_size", [32, 100])
def test_mosaic_window_tiles_keep_the_mask(tmp_path, tile_size):
    """The COG writer's mosaic tiles (the normal-size window path) carry the source's mask:
    every mini-COG's validity equals its source window's."""
    from databricks.labs.gbx.ds.cog_writer import _tile_grid_windows

    src_path = _write_masked_file(str(tmp_path / "src" / "masked.tif"))
    w = _make_writer(
        str(tmp_path / "out"), mosaic_opts=_default_mosaic_opts(tileSize=tile_size)
    )
    msg = w.write(iter([{"path": src_path}]))
    tiles = [p for p in msg.paths if p.endswith(".tif")]
    assert tiles
    seen_masked = 0
    with rasterio.open(src_path) as src:
        windows = {
            (r, c): win
            for r, c, win in _tile_grid_windows(src.width, src.height, tile_size, 0.0)
        }
        for tile in tiles:
            name = os.path.basename(tile)[:-4].split("_")
            win = windows[(int(name[-2]), int(name[-1]))]
            want = src.read_masks(1, window=win) != 0
            with rasterio.open(tile) as out:
                got = out.read_masks(1) != 0
            seen_masked += int((~want).sum() > 0)
            assert (got == want).all(), f"{os.path.basename(tile)}: validity differs"
    assert seen_masked, "fixture: some tile must hold masked pixels"
