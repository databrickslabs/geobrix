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
from scipy import ndimage

from databricks.labs.gbx.ds import _encode
from databricks.labs.gbx.ds.cog_writer import parse_mosaic_options
from databricks.labs.gbx.ds.writer import _apply_compression
from databricks.labs.gbx.pyrx.core import _nodata as nd

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


# --- the grid mosaic modes and the pruneEmpty check -----------------------------------------
# A cell of the quadbin / h3 / BNG mosaic modes is reprojected from a window of the source onto
# the cell's grid, so its mask band is warped with it. A window or cell with no valid pixel
# (entirely transparent, entirely masked, or entirely NoData) is pruned.

GRID_ROWS, GRID_COLS = 100, 200
GRID_MASKED = tuple(range(20, 30))
GRID_TRANSPARENT = tuple(range(60, 70))
# mode -> (gridSystem options, source CRS, source origin)
GRID_MODES = {
    "quadbin": (
        {"gridSystem": "quadbin", "gridResolution": "12"},
        "EPSG:32630",
        (500000.0, 5700000.0),
    ),
    "h3": (
        {"gridSystem": "h3", "gridResolution": "5"},
        "EPSG:32630",
        (500000.0, 5700000.0),
    ),
    "bng": (
        {"gridSystem": "bng", "gridResolution": "10km"},
        "EPSG:27700",
        (530000.0, 180000.0),
    ),
}


def _grid_source(
    tmp_path, mode, name, masked=GRID_MASKED, transparent=GRID_TRANSPARENT
):
    _, crs, origin = GRID_MODES[mode]
    raw = al.row_coded(
        name,
        width=GRID_COLS,
        height=GRID_ROWS,
        masked_rows=masked,
        transparent_rows=transparent,
        crs=crs,
        transform=from_origin(origin[0], origin[1], 100.0, 100.0),
    )
    path = str(tmp_path / "src" / f"{name}.tif")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(raw)
    return path


def _grid_tiles(tmp_path, mode, src_path):
    opts = {"vrtMosaic": "true", **GRID_MODES[mode][0]}
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=parse_mosaic_options(opts))
    msg = w.write(iter([{"path": src_path}]))
    return [p for p in msg.paths if p.endswith(".tif")]


def _interior(rows, margin=3):
    """The rows of ``rows`` (a run of source rows) that are at least ``margin`` rows from its
    ends: a reprojection mixes the pixels next to a boundary."""
    rows = sorted(rows)
    return rows[margin:-margin]


@pytest.mark.parametrize("name", ["rgb_nir", "rgba", "gray_x_alpha"])
@pytest.mark.parametrize("mode", list(GRID_MODES))
def test_grid_mosaic_cells_keep_the_mask_and_the_alpha_band(tmp_path, mode, name):
    """Every cell's pixels that come from a masked source row are invalid, and so are the
    ones from a transparent row of an alpha source; the others are valid. The 4th band of
    RGB+NIR stays data."""
    alpha = al.LAYOUTS[name][1]
    tiles = _grid_tiles(tmp_path, mode, _grid_source(tmp_path, mode, name))
    assert tiles, f"{mode}: no cells written"
    hidden = _interior(GRID_MASKED) + (_interior(GRID_TRANSPARENT) if alpha else [])
    visible = (
        _interior(range(0, 17)) + _interior(range(33, 57)) + _interior(range(73, 100))
    )
    seen_hidden = seen_visible = 0
    for tile in tiles:
        with rasterio.open(tile) as out:
            assert al.labelled_alpha(open(tile, "rb").read()) == (
                [out.count - 1] if alpha else []
            )
            coded = out.read(1).astype("int64") - 100
            valid = nd.valid_mask(out)
        # a pixel next to the source's edge mixes in the fill: leave the ring out
        inner = ndimage.binary_erosion(coded >= 0, structure=np.ones((3, 3)))
        hid = np.isin(coded, hidden) & inner
        vis = np.isin(coded, visible) & inner
        seen_hidden += int(hid.sum())
        seen_visible += int(vis.sum())
        assert not valid[hid].any(), f"{os.path.basename(tile)}: hidden pixels valid"
        assert valid[vis].all(), f"{os.path.basename(tile)}: visible pixels invalid"
    assert seen_hidden and seen_visible, "fixture: the cells must hold both"


@pytest.mark.parametrize("name", ["rgba", "gray_x_alpha"])
def test_h3_cells_of_an_alpha_source_declare_no_nodata(tmp_path, name):
    """h3 cells always declare a NoData for the pixels outside the hexagon, which would
    shadow an alpha band: for an alpha source the pixels outside are written alpha 0 instead.
    """
    tiles = _grid_tiles(tmp_path, "h3", _grid_source(tmp_path, "h3", name))
    assert tiles
    for tile in tiles:
        with rasterio.open(tile) as out:
            assert out.nodata is None
            clear = out.read(out.count) == 0
            valid = nd.valid_mask(out)
        assert clear.any(), "the pixels outside the hexagon are alpha 0"
        assert not valid[clear].any()


def _half_hidden_source(tmp_path, mode, name, hide):
    """A 600x300 source (60 x 30 km) of one value, whose left half is hidden by the mask
    (``hide="masked"``) or by alpha 0 (``"transparent"``), or not hidden at all (``None``).
    """
    _, crs, origin = GRID_MODES[mode]
    width, height = 640, 320
    data = np.full((len(al.LAYOUTS[name][0]), height, width), 50, dtype="uint8")
    mask = None
    left = np.arange(width)[None, :] < width // 2
    if hide == "masked":
        mask = np.where(left, 0, 255).astype("uint8") * np.ones((height, 1), "uint8")
    elif hide == "transparent":
        data[-1] = np.where(left, 0, 255)
    raw = al.layout_bytes(
        name,
        data=data,
        mask=mask,
        crs=crs,
        transform=from_origin(origin[0], origin[1], 100.0, 100.0),
    )
    path = str(tmp_path / "src" / f"{name}_{hide}.tif")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(raw)
    return path


@pytest.mark.parametrize("mode", list(GRID_MODES))
@pytest.mark.parametrize("hide,name", [("masked", "rgb_nir"), ("transparent", "rgba")])
def test_grid_mosaic_prunes_cells_with_no_valid_pixel(tmp_path, mode, hide, name):
    """Cells that lie wholly in the hidden half of the source (masked, or transparent) are
    not written: fewer cells than for the same source with nothing hidden. A cell that
    reaches the visible half is written."""
    shown = _grid_tiles(
        tmp_path / "a", mode, _half_hidden_source(tmp_path / "a", mode, name, None)
    )
    pruned = _grid_tiles(
        tmp_path / "b", mode, _half_hidden_source(tmp_path / "b", mode, name, hide)
    )
    assert pruned and len(pruned) < len(shown), (len(pruned), len(shown))


@pytest.mark.parametrize("hide", ["masked", "transparent"])
def test_mosaic_window_prune_checks_the_mask_and_the_alpha_band(tmp_path, hide):
    """tileSize 32 over 64x64: the window that is entirely masked (or entirely transparent)
    is not written; the other three are."""
    name = "rgb_nir" if hide == "masked" else "rgba"
    width = height = 64
    rows = np.arange(height)[:, None]
    cols = np.arange(width)[None, :]
    corner = (rows < 32) & (cols < 32)  # window (0, 0)
    data = np.full((len(al.LAYOUTS[name][0]), height, width), 50, dtype="uint8")
    mask = None
    if hide == "masked":
        mask = np.where(corner, 0, 255).astype("uint8")
    else:
        data[-1] = np.where(corner, 0, 255)
    raw = al.layout_bytes(name, data=data, mask=mask)
    src = str(tmp_path / "src" / "w.tif")
    os.makedirs(os.path.dirname(src))
    with open(src, "wb") as fh:
        fh.write(raw)
    w = _make_writer(
        str(tmp_path / "out"), mosaic_opts=_default_mosaic_opts(tileSize=32)
    )
    msg = w.write(iter([{"path": src}]))
    names = sorted(os.path.basename(p) for p in msg.paths if p.endswith(".tif"))
    assert len(names) == 3, names
    assert not any(n.endswith("_0_0.tif") for n in names), names
