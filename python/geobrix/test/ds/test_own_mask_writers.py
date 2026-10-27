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


@pytest.mark.parametrize(
    "mode,name",
    [(m, n) for m in GRID_MODES for n in ("rgb_nir", "rgba", "gray_x_alpha")]
    # the 6-band alpha layout and the non-last alpha label, on the cheapest grids
    + [("bng", "rgb_x_alpha"), ("bng", "rgba_x"), ("h3", "rgba_x")],
)
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
    """A 640x320 source (64 x 32 km) of one value, whose left half is hidden by the mask
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


def _cell_ids(tiles):
    """The cell id in each tile's name (``cell_<source>_<cellid>.tif``)."""
    return {os.path.basename(t)[:-4].split("_", 2)[2] for t in tiles}


@pytest.mark.parametrize("mode", list(GRID_MODES))
@pytest.mark.parametrize(
    "hide,name",
    [("masked", "rgb_nir"), ("transparent", "rgba"), ("transparent", "gray_x_alpha")],
)
def test_grid_mosaic_prunes_cells_with_no_valid_pixel(tmp_path, mode, hide, name):
    """Cells that lie wholly in the hidden half of the source (masked, or transparent) are not
    written; every cell that reaches the visible half is, and holds a valid pixel."""
    from rasterio.warp import transform_bounds

    src_a = _half_hidden_source(tmp_path / "a", mode, name, None)
    src_b = _half_hidden_source(tmp_path / "b", mode, name, hide)
    shown = _grid_tiles(tmp_path / "a", mode, src_a)
    pruned = _grid_tiles(tmp_path / "b", mode, src_b)
    assert pruned and len(pruned) < len(shown), (len(pruned), len(shown))
    with rasterio.open(src_b) as src:
        visible_from = src.bounds.left + src.width // 2 * src.transform.a
        src_crs = src.crs
    written = _cell_ids(pruned)
    assert written <= _cell_ids(shown)
    for tile in shown:
        with rasterio.open(tile) as cell:
            right = transform_bounds(cell.crs, src_crs, *cell.bounds)[2]
        reaches = (
            right > visible_from + 1000.0
        )  # a margin: the cell's bbox edge is not its data
        if reaches:
            assert (
                _cell_ids([tile]) <= written
            ), f"{os.path.basename(tile)}: not written"
    for tile in pruned:
        with rasterio.open(tile) as cell:
            assert nd.valid_mask(
                cell
            ).any(), f"{os.path.basename(tile)}: no valid pixel"


@pytest.mark.parametrize("mode", list(GRID_MODES))
@pytest.mark.parametrize("name", ["rgba", "gray_x_alpha", "rgb_nir"])
def test_the_mosaic_vrt_of_the_cells_of_an_opaque_source_is_valid_inside_the_source(
    tmp_path, mode, name
):
    """The index VRT composites the cells: where the source is opaque, the mosaic is valid. The h3
    cells of an alpha source declare no NoData, and their bounding-box corners used to overwrite the
    neighbouring cells (about a quarter of the pixels came out transparent)."""
    from rasterio.warp import transform_bounds

    from databricks.labs.gbx.ds.cog_writer import _build_mosaic_vrt

    src_path = _half_hidden_source(tmp_path, mode, name, None)
    tiles = _grid_tiles(tmp_path, mode, src_path)
    assert len(tiles) > 1
    vrt_path = _build_mosaic_vrt(tiles, str(tmp_path / "out"))
    with rasterio.open(src_path) as src, rasterio.open(vrt_path) as vrt:
        left, bottom, right, top = transform_bounds(src.crs, vrt.crs, *src.bounds)
        w, h = right - left, top - bottom
        inner = vrt.window(
            left + 0.1 * w, bottom + 0.1 * h, right - 0.1 * w, top - 0.1 * h
        )
        valid = nd.valid_mask(vrt, window=inner)
        labels = al.labelled_alpha(open(tiles[0], "rb").read())
        assert [i for i, c in enumerate(vrt.colorinterp) if c.name == "alpha"] == labels
    assert valid.size > 1000
    assert (
        valid.all()
    ), f"{int((~valid).sum())} of {valid.size} pixels inside the source are invalid"


def test_the_mosaic_vrt_labels_a_non_last_alpha_band_as_data(tmp_path):
    """RGBA+extra: the alpha label is not on the last band, so the index VRT carries no alpha band."""
    from databricks.labs.gbx.ds.cog_writer import _build_mosaic_vrt

    tiles = []
    for i, x in enumerate((10, 11)):
        raw = al.layout_bytes("rgba_x", transform=from_origin(x, 50, 0.25, 0.25))
        path = str(tmp_path / f"t{i}.tif")
        with open(path, "wb") as fh:
            fh.write(raw)
        tiles.append(path)
    vrt_path = _build_mosaic_vrt(tiles, str(tmp_path))
    with rasterio.open(vrt_path) as vrt:
        assert [c.name for c in vrt.colorinterp] == [
            "red",
            "green",
            "blue",
            "undefined",
            "undefined",
        ]


@pytest.mark.parametrize(
    "mode,width,height,name",
    [
        ("quadbin", 600, 300, "rgb_nir"),
        ("h3", 600, 300, "rgba"),
        ("h3", 800, 300, "rgb_nir"),
        ("bng", 600, 300, "rgba"),
    ],
)
def test_a_cell_whose_source_window_rounds_to_no_pixel_is_skipped(
    tmp_path, mode, width, height, name
):
    """A cell the source barely touches reads a window of fewer than half a pixel: the read comes back
    with no rows and the reprojection raised ``Invalid dataset dimensions : 62 x 0``."""
    _, crs, origin = GRID_MODES[mode]
    ox, oy = (origin[0] + 37.0, origin[1] - 53.0) if mode == "bng" else origin
    count = len(al.LAYOUTS[name][0])
    raw = al.layout_bytes(
        name,
        data=np.full((count, height, width), 50, dtype="uint8"),
        crs=crs,
        transform=from_origin(ox, oy, 100.0, 100.0),
    )
    path = str(tmp_path / "src" / f"{name}.tif")
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as fh:
        fh.write(raw)
    res = {"h3": {"gridSystem": "h3", "gridResolution": "6"}}.get(
        mode, GRID_MODES[mode][0]
    )
    opts = {"vrtMosaic": "true", **res}
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=parse_mosaic_options(opts))
    msg = w.write(iter([{"path": path}]))
    assert [p for p in msg.paths if p.endswith(".tif")], f"{mode}: no cell written"


@pytest.mark.parametrize(
    "hide,name",
    [("masked", "rgb_nir"), ("transparent", "rgba"), ("transparent", "gray_x_alpha")],
)
def test_mosaic_window_prune_checks_the_mask_and_the_alpha_band(tmp_path, hide, name):
    """tileSize 32 over 64x64: the window that is entirely masked (or entirely transparent)
    is not written; the other three are."""
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


# --- an alpha band AND an own mask band ------------------------------------------------------


@pytest.mark.parametrize("tile_format", ["gtiff", "cog"])
@pytest.mark.parametrize("name", ["rgba", "gray_x_alpha"])
def test_encode_tile_keeps_the_mask_beside_the_alpha_band(tile_format, name):
    """GDAL reports only the own mask of a raster that has both: the tile encoder writes it with the
    pixels and the alpha band, and the rows hidden by either stay invalid."""
    raw = al.row_coded(name, height=12, masked_rows=(3, 4), transparent_rows=(8,))
    with MemoryFile(raw) as mf, mf.open() as ds:
        want = nd.valid_mask(ds)
        _, out, _ = _encode.encode_tile(
            ds, (0, 0, 12, 12), "/x/src.tif", "", tile_format=tile_format
        )
    with MemoryFile(out) as mf, mf.open() as got:
        assert nd.has_own_mask(got) and nd.has_alpha(got)
        assert (nd.valid_mask(got) == want).all()
    assert not want.all() and want.any()


@pytest.mark.parametrize("name", ["rgba", "gray_x_alpha"])
def test_apply_compression_keeps_the_mask_beside_the_alpha_band(name):
    raw = al.row_coded(name, height=12, masked_rows=(3, 4), transparent_rows=(8,))
    out = _apply_compression(raw, "deflate", None, None)
    with MemoryFile(raw) as a, a.open() as src, MemoryFile(out) as b, b.open() as got:
        assert (nd.valid_mask(got) == nd.valid_mask(src)).all()
        assert nd.has_own_mask(got) and nd.has_alpha(got)
