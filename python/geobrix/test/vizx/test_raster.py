import io

import matplotlib
import pytest

matplotlib.use("Agg")  # headless: no display needed
from test.pyrx.conftest import make_geotiff_bytes  # noqa: E402

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import rasterio  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.vizx import (  # noqa: E402
    _raster,
    plot_file,
    plot_mask_layers,
    plot_raster,
)

NODATA = -9999.0


@pytest.fixture(autouse=True)
def _close_figures():
    plt.close("all")
    yield
    plt.close("all")


def _gtiff(bands, *, dtype="float32", nodata=None, origin_top=None):
    """GTiff bytes from a list of 2-D arrays (one per band)."""
    h, w = bands[0].shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=len(bands),
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(0.0, float(origin_top or h), 1.0, 1.0),
        nodata=nodata,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        for i, b in enumerate(bands, 1):
            ds.write(b.astype(dtype), i)
    return buf.getvalue()


def _rgb(fig):
    fig.canvas.draw()
    return np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)


def _coloured(rgb, thresh=25):
    r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
    return int(
        (
            (np.abs(r - g) > thresh)
            | (np.abs(g - b) > thresh)
            | (np.abs(r - b) > thresh)
        ).sum()
    )


def _depth_gtiff():
    """3-band 4x4: band 1 covers all, band 2 the left half, band 3 the top-left quadrant."""
    b1 = np.ones((4, 4))
    b2 = np.full((4, 4), NODATA)
    b2[:, :2] = 1.0
    b3 = np.full((4, 4), NODATA)
    b3[:2, :2] = 1.0
    return _gtiff([b1, b2, b3], nodata=NODATA)


def _correlated_gtiff():
    """3-band uint16 ramp [base, 0.8 base, 0.6 base]: shared stretch keeps the offsets."""
    base = np.linspace(1000, 30000, 256).reshape(16, 16)
    return _gtiff([base, base * 0.8, base * 0.6], dtype="uint16", nodata=65535)


def _disk_gtiff(tmp_path, name="t.tif", width=8, height=8):
    p = tmp_path / name
    arr = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(
        str(p),
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    ) as ds:
        ds.write(arr, 1)
    return str(p)


def _virtual_tile(path, window=(0, 0, 8, 8)):
    win = (
        dict(zip(("col_off", "row_off", "width", "height"), window)) if window else None
    )
    return {
        "cellid": 1,
        "raster": None,
        "path": path,
        "path_mode": "external",
        "window": win,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def _first_image():
    images = plt.gcf().axes[0].get_images()
    assert images, "nothing drawn on the axes"
    return images[0]


@pytest.mark.parametrize("scheme", ["", "dbfs:", "file:", "file://"])
def test_plot_raster_and_plot_file_each_produce_one_figure(tmp_path, scheme):
    # Scheme-qualified Databricks paths are read through the FUSE path, not rejected by rasterio.
    p = tmp_path / "t.tif"
    p.write_bytes(make_geotiff_bytes(width=8, height=8, count=3))
    plot_file(f"{scheme}{p}")
    assert len(plt.get_fignums()) == 1
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=8, height=8, count=1))
    assert len(plt.get_fignums()) == 1


def test_coverage_depth_counts_the_bands_covering_each_pixel_and_renders():
    gtiff = _depth_gtiff()
    with MemoryFile(gtiff) as mf, mf.open() as src:
        data, nd = src.read(masked=True), src.nodata
    depth = _raster._coverage_depth(data, nodata=nd)
    assert np.all(depth[:2, :2] == 3.0)  # all three bands
    assert np.all(depth[:2, 2:] == 1.0)  # band 1 only
    assert np.all(depth[2:, :2] == 2.0)  # bands 1 and 2
    assert np.all(depth[2:, 2:] == 1.0)
    plot_raster(gtiff, composite="depth", fig_w=6, fig_h=6)
    assert len(plt.get_fignums()) == 1


def test_plot_mask_layers_draws_each_mask_in_its_own_colour_with_a_legend():
    def mask(lo, hi):
        a = np.full((16, 16), NODATA)
        a[lo:hi, lo:hi] = 1.0
        return _gtiff([a], nodata=NODATA)

    plot_mask_layers(
        [("big", mask(2, 14)), ("small", mask(5, 11))],
        colors=["#1f77b4", "#ff7f0e"],
        fig_w=5,
        fig_h=5,
    )
    ax = plt.gcf().axes[0]
    assert len(ax.get_images()) == 2
    assert len(ax.get_legend().get_texts()) == 2
    buf = _rgb(plt.gcf())

    def count_near(rgb):
        return int((np.abs(buf - np.array(rgb)).sum(axis=2) < 40).sum())

    assert count_near([31, 119, 180]) > 100 and count_near([255, 127, 14]) > 100


def test_a_constant_value_presence_mask_draws_its_footprint_not_a_blank_plot():
    # rasterio.plot.show() rendered a constant band blank (it ignored vmin/vmax); the
    # single-band branch uses imshow with an explicit, non-degenerate clim.
    data = np.full((8, 8), NODATA)
    data[2:6, 2:6] = 1.0
    plot_raster(_gtiff([data], nodata=NODATA))
    vmin, vmax = _first_image().get_clim()
    assert vmin < vmax
    assert _coloured(_rgb(plt.gcf())) > 200


def test_shared_stretch_keeps_inter_band_colour_and_perband_collapses_it():
    gtiff = _correlated_gtiff()
    plot_raster(gtiff, stretch="shared", fig_w=6, fig_h=6)
    shared = _coloured(_rgb(plt.gcf()))
    plt.close("all")
    plot_raster(gtiff, stretch="perband", fig_w=6, fig_h=6)
    perband = _coloured(_rgb(plt.gcf()))
    assert shared > 500 and perband < 200


def test_multiband_renders_are_coloured_not_blank_white():
    # float reflectance (max >> 1) is scaled for display; uint8 RGB keeps its red and green
    # blocks; a fixed (0, 255) stretch applies even to uint8; bands=(1,2,3) picks the composite.
    ramp = np.linspace(200, 8000, 256).reshape(16, 16)
    plot_raster(_gtiff([ramp, ramp * 0.75, ramp * 0.5]), stretch="shared")
    assert _coloured(_rgb(plt.gcf()), 20) > 100
    plt.close("all")

    red_green = [np.zeros((16, 16)) for _ in range(3)]
    for band, (red, green) in zip(red_green, [(200, 40), (40, 180), (40, 40)]):
        band[:, :8], band[:, 8:] = red, green
    plot_raster(_gtiff(red_green, dtype="uint8"), bands=(1, 2, 3), fig_w=6, fig_h=6)
    r, g, b = (c for c in np.moveaxis(_rgb(plt.gcf()), -1, 0))
    assert int(((r > g + 40) & (r > b + 40)).sum()) > 50
    assert int(((g > r + 40) & (g > b + 40)).sum()) > 50
    plt.close("all")

    ramp8 = np.arange(64).reshape(8, 8)
    plot_raster(
        _gtiff([ramp8, ramp8 // 2, np.full((8, 8), 50)], dtype="uint8"),
        stretch=(0, 255),
        fig_w=6,
        fig_h=6,
    )
    assert _coloured(_rgb(plt.gcf()), 20) > 20


def test_fill_pixels_render_as_background_and_the_data_region_still_draws():
    b1 = np.linspace(100, 1000, 256).reshape(16, 16)
    b2 = np.linspace(50, 500, 256).reshape(16, 16)
    b3 = np.linspace(200, 2000, 256).reshape(16, 16)
    for band in (b1, b2, b3):
        band[:4, :4] = 0  # the fill corner
    plot_raster(_gtiff([b1, b2, b3]), fill=0, fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba())
    corner = rgba[:4, :4, :].astype(int)
    assert int((np.abs(corner[..., 0] - corner[..., 1]) < 20).sum()) > 8
    assert int((np.abs(rgba[4:, 4:, :3].astype(int)[..., 0] - 245) > 40).sum()) > 20


def test_unsupported_band_requests_raise_value_error():
    two_band = _gtiff([np.arange(64).reshape(8, 8)] * 2, dtype="uint8")
    three_band = _correlated_gtiff()
    with pytest.raises(ValueError, match="RGB.*>=3 bands"):
        plot_raster(two_band, fill=0)
    with pytest.raises(ValueError, match="depth.*does not support"):
        plot_raster(_depth_gtiff(), composite="depth", bands=(1, 2))
    for bands, match in [
        ((1, 2), "2 bands"),
        ((4,), "band index"),
        ((0,), "band index"),
    ]:
        with pytest.raises(ValueError, match=match):
            plot_raster(three_band, bands=bands)
    with pytest.raises(ValueError, match="hi.*lo"):
        plot_raster(three_band, stretch=(100, 50))


@pytest.mark.parametrize(
    "window, shape",
    [((4, 4, 4, 4), (4, 4)), ((12, 12, 4, 4), (4, 4)), (None, (16, 16))],
)
def test_virtual_tile_renders_its_window_and_matches_a_direct_read(
    tmp_path, window, shape
):
    path = _disk_gtiff(tmp_path, width=16, height=16)
    plot_raster(_virtual_tile(path, window))
    rendered = np.asarray(_first_image().get_array())
    assert rendered.shape == shape
    if window:
        win = rasterio.windows.Window(*window)
        with rasterio.open(path) as src:
            expected, _, _ = _raster._read_windowed(src, 2000, window=win)
        # plot_raster percentile-stretches float data outside [0, 1] before imshow.
        stretched = np.asarray(_raster._percentile_stretch(expected)[0])
        assert np.allclose(rendered, stretched, atol=0.01)


def test_a_materialized_tile_struct_renders_as_before():
    tile = _virtual_tile(None, None)
    tile.update(raster=make_geotiff_bytes(width=8, height=8, count=1), path_mode=None)
    plot_raster(tile)
    assert len(plt.get_fignums()) == 1


def test_cmap_is_honoured_on_every_input_form_and_defaults_to_viridis(tmp_path):
    gt = make_geotiff_bytes(width=8, height=8, count=1)
    plot_raster(gt)
    assert _first_image().get_cmap().name == "viridis"
    plt.close("all")
    p = tmp_path / "cmap.tif"
    p.write_bytes(gt)
    for draw in (
        lambda: plot_raster(gt, cmap="terrain"),
        lambda: plot_file(str(p), cmap="terrain"),
        lambda: plot_raster(_virtual_tile(_disk_gtiff(tmp_path)), cmap="terrain"),
    ):
        draw()
        assert _first_image().get_cmap().name == "terrain"
        plt.close("all")
