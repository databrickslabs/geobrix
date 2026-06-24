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

from databricks.labs.gbx.vizx import _raster, plot_file, plot_raster  # noqa: E402

NODATA = -9999.0


def _make_depth_test_gtiff():
    """3-band 4×4 GTiff for coverage-depth tests.

    Band 1 covers all 16 pixels (value 1.0).
    Band 2 covers the left half (8 pixels, columns 0-1) — nodata on right.
    Band 3 covers the top-left quadrant (4 pixels, rows 0-1, cols 0-1) — nodata elsewhere.

    Expected depth per pixel:
      top-left 2×2     → 3  (covered by all three bands)
      top-right 2×2    → 1  (only band 1)
      bottom-left 2×2  → 2  (bands 1 and 2)
      bottom-right 2×2 → 1  (only band 1)
    """
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    shape = (4, 4)
    b1 = np.ones(shape, dtype="float32")
    b2 = np.full(shape, NODATA, dtype="float32")
    b2[:, :2] = 1.0  # left half covered
    b3 = np.full(shape, NODATA, dtype="float32")
    b3[:2, :2] = 1.0  # top-left quadrant covered
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=3,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=NODATA,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    return buf.getvalue()


def test_needs_stretch_true_for_uint16_over_255():
    data = np.array([[0, 300], [1000, 65535]], dtype="uint16")
    assert _raster._needs_percentile_stretch(data) is True


def test_needs_stretch_false_for_float_and_small_int():
    assert (
        _raster._needs_percentile_stretch(np.array([[0.1, 0.9]], dtype="float32"))
        is False
    )
    assert (
        _raster._needs_percentile_stretch(np.array([[0, 200]], dtype="uint8")) is False
    )


def test_percentile_stretch_scales_to_unit_range_ignoring_mask():
    band = np.arange(100, dtype="uint16").reshape(1, 10, 10) * 10  # 0..9900
    masked = np.ma.MaskedArray(band, mask=np.zeros_like(band, dtype=bool))
    masked.mask[0, 0, 0] = True  # exclude an outlier-free pixel
    out = _raster._percentile_stretch(masked)
    assert out.dtype == np.float32
    assert float(out.min()) >= 0.0 and float(out.max()) <= 1.0
    assert isinstance(out, np.ma.MaskedArray)
    assert out.mask[0, 0, 0]  # mask preserved


def test_plot_raster_produces_a_figure():
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=8, height=8, count=1))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_file_produces_a_figure(tmp_path):
    p = tmp_path / "t.tif"
    p.write_bytes(make_geotiff_bytes(width=8, height=8, count=3))
    plt.close("all")
    plot_file(str(p))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


@pytest.mark.parametrize("scheme", ["dbfs:", "file:", "file://"])
def test_plot_file_strips_uri_scheme(tmp_path, scheme):
    # Databricks paths are often scheme-qualified (dbfs:/..., file:///...);
    # plot_file should read the FUSE-mount path by stripping the scheme rather
    # than failing in rasterio.
    p = tmp_path / "t.tif"
    p.write_bytes(make_geotiff_bytes(width=8, height=8, count=1))
    plt.close("all")
    plot_file(f"{scheme}{p}")
    assert len(plt.get_fignums()) == 1
    plt.close("all")


# ---------------------------------------------------------------------------
# coverage_depth helper — unit tests
# ---------------------------------------------------------------------------


def test_coverage_depth_masked_array():
    """_coverage_depth counts unmasked bands per pixel from a MaskedArray."""
    # 3 bands, 2×2 pixels
    data = np.ones((3, 2, 2), dtype="float32")
    mask = np.zeros((3, 2, 2), dtype=bool)
    # band 0 fully covered; band 1 covers only pixel [0,0]; band 2 covers nothing
    mask[1, 0, 1] = True
    mask[1, 1, 0] = True
    mask[1, 1, 1] = True
    mask[2, :, :] = True
    ma = np.ma.MaskedArray(data, mask=mask)

    depth = _raster._coverage_depth(ma, nodata=NODATA)

    assert depth.shape == (2, 2)
    assert depth[0, 0] == 2.0  # bands 0 + 1
    assert depth[0, 1] == 1.0  # band 0 only
    assert depth[1, 0] == 1.0  # band 0 only
    assert depth[1, 1] == 1.0  # band 0 only


def test_coverage_depth_plain_array_with_nodata():
    """_coverage_depth falls back to nodata sentinel comparison for plain arrays."""
    nd = NODATA
    # 3 bands, 2×2
    b1 = np.ones((2, 2), dtype="float32")
    b2 = np.array([[1.0, nd], [nd, nd]], dtype="float32")
    b3 = np.full((2, 2), nd, dtype="float32")
    data = np.stack([b1, b2, b3])  # plain ndarray, not masked

    depth = _raster._coverage_depth(data, nodata=nd)

    assert depth[0, 0] == 2.0  # bands 0 + 1
    assert depth[0, 1] == 1.0  # band 0 only
    assert depth[1, 0] == 1.0  # band 0 only
    assert depth[1, 1] == 1.0  # band 0 only


def test_coverage_depth_known_geometry():
    """Verify depth values for the structured 3-band GTiff fixture."""
    gtiff_bytes = _make_depth_test_gtiff()
    with MemoryFile(gtiff_bytes) as mf:
        with mf.open() as src:
            data = src.read(masked=True)
            nd = src.nodata

    depth = _raster._coverage_depth(data, nodata=nd)

    # top-left 2×2: covered by all 3 bands
    assert np.all(depth[:2, :2] == 3.0), f"top-left expected 3, got {depth[:2, :2]}"
    # top-right 2×2: only band 1 covers
    assert np.all(depth[:2, 2:] == 1.0), f"top-right expected 1, got {depth[:2, 2:]}"
    # bottom-left 2×2: bands 1 and 2 cover
    assert np.all(depth[2:, :2] == 2.0), f"bottom-left expected 2, got {depth[2:, :2]}"
    # bottom-right 2×2: only band 1
    assert np.all(depth[2:, 2:] == 1.0), f"bottom-right expected 1, got {depth[2:, 2:]}"


# ---------------------------------------------------------------------------
# composite="depth" integration tests
# ---------------------------------------------------------------------------


def test_plot_raster_composite_depth_produces_figure():
    """plot_raster(composite='depth') renders a figure for a 3-band GTiff."""
    gtiff_bytes = _make_depth_test_gtiff()
    plt.close("all")
    plot_raster(gtiff_bytes, composite="depth", fig_w=6, fig_h=6)
    assert len(plt.get_fignums()) == 1, "Expected exactly one figure"
    plt.close("all")


def test_plot_raster_composite_auto_unchanged_for_single_band():
    """composite='auto' (default) still works for a single-band raster."""
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=8, height=8, count=1))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


# ---------------------------------------------------------------------------
# _single_band_clim unit tests
# ---------------------------------------------------------------------------


def test_single_band_clim_constant_ones():
    """Constant array of 1.0 -> (0.0, 1.0) non-degenerate range."""
    result = _raster._single_band_clim(np.array([1.0, 1.0, 1.0]))
    assert result == (0.0, 1.0)


def test_single_band_clim_varying():
    """Varying data -> None (let matplotlib auto-scale)."""
    result = _raster._single_band_clim(np.array([10.0, 50.0]))
    assert result is None


def test_single_band_clim_empty():
    """Empty array -> None."""
    result = _raster._single_band_clim(np.array([]))
    assert result is None


def test_single_band_clim_constant_zeros():
    """Constant zeros -> (0.0, 1.0)."""
    result = _raster._single_band_clim(np.array([0.0, 0.0]))
    assert result == (0.0, 1.0)


# ---------------------------------------------------------------------------
# Presence mask render test
# ---------------------------------------------------------------------------


def _make_presence_mask_gtiff():
    """Single-band 8x8 GTiff: 1.0 in center 4x4, NoData=-9999 on border."""
    transform = from_origin(0.0, 8.0, 1.0, 1.0)
    data = np.full((8, 8), NODATA, dtype="float32")
    data[2:6, 2:6] = 1.0
    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=NODATA,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(data, 1)
    return buf.getvalue()


def test_plot_mask_layers_overlays_distinct_colours_with_legend():
    """plot_mask_layers draws each mask as its own colour on one axes, with a legend.

    Asserts real drawn content: two AxesImages, a 2-entry legend, and both requested
    colours present in the rasterized buffer (not a single blended/blank layer).
    """
    from databricks.labs.gbx.vizx import plot_mask_layers

    # Two nested masks on the SAME 16x16 grid: big (12x12) and small (6x6).
    transform = from_origin(0.0, 16.0, 1.0, 1.0)
    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=NODATA,
    )

    def mask(lo, hi):
        a = np.full((16, 16), NODATA, dtype="float32")
        a[lo:hi, lo:hi] = 1.0
        b = io.BytesIO()
        with rasterio.open(b, "w", **profile) as ds:
            ds.write(a, 1)
        return b.getvalue()

    plt.close("all")
    plot_mask_layers(
        [("big", mask(2, 14)), ("small", mask(5, 11))],
        colors=["#1f77b4", "#ff7f0e"],  # blue, orange
        fig_w=5,
        fig_h=5,
    )
    fig = plt.gcf()
    ax = fig.axes[0]
    assert len(ax.get_images()) == 2, "expected two overlaid layers"
    legend = ax.get_legend()
    assert (
        legend is not None and len(legend.get_texts()) == 2
    ), "expected a 2-entry legend"

    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    def count_near(rgb):
        d = np.abs(buf - np.array(rgb)).sum(axis=2)
        return int((d < 40).sum())

    assert count_near([31, 119, 180]) > 100, "blue layer not drawn"
    assert count_near([255, 127, 14]) > 100, "orange layer not drawn"
    plt.close("all")


def test_plot_raster_presence_mask_actually_draws_footprint():
    """A constant-value presence mask must DRAW its footprint, not render blank.

    rasterio.plot.show() rendered a constant single band as an empty plot (it
    ignored the explicit vmin/vmax), so the single-band branch uses ax.imshow, which
    honors both the clim and the masked array. This asserts real drawn pixels — the
    earlier check (a Figure object merely exists) passed while the plot was blank.
    """
    plt.close("all")
    plot_raster(_make_presence_mask_gtiff())
    fig = plt.gcf()
    ax = fig.axes[0]

    # An AxesImage with a non-degenerate colour range must exist (vmin==vmax blanks).
    images = ax.get_images()
    assert images, "no image drawn on the axes"
    vmin, vmax = images[0].get_clim()
    assert vmin < vmax, f"degenerate colour range ({vmin}, {vmax}) -> blank render"

    # And the rasterized figure must contain coloured (non-grey) pixels: the viridis
    # footprint, as opposed to the grey/white background. A blank render has none.
    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)
    r, g, b = buf[..., 0], buf[..., 1], buf[..., 2]
    coloured = (np.abs(r - g) > 25) | (np.abs(g - b) > 25) | (np.abs(r - b) > 25)
    assert coloured.sum() > 200, (
        f"presence-mask footprint not drawn (only {int(coloured.sum())} coloured "
        "px) -- render is effectively blank"
    )
    plt.close("all")
