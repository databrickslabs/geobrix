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


# ---------------------------------------------------------------------------
# _select_bands unit tests
# ---------------------------------------------------------------------------


def test_select_bands_subsets_and_reorders():
    """_select_bands selects 1-based band indices and reorders them."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)  # 3 bands, 2x2 each
    result = _raster._select_bands(data, bands=(3, 1, 2))
    assert result.shape == (3, 2, 2)
    assert np.allclose(result[0], data[2])  # band 3 (index 2) first
    assert np.allclose(result[1], data[0])  # band 1 (index 0) second
    assert np.allclose(result[2], data[1])  # band 2 (index 1) third


def test_select_bands_single_band():
    """_select_bands works with a single-band selection."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    result = _raster._select_bands(data, bands=(2,))
    assert result.shape == (1, 2, 2)
    assert np.allclose(result[0], data[1])


def test_select_bands_none_returns_all():
    """_select_bands with None returns data unchanged."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    result = _raster._select_bands(data, bands=None)
    assert result is data or np.allclose(result, data)


def test_select_bands_out_of_range_raises():
    """_select_bands raises ValueError for out-of-range indices."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    with pytest.raises(ValueError, match="band index"):
        _raster._select_bands(data, bands=(4,))
    with pytest.raises(ValueError, match="band index"):
        _raster._select_bands(data, bands=(0,))


def test_select_bands_length_2_raises():
    """_select_bands raises ValueError for length-2 selection (ambiguous)."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    with pytest.raises(ValueError, match="2 bands"):
        _raster._select_bands(data, bands=(1, 2))


# ---------------------------------------------------------------------------
# _apply_fill_mask unit tests
# ---------------------------------------------------------------------------


def test_apply_fill_mask_masks_equal_values():
    """_apply_fill_mask masks pixels equal to fill value."""
    data = np.array([[[1, 2], [0, 3]]], dtype="float32")
    result = _raster._apply_fill_mask(data, fill=0)
    assert isinstance(result, np.ma.MaskedArray)
    assert result.mask[0, 1, 0]  # pixel with value 0 is masked
    assert not result.mask[0, 0, 0]  # pixel with value 1 is not masked


def test_apply_fill_mask_none_returns_unchanged():
    """_apply_fill_mask with fill=None returns data unchanged."""
    data = np.array([[[1, 2], [0, 3]]], dtype="float32")
    result = _raster._apply_fill_mask(data, fill=None)
    assert result is data or np.allclose(result, data)


def test_apply_fill_mask_combines_with_existing_mask():
    """_apply_fill_mask combines with an existing MaskedArray mask."""
    data = np.array([[[1, 2], [0, 3]]], dtype="float32")
    masked = np.ma.MaskedArray(data, mask=np.array([[False, True], [False, False]]))
    result = _raster._apply_fill_mask(masked, fill=0)
    assert isinstance(result, np.ma.MaskedArray)
    assert result.mask[0, 0, 1]  # already masked
    assert result.mask[0, 1, 0]  # masked by fill


# ---------------------------------------------------------------------------
# _stretch_shared unit tests
# ---------------------------------------------------------------------------


def test_stretch_shared_dtype_and_range():
    """_stretch_shared output is float32 in [0,1]."""
    data = np.arange(3 * 10 * 10, dtype="uint16").reshape(3, 10, 10) * 50  # 0..max>255
    result = _raster._stretch_shared(data)
    assert result.dtype == np.float32
    assert float(result.min()) >= 0.0 and float(result.max()) <= 1.0


def test_stretch_shared_preserves_inter_band_offset():
    """_stretch_shared preserves inter-band color relationships (correlated bands).

    Create a 3-band raster where bands are [base, base*0.8, base*0.6] — they're
    correlated (same spatial variation) but different scales. After shared stretch,
    the per-pixel channel offsets should remain much larger than after per-band
    stretch (which normalizes each band independently, collapsing offsets to ~0).
    """
    # Create a base with spatial variation (ramps, some random)
    np.random.seed(42)
    base = (
        np.linspace(100, 30000, 100).reshape(10, 10) + np.random.rand(10, 10) * 1000
    ).astype("uint16")
    b1 = base
    b2 = (base * 0.8).astype("uint16")
    b3 = (base * 0.6).astype("uint16")
    data = np.stack([b1, b2, b3])

    # Shared stretch
    shared = _raster._stretch_shared(data)
    # Per-band stretch (existing)
    perband = _raster._percentile_stretch(data)

    # Compute per-pixel channel spread (max - min across 3 bands)
    shared_spread = shared.max(axis=0) - shared.min(axis=0)
    perband_spread = perband.max(axis=0) - perband.min(axis=0)

    # shared stretch should preserve offsets; perband should collapse them
    assert (
        float(shared_spread.mean()) > 0.05
    ), "shared stretch should preserve inter-band variation"
    assert (
        float(perband_spread.mean()) < 0.01
    ), "perband stretch should collapse correlated bands"


def test_stretch_shared_preserves_mask():
    """_stretch_shared preserves MaskedArray mask."""
    data = np.arange(12, dtype="uint16").reshape(3, 2, 2) * 100
    masked = np.ma.MaskedArray(data, mask=np.zeros_like(data, dtype=bool))
    masked.mask[0, 0, 0] = True
    result = _raster._stretch_shared(masked)
    assert isinstance(result, np.ma.MaskedArray)
    assert result.mask[0, 0, 0]


def test_stretch_shared_excludes_fill_from_percentile():
    """_stretch_shared with fill mask excludes fill pixels from percentile computation."""
    # Create data with a fill corner and varied data outside
    data = np.arange(3 * 4 * 4, dtype="uint16").reshape(3, 4, 4) * 100 + 1000
    data[:, :2, :2] = 0  # fill corner (0 is the fill value)
    masked = _raster._apply_fill_mask(data, fill=0)

    result = _raster._stretch_shared(masked)
    # The stretch should be based on the non-zero values, so max should be ~1.0
    max_val = float(result.max())
    assert max_val > 0.95, f"max value should be near 1.0, got {max_val}"
    # Fill pixels should be masked
    assert isinstance(result, np.ma.MaskedArray), "result should be MaskedArray"
    assert result.mask[0, 0, 0], "fill pixels should be masked"


# ---------------------------------------------------------------------------
# _stretch_fixed unit tests
# ---------------------------------------------------------------------------


def test_stretch_fixed_value_mapping():
    """_stretch_fixed maps (x - lo) / (hi - lo) clipped to [0,1]."""
    data = np.array([[[0, 500, 1000, 2000]]], dtype="float32")
    result = _raster._stretch_fixed(data, lo=0, hi=1000)
    expected = np.array([[[0.0, 0.5, 1.0, 1.0]]], dtype="float32")  # 2000 clips to 1.0
    assert np.allclose(result, expected)


def test_stretch_fixed_hi_le_lo_raises():
    """_stretch_fixed raises ValueError when hi <= lo."""
    data = np.array([[[100]]], dtype="float32")
    with pytest.raises(ValueError, match="hi.*lo"):
        _raster._stretch_fixed(data, lo=100, hi=100)
    with pytest.raises(ValueError, match="hi.*lo"):
        _raster._stretch_fixed(data, lo=100, hi=50)


def test_stretch_fixed_preserves_mask():
    """_stretch_fixed preserves MaskedArray mask."""
    data = np.array([[[100, 500]]], dtype="float32")
    masked = np.ma.MaskedArray(data, mask=np.array([[[True, False]]]))
    result = _raster._stretch_fixed(masked, lo=0, hi=1000)
    assert isinstance(result, np.ma.MaskedArray)
    assert result.mask[0, 0, 0]


# ---------------------------------------------------------------------------
# _compose_rgba unit tests
# ---------------------------------------------------------------------------


def test_compose_rgba_shape():
    """_compose_rgba converts (3,H,W) to (H,W,4)."""
    rgb = np.random.rand(3, 10, 8).astype("float32")
    result = _raster._compose_rgba(rgb, base_alpha=0.8)
    assert result.shape == (10, 8, 4)


def test_compose_rgba_masked_pixel_transparent():
    """_compose_rgba sets alpha=0 for pixels masked in any channel."""
    rgb = np.ones((3, 2, 2), dtype="float32") * 0.5
    mask = np.zeros((3, 2, 2), dtype=bool)
    mask[0, 0, 0] = True  # mask first channel at pixel (0,0)
    masked_rgb = np.ma.MaskedArray(rgb, mask=mask)

    result = _raster._compose_rgba(masked_rgb, base_alpha=0.8)
    assert result[0, 0, 3] == 0.0, "pixel masked in channel 0 should have alpha=0"
    assert result[0, 1, 3] == 0.8, "unmasked pixel should have base_alpha"
    assert result[1, 1, 3] == 0.8


def test_compose_rgba_rgb_clipped_0_1():
    """_compose_rgba clips RGB channels to [0,1]."""
    rgb = np.array([[[2.0, -0.5]]], dtype="float32").reshape(1, 1, 2)
    # Reshape to 3 bands: add 2 more bands (can be zeros)
    rgb = np.vstack([rgb, np.zeros((2, 1, 2), dtype="float32")])

    result = _raster._compose_rgba(rgb, base_alpha=1.0)
    assert result[0, 0, 0] == 1.0, "R should clip to 1.0"
    assert result[0, 0, 1] == 0.0, "G should clip to 0.0"


# ---------------------------------------------------------------------------
# Integration tests: plot_raster with new kwargs
# ---------------------------------------------------------------------------


def _make_correlated_3band_gtiff():
    """Create a 3-band 16×16 uint16 GTiff with correlated bands [base, 0.8*base, 0.6*base].

    Each band has the same spatial variation (ramp) but different amplitudes, so
    shared stretch preserves inter-band offsets while perband normalizes them away.
    """
    transform = from_origin(0.0, 16.0, 1.0, 1.0)
    # Create a base ramp (spatial variation, max > 255 to trigger stretch)
    base = np.linspace(1000, 30000, 256).reshape(16, 16).astype("uint16")
    b1 = base
    b2 = (base * 0.8).astype("uint16")
    b3 = (base * 0.6).astype("uint16")

    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=3,
        dtype="uint16",
        crs="EPSG:4326",
        transform=transform,
        nodata=65535,  # Valid uint16 nodata value
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    return buf.getvalue()


def test_plot_raster_bands_selection_renders():
    """plot_raster with bands=(1,2,3) renders an RGB composite."""
    plt.close("all")
    gtiff_bytes = _make_correlated_3band_gtiff()
    plot_raster(gtiff_bytes, bands=(1, 2, 3))
    assert len(plt.get_fignums()) == 1, "Expected exactly one figure"
    plt.close("all")


def test_plot_raster_stretch_shared_preserves_color_offset():
    """stretch='shared' on correlated 3-band tile produces MANY colored pixels.

    A correlated 3-band raster (base, 0.8*base, 0.6*base) has inherent inter-channel
    variation. Under 'shared' stretch, this variation is preserved in the rendered RGB.
    We verify by counting pixels with |r-g| > 25 or similar thresholds — these should
    be abundant in a colorful render and sparse in a desaturated render.
    """
    plt.close("all")
    gtiff_bytes = _make_correlated_3band_gtiff()
    plot_raster(gtiff_bytes, stretch="shared", fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    # Count pixels with significant color spread (|r-g|, |g-b|, |r-b| > 25)
    r, g, b = buf[..., 0], buf[..., 1], buf[..., 2]
    colored = (np.abs(r - g) > 25) | (np.abs(g - b) > 25) | (np.abs(r - b) > 25)
    colored_count = int(colored.sum())

    assert colored_count > 500, (
        f"shared stretch should preserve inter-band color (got {colored_count} colored px); "
        "render may be desaturated"
    )
    plt.close("all")


def test_plot_raster_stretch_perband_less_colored():
    """stretch='perband' (default) on the same correlated tile produces FEWER colored pixels.

    Per-band stretch normalizes each band independently, collapsing inter-band
    offsets, so the result appears more desaturated/gray.
    """
    plt.close("all")
    gtiff_bytes = _make_correlated_3band_gtiff()
    plot_raster(gtiff_bytes, stretch="perband", fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    r, g, b = buf[..., 0], buf[..., 1], buf[..., 2]
    colored = (np.abs(r - g) > 25) | (np.abs(g - b) > 25) | (np.abs(r - b) > 25)
    colored_count = int(colored.sum())

    assert colored_count < 200, (
        f"perband stretch should desaturate correlated bands (got {colored_count} colored px); "
        "render may be too colorful"
    )
    plt.close("all")


def test_plot_raster_fill_renders_transparent():
    """plot_raster with fill=0 renders fill pixels as transparent (background).

    Also verifies that the DATA region (non-fill) renders visible content.
    """
    # Create a 3-band tile with a zero-fill corner and uncorrelated data elsewhere
    # (uncorrelated so per-band stretching doesn't collapse to gray)
    transform = from_origin(0.0, 16.0, 1.0, 1.0)
    b1 = np.linspace(100, 1000, 256).reshape(16, 16).astype("float32")
    b2 = np.linspace(50, 500, 256).reshape(16, 16).astype("float32")
    b3 = np.linspace(200, 2000, 256).reshape(16, 16).astype("float32")
    # Set top-left 4×4 corner to 0 (fill)
    b1[:4, :4] = 0
    b2[:4, :4] = 0
    b3[:4, :4] = 0

    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=3,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    gtiff_bytes = buf.getvalue()

    plt.close("all")
    plot_raster(gtiff_bytes, fill=0, fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf_rgba = np.asarray(fig.canvas.buffer_rgba())

    # The top-left corner should be mostly background (whitesmoke or white)
    # Check that pixels there have low saturation or high alpha-transparency
    # Whitesmoke is RGB(245, 245, 245); white is (255, 255, 255)
    corner = buf_rgba[:4, :4, :]  # RGBA
    r, g = corner[..., 0], corner[..., 1]

    # Pixels should be near-white (r≈g≈b) or low-alpha (a near 0)
    gray_mask = np.abs(r.astype(int) - g.astype(int)) < 20
    background_count = int(gray_mask.sum())
    assert background_count > 8, (
        f"fill corner should render as background/transparent, got {background_count} "
        "near-white pixels in corner"
    )

    # The DATA region should have rendered (not all background/white)
    # Just verify it's not entirely whitesmoke/white; most pixels should differ from corners
    data_region = buf_rgba[4:, 4:, :3].astype(int)
    # Count pixels that are NOT near-white (indication that data was rendered)
    white_threshold = (245, 245, 245)
    not_white = np.sum(
        np.abs(data_region[..., 0].astype(int) - white_threshold[0]) > 40
    )
    assert (
        not_white > 20
    ), "data region should render non-white content (not all background)"
    plt.close("all")


def test_plot_raster_default_no_new_kwargs_regression():
    """Default plot_raster() call (no new kwargs) still produces a figure."""
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=16, height=16, count=3))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_raster_float_multiband_not_white():
    """Float 3-band reflectance tile (max>1) with enhanced path is NOT all-white.

    This tests that float tiles are scaled for display (Fix 1: _needs_display_scaling).
    Without this, float max>1 clipped to [0,1] by _compose_rgba would be all-white.
    """
    # Create a float32 3-band tile with reflectance-scale values (max ~8000)
    transform = from_origin(0.0, 16.0, 1.0, 1.0)
    b1 = np.linspace(200, 8000, 256).reshape(16, 16).astype("float32")
    b2 = np.linspace(150, 6000, 256).reshape(16, 16).astype("float32")
    b3 = np.linspace(100, 4000, 256).reshape(16, 16).astype("float32")

    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=3,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    gtiff_bytes = buf.getvalue()

    plt.close("all")
    # Use the enhanced path with stretch="shared" to trigger display scaling
    plot_raster(gtiff_bytes, stretch="shared", fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf_rgb = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    # The render should NOT be blank white; verify colored pixels exist
    r, g, b = buf_rgb[..., 0], buf_rgb[..., 1], buf_rgb[..., 2]
    colored = (np.abs(r - g) > 20) | (np.abs(g - b) > 20) | (np.abs(r - b) > 20)
    colored_count = int(colored.sum())

    assert colored_count > 100, (
        f"float reflectance tile should render colored (not all-white); "
        f"got {colored_count} colored px"
    )
    plt.close("all")


def test_apply_fill_mask_nan():
    """_apply_fill_mask with fill=float('nan') masks NaN pixels."""
    data = np.array([[[1.0, 2.0], [np.nan, 4.0]]], dtype="float32")
    result = _raster._apply_fill_mask(data, fill=float("nan"))
    assert isinstance(result, np.ma.MaskedArray)
    # NaN pixel (0,1,0) should be masked
    assert result.mask[0, 1, 0]
    # Non-NaN pixels should not be masked
    assert not result.mask[0, 0, 0]
    assert not result.mask[0, 0, 1]
    assert not result.mask[0, 1, 1]


def test_plot_raster_two_band_enhanced_raises():
    """A native 2-band raster + enhanced path (fill=0) raises ValueError."""
    transform = from_origin(0.0, 8.0, 1.0, 1.0)
    b1 = np.arange(64, dtype="uint8").reshape(8, 8)
    b2 = np.arange(64, dtype="uint8").reshape(8, 8)

    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=2,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
    gtiff_bytes = buf.getvalue()

    plt.close("all")
    with pytest.raises(ValueError, match="RGB.*>=3 bands"):
        plot_raster(gtiff_bytes, fill=0)
    plt.close("all")


def test_select_bands_empty_raises():
    """_select_bands with empty selection raises ValueError."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    with pytest.raises(ValueError, match="empty|length.*0"):
        _raster._select_bands(data, bands=())


def test_select_bands_scalar_int():
    """_select_bands accepts scalar int to select a single band."""
    data = np.arange(12, dtype="float32").reshape(3, 2, 2)
    result = _raster._select_bands(data, bands=2)
    assert result.shape == (1, 2, 2)
    assert np.allclose(result[0], data[1])  # band 2 (index 1)


def test_compose_rgba_wrong_band_count_raises():
    """_compose_rgba with wrong band count raises ValueError."""
    two_band = np.ones((2, 10, 8), dtype="float32")
    with pytest.raises(ValueError, match="3 RGB bands"):
        _raster._compose_rgba(two_band, base_alpha=1.0)


def test_plot_raster_depth_with_bands_raises():
    """composite='depth' + bands= raises ValueError (incompatible)."""
    gtiff_bytes = _make_depth_test_gtiff()
    plt.close("all")
    with pytest.raises(ValueError, match="depth.*does not support"):
        plot_raster(gtiff_bytes, composite="depth", bands=(1, 2))
    plt.close("all")


def test_plot_raster_uint8_rgb_bands_renders_colors():
    """uint8 3-band tile with bands=(1,2,3) renders colored (not blank white).

    Creates a uint8 tile with a RED block (left half) and GREEN block (right half),
    verifies the rendered canvas contains both reddish and greenish pixels.
    This guards against regression where uint8 RGB data would be clipped to all-white.
    """
    # Create a uint8 3-band tile: left=RED, right=GREEN
    transform = from_origin(0.0, 16.0, 1.0, 1.0)
    b1 = np.zeros((16, 16), dtype="uint8")
    b2 = np.zeros((16, 16), dtype="uint8")
    b3 = np.zeros((16, 16), dtype="uint8")

    # Left half: RED [200, 40, 40]
    b1[:, :8] = 200
    b2[:, :8] = 40
    b3[:, :8] = 40

    # Right half: GREEN [40, 180, 40]
    b1[:, 8:] = 40
    b2[:, 8:] = 180
    b3[:, 8:] = 40

    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    gtiff_bytes = buf.getvalue()

    plt.close("all")
    plot_raster(gtiff_bytes, bands=(1, 2, 3), fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf_rgb = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    r, g, b = buf_rgb[..., 0], buf_rgb[..., 1], buf_rgb[..., 2]

    # Count reddish pixels (left half): r > g and r > b by >40
    reddish = (r > g + 40) & (r > b + 40)
    reddish_count = int(reddish.sum())

    # Count greenish pixels (right half): g > r and g > b by >40
    greenish = (g > r + 40) & (g > b + 40)
    greenish_count = int(greenish.sum())

    assert (
        reddish_count > 50
    ), f"RED block should render with reddish pixels (got {reddish_count})"
    assert (
        greenish_count > 50
    ), f"GREEN block should render with greenish pixels (got {greenish_count})"
    plt.close("all")


def test_plot_raster_fixed_range_uint8_renders():
    """uint8 3-band tile with stretch=(0,255) renders visible (non-blank) content.

    Guards that fixed-range stretch applies unconditionally (not gated by
    _needs_percentile_stretch).
    """
    # Create a uint8 3-band tile with varied content
    transform = from_origin(0.0, 8.0, 1.0, 1.0)
    b1 = np.arange(64, dtype="uint8").reshape(8, 8)
    b2 = np.arange(64, dtype="uint8").reshape(8, 8) // 2
    b3 = np.full((8, 8), 50, dtype="uint8")

    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    )
    buf = io.BytesIO()
    with rasterio.open(buf, "w", **profile) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
        ds.write(b3, 3)
    gtiff_bytes = buf.getvalue()

    plt.close("all")
    plot_raster(gtiff_bytes, stretch=(0, 255), fig_w=6, fig_h=6)
    fig = plt.gcf()
    fig.canvas.draw()
    buf_rgb = np.asarray(fig.canvas.buffer_rgba())[..., :3].astype(int)

    # Ensure the render is not blank white (should have color variation)
    r, g, b = buf_rgb[..., 0], buf_rgb[..., 1], buf_rgb[..., 2]
    # Count non-gray pixels (where channels differ significantly)
    colored = (np.abs(r - g) > 20) | (np.abs(g - b) > 20) | (np.abs(r - b) > 20)
    colored_count = int(colored.sum())

    assert colored_count > 20, (
        f"fixed-range stretch should render visible content "
        f"(got {colored_count} colored px)"
    )
    plt.close("all")
