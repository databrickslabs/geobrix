"""Offline tests for plot_tile (in-memory tile draped over a contextily basemap).

basemap=False keeps these offline (no tile egress); the basemap path itself
degrades gracefully and is covered by the plot_cog tests.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _tile_bytes(size=40, crs="EPSG:4326"):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    data = (np.random.rand(size, size) * 100).astype("float32")
    data[16:22, 16:22] = 5000.0  # a hot blob in the middle
    transform = from_bounds(-103.90, 31.60, -103.80, 31.70, size, size)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            height=size,
            width=size,
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(data, 1)
        return mf.read()


def test_plot_tile_renders_bytes_with_colorbar():
    from databricks.labs.gbx.vizx import plot_tile

    plt.close("all")
    ax = plot_tile(_tile_bytes(), basemap=False, colorbar_label="enhancement (ppm·m)")
    assert ax is not None
    assert len(plt.get_fignums()) >= 1
    assert len(ax.images) == 1  # the raster layer was drawn
    plt.close("all")


def test_plot_tile_accepts_tile_struct_dict():
    from databricks.labs.gbx.vizx import plot_tile

    plt.close("all")
    ax = plot_tile({"raster": _tile_bytes()}, basemap=False)
    assert ax is not None and len(ax.images) == 1
    plt.close("all")


def test_plot_tile_absolute_mask_hides_background():
    from databricks.labs.gbx.vizx import plot_tile

    plt.close("all")
    # mask below 1000 -> only the 5000 blob survives; the drawn array is masked
    ax = plot_tile(_tile_bytes(), basemap=False, mask_below=1000.0)
    arr = ax.images[0].get_array()
    assert np.ma.isMaskedArray(arr)
    assert arr.mask.any()  # the ~100 background is masked out
    assert not arr.mask.all()  # the blob survives
    plt.close("all")


def test_plot_tile_window_beyond_raster_is_boundless():
    from databricks.labs.gbx.vizx import plot_tile

    plt.close("all")
    # window extends beyond the tile's extent on all sides: a boundless read pads the
    # out-of-raster area (masked) so the drawn array spans the full requested window
    # rather than collapsing into a corner (the plume-near-scene-edge bug).
    ax = plot_tile(
        _tile_bytes(size=40), basemap=False,
        window_bounds=(-103.95, 31.55, -103.75, 31.75),
    )
    arr = ax.images[0].get_array()
    assert np.ma.isMaskedArray(arr) and arr.mask.any()  # padded edges are masked
    plt.close("all")


def test_plot_tile_window_bounds_crops():
    from databricks.labs.gbx.vizx import plot_tile

    plt.close("all")
    full = plot_tile(_tile_bytes(), basemap=False)
    full_shape = full.images[0].get_array().shape
    plt.close("all")
    win = plot_tile(
        _tile_bytes(),
        basemap=False,
        window_bounds=(-103.87, 31.63, -103.83, 31.67),
    )
    win_shape = win.images[0].get_array().shape
    assert win_shape[0] < full_shape[0] and win_shape[1] < full_shape[1]
    plt.close("all")
