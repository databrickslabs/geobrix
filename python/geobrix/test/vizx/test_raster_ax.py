"""TDD test for plot_file / plot_raster ax= + title= (multi-panel composition).

Mirrors test_cog_ax.py: with a caller-supplied Axes, the function draws onto that
Axes (not a new figure) and returns it — so before/after and N-up comparisons can
be composed with a single figure.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def _rgb_tif(path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    data = np.random.default_rng(0).integers(1, 256, size=(3, 16, 16), dtype="uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=16,
        width=16,
        count=3,
        dtype="uint8",
        nodata=0,
        crs="EPSG:3857",
        transform=from_origin(0, 16, 1, 1),
    ) as ds:
        ds.write(data)
    return path


def test_plot_file_draws_on_provided_axes(tmp_path):
    from databricks.labs.gbx.vizx._raster import plot_file

    p = _rgb_tif(str(tmp_path / "rgb.tif"))
    fig, ax = plt.subplots()
    n_before = len(ax.images)
    out = plot_file(p, ax=ax, title="Before")
    assert out is ax  # returned the same Axes
    assert len(ax.images) > n_before  # drew onto it (no new figure)
    assert ax.get_title() == "Before"  # honored the title override
    plt.close(fig)


def test_plot_raster_draws_on_provided_axes(tmp_path):
    import rasterio

    from databricks.labs.gbx.vizx._raster import plot_raster

    p = _rgb_tif(str(tmp_path / "rgb2.tif"))
    with open(p, "rb") as f:
        raster_bytes = f.read()
    # sanity: the file is readable as a 3-band raster
    with rasterio.open(p) as ds:
        assert ds.count == 3

    fig, ax = plt.subplots()
    n_before = len(ax.images)
    out = plot_raster(raster_bytes, ax=ax, title="Panel")
    assert out is ax
    assert len(ax.images) > n_before
    assert ax.get_title() == "Panel"
    plt.close(fig)


def test_plot_file_default_still_creates_own_figure(tmp_path):
    # Back-compat: no ax → returns an Axes from a freshly created figure.
    from databricks.labs.gbx.vizx._raster import plot_file

    p = _rgb_tif(str(tmp_path / "rgb3.tif"))
    out = plot_file(p)
    assert out is not None  # returns the Axes it created
    plt.close("all")
