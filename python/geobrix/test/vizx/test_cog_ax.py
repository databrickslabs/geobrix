"""TDD test for plot_cog ax= parameter (static multi-layer composition)."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def test_plot_cog_draws_on_provided_axes(tmp_path):
    # build a tiny 1-band GeoTIFF
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.vizx._cog import plot_cog

    p = tmp_path / "x.tif"
    data = np.arange(64, dtype="float32").reshape(8, 8)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=from_origin(0, 8, 1, 1),
    ) as ds:
        ds.write(data, 1)
    fig, ax = plt.subplots()
    n_before = len(ax.images)
    out = plot_cog(str(p), basemap=False, ax=ax)
    assert out is ax
    assert len(ax.images) > n_before  # drew onto the SAME axes
    plt.close(fig)
