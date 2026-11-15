"""Regression test: COG raster renders ABOVE contextily basemap in plot_cog.

Before the fix, cx.add_basemap() was called after ax.imshow() with no zorder
specified on either — contextily defaulted to zorder=0, but matplotlib's
AxesImage default (zorder=1) meant the basemap landed ON TOP of the raster
for opaque full-extent COGs (e.g. a DEM showed only the street map).

Fix: raster zorder=2, basemap zorder=1.  This test proves the invariant.
"""

from test._volume_safe import open_raster

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _write_single_band_tif(tmp_path, size=16, crs="EPSG:3857"):
    """Write a tiny float32 single-band GeoTIFF; return its path as str."""
    from rasterio.transform import from_bounds

    path = tmp_path / "dem.tif"
    data = np.linspace(0, 1000, size * size, dtype="float32").reshape(size, size)
    transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
    with open_raster(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return str(path)


def test_cog_raster_above_basemap_single_band(tmp_path, monkeypatch):
    """Raster AxesImage must have zorder > basemap zorder (raster on top)."""
    import contextily as cx

    from databricks.labs.gbx.vizx._cog import plot_cog

    basemap_calls: list[dict] = []

    def _spy_add_basemap(ax, **kwargs):
        basemap_calls.append(kwargs)
        # no tile fetch; add a silent Patch so the axes has a non-image artist
        import matplotlib.patches as mpatches

        ax.add_patch(
            mpatches.Rectangle(
                (0, 0), 1, 1, transform=ax.transAxes, zorder=kwargs.get("zorder", 0)
            )
        )

    monkeypatch.setattr(cx, "add_basemap", _spy_add_basemap)

    plt.close("all")
    path = _write_single_band_tif(tmp_path)
    ax = plot_cog(path, basemap=True)

    # (a) basemap spy was called with zorder=1
    assert basemap_calls, "cx.add_basemap was never called"
    assert (
        basemap_calls[0].get("zorder") == 1
    ), f"Expected basemap zorder=1, got {basemap_calls[0].get('zorder')!r}"

    # (b) at least one AxesImage (the COG) has zorder strictly above basemap zorder
    images = ax.get_images()
    assert images, "No AxesImage found on axes — COG was not drawn"
    raster_zorders = [img.get_zorder() for img in images]
    basemap_zorder = basemap_calls[0].get("zorder")
    assert all(
        z > basemap_zorder for z in raster_zorders
    ), f"COG raster zorder(s) {raster_zorders} must all be > basemap zorder {basemap_zorder}"
    assert all(
        z == 2 for z in raster_zorders
    ), f"Expected COG raster zorder=2, got {raster_zorders}"

    plt.close("all")
