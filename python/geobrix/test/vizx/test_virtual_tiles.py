"""Every vizx tile plot accepts a VIRTUAL tile (raster NULL, path set), as the readers return.

raster_gbx / gtiff_gbx tiles are virtual in 0.6.0. Each entry point that takes a tile gets
the same tile built from a real small GeoTIFF on disk, as a reader would make it.
"""

from test._volume_safe import open_raster

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile  # noqa: E402
from databricks.labs.gbx.vizx._layers import raster_layer  # noqa: E402

_SIZE = 64


@pytest.fixture
def scene(tmp_path):
    path = tmp_path / "scene.tif"
    with open_raster(
        str(path),
        "w",
        driver="GTiff",
        width=_SIZE,
        height=_SIZE,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-103.0, 31.6, 0.001, 0.001),
    ) as ds:
        ds.write(np.arange(_SIZE * _SIZE, dtype="float32").reshape(_SIZE, _SIZE), 1)
    yield str(path)
    plt.close("all")


def _virtual(path, window=None):
    return {
        "cellid": -1,
        "raster": None,
        "path": path,
        "window": (
            None
            if window is None
            else dict(zip(("col_off", "row_off", "width", "height"), window))
        ),
        "metadata": {"driver": "GTiff"},
    }


def test_plot_tile_accepts_a_virtual_tile(scene):
    from databricks.labs.gbx.vizx import plot_tile

    ax = plot_tile(_virtual(scene), band=1, basemap=False)
    assert ax.get_images()[0].get_array().shape == (_SIZE, _SIZE)


def test_plot_tile_reads_the_window_of_a_virtual_tile(scene):
    from databricks.labs.gbx.vizx import plot_tile

    ax = plot_tile(_virtual(scene, (8, 4, 16, 12)), band=1, basemap=False)
    assert ax.get_images()[0].get_array().shape == (12, 16)


def test_plot_tile_accepts_a_VirtualTile_object(scene):
    from databricks.labs.gbx.vizx import plot_tile

    vt = VirtualTile(cellid=-1, raster=None, path=scene)
    assert plot_tile(vt, band=1, basemap=False) is not None


def test_plot_tile_still_accepts_bytes_and_a_materialized_tile(scene):
    from databricks.labs.gbx.vizx import plot_tile

    raw = open(scene, "rb").read()
    assert plot_tile(raw, band=1, basemap=False) is not None
    assert plot_tile({"cellid": 0, "raster": raw}, band=1, basemap=False) is not None


def test_plot_raster_accepts_a_virtual_tile(scene):
    from databricks.labs.gbx.vizx import plot_raster

    assert plot_raster(_virtual(scene)) is not None


def test_plot_tiles_accepts_a_virtual_tile_in_every_mode(scene):
    from databricks.labs.gbx.vizx import plot_tiles

    class _Df:  # the three modes only need limit(n).collect()
        def limit(self, n):
            return self

        def collect(self):
            return [{"tile": _virtual(scene)}]

    for mode in ("first", "facet", "mosaic"):
        assert plot_tiles(_Df(), mode=mode) is not None


def test_interactive_raster_layer_accepts_a_virtual_tile(scene):
    from databricks.labs.gbx.vizx._maplibre import (
        _raster_to_image,
        layer_to_sources_layers,
    )

    png_virtual, corners = _raster_to_image(raster_layer(_virtual(scene)))
    png_path, corners_path = _raster_to_image(raster_layer(scene))
    assert png_virtual == png_path and corners == corners_path
    sources, layers, _ = layer_to_sources_layers(raster_layer(_virtual(scene)), 0)
    assert layers


def test_plot_interactive_embeds_a_virtual_tile(scene, monkeypatch):
    import databricks.labs.gbx.vizx._interactive as itx
    from databricks.labs.gbx.vizx import plot_interactive

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    monkeypatch.setitem(__import__("sys").modules, "IPython.display", None)
    html = plot_interactive([raster_layer(_virtual(scene))], max_embed_mb=20)
    assert "maplibregl.Map" in html


def test_plot_static_composites_a_virtual_tile_layer(scene):
    from databricks.labs.gbx.vizx import plot_static

    ax = plot_static([raster_layer(_virtual(scene))], basemap=False)
    assert ax.get_images()
