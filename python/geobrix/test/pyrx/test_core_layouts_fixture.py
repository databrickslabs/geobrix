"""The 3-layout corpus fixture writes identical pixels in COG / tiled / striped
form. This self-test pins that invariant so downstream open_tile tests can trust
'same window -> same pixels across layouts'.
"""
import numpy as np
import rasterio

from . import _layouts


def test_three_layouts_same_pixels(tmp_path):
    w, h = 512, 384
    cog = _layouts.write_cog(str(tmp_path / "a.cog.tif"), w, h)
    tiled = _layouts.write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), w, h)
    striped = _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), w, h)

    with rasterio.open(cog) as c, rasterio.open(tiled) as t, rasterio.open(striped) as s:
        assert c.profile["tiled"] is True
        assert t.profile["tiled"] is True
        assert s.profile.get("tiled", False) is False
        assert c.overviews(1)  # COG has overviews
        arr_c, arr_t, arr_s = c.read(1), t.read(1), s.read(1)
    assert np.array_equal(arr_c, arr_t)
    assert np.array_equal(arr_t, arr_s)
    assert np.array_equal(arr_c, _layouts.PIXELS(w, h))
