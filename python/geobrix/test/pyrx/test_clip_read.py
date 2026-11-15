from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import pytest
import shapely.geometry as sg
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core.clip_read import read_clip


def _kernel_clip(blob, poly, crs=None):
    from databricks.labs.gbx.pyrx.core.edit import clip_to_geom

    with MemoryFile(blob) as mf, mf.open() as ds:
        return clip_to_geom(ds, poly, False, geom_crs=crs)


def _pixels(gtiff):
    with MemoryFile(gtiff) as mf, mf.open() as ds:
        return ds.read(), ds.transform, ds.crs, ds.nodata


def test_clip_matches_rst_clip_kernel():
    blob = _cog_bytes()
    poly = sg.Polygon([(405000, 3990000), (412000, 3991000), (409000, 3996000)])
    with MemoryFile(blob) as mf, mf.open() as ds:
        got = read_clip(ds, poly)
    want = _kernel_clip(blob, poly)
    g, w = _pixels(got), _pixels(want)
    assert np.array_equal(g[0], w[0]) and g[1] == w[1] and g[2] == w[2] and g[3] == w[3]


def test_clip_other_crs():
    blob = _cog_bytes()
    from pyproj import Transformer

    t = Transformer.from_crs("EPSG:32614", "EPSG:4326", always_xy=True)
    poly4326 = sg.Polygon(
        [
            t.transform(x, y)
            for x, y in [(405000, 3990000), (412000, 3991000), (409000, 3996000)]
        ]
    )
    with MemoryFile(blob) as mf, mf.open() as ds:
        got = read_clip(ds, poly4326, "EPSG:4326")
    assert got is not None and _pixels(got)[2].to_epsg() == 32614
    g, w = _pixels(got), _pixels(_kernel_clip(blob, poly4326, "EPSG:4326"))
    assert np.array_equal(g[0], w[0]) and g[1] == w[1]


def test_clip_outside_is_none():
    blob = _cog_bytes()
    with MemoryFile(blob) as mf, mf.open() as ds:
        assert read_clip(ds, sg.box(0, 0, 10, 10)) is None


def test_whole_raster_without_geom():
    blob = _cog_bytes(width=700, height=600)
    with MemoryFile(blob) as mf, mf.open() as ds:
        arr, *_ = _pixels(read_clip(ds))
        assert arr.shape == (3, 600, 700)


def test_cap_refusal():
    from databricks.labs.gbx.ds.file_gbx import StageTooLargeError

    blob = _cog_bytes()
    with MemoryFile(blob) as mf, mf.open() as ds:
        with pytest.raises(StageTooLargeError, match="Fix:"):
            read_clip(ds, None, cap_bytes=1024)
