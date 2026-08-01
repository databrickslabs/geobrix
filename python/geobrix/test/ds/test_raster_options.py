"""Reader option parsing: clipPolygons/windows/clipCrs normalization + exclusion."""

import pytest
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxReader


def _wkb():
    return shapely.wkb.dumps(box(0, 0, 1, 1))


def test_single_clip_polygon_normalizes_to_list():
    r = RasterGbxReader({"path": "/x", "clipPolygons": _wkb()})
    assert isinstance(r.clip_polygons, list) and len(r.clip_polygons) == 1
    assert r.windows == []


def test_list_clip_polygons():
    r = RasterGbxReader({"path": "/x", "clipPolygons": [_wkb(), _wkb()]})
    assert len(r.clip_polygons) == 2


def test_single_window_normalizes_to_list():
    r = RasterGbxReader({"path": "/x", "windows": (0, 0, 256, 256)})
    assert r.windows == [(0, 0, 256, 256)]
    assert r.clip_polygons == []


def test_list_windows():
    r = RasterGbxReader(
        {"path": "/x", "windows": [(0, 0, 256, 256), (256, 0, 256, 256)]}
    )
    assert len(r.windows) == 2


def test_clip_crs_parsed():
    r = RasterGbxReader({"path": "/x", "clipPolygons": _wkb(), "clipCrs": "EPSG:4326"})
    assert r.clip_crs == "EPSG:4326"


def test_clip_and_windows_mutually_exclusive():
    with pytest.raises(ValueError):
        RasterGbxReader({"path": "/x", "clipPolygons": _wkb(), "windows": (0, 0, 8, 8)})


def test_no_bbox_option_attribute():
    r = RasterGbxReader({"path": "/x"})
    assert r.clip_polygons == [] and r.windows == [] and r.clip_crs is None
    assert not hasattr(r, "bbox")
