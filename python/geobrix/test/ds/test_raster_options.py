"""Reader option parsing: clipPolygons/windows/clipCrs normalization + exclusion.

Covers BOTH the programmatic path (real Python lists, as unit callers pass) AND
the Spark ``.option()`` path (everything is a STRING — a list must be a JSON
string). The JSON-string cases are the ones that mirror the real Spark read API;
a plain Python list never survives ``.option()`` (Spark str()-repr's it).
"""

import json

import pytest
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxReader

_WKT1 = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
_WKT2 = "POLYGON((2 2,3 2,3 3,2 3,2 2))"


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


# --- Spark .option() string path (the real read API — lists are JSON strings) ---


def test_single_wkt_string_stays_one_geometry():
    # a bare WKT string is not JSON -> one geometry (not char-split, not error)
    r = RasterGbxReader({"path": "/x", "clipPolygons": _WKT1})
    assert r.clip_polygons == [_WKT1]


def test_json_array_string_of_wkt_is_a_list():
    # this is what Spark .option() actually receives for a list
    r = RasterGbxReader({"path": "/x", "clipPolygons": json.dumps([_WKT1, _WKT2])})
    assert r.clip_polygons == [_WKT1, _WKT2]


def test_ewkt_string_with_semicolon_not_mistaken_for_json():
    ewkt = "SRID=4326;" + _WKT1
    r = RasterGbxReader({"path": "/x", "clipPolygons": ewkt})
    assert r.clip_polygons == [ewkt]  # single, not JSON


def test_single_window_json_string():
    r = RasterGbxReader({"path": "/x", "windows": "[0,0,256,256]"})
    assert r.windows == [(0, 0, 256, 256)]


def test_list_windows_json_string():
    r = RasterGbxReader({"path": "/x", "windows": "[[0,0,256,256],[256,0,256,256]]"})
    assert r.windows == [(0, 0, 256, 256), (256, 0, 256, 256)]


def test_non_json_windows_string_raises_clear_error():
    # a comma-string (old bbox habit) is not JSON -> clear ValueError, not raw JSONDecodeError
    with pytest.raises(ValueError, match="windows.*JSON"):
        RasterGbxReader({"path": "/x", "windows": "0,0,256,256"})
