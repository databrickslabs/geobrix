"""Raster reader option table: clipPolygons / windows / clipCrs / tileSize / overlapPercent.

Both ways in are rows: the programmatic one (real Python lists, as unit callers pass) and the Spark
``.option()`` one, where every value is a STRING and a list must be a JSON string (a plain list never
survives ``.option()``: Spark str()-reprs it). Pure Python, no Spark.
"""

import json
import re

import pytest
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxReader

_WKT1 = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
_WKT2 = "POLYGON((2 2,3 2,3 3,2 3,2 2))"
_WKB = shapely.wkb.dumps(box(0, 0, 1, 1))
_EWKT = "SRID=4326;" + _WKT1

# name -> (options, attributes the reader must carry); attributes not listed stay at their default
DEFAULT = {
    "clip_polygons": [],
    "windows": [],
    "clip_crs": None,
    "tile_size": None,
    "overlap_percent": 0,
}
PARSE = {
    "defaults": ({}, {}),
    "one_wkb_polygon": ({"clipPolygons": _WKB}, {"clip_polygons": [_WKB]}),
    "list_of_wkb_polygons": (
        {"clipPolygons": [_WKB, _WKB]},
        {"clip_polygons": [_WKB, _WKB]},
    ),
    "one_window_tuple": (
        {"windows": (0, 0, 256, 256)},
        {"windows": [(0, 0, 256, 256)]},
    ),
    "list_of_window_tuples": (
        {"windows": [(0, 0, 256, 256), (256, 0, 256, 256)]},
        {"windows": [(0, 0, 256, 256), (256, 0, 256, 256)]},
    ),
    "clip_crs": (
        {"clipPolygons": _WKB, "clipCrs": "EPSG:4326"},
        {"clip_polygons": [_WKB], "clip_crs": "EPSG:4326"},
    ),
    # Spark .option() strings
    "bare_wkt_is_one_geometry_not_split": (
        {"clipPolygons": _WKT1},
        {"clip_polygons": [_WKT1]},
    ),
    "json_array_of_wkt": (
        {"clipPolygons": json.dumps([_WKT1, _WKT2])},
        {"clip_polygons": [_WKT1, _WKT2]},
    ),
    "ewkt_semicolon_is_not_json": ({"clipPolygons": _EWKT}, {"clip_polygons": [_EWKT]}),
    "one_window_json": ({"windows": "[0,0,256,256]"}, {"windows": [(0, 0, 256, 256)]}),
    "list_of_windows_json": (
        {"windows": "[[0,0,256,256],[256,0,256,256]]"},
        {"windows": [(0, 0, 256, 256), (256, 0, 256, 256)]},
    ),
    "tile_size_w_h": ({"tileSize": "512,256"}, {"tile_size": (512, 256)}),
    "tile_size_square": ({"tileSize": "512"}, {"tile_size": (512, 512)}),
    "overlap_percent": (
        {"tileSize": "256", "overlapPercent": "25"},
        {"tile_size": (256, 256), "overlap_percent": 25},
    ),
}


@pytest.mark.parametrize("opts,attrs", PARSE.values(), ids=PARSE.keys())
def test_reader_options_parse(opts, attrs):
    r = RasterGbxReader({"path": "/x", **opts})
    for name, want in {**DEFAULT, **attrs}.items():
        assert getattr(r, name) == want, name
    assert not hasattr(r, "bbox")


_EXCLUSIVE = "raster_gbx: 'clipPolygons', 'windows', and 'tileSize' are mutually exclusive; supply at most one."

# name -> (options, the whole message)
REJECT = {
    "clip_and_windows": ({"clipPolygons": _WKB, "windows": (0, 0, 8, 8)}, _EXCLUSIVE),
    "tile_size_and_windows": ({"tileSize": "256", "windows": "[0,0,8,8]"}, _EXCLUSIVE),
    "tile_size_and_clip": ({"tileSize": "256", "clipPolygons": _WKT1}, _EXCLUSIVE),
    "comma_string_windows_is_not_json": (
        {"windows": "0,0,256,256"},
        "raster_gbx: 'windows' must be a JSON 4-int array '[c,r,w,h]' or a JSON array of them '[[..],[..]]'; got '0,0,256,256'",
    ),
    "overlap_without_tile_size": (
        {"overlapPercent": "25"},
        "raster_gbx: 'overlapPercent' requires 'tileSize' (it modifies the regular tiling grid only).",
    ),
    "tile_size_not_a_number": (
        {"tileSize": "abc"},
        "invalid literal for int() with base 10: 'abc'",
    ),
    "overlap_over_99": (
        {"tileSize": "256", "overlapPercent": "150"},
        "raster_gbx: 'overlapPercent' must be 0..99; got 150",
    ),
}


@pytest.mark.parametrize("opts,sentence", REJECT.values(), ids=REJECT.keys())
def test_reader_options_rejected(opts, sentence):
    with pytest.raises(ValueError, match=re.escape(sentence)):
        RasterGbxReader({"path": "/x", **opts})
