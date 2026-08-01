from databricks.labs.gbx.ds.cog import CogGbxDataSource, CogGbxReader


def test_cog_reader_defaults_no_split():
    r = CogGbxReader({"path": "/x"})
    # cog lane does not force splitting; strategy resolves to none by default
    assert r.strategy == "none"


def test_cog_reader_honors_clip_polygons():
    # bbox/bboxCrs were removed in favor of clipPolygons/clipCrs (a box is its
    # own envelope). The cog reader inherits RasterGbxReader's option parsing.
    wkt = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
    r = CogGbxReader({"path": "/x", "clipPolygons": wkt, "clipCrs": "EPSG:4326"})
    assert r.clip_polygons == [wkt]
    assert r.clip_crs == "EPSG:4326"
    assert not hasattr(r, "bbox")


def test_datasource_name():
    assert CogGbxDataSource.name() == "cog_gbx"
