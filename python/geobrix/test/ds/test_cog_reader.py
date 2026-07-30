from databricks.labs.gbx.ds.cog import CogGbxReader, CogGbxDataSource


def test_cog_reader_defaults_no_split():
    r = CogGbxReader({"path": "/x"})
    # cog lane does not force splitting; strategy resolves to none by default
    assert r.strategy == "none"


def test_cog_reader_honors_bbox():
    r = CogGbxReader({"path": "/x", "bbox": "0,0,1,1"})
    assert r.bbox == (0.0, 0.0, 1.0, 1.0)


def test_datasource_name():
    assert CogGbxDataSource.name() == "cog_gbx"
