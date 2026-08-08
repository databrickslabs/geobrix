from databricks.labs.gbx.pyrx import _serde


def test_build_error_tile_shape():
    t = _serde.build_error_tile("RST_ReTile: unreadable raster")
    assert t["raster"] is None
    assert t["cellid"] == -1
    assert t["metadata"]["last_error"] == "RST_ReTile: unreadable raster"


def test_tile_schema_raster_is_nullable():
    field = [f for f in _serde.TILE_SCHEMA.fields if f.name == "raster"][0]
    assert field.nullable is True
