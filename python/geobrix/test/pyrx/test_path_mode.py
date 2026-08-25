# python/geobrix/test/pyrx/test_path_mode.py
from databricks.labs.gbx.pyrx.core.virtual_tile import (
    V2_TILE_SCHEMA,
    VirtualTile,
    effective_path_mode,
)


def test_schema_path_mode_after_path_metadata_last():
    names = [f.name for f in V2_TILE_SCHEMA.fields]
    assert len(names) == 9
    # path_mode moved to right after path (index 3); metadata is now the last field.
    assert names.index("path_mode") == 3
    assert names[names.index("path") + 1] == "path_mode"
    assert names[-1] == "metadata"
    assert V2_TILE_SCHEMA["path_mode"].nullable is True


def test_from_v1_sets_none_path_mode():
    vt = VirtualTile.from_v1(cellid=1, raster=b"xx")
    assert vt.path_mode is None


def test_to_row_from_row_roundtrips_path_mode():
    vt = VirtualTile(cellid=1, path="/Volumes/a/b/c.tif", path_mode="managed")
    back = VirtualTile.from_row(vt.to_row())
    assert back.path_mode == "managed"


def test_effective_path_mode_infers_when_absent():
    assert effective_path_mode(VirtualTile(cellid=1, raster=b"xx")) is None
    assert (
        effective_path_mode(VirtualTile(cellid=1, path="/Volumes/a/b.tif"))
        == "external"
    )
    assert (
        effective_path_mode(VirtualTile(cellid=1, path="/V/x.tif", path_mode="managed"))
        == "managed"
    )
