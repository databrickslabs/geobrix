"""v2 reader schema + row builder: single struct-assembly point for the reader."""

from databricks.labs.gbx.ds.raster import _v2_tile_row, reader_schema_v2
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA


def test_reader_schema_v2_is_source_plus_v2_tile():
    sch = reader_schema_v2()
    assert [f.name for f in sch.fields] == ["source", "tile"]
    assert sch["tile"].dataType == V2_TILE_SCHEMA
    assert sch["tile"].dataType["raster"].nullable is True


def test_v2_row_materialized_shape_and_order():
    row = _v2_tile_row(
        cellid=-1,
        raster=b"abc",
        path="/v/x.tif",
        window=(1, 2, 300, 400),
        metadata={"driver": "GTiff"},
    )
    # 8-tuple in V2_TILE_SCHEMA field order
    assert row[0] == -1  # cellid
    assert row[1] == b"abc"  # raster
    assert row[2] == "/v/x.tif"  # path
    assert row[3] == {"col_off": 1, "row_off": 2, "width": 300, "height": 400}
    assert (
        row[4] is None and row[5] is None and row[6] is None
    )  # clip_polygon/clip_crs/crs
    assert row[7] == {"driver": "GTiff"}  # metadata


def test_v2_row_virtual_null_raster_and_window_dict():
    row = _v2_tile_row(
        cellid=-1,
        raster=None,
        path="/v/x.tif",
        window=(0, 0, 512, 512),
        metadata={"format": "cog"},
    )
    assert row[1] is None  # raster null (virtual)
    assert row[3] == {"col_off": 0, "row_off": 0, "width": 512, "height": 512}


def test_v2_row_none_window_serializes_to_none():
    row = _v2_tile_row(
        cellid=-1, raster=b"abc", path="/v/x.tif", window=None, metadata={}
    )
    assert row[3] is None
