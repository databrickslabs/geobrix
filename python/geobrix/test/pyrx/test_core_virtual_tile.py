"""v2 VirtualTile struct: validation + Spark round-trip.

The struct is deliberately full (path, window, clip_polygon, clip_crs, crs)
so parity locks once; the reader does not emit it yet (later increment).
"""
import pytest

from databricks.labs.gbx.pyrx.core import virtual_tile as vt


def test_materialized_tile_valid_with_raster_only():
    t = vt.VirtualTile(cellid=1, raster=b"\x00\x01")
    assert not t.is_virtual()


def test_virtual_tile_valid_with_path_and_window():
    t = vt.VirtualTile(cellid=2, path="/Volumes/x.tif", window=(0, 0, 256, 256))
    assert t.is_virtual()
    assert t.window == (0, 0, 256, 256)


def test_rejects_no_raster_and_no_path():
    with pytest.raises(ValueError):
        vt.VirtualTile(cellid=3)


def test_rejects_virtual_without_window():
    with pytest.raises(ValueError):
        vt.VirtualTile(cellid=4, path="/Volumes/x.tif")  # no window


def test_row_roundtrip_virtual():
    t = vt.VirtualTile(
        cellid=5,
        path="/Volumes/x.tif",
        window=(1, 2, 300, 400),
        clip_polygon=b"WKB",
        clip_crs="EPSG:4326",
        crs="EPSG:3857",
        metadata={"gbx_format": "cog"},
    )
    back = vt.VirtualTile.from_row(t.to_row())
    assert back == t


def test_row_roundtrip_materialized_null_window():
    t = vt.VirtualTile(cellid=6, raster=b"abc", metadata={})
    row = t.to_row()
    assert row["window"] is None
    assert vt.VirtualTile.from_row(row) == t


def test_schema_has_v2_fields():
    names = set(V.name for V in vt.V2_TILE_SCHEMA.fields)
    assert names == {
        "cellid", "raster", "path", "window", "clip_polygon", "clip_crs", "crs", "metadata"
    }
