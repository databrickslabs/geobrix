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


def test_path_only_no_window_is_allowed():
    # Window is no longer required at construction — managed FILE tiles have a
    # path but no window; validation is deferred to the reader.
    t = vt.VirtualTile(cellid=4, path="/Volumes/x.tif")
    assert t.is_virtual()


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


def test_v2_schema_field_order_matches_heavy_contract():
    names = [f.name for f in vt.V2_TILE_SCHEMA.fields]
    assert names == [
        "cellid",
        "raster",
        "path",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
        "path_mode",
    ]


def test_schema_has_v2_fields():
    names = set(V.name for V in vt.V2_TILE_SCHEMA.fields)
    assert names == {
        "cellid",
        "raster",
        "path",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
        "path_mode",
    }


def test_build_tile_returns_v2_materialized_shape():
    """build_tile emits the 9-field v2 struct: raster set, provenance NULL,
    metadata carries driver/width/height/count (computed by opening the raster)."""
    import numpy as np
    import rasterio

    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    # a tiny in-memory 4x3 single-band GeoTIFF
    profile = dict(driver="GTiff", height=3, width=4, count=1, dtype="uint8")
    with rasterio.io.MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.zeros((1, 3, 4), dtype="uint8"))
        raster = mf.read()

    d = _serde.build_tile(raster, "GTiff", 7)

    # exactly the 8 v2 fields, exact names
    assert set(d.keys()) == {f.name for f in V2_TILE_SCHEMA.fields}
    assert d["cellid"] == 7
    assert d["raster"] == raster
    # provenance fields NULL for a materialized tile
    for prov in ("path", "window", "clip_polygon", "clip_crs", "crs"):
        assert d[prov] is None, f"{prov} should be NULL on a materialized tile"
    # metadata computed by opening the raster (regression guard: NOT dropped)
    assert d["metadata"]["driver"] == "GTiff"
    assert d["metadata"]["width"] == "4"
    assert d["metadata"]["height"] == "3"
    assert d["metadata"]["count"] == "1"
