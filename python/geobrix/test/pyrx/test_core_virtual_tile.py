"""The v2 VirtualTile struct: validation, the row round trip, the schema, and the conversions
(v1 widen, virtual -> materialized).

The struct is deliberately full (path, window, clip_polygon, clip_crs, crs) so parity locks once.
The virtual -> materialized output must read back through the raster-precedence path exactly as
the source window does.
"""

import numpy as np
import pytest
import rasterio

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import virtual_tile as vt

from . import _layouts
from .conftest import make_geotiff_bytes

WINDOW = (128, 64, 200, 300)

# kwargs -> is_virtual (None: refused). A window is not required at construction: managed FILE
# tiles have a path and no window, and the reader validates it.
_VALIDITY = {
    "raster only": ({"raster": b"\x00\x01"}, False),
    "path and window": ({"path": "/Volumes/x.tif", "window": (0, 0, 256, 256)}, True),
    "path only": ({"path": "/Volumes/x.tif"}, True),
    "neither": ({}, None),
}


@pytest.mark.parametrize("name", sorted(_VALIDITY), ids=str)
def test_a_tile_needs_a_raster_or_a_path(name):
    kwargs, virtual = _VALIDITY[name]
    if virtual is None:
        with pytest.raises(ValueError):
            vt.VirtualTile(cellid=3, **kwargs)
        return
    t = vt.VirtualTile(cellid=2, **kwargs)
    assert t.is_virtual() is virtual
    assert t.window == kwargs.get("window")


def test_row_roundtrip_virtual_and_materialized():
    t = vt.VirtualTile(
        cellid=5,
        path="/Volumes/x.tif",
        window=(1, 2, 300, 400),
        clip_polygon=b"WKB",
        clip_crs="EPSG:4326",
        crs="EPSG:3857",
        metadata={"gbx_format": "cog"},
    )
    assert vt.VirtualTile.from_row(t.to_row()) == t
    m = vt.VirtualTile(cellid=6, raster=b"abc", metadata={})
    row = m.to_row()
    assert row["window"] is None
    assert vt.VirtualTile.from_row(row) == m


def test_v2_schema_field_order_matches_heavy_contract():
    assert [f.name for f in vt.V2_TILE_SCHEMA.fields] == [
        "cellid",
        "raster",
        "path",
        "path_mode",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
    ]


def test_build_tile_returns_v2_materialized_shape():
    """build_tile emits the v2 struct: raster set, provenance NULL, metadata computed by opening
    the raster (driver/width/height/count: not dropped)."""
    profile = dict(driver="GTiff", height=3, width=4, count=1, dtype="uint8")
    with rasterio.io.MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.zeros((1, 3, 4), dtype="uint8"))
        raster = mf.read()
    d = _serde.build_tile(raster, "GTiff", 7)
    assert set(d) == {f.name for f in vt.V2_TILE_SCHEMA.fields}
    assert d["cellid"] == 7 and d["raster"] == raster
    for prov in ("path", "window", "clip_polygon", "clip_crs", "crs"):
        assert d[prov] is None, prov
    assert {k: d["metadata"][k] for k in ("driver", "width", "height", "count")} == {
        "driver": "GTiff",
        "width": "4",
        "height": "3",
        "count": "1",
    }


def test_from_v1_widens_losslessly():
    b = make_geotiff_bytes(width=4, height=3)
    t = vt.VirtualTile.from_v1(cellid=7, raster=b, metadata={"driver": "GTiff"})
    assert t.raster == b
    assert t.path is None and t.window is None and t.clip_polygon is None
    assert t.clip_crs is None and t.crs is None
    assert not t.is_virtual()
    assert t.metadata == {"driver": "GTiff"}


@pytest.mark.parametrize("meta", [{"k": "v"}, None], ids=["metadata", "no-metadata"])
def test_materialize_to_bytes_keeps_provenance_and_reads_the_window(tmp_path, meta):
    path = _layouts.write_tiled_gtiff(str(tmp_path / "a.tif"), 512, 512, 256)
    virt = vt.VirtualTile(cellid=8, path=path, window=WINDOW, metadata=meta)
    mat = ot.materialize_to_bytes(virt)
    # materialized: raster set, provenance kept, tile_byte_size stamped
    assert mat.raster is not None and not mat.is_virtual()
    assert mat.path == path and mat.window == WINDOW and mat.cellid == 8
    assert mat.metadata == {**(meta or {}), "tile_byte_size": str(len(mat.raster))}
    # the raster-precedence path yields exactly the window's pixels
    with ot.open_tile(mat) as ds:
        got = ds.read(1)
    with rasterio.open(path) as ds:
        full = ds.read(1)
    c, r, w, h = WINDOW
    assert np.array_equal(got, full[r : r + h, c : c + w])
