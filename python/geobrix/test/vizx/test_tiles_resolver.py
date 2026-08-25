import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds


def _write_tif(path, size=32, dtype="uint16", nodata=0, crs="EPSG:4326"):
    data = (np.random.rand(size, size) * 100).astype(dtype)
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype=dtype,
        crs=crs,
        nodata=nodata,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return size


def _materialized_bytes(size=32):
    data = (np.random.rand(size, size) * 100).astype("uint16")
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            height=size,
            width=size,
            count=1,
            dtype="uint16",
            crs="EPSG:4326",
        ) as dst:
            dst.write(data, 1)
        return mf.read()


def test_resolve_virtual_v2_row(tmp_path):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    p = str(tmp_path / "v.tif")
    sz = _write_tif(p)
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    with resolve_tile_row(vt) as ds:
        assert ds.count == 1 and ds.width == sz


def test_resolve_materialized_and_bytes():
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    b = _materialized_bytes()
    with resolve_tile_row(b) as ds:  # raw bytes
        assert ds.width == 32
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    vt = VirtualTile(cellid=0, raster=b)  # materialized v2
    with resolve_tile_row(vt) as ds:
        assert ds.width == 32


def test_resolve_v1_three_field_dict():
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    b = _materialized_bytes()
    v1 = {"cellid": 0, "raster": b, "metadata": {}}  # v1 shape
    with resolve_tile_row(v1) as ds:
        assert ds.width == 32


def test_resolve_row_with_tile_col(tmp_path):
    # a Row-like dict whose tile_col holds the struct
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    p = str(tmp_path / "v.tif")
    sz = _write_tif(p)
    vt_row = {
        "cellid": -1,
        "raster": None,
        "path": p,
        "window": {"col_off": 0, "row_off": 0, "width": sz, "height": sz},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    row = {"tile": vt_row, "other": 1}
    with resolve_tile_row(row, tile_col="tile") as ds:
        assert ds.width == sz


def test_resolve_virtual_honors_pending_nodata(tmp_path):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    p = str(tmp_path / "nn.tif")
    sz = _write_tif(p, dtype="float32", nodata=None)
    vt = VirtualTile(
        cellid=-1,
        raster=None,
        path=p,
        window=(0, 0, sz, sz),
        metadata={"pending_nodata": "-9999.0"},
    )
    with resolve_tile_row(vt) as ds:
        assert ds.nodata == -9999.0
