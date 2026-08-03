import numpy as np
import rasterio
from rasterio.transform import from_bounds


def _write_tif(path, size=16, dtype="uint16", crs="EPSG:4326"):
    data = (np.arange(size * size).reshape(size, size) % 50).astype(dtype)
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
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return size


def test_tile_to_numpy_virtual(tmp_path):
    from databricks.labs.gbx.pyrx.core.escape import tile_to_numpy
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    p = str(tmp_path / "v.tif")
    sz = _write_tif(p)
    vt_row = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz)).to_row()
    arr = tile_to_numpy(vt_row)
    assert arr is not None and arr.shape[-1] == sz


def test_tile_to_numpy_bytes_and_v1_still_work(tmp_path):
    from databricks.labs.gbx.pyrx.core.escape import tile_to_numpy

    p = str(tmp_path / "m.tif")
    sz = _write_tif(p)
    raw = open(p, "rb").read()
    assert tile_to_numpy(raw).shape[-1] == sz  # bytes
    assert (
        tile_to_numpy({"cellid": 0, "raster": raw, "metadata": {}}).shape[-1] == sz
    )  # v1
