import numpy as np
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features


def test_rasterize_burns_value_inside_nodata_outside():
    # extent x:[0,4] y:[0,4], 4x4 px (1 unit/px). Box covers x:[1,3] y:[1,3].
    geom = shapely.wkb.dumps(box(1.0, 1.0, 3.0, 3.0))
    out = features.rasterize_geom(geom, 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326)
    with _serde.open_tile(out) as ds:
        assert ds.crs.to_epsg() == 4326
        assert (ds.width, ds.height) == (4, 4)
        assert ds.nodata == -9999.0
        arr = ds.read(1)
        # center pixels burned to 5.0, corners remain nodata
        assert 5.0 in np.unique(arr)
        assert -9999.0 in np.unique(arr)


def test_fillnodata_fills_hole():
    # Build a 1-band 5x5 float32 raster, all 1.0 except a nodata hole in the middle.
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.ones((5, 5), dtype="float32")
    data[2, 2] = -9999.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src_bytes = mf.read()
    with _serde.open_tile(src_bytes) as ds:
        out = features.fill_nodata(ds)
    with _serde.open_tile(out) as o:
        filled = o.read(1)
        assert filled[2, 2] != -9999.0  # the hole was interpolated
        assert filled[2, 2] == 1.0  # surrounded by 1.0


def test_fillnodata_reads_the_alpha_band_as_the_mask_never_as_values():
    """The UC level's fill on an RGBA tile with a declared NoData: the transparent pixels are the holes, the alpha is
    never interpolated (a filled pixel becomes opaque at the type's top, any other keeps its alpha), and a pixel the
    search does not reach stays transparent."""
    from affine import Affine
    from rasterio.io import MemoryFile

    n = 9
    z = np.zeros((4, n, n), "uint8")
    z[:3] = (50 + np.arange(n)[None, None, :] + 3 * np.arange(n)[None, :, None]).astype("uint8")
    z[3] = (100 + 155 * np.arange(n * n).reshape(n, n) // (n * n - 1)).astype("uint8")
    z[3, 3:6, 3:6] = 0
    z[:3, 4, 4] = 0
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff", width=n, height=n, count=4, dtype="uint8", crs="EPSG:3857",
            transform=Affine(1, 0, 0, 0, -1, n), nodata=0, photometric="RGB", ALPHA="YES",
        ) as ds:
            ds.write(z)
        with mf.open() as ds:
            near = features.fill_nodata(ds, 1, 0)
            full = features.fill_nodata(ds, 100, 0)
    clear = z[3] == 0
    with _serde.open_tile(full) as out:
        a = out.read(4)
        assert (a[clear] == 255).all() and np.array_equal(a[~clear], z[3][~clear])
        assert out.read(1)[4, 4] != 0
    with _serde.open_tile(near) as out:
        assert out.read(4)[4, 4] == 0 and (out.read(4)[3:6, 3:6] == 255).sum() == 8
