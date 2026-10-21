import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit

from .conftest import make_geotiff_bytes


def test_clip_crops_to_geometry():
    # raster extent x:[10,12] y:[48.5,50], 0.5px. Box over col1..2 / row1 -> 2x1.
    geom_wkb = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = edit.clip_to_geom(ds, geom_wkb, all_touched=False)
    with _serde.open_tile(out) as o:
        assert o.crs.to_epsg() == 4326
        assert o.width < 4 and o.height < 3
        assert o.width > 0 and o.height > 0


def test_update_type_casts_dtype():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3)) as ds:
        out = edit.update_type(ds, "Int32")
    with _serde.open_tile(out) as o:
        assert o.dtypes[0] == "int32"
        assert (o.width, o.height) == (4, 3)


def test_init_nodata_sets_when_missing():
    with _serde.open_tile(make_geotiff_bytes(nodata=None)) as ds:
        assert ds.nodata is None
        out = edit.init_nodata(ds)
    with _serde.open_tile(out) as o:
        # float32 fallback = heavy GDAL.getNoDataConstant(Float32) = Float.MinValue
        assert o.nodata == float(np.finfo(np.float32).min)


def test_init_nodata_preserves_existing():
    with _serde.open_tile(make_geotiff_bytes(nodata=-1.0)) as ds:
        out = edit.init_nodata(ds)
    with _serde.open_tile(out) as o:
        assert o.nodata == -1.0


def _u16_no_nodata(zeros=False):
    arr = np.random.default_rng(3).integers(1, 4000, (48, 64)).astype("uint16")
    if zeros:
        arr[:8, :8] = 0  # real zero pixels, not nodata
    prof = dict(
        driver="GTiff",
        width=64,
        height=48,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_bounds(-122.52, 37.70, -122.50, 37.715, 64, 48),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(arr, 1)
        return mf.read()


def test_threshold_uint16_without_nodata_uses_fallback():
    with MemoryFile(_u16_no_nodata()) as mf, mf.open() as ds:
        out = edit.threshold(ds, ">", 2000)
    with MemoryFile(out) as m, m.open() as c:
        assert c.nodata == 65535.0
        assert (c.read_masks(1) == 0).any() and (c.read_masks(1) != 0).any()


def test_init_nodata_uint16_without_nodata_stamps_fallback():
    with MemoryFile(_u16_no_nodata()) as mf, mf.open() as ds:
        out = edit.init_nodata(ds)
    with MemoryFile(out) as m, m.open() as c:
        assert c.nodata == 65535.0


def test_init_nodata_keeps_existing_nodata():
    prof = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="uint16",
        nodata=7,
        crs="EPSG:4326",
        transform=from_bounds(0, 0, 1, 1, 4, 4),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.ones((1, 4, 4), "uint16"))
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = edit.init_nodata(ds)
    with MemoryFile(out) as m, m.open() as c:
        assert c.nodata == 7.0
