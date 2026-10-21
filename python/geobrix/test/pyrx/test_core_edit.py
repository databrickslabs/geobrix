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


def _masked_u16(nodata=None):
    """8x8 uint16 with an internal per-dataset mask over every fifth pixel, an unmasked
    65535 (the fallback) at pixel 7 and an unmasked 7 at pixel 9."""
    import rasterio

    vals = (np.arange(64) + 100).astype("uint16")
    vals[7], vals[9] = 65535, 7
    mask = np.where(np.arange(64) % 5 == 0, 0, 255).astype("uint8")
    prof = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="uint16",
        nodata=nodata,
        crs="EPSG:4326",
        transform=from_bounds(0, 0, 1, 1, 8, 8),
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**prof) as d:
                d.write(vals.reshape(1, 8, 8))
                d.write_mask(mask.reshape(8, 8))
            return mf.read(), vals, mask == 0


def test_init_nodata_keeps_a_sources_own_mask():
    """The masked pixels are NoData in the output, and so is the real 65535."""
    b, vals, hidden = _masked_u16()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = edit.init_nodata(ds)
    with MemoryFile(out) as m, m.open() as c:
        assert c.nodata == 65535.0
        valid = (c.read_masks(1) != 0).ravel()
        data = c.read(1).ravel()
    assert not valid[hidden].any(), "masked source pixels became valid"
    assert not valid[7], "the real 65535 stayed valid"
    keep = ~hidden & (vals != 65535)
    assert valid[keep].all() and (data[keep] == vals[keep]).all()


def test_init_nodata_keeps_the_mask_of_a_source_with_nodata():
    """A source with NoData 7 keeps it; its masked pixels and its 7 are NoData."""
    b, vals, hidden = _masked_u16(nodata=7)
    with MemoryFile(b) as mf, mf.open() as ds:
        out = edit.init_nodata(ds)
    with MemoryFile(out) as m, m.open() as c:
        assert c.nodata == 7.0
        valid = (c.read_masks(1) != 0).ravel()
        data = c.read(1).ravel()
    assert not valid[hidden].any(), "masked source pixels became valid"
    assert not valid[9], "the source's NoData 7 pixel became valid"
    keep = ~hidden & (vals != 7)
    assert valid[keep].all() and (data[keep] == vals[keep]).all()
