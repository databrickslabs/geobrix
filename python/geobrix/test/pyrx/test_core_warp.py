import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import warp

from .conftest import make_geotiff_bytes


def test_reproject_to_srid_changes_crs_preserves_bands():
    with _serde.open_tile(make_geotiff_bytes(epsg=4326, count=2)) as ds:
        out = warp.reproject_to_srid(ds, 3857)
    with _serde.open_tile(out) as ds2:
        assert ds2.crs.to_epsg() == 3857
        assert ds2.count == 2
        assert ds2.width > 0 and ds2.height > 0


def test_reproject_to_srid_resampling_arg_accepted():
    with _serde.open_tile(make_geotiff_bytes(epsg=4326)) as ds:
        out = warp.reproject_to_srid(ds, 3857, resampling="bilinear")
    with _serde.open_tile(out) as ds2:
        assert ds2.crs.to_epsg() == 3857


def _fill_source(*, nodata=None, masked=False):
    """40x40 uint16 EPSG:4326 over Norfolk (the 27700 warp rotates it ~2.4 degrees, so
    its corners are fill): pixel p holds p + 1, row 3 holds 65535, no zeros."""
    n = 40
    data = (np.arange(n * n, dtype="uint32").reshape(n, n) + 1).astype("uint16")
    data[3, :] = 65535
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_bounds(1.00, 52.40, 1.20, 52.60, n, n),
    )
    if nodata is not None:
        prof["nodata"] = nodata
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**prof) as d:
                d.write(data, 1)
                if masked:
                    mask = np.full((n, n), 255, dtype="uint8")
                    mask[10:12, :] = 0
                    d.write_mask(mask)
            return mf.read()


def _warped(b, **kw):
    """``(nodata, valid mask, values)`` of the EPSG:27700 warp of GTiff bytes ``b``."""
    with MemoryFile(b) as mf, mf.open() as ds:
        out = warp.reproject_to_srid(ds, 27700, **kw)
    with MemoryFile(out) as mf, mf.open() as w:
        return w.nodata, w.read_masks(1) != 0, w.read(1)


def test_reproject_to_srid_leaves_the_fill_unmarked_by_default():
    nodata, valid, vals = _warped(_fill_source())
    assert nodata is None
    assert valid.all() and (vals == 0).any()  # the fill is valid zeros


def test_reproject_to_srid_mark_fill_nodata_declares_the_fallback():
    nodata, valid, vals = _warped(_fill_source(), mark_fill_nodata=True)
    assert nodata == 65535.0
    assert not valid.all()  # the fill is NoData
    assert not (vals[valid] == 0).any() and not (vals[valid] == 65535).any()
    assert ((vals[valid] >= 1) & (vals[valid] <= 40 * 40)).all()


def test_reproject_to_srid_mark_fill_nodata_keeps_a_sources_own_nodata():
    b = _fill_source(nodata=0)
    assert _warped(b, mark_fill_nodata=True)[0] == 0.0
    plain = _warped(b)
    marked = _warped(b, mark_fill_nodata=True)
    assert all((x == y).all() for x, y in zip(plain[1:], marked[1:]))


def test_reproject_to_srid_mark_fill_nodata_keeps_a_sources_own_mask():
    n = 40
    nodata, valid, vals = _warped(_fill_source(masked=True), mark_fill_nodata=True)
    assert nodata == 65535.0
    v = vals[valid].astype(int)
    assert not (v == 0).any()
    # a valid pixel names its source row: rows 10-11 are masked in the source. On a
    # masked source the warp keeps its own handling of the 65535s of row 3 (moved to
    # 65534), so those are not compared.
    rows = np.where(v == 65534, -1, (v - 1) // n)
    assert not np.isin(rows, (10, 11)).any()
