import warnings

import numpy as np
import pytest
import rasterio
from rasterio.errors import NodataShadowWarning
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
    # the 65535s of row 3 are NoData, not moved to 65534 and kept valid
    assert ((v >= 1) & (v <= n * n)).all(), sorted(set(v[v > n * n]))[:3]
    # a valid pixel names its source row: rows 10-11 are masked in the source
    assert not np.isin((v - 1) // n, (10, 11)).any()


def test_reproject_to_srid_mark_fill_nodata_keeps_the_mask_of_a_source_with_nodata():
    """A source NoData would make the warp drop the mask: the masked pixels stay NoData,
    the source's NoData is kept, and its 65535s stay real values."""
    n = 40
    nodata, valid, vals = _warped(
        _fill_source(nodata=7, masked=True), mark_fill_nodata=True
    )
    assert nodata == 7.0
    v = vals[valid].astype(int)
    assert not (v == 7).any()
    assert (v == 65535).any()
    named = v[v <= n * n]
    assert not np.isin((named - 1) // n, (10, 11)).any()


def test_reproject_to_crs_mark_fill_nodata_warps_as_reproject_to_srid():
    """Both entry points share the fill policy: a string-given CRS warps the same."""
    for kw in ({}, {"masked": True}, {"nodata": 7, "masked": True}):
        b = _fill_source(**kw)
        with MemoryFile(b) as mf, mf.open() as ds:
            by_srid = warp.reproject_to_srid(ds, 27700, mark_fill_nodata=True)
            by_crs = warp.reproject_to_crs(ds, "EPSG:27700", mark_fill_nodata=True)
        with (
            MemoryFile(by_srid) as a,
            a.open() as da,
            MemoryFile(by_crs) as c,
            c.open() as dc,
        ):
            assert dc.nodata == da.nodata is not None, kw
            assert (dc.read_masks(1) == da.read_masks(1)).all(), kw
            assert (dc.read(1) == da.read(1)).all(), kw


def test_reproject_to_srid_mark_fill_nodata_leaves_an_alpha_source_alone():
    """The warped alpha band marks the fill, so no NoData is declared over it."""
    n = 40
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=4,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(1.00, 52.40, 1.20, 52.60, n, n),
        photometric="RGB",
        alpha="YES",
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.full((4, n, n), 200, dtype="uint8"))
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        plain = warp.reproject_to_srid(ds, 27700)
        marked = warp.reproject_to_srid(ds, 27700, mark_fill_nodata=True)
    with (
        MemoryFile(plain) as p,
        p.open() as dp,
        MemoryFile(marked) as m,
        m.open() as dm,
    ):
        assert dm.nodatavals == (None,) * 4
        assert dm.colorinterp == dp.colorinterp
        assert (dm.read() == dp.read()).all()
        assert (dm.read(4) == 0).any()  # the fill is transparent


def _banded_source(layout):
    """uint8/uint16 EPSG:4326 source over Norfolk whose last band is, or is not, alpha."""
    n, count, dtype, alpha = {
        "rgb_nir_u8": (16, 4, "uint8", False),
        "rgba_u16": (16, 4, "uint16", True),
    }[layout]
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=count,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_bounds(1.00, 52.40, 1.20, 52.60, n, n),
        photometric="RGB",
    )
    if alpha:
        prof["alpha"] = "YES"
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.full((count, n, n), 100, dtype=dtype))
        return mf.read()


def _alpha_labels(b):
    with MemoryFile(b) as mf, mf.open() as ds:
        return [c.name == "alpha" for c in ds.colorinterp]


@pytest.mark.parametrize("target", [27700, 4326])  # a warp, then the identity copy
@pytest.mark.parametrize("by", ["srid", "crs"])
@pytest.mark.parametrize("layout", ["rgb_nir_u8", "rgba_u16"])
def test_warp_labels_a_band_alpha_exactly_when_the_source_does(layout, by, target):
    """GDAL labels band 4 of a 4-band uint8 GTiff as alpha, so a warp turned the
    near-infrared band of RGB+NIR into a transparency mask (and read_masks then warned
    that the NoData shadows it), and lost the alpha band of a uint16 RGBA source."""
    src = _banded_source(layout)
    with MemoryFile(src) as mf, mf.open() as ds:
        if by == "srid":
            out = warp.reproject_to_srid(ds, target, mark_fill_nodata=True)
        else:
            out = warp.reproject_to_crs(ds, f"EPSG:{target}", mark_fill_nodata=True)
    assert _alpha_labels(out) == _alpha_labels(src)
    with MemoryFile(out) as mf, mf.open() as w, warnings.catch_warnings():
        warnings.simplefilter("error", NodataShadowWarning)
        w.read_masks()
