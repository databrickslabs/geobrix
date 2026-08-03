import glob

import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _a_tif():
    # any real multi-band tif; bench-corpus rows are 4-band EPSG:4326
    for c in sorted(
        glob.glob("sample-data/Volumes/main/default/bench-corpus/rows/*.tif")
    ):
        return c
    return sorted(glob.glob("target/test-classes/modis/*_B02.TIF"))[0]


def _virtual(tif, **meta):
    with rasterio.open(tif) as ds:
        w, h = ds.width, ds.height
    return VirtualTile(
        cellid=-1, raster=None, path=tif, window=(0, 0, w, h), metadata=dict(meta)
    )


def test_pending_nodata_applied_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.nodata == -9999.0


def test_pending_srid_relabels_crs_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_srid="3857")
    with ot.open_tile(vt) as ds:
        assert ds.crs.to_epsg() == 3857


def test_pending_bands_selects_bands_at_open():
    tif = _a_tif()
    with rasterio.open(tif) as ds:
        assert ds.count >= 2, "test needs a multi-band source"
    vt = _virtual(tif, pending_bands="1")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1


def test_pending_apply_order_band_then_nodata():
    # band-select THEN nodata: result is single-band with nodata set
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1
        assert ds.nodata == -9999.0


def test_no_pending_keys_is_noop():
    tif = _a_tif()
    vt = _virtual(tif)
    with rasterio.open(tif) as src:
        want_bands = src.count
    with ot.open_tile(vt) as ds:
        assert ds.count == want_bands


def test_open_header_reflects_pending_bands_and_srid():
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_srid="3857")
    with ot.open_header(vt) as ds:
        assert ds.count == 1
        assert ds.crs.to_epsg() == 3857


def test_materialize_strips_pending_keys():
    from databricks.labs.gbx.pyrx.core.open_tile import (
        PENDING_NODATA,
        materialize_to_bytes,
    )

    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999", pending_srid="3857")
    mat = materialize_to_bytes(vt)
    assert mat.raster is not None
    assert PENDING_NODATA not in (mat.metadata or {})
    assert "pending_srid" not in (mat.metadata or {})
    # and the bytes actually honor the instructions
    import io

    from rasterio.io import MemoryFile

    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 3857
