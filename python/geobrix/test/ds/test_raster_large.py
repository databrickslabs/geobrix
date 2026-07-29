"""Tests for splitStrategy / tileFormat / layout-aware chunking in raster_gbx."""

import numpy as np
import rasterio
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition
from databricks.labs.gbx.pyrx.core import cog


def _write_striped(path, w=4000, h=4000):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity(),
                   tiled=False)  # striped
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.zeros((1, h, w), dtype="uint8"))


def test_default_auto_splits_large_striped(tmp_path):
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "serverless",
                         "sizeInMB": "-1"})  # sizeInMB unset-equivalent
    # Force a tiny budget via serverless? Instead assert >1 row for a raster
    # that exceeds the budget. Use a monkeypatched small budget:
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=1024 * 1024,
                          tile_format="auto", cog_blocksize=512,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) > 1  # auto-split kicked in


def test_none_strategy_single_row(tmp_path):
    p = tmp_path / "s.tif"
    _write_striped(str(p), 512, 512)
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "none"})
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=0,
                          tile_format="gtiff", cog_blocksize=512,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) == 1


def test_auto_tileformat_cog_when_split(tmp_path):
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader({"path": str(tmp_path)})
    part = _FilePartition(str(p), size_mib=-1, budget_bytes=1024 * 1024,
                          tile_format="auto", cog_blocksize=256,
                          cog_overview_resampling="AVERAGE")
    rows = list(r.read(part))
    assert len(rows) > 1
    # Split tiles under tileFormat=auto are COG.
    _, tile = rows[0]
    cellid, raster_bytes, md = tile
    assert md[cog.GBX_FORMAT] == "cog"


def test_options_default_resolution():
    r = RasterGbxReader({"path": "/x", "splitStrategy": "auto"})
    assert r.strategy in ("serverless", "classic")
    assert r.tile_format == "auto"


def test_gtiff_reader_inherits_options(tmp_path):
    from databricks.labs.gbx.ds.gtiff import GTiffGbxReader
    r = GTiffGbxReader({"path": str(tmp_path), "splitStrategy": "classic",
                        "tileFormat": "cog"})
    assert r.strategy == "classic"
    assert r.tile_format == "cog"
    assert r.driver == "GTiff"


def test_netcdf_reader_accepts_options(tmp_path):
    """NetcdfRasterReader constructs without error and propagates tile options."""
    from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
    r = NetcdfRasterReader({"path": str(tmp_path), "splitStrategy": "serverless",
                            "tileFormat": "cog", "cogBlockSize": "256",
                            "cogOverviewResampling": "NEAREST"})
    assert r.strategy == "serverless"
    assert r.tile_format == "cog"
    assert r.cog_blocksize == 256
    assert r.cog_overview_resampling == "NEAREST"
