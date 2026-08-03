"""Tests for splitStrategy / layout-aware chunking in raster_gbx."""

import numpy as np
import rasterio

from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition
from databricks.labs.gbx.pyrx.core import cog


def _write_striped(path, w=4000, h=4000):
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.Affine.identity(),
        tiled=False,
    )  # striped
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.zeros((1, h, w), dtype="uint8"))


def test_default_auto_splits_large_striped(tmp_path):
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader(
        {"path": str(tmp_path), "splitStrategy": "serverless", "sizeInMB": "-1"}
    )  # sizeInMB unset-equivalent
    # Force a tiny budget via serverless? Instead assert >1 row for a raster
    # that exceeds the budget. Use a monkeypatched small budget:
    part = _FilePartition(
        str(p),
        size_mib=-1,
        budget_bytes=1024 * 1024,
        tile_format="auto",
        cog_blocksize=512,
        cog_overview_resampling="AVERAGE",
    )
    rows = list(r.read(part))
    assert len(rows) > 1  # auto-split kicked in


def test_none_strategy_single_row(tmp_path):
    p = tmp_path / "s.tif"
    _write_striped(str(p), 512, 512)
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "none"})
    part = _FilePartition(
        str(p),
        size_mib=-1,
        budget_bytes=0,
        tile_format="gtiff",
        cog_blocksize=512,
        cog_overview_resampling="AVERAGE",
    )
    rows = list(r.read(part))
    assert len(rows) == 1


def test_optin_split_emits_gtiff_not_cog(tmp_path):
    """Opt-in split via _FilePartition(budget_bytes>0) emits plain GTiff tiles.

    COG creation is now a writer concern (cog_gbx writer). The reader always
    emits plain GTiff on split — the old tileFormat=auto-cog behaviour is retired.
    """
    p = tmp_path / "big.tif"
    _write_striped(str(p))
    r = RasterGbxReader({"path": str(tmp_path), "splitStrategy": "serverless"})
    part = _FilePartition(
        str(p),
        size_mib=-1,
        budget_bytes=1024 * 1024,
    )
    rows = list(r.read(part))
    assert len(rows) > 1
    # Split tiles must be plain GTiff (not COG — COG is a writer concern).
    # read() now yields a raw v2 tile tuple in V2_TILE_SCHEMA order
    # (cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata);
    # metadata is the last field.
    _, tile = rows[0]
    md = tile[-1]
    assert md[cog.GBX_FORMAT] == "gtiff"


def test_options_default_resolution():
    r = RasterGbxReader({"path": "/x", "splitStrategy": "auto"})
    assert r.strategy in ("serverless", "classic")
    # tileFormat/cogBlockSize/cogOverviewResampling are reader options no longer;
    # the reader always emits plain GTiff. Only strategy is stored.
    assert not hasattr(r, "tile_format"), "tile_format removed from reader in 0.4.4+"


def test_gtiff_reader_inherits_options(tmp_path):
    from databricks.labs.gbx.ds.gtiff import GTiffGbxReader

    r = GTiffGbxReader({"path": str(tmp_path), "splitStrategy": "classic"})
    assert r.strategy == "classic"
    assert r.driver == "GTiff"
    # tileFormat no longer a reader attribute — COG is a writer concern.
    assert not hasattr(r, "tile_format"), "tile_format removed from reader in 0.4.4+"


def test_netcdf_reader_accepts_options(tmp_path):
    """NetcdfRasterReader constructs without error and propagates strategy."""
    from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader

    r = NetcdfRasterReader(
        {
            "path": str(tmp_path),
            "splitStrategy": "serverless",
        }
    )
    assert r.strategy == "serverless"
    # tileFormat/cogBlockSize/cogOverviewResampling are no longer reader options.
    assert not hasattr(r, "tile_format"), "tile_format removed from reader in 0.4.4+"
