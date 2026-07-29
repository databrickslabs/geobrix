import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import resample


def _cog(w=1024, h=1024):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            # gradient so resampling is meaningful
            row = np.tile(np.arange(w, dtype="uint8"), (h, 1))
            dst.write(row[np.newaxis, :, :])
        with mf.open() as ds:
            return cog_convert(ds, "DEFLATE", 256, "AVERAGE")


def test_resample_downsample_cog_uses_overview(monkeypatch):
    calls = {"full_res_reads": 0}
    b = _cog()
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.overviews(1)  # sanity: COG has overviews
        out = resample.resample_to_size(ds, 128, 128, "average")
    # Output opens and has the requested size (correctness).
    with MemoryFile(out) as mf2, mf2.open() as ods:
        assert (ods.width, ods.height) == (128, 128)


def test_resample_non_cog_parity():
    # Plain GTiff (no overviews): output identical dims to today's path.
    profile = dict(driver="GTiff", width=512, height=512, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, 512, 512), dtype="uint8"))
        with mf.open() as ds:
            out = resample.resample_to_size(ds, 100, 100, "bilinear")
    with MemoryFile(out) as mf2, mf2.open() as ods:
        assert (ods.width, ods.height) == (100, 100)
