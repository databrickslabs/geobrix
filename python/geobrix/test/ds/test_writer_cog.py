import numpy as np
import rasterio
from rasterio.io import MemoryFile
from databricks.labs.gbx.ds.writer import RasterGbxWriter
from databricks.labs.gbx.ds.raster import reader_schema
from databricks.labs.gbx.pyrx.core import cog


def _plain_gtiff(w=512, h=512):
    profile = dict(driver="GTiff", width=w, height=h, count=1, dtype="uint8",
                   crs="EPSG:4326", transform=rasterio.Affine.identity())
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, h, w), dtype="uint8"))
        return mf.read()


class _Row(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


def test_writer_cog_true_converts_plain_gtiff(tmp_path):
    w = RasterGbxWriter(str(tmp_path), reader_schema(), overwrite=True,
                        cog=True, cog_blocksize=256)
    b = _plain_gtiff()
    row = {"source": "x", "tile": {"cellid": -1, "raster": b,
                                   "metadata": {"driver": "GTiff"}}}
    w.write(iter([row]))
    import glob, os
    out = glob.glob(os.path.join(str(tmp_path), "*.tif"))[0]
    with open(out, "rb") as fh:
        assert cog.sniff_header(fh.read()).is_cog is True


def test_writer_cog_passthrough_already_cog(tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert
    with MemoryFile(_plain_gtiff()) as mf, mf.open() as ds:
        cb = cog_convert(ds, "DEFLATE", 256, "AVERAGE")
    w = RasterGbxWriter(str(tmp_path), reader_schema(), overwrite=True, cog=True)
    row = {"source": "x", "tile": {"cellid": -1, "raster": cb,
                                   "metadata": {cog.GBX_FORMAT: "cog"}}}
    w.write(iter([row]))
    import glob, os
    out = glob.glob(os.path.join(str(tmp_path), "*.tif"))[0]
    with open(out, "rb") as fh:
        assert cog.sniff_header(fh.read()).is_cog is True
