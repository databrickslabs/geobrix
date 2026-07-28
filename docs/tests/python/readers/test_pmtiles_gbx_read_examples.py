"""Executes the pmtiles_gbx reader doc examples against synthesized data (Docker).

Mirrors python/geobrix/test/ds/test_pmtiles_reader.py: synthesize a small COG,
tile it via source="raster", write the tiles to a single .pmtiles archive, then
read them back via source="archive".
"""

import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pmtiles_gbx_read_examples as ex  # noqa: E402

_BBOX = "-122.50,37.74,-122.40,37.79"


def _write_cog(path, w, s, e, n, px=256, val=200):
    data = np.full((3, px, px), val, dtype="uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=px,
        height=px,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(w, s, e, n, px, px),
    ) as ds:
        ds.write(data)


def test_read_raster(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    df = ex.read_raster(spark, str(tmp_path), _BBOX, 14, 16)
    assert df.count() > 0


def test_raster_write_back_and_read_archive(spark, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _write_cog(str(src / "a.tif"), -122.50, 37.74, -122.40, 37.79, val=180)
    # raster source -> tiles
    tiles = ex.read_raster(spark, str(src), _BBOX, 14, 15)
    n_tiles = tiles.count()
    # WRITE_BACK: feed the (z, x, y, bytes) rows straight into the writer.
    out = str(tmp_path / "sf.pmtiles")
    tiles.write.format("pmtiles_gbx").option("shardZoom", "0").mode(
        "overwrite"
    ).save(out)
    # READ_ARCHIVE: read the archive back; same schema, same tile count.
    back = ex.read_archive(spark, out)
    assert back.count() == n_tiles
