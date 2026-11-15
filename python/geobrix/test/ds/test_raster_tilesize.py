"""tileSize grid emission: materialized per-cell bytes + virtual round-trip."""

from test._volume_safe import open_raster

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _write(tmp_path, width=512, height=512):
    p = str(tmp_path / "r.tif")
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.001, 0.001),
        nodata=-9999.0,
    )
    with open_raster(p, "w", **prof) as ds:
        ds.write(np.arange(width * height, dtype="float32").reshape(height, width), 1)
    return p


# (virtualTiles, overlapPercent) -> tiles. Materialized tiles carry their pixels, virtual ones none (they open
# from the window); either way each tile holds the source pixels of its window. A 25% overlap makes a 3 x 3 grid.
TILE_GRIDS = {
    "materialized": ("false", None, 4),
    "virtual": ("true", None, 4),
    "virtual_with_overlap": ("true", "25", 9),
}


@pytest.mark.parametrize(
    "virtual,overlap,n", TILE_GRIDS.values(), ids=TILE_GRIDS.keys()
)
def test_tile_size_grid(spark, tmp_path, virtual, overlap, n):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path, 512, 512)
    reader = (
        spark.read.format("raster_gbx")
        .option("tileSize", "256,256")
        .option("virtualTiles", virtual)
    )
    rows = (
        (reader.option("overlapPercent", overlap) if overlap else reader)
        .load(p)
        .collect()
    )
    assert len(rows) == n
    with rasterio.open(p) as ds:
        full = ds.read(1)
    for r in rows:
        w, tile = r["tile"]["window"], r["tile"]
        assert tile["clip_polygon"] is None and (
            w["width"] == 256 or overlap
        ), "edge tiles are cut only by an overlap grid"
        if virtual == "true":
            assert tile["raster"] is None
            with ot.open_tile(VirtualTile.from_row(tile)) as t:
                got = t.read(1)
        else:
            with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as t:
                got = t.read(1)
        want = full[
            w["row_off"] : w["row_off"] + w["height"],
            w["col_off"] : w["col_off"] + w["width"],
        ]
        assert np.array_equal(got, want)
