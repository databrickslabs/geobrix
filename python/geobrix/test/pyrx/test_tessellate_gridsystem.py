"""The `gridSystem` key of a tessellate tile's metadata, for each grid and each assignment.

Each light tessellate table function is driven through a real PySpark session (local, no JAR), so the key is read from
the metadata map of the row it yields. The cell rasterizers' own tile (`_serde.build_tile` without a grid) has none.
"""

import pytest

from databricks.labs.gbx.pyrx import _serde

from . import _grid_defs as gd
from .conftest import make_geotiff_bytes

# grid -> (SQL arguments after the tile, the tile)
GRIDS = {
    "h3": ("5", lambda: gd.gtiff("EPSG:4326", (-0.12, 51.20, 0.20, 51.52), 32)),
    "quadbin": ("10", lambda: gd.gtiff("EPSG:4326", (-0.12, 51.20, 0.20, 51.52), 32)),
    "bng": ("3", lambda: gd.gtiff("EPSG:27700", (530000, 174000, 538000, 182000), 8)),
}


@pytest.mark.parametrize("mode", ["covering", "centroid"])
@pytest.mark.parametrize("grid", sorted(GRIDS))
def test_a_tessellate_tile_carries_the_grid_system_of_its_grid(spark, grid, mode):
    from pyspark.sql import functions as f

    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx.rasterx import functions as rx

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    res, build = GRIDS[grid]
    df = spark.createDataFrame([(bytearray(build()),)], ["raster"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView(f"_gs_{grid}_{mode}")
    rows = spark.sql(
        f"SELECT t.metadata FROM _gs_{grid}_{mode}, "
        f"LATERAL gbx_rst_{grid}_tessellate(tile, {res}, '{mode}') t"
    ).collect()
    assert rows
    for row in rows:
        assert dict(row["metadata"] or {}).get("gridSystem") == grid


def test_a_tile_built_without_a_grid_has_no_grid_system():
    # the cell rasterizers build their tile this way (cellid 0): the key is the tessellations' alone
    tile = _serde.build_tile(make_geotiff_bytes(), "GTiff", 0)
    assert "gridSystem" not in (tile["metadata"] or {})
