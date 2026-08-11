"""
Tests for coordinate-transform & tiling light-tier examples.
Each function is called on the single-band fixture and assertions verify the return values.
"""

from pyspark.sql import functions as f
from _fixtures import single_band_tile_df

try:
    from . import rasterx_transforms_python_light as transforms_examples
except (ModuleNotFoundError, ImportError):
    import rasterx_transforms_python_light as transforms_examples


def test_rst_rastertoworldcoord_python_light_example(spark):
    """Pixel (100, 80) → world (2122955.0, -10791275.0)."""
    coord = transforms_examples.rst_rastertoworldcoord_python_light_example(spark)
    assert coord is not None
    assert coord.x == 2122955.0 and coord.y == -10791275.0


def test_rst_rastertoworldcoordx_python_light_example(spark):
    """Pixel (100, 80) → world easting 2122955.0."""
    x = transforms_examples.rst_rastertoworldcoordx_python_light_example(spark)
    assert x == 2122955.0


def test_rst_rastertoworldcoordy_python_light_example(spark):
    """Pixel (100, 80) → world northing -10791275.0."""
    y = transforms_examples.rst_rastertoworldcoordy_python_light_example(spark)
    assert y == -10791275.0


def test_rst_worldtorastercoord_python_light_example(spark):
    """World (2122955, -10791275) → pixel (100, 80) — inverse round-trip."""
    coord = transforms_examples.rst_worldtorastercoord_python_light_example(spark)
    assert coord is not None
    assert coord.x == 100 and coord.y == 80


def test_rst_worldtorastercoordx_python_light_example(spark):
    """World (2122955, -10791275) → pixel column 100."""
    col = transforms_examples.rst_worldtorastercoordx_python_light_example(spark)
    assert col == 100


def test_rst_worldtorastercoordy_python_light_example(spark):
    """World (2122955, -10791275) → pixel row 80."""
    row = transforms_examples.rst_worldtorastercoordy_python_light_example(spark)
    assert row == 80


def test_rst_to_webmercator_python_light_example(spark):
    """Test reprojection to Web Mercator."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    df = single_band_tile_df(spark)
    result = df.select(rx.rst_to_webmercator("tile").alias("tile")).first()
    tile = result["tile"]
    assert tile is not None
    # Light tiles have raster bytes populated
    assert tile[0] == 0  # band count
    assert tile[1] is not None  # raster bytes


def test_rst_tilexyz_python_light_example(spark):
    """rst_tilexyz renders a single XYZ tile as PNG bytes (never null)."""
    result = transforms_examples.rst_tilexyz_python_light_example(spark)
    assert result is not None
    assert len(bytes(result)) > 0


def test_rst_xyzpyramid_python_light_example(spark):
    """rst_xyzpyramid (UDTF via LATERAL) yields one row per XYZ tile."""
    rows = transforms_examples.rst_xyzpyramid_python_light_example(spark)
    assert isinstance(rows, list) and len(rows) >= 1
    assert "bytes" in rows[0].asDict()


def test_rst_h3_tessellate_python_light_example(spark):
    """rst_h3_tessellate (UDTF via LATERAL) yields one v2-Tile row per H3 cell."""
    rows = transforms_examples.rst_h3_tessellate_python_light_example(spark)
    assert isinstance(rows, list) and len(rows) >= 1
    assert "cellid" in rows[0].asDict() and rows[0]["raster"] is not None


def test_rst_bng_tessellate_python_light_example(spark):
    """rst_bng_tessellate (UDTF via LATERAL) — no cells for a non-GB raster."""
    rows = transforms_examples.rst_bng_tessellate_python_light_example(spark)
    # NYC-area sample lies outside Great Britain, so BNG tessellation is empty.
    assert isinstance(rows, list) and len(rows) == 0


def test_rst_quadbin_tessellate_python_light_example(spark):
    """rst_quadbin_tessellate (UDTF via LATERAL) yields one row per quadbin cell."""
    rows = transforms_examples.rst_quadbin_tessellate_python_light_example(spark)
    assert isinstance(rows, list) and len(rows) >= 1
    assert "cellid" in rows[0].asDict() and rows[0]["raster"] is not None
