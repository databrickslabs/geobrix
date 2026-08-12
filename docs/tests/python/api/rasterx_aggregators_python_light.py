"""
Python code examples for the light (pyrx) tier of RasterX aggregator functions.
Single source of truth for the aggregators tab in docs/docs/api/raster-functions.mdx.

All aggregators reduce MULTIPLE tile rows into one result tile per group.  Examples
build a small multi-row DataFrame and aggregate via groupBy(...).agg(...) —
this is the correct DataFrame-native invocation (not raw SQL inside a Python tab).

The shown code invokes the *_agg function directly — no accessor wrapper (rst_format,
rst_numbands, etc.) hides the real return type.  All *_agg functions return a tile
v2 Tile (see docs/docs/api/tile-structure); the output
constants show representative tile-struct values.

Fixture: MULTI-TILE (rgb_nir_small.tif split into 3 per-band rows, same grid).
  multi_band_tiles_df(spark)  -> 3 rows: tile (1-band), band_index (1/2/3), region
  Used for: rst_combineavg_agg, rst_frombands_agg, rst_derivedband_agg, rst_merge_agg.

Rasterize family: synthesized cell-id / geometry / point rows per function.
  rst_rasterize_agg, rst_gridfrompoints_agg, rst_dtmfromgeoms_agg,
  rst_h3_rasterize_agg, rst_quadbin_rasterize_agg, rst_bng_rasterize_agg.
"""

try:
    from databricks.labs.gbx.pyrx import functions as rx
except ImportError:
    rx = None


# ---------------------------------------------------------------------------
# Shared helper -- multi-band per-tile DataFrame (3 aligned band tiles)
# ---------------------------------------------------------------------------


def _get_multi_band_tiles_df(spark):
    from _fixtures import multi_band_tiles_df  # noqa: PLC0415

    return multi_band_tiles_df(spark)


# ---------------------------------------------------------------------------
# rst_combineavg_agg -- per-pixel mean across aligned tiles (same grid/CRS)
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif, same grid)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_combineavg_agg_python_light_example(spark):
    """Average aligned raster tiles per group using the light pyrx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif split by rst_band.
    All 3 tiles share the same grid (extent/shape/CRS), satisfying combineavg_agg's
    alignment requirement. Grouped by region, producing 1 averaged tile row.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = _get_multi_band_tiles_df(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_combineavg_agg("tile").alias("avg_tile"))
        .first()
    )
    return result["avg_tile"]


rst_combineavg_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|avg_tile                                                   |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_derivedband_agg -- apply a Python pixel function across a group's tiles
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Pixel function: identity (returns band 0, i.e. the first input band)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_derivedband_agg_python_light_example(spark):
    """Apply a Python pixel function across a group's band tiles using the light pyrx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif.  Each tile
    contributes one band to the VRT; the pixel function selects the first band
    (identity). Grouped by region, producing 1 derived tile row.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    pyfunc = (
        "def fn(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0]\n"
    )
    df = _get_multi_band_tiles_df(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_derivedband_agg("tile", pyfunc, "fn").alias("derived"))
        .first()
    )
    return result["derived"]


rst_derivedband_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|derived                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_frombands_agg -- stack per-band tiles into one multi-band tile
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Stacks by ascending band_index (1->band1, 2->band2, 3->band3)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_frombands_agg_python_light_example(spark):
    """Stack per-band tiles into one multi-band tile per group using the light pyrx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif, each with
    band_index=1/2/3.  Grouped by region, stacks ascending by band_index,
    producing 1 three-band tile row.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = _get_multi_band_tiles_df(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_frombands_agg("tile", "band_index").alias("stacked"))
        .first()
    )
    return result["stacked"]


rst_frombands_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|stacked                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_merge_agg -- spatial mosaic of a group's tiles (union extent)
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif, same extent)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_merge_agg_python_light_example(spark):
    """Merge a group's raster tiles into one spatial mosaic using the light pyrx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif.  Each tile
    covers the same extent (all from the same source), so the merged result
    has the same bounding box as the inputs.  Grouped by region.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = _get_multi_band_tiles_df(spark)
    result = df.groupBy("region").agg(rx.rst_merge_agg("tile").alias("mosaic")).first()
    return result["mosaic"]


rst_merge_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|mosaic                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_rasterize_agg -- burn geometry/value rows into one tile per group
# Fixture: synthesized 3-row DataFrame of WKB polygon + value + extent constants
# A small 4x4 EPSG:4326 canvas; 3 rows with burn values 1.0/2.0/3.0
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_rasterize_agg_python_light_example(spark):
    """Burn geometry/value rows into one tile per group using the light pyrx tier.

    Multi-row fixture: 3 rows of (geom_wkb, burn_value) over a shared 4x4 extent
    in EPSG:4326.  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )  # noqa: PLC0415

    # WKB POLYGON((0 0, 4 0, 4 4, 0 4, 0 0)) in EPSG:4326
    poly = bytes.fromhex(
        "0103000000010000000500000000000000000000000000000000000000"
        "0000000000001040000000000000000000000000000010400000000000001040"
        "000000000000000000000000000010400000000000000000"
        "0000000000000000"
    )
    rows = [(poly, 1.0, "R1"), (poly, 2.0, "R1"), (poly, 3.0, "R1")]
    schema = StructType(
        [
            StructField("geom", BinaryType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_rasterize_agg(
                "geom",
                "value",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(4.0),
                f.lit(4.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("burned")
        )
        .first()
    )
    return result["burned"]


rst_rasterize_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|burned                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_gridfrompoints_agg -- IDW interpolation: one point/value per row -> one tile
# Fixture: synthesized 4 point rows, EPSG:4326 small extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_gridfrompoints_agg_python_light_example(spark):
    """IDW-interpolate point/value rows into one tile per group using the light pyrx tier.

    Multi-row fixture: 4 rows of (WKB point, observation) over a shared extent
    [0,0,1,1] EPSG:4326.  Grouped by region, producing 1 Float64 IDW tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )  # noqa: PLC0415

    def _wkb_point(x, y):
        """Build a WKB POINT from (x, y) as bytes."""
        import struct  # noqa: PLC0415

        return struct.pack("<bIdd", 1, 1, x, y)

    rows = [
        (_wkb_point(0.1, 0.1), 10.0, "R1"),
        (_wkb_point(0.9, 0.1), 20.0, "R1"),
        (_wkb_point(0.1, 0.9), 30.0, "R1"),
        (_wkb_point(0.9, 0.9), 40.0, "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("val", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_gridfrompoints_agg(
                "pt",
                "val",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("idw")
        )
        .first()
    )
    return result["idw"]


rst_gridfrompoints_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|idw                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_dtmfromgeoms_agg -- Delaunay TIN DTM: one Z-point per row -> one tile
# Fixture: synthesized 4 Z-valued WKB points, small EPSG:4326 extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_dtmfromgeoms_agg_python_light_example(spark):
    """Build a Delaunay TIN DTM from Z-valued points per group using the light pyrx tier.

    Multi-row fixture: 4 rows of WKB POINT Z with elevation values, over a
    [0,0,1,1] EPSG:4326 extent.  Grouped by region, producing 1 DTM tile via
    barycentric interpolation over an unconstrained Delaunay triangulation.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        StringType,
        StructField,
        StructType,
    )  # noqa: PLC0415

    def _wkb_point_z(x, y, z):
        """Build a WKB POINT Z (ISO wkbType 1001) as bytes."""
        import struct  # noqa: PLC0415

        return struct.pack("<bIddd", 1, 1001, x, y, z)

    rows = [
        (_wkb_point_z(0.1, 0.1, 100.0), "R1"),
        (_wkb_point_z(0.9, 0.1, 200.0), "R1"),
        (_wkb_point_z(0.1, 0.9, 150.0), "R1"),
        (_wkb_point_z(0.9, 0.9, 250.0), "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_dtmfromgeoms_agg(
                "pt",
                f.lit(None).cast("array<binary>"),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("dtm")
        )
        .first()
    )
    return result["dtm"]


rst_dtmfromgeoms_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|dtm                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_h3_rasterize_agg -- burn H3 cells into one tile per group
# Fixture: synthesized 3 H3 resolution-9 cell rows, burn values 1.0/2.0/3.0
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_h3_rasterize_agg_python_light_example(spark):
    """Rasterize H3 cell/value rows into one tile per group using the light pyrx tier.

    Multi-row fixture: 3 rows of (H3 cell id BIGINT, burn value) at resolution 9
    near lon/lat (0.01, 0.01).  Grouped by region, producing 1 rasterized tile.
    The extent is auto-derived from the cell set; kring_pad=1 (default).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    import h3  # noqa: PLC0415

    res = 9
    cell_strs = [
        h3.latlng_to_cell(0.01, 0.01, res),
        h3.latlng_to_cell(0.02, 0.01, res),
        h3.latlng_to_cell(0.01, 0.02, res),
    ]
    cells = [h3.str_to_int(c) for c in cell_strs]
    rows = [
        (int(cells[0]), 1.0, "R1"),
        (int(cells[1]), 2.0, "R1"),
        (int(cells[2]), 3.0, "R1"),
    ]
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )  # noqa: PLC0415

    schema = StructType(
        [
            StructField("cellid", LongType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(rx.rst_h3_rasterize_agg("cellid", "value").alias("tile"))
        .first()
    )
    return result["tile"]


rst_h3_rasterize_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_quadbin_rasterize_agg -- burn quadbin cells into one tile per group
# Fixture: synthesized 3 quadbin zoom-12 cell rows near central London
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_quadbin_rasterize_agg_python_light_example(spark):
    """Rasterize quadbin cell/value rows into one tile per group using the light pyrx tier.

    Multi-row fixture: 3 rows of (quadbin cell id BIGINT, burn value) at zoom 12
    near central London (lon/lat).  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qbx  # noqa: PLC0415

    qbx.register(spark)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _qb_cells AS
        SELECT region,
               gbx_quadbin_pointascell(cast(lon as double), cast(lat as double), 12) AS cellid,
               cast(val as double) AS value
        FROM (VALUES
            ('R1', -0.10, 51.50, 1.0),
            ('R1', -0.11, 51.51, 2.0),
            ('R1', -0.09, 51.49, 3.0)
        ) AS t(region, lon, lat, val)
    """)
    df = spark.table("_qb_cells")
    result = (
        df.groupBy("region")
        .agg(rx.rst_quadbin_rasterize_agg("cellid", "value").alias("tile"))
        .first()
    )
    spark.catalog.dropTempView("_qb_cells")
    return result["tile"]


rst_quadbin_rasterize_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_bng_rasterize_agg -- burn BNG cells into one tile per group
# Fixture: synthesized 3 BNG 1km cell STRING rows near central London
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_bng_rasterize_agg_python_light_example(spark):
    """Rasterize BNG cell/value rows into one tile per group using the light pyrx tier.

    Multi-row fixture: 3 rows of (BNG STRING cell id, burn value) at 1km resolution
    near central London (EPSG:27700 eastings/northings).  Grouped by region,
    producing 1 rasterized tile in EPSG:27700.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bngx  # noqa: PLC0415

    bngx.register(spark)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _bng_cells AS
        SELECT region,
               gbx_bng_eastnorthasbng(e, n, 3) AS cellid,
               val AS value
        FROM (VALUES
            ('R1', cast(530000.0 as double), cast(180000.0 as double), cast(1.0 as double)),
            ('R1', cast(531000.0 as double), cast(181000.0 as double), cast(2.0 as double)),
            ('R1', cast(529000.0 as double), cast(179000.0 as double), cast(3.0 as double))
        ) AS t(region, e, n, val)
    """)
    df = spark.table("_bng_cells")
    result = (
        df.groupBy("region")
        .agg(rx.rst_bng_rasterize_agg("cellid", "value").alias("tile"))
        .first()
    )
    spark.catalog.dropTempView("_bng_cells")
    return result["tile"]


rst_bng_rasterize_agg_python_light_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""
