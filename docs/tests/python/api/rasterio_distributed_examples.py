"""'Rasterio Distributed' page examples — single source of truth.

Code shown in docs/docs/api/rasterio-distributed.mdx is imported from here.
Each flagship op is shown as a familiar single-node rasterio snippet next to the
distributed pyrx equivalent. The paired verifier functions run BOTH sides and the
test asserts they agree on tiny synthesized rasters — proving the equivalence the
page claims, not merely that pyrx runs.
"""

# ---------------------------------------------------------------------------
# Snippet constants (verbatim blocks on the docs page)
# ---------------------------------------------------------------------------

REGISTER = """# Register the GeoBrix lightweight (pyrx) functions once per session
import databricks.labs.gbx.pyrx.functions as rx
import databricks.labs.gbx.ds.register as gbx_readers
gbx_readers.register(spark)  # raster_gbx and other lightweight data sources
rx.register(spark)            # gbx_rst_* SQL functions"""

WARP_RASTERIO = """# Single-node rasterio: reproject one file to EPSG:3857
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
with rasterio.open("in.tif") as src:
    t, w, h = calculate_default_transform(
        src.crs, "EPSG:3857", src.width, src.height, *src.bounds)
    # ...write a reprojected file, one machine, one file at a time"""

WARP_PYRX = """# GeoBrix: reproject a whole DataFrame of tiles, distributed
from databricks.labs.gbx.rasterx import functions as rx
df2 = df.withColumn("tile", rx.rst_transform("tile", 3857))
# rst_transform runs rasterio.warp on each tile in an Arrow UDF across the cluster"""

CLIP_RASTERIO = """# Single-node rasterio: clip one raster to a geometry
import rasterio
from rasterio.mask import mask as rio_mask
with rasterio.open("in.tif") as src:
    out, out_transform = rio_mask(src, [geom], crop=True)"""

CLIP_PYRX = """# GeoBrix: clip every tile to a geometry, distributed
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql import functions as f
# clip_geom column holds WKB/WKT geometry; third arg controls border pixel inclusion
df2 = df.withColumn("tile", rx.rst_clip("tile", "clip_geom", f.lit(False)))"""

NDVI_RASTERIO = """# Single-node rasterio + NumPy: NDVI for one raster (band1=red, band2=nir)
import rasterio, numpy as np
with rasterio.open("in.tif") as src:
    red = src.read(1).astype("float32")
    nir = src.read(2).astype("float32")
    ndvi = (nir - red) / (nir + red)"""

NDVI_PYRX = """# GeoBrix: NDVI across a DataFrame of tiles, distributed
from databricks.labs.gbx.rasterx import functions as rx
# rst_ndvi(tile, red_idx, nir_idx) — band indices are 1-based
df2 = df.withColumn("tile", rx.rst_ndvi("tile", 1, 2))"""


# ---------------------------------------------------------------------------
# Internal helpers (not page snippets)
# ---------------------------------------------------------------------------

def _register(spark):
    """Register the lightweight data sources and pyrx SQL functions."""
    from databricks.labs.gbx import manager as gbx_manager
    gbx_manager.register_sources(spark)
    gbx_manager.register_family(spark, ('gbx_rst_', 'gbx_pmtiles_'), level="L3")


def _one_tile_df(spark, src_path):
    """Load a single-file raster as a one-row (source, tile) DataFrame via the
    lightweight raster_gbx reader (materialized — pyrx UDFs require raster bytes)."""
    return spark.read.format("raster_gbx").option("virtualTiles", "false").load(src_path)


# ---------------------------------------------------------------------------
# Verifier functions — run both sides, return results for the test to compare
# ---------------------------------------------------------------------------

def warp_both(spark, src_path):
    """Reproject the raster to EPSG:3857 on both sides; return a result dict.

    rasterio side: ``calculate_default_transform`` computes the target grid
    (width, height, affine transform) — real warp math, not a constant.

    pyrx side: ``rst_transform("tile", 3857)`` runs the distributed Arrow UDF;
    the result tile is read back via MemoryFile to extract its CRS (EPSG),
    pixel dimensions, and geographic bounds.

    Dict keys:
        rio_epsg (int): always 3857 — the CRS rasterio was asked to target.
        rio_w, rio_h (int): target grid dimensions from calculate_default_transform.
        rio_bounds (tuple): (west, south, east, north) in EPSG:3857 metres.
        pyrx_epsg (int): EPSG reported by the transformed tile's CRS.
        pyrx_w, pyrx_h (int): pixel dimensions of the transformed tile.
        pyrx_bounds (tuple): (left, bottom, right, top) of the transformed tile.
    """
    import rasterio
    from rasterio.warp import calculate_default_transform
    from rasterio.transform import array_bounds
    from rasterio.io import MemoryFile
    from pyspark.sql import functions as F
    from databricks.labs.gbx.rasterx import functions as rx_api

    _register(spark)

    # rasterio side: compute the target grid for an EPSG:3857 reprojection.
    # calculate_default_transform does real warp math — this is the same function
    # a single-node pipeline would call before writing the output file.
    with rasterio.open(src_path) as src:
        t, rio_w, rio_h = calculate_default_transform(
            src.crs, "EPSG:3857", src.width, src.height, *src.bounds
        )
    rio_epsg = 3857
    # Convert the affine transform + dimensions to geographic bounds (W, S, E, N).
    rio_bounds = array_bounds(rio_h, rio_w, t)

    # pyrx side: distributed Arrow UDF; collect the result tile in one Spark action
    df = (
        _one_tile_df(spark, src_path)
        .withColumn("tile", rx_api.rst_transform("tile", 3857))
    )
    tile_bytes = bytes(
        df.select(F.col("tile.raster").alias("r")).first()["r"]
    )

    # Read the reprojected tile back to extract CRS, dimensions, and bounds.
    with MemoryFile(tile_bytes) as mf, mf.open() as ds:
        pyrx_epsg = ds.crs.to_epsg()
        pyrx_w = ds.width
        pyrx_h = ds.height
        b = ds.bounds
        pyrx_bounds = (b.left, b.bottom, b.right, b.top)

    return {
        "rio_epsg": rio_epsg,
        "rio_w": rio_w,
        "rio_h": rio_h,
        "rio_bounds": rio_bounds,
        "pyrx_epsg": pyrx_epsg,
        "pyrx_w": pyrx_w,
        "pyrx_h": pyrx_h,
        "pyrx_bounds": pyrx_bounds,
    }


def ndvi_both(spark, src_path):
    """Return (rasterio_ndvi_array, pyrx_ndvi_array) for a 2-band raster
    where band1=red, band2=nir.

    Both sides compute (nir - red) / (nir + red).  The test asserts
    np.allclose on the two float32 arrays.
    """
    import numpy as np
    import rasterio
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.rasterx import functions as rx_api

    _register(spark)

    # rasterio side: single-node
    with rasterio.open(src_path) as src:
        red = src.read(1).astype("float32")
        nir = src.read(2).astype("float32")
        with np.errstate(divide="ignore", invalid="ignore"):
            rio_ndvi = (nir - red) / (nir + red)

    # pyrx side: distributed UDF, read result tile back as NumPy
    # rst_ndvi(tile, red_band, nir_band): band1=red, band2=nir
    df = (
        _one_tile_df(spark, src_path)
        .withColumn("tile", rx_api.rst_ndvi("tile", 1, 2))
    )
    tile_bytes = bytes(
        df.selectExpr("tile.raster AS r").collect()[0]["r"]
    )
    with MemoryFile(tile_bytes) as mf, mf.open() as ds:
        pyrx_ndvi = ds.read(1).astype("float32")

    return (rio_ndvi, pyrx_ndvi)
