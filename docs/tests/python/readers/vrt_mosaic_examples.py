"""VRT-mosaic doc examples — single source of truth for docs/docs/api/vrt-mosaic.mdx.

Code shown on the VRT Mosaics page is imported from here via raw-loader. Tests
exercise the mosaic lifecycle end to end against real sample data:

  file_gbx  ->  cog_gbx writer (vrtMosaic=true)  ->  mosaic.vrt
  mosaic.vrt  ->  raster_gbx reader  ->  one virtual tile row per member
  member tiles  ->  mint_vrt  ->  windowed rasterio read

Light tier (pure Python, no JAR): readers register via ds.register.register,
never rasterx.register.
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_SINGLE = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_RASTER_LONDON = f"{SAMPLE_DATA_BASE}/london/sentinel2/london_sentinel2_red.tif"

# ---------------------------------------------------------------------------
# Display constants (payload rendered in docs via raw-loader)
# ---------------------------------------------------------------------------

VRT_PREPARE = r"""from databricks.labs.gbx.ds.register import register
register(spark)

# file_gbx gives one path-reference row per source file
sources = spark.read.format("file_gbx").load("/Volumes/catalog/schema/volume/raw/")

(
    sources
    .write.format("cog_gbx")
    .option("vrtMosaic", "true")   # activate mosaic mode
    .option("tileSize", "1024")    # tile edge in pixels (default 1024)
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/mosaic/")
)"""

VRT_READ_EXPAND = r"""from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx.functions import rst_avg
from pyspark.sql.functions import col

register(spark)

# Load the VRT: one whole-file virtual tile per member mini-COG
df = spark.read.format("raster_gbx").load("/Volumes/catalog/schema/vol/mosaic/mosaic.vrt")

# Apply rst_* per tile — exactly as you would for a directory of flat files
result = df.select(
    col("tile.path").alias("member"),
    rst_avg(col("tile")).alias("avg"),
)"""

VRT_QUADBIN = r"""from databricks.labs.gbx.ds.register import register
from pyspark.sql.functions import col

register(spark)

# Write a quadbin mosaic: each source is reprojected to EPSG:3857 and split
# into one mini-COG per overlapping quadbin cell at the chosen resolution.
sources = spark.read.format("file_gbx").load("/Volumes/catalog/schema/volume/raw/")
(
    sources
    .write.format("cog_gbx")
    .option("gridSystem", "quadbin")   # cell-aligned to the quadbin grid
    .option("gridResolution", "7")     # quadbin resolution 0–20
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/mosaic_qb/")
)

# Read back: one virtual tile per quadbin cell
df = spark.read.format("raster_gbx").load(
    "/Volumes/catalog/schema/volume/mosaic_qb/mosaic.vrt"
)

# tile.metadata carries the quadbin cell id and grid system tag
result = df.select(
    col("tile.path").alias("member"),
    col("tile.metadata")["cellid"].alias("cellid"),
    col("tile.metadata")["gridSystem"].alias("gridSystem"),
)"""

VRT_H3 = r"""from databricks.labs.gbx.ds.register import register
from pyspark.sql.functions import col

register(spark)

# Write an h3 mosaic: each source is reprojected to EPSG:4326, clipped to
# the hexagon boundary, and tagged with its h3 cell id (GBX_CELLID).
sources = spark.read.format("file_gbx").load("/Volumes/catalog/schema/volume/raw/")
(
    sources
    .write.format("cog_gbx")
    .option("gridSystem", "h3")      # cell-aligned to the h3 grid
    .option("gridResolution", "6")   # h3 resolution 0-15
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/mosaic_h3/")
)

# Read back: one virtual tile per h3 cell
df = spark.read.format("raster_gbx").load(
    "/Volumes/catalog/schema/volume/mosaic_h3/mosaic.vrt"
)

# tile.metadata carries the h3 cell id and grid system tag
raster_cells = df.select(
    col("tile.path").alias("member"),
    col("tile.metadata")["cellid"].alias("cellid"),
    col("tile.metadata")["gridSystem"].alias("gridSystem"),
)

# Equi-join: any h3-indexed analytics table joins on cellid —
# the cellid is a plain string key, compatible with h3.str_to_int and similar.
analytics = spark.table("catalog.schema.h3_metrics")  # h3-indexed DataFrame
result = raster_cells.join(analytics, on="cellid", how="inner")"""

VRT_BNG = r"""from databricks.labs.gbx.ds.register import register
from pyspark.sql.functions import col

register(spark)

# Write a BNG mosaic: each source is reprojected to EPSG:27700 and split into
# one mini-COG per overlapping BNG cell at the chosen resolution.
# BNG is valid over Great Britain only (EPSG:27700 extent).
sources = spark.read.format("file_gbx").load("/Volumes/catalog/schema/volume/raw/")
(
    sources
    .write.format("cog_gbx")
    .option("gridSystem", "bng")      # cell-aligned to the BNG grid (EPSG:27700)
    .option("gridResolution", "1km")  # BNG: integer index ±1..±6 or string key
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/mosaic_bng/")
)

# Read back: one virtual tile per BNG cell
df = spark.read.format("raster_gbx").load(
    "/Volumes/catalog/schema/volume/mosaic_bng/mosaic.vrt"
)

# tile.metadata carries the BNG cell id and grid system tag
raster_cells = df.select(
    col("tile.path").alias("member"),
    col("tile.metadata")["cellid"].alias("cellid"),
    col("tile.metadata")["gridSystem"].alias("gridSystem"),
)

# Equi-join: any BNG-indexed analytics table joins on cellid —
# the cellid is a standard BNG string (e.g. "SU1234"), compatible with
# gbx_bng_* functions and any BNG-indexed dataset.
analytics = spark.table("catalog.schema.bng_metrics")  # BNG-indexed DataFrame
result = raster_cells.join(analytics, on="cellid", how="inner")"""

VRT_MINT = r"""from databricks.labs.gbx.ds._mosaic import mint_vrt

# Build a transient VRT over an explicit tile list
tile_paths = [
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_0_0.tif",
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_0_1.tif",
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_1_0.tif",
]
vrt_path = mint_vrt(tile_paths)

# Open the VRT with rasterio for a windowed read across the mosaic
import rasterio
from rasterio.windows import Window

viewport = Window(col_off=400, row_off=200, width=600, height=400)
with rasterio.open(vrt_path) as vrt_ds:
    data = vrt_ds.read(window=viewport)
    # Only the tiles that intersect the viewport are read"""

# ---------------------------------------------------------------------------
# Test functions (real assertions against real sample data)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def _write_mosaic(spark, src, out_dir, tile_size=128, prune_empty=None):
    """Write a VRT mosaic and return (sorted member tile paths, mosaic.vrt path)."""
    writer = (
        spark.read.format("file_gbx")
        .load(src)
        .write.format("cog_gbx")
        .option("vrtMosaic", "true")
        .option("tileSize", str(tile_size))
    )
    if prune_empty is not None:
        writer = writer.option("pruneEmpty", "true" if prune_empty else "false")
    writer.mode("overwrite").save(out_dir)

    tiles = sorted(
        os.path.join(out_dir, f)
        for f in os.listdir(out_dir)
        if f.startswith("tile_") and f.lower().endswith(".tif")
    )
    return tiles, os.path.join(out_dir, "mosaic.vrt")


def vrt_prepare(spark, src_path=None):
    """cog_gbx mosaic mode splits a source into mini-COGs + a mosaic.vrt index."""
    import rasterio

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    refs = spark.read.format("file_gbx").load(src)
    assert refs.count() >= 1, "file_gbx must find the source raster"

    with tempfile.TemporaryDirectory() as out_dir:
        # A small tileSize forces the single sample raster to split into a grid
        # of mini-COGs; pruneEmpty=false keeps every cell so the VRT
        # reconstitutes the full source extent.
        tiles, mosaic_vrt = _write_mosaic(
            spark, src, out_dir, tile_size=128, prune_empty=False
        )

        assert os.path.exists(mosaic_vrt), "mosaic mode must write mosaic.vrt"
        assert len(tiles) > 1, (
            f"tileSize=128 over the sample raster must yield >1 mini-COG, got {len(tiles)}"
        )

        with rasterio.open(src) as src_ds:
            src_w, src_h, src_crs = src_ds.width, src_ds.height, src_ds.crs
        with rasterio.open(mosaic_vrt) as vrt_ds:
            # The index opens in plain GDAL/rasterio and covers the full source.
            assert abs(vrt_ds.width - src_w) <= 1, (vrt_ds.width, src_w)
            assert abs(vrt_ds.height - src_h) <= 1, (vrt_ds.height, src_h)
            assert vrt_ds.crs == src_crs, (vrt_ds.crs, src_crs)
    return True


def vrt_read_expand(spark, src_path=None):
    """raster_gbx expands mosaic.vrt into one virtual tile row per member mini-COG."""
    from pyspark.sql.functions import col

    from databricks.labs.gbx.pyrx.functions import rst_avg

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        tiles, mosaic_vrt = _write_mosaic(spark, src, out_dir, tile_size=128)
        assert len(tiles) > 1, "expected a multi-member mosaic to expand"

        df = spark.read.format("raster_gbx").load(mosaic_vrt)
        # One row per member mini-COG.
        assert df.count() == len(tiles), (df.count(), len(tiles))

        # rst_* run per tile; each retained member has data, so avg is non-null.
        result = df.select(
            col("tile.path").alias("member"),
            rst_avg(col("tile")).alias("avg"),
        ).collect()
        assert len(result) == len(tiles)
        for row in result:
            assert row["member"] is not None, "member path must be set on a virtual tile"
            assert row["avg"] is not None, f"rst_avg returned null for {row['member']}"
    return df


def vrt_quadbin_mosaic(spark, src_path=None):
    """cog_gbx quadbin mode writes cell-aligned mini-COGs in EPSG:3857 tagged with cell id."""
    import rasterio
    from pyspark.sql.functions import col

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        # quadbin resolution 7 produces a small number of cells over the NYC sample.
        (
            spark.read.format("file_gbx")
            .load(src)
            .write.format("cog_gbx")
            .option("gridSystem", "quadbin")
            .option("gridResolution", "7")
            .mode("overwrite")
            .save(out_dir)
        )

        mosaic_vrt = os.path.join(out_dir, "mosaic.vrt")
        assert os.path.exists(mosaic_vrt), "quadbin mosaic mode must write mosaic.vrt"

        cell_tiles = sorted(
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.startswith("cell_") and f.lower().endswith(".tif")
        )
        assert len(cell_tiles) >= 1, (
            f"quadbin mosaic must produce at least one cell mini-COG, got {len(cell_tiles)}"
        )

        # Each cell is reprojected to EPSG:3857.
        with rasterio.open(cell_tiles[0]) as ds:
            assert ds.crs.to_epsg() == 3857, (
                f"quadbin cell must be in EPSG:3857, got {ds.crs}"
            )

        # raster_gbx expands the VRT into one virtual tile row per quadbin cell.
        df = spark.read.format("raster_gbx").load(mosaic_vrt)
        assert df.count() == len(cell_tiles), (df.count(), len(cell_tiles))

        # tile.metadata carries cellid (quadbin cell id as string) and gridSystem.
        rows = df.select(
            col("tile.path").alias("path"),
            col("tile.metadata")["cellid"].alias("cellid"),
            col("tile.metadata")["gridSystem"].alias("grid_system"),
        ).collect()

        for row in rows:
            assert row["cellid"] is not None, (
                "cellid must be set in tile.metadata for quadbin cells"
            )
            assert row["grid_system"] == "quadbin", (
                f"gridSystem must be 'quadbin' in tile.metadata, got {row['grid_system']!r}"
            )

    return True


def vrt_h3_mosaic(spark, src_path=None):
    """cog_gbx h3 mode writes cell-aligned mini-COGs in EPSG:4326 tagged with h3 cell id."""
    import rasterio
    from pyspark.sql.functions import col

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        # h3 resolution 6 produces a small number of cells over the NYC sample.
        (
            spark.read.format("file_gbx")
            .load(src)
            .write.format("cog_gbx")
            .option("gridSystem", "h3")
            .option("gridResolution", "6")
            .mode("overwrite")
            .save(out_dir)
        )

        mosaic_vrt = os.path.join(out_dir, "mosaic.vrt")
        assert os.path.exists(mosaic_vrt), "h3 mosaic mode must write mosaic.vrt"

        cell_tiles = sorted(
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.startswith("cell_") and f.lower().endswith(".tif")
        )
        assert len(cell_tiles) >= 1, (
            f"h3 mosaic must produce at least one cell mini-COG, got {len(cell_tiles)}"
        )

        # Each cell is reprojected to EPSG:4326.
        with rasterio.open(cell_tiles[0]) as ds:
            assert ds.crs.to_epsg() == 4326, (
                f"h3 cell must be in EPSG:4326, got {ds.crs}"
            )

        # raster_gbx expands the VRT into one virtual tile row per h3 cell.
        df = spark.read.format("raster_gbx").load(mosaic_vrt)
        assert df.count() == len(cell_tiles), (df.count(), len(cell_tiles))

        # tile.metadata carries cellid (h3index string) and gridSystem.
        rows = df.select(
            col("tile.path").alias("path"),
            col("tile.metadata")["cellid"].alias("cellid"),
            col("tile.metadata")["gridSystem"].alias("grid_system"),
        ).collect()

        for row in rows:
            assert row["cellid"] is not None, (
                "cellid must be set in tile.metadata for h3 cells"
            )
            assert row["grid_system"] == "h3", (
                f"gridSystem must be 'h3' in tile.metadata, got {row['grid_system']!r}"
            )

        # Equi-join: a synthetic h3-indexed DataFrame joins to raster cells on cellid.
        # Each raster cell gets a matching row in the analytics table; the join on
        # cellid demonstrates unification of raster and tabular data by h3 cell id.
        cellids = [row["cellid"] for row in rows]
        analytics = spark.createDataFrame(
            [(cid, float(i)) for i, cid in enumerate(cellids)],
            ["cellid", "value"],
        )
        raster_cells = df.select(
            col("tile.path").alias("member"),
            col("tile.metadata")["cellid"].alias("cellid"),
            col("tile.metadata")["gridSystem"].alias("gridSystem"),
        )
        result = raster_cells.join(analytics, on="cellid", how="inner")
        result_rows = result.collect()
        assert len(result_rows) >= 1, (
            "equi-join on cellid must produce at least one row"
        )
        for r in result_rows:
            assert r["gridSystem"] == "h3", (
                f"joined row must carry gridSystem='h3', got {r['gridSystem']!r}"
            )

    return True


def _write_gb_source(out_dir: str) -> str:
    """Create a 200×200, 100 m/px, EPSG:27700 synthetic raster near London.

    Upper-left at E=530 000, N=180 000; spans 20 km × 20 km — four 10-km BNG
    cells and sixteen 1-km BNG cells at most. Always within the GB EPSG:27700
    envelope. BNG mosaic tests use this helper to avoid depending on a
    projection chain from an external CRS (e.g. EPSG:32630) which requires
    datum-shift grids that may not be present in the test environment.
    """
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    src_path = os.path.join(out_dir, "src_bng", "source_gb.tif")
    os.makedirs(os.path.dirname(src_path), exist_ok=True)
    w, h = 200, 200
    data = np.arange(w * h, dtype=np.uint16).reshape(1, h, w) % 60000
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint16",
        crs="EPSG:27700",
        transform=from_origin(530000.0, 180000.0, 100.0, 100.0),
    )
    with rasterio.open(src_path, "w", **profile) as ds:
        ds.write(data)
    return src_path


def vrt_bng_mosaic(spark, src_path=None):
    """cog_gbx bng mode writes cell-aligned mini-COGs in EPSG:27700 tagged with BNG cell id."""
    import rasterio
    from pyspark.sql.functions import col

    _register(spark)

    with tempfile.TemporaryDirectory() as tmp_root:
        # BNG requires a GB-envelope source.  Use a synthetic EPSG:27700 raster
        # near London (E=530 000, N=180 000, 20 km × 20 km) so the test does not
        # depend on datum-shift grids for an external CRS transform.
        src = src_path or _write_gb_source(tmp_root)
        out_dir = os.path.join(tmp_root, "mosaic_bng")
        os.makedirs(out_dir, exist_ok=True)

        # BNG resolution "1km" (index 3) produces ~16 cells over the 20 km source.
        (
            spark.read.format("file_gbx")
            .load(src)
            .write.format("cog_gbx")
            .option("gridSystem", "bng")
            .option("gridResolution", "1km")
            .mode("overwrite")
            .save(out_dir)
        )

        mosaic_vrt = os.path.join(out_dir, "mosaic.vrt")
        assert os.path.exists(mosaic_vrt), "bng mosaic mode must write mosaic.vrt"

        cell_tiles = sorted(
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.startswith("cell_") and f.lower().endswith(".tif")
        )
        assert len(cell_tiles) >= 1, (
            f"bng mosaic must produce at least one cell mini-COG, got {len(cell_tiles)}"
        )

        # Each cell is reprojected to EPSG:27700.
        with rasterio.open(cell_tiles[0]) as ds:
            assert ds.crs.to_epsg() == 27700, (
                f"bng cell must be in EPSG:27700, got {ds.crs}"
            )

        # raster_gbx expands the VRT into one virtual tile row per BNG cell.
        df = spark.read.format("raster_gbx").load(mosaic_vrt)
        assert df.count() == len(cell_tiles), (df.count(), len(cell_tiles))

        # tile.metadata carries cellid (BNG string, e.g. "TQ28") and gridSystem.
        rows = df.select(
            col("tile.path").alias("path"),
            col("tile.metadata")["cellid"].alias("cellid"),
            col("tile.metadata")["gridSystem"].alias("grid_system"),
        ).collect()

        for row in rows:
            assert row["cellid"] is not None, (
                "cellid must be set in tile.metadata for bng cells"
            )
            assert row["grid_system"] == "bng", (
                f"gridSystem must be 'bng' in tile.metadata, got {row['grid_system']!r}"
            )

        # Equi-join: a synthetic BNG-indexed DataFrame joins to raster cells on cellid.
        # Each raster cell gets a matching row in the analytics table; the join on
        # cellid demonstrates unification of raster and tabular data by BNG cell id.
        cellids = [row["cellid"] for row in rows]
        analytics = spark.createDataFrame(
            [(cid, float(i)) for i, cid in enumerate(cellids)],
            ["cellid", "value"],
        )
        raster_cells = df.select(
            col("tile.path").alias("member"),
            col("tile.metadata")["cellid"].alias("cellid"),
            col("tile.metadata")["gridSystem"].alias("gridSystem"),
        )
        result = raster_cells.join(analytics, on="cellid", how="inner")
        result_rows = result.collect()
        assert len(result_rows) >= 1, (
            "equi-join on cellid must produce at least one row"
        )
        for r in result_rows:
            assert r["gridSystem"] == "bng", (
                f"joined row must carry gridSystem='bng', got {r['gridSystem']!r}"
            )

    return True


def vrt_mint_windowed(spark, src_path=None):
    """mint_vrt builds a transient VRT over member tiles for a windowed rasterio read."""
    import rasterio
    from rasterio.windows import Window

    from databricks.labs.gbx.ds._mosaic import mint_vrt, minted_vrt

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        tiles, _ = _write_mosaic(spark, src, out_dir, tile_size=128)
        assert len(tiles) > 1, "expected >1 member tile to mint a VRT over"

        # Primary example: mint a transient VRT over an explicit tile list.
        vrt_path = mint_vrt(tiles)
        try:
            with rasterio.open(vrt_path) as vrt_ds:
                bands, vrt_w, vrt_h = vrt_ds.count, vrt_ds.width, vrt_ds.height
                win_w, win_h = min(64, vrt_w), min(64, vrt_h)
                window = Window(col_off=0, row_off=0, width=win_w, height=win_h)
                data = vrt_ds.read(window=window)
            # A windowed read returns exactly (bands, height, width).
            assert data.shape == (bands, win_h, win_w), data.shape
        finally:
            # mint_vrt leaves the temp dir to the caller; clean it up.
            import shutil

            shutil.rmtree(os.path.dirname(vrt_path), ignore_errors=True)

        # Context-manager form auto-cleans the transient VRT on exit.
        with minted_vrt(tiles) as ctx_vrt:
            assert os.path.exists(ctx_vrt)
            ctx_dir = os.path.dirname(ctx_vrt)
        assert not os.path.exists(ctx_dir), "minted_vrt must remove its temp dir on exit"

    return True
