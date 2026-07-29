"""pmtiles_gbx (lightweight) Reader Examples — single source of truth.

Code shown in docs/docs/readers/pmtiles.mdx is imported from here. Pure-Python
DataSource V2 reader; no JAR (registered via gbx.ds.register). Two sources,
chosen by the `source` option:

- source="raster" — build an XYZ tile mosaic pyramid from a directory of COGs /
  GeoTIFFs (render one tile per (z, x, y) with rio-tiler).
- source="archive" — read tiles back out of an existing .pmtiles archive.

Both emit the same (z: int, x: int, y: int, bytes: binary) schema — the exact
input schema the pmtiles writer expects, so a read feeds straight back into a
write.
"""

REGISTER = """# Register the lightweight DataSources (once per session)
from databricks.labs.gbx.ds.register import register
register(spark)"""

READ_RASTER = """# source="raster": tile a directory of COGs into an XYZ pyramid.
# Emits one rendered (z, x, y, bytes) tile row per slippy tile in the AOI.
df = (spark.read.format("pmtiles_gbx")
      .option("source", "raster")
      .option("bbox", "-122.50,37.74,-122.40,37.79")   # minx,miny,maxx,maxy (WGS84)
      .option("minZoom", "12")
      .option("maxZoom", "16")
      .option("tileFormat", "png")
      .load("/Volumes/main/geobrix_samples/naip/sf/"))
# columns: z (int), x (int), y (int), bytes (binary — a rendered PNG tile)
df.show()"""

WRITE_BACK = """# The (z, x, y, bytes) rows feed straight into the pmtiles writer —
# no reshaping — packaging the mosaic pyramid into a single archive.
(df.write.format("pmtiles_gbx")
   .option("shardZoom", "0")              # 0 = one single .pmtiles archive
   .mode("overwrite")
   .save("/Volumes/main/geobrix_samples/naip/sf.pmtiles"))"""

READ_ARCHIVE = """# source="archive": read tiles back out of an existing .pmtiles file.
df = (spark.read.format("pmtiles_gbx")
      .option("source", "archive")
      .load("/Volumes/main/geobrix_samples/naip/sf.pmtiles"))
# same (z, x, y, bytes) schema — bytes are the stored tile payload, verbatim
df.show()"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def read_raster(spark, path, bbox, min_zoom, max_zoom):
    """Verify READ_RASTER: raster source yields the (z, x, y, bytes) schema,
    fans out into partitions, and renders each tile exactly once."""
    _register(spark)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("bbox", bbox)
        .option("minZoom", str(min_zoom))
        .option("maxZoom", str(max_zoom))
        .option("tileFormat", "png")
        .load(path)
    )
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
    rows = df.collect()
    assert len(rows) > 0
    keys = [(r["z"], r["x"], r["y"]) for r in rows]
    assert len(keys) == len(set(keys))  # each tile produced once
    return df


def read_archive(spark, path):
    """Verify READ_ARCHIVE: archive source yields the (z, x, y, bytes) schema."""
    _register(spark)
    df = spark.read.format("pmtiles_gbx").option("source", "archive").load(path)
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
    assert df.count() >= 1
    return df
