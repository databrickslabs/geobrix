"""Context reference geometries: EIA Permian shale plays + TIGER counties.

Static reference (not observations) — read once per run from the Volume with the
GeoBrix light vector reader (geojson_gbx / shapefile_gbx, pyogrio-backed, no JAR)
and emit native GEOMETRY at SRID 4326 for the AI/BI choropleths and the gold
point-in-polygon rollups. The reader emits geometry as WKB in `geom_0`."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import paths, register_gbx


@dp.materialized_view(
    name="ref_shale_plays",
    comment="EIA tight-oil/shale plays for the Permian basin (named play polygons)",
)
def ref_shale_plays():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)
    src = f"{p['context']}/plays/plays.geojson"
    return (
        spark.read.format("geojson_gbx").load(src)
        .filter(F.col("Basin") == "Permian")
        .select(
            F.col("Shale_play").alias("play_name"),
            F.col("Area_sq_km").cast("double").alias("area_sq_km"),
            F.expr("st_setsrid(st_geomfromwkb(geom_0), 4326)").alias("play_geom"),
        )
    )


@dp.materialized_view(
    name="ref_counties",
    comment="US Census TIGER counties (TX + NM) clipped to the AOI",
)
def ref_counties():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)
    src = f"{p['context']}/counties/cb_2024_us_county_500k.zip"
    return (
        spark.read.format("shapefile_gbx").load(src)
        .filter(F.col("STATEFP").isin("48", "35"))
        .select(
            F.col("NAME").alias("county_name"),
            F.col("STATEFP").alias("state_fp"),
            F.col("GEOID").alias("geoid"),
            F.expr("st_setsrid(st_geomfromwkb(geom_0), 4326)").alias("county_geom"),
        )
    )
