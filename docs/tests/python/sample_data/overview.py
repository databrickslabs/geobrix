"""
Overview page code examples - Geographic Coherence section.

Snippets are constants so docs show payload only (no wrapper).
Single source of truth for docs/docs/sample-data/overview.mdx.
Tested by: docs/tests/python/sample_data/test_overview.py
"""

# NYC workflows
GEOGRAPHIC_COHERENCE_NYC_ZONAL = """# Zonal statistics: Average elevation per borough
sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
boroughs = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/boroughs/nyc_boroughs.geojson")
elevation = spark.read.format("gdal").load(f"{sample_path}/nyc/elevation/srtm_n40w074.tif")

# Spatial join and compute statistics
borough_elevation = boroughs.join(elevation, "spatial_intersect") \\
    .groupBy("boro_name") \\
    .agg(rx.rst_avg("tile").alias("avg_elevation"))
borough_elevation.show()"""

GEOGRAPHIC_COHERENCE_NYC_ZONAL_output = """
+----------+-----------------+
|boro_name |avg_elevation    |
+----------+-----------------+
|Manhattan |45.2             |
|Bronx     |38.1             |
|Brooklyn  |22.3             |
|Queens    |28.7             |
|Staten Is.|42.0             |
+----------+-----------------+"""

GEOGRAPHIC_COHERENCE_NYC_CLIP = """# Clip satellite imagery to taxi zone
sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
zones = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/taxi-zones/nyc_taxi_zones.geojson")
sentinel = spark.read.format("gdal").load(f"{sample_path}/nyc/sentinel2/nyc_sentinel2_red.tif")

# Clip raster to specific zone
jfk_zone = zones.filter("LocationID = 132")  # JFK Airport zone
jfk_imagery = sentinel.withColumn(
    "clipped",
    rx.rst_clip("tile", jfk_zone.geom)
)
jfk_imagery.select("path", "clipped").show(1, truncate=False)"""

GEOGRAPHIC_COHERENCE_NYC_CLIP_output = """
+--------------------+------------------+
|path                |clipped           |
+--------------------+------------------+
|.../nyc_sentinel2...|[RasterTile(...)] |
+--------------------+------------------+"""

GEOGRAPHIC_COHERENCE_NYC_MULTISCALE = """# Multi-scale analysis: Taxi zones within boroughs
sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
zones = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/taxi-zones/nyc_taxi_zones.geojson")
boroughs = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/boroughs/nyc_boroughs.geojson")
neighborhoods = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/neighborhoods/nyc_nta.geojson")

# Hierarchical spatial joins
zones_with_context = zones \\
    .join(boroughs, "spatial_within") \\
    .join(neighborhoods, "spatial_within")
zones_with_context.show(5)"""

GEOGRAPHIC_COHERENCE_NYC_MULTISCALE_output = """
+----+--------+----------+-----+
|... |boro_...|ntaname   |...  |
+----+--------+----------+-----+
|132 |Staten..|Port Rich.|...  |
|... |...     |...       |...  |
+----+--------+----------+-----+"""

# London BNG
GEOGRAPHIC_COHERENCE_LONDON_BNG = """from databricks.labs.gbx.gridx.bng import functions as bx

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Index London postcodes to BNG grid
postcodes = spark.read.format("geojson_ogr").load(f"{sample_path}/london/postcodes/london_postcodes.geojson")
postcodes_bng = postcodes.withColumn(
    "bng_1km",
    bx.bng_polyfill(f.col("geometry"), 1000)
)

# Aggregate Sentinel-2 data to BNG cells
sentinel = spark.read.format("gdal").load(f"{sample_path}/london/sentinel2/london_sentinel2_red.tif")
bng_cells = bx.bng_tessellate(sentinel, 1000)
bng_cells.show(5)"""

GEOGRAPHIC_COHERENCE_LONDON_BNG_output = """
+-----------+-----------+-----+
|path       |bng_1km    |...  |
+-----------+-----------+-----+
|.../londo..|[550000... |...  |
+-----------+-----------+-----+"""
