"""
Synthetic Points (Vector) example — requires Databricks Runtime (DBR).

Uses built-in st_point, st_astext (Databricks Spatial SQL). Single source for
docs/docs/sample-data/additional.mdx § Synthetic Points (Vector).
Tested by: docs/tests-dbr/python/sample_data/test_additional.py
"""

# Uses st_point, st_astext — DBR-only
SYNTHETIC_POINTS = """# Generate synthetic point data
from pyspark.sql import functions as f
import random

# Create 1000 random points in London area
random.seed(42)
points = spark.range(1000).select(
    f.col("id"),
    (f.lit(51.5) + (f.rand() - 0.5) * 0.5).alias("latitude"),   # London area
    (f.lit(-0.1) + (f.rand() - 0.5) * 0.5).alias("longitude"),
    (f.rand() * 100).cast("int").alias("value")
)

# Add WKT geometry (st_point, st_astext require DBR)
points = points.withColumn(
    "geom",
    f.expr("st_astext(st_point(longitude, latitude))")
)

# Save as table or use directly
points.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.synthetic_points")
print(f"✅ Created {points.count()} synthetic points")
points.show(5)"""

SYNTHETIC_POINTS_output = """✅ Created 1000 synthetic points
+---+--------+---------+-----+
|id |latitude|longitude|value|
+---+--------+---------+-----+
|0  |51.23   |-0.15    |42   |
|1  |51.67   |0.08     |91   |
|...|...     |...      |...  |
+---+--------+---------+-----+"""
