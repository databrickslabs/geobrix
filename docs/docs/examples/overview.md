---
sidebar_position: 1
---

# Examples Overview

This section provides practical examples of using GeoBrix for common geospatial workflows.

## Available Examples

Examples are organized by use case and demonstrate end-to-end workflows.

### Coming Soon

Detailed examples will be added including:

- **Raster Processing Workflows**
  - Satellite imagery cataloging
  - Elevation model analysis
  - Multi-temporal raster analysis
  
- **Vector Data Workflows**
  - Shapefile to Delta Lake migration
  - Spatial joins and analysis
  - Legacy Mosaic migration

- **Grid Indexing Workflows**
  - BNG spatial aggregation
  - Multi-resolution analysis
  - Location-based services

- **Integration Examples**
  - Combining raster and vector data
  - Using with Databricks spatial functions
  - Performance optimization patterns

## Example Notebooks

For now, please refer to the example notebooks included in the [beta-dist](https://github.com/databrickslabs/geobrix/tree/main/resources/beta-dist) directory of the GeoBrix repository.

## Quick Examples

### Example 1: Read and Catalog Rasters

```python
from databricks.labs.gbx.rasterx import functions as rx

rx.register(spark)

# Read rasters
rasters = spark.read.format("gdal").load("/data/satellite")

# Build catalog
catalog = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bounds"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_metadata("tile").alias("metadata")
)

catalog.write.mode("overwrite").saveAsTable("raster_catalog")
```

### Example 2: Spatial Aggregation with BNG

```python
from databricks.labs.gbx.gridx.bng import functions as bx

bx.register(spark)

# Aggregate points by BNG cell
result = spark.sql("""
    SELECT
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
        COUNT(*) as count,
        AVG(value) as avg_value
    FROM measurements
    WHERE country = 'GB'
    GROUP BY bng_cell
""")

result.write.mode("overwrite").saveAsTable("bng_aggregated")
```

### Example 3: Migrate from Mosaic

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Convert legacy geometries
legacy = spark.table("legacy_mosaic_table")

migrated = legacy.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
).drop("mosaic_geom")

migrated.write.mode("overwrite").saveAsTable("migrated_table")
```

## Contributing Examples

If you have interesting GeoBrix use cases, consider contributing examples to the documentation!

## Next Steps

- [View API Reference](../api/overview.md)
- [Package Documentation](../packages/overview.md)
- [Reader Documentation](../readers/overview.md)

