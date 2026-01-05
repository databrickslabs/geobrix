---
sidebar_position: 4
---

# VectorX

![VectorX](../../../resources/images/VectorX.png)

VectorX is GeoBrix's vector data operations package, designed to augment Databricks built-in spatial functions and provide migration tools from legacy Mosaic geometries.

## Overview

VectorX is a refactor of select DBLabs Mosaic vector functions that augment existing product ST Geospatial Functions. Currently, this primarily includes functions to handle updating existing Mosaic geometry data to formats supported by Databricks product, so that users do not need to install (older) Mosaic in order to get to using the latest spatial features.

## Key Features

- **Legacy Mosaic Support**: Convert legacy Mosaic geometries to modern formats
- **WKB/WKT Output**: Standard geometry format output
- **Databricks Integration**: Seamless conversion to Databricks spatial types
- **Migration Tools**: Smooth transition from Mosaic to GeoBrix/Databricks

## Available Functions

### Geometry Conversion

#### `gbx_st_legacyaswkb`

Converts legacy DBLabs Mosaic geometry formats to Well-Known Binary (WKB) format.

**Syntax:**
```sql
gbx_st_legacyaswkb(legacy_geometry) -> binary
```

**Parameters:**
- `legacy_geometry`: Geometry in legacy Mosaic format

**Returns:** Binary WKB representation

**Example:**
```python
from databricks.labs.gbx.vectorx import functions as vx
vx.register(spark)

# Convert legacy Mosaic geometry
converted = spark.sql("""
    SELECT
        feature_id,
        gbx_st_legacyaswkb(mosaic_geom) as wkb_geom
    FROM legacy_mosaic_table
""")
```

## Usage Examples

### Python/PySpark

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

# Register VectorX functions
vx.register(spark)

# Read legacy Mosaic data
legacy_df = spark.table("legacy_mosaic_geometries")

# Convert to WKB
wkb_df = legacy_df.select(
    "feature_id",
    "properties",
    vx.st_legacyaswkb("mosaic_geom").alias("geom_wkb")
)

# Convert WKB to Databricks GEOMETRY type
geometry_df = wkb_df.select(
    "feature_id",
    "properties",
    "geom_wkb",
    expr("st_geomfromwkb(geom_wkb)").alias("geometry")
)

# Now use built-in Databricks ST functions
result = geometry_df.select(
    "feature_id",
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_length(geometry)").alias("perimeter"),
    expr("st_centroid(geometry)").alias("centroid")
)

result.show()
```

### Scala

```scala
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Register functions
vx.register(spark)

// Convert legacy geometries
val legacyDf = spark.table("legacy_mosaic_geometries")

val wkbDf = legacyDf.select(
  col("feature_id"),
  vx.st_legacyaswkb(col("mosaic_geom")).alias("geom_wkb")
)

// Convert to GEOMETRY type
val geometryDf = wkbDf.select(
  col("feature_id"),
  col("geom_wkb"),
  expr("st_geomfromwkb(geom_wkb)").alias("geometry")
)

geometryDf.show()
```

### SQL

```sql
-- Register functions first in Python/Scala notebook

-- Convert legacy Mosaic geometries
CREATE OR REPLACE TEMP VIEW converted_geometries AS
SELECT
    feature_id,
    properties,
    gbx_st_legacyaswkb(mosaic_geom) as geom_wkb,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;

-- Use Databricks built-in spatial functions
SELECT
    feature_id,
    geometry,
    st_area(geometry) as area,
    st_length(geometry) as perimeter,
    st_centroid(geometry) as centroid,
    st_envelope(geometry) as bbox
FROM converted_geometries;

-- Spatial join with converted geometries
SELECT
    a.feature_id,
    b.poi_name,
    st_distance(a.geometry, b.location) as distance
FROM converted_geometries a
JOIN points_of_interest b
    ON st_contains(a.geometry, b.location);
```

## Migration Workflows

### Workflow 1: Full Table Migration

Migrate an entire table from Mosaic to Databricks spatial types:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr
vx.register(spark)

# Read legacy table
legacy_table = spark.table("catalog.schema.legacy_mosaic_table")

# Convert all geometries
migrated = legacy_table.select(
    "*",  # Keep all original columns
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
).drop("mosaic_geom")  # Drop legacy column

# Write to new table with GEOMETRY type
migrated.write.mode("overwrite").saveAsTable("catalog.schema.migrated_table")

# Verify migration
spark.sql("""
    SELECT
        COUNT(*) as total_records,
        COUNT(geometry) as valid_geometries
    FROM catalog.schema.migrated_table
""").show()
```

### Workflow 2: Incremental Migration

Migrate data in batches:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr
vx.register(spark)

# Process in batches
batch_size = 10000
total_records = spark.table("legacy_mosaic_table").count()
num_batches = (total_records // batch_size) + 1

for batch_id in range(num_batches):
    offset = batch_id * batch_size
    
    # Read batch
    batch = spark.sql(f"""
        SELECT *
        FROM legacy_mosaic_table
        LIMIT {batch_size} OFFSET {offset}
    """)
    
    # Convert
    converted = batch.select(
        "*",
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    ).drop("mosaic_geom")
    
    # Append to target table
    converted.write.mode("append").saveAsTable("migrated_table")
    
    print(f"Batch {batch_id + 1}/{num_batches} completed")
```

### Workflow 3: Validation and Quality Checks

Validate migrated geometries:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr, col, when
vx.register(spark)

# Migrate with validation
migrated = spark.sql("""
    SELECT
        feature_id,
        mosaic_geom,
        gbx_st_legacyaswkb(mosaic_geom) as wkb_geom,
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
    FROM legacy_mosaic_table
""")

# Validate geometries
validated = migrated.select(
    "*",
    expr("st_isvalid(geometry)").alias("is_valid"),
    expr("st_geometrytype(geometry)").alias("geom_type"),
    when(col("geometry").isNull(), "NULL_GEOMETRY")
        .when(~expr("st_isvalid(geometry)"), "INVALID_GEOMETRY")
        .otherwise("VALID")
        .alias("validation_status")
)

# Check results
validation_summary = validated.groupBy("validation_status", "geom_type").count()
validation_summary.show()

# Filter and save only valid geometries
valid_geometries = validated.filter(col("is_valid") == True)
valid_geometries.write.mode("overwrite").saveAsTable("validated_migrated_table")
```

### Workflow 4: Side-by-Side Comparison

Compare legacy and migrated data:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr
vx.register(spark)

# Create comparison view
comparison = spark.sql("""
    SELECT
        feature_id,
        mosaic_geom as legacy_geom,
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as new_geom,
        -- Compare areas (example)
        st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as new_area
    FROM legacy_mosaic_table
    LIMIT 100
""")

comparison.show()
```

## Integration with Databricks Spatial Functions

VectorX is designed to work seamlessly with Databricks built-in spatial functions:

### Geometry Construction

```sql
-- Convert from legacy and use ST functions
SELECT
    feature_id,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry,
    st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as area,
    st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as centroid
FROM legacy_data;
```

### Spatial Relationships

```sql
-- Use converted geometries in spatial joins
SELECT
    a.id as building_id,
    b.id as zone_id
FROM
    (SELECT id, st_geomfromwkb(gbx_st_legacyaswkb(geom)) as geometry FROM buildings) a
JOIN
    (SELECT id, st_geomfromwkb(gbx_st_legacyaswkb(geom)) as geometry FROM zones) b
    ON st_contains(b.geometry, a.geometry);
```

### Spatial Aggregations

```sql
-- Aggregate with converted geometries
SELECT
    zone_id,
    st_union_agg(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as merged_geometry,
    COUNT(*) as feature_count
FROM legacy_features
GROUP BY zone_id;
```

## Output Formats

### WKB (Well-Known Binary)

Default output format from `gbx_st_legacyaswkb`:
- Compact binary representation
- Industry standard format
- Efficient for storage and transmission
- Compatible with all spatial systems

### Converting to Other Formats

```sql
-- WKB to WKT (Well-Known Text)
SELECT
    feature_id,
    st_astext(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as wkt
FROM legacy_data;

-- WKB to GeoJSON
SELECT
    feature_id,
    st_asgeojson(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as geojson
FROM legacy_data;

-- WKB to Databricks GEOMETRY
SELECT
    feature_id,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_data;
```

## Integration with Readers

VectorX works well with GeoBrix readers:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr
vx.register(spark)

# Read shapefile (outputs WKB by default)
shapefile_df = spark.read.format("shapefile").load("/path/to/shapefiles")

# Convert to Databricks GEOMETRY type
geometry_df = shapefile_df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Use with Databricks ST functions
result = geometry_df.select(
    "feature_id",
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_buffer(geometry, 100)").alias("buffer")
)
```

## Performance Considerations

### Batch Conversion

For large datasets, process in batches:

```python
# Use repartition for parallel processing
legacy_df = spark.table("large_legacy_table").repartition(100)

converted = legacy_df.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
)

# Cache if reusing
converted.cache()
```

### Optimization Tips

1. **Convert once, store in Delta**: Don't repeatedly convert the same data
2. **Use appropriate data types**: Store as GEOMETRY type after conversion
3. **Partition appropriately**: Partition by spatial attributes for better performance
4. **Cache intermediate results**: Cache converted geometries if used multiple times

```python
# Optimal pattern
converted = legacy_df.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
)

# Save as Delta with optimizations
converted.write.mode("overwrite").format("delta").saveAsTable("optimized_geometries")

# Optimize table
spark.sql("""
    OPTIMIZE optimized_geometries
    ZORDER BY (geometry)
""")
```

## Use Cases

### Mosaic to GeoBrix Migration
- Migrate existing Mosaic pipelines to GeoBrix
- Update geometry formats without data loss
- Maintain compatibility during transition

### Data Format Standardization
- Convert various geometry formats to WKB
- Prepare data for Databricks spatial functions
- Ensure consistency across datasets

### Legacy System Integration
- Read data from older systems
- Convert to modern formats
- Enable modern spatial analysis

## Known Limitations

- Currently focused on Mosaic legacy format conversion
- Other vector functions planned for future releases
- Some Mosaic functions not yet ported (e.g., `st_interpolateelevation`, `st_triangulate`)
- Spatial KNN not yet available

## Future Enhancements

Planned additions to VectorX:
- Additional geometry transformation functions
- Advanced vector operations
- Spatial KNN operations
- Enhanced topology functions

## Next Steps

- [View API Reference](../api/overview.md)
- [Check Examples](../examples/overview.md)
- [Learn about Readers](../readers/overview.md)
- [Learn about RasterX](./rasterx.md)
- [Learn about GridX](./gridx.md)

