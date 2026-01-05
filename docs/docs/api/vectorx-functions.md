---
sidebar_position: 7
---

# VectorX Function Reference

Complete reference for all VectorX functions with detailed descriptions, parameters, return values, and examples.

## Overview

VectorX provides functions for vector geometry operations, with a primary focus on migrating legacy DBLabs Mosaic geometries to modern formats compatible with Databricks spatial types.

## Geometry Conversion Functions

### st_legacyaswkb

Convert legacy DBLabs Mosaic geometry format to Well-Known Binary (WKB).

**Signature:**
```scala
st_legacyaswkb(legacyGeometry: Column): Column
```

**Parameters:**
- `legacyGeometry` - Column containing legacy Mosaic geometry data

**Returns:**
- Binary WKB representation of the geometry

**Description:**

This function is essential for migrating existing Mosaic workloads to GeoBrix and Databricks native spatial functions. It converts legacy Mosaic geometry formats into standard WKB format, which can then be used with Databricks built-in ST functions.

**Examples:**

**Python:**
```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Read legacy Mosaic table
legacy_data = spark.table("legacy_mosaic_geometries")

# Convert to WKB
wkb_data = legacy_data.select(
    "feature_id",
    "properties",
    vx.st_legacyaswkb("mosaic_geom").alias("wkb_geom")
)

# Convert WKB to Databricks GEOMETRY type
geometry_data = wkb_data.select(
    "feature_id",
    "properties",
    "wkb_geom",
    expr("st_geomfromwkb(wkb_geom)").alias("geometry")
)

geometry_data.show()
```

**Scala:**
```scala
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

vx.register(spark)

val legacyData = spark.table("legacy_mosaic_geometries")

val wkbData = legacyData.select(
  col("feature_id"),
  col("properties"),
  vx.st_legacyaswkb(col("mosaic_geom")).alias("wkb_geom")
)

val geometryData = wkbData.select(
  col("feature_id"),
  col("properties"),
  col("wkb_geom"),
  expr("st_geomfromwkb(wkb_geom)").alias("geometry")
)

geometryData.show()
```

**SQL:**
```sql
-- First register functions in Python/Scala
-- Then use in SQL

-- Convert legacy geometries
SELECT
    feature_id,
    properties,
    gbx_st_legacyaswkb(mosaic_geom) as wkb_geom,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;
```

---

## Complete Migration Examples

### Example 1: Full Table Migration

Migrate an entire legacy Mosaic table to Databricks spatial types:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Read legacy table
legacy_table = spark.table("catalog.schema.legacy_mosaic_features")

# Convert all geometries
migrated = legacy_table.select(
    "*",  # Keep all original columns
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
).drop("mosaic_geom")  # Remove legacy column

# Write to new table with GEOMETRY type
migrated.write.mode("overwrite").saveAsTable("catalog.schema.migrated_features")

# Verify migration
verification = spark.sql("""
    SELECT
        COUNT(*) as total_records,
        COUNT(geometry) as records_with_geometry,
        SUM(CASE WHEN st_isvalid(geometry) THEN 1 ELSE 0 END) as valid_geometries
    FROM catalog.schema.migrated_features
""")

verification.show()
```

---

### Example 2: Migration with Validation

Migrate with comprehensive validation and quality checks:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr, col, when

vx.register(spark)

# Read and convert
legacy = spark.table("legacy_features")

converted = legacy.select(
    "*",
    expr("gbx_st_legacyaswkb(mosaic_geom)").alias("wkb_geom"),
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
)

# Add validation columns
validated = converted.select(
    "*",
    expr("st_isvalid(geometry)").alias("is_valid"),
    expr("st_geometrytype(geometry)").alias("geom_type"),
    expr("st_srid(geometry)").alias("srid"),
    when(col("geometry").isNull(), "NULL_GEOMETRY")
        .when(~expr("st_isvalid(geometry)"), "INVALID_GEOMETRY")
        .otherwise("VALID")
        .alias("validation_status")
)

# Generate validation report
validation_report = validated.groupBy("validation_status", "geom_type").count()
validation_report.show()

# Save valid geometries only
valid_features = validated.filter("validation_status = 'VALID'")
valid_features.write.mode("overwrite").saveAsTable("validated_migrated_features")

# Save problematic records for review
invalid_features = validated.filter("validation_status != 'VALID'")
invalid_features.write.mode("overwrite").saveAsTable("migration_issues")
```

---

### Example 3: Incremental Migration

Migrate large tables in batches:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Configuration
batch_size = 100000
source_table = "legacy_mosaic_table"
target_table = "migrated_table"

# Get total count
total_count = spark.table(source_table).count()
num_batches = (total_count // batch_size) + 1

print(f"Migrating {total_count} records in {num_batches} batches...")

# Process in batches
for batch_id in range(num_batches):
    offset = batch_id * batch_size
    
    print(f"Processing batch {batch_id + 1}/{num_batches} (offset: {offset})")
    
    # Read batch
    batch = spark.sql(f"""
        SELECT *
        FROM {source_table}
        LIMIT {batch_size} OFFSET {offset}
    """)
    
    # Convert
    converted = batch.select(
        "*",
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    ).drop("mosaic_geom")
    
    # Append to target table
    if batch_id == 0:
        converted.write.mode("overwrite").saveAsTable(target_table)
    else:
        converted.write.mode("append").saveAsTable(target_table)
    
    print(f"Batch {batch_id + 1} completed")

print("Migration complete!")

# Optimize table
spark.sql(f"OPTIMIZE {target_table} ZORDER BY (geometry)")
```

---

### Example 4: Migration with Spatial Analysis

Migrate and immediately perform spatial analysis:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Migrate and analyze
migrated_with_analysis = spark.sql("""
    SELECT
        feature_id,
        feature_name,
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry,
        -- Spatial metrics
        st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as area_sqm,
        st_length(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as perimeter_m,
        st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as centroid,
        st_envelope(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as bbox,
        -- Geometry properties
        st_geometrytype(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as geom_type,
        st_numgeometries(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as num_parts
    FROM legacy_features
""")

migrated_with_analysis.write.mode("overwrite").saveAsTable("features_with_metrics")
```

---

### Example 5: Migration with Coordinate Transformation

Migrate and reproject to different CRS:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Migrate and transform to WGS84
transformed = spark.sql("""
    SELECT
        feature_id,
        original_crs,
        -- Original geometry
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as original_geom,
        -- Transform to WGS84
        st_transform(
            st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)),
            original_crs,
            'EPSG:4326'
        ) as wgs84_geom
    FROM legacy_features_with_crs
""")

transformed.write.mode("overwrite").saveAsTable("features_wgs84")
```

---

### Example 6: Side-by-Side Comparison

Compare legacy and migrated data:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr, col

vx.register(spark)

# Create comparison
comparison = spark.sql("""
    SELECT
        feature_id,
        -- Legacy
        mosaic_geom as legacy_geom,
        -- Converted
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as new_geom,
        -- Comparison metrics
        st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as new_area,
        st_isvalid(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as is_valid
    FROM legacy_features
    LIMIT 100
""")

comparison.show(truncate=False)
```

---

### Example 7: Spatial Join After Migration

Migrate and perform spatial join with other data:

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

vx.register(spark)

# Migrate legacy data
migrated_parcels = spark.sql("""
    SELECT
        parcel_id,
        st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
    FROM legacy_parcels
""")

# Read zones (already in GEOMETRY format)
zones = spark.sql("""
    SELECT
        zone_id,
        zone_name,
        geometry
    FROM planning_zones
""")

# Spatial join
parcels_in_zones = migrated_parcels.join(
    zones,
    expr("st_intersects(migrated_parcels.geometry, zones.geometry)"),
    "inner"
)

parcels_in_zones.select(
    "parcel_id",
    "zone_id",
    "zone_name"
).write.mode("overwrite").saveAsTable("parcel_zone_assignments")
```

---

## Integration with Databricks Spatial Functions

After converting with VectorX, all Databricks spatial functions are available:

### Geometric Measurements

```sql
SELECT
    feature_id,
    geometry,
    st_area(geometry) as area,
    st_length(geometry) as length,
    st_perimeter(geometry) as perimeter,
    st_distance(geometry, st_geomfromtext('POINT(0 0)')) as distance_from_origin
FROM migrated_features;
```

### Geometric Relationships

```sql
SELECT
    a.id as feature_a,
    b.id as feature_b,
    st_intersects(a.geometry, b.geometry) as intersects,
    st_contains(a.geometry, b.geometry) as a_contains_b,
    st_within(a.geometry, b.geometry) as a_within_b,
    st_overlaps(a.geometry, b.geometry) as overlaps
FROM migrated_features a
CROSS JOIN migrated_features b
WHERE a.id < b.id;
```

### Geometric Transformations

```sql
SELECT
    feature_id,
    geometry as original,
    st_buffer(geometry, 100) as buffered_100m,
    st_centroid(geometry) as center,
    st_envelope(geometry) as bbox,
    st_convexhull(geometry) as convex_hull
FROM migrated_features;
```

### Spatial Aggregations

```sql
SELECT
    region,
    st_union_agg(geometry) as merged_geometry,
    st_envelope_agg(geometry) as region_bbox,
    COUNT(*) as feature_count,
    SUM(st_area(geometry)) as total_area
FROM migrated_features
GROUP BY region;
```

---

## Migration Best Practices

### 1. Test with Sample Data

```python
# Test migration on sample first
sample = spark.table("legacy_table").sample(0.01)
test_migrated = sample.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
)
test_migrated.show()
```

### 2. Monitor Memory Usage

```python
# For large tables, use repartitioning
legacy_data = spark.table("large_legacy_table")
legacy_data = legacy_data.repartition(200)

migrated = legacy_data.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
)
```

### 3. Create Backups

```python
# Backup before migration
spark.sql("CREATE TABLE legacy_backup AS SELECT * FROM legacy_table")

# Proceed with migration
# ...

# Verify before dropping backup
spark.sql("DROP TABLE IF EXISTS legacy_backup")
```

### 4. Document Metadata

```python
# Add migration metadata
from pyspark.sql.functions import current_timestamp, lit

migrated = migrated.withColumn("migration_date", current_timestamp())
migrated = migrated.withColumn("source_system", lit("mosaic"))
migrated = migrated.withColumn("migration_version", lit("1.0"))
```

---

## Troubleshooting

### Issue: NULL Geometries

```python
# Identify NULL geometries
nulls = converted.filter("geometry IS NULL")
null_count = nulls.count()

if null_count > 0:
    print(f"Warning: {null_count} NULL geometries found")
    nulls.select("feature_id").show()
```

### Issue: Invalid Geometries

```python
# Find and fix invalid geometries
invalid = converted.filter("NOT st_isvalid(geometry)")

# Attempt to fix with buffer(0)
fixed = invalid.select(
    "*",
    expr("st_buffer(geometry, 0)").alias("fixed_geometry")
)
```

### Issue: Performance

```python
# Optimize for large tables
converted.write \
    .mode("overwrite") \
    .option("optimizeWrite", "true") \
    .saveAsTable("migrated_features")

# Z-order for spatial queries
spark.sql("OPTIMIZE migrated_features ZORDER BY (geometry)")
```

---

## Next Steps

- [RasterX Function Reference](./rasterx-functions.md)
- [GridX Function Reference](./gridx-functions.md)
- [VectorX Package Documentation](../packages/vectorx.md)
- [Databricks Spatial Functions](https://docs.databricks.com/sql/language-manual/sql-ref-st-geospatial-functions.html)

