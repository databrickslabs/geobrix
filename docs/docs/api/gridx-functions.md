---
sidebar_position: 6
---

# GridX Function Reference (BNG)

Complete reference for all GridX British National Grid (BNG) functions with detailed descriptions, parameters, return values, and examples.

## Overview

GridX provides functions for working with the British National Grid coordinate system, a specialized grid system used in Great Britain for spatial indexing and location-based services.

## Cell Operation Functions

### bng_cellarea

Calculate the area of a BNG grid cell.

**Signature:**
```scala
bng_cellarea(gridLetter: Column, precision: Column): Column
```

**Parameters:**
- `gridLetter` - Two-letter BNG grid square identifier (e.g., "TQ", "SU")
- `precision` - Grid cell precision in meters (e.g., 1000 for 1km)

**Returns:**
- Double representing the cell area in square meters

**Examples:**

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Calculate area of 1km cell in TQ grid
area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm")
area.show()
# Output: area_sqm = 1000000.0

# Multiple precisions
areas = spark.sql("""
    SELECT
        gbx_bng_cellarea('TQ', 10000) as area_10km,
        gbx_bng_cellarea('TQ', 1000) as area_1km,
        gbx_bng_cellarea('TQ', 100) as area_100m
""")
```

```scala
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
bx.register(spark)

val area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area")
```

---

### bng_celldistance

Calculate distance between two BNG grid cells.

**Signature:**
```scala
bng_celldistance(cell1: Column, cell2: Column): Column
```

**Parameters:**
- `cell1` - First BNG cell reference
- `cell2` - Second BNG cell reference

**Returns:**
- Double representing distance in meters

**Examples:**

```python
distance = spark.sql("""
    SELECT gbx_bng_celldistance('TQ3080', 'TQ3180') as distance_m
""")
```

---

### bng_cellkring

Generate a k-ring of cells around a center cell.

**Signature:**
```scala
bng_cellkring(centerCell: Column, k: Column): Column
```

**Parameters:**
- `centerCell` - BNG cell reference for center
- `k` - Integer ring distance

**Returns:**
- Array of BNG cell references in the k-ring

**Examples:**

```python
# Get cells within 2-ring distance
nearby = spark.sql("""
    SELECT
        'TQ3080' as center,
        gbx_bng_cellkring('TQ3080', 2) as nearby_cells
""")

# Explode to individual cells
from pyspark.sql.functions import explode
expanded = nearby.select(
    "center",
    explode("nearby_cells").alias("nearby_cell")
)
```

---

### bng_cellfromprecision

Create a BNG cell reference at specified precision.

**Signature:**
```scala
bng_cellfromprecision(easting: Column, northing: Column, precision: Column): Column
```

**Parameters:**
- `easting` - Easting coordinate
- `northing` - Northing coordinate
- `precision` - Cell precision in meters

**Returns:**
- String BNG cell reference

---

## Coordinate Conversion Functions

### bng_pointtocell

Convert a point geometry to a BNG grid cell.

**Signature:**
```scala
bng_pointtocell(point: Column, precision: Column): Column
```

**Parameters:**
- `point` - Point geometry (WGS84 or OSGB36)
- `precision` - Grid cell precision in meters

**Returns:**
- String BNG cell reference

**Examples:**

```python
from pyspark.sql.functions import expr

# Convert points to BNG cells
locations = spark.table("uk_locations")

bng_cells = locations.select(
    "location_id",
    "latitude",
    "longitude",
    expr("gbx_bng_pointtocell(st_point(longitude, latitude), 1000)").alias("bng_1km"),
    expr("gbx_bng_pointtocell(st_point(longitude, latitude), 100)").alias("bng_100m")
)

bng_cells.show()
```

```sql
-- Convert points to BNG cells at multiple resolutions
SELECT
    location_id,
    latitude,
    longitude,
    gbx_bng_pointtocell(st_point(longitude, latitude), 10000) as bng_10km,
    gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_1km,
    gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_100m
FROM uk_locations;
```

---

### bng_celltoboundary

Get the boundary polygon of a BNG grid cell.

**Signature:**
```scala
bng_celltoboundary(cell: Column): Column
```

**Parameters:**
- `cell` - BNG cell reference

**Returns:**
- Geometry representing the cell boundary

**Examples:**

```python
boundaries = spark.sql("""
    SELECT
        bng_cell,
        gbx_bng_celltoboundary(bng_cell) as cell_boundary
    FROM bng_cells
""")
```

---

### bng_celltopoint

Get the center point of a BNG grid cell.

**Signature:**
```scala
bng_celltopoint(cell: Column): Column
```

**Parameters:**
- `cell` - BNG cell reference

**Returns:**
- Point geometry at cell center

**Examples:**

```python
centers = spark.sql("""
    SELECT
        bng_cell,
        gbx_bng_celltopoint(bng_cell) as center_point
    FROM bng_cells
""")
```

---

## Grid Property Functions

### bng_getgridletter

Extract the grid letter prefix from a BNG reference.

**Signature:**
```scala
bng_getgridletter(cell: Column): Column
```

**Parameters:**
- `cell` - BNG cell reference

**Returns:**
- String grid letter (e.g., "TQ")

**Examples:**

```python
grid_letters = spark.sql("""
    SELECT
        bng_cell,
        gbx_bng_getgridletter(bng_cell) as grid_square
    FROM locations
""")
```

---

### bng_getprecision

Get the precision of a BNG grid reference.

**Signature:**
```scala
bng_getprecision(cell: Column): Column
```

**Parameters:**
- `cell` - BNG cell reference

**Returns:**
- Integer precision in meters

**Examples:**

```python
precisions = spark.sql("""
    SELECT
        bng_cell,
        gbx_bng_getprecision(bng_cell) as precision_m
    FROM locations
""")
```

---

### bng_isvalid

Validate a BNG grid reference.

**Signature:**
```scala
bng_isvalid(cell: Column): Column
```

**Parameters:**
- `cell` - BNG cell reference to validate

**Returns:**
- Boolean indicating validity

**Examples:**

```python
validated = spark.sql("""
    SELECT
        bng_ref,
        gbx_bng_isvalid(bng_ref) as is_valid
    FROM input_data
""")

# Filter to valid references only
valid_only = validated.filter("is_valid = true")
```

---

## Complete Examples

### Example 1: Spatial Aggregation by BNG Cell

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr, count, avg

bx.register(spark)

# Aggregate measurements by BNG cell
aggregated = spark.sql("""
    SELECT
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
        COUNT(*) as measurement_count,
        AVG(temperature) as avg_temp,
        MAX(temperature) as max_temp,
        MIN(temperature) as min_temp,
        STDDEV(temperature) as stddev_temp
    FROM weather_measurements
    WHERE country = 'GB'
    GROUP BY bng_cell
    ORDER BY measurement_count DESC
""")

# Save results
aggregated.write.mode("overwrite").saveAsTable("weather_by_bng")
```

---

### Example 2: Multi-Resolution Analysis

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Create multi-resolution grid
multi_res = spark.sql("""
    SELECT
        location_id,
        name,
        st_point(longitude, latitude) as location,
        gbx_bng_pointtocell(st_point(longitude, latitude), 10000) as bng_10km,
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_1km,
        gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_100m
    FROM poi_uk
""")

# Count by resolution
spark.sql("""
    SELECT '10km' as resolution, COUNT(DISTINCT bng_10km) as unique_cells
    FROM multi_res
    UNION ALL
    SELECT '1km', COUNT(DISTINCT bng_1km) FROM multi_res
    UNION ALL
    SELECT '100m', COUNT(DISTINCT bng_100m) FROM multi_res
    ORDER BY resolution
""").show()
```

---

### Example 3: Spatial Join Using BNG

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Index both datasets with BNG
incidents = spark.sql("""
    SELECT
        incident_id,
        incident_type,
        gbx_bng_pointtocell(location, 1000) as bng_cell
    FROM incidents
""")

resources = spark.sql("""
    SELECT
        resource_id,
        resource_type,
        gbx_bng_pointtocell(location, 1000) as bng_cell
    FROM emergency_resources
""")

# Join on BNG cell for fast spatial matching
matched = incidents.join(resources, on="bng_cell", how="inner")

# Find incidents with nearby resources
matched.select(
    "incident_id",
    "incident_type",
    "resource_id",
    "resource_type",
    "bng_cell"
).show()
```

---

### Example 4: K-Ring Analysis

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr, explode
bx.register(spark)

# Find all data within k-ring of important locations
important_sites = spark.sql("""
    SELECT
        site_id,
        site_name,
        gbx_bng_pointtocell(location, 1000) as center_cell
    FROM critical_sites
""")

# Generate 3-ring around each site
nearby_cells = important_sites.select(
    "site_id",
    "site_name",
    "center_cell",
    expr("gbx_bng_cellkring(center_cell, 3)").alias("nearby_cells")
)

# Explode to individual cells
expanded = nearby_cells.select(
    "site_id",
    "site_name",
    explode("nearby_cells").alias("bng_cell")
)

# Join with sensor data
sensor_data = spark.table("sensor_readings_by_bng")

results = expanded.join(
    sensor_data,
    on="bng_cell",
    how="inner"
)

results.write.mode("overwrite").saveAsTable("site_nearby_sensors")
```

---

### Example 5: BNG Grid Catalog

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Create a comprehensive BNG grid catalog
catalog = spark.sql("""
    SELECT
        bng_cell,
        gbx_bng_getgridletter(bng_cell) as grid_square,
        gbx_bng_getprecision(bng_cell) as precision_m,
        gbx_bng_cellarea(
            gbx_bng_getgridletter(bng_cell),
            gbx_bng_getprecision(bng_cell)
        ) as area_sqm,
        gbx_bng_celltopoint(bng_cell) as center_point,
        gbx_bng_celltoboundary(bng_cell) as boundary
    FROM (
        SELECT DISTINCT bng_cell
        FROM location_index
    )
""")

catalog.write.mode("overwrite").saveAsTable("bng_grid_catalog")
```

---

## BNG Reference Format

### Standard Format
BNG references follow: `[Letters][Eastings][Northings]`

### Precision Levels

| Precision | Grid Size | Digits | Example | Use Case |
|-----------|-----------|--------|---------|----------|
| 100000m | 100km × 100km | 0 | TQ | Regional |
| 10000m | 10km × 10km | 1 | TQ38 | District |
| 1000m | 1km × 1km | 2 | TQ3080 | Local area |
| 100m | 100m × 100m | 3 | TQ308808 | Neighborhood |
| 10m | 10m × 10m | 4 | TQ30808080 | Building |
| 1m | 1m × 1m | 5 | TQ3080080800 | Precise location |

### Grid Squares

Major 100km grid squares in Great Britain:
- **TQ** - London area
- **SU** - South Hampshire
- **NT** - Edinburgh area
- **SD** - Lake District
- **ST** - Bristol area

---

## Performance Tips

### 1. Pre-compute BNG Cells

```python
# Add BNG cell column to table
enriched = locations.withColumn(
    "bng_1km",
    expr("gbx_bng_pointtocell(st_point(lon, lat), 1000)")
)

# Partition by BNG for efficient queries
enriched.write.partitionBy("bng_1km").saveAsTable("locations_indexed")
```

### 2. Use Z-Ordering

```sql
-- Optimize table with Z-ordering on BNG cell
OPTIMIZE locations_indexed
ZORDER BY (bng_1km);
```

### 3. Choose Appropriate Precision

```python
# Use coarser resolution for broad analysis
coarse = expr("gbx_bng_pointtocell(location, 10000)")  # 10km

# Use finer resolution for detailed analysis
fine = expr("gbx_bng_pointtocell(location, 100)")  # 100m
```

---

## Next Steps

- [RasterX Function Reference](./rasterx-functions.md)
- [VectorX Function Reference](./vectorx-functions.md)
- [GridX Package Documentation](../packages/gridx.md)

