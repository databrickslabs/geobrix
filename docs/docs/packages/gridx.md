---
sidebar_position: 3
---

# GridX

![GridX](../../../resources/images/GridX.png)

GridX is GeoBrix's grid indexing package, providing discrete global grid indexing capabilities with a focus on the British National Grid (BNG) system.

## Overview

GridX is a refactor of Mosaic discrete global grid indexing functions. The current focus has been on porting BNG (British National Grid) for Great Britain customers, providing specialized grid operations for UK-based spatial data.

## British National Grid (BNG)

The British National Grid is the national coordinate system for Great Britain. It is based on the Ordnance Survey National Grid (OSGB36) and divides the UK into grid squares with letter-based prefixes and numeric coordinates.

### BNG Structure

- **Grid Squares**: 100km × 100km squares identified by two letters (e.g., "TQ", "SU")
- **Eastings & Northings**: Numeric coordinates within each grid square
- **Precision**: Supports various precision levels (1m, 10m, 100m, 1km, etc.)

## Key Features

- **Grid Cell Operations**: Create, manipulate, and query BNG grid cells
- **Area Calculations**: Calculate areas of grid cells at different precisions
- **Coordinate Conversion**: Convert between grid references and coordinates
- **Spatial Indexing**: Use BNG for efficient spatial indexing
- **Multi-Resolution Support**: Work with different grid resolutions

## Function Categories

### Cell Operations

- `gbx_bng_cellarea` - Calculate the area of a BNG grid cell
- `gbx_bng_celldistance` - Calculate distance between BNG cells
- `gbx_bng_cellkring` - Generate k-ring of cells around a center cell
- `gbx_bng_cellfromprecision` - Create cell reference at specified precision

### Coordinate Conversion

- `gbx_bng_pointtocell` - Convert point to BNG grid cell
- `gbx_bng_celltoboundary` - Get boundary geometry of BNG cell
- `gbx_bng_celltopoint` - Get center point of BNG cell

### Grid Properties

- `gbx_bng_getgridletter` - Extract grid letter prefix
- `gbx_bng_getprecision` - Get precision of BNG reference
- `gbx_bng_isvalid` - Validate BNG grid reference

## Usage Examples

### Python/PySpark

```python
from databricks.labs.gbx.gridx.bng import functions as bx

# Register BNG functions
bx.register(spark)

# Calculate cell area for TQ grid square at 1km precision
area_df = spark.sql("""
    SELECT gbx_bng_cellarea('TQ', 1000) as cell_area
""")
area_df.show()

# Convert point to BNG cell
from pyspark.sql.functions import expr

points_df = spark.createDataFrame([
    (51.5074, -0.1278),  # London coordinates (lat, lon)
], ["lat", "lon"])

bng_cells = points_df.select(
    "lat",
    "lon",
    expr("gbx_bng_pointtocell(st_point(lon, lat), 1000)").alias("bng_cell")
)

bng_cells.show()
```

### Scala

```scala
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Register functions
bx.register(spark)

// Calculate cell area
val areaDf = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area")
areaDf.show()

// Create BNG cells from points
val pointsDf = Seq(
  (51.5074, -0.1278)
).toDF("lat", "lon")

val bngCells = pointsDf.select(
  col("lat"),
  col("lon"),
  expr("gbx_bng_pointtocell(st_point(lon, lat), 1000)").alias("bng_cell")
)

bngCells.show()
```

### SQL

```sql
-- Register functions first in Python/Scala notebook

-- Calculate area of a BNG grid cell
SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm;

-- Convert points to BNG cells
SELECT
    location_id,
    latitude,
    longitude,
    gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell_1km,
    gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_cell_100m
FROM locations
WHERE country = 'GB';

-- Get cell boundaries
SELECT
    bng_reference,
    gbx_bng_celltoboundary(bng_reference) as cell_boundary,
    gbx_bng_celltopoint(bng_reference) as cell_center
FROM bng_cells;

-- Generate k-ring around a location
SELECT
    origin_cell,
    gbx_bng_cellkring(origin_cell, 2) as neighboring_cells
FROM important_locations;
```

## Common Workflows

### Workflow 1: Spatial Aggregation with BNG

Aggregate data points into BNG grid cells:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr, count, avg
bx.register(spark)

# Aggregate points by BNG cell
aggregated = spark.sql("""
    SELECT
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
        COUNT(*) as point_count,
        AVG(value) as avg_value
    FROM measurements
    WHERE country = 'GB'
    GROUP BY bng_cell
""")

aggregated.show()
```

### Workflow 2: Grid-Based Spatial Joins

Join datasets using BNG grid indexing:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Index both datasets with BNG
locations_indexed = spark.sql("""
    SELECT
        *,
        gbx_bng_pointtocell(st_point(lon, lat), 1000) as bng_cell
    FROM locations
""")

poi_indexed = spark.sql("""
    SELECT
        *,
        gbx_bng_pointtocell(st_point(lon, lat), 1000) as bng_cell
    FROM points_of_interest
""")

# Join on BNG cell
joined = locations_indexed.join(
    poi_indexed,
    on="bng_cell",
    how="inner"
)

joined.show()
```

### Workflow 3: Multi-Resolution Analysis

Analyze data at different BNG resolutions:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr
bx.register(spark)

# Create multi-resolution grid
multi_res = spark.sql("""
    SELECT
        location_id,
        latitude,
        longitude,
        gbx_bng_pointtocell(st_point(longitude, latitude), 10000) as bng_10km,
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_1km,
        gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_100m
    FROM uk_locations
""")

# Aggregate at different resolutions
agg_10km = multi_res.groupBy("bng_10km").count()
agg_1km = multi_res.groupBy("bng_1km").count()
agg_100m = multi_res.groupBy("bng_100m").count()
```

### Workflow 4: K-Ring Analysis

Find all locations within a certain distance using BNG k-ring:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr, explode
bx.register(spark)

# Get all cells within k-ring
nearby_cells = spark.sql("""
    SELECT
        location_id,
        center_bng_cell,
        gbx_bng_cellkring(center_bng_cell, 3) as nearby_cells
    FROM important_sites
""")

# Explode array to individual cells
expanded = nearby_cells.select(
    "location_id",
    "center_bng_cell",
    explode("nearby_cells").alias("nearby_cell")
)

# Join with data in those cells
results = expanded.join(
    data_by_cell,
    expanded.nearby_cell == data_by_cell.bng_cell,
    "inner"
)
```

## BNG Grid Reference Format

### Standard Format

BNG references follow the format: `[Letters][Eastings][Northings]`

Examples:
- `TQ 38 80` - 1km precision (Tower of London area)
- `TQ 3800 8000` - 100m precision
- `TQ 38000 80000` - 10m precision
- `SU 12 34` - Different grid square

### Precision Levels

| Precision | Grid Size | Use Case |
|-----------|-----------|----------|
| 100000m | 100km × 100km | Regional analysis |
| 10000m | 10km × 10km | District-level |
| 1000m | 1km × 1km | Local area analysis |
| 100m | 100m × 100m | Neighborhood level |
| 10m | 10m × 10m | Building level |
| 1m | 1m × 1m | Precise location |

## Performance Considerations

### Indexing Strategy

Use BNG indexing for efficient spatial operations:

```python
# Partition data by BNG grid
df.repartition("bng_cell").write.partitionBy("bng_cell").saveAsTable("data_by_bng")
```

### Resolution Selection

Choose appropriate resolution for your use case:
- **Coarser resolution** (10km, 1km): Faster joins, less precision
- **Finer resolution** (100m, 10m): More precision, more cells

### Optimization Tips

1. **Use appropriate precision**: Match grid resolution to your analysis needs
2. **Partition by grid cell**: Improve query performance
3. **Pre-compute grid cells**: Calculate BNG cells once and store
4. **Use Z-ordering**: Apply Z-ordering on BNG cell columns in Delta tables

```python
# Z-order by BNG cell for better performance
spark.sql("""
    OPTIMIZE uk_locations
    ZORDER BY (bng_cell)
""")
```

## Integration with Other Packages

### With RasterX

Combine BNG grid with raster data:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from databricks.labs.gbx.rasterx import functions as rx

bx.register(spark)
rx.register(spark)

# Aggregate raster values by BNG cells
raster_by_bng = spark.sql("""
    SELECT
        gbx_bng_pointtocell(centroid, 1000) as bng_cell,
        AVG(pixel_value) as avg_value
    FROM raster_pixels
    GROUP BY bng_cell
""")
```

### With VectorX

Use BNG for vector indexing:

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from databricks.labs.gbx.vectorx import functions as vx

bx.register(spark)
vx.register(spark)

# Index vector data with BNG
indexed_vectors = spark.sql("""
    SELECT
        feature_id,
        gbx_st_legacyaswkb(geom) as geometry_wkb,
        gbx_bng_pointtocell(st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(geom))), 1000) as bng_cell
    FROM vector_features
""")
```

## Use Cases

### Urban Planning
- Analyze population density by grid square
- Plan infrastructure at different resolutions
- Aggregate census data

### Environmental Monitoring
- Track pollution levels by grid cell
- Monitor wildlife sightings
- Aggregate weather station data

### Retail Analysis
- Customer density mapping
- Store catchment area analysis
- Competitor location analysis

### Emergency Services
- Resource allocation by grid
- Incident density analysis
- Response time optimization

## Known Limitations

- Currently focused on British National Grid (BNG)
- Other grid systems (H3, S2, etc.) planned for future releases
- Custom gridding not fully ported yet

## Next Steps

- [View API Reference](../api/overview.md)
- [Check Examples](../examples/overview.md)
- [Learn about RasterX](./rasterx.md)
- [Learn about VectorX](./vectorx.md)

