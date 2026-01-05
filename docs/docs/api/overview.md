---
sidebar_position: 1
---

# API Reference Overview

GeoBrix provides APIs in three languages: Scala, Python, and SQL. All APIs provide access to the same underlying functionality with language-appropriate idioms.

## Function References

For detailed documentation of each function with parameters, return values, and examples:

- [RasterX Function Reference](./rasterx-functions.md) - Complete reference for all raster functions
- [GridX Function Reference](./gridx-functions.md) - Complete reference for all BNG grid functions
- [VectorX Function Reference](./vectorx-functions.md) - Complete reference for all vector functions

## API Languages

### Scala
The native implementation language, providing the most direct access to GeoBrix functionality.

[Scala API Documentation →](./scala.md)

### Python
Python bindings via PySpark, providing Pythonic access to all GeoBrix features.

[Python API Documentation →](./python.md)

### SQL
SQL functions registered in the Spark catalog, usable from any SQL context.

[SQL API Documentation →](./sql.md)

## Function Naming Convention

All GeoBrix SQL functions use the `gbx_` prefix to clearly identify them as GeoBrix functions:

| Package | Prefix | Example |
|---------|--------|---------|
| **RasterX** | `gbx_rst_` | `gbx_rst_boundingbox` |
| **GridX/BNG** | `gbx_bng_` | `gbx_bng_cellarea` |
| **VectorX** | `gbx_st_` | `gbx_st_legacyaswkb` |

This makes it easy to:
- Identify GeoBrix functions in your code
- Distinguish from Databricks built-in `st_*` functions
- Track usage and attribution

## Registration

Before using GeoBrix functions in Python or SQL, you must register them:

### Python

```python
from databricks.labs.gbx.rasterx import functions as rx
from databricks.labs.gbx.gridx.bng import functions as bx
from databricks.labs.gbx.vectorx import functions as vx

# Register each package
rx.register(spark)
bx.register(spark)
vx.register(spark)
```

### Scala

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.{functions => vx}

// Register each package
rx.register(spark)
bx.register(spark)
vx.register(spark)
```

### SQL

SQL functions are registered via Python or Scala. Once registered, they're available in any SQL context:

```sql
-- No registration needed in SQL
-- Functions are available after Python/Scala registration

SHOW FUNCTIONS LIKE 'gbx_*';
```

## API Categories

### RasterX Functions

Functions for raster data processing:

- **Accessors**: Get raster properties (width, height, bounds, metadata)
- **Constructors**: Load or create rasters
- **Transformations**: Clip, resample, reproject rasters
- **Grid Operations**: Raster to grid conversions
- **Band Operations**: Multi-band raster operations
- **Aggregations**: Combine and merge rasters

[View RasterX Functions →](./python.md#rasterx-functions)

### GridX Functions

Functions for grid indexing (BNG):

- **Cell Operations**: Create and manipulate grid cells
- **Coordinate Conversion**: Convert between coordinates and grid references
- **Grid Properties**: Get grid cell attributes
- **Spatial Indexing**: Use grid cells for efficient spatial operations

[View GridX Functions →](./python.md#gridx-functions)

### VectorX Functions

Functions for vector operations:

- **Geometry Conversion**: Convert legacy formats to WKB/WKT
- **Format Transformation**: Prepare data for Databricks spatial types

[View VectorX Functions →](./python.md#vectorx-functions)

## Common Patterns

### Pattern 1: Import and Register

```python
# Import functions with alias
from databricks.labs.gbx.rasterx import functions as rx

# Register with Spark
rx.register(spark)

# Use in DataFrame operations
df = rasters.select(rx.rst_boundingbox("tile"))

# Or use in SQL after registration
spark.sql("SELECT gbx_rst_boundingbox(tile) FROM rasters")
```

### Pattern 2: Mixed Language Usage

```python
# Register in Python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Use in Python
python_result = rasters.select(rx.rst_boundingbox("tile"))

# Use in SQL
sql_result = spark.sql("""
    SELECT gbx_rst_boundingbox(tile) as bbox
    FROM rasters
""")

# Both return the same results
```

### Pattern 3: Chaining Operations

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr

rx.register(spark)

# Chain multiple operations
result = (
    rasters
    .select(
        "path",
        rx.rst_clip("tile", expr("st_geomfromtext('POLYGON(...)')")).alias("clipped")
    )
    .select(
        "path",
        "clipped",
        rx.rst_boundingbox("clipped").alias("new_bounds")
    )
)
```

## Error Handling

### Check Function Availability

```python
# List registered functions
spark.sql("SHOW FUNCTIONS LIKE 'gbx_*'").show()

# Describe a specific function
spark.sql("DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox").show()
```

### Common Issues

#### Functions Not Registered

```python
# Error: Function 'gbx_rst_boundingbox' not found

# Solution: Register functions first
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)
```

#### Import Errors

```python
# Error: No module named 'databricks.labs.gbx'

# Solution: Ensure the wheel is installed on the cluster
# Check cluster libraries
```

## Performance Tips

### 1. Register Once

```python
# Register functions once at the start of your notebook
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Then use throughout the notebook
# Don't re-register in every cell
```

### 2. Use DataFrame API

```python
# Prefer DataFrame API for complex operations
result = df.select(rx.rst_boundingbox("tile"))

# Over repeated SQL calls
# result = spark.sql("SELECT gbx_rst_boundingbox(tile) FROM df")
```

### 3. Batch Operations

```python
# Process multiple columns at once
result = df.select(
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_metadata("tile").alias("metadata")
)
```

## Next Steps

- [Scala API Reference](./scala.md)
- [Python API Reference](./python.md)
- [SQL API Reference](./sql.md)
- [RasterX Functions](./rasterx-functions.md)
- [GridX Functions](./gridx-functions.md)
- [VectorX Functions](./vectorx-functions.md)
- [Package Documentation](../packages/overview.md)
- [Examples](../examples/overview.md)

