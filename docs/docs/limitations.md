---
sidebar_position: 9
---

# Known Limitations

GeoBrix Beta has some known limitations that will be addressed in future releases.

## Databricks Spatial Types

### Current State

The Beta does not yet support Databricks Spatial Types directly but is standardized to WKB or WKT where geometries are involved.

### Workaround

Convert GeoBrix output to Databricks types:

```python
from pyspark.sql.functions import expr

# Read with GeoBrix
df = spark.read.format("shapefile").load("/data/shapes.shp")

# Convert to GEOMETRY type
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Now use built-in ST functions
result = geometry_df.select(
    "geometry",
    expr("st_area(geometry)").alias("area")
)
```

### Example Notebooks

The provided notebooks (e.g., Shapefile Reader) have examples of converting to our built-in [GEOMETRY](https://docs.databricks.com/aws/en/sql/language-manual/data-types/geometry-type#gsc.tab=0) type and using our built-in ST Geospatial Functions.

### Future Plans

Support for Databricks Spatial Types is planned for future releases.

## Function Availability

### Not Yet Ported

A handful of functions from DBLabs Mosaic are not yet ported:

#### RasterX
- `rst_dtmfromgeoms` - Digital Terrain Model from geometries

#### VectorX
- `st_interpolateelevation` - Interpolate elevation values
- `st_triangulate` - Triangulation operations

### Spatial KNN

Spatial K-Nearest Neighbors is not yet ported:
- No KNN operations currently available
- H3 support for Geometry-based K-Ring and K-Loop not included

### Future Plans

These functions may be added in future releases based on user demand.

## Grid Systems

### Currently Supported

- **British National Grid (BNG)** - Fully supported
- **H3** - Uber's Hexagonal Hierarchical Spatial Index is supported by Databricks built-in functions

### Not Supported

- **Custom Gridding** - Not fully ported

### Future Plans

Additional grid systems will be considered based on user requirements.

## Compute Requirements

### Classic Clusters Only

GeoBrix requires Databricks Classic Clusters:
- **Not** currently compatible with Serverless compute
- Requires GDAL native libraries via init scripts
- Init scripts only supported on classic clusters

### Databricks Runtime

- **Minimum**: DBR 17.0
- **Recommended**: DBR 17.3 LTS or later
- Built to work with product spatial functions (DBR 17.1+)

## Format Support

### Raster Formats

Primary focus is on **GeoTIFF**:
- Have a named reader for GeoTIFF
- Other GDAL formats available but less tested
- May encounter issues with specialized formats
- Some format-specific features may not be supported

### Vector Formats

Most common formats supported via named readers:
- Shapefile
- GeoJSON
- GeoPackage
- File Geodatabase

Advanced features of some formats may have limited support.

## Performance Considerations

### Large Files

Very large single files may have performance implications:
- Use `sizeInMB` option to control splitting
- Consider pre-processing extremely large files
- Monitor memory usage on executors

### Complex Geometries

Very complex geometries may be slow to process:
- Simplify geometries when appropriate
- Use spatial indexing
- Consider breaking into smaller chunks

## Output Formats

### Current State

GeoBrix primarily outputs:
- **Raster**: Binary tile format
- **Vector**: WKB or WKT format

### Databricks Types

Not yet direct output to:
- Databricks GEOMETRY type
- Databricks GEOGRAPHY type

Must convert to built-in types, e.g. using `st_geomfromwkb()` or `st_geomfromwkt()`

## Platform Limitations

### Cloud Storage

Tested with:
- Unity Catalog Volumes

Some cloud storage configurations may have specific requirements.

### Unity Catalog

Full Unity Catalog integration:
- Can read from/write to Unity Catalog Volumes
- Can create tables in Unity Catalog
- Some advanced governance features may need testing

## Documentation

### Beta Status

As a Beta release:
- Documentation may be incomplete in some areas
- Examples may not cover all use cases
- API may evolve based on feedback

### User Guide

The PDF User Guide may contain additional information not yet fully migrated to this documentation.

## Reporting Issues

If you encounter limitations not listed here:

1. Check the [GitHub Issues](https://github.com/databrickslabs/geobrix/issues)
2. File a new issue if not already reported
3. Provide detailed reproduction steps
4. Include DBR version and configuration

## Workarounds and Best Practices

### For Missing Functions

- Use Databricks built-in spatial functions when possible
- Combine GeoBrix with other libraries if needed
- Request priority functions via GitHub Issues

### For Format Issues

- Convert data to well-supported formats (GeoTIFF, Shapefile, GeoJSON)
- Consider standard projections (EPSG:4326, EPSG:3857)
- Test with small samples first

### For Performance Issues

- Optimize file sizes and partitioning
- Use appropriate chunk sizes
- Cache intermediate results
- Monitor executor memory

## Future Roadmap

Potential improvements include:

1. **Direct Databricks Type Support**: Native GEOMETRY/GEOGRAPHY
2. **Additional Functions**: Port identified Mosaic functions
3. **Grid Systems**: Port custom grids
4. **Broader Format Support**: Additional GDAL formats
5. **Serverless Support**: If technically feasible

## Next Steps

- [View Installation Guide](./installation.md)
- [Check Quick Start](./quick-start.md)
- [Review API Documentation](./api/overview.md)
- [Get Support](./support.md)

