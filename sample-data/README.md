# GeoBrix Test Data

Sample geospatial data for GeoBrix documentation tests and examples.

## Structure

```
sample-data/Volumes/main/default/
├── geobrix_samples/geobrix-examples/  (Downloaded sample data - gitignored)
│   ├── nyc/           (New York City datasets)
│   ├── london/        (London datasets)
│   └── test-subfolder/
└── test-data/         (Test datasets - committed to git)
    ├── generic_features.geojson
    └── geobrix-examples/   (Minimal doc-test bundle, same layout as above)
        └── nyc/
            ├── boroughs/   (nyc_boroughs.geojson, nyc_boroughs.geojsonl)
            ├── subway/     (nyc_subway.shp.zip)
            ├── geopackage/ (nyc_complete.gpkg)
            └── sentinel2/  (nyc_sentinel2_red.tif, small)
```

**Path token for doc tests:** Use `sample-data/Volumes/main/default/test-data` in place of `/Volumes/main/default/geobrix_samples` so examples work with the minimal bundle (e.g. `sample-data/Volumes/main/default/test-data/geobrix-examples/nyc/subway/nyc_subway.shp.zip`). This structure mirrors Unity Catalog Volumes in Databricks.

## Data Inventory

### NYC Datasets (~375 MB)

| Category | File | Format | Size | Features | Notes |
|----------|------|--------|------|----------|-------|
| **Vector - Boundaries** |
| Boroughs | `nyc_boroughs.geojson` | GeoJSON | 3.0 MB | 5 | NYC borough boundaries |
| Boroughs | `nyc_boroughs.geojsonl` | GeoJSONSeq | 3.2 MB | 5 | ✅ Newline-delimited (auto-generated) |
| Boroughs | `nyc_boroughs.zip` | Shapefile | 1.3 MB | 5 | ✅ Zipped shapefile |
| Taxi Zones | `nyc_taxi_zones.geojson` | GeoJSON | 3.7 MB | 263 | TLC taxi zones |
| Taxi Zones | `nyc_taxi_zones.zip` | Shapefile | 1.7 MB | 263 | ✅ Zipped shapefile |
| Neighborhoods | `nyc_nta.geojson` | GeoJSON | 4.1 MB | 250 | Neighborhood Tabulation Areas |
| **Vector - Features** |
| Parks | `nyc_parks.zip` | Shapefile | 2.1 MB | 2065 | NYC parks and playgrounds |
| Subway | `nyc_subway.zip` | Shapefile | 118 KB | 2120 | ✅ Subway station locations |
| **Raster - Imagery** |
| Sentinel-2 | `nyc_sentinel2_red.tif` | GeoTIFF | 205 MB | - | Red band imagery |
| **Raster - Elevation** |
| Elevation West | `srtm_n40w074.tif` | GeoTIFF | 24.7 MB | - | SRTM 90m DEM (GeoTIFF) |
| Elevation East | `srtm_n40w073.tif` | GeoTIFF | 24.7 MB | - | SRTM 90m DEM (GeoTIFF) |
| **Multi-Format** |
| Complete Package | `nyc_complete.gpkg` | GeoPackage | 7.1 MB | - | Multi-layer (boroughs, zones, parks, subway) |
| Complete FileGDB | `NYC_Sample.gdb/` | FileGDB | ~1 MB | - | Multi-feature class geodatabase |
| **Weather** |
| HRRR Weather | `hrrr_nyc_*.grib2` | GRIB2 | 135 MB | - | High-resolution weather model |

### London Datasets (~118 MB)

| Category | File | Format | Size | Features | Notes |
|----------|------|--------|------|----------|-------|
| Postcodes | `london_postcodes.geojson` | GeoJSON | 0.9 MB | - | London postcode boundaries |
| Sentinel-2 | `london_sentinel2_red.tif` | GeoTIFF | 92.7 MB | - | Red band imagery |
| Elevation | `srtm_n51w001.tif` | GeoTIFF | 24.7 MB | - | SRTM 90m DEM (GeoTIFF) |

### Minimal doc-test bundle (`test-data/geobrix-examples/`)

**Purpose**: Small versions of all complete-bundle formats for CI and doc tests. Same relative paths as full bundle. Extracted by bounding box so joins and spatial operations match (NYC: center of Manhattan; London: center of London).

**Generate**: Run after full bundle download:
```bash
gbx:data:generate-minimal-bundle
```
Or: `python3 sample-data/generate-minimal-bundle.py` (in Docker or with geopandas/GDAL).

**Options** (defaults can be overridden):
- **NYC center**: `--nyc-lon`, `--nyc-lat` (default: -73.9857, 40.7484, Manhattan)
- **London center**: `--london-lon`, `--london-lat` (default: -0.1276, 51.5074)
- **Bbox size**: `--bbox-size` half-width/height in degrees (default: 0.02)
- **Vector row limit**: `--max-rows` (default: 10); raster is full data within bbox

**Contents**: NYC (boroughs, taxi-zones, neighborhoods, subway, parks, sentinel2, elevation, geopackage, filegdb, hrrr), London (postcodes, boroughs, sentinel2, elevation).

**London CRS:** London data may be in BNG (British National Grid, EPSG:27700) rather than WGS84 (4326). The generator passes London center in WGS84 and transforms the clip bbox to the source CRS when clipping (so BNG rasters are clipped correctly). Vector output is always WGS84; raster output keeps the source CRS (e.g. London rasters stay BNG if the source is BNG).

**Path token for doc tests**: Set `GBX_SAMPLE_DATA_ROOT` to the minimal bundle root (e.g. `sample-data/Volumes/main/default/test-data` or absolute path) so tests use the minimal bundle; otherwise they use `/Volumes/main/default/geobrix_samples`.

### Test Data (Generic Schemas) (~1 KB)

**Purpose**: Generic test datasets with standardized schemas for documentation and testing.

| Category | File | Format | Size | Features | Schema |
|----------|------|--------|------|----------|--------|
| Generic Features | `test-data/generic_features.geojson` | GeoJSON | ~1 KB | 5 | ✅ Standardized (committed) |

**Standardized Schema**:
- **id**: Unique identifier (integer)
- **name**: Human-readable name (string)
- **type**: Feature category (park, building, bridge, etc.)
- **description**: Detailed description (string)
- **geom_0**: WKB geometry (Polygon, Point, LineString)
- **area** or **length**: Measurements (double, geometry-type dependent)

**Features**:
1. Central Park - Polygon (341.15 hectares)
2. Empire State Building - Point landmark
3. Brooklyn Bridge - LineString (1.825 km)
4. Times Square - Point plaza
5. Prospect Park - Polygon (237.0 hectares)

**Use Cases**:
- Documentation examples requiring predictable column names
- Schema validation tests
- Generic reader examples
- Column name standardization demonstrations

**Total Data Size**: ~795 MB

## Setup

### Option 1: Essential Bundle Only (~355 MB)
Minimal datasets for basic testing:
```bash
docker exec geobrix-dev python3 /root/geobrix/sample-data/download-essential-bundle.py
```

Includes:
- NYC: boroughs, taxi zones, Sentinel-2, elevation
- London: postcodes, Sentinel-2, elevation

### Option 2: Complete Bundle (~795 MB)
Full datasets including shapefiles, GeoPackage, FileGDB:
```bash
docker exec geobrix-dev python3 /root/geobrix/sample-data/download-complete-bundle.py
```

Adds to essential:
- NYC: neighborhoods, parks, subway, GeoPackage, FileGDB, weather data
- Additional elevation tiles

### Databricks notebooks

Use the same scripts and paths in Databricks notebooks:

1. Create a Unity Catalog volume: `CREATE VOLUME IF NOT EXISTS main.default.geobrix_samples`
2. In a notebook cell, run the download script (e.g. copy from `sample-data/download-essential-bundle.py` or run it via `%run` if the repo is in your workspace)
3. Use the Volumes path in your code: `/Volumes/main/default/geobrix_samples/geobrix-examples/` (e.g. `nyc/taxi-zones/nyc_taxi_zones.geojson`, `london/postcodes/london_postcodes.geojson`)

All documentation examples that reference sample data use this path. There are no separate notebook-specific download scripts.

## Using Zipped Shapefiles

**Why `.zip` format?**
- Shapefiles consist of multiple files (.shp, .shx, .dbf, .prj, .cpg)
- Real-world distribution format
- Easier to manage and transfer

**GeoBrix supports `.zip` directly:**
```python
# Read zipped shapefile
df = spark.read.format("shapefile").load("/path/to/shapefile.zip")
```

**Available zipped shapefiles:**
- ✅ `nyc_boroughs.zip` - 5 boroughs
- ✅ `nyc_taxi_zones.zip` - 263 taxi zones
- ✅ `nyc_subway.zip` - 2120 stations
- ⚠️ `nyc_parks.zip` - 2065 parks (may have data validation issues)

## GeoJSON Format Notes

**Standard GeoJSON (FeatureCollection)**
Use `.option("multi", "false")` for standard GeoJSON files:
```python
df = spark.read.format("geojson") \
    .option("multi", "false") \
    .load("/path/to/file.geojson")
```

**GeoJSONSeq (newline-delimited)**
Default behavior or explicit `.option("multi", "true")`:
```python
df = spark.read.format("geojson").load("/path/to/file.geojsonl")
```

## Data Sources

- **NYC Open Data**: https://opendata.cityofnewyork.us/
- **Sentinel-2**: Microsoft Planetary Computer
- **SRTM Elevation**: AWS Terrain Tiles
- **HRRR Weather**: NOAA Big Data Program

## Testing

```bash
# Run all documentation tests
./scripts/ci/run-doc-tests.sh local

# Run readers tests only
docker exec geobrix-dev pytest docs/tests/python/readers/ -v
```

## Docker Mount

In Docker container, sample-data is mounted to:
```
Host:      /path/to/geobrix/sample-data/
Container: /root/geobrix/sample-data/
```

Unity Catalog path simulation:
```
Container: /Volumes/main/default/geobrix_samples/
Maps to:   /root/geobrix/sample-data/Volumes/main/default/geobrix_samples/
```

## Maintenance

### Adding New Data

1. Download/create data files
2. Place in appropriate subdirectory under `geobrix-examples/`
3. Update this README
4. Add corresponding tests in `docs/tests/python/readers/`

### Converting to Zipped Shapefile

```python
import geopandas as gpd
import zipfile
from pathlib import Path

# Read source data
gdf = gpd.read_file("source.geojson")

# Write shapefile components
gdf.to_file("output.shp")

# Zip all components
with zipfile.ZipFile("output.zip", "w") as zipf:
    for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
        f = Path(f"output{ext}")
        if f.exists():
            zipf.write(f, f.name)
            f.unlink()  # Remove unzipped file
```

## Troubleshooting

**Q: Shapefiles won't read**
- Verify `.zip` file contains all required components (.shp, .shx, .dbf)
- Check CRS is defined (.prj file)
- Ensure file is not corrupted

**Q: GeoJSON returns 0 rows**
- Add `.option("multi", "false")` for standard GeoJSON
- Check file is not empty or malformed
- Verify GeoJSON has `type: "FeatureCollection"`

**Q: Memory errors with large files**
- Use `.option("sizeInMB", "32")` for raster readers
- Use `.option("chunkSize", "50000")` for vector readers
- Repartition after reading: `.repartition(200)`

## License

Test data compiled from public sources for GeoBrix testing and documentation.
Each dataset retains its original license. See individual data sources for details.
