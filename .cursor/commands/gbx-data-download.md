# Download Sample Geospatial Data

Downloads sample geospatial data bundles (essential and/or complete) for testing and documentation.

## Usage

```bash
bash .cursor/commands/gbx-data-download.sh [OPTIONS]
```

## Options

- `--bundle <type>` - Which bundle to download: `essential`, `complete`, or `both` (default: both)
- `--force` - Force re-download even if data already exists
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Download both essential and complete bundles
bash .cursor/commands/gbx-data-download.sh

# Download only essential bundle
bash .cursor/commands/gbx-data-download.sh --bundle essential

# Force re-download of complete bundle
bash .cursor/commands/gbx-data-download.sh --bundle complete --force

# Download with logging
bash .cursor/commands/gbx-data-download.sh --log data-download.log
```

## Data Bundles

### Essential Bundle
- NYC Boroughs (GeoJSON)
- NYC Taxi Zones (GeoJSON)
- NYC Neighborhoods (GeoJSON)
- London Boroughs (GeoJSON)
- NYC Sentinel-2 imagery (GeoTIFF)
- London Sentinel-2 imagery (GeoTIFF)
- SRTM elevation data (HGT)

### Complete Bundle
- NYC Parks (zipped shapefile)
- NYC Subway (zipped shapefile)
- NYC Sample FileGDB (zipped)
- NYC Complete GeoPackage (multi-layer)
- HRRR weather data (GRIB2)

## Data Location

- **Base Path**: `sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/`
- **Docker Mount**: `/Volumes/main/default/geobrix_samples/geobrix-examples/`

## Notes

- Runs Python download scripts from `sample-data/` directory
- Essential bundle is required for most tests
- Complete bundle includes additional formats (shapefiles, GPKG, FileGDB, GRIB2)
- Downloads from public data sources (NYC Open Data, London Datastore, etc.)
- Default log location: `test-logs/` (if filename only provided)
