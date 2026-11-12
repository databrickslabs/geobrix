# Download Sample Geospatial Data

Downloads sample geospatial data bundles (essential and/or complete) for testing and documentation.

## Usage

```bash
bash scripts/commands/gbx-data-download.sh [OPTIONS]
```

## Options

- `--bundle <type>` - Which bundle to download: `essential`, `complete`, or `both` (default: both)
- `--force` - Refused (exit 2): the bundles skip files that exist. Use `--refresh <rel>` to re-download a file.
- `--refresh <rel>` - Delete one file under `geobrix-examples` (relative path, no `..`), then re-download it. Repeatable. Pick the bundle that owns the file.
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Download both essential and complete bundles
bash scripts/commands/gbx-data-download.sh

# Download only essential bundle
bash scripts/commands/gbx-data-download.sh --bundle essential

# Re-download only the NYC neighborhoods file (it lives in the complete bundle)
bash scripts/commands/gbx-data-download.sh --bundle complete --refresh nyc/neighborhoods/nyc_nta.geojson

# Download with logging
bash scripts/commands/gbx-data-download.sh --log data-download.log
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
