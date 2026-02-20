"""
Vector Data page code examples. Snippets are constants (payload only in docs).

Single source of truth for docs/docs/sample-data/vector-data.mdx.
Tested by: docs/tests/python/sample_data/test_vector_data.py
"""

DOWNLOAD_NYC_TAXI_ZONES = """# Download NYC Taxi Zones
import requests
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# NYC Open Data - Socrata API (dataset: 8meu-9t5y)
url = 'https://data.cityofnewyork.us/resource/8meu-9t5y.geojson?$limit=300'

# Output path (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc/taxi-zones")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "nyc_taxi_zones.geojson"

# Download
response = requests.get(url)
response.raise_for_status()

output_file.write_bytes(response.content)

print(f"✅ Downloaded NYC Taxi Zones ({len(response.content):,} bytes)")
print(f"   Path: {output_file}")

# Verify by reading
nyc_zones = spark.read.format("geojson_ogr").load(str(output_file))
print(f"   Features: {nyc_zones.count()}")
nyc_zones.select("zone", "borough").show(5)"""

DOWNLOAD_NYC_TAXI_ZONES_output = """✅ Downloaded NYC Taxi Zones (1,234,567 bytes)
   Path: /Volumes/.../nyc/taxi-zones/nyc_taxi_zones.geojson
   Features: 263
+------------------+---------+
|zone              |borough  |
+------------------+---------+
|Newark Airport    |EWR      |
|Jamaica Bay       |Unknown  |
|Allerton/Pelham...|Bronx    |
|...               |...      |
+------------------+---------+"""

USAGE_NYC_TAXI_ZONES = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
nyc_zones = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/taxi-zones/nyc_taxi_zones.geojson")"""

DOWNLOAD_LONDON_POSTCODES = """# Download London Postcode Zones
import requests
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# GitHub public data
url = 'https://raw.githubusercontent.com/sjwhitworth/london_geojson/master/london_postcodes.json'

# Output path (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/london/postcodes")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "london_postcodes.geojson"

# Download
response = requests.get(url)
response.raise_for_status()

output_file.write_bytes(response.content)

print(f"✅ Downloaded London Postcode Zones ({len(response.content):,} bytes)")
print(f"   Path: {output_file}")

# Verify by reading
london_zones = spark.read.format("geojson_ogr").load(str(output_file))
print(f"   Features: {london_zones.count()}")
london_zones.select("name").show(5)"""

DOWNLOAD_LONDON_POSTCODES_output = """✅ Downloaded London Postcode Zones (912,345 bytes)
   Path: /Volumes/.../london/postcodes/london_postcodes.geojson
   Features: 119
+-----+
|name |
+-----+
|E1   |
|E2   |
|E3   |
|...  |
+-----+"""

USAGE_LONDON_POSTCODES = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
london_zones = spark.read.format("geojson_ogr").load(f"{sample_path}/london/postcodes/london_postcodes.geojson")"""

DOWNLOAD_NYC_BOROUGHS = """# Download NYC Boroughs
import requests
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# NYC Open Data - Socrata API (dataset: gthc-hcne)
url = 'https://data.cityofnewyork.us/resource/gthc-hcne.geojson?$limit=10'

# Output path (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc/boroughs")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "nyc_boroughs.geojson"

# Download
response = requests.get(url)
response.raise_for_status()

output_file.write_bytes(response.content)

file_size = len(response.content) / 1024
print(f"✅ Downloaded NYC Boroughs ({file_size:.1f} KB)")
print(f"   Path: {output_file}")

# Verify
nyc_boroughs = spark.read.format("geojson_ogr").load(str(output_file))
print(f"   Boroughs: {nyc_boroughs.count()}")
nyc_boroughs.select("boro_name", "boro_code").show()"""

DOWNLOAD_NYC_BOROUGHS_output = """✅ Downloaded NYC Boroughs (12.3 KB)
   Path: /Volumes/.../nyc/boroughs/nyc_boroughs.geojson
   Boroughs: 5
+----------+---------+
|boro_name |boro_code|
+----------+---------+
|Manhattan |1        |
|Bronx     |2        |
|...       |...      |
+----------+---------+"""

USAGE_NYC_BOROUGHS = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
nyc_boroughs = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc/boroughs/nyc_boroughs.geojson")"""

DOWNLOAD_NYC_NTA = """# Download NYC Neighborhood Tabulation Areas (NTA)
import requests
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# NYC Open Data - Socrata API (dataset: 9nt8-h7nd)
url = 'https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson?$limit=250'

# Output path (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc-neighborhoods")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "nyc_nta.geojson"

print("Downloading NYC Neighborhoods (this may take a moment)...")
response = requests.get(url)
response.raise_for_status()

output_file.write_bytes(response.content)

file_size = len(response.content) / (1024 * 1024)
print(f"✅ Downloaded NYC Neighborhoods ({file_size:.1f} MB)")
print(f"   Path: {output_file}")

# Verify
nyc_nta = spark.read.format("geojson_ogr").load(str(output_file))
print(f"   Neighborhoods: {nyc_nta.count()}")
nyc_nta.select("ntaname", "boroname").show(10)"""

DOWNLOAD_NYC_NTA_output = """✅ Downloaded NYC Neighborhoods (4.1 MB)
   Path: /Volumes/.../nyc-neighborhoods/nyc_nta.geojson
   Neighborhoods: 250
+------------------+----------+
|ntaname           |boroname  |
+------------------+----------+
|Battery Park City |Manhattan |
|Tribeca           |Manhattan |
|...                 |...       |
+------------------+----------+"""

USAGE_NYC_NTA = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
nyc_neighborhoods = spark.read.format("geojson_ogr").load(f"{sample_path}/nyc-neighborhoods/nyc_nta.geojson")"""

DOWNLOAD_LONDON_BOROUGHS = """# Download London Boroughs (GeoJSON)
# Converts official shapefile to GeoJSON for use with geojson_ogr reader.
import requests
from pathlib import Path
import zipfile
import io

import geopandas as gpd

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# London Datastore - Statistical GIS Boundary Files (shapefile; we convert to GeoJSON)
url = 'https://data.london.gov.uk/download/statistical-gis-boundary-files-london/9ba8c833-6370-4b11-abdc-314aa020d5e0/statistical-gis-boundaries-london.zip'

output_dir = Path(f"{sample_path}/london/boroughs")
output_dir.mkdir(parents=True, exist_ok=True)
tmp_dir = output_dir / "_tmp"
tmp_dir.mkdir(parents=True, exist_ok=True)

print("Downloading London Boroughs...")
response = requests.get(url)
response.raise_for_status()

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    for name in z.namelist():
        if 'London_Borough' in name:
            z.extract(name, tmp_dir)

shp_files = list(tmp_dir.glob("**/London_Borough*.shp"))
if shp_files:
    gdf = gpd.read_file(str(shp_files[0]))
    out_file = output_dir / "london_boroughs.geojson"
    gdf.to_file(str(out_file), driver='GeoJSON')
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"✅ Downloaded London Boroughs ({len(response.content) / (1024*1024):.1f} MB → GeoJSON)")
    print(f"   Path: {out_file}")
    london_boroughs = spark.read.format("geojson_ogr").load(str(out_file))
    print(f"   Boroughs: {london_boroughs.count()}")
    london_boroughs.select("NAME").show(5)"""

DOWNLOAD_LONDON_BOROUGHS_output = """✅ Downloaded London Boroughs (2.1 MB → GeoJSON)
   Path: .../london/boroughs/london_boroughs.geojson
   Boroughs: 33
+------------------+
|NAME              |
+------------------+
|Westminster       |
|Camden            |
|Islington         |
|Hackney           |
|Tower Hamlets     |
+------------------+"""

USAGE_LONDON_BOROUGHS = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
london_boroughs = spark.read.format("geojson_ogr").load(f"{sample_path}/london/boroughs/london_boroughs.geojson")"""

DOWNLOAD_NYC_PARKS = """# Download NYC Parks Shapefile
import requests
import zipfile
from pathlib import Path
import io

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# NYC Open Data API - Shapefile export
url = 'https://data.cityofnewyork.us/api/geospatial/enfh-gkve?method=export&format=Shapefile'

# Output directory (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc/parks")
output_dir.mkdir(parents=True, exist_ok=True)

print("Downloading NYC Parks Shapefile (~5MB)...")
response = requests.get(url)
response.raise_for_status()

# Extract shapefile components
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.extractall(output_dir)

file_size = len(response.content) / (1024 * 1024)
print(f"✅ Downloaded NYC Parks Shapefile ({file_size:.1f} MB)")
print(f"   Path: {output_dir}/")

# Find extracted shapefile
shp_files = list(output_dir.glob("*.shp"))
if shp_files:
    shp_file = str(shp_files[0])
    print(f"   Shapefile: {shp_file}")
    
    # Verify
    parks = spark.read.format("shapefile_ogr").load(shp_file)
    print(f"   Parks: {parks.count()}")
    parks.select("name311", "borough", "acres").show(5)"""

DOWNLOAD_NYC_PARKS_output = """✅ Downloaded NYC Parks Shapefile (5.2 MB)
   Path: .../nyc/parks/
   Shapefile: .../nyc/parks/geo_export_....shp
   Parks: 2100+
+------------------+--------+-----+
|name311           |borough |acres|
+------------------+--------+-----+
|Fort Greene Park  |BROOKLYN|0.5  |
|...               |...     |...  |
+------------------+--------+-----+"""

USAGE_NYC_PARKS = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
parks = spark.read.format("shapefile_ogr").load(f"{sample_path}/nyc/parks/*.shp")"""

DOWNLOAD_NYC_SUBWAY = """# Download NYC Subway Stations Shapefile
import requests
import zipfile
from pathlib import Path
import io

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# NYC Open Data - Subway stations
url = 'https://data.ny.gov/api/geospatial/i9wp-a4ja?method=export&format=Shapefile'

# Output directory (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc/subway")
output_dir.mkdir(parents=True, exist_ok=True)

print("Downloading NYC Subway Stations Shapefile (~1MB)...")
response = requests.get(url)
response.raise_for_status()

# Extract shapefile components
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.extractall(output_dir)

file_size = len(response.content) / (1024 * 1024)
print(f"✅ Downloaded NYC Subway Stations Shapefile ({file_size:.1f} MB)")
print(f"   Path: {output_dir}/")

# Find extracted shapefile
shp_files = list(output_dir.glob("*.shp"))
if shp_files:
    shp_file = str(shp_files[0])
    print(f"   Shapefile: {shp_file}")
    
    # Verify
    subway = spark.read.format("shapefile_ogr").load(shp_file)
    print(f"   Stations: {subway.count()}")
    subway.select("Station_Na", "Line").show(5)"""

DOWNLOAD_NYC_SUBWAY_output = """✅ Downloaded NYC Subway Stations Shapefile (1.1 MB)
   Path: .../nyc/subway/
   Shapefile: .../nyc/subway/geo_export_....shp
   Stations: 2120
+------------------+------------------+
|Station_Na        |Line              |
+------------------+------------------+
|72 St             |N Q R W           |
|Lexington Av/63   |F Q               |
|...               |...               |
+------------------+------------------+"""

USAGE_NYC_SUBWAY = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
subway = spark.read.format("shapefile_ogr").load(f"{sample_path}/nyc/subway/*.shp")"""

CREATE_GEOPACKAGE_NYC = """# Create NYC Multi-Layer GeoPackage
# Requires: Essential Bundle and Complete Bundle shapefiles already downloaded

%pip install geopandas --quiet

import geopandas as gpd
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Output directory and file
output_dir = Path(f"{sample_path}/nyc/geopackage")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "nyc_complete.gpkg"

print("Creating NYC Multi-Layer GeoPackage...")
print("This combines multiple datasets into one file with separate layers.")
print()

# Layer 1: Boroughs
print("  Adding layer: boroughs...")
boroughs_file = Path(f"{sample_path}/nyc/boroughs/nyc_boroughs.geojson")
boroughs = gpd.read_file(str(boroughs_file))
boroughs.to_file(str(output_file), layer='boroughs', driver='GPKG')
print(f"    ✅ {len(boroughs)} features")

# Layer 2: Taxi Zones
print("  Adding layer: taxi_zones...")
zones_file = Path(f"{sample_path}/nyc/taxi-zones/nyc_taxi_zones.geojson")
zones = gpd.read_file(str(zones_file))
zones.to_file(str(output_file), layer='taxi_zones', driver='GPKG', mode='a')
print(f"    ✅ {len(zones)} features")

# Layer 3: Parks
parks_dir = Path(f"{sample_path}/nyc/parks")
park_files = list(parks_dir.glob("*.shp"))
if park_files:
    print("  Adding layer: parks...")
    parks = gpd.read_file(str(park_files[0]))
    parks.to_file(str(output_file), layer='parks', driver='GPKG', mode='a')
    print(f"    ✅ {len(parks)} features")

# Layer 4: Subway Stations
subway_dir = Path(f"{sample_path}/nyc/subway")
subway_files = list(subway_dir.glob("*.shp"))
if subway_files:
    print("  Adding layer: subway_stations...")
    subway = gpd.read_file(str(subway_files[0]))
    subway.to_file(str(output_file), layer='subway_stations', driver='GPKG', mode='a')
    print(f"    ✅ {len(subway)} features")

file_size = output_file.stat().st_size / (1024 * 1024)
print()
print(f"✅ Created NYC Multi-Layer GeoPackage ({file_size:.1f} MB)")
print(f"   Path: {output_file}")
print(f"   Layers: boroughs, taxi_zones, parks, subway_stations")"""

CREATE_GEOPACKAGE_NYC_output = """Creating NYC Multi-Layer GeoPackage...
This combines multiple datasets into one file with separate layers.

  Adding layer: boroughs...
    ✅ 5 features
  Adding layer: taxi_zones...
    ✅ 263 features
  Adding layer: parks...
    ✅ 2100+ features
  Adding layer: subway_stations...
    ✅ 2120 features

✅ Created NYC Multi-Layer GeoPackage (7.1 MB)
   Path: .../nyc/geopackage/nyc_complete.gpkg
   Layers: boroughs, taxi_zones, parks, subway_stations"""

USAGE_GEOPACKAGE_NYC = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Read specific layer
boroughs = spark.read.format("gpkg") \\
    .option("layerName", "boroughs") \\
    .load(f"{sample_path}/nyc/geopackage/nyc_complete.gpkg")

# Read another layer
parks = spark.read.format("gpkg") \\
    .option("layerName", "parks") \\
    .load(f"{sample_path}/nyc/geopackage/nyc_complete.gpkg")"""

CREATE_FILEGDB_NYC = """# Create NYC Multi-Feature Class FileGDB
# Requires: Essential Bundle datasets, Format Examples shapefiles, AND FileGDB driver

from osgeo import ogr, osr
import geopandas as gpd
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Output directory and geodatabase
output_dir = Path(f"{sample_path}/nyc/filegdb")
output_dir.mkdir(parents=True, exist_ok=True)
output_gdb = output_dir / "NYC_Sample.gdb"

print("Creating NYC File Geodatabase...")
print("This creates an Esri FileGDB with multiple feature classes.")
print()

# Create FileGDB
driver = ogr.GetDriverByName('FileGDB')
if driver is None:
    print("⚠️  FileGDB driver not available. Requires GDAL with FileGDB support.")
    print("    Alternative: Use OpenFileGDB driver for reading existing FileGDBs.")
else:
    # Remove if exists
    import shutil
    if output_gdb.exists():
        shutil.rmtree(output_gdb)
    
    gdb = driver.CreateDataSource(str(output_gdb))
    
    if gdb is None:
        print("⚠️  Could not create FileGDB")
    else:
        # Define spatial reference (WGS84)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        
        # Feature Class 1: Boroughs
        print("  Adding feature class: NYC_Boroughs...")
        boroughs_file = Path(f"{sample_path}/nyc/boroughs/nyc_boroughs.geojson")
        boroughs = gpd.read_file(str(boroughs_file))
        layer_boroughs = gdb.CreateLayer('NYC_Boroughs', srs, ogr.wkbMultiPolygon)
        
        # Add fields
        layer_boroughs.CreateField(ogr.FieldDefn('BORO_NAME', ogr.OFTString))
        layer_boroughs.CreateField(ogr.FieldDefn('BORO_CODE', ogr.OFTString))
        
        # Add features
        for idx, row in boroughs.iterrows():
            feature = ogr.Feature(layer_boroughs.GetLayerDefn())
            feature.SetField('BORO_NAME', str(row.get('boro_name', '')))
            feature.SetField('BORO_CODE', str(row.get('boro_code', '')))
            feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
            layer_boroughs.CreateFeature(feature)
            feature = None
        print(f"    ✅ {len(boroughs)} features")
        
        # Feature Class 2: Taxi Zones
        print("  Adding feature class: NYC_TaxiZones...")
        zones_file = Path(f"{sample_path}/nyc/taxi-zones/nyc_taxi_zones.geojson")
        zones = gpd.read_file(str(zones_file))
        layer_zones = gdb.CreateLayer('NYC_TaxiZones', srs, ogr.wkbMultiPolygon)
        
        # Add fields
        layer_zones.CreateField(ogr.FieldDefn('ZONE_NAME', ogr.OFTString))
        layer_zones.CreateField(ogr.FieldDefn('LOCATION_ID', ogr.OFTInteger))
        
        # Add features
        for idx, row in zones.iterrows():
            feature = ogr.Feature(layer_zones.GetLayerDefn())
            feature.SetField('ZONE_NAME', str(row.get('zone', '')))
            feature.SetField('LOCATION_ID', int(row.get('LocationID', 0)))
            feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
            layer_zones.CreateFeature(feature)
            feature = None
        print(f"    ✅ {len(zones)} features")
        
        # Feature Class 3: Parks (if available)
        parks_dir = Path(f"{sample_path}/nyc/parks")
        park_files = list(parks_dir.glob("*.shp"))
        if park_files:
            print("  Adding feature class: NYC_Parks...")
            parks = gpd.read_file(str(park_files[0]))
            layer_parks = gdb.CreateLayer('NYC_Parks', srs, ogr.wkbMultiPolygon)
            
            # Add fields
            layer_parks.CreateField(ogr.FieldDefn('PARK_NAME', ogr.OFTString))
            layer_parks.CreateField(ogr.FieldDefn('ACRES', ogr.OFTReal))
            
            # Add features (limit to first 100 for size)
            for idx, row in parks.head(100).iterrows():
                feature = ogr.Feature(layer_parks.GetLayerDefn())
                feature.SetField('PARK_NAME', str(row.get('name311', '')))
                feature.SetField('ACRES', float(row.get('acres', 0)))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer_parks.CreateFeature(feature)
                feature = None
            print(f"    ✅ {min(len(parks), 100)} features")
        
        # Feature Class 4: Subway Stations (if available)
        subway_dir = Path(f"{sample_path}/nyc/subway")
        subway_files = list(subway_dir.glob("*.shp"))
        if subway_files:
            print("  Adding feature class: NYC_SubwayStations...")
            subway = gpd.read_file(str(subway_files[0]))
            layer_subway = gdb.CreateLayer('NYC_SubwayStations', srs, ogr.wkbPoint)
            
            # Add fields
            layer_subway.CreateField(ogr.FieldDefn('STATION_NAME', ogr.OFTString))
            layer_subway.CreateField(ogr.FieldDefn('LINE', ogr.OFTString))
            
            # Add features
            for idx, row in subway.iterrows():
                feature = ogr.Feature(layer_subway.GetLayerDefn())
                feature.SetField('STATION_NAME', str(row.get('Station_Na', '')))
                feature.SetField('LINE', str(row.get('Line', '')))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer_subway.CreateFeature(feature)
                feature = None
            print(f"    ✅ {len(subway)} features")
        
        # Close geodatabase
        gdb = None
        
        # Get size
        gdb_size = sum(f.stat().st_size for f in output_gdb.rglob('*') if f.is_file()) / (1024 * 1024)
        print()
        print(f"✅ Created NYC File Geodatabase ({gdb_size:.1f} MB)")
        print(f"   Path: {output_gdb}")
        print(f"   Feature Classes: NYC_Boroughs, NYC_TaxiZones, NYC_Parks, NYC_SubwayStations")"""

CREATE_FILEGDB_NYC_output = """Creating NYC File Geodatabase...
This creates an Esri FileGDB with multiple feature classes.

  Adding feature class: NYC_Boroughs...
    ✅ 5 features
  Adding feature class: NYC_TaxiZones...
    ✅ 263 features
  Adding feature class: NYC_Parks...
    ✅ 100 features
  Adding feature class: NYC_SubwayStations...
    ✅ 2120 features

✅ Created NYC File Geodatabase (3.2 MB)
   Path: .../nyc/filegdb/NYC_Sample.gdb
   Feature Classes: NYC_Boroughs, NYC_TaxiZones, NYC_Parks, NYC_SubwayStations"""

USAGE_FILEGDB_NYC = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Read FileGDB feature classes (created in Complete Bundle)
boroughs = spark.read.format("filegdb") \\
    .option("layerName", "NYC_Boroughs") \\
    .load(f"{sample_path}/nyc/filegdb/NYC_Sample.gdb")

zones = spark.read.format("filegdb") \\
    .option("layerName", "NYC_TaxiZones") \\
    .load(f"{sample_path}/nyc/filegdb/NYC_Sample.gdb")

parks = spark.read.format("filegdb") \\
    .option("layerName", "NYC_Parks") \\
    .load(f"{sample_path}/nyc/filegdb/NYC_Sample.gdb")

subway = spark.read.format("filegdb") \\
    .option("layerName", "NYC_SubwayStations") \\
    .load(f"{sample_path}/nyc/filegdb/NYC_Sample.gdb")"""
