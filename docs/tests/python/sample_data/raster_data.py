"""
Raster Data page code examples. Snippets are constants (payload only in docs).

Single source of truth for docs/docs/sample-data/raster-data.mdx.
Tested by: docs/tests/python/sample_data/test_raster_data.py
"""

DOWNLOAD_SENTINEL_NYC = """# Install pystac-client if needed
%pip install pystac-client planetary-computer --quiet

# Search and download Sentinel-2 imagery over NYC
import pystac_client
import planetary_computer
import requests
from pathlib import Path
from databricks.labs.gbx.rasterx import functions as rx

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Configure
rx.register(spark)
output_dir = Path(f"{sample_path}/nyc/sentinel2")
output_dir.mkdir(parents=True, exist_ok=True)

# Connect to Microsoft Planetary Computer STAC API
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# Define NYC bounding box (approximate)
nyc_bbox = [-74.25, 40.50, -73.70, 40.92]  # [west, south, east, north]

# Search for Sentinel-2 scenes
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=nyc_bbox,
    datetime="2023-06-01/2023-08-31",  # Summer for less clouds
    query={
        "eo:cloud_cover": {"lt": 20}  # Less than 20% cloud cover
    },
    limit=5
)

items = list(search.items())
print(f"Found {len(items)} Sentinel-2 scenes over NYC")

if items:
    # Get the least cloudy scene
    best_item = min(items, key=lambda x: x.properties.get("eo:cloud_cover", 100))
    print(f"Selected scene: {best_item.id}")
    print(f"  Date: {best_item.datetime}")
    print(f"  Cloud cover: {best_item.properties.get('eo:cloud_cover')}%")
    
    # Download Red band (B04) - 10m resolution
    red_band = best_item.assets["B04"]
    red_url = red_band.href
    
    output_file = output_dir / "nyc_sentinel2_red.tif"
    print(f"Downloading Red band (~30MB)...")
    
    response = requests.get(red_url, stream=True)
    response.raise_for_status()
    
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"✅ Downloaded Sentinel-2 Red band ({file_size:.1f} MB)")
    print(f"   Path: {output_file}")
    
    # Verify
    raster = spark.read.format("gdal").load(str(output_file))
    raster.select(
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_numbands("tile").alias("bands"),
        rx.rst_pixelwidth("tile").alias("pixel_size")
    ).show()"""

DOWNLOAD_SENTINEL_NYC_output = """Found 5 Sentinel-2 scenes over NYC
Selected scene: S2A_MSIL2A_20230715...
  Date: 2023-07-15
  Cloud cover: 8%
✅ Downloaded Sentinel-2 Red band (32.1 MB)
   Path: .../nyc/sentinel2/nyc_sentinel2_red.tif
+-----+------+-----+-----------+
|width|height|bands|pixel_size |
+-----+------+-----+-----------+
|10980|10980 |1    |10.0       |
+-----+------+-----+-----------+"""

USAGE_SENTINEL_NYC = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
sentinel_nyc = spark.read.format("gdal").load(f"{sample_path}/nyc/sentinel2/nyc_sentinel2_red.tif")"""

DOWNLOAD_SENTINEL_LONDON = """# Install pystac-client if needed
%pip install pystac-client planetary-computer --quiet

# Search and download Sentinel-2 imagery over London
import pystac_client
import planetary_computer
import requests
from pathlib import Path
from databricks.labs.gbx.rasterx import functions as rx

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Configure
rx.register(spark)
output_dir = Path(f"{sample_path}/london/sentinel2")
output_dir.mkdir(parents=True, exist_ok=True)

# Connect to Microsoft Planetary Computer STAC API
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# Define London bounding box (approximate)
london_bbox = [-0.51, 51.28, 0.33, 51.70]  # [west, south, east, north]

# Search for Sentinel-2 scenes
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=london_bbox,
    datetime="2023-06-01/2023-08-31",  # Summer for better weather
    query={
        "eo:cloud_cover": {"lt": 30}  # Less than 30% cloud cover (UK has more clouds)
    },
    limit=10
)

items = list(search.items())
print(f"Found {len(items)} Sentinel-2 scenes over London")

if items:
    # Get the least cloudy scene
    best_item = min(items, key=lambda x: x.properties.get("eo:cloud_cover", 100))
    print(f"Selected scene: {best_item.id}")
    print(f"  Date: {best_item.datetime}")
    print(f"  Cloud cover: {best_item.properties.get('eo:cloud_cover')}%")
    
    # Download Red band (B04) - 10m resolution
    red_band = best_item.assets["B04"]
    red_url = red_band.href
    
    output_file = output_dir / "london_sentinel2_red.tif"
    print(f"Downloading Red band (~40MB)...")
    
    response = requests.get(red_url, stream=True)
    response.raise_for_status()
    
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"✅ Downloaded Sentinel-2 Red band ({file_size:.1f} MB)")
    print(f"   Path: {output_file}")
    
    # Verify
    raster = spark.read.format("gdal").load(str(output_file))
    raster.select(
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_srid("tile").alias("srid")
    ).show()"""

DOWNLOAD_SENTINEL_LONDON_output = """Found 10 Sentinel-2 scenes over London
Selected scene: S2B_MSIL2A_20230722...
  Date: 2023-07-22
  Cloud cover: 12%
✅ Downloaded Sentinel-2 Red band (38.2 MB)
   Path: .../london/sentinel2/london_sentinel2_red.tif
+-----+------+----+
|width|height|srid|
+-----+------+----+
|10980|10980 |4326|
+-----+------+----+"""

USAGE_SENTINEL_LONDON = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
sentinel_london = spark.read.format("gdal").load(f"{sample_path}/london/sentinel2/london_sentinel2_red.tif")"""

DOWNLOAD_SRTM_NYC = """# Download SRTM Elevation Tiles for NYC area (from AWS Public Data)
import requests
import gzip
import shutil
import io
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# SRTM 30m resolution - NYC area tiles  
# AWS Public Data: https://registry.opendata.aws/terrain-tiles/
tiles = [
    "N40W074",  # NYC area (primary)
    "N40W073",  # Eastern NYC/Long Island
]

output_dir = Path(f"{sample_path}/nyc-elevation")
output_dir.mkdir(parents=True, exist_ok=True)

for tile in tiles:
    # AWS SRTM format: .hgt.gz (gzipped raw binary elevation)
    lat_dir = tile[:3]  # e.g., "N40"
    url = f'https://elevation-tiles-prod.s3.amazonaws.com/skadi/{lat_dir}/{tile}.hgt.gz'
    output_file = output_dir / f"srtm_{tile.lower()}.hgt"
    
    print(f"Downloading SRTM {tile}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Decompress gzip on the fly
    with gzip.open(io.BytesIO(response.content), 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"✅ Downloaded SRTM {tile} ({file_size:.1f} MB)")

print(f"\\n✅ NYC elevation data ready at: {output_dir}")

# Verify
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

elevation = spark.read.format("gdal").load(f"{output_dir}/*.hgt")
elevation.select(
    "path",
    rx.rst_min("tile").alias("min_elevation"),
    rx.rst_max("tile").alias("max_elevation"),
    rx.rst_avg("tile").alias("avg_elevation")
).show()"""

DOWNLOAD_SRTM_NYC_output = """Downloading SRTM N40W074...
✅ Downloaded SRTM N40W074 (24.7 MB)
Downloading SRTM N40W073...
✅ Downloaded SRTM N40W073 (24.7 MB)
✅ NYC elevation data ready at: .../nyc-elevation
+--------------------+-------------+-------------+-------------+
|path                |min_elevation|max_elevation|avg_elevation|
+--------------------+-------------+-------------+-------------+
|.../srtm_n40w074.hgt|-2.0         |389.0        |45.2         |
|.../srtm_n40w073.hgt|-1.0         |124.0        |18.1         |
+--------------------+-------------+-------------+-------------+"""

USAGE_SRTM_NYC = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
elevation = spark.read.format("gdal").load(f"{sample_path}/nyc-elevation/srtm_n40w074.hgt")"""

DOWNLOAD_SRTM_LONDON = """# Download SRTM Elevation Tile for London (from AWS Public Data)
import requests
import gzip
import shutil
import io
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# London area tile - AWS SRTM
tile = "N51W001"
lat_dir = tile[:3]  # "N51"
url = f'https://elevation-tiles-prod.s3.amazonaws.com/skadi/{lat_dir}/{tile}.hgt.gz'

# Output directory (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/london-elevation")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "srtm_n51w001.hgt"

print("Downloading SRTM London area...")
response = requests.get(url, stream=True)
response.raise_for_status()

# Decompress gzip on the fly
with gzip.open(io.BytesIO(response.content), 'rb') as f_in:
    with open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

file_size = output_file.stat().st_size / (1024 * 1024)
print(f"✅ Downloaded SRTM London ({file_size:.1f} MB)")
print(f"   Path: {output_file}")

# Verify
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

elevation = spark.read.format("gdal").load(str(output_file))
elevation.select(
    rx.rst_min("tile").alias("min_elevation"),
    rx.rst_max("tile").alias("max_elevation")
).show()"""

DOWNLOAD_SRTM_LONDON_output = """Downloading SRTM London area...
✅ Downloaded SRTM London (24.7 MB)
   Path: .../london-elevation/srtm_n51w001.hgt
+-------------+-------------+
|min_elevation|max_elevation|
+-------------+-------------+
|-2.0         |134.0        |
+-------------+-------------+"""

USAGE_SRTM_LONDON = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
elevation_london = spark.read.format("gdal").load(f"{sample_path}/london-elevation/srtm_n51w001.hgt")"""

DOWNLOAD_HRRR_NYC = """# Download NOAA HRRR NetCDF for NYC area
import requests
from pathlib import Path
from datetime import datetime, timedelta

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Output directory (each dataset in its own subfolder)
output_dir = Path(f"{sample_path}/nyc/hrrr-weather")
output_dir.mkdir(parents=True, exist_ok=True)

# Get recent HRRR file (example: yesterday's 12Z forecast)
yesterday = datetime.now(datetime.UTC) - timedelta(days=1)
date_str = yesterday.strftime("%Y%m%d")
hour = "12"  # 12Z (noon UTC)

# HRRR AWS public dataset - Surface level, 2D variables
hrrr_url = f"https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{date_str}/conus/hrrr.t{hour}z.wrfsfcf00.grib2"
output_file = output_dir / f"hrrr_nyc_{date_str}_{hour}z.grib2"

print(f"Downloading HRRR forecast for {date_str} {hour}Z...")
print("This may take 2-3 minutes (~20-30MB)...")

response = requests.get(hrrr_url, stream=True)
response.raise_for_status()

with open(output_file, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

file_size = output_file.stat().st_size / (1024 * 1024)
print(f"✅ Downloaded HRRR data ({file_size:.1f} MB)")
print(f"   Path: {output_file}")

# Verify with GDAL (HRRR uses GRIB2, readable by GDAL)
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# GDAL can read GRIB2 files
hrrr = spark.read.format("gdal").load(str(output_file))
print(f"   Subdatasets/bands: {hrrr.count()}")
hrrr.select(
    "path",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height")
).show(5)"""

DOWNLOAD_HRRR_NYC_output = """Downloading HRRR forecast for 20240115 12Z...
This may take 2-3 minutes (~20-30MB)...

✅ Downloaded HRRR data (28.5 MB)
   Path: .../nyc/hrrr-weather/hrrr_nyc_20240115_12z.grib2
   Subdatasets/bands: 50+
+-------------------------+-----+------+
|path                     |width|height|
+-------------------------+-----+------+
|.../hrrr_nyc_....grib2   |1500 |1121  |
|...                      |...  |...   |
+-------------------------+-----+------+"""

USAGE_HRRR_NYC = """sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
hrrr = spark.read.format("gdal").load(f"{sample_path}/nyc/hrrr-weather/hrrr_*.grib2")"""
