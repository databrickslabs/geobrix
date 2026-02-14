"""
Additional page code examples. Snippets are constants (payload only in docs).

Single source of truth for docs/docs/sample-data/additional.mdx.
Tested by: docs/tests/python/sample_data/test_additional.py.

Note: Synthetic Points (Vector) uses st_* functions and lives in tests-dbr;
see docs/tests-dbr/python/sample_data/additional.py.
"""

SYNTHETIC_RASTER = """# Generate a small synthetic raster using GDAL
from osgeo import gdal, osr
import numpy as np
from pathlib import Path

sample_path = "/Volumes/main/default/geobrix_samples/geobrix-examples"
# Output directory and file
output_dir = Path(f"{sample_path}/synthetic-raster")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "synthetic_100x100.tif"

# Create 100x100 raster with random values
width, height = 100, 100
data = np.random.randint(0, 255, (height, width), dtype=np.uint8)

# Create GeoTIFF
driver = gdal.GetDriverByName('GTiff')
dataset = driver.Create(str(output_file), width, height, 1, gdal.GDT_Byte)

# Set geotransform (top-left corner at 0,0, pixel size 1x1)
dataset.SetGeoTransform([0, 1, 0, 0, 0, -1])

# Set projection (WGS84)
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
dataset.SetProjection(srs.ExportToWkt())

# Write data
band = dataset.GetRasterBand(1)
band.WriteArray(data)
band.SetNoDataValue(0)

# Close and flush
band.FlushCache()
dataset = None

print(f"✅ Created synthetic 100x100 raster")
print(f"   Path: {output_file}")

# Verify
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

synthetic = spark.read.format("gdal").load(str(output_file))
synthetic.select(
    rx.rst_width("tile"),
    rx.rst_height("tile"),
    rx.rst_min("tile"),
    rx.rst_max("tile")
).show()"""

SYNTHETIC_RASTER_output = """✅ Created synthetic 100x100 raster
   Path: .../synthetic-raster/synthetic_100x100.tif
+-----+------+----+----+
|width|height|min |max |
+-----+------+----+----+
|100  |100   |0   |254 |
+-----+------+----+----+"""

STAC_ANY_LOCATION = """# Any location worldwide
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# Search by your area of interest
my_bbox = [west, south, east, north]  # Your coordinates
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=my_bbox,
    datetime="2023-01-01/2023-12-31",
    query={"eo:cloud_cover": {"lt": 20}}
)"""
