"""
GeoBrix GDAL CLI Integration Examples

Code examples for using GDAL command-line utilities with GeoBrix.
Single source for docs/docs/advanced/gdal-cli.mdx.
Tested by: docs/tests/python/advanced/test_gdal_cli.py
"""

import subprocess
import json
import tempfile
import os

# ----- CLI examples: code block (command only) vs output (shell result only) -----

# gdalinfo: code block shows only the command; outputConstant shows only shell output
gdalinfo_cli_command = """gdalinfo /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"""

gdalinfo_cli_output = """Driver: GTiff/GeoTIFF
Files: /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif
Size is 10980, 10980
Coordinate System is:
PROJCS["WGS 84 / UTM zone 18N",
    ...
Origin = (-8239980.000000000000000,4960220.000000000000000)
Pixel Size = (10.000000000000000,-10.000000000000000)
Metadata:
  ...
Corner Coordinates:
Upper Left  ( -8239980.000, 4960220.000) ( 74d15'56.10"W, 40d42'22.31"N)
Lower Right (-8129820.000, 4950320.000) ( 73d52'57.89"W, 40d38'32.56"N)"""

# Preprocessing: code block = commands only; output = shell results per step
preprocessing_cli_commands = """# 1. Reproject to common CRS
gdalwarp -t_srs EPSG:4326 /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif /tmp/reprojected.tif

# 2. Create Cloud-Optimized GeoTIFF
gdal_translate -co TILED=YES -co COMPRESS=LZW -co COPY_SRC_OVERVIEWS=YES \\
  /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif /tmp/output_cog.tif

# 3. Then load with GeoBrix: spark.read.format("gdal").load("/tmp/output_cog.tif")"""

preprocessing_cli_output = """Step 1 — gdalwarp:
Creating output file that is 10980P x 10980L.
Processing input file ...
0...10...20...30...40...50...60...70...80...90...100 - done.

Step 2 — gdal_translate:
Input file size is 10980, 10980
0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdalwarp: command (code block) + shell output (Example output)
gdalwarp_cli_command = """gdalwarp -t_srs EPSG:4326 /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif /tmp/reprojected.tif"""

gdalwarp_cli_output = """Creating output file that is 10980P x 10980L.
Processing input file ...
0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdal_translate: command (code block) + shell output (Example output)
gdal_translate_cli_command = """gdal_translate -co TILED=YES -co COMPRESS=LZW -co COPY_SRC_OVERVIEWS=YES \\
  -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 \\
  /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif /tmp/output_cog.tif"""

gdal_translate_cli_output = """Input file size is 10980, 10980
0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdal_merge: command (code block) + shell output (Example output)
gdal_merge_cli_command = """gdal_merge.py -co COMPRESS=LZW -co TILED=YES -o /tmp/merged.tif /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/*.tif"""

gdal_merge_cli_output = """0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdalbuildvrt: command (code block) + shell output (Example output)
gdalbuildvrt_cli_command = """gdalbuildvrt -resolution highest /tmp/mosaic.vrt /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/*.tif"""

gdalbuildvrt_cli_output = """0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdaldem: command (code block) + shell output (Example output); uses elevation from sample-data
gdaldem_cli_command = """gdaldem hillshade -z 2 /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/elevation/srtm_n40w073.hgt /tmp/hillshade.tif"""

gdaldem_cli_output = """0...10...20...30...40...50...60...70...80...90...100 - done."""

# gdal_calc: command (code block) + shell output (Example output)
gdal_calc_cli_command = """gdal_calc.py -A /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif \\
  --outfile=/tmp/threshold.tif --calc="A>100" --NoDataValue=-9999"""

gdal_calc_cli_output = """0...10...20...30...40...50...60...70...80...90...100 - done."""

# ogr2ogr: command (code block) + shell output (Example output); uses vector from sample-data
ogr2ogr_cli_command = """ogr2ogr -t_srs EPSG:4326 /tmp/reprojected.geojson /Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"""

ogr2ogr_cli_output = """0...10...20...30...40...50...60...70...80...90...100 - done."""

# Scenario: Satellite Image Processing — script (code block) + shell output (Example output); sample-data Volumes path
satellite_preprocessing_cli_commands = '''#!/bin/bash
# preprocessing.sh — uses sample-data Volumes path

INPUT_DIR="/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2"
OUTPUT_DIR="/tmp/processed"

mkdir -p "$OUTPUT_DIR"

echo "Step 1: Reproject to WGS84"
for f in "$INPUT_DIR"/*.tif; do
  base=$(basename "$f" .tif)
  gdalwarp -t_srs EPSG:4326 -r bilinear \\
    -co COMPRESS=LZW -co TILED=YES \\
    "$f" "$OUTPUT_DIR/${base}_wgs84.tif"
done

echo "Step 2: Create overviews"
for f in "$OUTPUT_DIR"/*_wgs84.tif; do
  gdaladdo -r average "$f" 2 4 8 16
done

echo "Step 3: Create VRT catalog"
gdalbuildvrt "$OUTPUT_DIR/catalog.vrt" "$OUTPUT_DIR"/*_wgs84.tif

echo "Preprocessing complete!"'''

satellite_preprocessing_cli_output = """Step 1: Reproject to WGS84
Creating output file that is 10980P x 10980L.
Processing input file ...
0...10...20...30...40...50...60...70...80...90...100 - done.

Step 2: Create overviews
0...10...20...30...40...50...60...70...80...90...100 - done.

Step 3: Create VRT catalog
0...10...20...30...40...50...60...70...80...90...100 - done.

Preprocessing complete!"""

# Conditional imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import udf, lit
    from pyspark.sql.types import BinaryType
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    def udf(*args, **kwargs):
        def decorator(f):
            return f
        return decorator if not args else decorator(args[0])
    def lit(x):
        return None


def gdal_cli_in_spark_udfs(spark):
    """
    Combine GDAL CLI with custom UDFs. Uses sample-data Volumes path; shows UDF definition and execution results.
    """
    import tempfile
    import os
    from pyspark.sql import functions as f

    @udf(BinaryType())
    def apply_gdal_operation(tile_binary, operation):
        """Apply GDAL CLI operation via UDF (e.g. hillshade or slope)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input.tif")
            output_path = os.path.join(tmpdir, "output.tif")
            with open(input_path, "wb") as fp:
                fp.write(bytes(tile_binary))
            if operation == "hillshade":
                subprocess.run(["gdaldem", "hillshade", input_path, output_path], check=True, capture_output=True)
            elif operation == "slope":
                subprocess.run(["gdaldem", "slope", input_path, output_path], check=True, capture_output=True)
            with open(output_path, "rb") as fp:
                return fp.read()

    # Sample-data Volumes path (same as prior CLI examples)
    raster_path = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
    rasters = spark.read.format("gdal").load(raster_path)
    processed = rasters.withColumn(
        "hillshade",
        apply_gdal_operation("tile", lit("hillshade"))
    )
    result = processed.select("path", f.length("hillshade").alias("hillshade_bytes"))
    result.limit(2).show(truncate=50)
    return result


gdal_cli_in_spark_udfs_output = """+--------------------------------------------------+----------------+
|path                                              |hillshade_bytes |
+--------------------------------------------------+----------------+
|/Volumes/.../nyc/sentinel2/nyc_sentinel2_red.tif  |1234567         |
|...                                               |...             |
+--------------------------------------------------+----------------+"""


if __name__ == "__main__":
    print("GeoBrix GDAL CLI Integration Examples")
    print("=" * 50)
    print("CLI constants + gdal_cli_in_spark_udfs (doc-referenced only)")
