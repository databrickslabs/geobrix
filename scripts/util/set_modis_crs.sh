#!/bin/bash
# Script to set proper ESRI:54008 CRS with AUTHORITY on MODIS test files

# MODIS Sinusoidal projection WKT with ESRI:54008 authority
# Based on ESRI:54008 definition
MODIS_WKT='PROJCRS["Sinusoidal",
    BASEGEOGCRS["WGS 84",
        DATUM["World Geodetic System 1984",
            ELLIPSOID["WGS 84",6378137,298.257223563,
                LENGTHUNIT["metre",1]],
            ID["EPSG",6326]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["Degree",0.0174532925199433]]],
    CONVERSION["Sinusoidal",
        METHOD["Sinusoidal"],
        PARAMETER["Longitude of natural origin",0,
            ANGLEUNIT["Degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["False easting",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8807]]],
    CS[Cartesian,2],
        AXIS["easting (X)",east,
            ORDER[1],
            LENGTHUNIT["metre",1]],
        AXIS["northing (Y)",north,
            ORDER[2],
            LENGTHUNIT["metre",1]],
    USAGE[
        SCOPE["Not known."],
        AREA["World."],
        BBOX[-90,-180,90,180]],
    ID["ESRI",54008]]'

# Directory containing MODIS files
MODIS_DIR="src/test/resources/modis"

# Update all MODIS TIF files
for tif in $MODIS_DIR/MCD43A4.*.TIF; do
    echo "Updating CRS for: $tif"
    gdal_edit.py -a_srs "$MODIS_WKT" "$tif"
    if [ $? -eq 0 ]; then
        echo "  ✓ Success"
    else
        echo "  ✗ Failed"
    fi
done

echo ""
echo "Verifying first file:"
gdalinfo "$MODIS_DIR/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF" | grep -A 5 "Coordinate System"

