#!/usr/bin/env python3
"""
Create NYC Multi-Feature Class File Geodatabase Sample Data

This script combines multiple NYC vector datasets into a single
Esri File Geodatabase with multiple feature classes for demonstrating
GeoBrix FileGDB reader capabilities.

Prerequisites:
- GDAL with FileGDB driver: gdal-config --formats | grep -i filegdb
- geopandas: pip install geopandas
- Essential Bundle datasets already downloaded

Usage:
    python3 sample-data/create-sample-filegdb.py
    
    # Or specify custom paths:
    python3 sample-data/create-sample-filegdb.py \\
        --input-dir /Volumes/catalog/schema/volume \\
        --output NYC_Sample.gdb
"""

import argparse
import geopandas as gpd
from pathlib import Path
import sys
import glob
import shutil
from osgeo import ogr, osr


def create_filegdb(input_dir: str, output_gdb: str, verbose: bool = True):
    """
    Create multi-feature class File Geodatabase from NYC datasets.
    
    Args:
        input_dir: Base directory containing NYC datasets
        output_gdb: Output FileGDB path (should end in .gdb)
        verbose: Print progress messages
    """
    input_path = Path(input_dir)
    output_path = Path(output_gdb)
    
    # Try OpenFileGDB driver first (open source, write support since GDAL 3.6)
    driver = ogr.GetDriverByName('OpenFileGDB')
    driver_name = 'OpenFileGDB'
    
    # Fall back to proprietary FileGDB driver if available
    if driver is None:
        driver = ogr.GetDriverByName('FileGDB')
        driver_name = 'FileGDB'
    
    if driver is None:
        if verbose:
            print("="*70)
            print("⚠️  No FileGDB Driver Available")
            print("="*70)
            print()
            print("Neither OpenFileGDB nor FileGDB driver is available.")
            print()
            print("OpenFileGDB driver provides read/write support since GDAL 3.6")
            print("and should be available in standard GDAL installations.")
            print()
            print("Options:")
            print("  1. Upgrade GDAL to version 3.6 or higher")
            print("  2. Use GeoPackage instead (similar multi-layer capabilities)")
            print()
            print("To check GDAL version and drivers:")
            print("  gdal-config --version")
            print("  gdal-config --formats | grep -i filegdb")
            print()
        return False
    
    if verbose:
        print(f"Using driver: {driver_name}")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing FileGDB
    if output_path.exists():
        shutil.rmtree(output_path)
        if verbose:
            print(f"Removed existing: {output_path}")
    
    if verbose:
        print("="*70)
        print("Creating NYC Multi-Feature Class File Geodatabase")
        print("="*70)
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        print()
    
    # Create FileGDB
    gdb = driver.CreateDataSource(str(output_path))
    if gdb is None:
        if verbose:
            print("⚠️  Could not create FileGDB")
        return False
    
    # Define spatial reference (WGS84)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    
    feature_classes_created = []
    
    try:
        # Feature Class 1: Boroughs
        boroughs_file = input_path / "nyc" / "boroughs" / "nyc_boroughs.geojson"
        if boroughs_file.exists():
            if verbose:
                print("Adding feature class: NYC_Boroughs")
            
            boroughs = gpd.read_file(str(boroughs_file))
            layer = gdb.CreateLayer('NYC_Boroughs', srs, ogr.wkbMultiPolygon)
            
            # Add fields
            layer.CreateField(ogr.FieldDefn('BORO_NAME', ogr.OFTString))
            layer.CreateField(ogr.FieldDefn('BORO_CODE', ogr.OFTString))
            
            # Add features
            for idx, row in boroughs.iterrows():
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField('BORO_NAME', str(row.get('boro_name', '')))
                feature.SetField('BORO_CODE', str(row.get('boro_code', '')))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer.CreateFeature(feature)
                feature = None
            
            feature_classes_created.append(f"NYC_Boroughs ({len(boroughs)} features)")
            if verbose:
                print(f"  ✅ {len(boroughs)} features")
        else:
            if verbose:
                print(f"  ⚠️  Not found: {boroughs_file}")
        
        # Feature Class 2: Taxi Zones
        zones_file = input_path / "nyc" / "taxi-zones" / "nyc_taxi_zones.geojson"
        if zones_file.exists():
            if verbose:
                print("Adding feature class: NYC_TaxiZones")
            
            zones = gpd.read_file(str(zones_file))
            layer = gdb.CreateLayer('NYC_TaxiZones', srs, ogr.wkbMultiPolygon)
            
            # Add fields
            layer.CreateField(ogr.FieldDefn('ZONE_NAME', ogr.OFTString))
            field_id = ogr.FieldDefn('LOCATION_ID', ogr.OFTInteger)
            layer.CreateField(field_id)
            
            # Add features
            for idx, row in zones.iterrows():
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField('ZONE_NAME', str(row.get('zone', '')))
                location_id = row.get('LocationID')
                if location_id is not None:
                    feature.SetField('LOCATION_ID', int(location_id))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer.CreateFeature(feature)
                feature = None
            
            feature_classes_created.append(f"NYC_TaxiZones ({len(zones)} features)")
            if verbose:
                print(f"  ✅ {len(zones)} features")
        else:
            if verbose:
                print(f"  ⚠️  Not found: {zones_file}")
        
        # Feature Class 3: Parks (optional)
        parks_pattern = str(input_path / "nyc" / "parks" / "*.shp")
        parks_files = glob.glob(parks_pattern)
        if parks_files:
            if verbose:
                print("Adding feature class: NYC_Parks")
            
            parks = gpd.read_file(parks_files[0])
            layer = gdb.CreateLayer('NYC_Parks', srs, ogr.wkbMultiPolygon)
            
            # Add fields
            layer.CreateField(ogr.FieldDefn('PARK_NAME', ogr.OFTString))
            field_acres = ogr.FieldDefn('ACRES', ogr.OFTReal)
            layer.CreateField(field_acres)
            layer.CreateField(ogr.FieldDefn('BOROUGH', ogr.OFTString))
            
            # Add features (limit to 500 to keep size reasonable)
            count = 0
            for idx, row in parks.head(500).iterrows():
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField('PARK_NAME', str(row.get('name311', '')))
                acres = row.get('acres')
                if acres is not None:
                    try:
                        feature.SetField('ACRES', float(acres))
                    except (ValueError, TypeError):
                        pass
                feature.SetField('BOROUGH', str(row.get('borough', '')))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer.CreateFeature(feature)
                feature = None
                count += 1
            
            feature_classes_created.append(f"NYC_Parks ({count} features)")
            if verbose:
                print(f"  ✅ {count} features")
        
        # Feature Class 4: Subway Stations (optional)
        subway_pattern = str(input_path / "nyc" / "subway" / "*.shp")
        subway_files = glob.glob(subway_pattern)
        if subway_files:
            if verbose:
                print("Adding feature class: NYC_SubwayStations")
            
            subway = gpd.read_file(subway_files[0])
            layer = gdb.CreateLayer('NYC_SubwayStations', srs, ogr.wkbPoint)
            
            # Add fields
            layer.CreateField(ogr.FieldDefn('STATION_NAME', ogr.OFTString))
            layer.CreateField(ogr.FieldDefn('LINE', ogr.OFTString))
            
            # Add features
            for idx, row in subway.iterrows():
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField('STATION_NAME', str(row.get('Station_Na', '')))
                feature.SetField('LINE', str(row.get('Line', '')))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(row.geometry.wkt))
                layer.CreateFeature(feature)
                feature = None
            
            feature_classes_created.append(f"NYC_SubwayStations ({len(subway)} features)")
            if verbose:
                print(f"  ✅ {len(subway)} features")
        
    finally:
        # Close geodatabase
        gdb = None
    
    # Summary
    if verbose:
        print()
        print("="*70)
        if feature_classes_created:
            # Calculate size
            gdb_size = sum(f.stat().st_size for f in output_path.rglob('*') if f.is_file()) / (1024 * 1024)
            print(f"✅ Created File Geodatabase: {output_path.name} ({gdb_size:.1f} MB)")
            print()
            print("Feature Classes:")
            for fc in feature_classes_created:
                print(f"  - {fc}")
            print()
            print("Usage in GeoBrix:")
            print(f'  boroughs = spark.read.format("filegdb") \\')
            print(f'      .option("layerName", "NYC_Boroughs") \\')
            print(f'      .load("{output_path}")')
        else:
            print("⚠️  No feature classes created - check input paths")
        print("="*70)
    
    return len(feature_classes_created) > 0


def main():
    parser = argparse.ArgumentParser(
        description="Create NYC Multi-Feature Class File Geodatabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using default paths (Databricks Volumes)
  python3 sample-data/create-sample-filegdb.py
  
  # Custom paths
  python3 sample-data/create-sample-filegdb.py \\
      --input-dir /Volumes/main/default/geobrix_samples \\
      --output /Volumes/main/default/geobrix_samples/formats/filegdb/NYC_Sample.gdb
  
  # Local filesystem (for development)
  python3 sample-data/create-sample-filegdb.py \\
      --input-dir ./sample_data \\
      --output ./NYC_Sample.gdb

Requirements:
  - GDAL with FileGDB driver support
  - Check: gdal-config --formats | grep -i filegdb
        """
    )
    
    parser.add_argument(
        '--input-dir',
        default='/Volumes/main/default/geobrix_samples/geobrix-examples',
        help='Base directory containing NYC datasets (default: /Volumes/main/default/geobrix_samples/geobrix-examples)'
    )
    
    parser.add_argument(
        '--output',
        default='/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb',
        help='Output FileGDB path (default: <input-dir>/nyc/filegdb/NYC_Sample.gdb)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )
    
    args = parser.parse_args()
    
    try:
        success = create_filegdb(
            input_dir=args.input_dir,
            output_gdb=args.output,
            verbose=not args.quiet
        )
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
