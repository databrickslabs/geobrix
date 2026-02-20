#!/usr/bin/env python3
"""
Create NYC Multi-Layer GeoPackage Sample Data

This script combines multiple NYC vector datasets into a single
multi-layer GeoPackage for demonstrating GeoBrix GPKG reader capabilities.

Prerequisites:
- geopandas: pip install geopandas
- Essential Bundle datasets already downloaded

Usage:
    python3 sample-data/create-sample-geopackage.py
    
    # Or specify custom paths:
    python3 sample-data/create-sample-geopackage.py \\
        --input-dir /Volumes/catalog/schema/volume \\
        --output nyc_sample.gpkg
"""

import argparse
import geopandas as gpd
from pathlib import Path
import sys
import glob


def create_geopackage(input_dir: str, output_file: str, verbose: bool = True):
    """
    Create multi-layer GeoPackage from NYC datasets.
    
    Args:
        input_dir: Base directory containing NYC datasets
        output_file: Output GeoPackage path
        verbose: Print progress messages
    """
    input_path = Path(input_dir)
    output_path = Path(output_file)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing file
    if output_path.exists():
        output_path.unlink()
        if verbose:
            print(f"Removed existing: {output_path}")
    
    if verbose:
        print("="*70)
        print("Creating NYC Multi-Layer GeoPackage")
        print("="*70)
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        print()
    
    layers_created = []
    
    # Layer 1: Boroughs
    boroughs_file = input_path / "nyc" / "boroughs" / "nyc_boroughs.geojson"
    if boroughs_file.exists():
        if verbose:
            print("Adding layer: boroughs")
        boroughs = gpd.read_file(str(boroughs_file))
        boroughs.to_file(str(output_path), layer='boroughs', driver='GPKG')
        layers_created.append(f"boroughs ({len(boroughs)} features)")
        if verbose:
            print(f"  ✅ {len(boroughs)} features")
    else:
        if verbose:
            print(f"  ⚠️  Not found: {boroughs_file}")
    
    # Layer 2: Taxi Zones
    zones_file = input_path / "nyc" / "taxi-zones" / "nyc_taxi_zones.geojson"
    if zones_file.exists():
        if verbose:
            print("Adding layer: taxi_zones")
        zones = gpd.read_file(str(zones_file))
        zones.to_file(str(output_path), layer='taxi_zones', driver='GPKG', mode='a')
        layers_created.append(f"taxi_zones ({len(zones)} features)")
        if verbose:
            print(f"  ✅ {len(zones)} features")
    else:
        if verbose:
            print(f"  ⚠️  Not found: {zones_file}")
    
    # Layer 3: Neighborhoods (optional - Complete bundle)
    nta_file = input_path / "nyc" / "neighborhoods" / "nyc_nta.geojson"
    if nta_file.exists():
        if verbose:
            print("Adding layer: neighborhoods")
        neighborhoods = gpd.read_file(str(nta_file))
        neighborhoods.to_file(str(output_path), layer='neighborhoods', driver='GPKG', mode='a')
        layers_created.append(f"neighborhoods ({len(neighborhoods)} features)")
        if verbose:
            print(f"  ✅ {len(neighborhoods)} features")
    
    # Layer 4: Parks
    parks_pattern = str(input_path / "nyc" / "parks" / "*.shp")
    parks_files = glob.glob(parks_pattern)
    if parks_files:
        if verbose:
            print("Adding layer: parks")
        parks = gpd.read_file(parks_files[0])
        parks.to_file(str(output_path), layer='parks', driver='GPKG', mode='a')
        layers_created.append(f"parks ({len(parks)} features)")
        if verbose:
            print(f"  ✅ {len(parks)} features")
    
    # Layer 5: Subway Stations
    subway_pattern = str(input_path / "nyc" / "subway" / "*.shp")
    subway_files = glob.glob(subway_pattern)
    if subway_files:
        if verbose:
            print("Adding layer: subway_stations")
        subway = gpd.read_file(subway_files[0])
        subway.to_file(str(output_path), layer='subway_stations', driver='GPKG', mode='a')
        layers_created.append(f"subway_stations ({len(subway)} features)")
        if verbose:
            print(f"  ✅ {len(subway)} features")
    
    # Summary
    if verbose:
        print()
        print("="*70)
        if layers_created:
            file_size = output_path.stat().st_size / (1024 * 1024)
            print(f"✅ Created GeoPackage: {output_path.name} ({file_size:.1f} MB)")
            print()
            print("Layers:")
            for layer in layers_created:
                print(f"  - {layer}")
            print()
            print("Usage in GeoBrix:")
            print(f'  boroughs = spark.read.format("gpkg") \\')
            print(f'      .option("layerName", "boroughs") \\')
            print(f'      .load("{output_path}")')
        else:
            print("⚠️  No layers created - check input paths")
        print("="*70)
    
    return len(layers_created) > 0


def main():
    parser = argparse.ArgumentParser(
        description="Create NYC Multi-Layer GeoPackage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using default paths (Databricks Volumes)
  python3 sample-data/create-sample-geopackage.py
  
  # Custom paths
  python3 sample-data/create-sample-geopackage.py \\
      --input-dir /Volumes/main/default/geobrix_samples \\
      --output /Volumes/main/default/geobrix_samples/formats/geopackage/nyc_complete.gpkg
  
  # Local filesystem (for development)
  python3 sample-data/create-sample-geopackage.py \\
      --input-dir ./sample_data \\
      --output ./nyc_sample.gpkg
        """
    )
    
    parser.add_argument(
        '--input-dir',
        default='/Volumes/main/default/geobrix_samples/geobrix-examples',
        help='Base directory containing NYC datasets (default: /Volumes/main/default/geobrix_samples/geobrix-examples)'
    )
    
    parser.add_argument(
        '--output',
        default='/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg',
        help='Output GeoPackage path (default: <input-dir>/nyc/geopackage/nyc_complete.gpkg)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )
    
    args = parser.parse_args()
    
    try:
        success = create_geopackage(
            input_dir=args.input_dir,
            output_file=args.output,
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
