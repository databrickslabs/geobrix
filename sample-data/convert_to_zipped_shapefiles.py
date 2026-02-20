#!/usr/bin/env python3
"""
Convert GeoJSON files to zipped shapefiles.

Shapefiles consist of multiple files (.shp, .shx, .dbf, .prj, etc.) and are
typically distributed as .zip files. This script converts our GeoJSON test data
to zipped shapefiles to match real-world usage.
"""

import os
import zipfile
from pathlib import Path

try:
    import geopandas as gpd
    GEOPANDAS_AVAILABLE = True
except ImportError:
    print("❌ geopandas not available. Install with: pip install geopandas")
    GEOPANDAS_AVAILABLE = False
    exit(1)

def convert_geojson_to_zipped_shapefile(geojson_path):
    """Convert a GeoJSON file to a zipped shapefile."""
    geojson_path = Path(geojson_path)
    
    if not geojson_path.exists():
        print(f"❌ File not found: {geojson_path}")
        return False
    
    print(f"\n📄 Processing: {geojson_path.name}")
    
    # Read GeoJSON
    try:
        gdf = gpd.read_file(geojson_path)
        print(f"   ✅ Loaded {len(gdf)} features")
        print(f"   📐 CRS: {gdf.crs}")
        print(f"   📊 Columns: {', '.join(gdf.columns[:5])}{'...' if len(gdf.columns) > 5 else ''}")
    except Exception as e:
        print(f"   ❌ Error reading GeoJSON: {e}")
        return False
    
    # Prepare output paths
    base_name = geojson_path.stem
    output_dir = geojson_path.parent
    shp_base = output_dir / base_name
    zip_path = output_dir / f"{base_name}.zip"
    
    # Save as shapefile (creates multiple files)
    try:
        gdf.to_file(str(shp_base) + ".shp")
        print(f"   ✅ Created shapefile components")
    except Exception as e:
        print(f"   ❌ Error creating shapefile: {e}")
        return False
    
    # Zip all shapefile components
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Shapefile extensions to include
            extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.sbn', '.sbx']
            files_added = []
            
            for ext in extensions:
                component_path = output_dir / f"{base_name}{ext}"
                if component_path.exists():
                    # Add to zip with just the filename (no directory structure)
                    zipf.write(component_path, component_path.name)
                    files_added.append(ext)
                    # Remove the unzipped component
                    component_path.unlink()
            
            print(f"   ✅ Created {zip_path.name} with: {', '.join(files_added)}")
            print(f"   📦 Zip size: {zip_path.stat().st_size / 1024:.1f} KB")
    except Exception as e:
        print(f"   ❌ Error creating zip: {e}")
        return False
    
    return True

def main():
    """Convert all GeoJSON files in sample-data to zipped shapefiles."""
    print("=" * 70)
    print("🗺️  Converting GeoJSON to Zipped Shapefiles")
    print("=" * 70)
    
    if not GEOPANDAS_AVAILABLE:
        return
    
    # Find all GeoJSON files in sample-data
    test_data_root = Path(__file__).parent / "Volumes" / "main" / "default" / "geobrix_samples" / "geobrix-examples"
    
    if not test_data_root.exists():
        print(f"❌ Test data directory not found: {test_data_root}")
        return
    
    geojson_files = list(test_data_root.glob("**/*.geojson"))
    
    if not geojson_files:
        print(f"❌ No GeoJSON files found in {test_data_root}")
        return
    
    print(f"\n📁 Found {len(geojson_files)} GeoJSON files\n")
    
    success_count = 0
    for geojson_file in sorted(geojson_files):
        if convert_geojson_to_zipped_shapefile(geojson_file):
            success_count += 1
    
    print("\n" + "=" * 70)
    print(f"✅ Successfully converted {success_count}/{len(geojson_files)} files")
    print("=" * 70)
    
    if success_count == len(geojson_files):
        print("\n🎉 All GeoJSON files converted to zipped shapefiles!")
        print("\nNext steps:")
        print("1. Update examples.py to use .zip extension for shapefile reader")
        print("2. Update test fixtures to point to .zip files")
        print("3. Re-run tests to verify shapefile reader works with .zip files")
    else:
        print("\n⚠️  Some conversions failed. Please check the errors above.")

if __name__ == "__main__":
    main()
