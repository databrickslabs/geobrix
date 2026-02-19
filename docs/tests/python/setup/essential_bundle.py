#!/usr/bin/env python3
"""
GeoBrix Essential Bundle Setup (~355MB)

This script downloads essential sample data for GeoBrix including:
- NYC and London vector data (GeoJSON)
- Sentinel-2 satellite imagery for both regions
- SRTM elevation data for both regions

Single Source of Truth:
    - Logic lives in bundle_lib; this script and the notebook both use it
    - Documentation imports this file (no copy-paste!)
    - Tested by: docs/tests/python/setup/test_bundles.py

Usage:
    python essential_bundle.py

Prerequisites:
    Unity Catalog Volume must exist:
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
"""

import sys
import subprocess
from pathlib import Path

# Configuration (available for import without running main())
CATALOG = "main"
SCHEMA = "default"
VOLUME = "geobrix_samples"
# Use path_config at runtime so tests use minimal bundle when present
try:
    _p = Path(__file__).resolve().parent.parent
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
    from path_config import SAMPLE_DATA_BASE
    SAMPLE_DATA_PATH = SAMPLE_DATA_BASE
except ImportError:
    SAMPLE_DATA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/geobrix-examples"

# Re-export bundle_lib helpers with script-style signatures (url, subfolder, filename, description)
# so docs and tests can keep using download_file(..., "nyc/boroughs", "nyc_boroughs.geojson", ...)
def download_file(url, subfolder, filename, description):
    """Download a file to Unity Catalog Volumes. Used by docs/tests."""
    import bundle_lib
    out = Path(SAMPLE_DATA_PATH) / subfolder / filename
    return bundle_lib.download_to_path(url, out, description)


def download_srtm_aws(tile, subfolder, filename, description):
    """Download SRTM from AWS and write as GeoTIFF. Used by docs/tests. filename should be .tif."""
    import bundle_lib
    out = Path(SAMPLE_DATA_PATH) / subfolder / filename
    if out.suffix.lower() == ".tif":
        hgt = Path(SAMPLE_DATA_PATH) / subfolder / (out.stem + ".hgt")
        out.parent.mkdir(parents=True, exist_ok=True)
        bundle_lib.download_srtm_to_path(tile, hgt, description)
        if hgt.exists():
            # NYC/London bboxes for synthetic fallback if GDAL can't read .hgt
            bbox = (-74.0, 40.0, -73.0, 41.0) if "n40" in out.stem else (-1.0, 51.0, 0.0, 52.0)
            bundle_lib.srtm_hgt_to_geotiff(hgt, out, bbox, description, quiet=True)
            hgt.unlink(missing_ok=True)
        return out if out.exists() else hgt
    return bundle_lib.download_srtm_to_path(tile, out, description)


def download_sentinel2(bbox, subfolder, filename, region_name):
    """Download least cloudy Sentinel-2 scene. Used by docs/tests."""
    import bundle_lib
    out = Path(SAMPLE_DATA_PATH) / subfolder / filename
    if out.exists():
        size_mb = out.stat().st_size / (1024 * 1024)
        print(f"⏭️  Sentinel-2 {region_name}: {size_mb:.1f} MB (already exists)")
        return out
    try:
        import pystac_client
        import planetary_computer
        import requests
    except ImportError:
        raise RuntimeError("pystac-client, planetary-computer, requests required")
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox,
        datetime="2023-06-01/2023-08-31",
        query={"eo:cloud_cover": {"lt": 30}},
        limit=10,
    )
    items = list(search.items())
    if not items:
        print(f"⚠️  No Sentinel-2 scenes found for {region_name}")
        return None
    best = min(items, key=lambda x: x.properties.get("eo:cloud_cover", 100))
    red_url = best.assets["B04"].href
    print(f"⬇️  Downloading Sentinel-2 {region_name} (scene: {best.id})...")
    response = requests.get(red_url, stream=True)
    response.raise_for_status()
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"✅ Sentinel-2 {region_name}: {size_mb:.1f} MB")
    return out


def main():
    """
    Main execution: install deps, run essential bundle via bundle_lib, then exit.
    Uses temp dir for interim work; copies only final files to Volumes.
    """
    print("Installing dependencies...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "-q",
        "--break-system-packages", "pystac-client", "planetary-computer", "requests"
    ])

    import bundle_lib
    Path(SAMPLE_DATA_PATH).mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("GeoBrix Essential Bundle Setup")
    print("=" * 70)
    print(f"Downloading ~355MB to: {SAMPLE_DATA_PATH}")
    print()

    result = bundle_lib.run_essential_bundle(SAMPLE_DATA_PATH)

    print("\n" + "=" * 70)
    print("Essential Bundle Complete!")
    print("=" * 70)
    print(f"\nData Location: {SAMPLE_DATA_PATH}")
    print("\nFiles:")
    for item in sorted(Path(SAMPLE_DATA_PATH).rglob("*")):
        if item.is_file():
            size_mb = item.stat().st_size / (1024 * 1024)
            print(f"  {item.relative_to(SAMPLE_DATA_PATH)}: {size_mb:.1f} MB")
    print(f"\nTotal: {result['file_count']} files, {result['total_size_mb']:.1f} MB")

    if result["errors"]:
        print(f"\n⚠️  PARTIAL SUCCESS - {len(result['errors'])} dataset(s) failed:")
        for dataset, error in result["errors"]:
            print(f"  ❌ {dataset}: {error[:80]}...")
        sys.exit(0 if result["file_count"] > 0 else 1)
    else:
        print("\n✅ All downloads successful!")
        sys.exit(0)


if __name__ == "__main__":
    main()
