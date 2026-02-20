#!/usr/bin/env python3
"""
GeoBrix Complete Bundle - Additional Datasets (~440MB more, ~795MB total)

Run AFTER Essential Bundle setup. Adds neighborhoods, extra elevation,
HRRR weather, shapefiles (parks, subway), and multi-layer GeoPackage.

Single Source of Truth:
    - Logic lives in bundle_lib; this script and the notebook both use it
    - Documentation imports this file (docs/docs/sample-data/setup.mdx)
    - Tested by: docs/tests/python/setup/test_bundles.py
"""
import sys
import subprocess
from pathlib import Path

# Configuration - must match Essential Bundle
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


def download_file(url, subfolder, filename, description):
    """Download a file to Unity Catalog Volumes. Used by docs/tests."""
    import bundle_lib
    out = Path(SAMPLE_DATA_PATH) / subfolder / filename
    return bundle_lib.download_to_path(url, out, description, skip_if_exists=False)


def download_srtm_aws(tile, subfolder, filename, description):
    """Download SRTM from AWS and write as GeoTIFF. Used by docs/tests. filename should be .tif."""
    import bundle_lib
    out = Path(SAMPLE_DATA_PATH) / subfolder / filename
    if out.suffix.lower() == ".tif":
        hgt = Path(SAMPLE_DATA_PATH) / subfolder / (out.stem + ".hgt")
        out.parent.mkdir(parents=True, exist_ok=True)
        bundle_lib.download_srtm_to_path(tile, hgt, description, skip_if_exists=False)
        if hgt.exists():
            bbox = (-73.0, 40.0, -72.0, 41.0)  # NYC East
            bundle_lib.srtm_hgt_to_geotiff(hgt, out, bbox, description, quiet=True)
            hgt.unlink(missing_ok=True)
        return out if out.exists() else hgt
    return bundle_lib.download_srtm_to_path(tile, out, description, skip_if_exists=False)


def main():
    """Download Complete Bundle additions via bundle_lib. Uses temp for interim work."""
    try:
        import requests
    except ImportError:
        print("Installing requests...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "requests", "--break-system-packages"])
        import requests

    import bundle_lib

    print("=" * 70)
    print("GeoBrix Complete Bundle - Additional Downloads (~440MB more, ~795MB total)")
    print("=" * 70)

    result = bundle_lib.run_complete_bundle(SAMPLE_DATA_PATH)

    print("\n" + "=" * 70)
    print("✅ Complete Bundle Setup Finished!")
    print("=" * 70)
    print(f"\nLocation: {SAMPLE_DATA_PATH}")
    print(f"Total files: {result['file_count']}, {result['total_size_mb']:.1f} MB")
    if result["errors"]:
        for dataset, error in result["errors"]:
            print(f"  ⚠️  {dataset}: {error[:80]}...")


if __name__ == "__main__":
    main()
