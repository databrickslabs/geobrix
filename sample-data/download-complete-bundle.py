#!/usr/bin/env python3
"""
Download Complete Bundle - Additional datasets (~440MB more, ~795MB total).

Thin wrapper: uses the same bundle_lib as the docs notebook and test-friendly
scripts. Run after download-essential-bundle.py.

The NYC File Geodatabase (nyc/filegdb/NYC_Sample.gdb.zip) is built by
run_complete_bundle itself when GDAL's Python bindings are available.
"""

import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parent
_setup_dir = _repo_root / "docs" / "tests" / "python" / "setup"
if not _setup_dir.is_dir():
    print(f"❌ bundle_lib not found at {_setup_dir}; run from repo root.")
    sys.exit(1)
sys.path.insert(0, str(_setup_dir))

import bundle_lib

SAMPLE_DATA_PATH = bundle_lib.get_volumes_path("main", "default", "geobrix_samples")

# Ensure essential bundle exists
for f in ["nyc/taxi-zones/nyc_taxi_zones.geojson", "nyc/boroughs/nyc_boroughs.geojson"]:
    if not (Path(SAMPLE_DATA_PATH) / f).exists():
        print(f"❌ Run download-essential-bundle.py first (missing {f}).")
        sys.exit(1)

print("=" * 70)
print("GeoBrix Complete Bundle")
print("=" * 70)
print(f"Adding ~440MB to: {SAMPLE_DATA_PATH}\n")

try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "requests", "--break-system-packages"])
    import requests

result = bundle_lib.run_complete_bundle(SAMPLE_DATA_PATH)

print("\n" + "=" * 70)
print("✅ Complete Bundle Finished!")
print("=" * 70)
print(f"\nLocation: {SAMPLE_DATA_PATH}")
print(f"Files: {result['file_count']}, {result['total_size_mb']:.1f} MB")
if result["errors"]:
    for name, err in result["errors"]:
        print(f"  ⚠️ {name}: {err[:80]}...")
sys.exit(0)
