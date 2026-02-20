#!/usr/bin/env python3
"""
Download Essential Bundle - Core datasets for GeoBrix examples (~355MB).

Thin wrapper: uses the same bundle_lib as the docs notebook and test-friendly
scripts (docs/tests/python/setup/). Run this from repo root or sample-data/.

  python sample-data/download-essential-bundle.py
  # or from sample-data/:
  python download-essential-bundle.py
"""

import sys
from pathlib import Path

# Resolve path to bundle_lib (docs/tests/python/setup/)
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parent
_setup_dir = _repo_root / "docs" / "tests" / "python" / "setup"
if not _setup_dir.is_dir():
    print(f"❌ bundle_lib not found at {_setup_dir}; run from repo root.")
    sys.exit(1)
sys.path.insert(0, str(_setup_dir))

import bundle_lib

SAMPLE_DATA_PATH = bundle_lib.get_volumes_path("main", "default", "geobrix_samples")
Path(SAMPLE_DATA_PATH).mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("GeoBrix Essential Bundle")
print("=" * 70)
print(f"Downloading ~355MB to: {SAMPLE_DATA_PATH}\n")

# Install deps then run (same as essential_bundle.main())
import subprocess
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "-q",
    "--break-system-packages", "pystac-client", "planetary-computer", "requests"
])

result = bundle_lib.run_essential_bundle(SAMPLE_DATA_PATH)

print("\n" + "=" * 70)
print("Essential Bundle Complete!")
print("=" * 70)
print(f"\nData: {SAMPLE_DATA_PATH}")
print(f"Files: {result['file_count']}, {result['total_size_mb']:.1f} MB")
if result["errors"]:
    for name, err in result["errors"]:
        print(f"  ⚠️ {name}: {err[:80]}...")
sys.exit(0 if result["file_count"] > 0 else 1)
