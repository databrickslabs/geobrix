#!/usr/bin/env python3
"""
Build the GeoBrix Python wheel and upload it to the Volume path in GBX_BUNDLE_WHEEL_VOLUME_PATH.

Overwrites if the file already exists. Loads config from notebooks/tests/databricks_cluster_config.env.

Usage:
  Set GBX_BUNDLE_WHEEL_VOLUME_PATH (e.g. /Volumes/main/default/geobrix_samples/wheels/geobrix.whl),
  DATABRICKS_HOST, DATABRICKS_TOKEN (or DATABRICKS_CONFIG_PROFILE). Then:
  python push_wheel_to_volume.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"
if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = v


def main() -> int:
    volume_path = os.environ.get("GBX_BUNDLE_WHEEL_VOLUME_PATH")
    if not volume_path:
        print("Set GBX_BUNDLE_WHEEL_VOLUME_PATH (e.g. /Volumes/catalog/schema/volume/wheels/geobrix.whl)", file=sys.stderr)
        return 2

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
        return 2

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    project_root = TESTS_DIR.parent.parent
    dist = project_root / "python" / "geobrix" / "dist"
    dist.mkdir(parents=True, exist_ok=True)

    print("Building wheel...")
    subprocess.run(
        [
            sys.executable, "-m", "pip", "wheel",
            "--no-deps", "-w", str(dist),
            str(project_root / "python" / "geobrix"),
        ],
        check=True,
        capture_output=False,
    )
    whl = next((f for f in dist.glob("geobrix-*.whl")), None)
    if not whl:
        print("No wheel produced", file=sys.stderr)
        return 1

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)
    try:
        w.files.create_directory(str(Path(volume_path).parent))
    except Exception:
        pass
    print("Uploading to %s (overwrite=True)..." % volume_path)
    w.files.upload_from(
        file_path=volume_path,
        source_path=str(whl.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % volume_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
