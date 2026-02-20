#!/usr/bin/env python3
"""
Build JAR first (unless GBX_BUNDLE_SKIP_JAR_UPLOAD=1), then build the GeoBrix Python wheel (python3 -m build)
and upload to GBX_ARTIFACT_VOLUME/<whl_filename>. JAR is built before the wheel so the package can include it.
Set GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 to skip wheel build/upload. Loads config from databricks_cluster_config.env.
Overwrites if file already exists.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

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
    artifact_volume = (os.environ.get("GBX_ARTIFACT_VOLUME") or "").strip().rstrip("/")
    if not artifact_volume:
        print("Set GBX_ARTIFACT_VOLUME (e.g. /Volumes/catalog/schema/volume/artifacts)", file=sys.stderr)
        return 2

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
        return 2

    skip_wheel = os.environ.get("GBX_BUNDLE_SKIP_WHEEL_UPLOAD", "").strip().lower() in ("1", "true", "yes")
    skip_jar = os.environ.get("GBX_BUNDLE_SKIP_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    project_root = TESTS_DIR.parent.parent
    pkg_dir = project_root / "python" / "geobrix"
    dist = pkg_dir / "dist"

    # JAR first so the wheel build can include it if needed
    if not skip_jar:
        lib_dir = pkg_dir / "lib"
        if lib_dir.exists():
            shutil.rmtree(lib_dir)
        lib_dir.mkdir(parents=True)
        print("Running push_jar_to_volume (JAR before wheel)...")
        rc = subprocess.run([sys.executable, str(TESTS_DIR / "push_jar_to_volume.py")], cwd=project_root)
        if rc.returncode != 0:
            return rc.returncode
    else:
        print("GBX_BUNDLE_SKIP_JAR_UPLOAD=1: skipping JAR push.")

    if not skip_wheel:
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
            return 2

        if dist.exists():
            shutil.rmtree(dist)
        dist.mkdir(parents=True)
        print("Building wheel (python3 -m build)...")
        rc = subprocess.run([sys.executable, "-m", "build", str(pkg_dir)], cwd=project_root, capture_output=False)
        if rc.returncode != 0:
            print("Build failed", file=sys.stderr)
            return 1
        whl = next((f for f in dist.glob("geobrix-*.whl")), None)
        if not whl:
            print("No geobrix-*.whl in dist/", file=sys.stderr)
            return 1

        volume_path = f"{artifact_volume}/{whl.name}"
        w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)
        try:
            w.files.create_directory(artifact_volume)
        except Exception:
            pass
        print("Uploading to %s (overwrite if exists)..." % volume_path)
        w.files.upload_from(
            file_path=volume_path,
            source_path=str(whl.resolve()),
            overwrite=True,
            use_parallel=False,
        )
        print("Done: %s" % volume_path)
    else:
        print("GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1: skipping wheel build/upload.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
