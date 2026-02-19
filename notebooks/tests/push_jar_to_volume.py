#!/usr/bin/env python3
"""
Build the GeoBrix JAR (mvn clean package -DskipTests) and upload *-jar-with-dependencies.jar
to GBX_ARTIFACT_VOLUME/<jar_filename>. Set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to skip build/upload.
Loads config from notebooks/tests/databricks_cluster_config.env. Overwrites if file already exists.
"""
from __future__ import annotations

import os
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
    if os.environ.get("GBX_BUNDLE_SKIP_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes"):
        print("GBX_BUNDLE_SKIP_JAR_UPLOAD=1: skipping JAR build/upload.")
        return 0

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

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    project_root = TESTS_DIR.parent.parent
    print("Running: mvn clean package -DskipTests ...")
    rc = subprocess.run(["mvn", "clean", "package", "-DskipTests"], cwd=project_root, capture_output=False)
    if rc.returncode != 0:
        print("Maven build failed", file=sys.stderr)
        return 1

    target = project_root / "target"
    jars = list(target.glob("*-jar-with-dependencies.jar"))
    if not jars:
        print("No *-jar-with-dependencies.jar found in target/", file=sys.stderr)
        return 1
    jar_path = jars[0]
    volume_path = f"{artifact_volume}/{jar_path.name}"

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)
    try:
        w.files.create_directory(artifact_volume)
    except Exception:
        pass
    print("Uploading to %s (overwrite if exists)..." % volume_path)
    w.files.upload_from(
        file_path=volume_path,
        source_path=str(jar_path.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % volume_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
