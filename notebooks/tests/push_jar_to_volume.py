#!/usr/bin/env python3
"""
Build the GeoBrix JAR (mvn clean package -DskipTests) and upload *-jar-with-dependencies.jar
to GBX_ARTIFACT_VOLUME/<jar_filename>. Set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to build the JAR
locally but skip the Databricks upload; set GBX_BUNDLE_SKIP_JAR=1 to skip the JAR step
ENTIRELY (no build, no upload). Loads config from notebooks/tests/databricks_cluster_config.env.
Overwrites if file already exists.
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
    # GBX_BUNDLE_SKIP_JAR: skip the JAR step ENTIRELY (no Maven build, no upload) —
    # for light-tier-only changes (pyrx/pyvx/pygx wheel) where the staged JAR is
    # unchanged. Distinct from GBX_BUNDLE_SKIP_JAR_UPLOAD, which still builds the JAR.
    if os.environ.get("GBX_BUNDLE_SKIP_JAR", "").strip().lower() in ("1", "true", "yes"):
        print("GBX_BUNDLE_SKIP_JAR=1: skipping JAR build + upload (wheel-only stage).")
        return 0

    skip_jar_upload = os.environ.get("GBX_BUNDLE_SKIP_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    project_root = TESTS_DIR.parent.parent

    # Auth/volume checks: only required when uploading
    if not skip_jar_upload:
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

    # Corp firewall blocks repo.maven.apache.org on the host; the geobrix-dev container
    # has the db-maven-proxy mirror wired up via scripts/docker/m2/settings.xml.
    mvn_cmd = (
        'unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && '
        'export MAVEN_OPTS="-Xmx4G -XX:+UseG1GC" && '
        'cd /root/geobrix && mvn clean package -DskipTests'
    )
    print("Running in geobrix-dev: mvn clean package -DskipTests ...")
    rc = subprocess.run(
        ["docker", "exec", "geobrix-dev", "/bin/bash", "-c", mvn_cmd],
        cwd=project_root,
        capture_output=False,
    )
    if rc.returncode != 0:
        print("Maven build failed (is the geobrix-dev container running? try gbx:docker:start)", file=sys.stderr)
        return 1

    target = project_root / "target"
    jars = list(target.glob("*-jar-with-dependencies.jar"))
    if not jars:
        print("No *-jar-with-dependencies.jar found in target/", file=sys.stderr)
        return 1
    jar_path = jars[0]

    if skip_jar_upload:
        print("GBX_BUNDLE_SKIP_JAR_UPLOAD=1: JAR built locally; skipping Databricks upload.")
        return 0

    volume_path = f"{artifact_volume}/{jar_path.name}"

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)
    try:
        w.files.create_directory(artifact_volume)
    except Exception:
        pass
    print("Uploading product jar to %s (overwrite if exists)..." % volume_path)
    w.files.upload_from(
        file_path=volume_path,
        source_path=str(jar_path.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % volume_path)

    # Also stage the bench tests.jar (carries HeavyRunner / the bench Scala harness). The
    # bench launcher attaches it as a cluster library from the BUNDLE volroot -- a DIFFERENT
    # Volume than the artifact volume the init script reads the product jar from. The same
    # `mvn package` already built it (maven-jar-plugin bench-test-jar goal), so stage it here
    # too: one command keeps both jars current (no manual fs cp). Skippable for product-only.
    if os.environ.get("GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD", "").strip().lower() in ("1", "true", "yes"):
        print("GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1: skipping tests.jar upload.")
        return 0

    test_jars = [j for j in target.glob("*-tests.jar")]
    if not test_jars:
        print("WARNING: no *-tests.jar in target/; tests.jar NOT staged (heavy bench needs it).", file=sys.stderr)
        return 0
    test_jar = test_jars[0]

    # Match the launcher's resolution exactly: explicit override, else BUNDLE volroot.
    tests_jar_dest = (os.environ.get("GBX_BENCH_TESTS_JAR_VOLUME_PATH") or "").strip()
    if not tests_jar_dest:
        b_cat = (os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main").strip()
        b_sch = (os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default").strip()
        b_vol = (os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples").strip()
        b_root = f"/Volumes/{b_cat}/{b_sch}/{b_vol}"
        try:
            w.files.create_directory(b_root)
        except Exception:
            pass
        tests_jar_dest = f"{b_root}/{test_jar.name}"
    print("Uploading tests.jar to %s (overwrite if exists)..." % tests_jar_dest)
    w.files.upload_from(
        file_path=tests_jar_dest,
        source_path=str(test_jar.resolve()),
        overwrite=True,
        use_parallel=False,
    )
    print("Done: %s" % tests_jar_dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
