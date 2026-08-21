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
        # The build's pip index comes from the AMBIENT environment (PIP_INDEX_URL /
        # pip.conf). If your network firewalls public PyPI, set PIP_INDEX_URL to your
        # mirror before running -- we do not hardcode one here.
        build_env = dict(os.environ)
        # Prefer the project venv (.venv-pyrx, Python 3.12) for the build: it has a coherent
        # build backend, so `--no-isolation` builds without any PyPI fetch (the ambient
        # `python` may be an unrelated interpreter with a mismatched setuptools/packaging that
        # breaks even isolated builds). Fall back to sys.executable + isolation + the proxy
        # (the CI path, where the runner's env is clean and just needs the mirror).
        venv_py = Path(project_root) / ".venv-pyrx" / "bin" / "python"
        if venv_py.exists():
            build_cmd = [str(venv_py), "-m", "build", "--no-isolation", str(pkg_dir)]
        else:
            build_cmd = [sys.executable, "-m", "build", str(pkg_dir)]
        rc = subprocess.run(
            build_cmd, cwd=project_root, capture_output=False, env=build_env
        )
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
        # Use files.upload (streaming PUT) — NOT files.upload_from. upload_from hits
        # an admin-gated bulk API that PermissionDenies non-account-admins on some
        # workspaces (e.g. dogfood: "This API is disabled for users without account
        # admin status"), while the streaming files.upload works for everyone.
        with open(whl.resolve(), "rb") as _fh:
            w.files.upload(volume_path, _fh, overwrite=True)
        print("Done: %s" % volume_path)
    else:
        print("GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1: skipping wheel build/upload.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
