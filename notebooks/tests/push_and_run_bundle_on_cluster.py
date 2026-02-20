#!/usr/bin/env python3
"""
Push the Essential bundle runner to the workspace and run it on a configured cluster.

Use this to test the bundle (or an updated _bundle.py) on a live Databricks cluster:
the script uploads a small notebook that runs run_essential_bundle() on the cluster,
then runs that notebook as a one-off job. The cluster must have GeoBrix installed
(e.g. as a cluster library or via init script), or you can build a wheel and upload
it to a Volume; set GBX_ARTIFACT_VOLUME (or GBX_BUNDLE_WHEEL_VOLUME_PATH) so the notebook installs it first.

Requires: databricks-sdk, and env config (see databricks_cluster_config.example.env).

Usage:
  1. Copy databricks_cluster_config.example.env to databricks_cluster_config.env.
  2. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*.
  3. Optional: set GBX_ARTIFACT_VOLUME (or GBX_BUNDLE_WHEEL_VOLUME_PATH) for wheel; notebook will %pip install from that path.
  4. Run: python push_and_run_bundle_on_cluster.py [--no-wait]
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"


def _strip_invisible(s: str) -> str:
    """Remove BOM and common invisible Unicode so env-derived paths are clean."""
    s = (s or "").strip()
    for c in ("\ufeff", "\u200b", "\u200c", "\u200d", "\r"):
        s = s.replace(c, "")
    return s.strip()


if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = _strip_invisible(v)


def _geobrix_version() -> str:
    """Read version from python package __init__.py (avoid heavy imports)."""
    init_py = TESTS_DIR.parent.parent / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx" / "__init__.py"
    if init_py.exists():
        with open(init_py) as f:
            for line in f:
                line = line.strip()
                if line.startswith("__version__"):
                    # __version__ = "0.2.0"
                    if "=" in line:
                        v = line.split("=", 1)[1].strip().strip("'\"").strip()
                        if v:
                            return v
    return "0.2.0"


def _notebook_json(
    catalog: str,
    schema: str,
    volume: str,
    wheel_volume_path: str | None,
) -> bytes:
    """Build a minimal .ipynb as JSON: optional pip cells (two) for wheel, then run_essential_bundle."""
    cells = []
    if wheel_volume_path:
        # 1) Install deps and wheel
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [f"%pip install --quiet {wheel_volume_path}"],
        })
        # 2) Force-reinstall dead-latest GeoBrix (no deps)
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [f"%pip install --quiet --no-deps --force-reinstall {wheel_volume_path}"],
        })
    # install additional deps
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [f"%pip install --quiet pystac-client planetary-computer geopandas"],
    })
    # Restart Python so the new GeoBrix code is loaded (no SDK path in bundle).
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": ["dbutils.library.restartPython()"],
    })
    # setup path 1x
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "from databricks.labs.gbx.sample import get_volumes_path\n\n",
            f"path = get_volumes_path({repr(catalog)}, {repr(schema)}, {repr(volume)})\n",
            "path",
        ],
    })
    # option: clear existing
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "## -- uncomment to remove everything under 'geobrix-examples' subfolder\n",
            "# dbutils.fs.rm(path, recurse=True)",
        ],
    })
    # execute essential bundle
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "from databricks.labs.gbx.sample import run_essential_bundle\n\n",
            "result = run_essential_bundle(path)\n",
            "print(\"file_count:\", result[\"file_count\"], \"total_size_mb:\", result[\"total_size_mb\"])\n",
            "if result.get(\"errors\"):\n",
            "    for name, err in result[\"errors\"]:\n",
            "        print(f\"  error {name}: {err}\")",
        ],
    })
    # execute essential bundle
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "from databricks.labs.gbx.sample import run_complete_bundle\n\n",
            "result = run_complete_bundle(path)\n",
            "print(\"file_count:\", result[\"file_count\"], \"total_size_mb:\", result[\"total_size_mb\"])\n",
            "if result.get(\"errors\"):\n",
            "    for name, err in result[\"errors\"]:\n",
            "        print(f\"  error {name}: {err}\")",
        ],
    })
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "python"}},
        "cells": cells,
    }
    return json.dumps(nb, indent=1).encode("utf-8")


def main() -> int:
    do_wait = "--no-wait" not in sys.argv

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
        return 2

    cluster_id = os.environ.get("CLUSTER_ID")
    if not cluster_id:
        print("Set CLUSTER_ID (existing cluster to run the job on)", file=sys.stderr)
        return 2

    catalog = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main")
    schema = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default")
    volume = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples")
    runner_dir = os.environ.get("GBX_RUNNER_DIR")
    bundle_notebook = os.environ.get("GBX_BUNDLE_RUNNER_NOTEBOOK", "geobrix_bundle_runner.ipynb")
    if not bundle_notebook.endswith(".ipynb"):
        bundle_notebook = bundle_notebook.rstrip("/") + ".ipynb"
    notebook_path_from_env = (runner_dir.rstrip("/") + "/" + bundle_notebook) if (runner_dir and bundle_notebook) else os.environ.get("GBX_BUNDLE_RUNNER_NOTEBOOK_PATH")
    # Wheel path for notebook pip cells: explicit path or derived from GBX_ARTIFACT_VOLUME
    wheel_volume_path = _strip_invisible(os.environ.get("GBX_BUNDLE_WHEEL_VOLUME_PATH") or "").strip() or None
    if not wheel_volume_path:
        artifact_volume = _strip_invisible(os.environ.get("GBX_ARTIFACT_VOLUME") or "").strip().rstrip("/")
        if artifact_volume:
            ver = _geobrix_version()
            wheel_volume_path = f"{artifact_volume}/geobrix-{ver}-py3-none-any.whl"
    skip_wheel_upload = os.environ.get("GBX_BUNDLE_SKIP_WHEEL_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    try:
        import io
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)

    if wheel_volume_path and not skip_wheel_upload:
        # Build wheel (python3 -m build) and upload to Volume; set GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 to use existing
        project_root = TESTS_DIR.parent.parent
        pkg_dir = project_root / "python" / "geobrix"
        dist = pkg_dir / "dist"
        dist.mkdir(parents=True, exist_ok=True)
        print("Building wheel (python3 -m build)...")
        rc = subprocess.run([sys.executable, "-m", "build", str(pkg_dir)], cwd=project_root, capture_output=True)
        if rc.returncode != 0:
            print("Wheel build failed", file=sys.stderr)
            return 1
        whl = next((f for f in dist.glob("geobrix-*.whl")), None)
        if not whl:
            print("No wheel produced", file=sys.stderr)
            return 1
        try:
            w.files.create_directory(str(Path(wheel_volume_path).parent))
        except Exception:
            pass
        w.files.upload_from(wheel_volume_path, str(whl.resolve()), overwrite=True)
        print(f"Uploaded wheel to {wheel_volume_path}")

    nb_bytes = _notebook_json(catalog, schema, volume, wheel_volume_path)

    # 1. Define the path (use env runner dir + notebook name, or fallback to user folder)
    me = w.current_user.me()
    default_path = f"/Users/{me.user_name}/geobrix_bundle_runner.ipynb"
    notebook_path = notebook_path_from_env or default_path
    if not notebook_path.endswith(".ipynb"):
        notebook_path = notebook_path.rstrip("/") + ".ipynb"

    # Ensure it doesn't have the /Workspace prefix for the SDK calls
    notebook_path = "/" + notebook_path.strip().removeprefix("/Workspace").lstrip("/")

    # 2. Ensure parent directory exists (workspace API)
    notebook_parent = Path(notebook_path).parent
    try:
        w.workspace.mkdirs(str(notebook_parent))
    except Exception:
        pass

    print(f"Uploading runner notebook to {notebook_path}...")

    # 3. CRITICAL: Use ImportFormat.JUPYTER
    # This ensures the file is created as a 'NOTEBOOK' type, not a 'FILE' type.
    w.workspace.upload(
        notebook_path,
        io.BytesIO(nb_bytes),
        format=ImportFormat.JUPYTER,
        overwrite=True
    )

    print("Submitting one-off run on cluster...")
    submit_waiter = w.jobs.submit(
        run_name="geobrix-bundle-runner",
        timeout_seconds=3600,
        tasks=[
            jobs.SubmitTask(
                task_key="run_bundle",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path, # Same path used in upload
                    source=jobs.Source.WORKSPACE,
                ),
            )
        ],
    )

    if do_wait:
        print("Waiting for run to finish...")
        run = submit_waiter.result()
        run_id = run.run_id
        state = run.state
        if state and getattr(state, "life_cycle_state", None):
            lc = state.life_cycle_state.value if hasattr(state.life_cycle_state, "value") else str(state.life_cycle_state)
            if lc == "TERMINATED":
                result_state = (state.result_state.value if state.result_state and hasattr(state.result_state, "value") else str(state.result_state)) if state.result_state else "UNKNOWN"
                if result_state == "SUCCESS":
                    print("Run finished successfully.")
                    return 0
                print(f"Run finished with result_state={result_state}", file=sys.stderr)
        else:
            print(f"Run state: {state}", file=sys.stderr)
        return 1
    # Fire-and-forget: submit returns a waiter; we don't call .result()
    print("Run submitted. Use the Databricks UI to check status, or run without --no-wait to wait here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
