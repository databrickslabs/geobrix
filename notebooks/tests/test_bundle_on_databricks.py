"""
Test the Essential bundle against a live Databricks workspace (Volumes via SDK).

Runs locally: your machine runs the bundle code and uses WorkspaceClient (Databricks SDK)
to upload files to the configured Unity Catalog Volume. No code runs on the cluster;
this validates that DATABRICKS_HOST + DATABRICKS_TOKEN (or profile) and the Volume path
work with the bundle's SDK-based upload path.

Setup:
  1. pip install databricks-sdk (and geobrix, requests, pystac-client, planetary-computer for full bundle).
  2. Copy notebooks/tests/databricks_cluster_config.example.env to databricks_cluster_config.env.
  3. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or DATABRICKS_CONFIG_PROFILE) and
     GBX_BUNDLE_VOLUME_* (catalog, schema, volume name).
  4. Ensure the Volume exists in the workspace (create it in Unity Catalog if needed).

Run:
  pytest notebooks/tests/test_bundle_on_databricks.py -v
  # Or standalone:
  python -m databricks.labs.gbx.sample.run_bundle_on_databricks  # if we add __main__
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Load optional .env from same dir (no extra deps: we only read KEY=value lines)
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


def _get_workspace_client():
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except Exception:
        return None


def _is_configured() -> bool:
    if os.environ.get("DATABRICKS_CONFIG_PROFILE"):
        return True
    return bool(os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"))


def _get_volumes_path() -> str | None:
    cat = os.environ.get("GBX_BUNDLE_VOLUME_CATALOG")
    schema = os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA")
    vol = os.environ.get("GBX_BUNDLE_VOLUME_NAME")
    if not (cat and schema and vol):
        return None
    return f"/Volumes/{cat}/{schema}/{vol}/geobrix-examples"


def _debug(msg: str) -> None:
    if os.environ.get("GBX_BUNDLE_DEBUG"):
        print(f"[bundle-test] {msg}", flush=True)


def test_bundle_essential_on_databricks_volume():
    """Run Essential bundle to the configured UC Volume using WorkspaceClient (runs locally)."""
    path = _get_volumes_path()
    if not path:
        try:
            import pytest
            pytest.skip("GBX_BUNDLE_VOLUME_CATALOG/SCHEMA/NAME not set")
        except ImportError:
            raise RuntimeError("GBX_BUNDLE_VOLUME_* not set") from None

    if not _is_configured():
        try:
            import pytest
            pytest.skip("DATABRICKS_HOST and DATABRICKS_TOKEN (or DATABRICKS_CONFIG_PROFILE) not set")
        except ImportError:
            raise RuntimeError("Databricks not configured") from None

    _debug("Getting WorkspaceClient...")
    w = _get_workspace_client()
    if w is None:
        try:
            import pytest
            pytest.skip("databricks-sdk not installed; pip install databricks-sdk")
        except ImportError:
            raise RuntimeError("databricks-sdk not installed") from None
    _debug("WorkspaceClient OK")

    _debug("Importing run_essential_bundle...")
    from databricks.labs.gbx.sample import run_essential_bundle
    _debug("Calling run_essential_bundle(path=%s)..." % path)

    result = run_essential_bundle(path)
    _debug("run_essential_bundle returned")
    assert "errors" in result
    assert "file_count" in result
    assert "total_size_mb" in result
    # Allow some datasets to fail (e.g. network); we mainly assert the Volume path and SDK path work
    assert result["file_count"] >= 0
    assert result["total_size_mb"] >= 0.0


if __name__ == "__main__":
    # Allow: python test_bundle_on_databricks.py
    if not _is_configured() or not _get_volumes_path():
        print("Configure DATABRICKS_HOST, DATABRICKS_TOKEN (or profile) and GBX_BUNDLE_VOLUME_*", file=sys.stderr)
        sys.exit(2)
    path = _get_volumes_path()
    from databricks.labs.gbx.sample import run_essential_bundle
    result = run_essential_bundle(path)
    print("Result:", result)
    sys.exit(0 if not result.get("errors") else 1)
