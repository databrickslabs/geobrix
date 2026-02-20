#!/usr/bin/env python3
"""
Push the primitive Volume-runner notebook to the workspace and run it on the configured cluster.

This notebook runs on the cluster where /Volumes/... is FUSE-mounted, so Python sees it as the
local filesystem. It tests: (a) volume exists (os.path.exists, os.listdir), (b) create subdirs
(os.makedirs), (c) read from volume (open().read()), (d) write to volume (temp file + shutil.copy),
(e) write to local then copy to volume (shutil.copy). We avoid random access (seek) on volume paths.
Use it to validate that the cluster can see and use the volume before running the full bundle.

Requires: databricks-sdk, and env config (see databricks_cluster_config.example.env).

Usage:
  1. Copy databricks_cluster_config.example.env to databricks_cluster_config.env.
  2. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*.
  3. Optional: GBX_RUNNER_DIR and GBX_PRIMITIVE_RUNNER_NOTEBOOK (default: user folder).
  4. Run: python push_and_run_primitive_on_cluster.py [--no-wait]
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"
def _strip_invisible(s: str) -> str:
    """Remove BOM and common invisible Unicode so env-derived paths don't break in the notebook."""
    s = (s or "").strip()
    for c in ("\ufeff", "\u200b", "\u200c", "\u200d", "\r"):
        s = s.replace(c, "")
    return s.strip()


def find_hidden_chars(s: str) -> list[tuple[int, str, int]]:
    """Return list of (index, char_repr, codepoint) for non-ASCII-printable or invisible chars.
    Use on a line from the notebook or .env to find hidden characters.
    """
    out = []
    for i, c in enumerate(s):
        code = ord(c)
        if code < 32 or code > 126:  # non-printable ASCII
            out.append((i, repr(c), code))
        elif c in "\u200b\u200c\u200d\ufeff":  # common invisibles in BMP
            out.append((i, repr(c), code))
    return out


if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = _strip_invisible(v)


def _primitive_notebook_json(catalog: str, schema: str, volume: str) -> bytes:
    """Build a minimal .ipynb that runs five primitive Volume tests on the cluster.

    On the cluster, /Volumes/... is FUSE-mounted so Python sees it as the local filesystem.
    We use normal path ops (os.*, open, shutil) and avoid random access (seek); temp files
    are used for writes then copied to the volume.
    """
    catalog, schema, volume = _strip_invisible(catalog), _strip_invisible(schema), _strip_invisible(volume)
    volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
    # (a) Volume exists: os.path.exists + os.listdir (FUSE mount)
    cell_a = [
        "# (a) Volume exists — on cluster /Volumes is FUSE-mounted; use os.path / os.listdir\n",
        "import os\n",
        f"volume_root = {repr(volume_root)}\n",
        "print(f\"Using volume_root = {volume_root}\")\n",
        "exists = os.path.exists(volume_root)\n",
        "print(f\"(a) Volume exists (os.path.exists): {exists}\")\n",
        "if exists:\n",
        "    entries = os.listdir(volume_root)\n",
        "    print(f\"    os.listdir(volume_root): {len(entries)} entries\")\n",
        "else:\n",
        "    print(\"    Check GBX_BUNDLE_VOLUME_* (catalog/schema/volume name).\")\n",
    ]
    # (b) Create subdirs (FUSE: os.makedirs)
    cell_b = [
        "# (b) Create subdirs under volume (os.makedirs; avoid random access on volume paths)\n",
        "subdir = volume_root + \"/primitive_tests/subdir1\"\n",
        "os.makedirs(subdir, exist_ok=True)\n",
        "print(f\"(b) Created directory: {subdir}\")\n",
    ]
    # (d) Write to volume: temp file then shutil.copy (no random access on volume)
    cell_d = [
        "# (d) Write to volume: write to temp file, then shutil.copy to volume\n",
        "import shutil\n",
        "import tempfile\n",
        "d_path = volume_root + \"/primitive_tests/write_test.txt\"\n",
        "with tempfile.NamedTemporaryFile(mode=\"w\", suffix=\".txt\", delete=False) as f:\n",
        "    f.write(\"hello from primitive (d)\")\n",
        "    local_d = f.name\n",
        "try:\n",
        "    shutil.copy(local_d, d_path)\n",
        "    print(f\"(d) Wrote to volume: {d_path}\")\n",
        "finally:\n",
        "    os.unlink(local_d)\n",
    ]
    # (c) Read from volume: open().read() (sequential read, no seek)
    cell_c = [
        "# (c) Read from volume: open(path).read() — sequential read, no random access\n",
        "with open(d_path) as f:\n",
        "    content = f.read()\n",
        "print(f\"(c) Read from volume: {content}\")\n",
    ]
    # (e) Write to local then copy to volume (shutil.copy)
    cell_e = [
        "# (e) Write to local path then copy to volume (shutil.copy)\n",
        "local_e = \"/tmp/primitive_e.txt\"\n",
        "with open(local_e, \"w\") as f:\n",
        "    f.write(\"hello from primitive (e)\")\n",
        "e_path = volume_root + \"/primitive_tests/copy_test.txt\"\n",
        "shutil.copy(local_e, e_path)\n",
        "print(f\"(e) Copied {local_e} -> {e_path}\")\n",
    ]

    def make_cell(source_lines: list[str]) -> dict:
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": source_lines,
        }

    cells = [
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Injected volume config (from env when notebook was generated)\n",
                f"volume_root = {repr(volume_root)}\n",
            ],
        },
        make_cell(cell_a),
        make_cell(cell_b),
        make_cell(cell_d),
        make_cell(cell_c),
        make_cell(cell_e),
    ]
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
    primitive_notebook = os.environ.get("GBX_PRIMITIVE_RUNNER_NOTEBOOK", "geobrix_primitive_runner.ipynb")
    if not primitive_notebook.endswith(".ipynb"):
        primitive_notebook = primitive_notebook.rstrip("/") + ".ipynb"
    notebook_path_from_env = (runner_dir.rstrip("/") + "/" + primitive_notebook) if (runner_dir and primitive_notebook) else None

    try:
        import io
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)

    me = w.current_user.me()
    default_path = f"/Users/{me.user_name}/geobrix_primitive_runner.ipynb"
    notebook_path = notebook_path_from_env or default_path
    if not notebook_path.endswith(".ipynb"):
        notebook_path = notebook_path.rstrip("/") + ".ipynb"

    notebook_path = "/" + notebook_path.strip().removeprefix("/Workspace").lstrip("/")
    notebook_parent = str(Path(notebook_path).parent)
    try:
        w.workspace.mkdirs(notebook_parent)
    except Exception:
        pass

    nb_bytes = _primitive_notebook_json(catalog, schema, volume)
    print(f"Uploading primitive runner notebook to {notebook_path}...")
    w.workspace.upload(
        notebook_path,
        io.BytesIO(nb_bytes),
        format=ImportFormat.JUPYTER,
        overwrite=True,
    )

    print("Submitting one-off run (primitive tests) on cluster...")
    submit_waiter = w.jobs.submit(
        run_name="geobrix-primitive-runner",
        timeout_seconds=3600,
        tasks=[
            jobs.SubmitTask(
                task_key="run_primitive",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
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
    print("Run submitted. Use the Databricks UI to check status, or run without --no-wait to wait here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
