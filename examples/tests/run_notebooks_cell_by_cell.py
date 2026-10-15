#!/usr/bin/env python3
"""
Run notebooks cell-by-cell (no Jupyter kernel). Used by gbx:test:notebooks.

Discovers notebooks under notebooks/ or runs only those under a given path.
Verbosity: GBX_NOTEBOOK_VERBOSITY=quiet|truncated|full (default: truncated).

Isolation (GBX_NOTEBOOK_ISOLATED_ENV=1, default for gbx-test-notebooks):
  - Every run uses a temporary venv; the inner process has GBX_NOTEBOOK_ISOLATED=1.
  - When path is a .py file: pytest runs inside that venv (same isolation).
  - When path is a dir or .ipynb or omitted: notebooks run cell-by-cell in that venv.
  - Each notebook gets a temporary working dir; shell/! and %pip run in the venv.

Usage (inside container):
  python3 run_notebooks_cell_by_cell.py [path]
  path: optional — test file (e.g. test_setup_sample_data.py), subdir (sample-data),
        or path to a single .ipynb. If omitted, runs fixtures + sample-data notebooks.

Exit: 0 if all pass, 1 if any failure.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Allow importing from same directory
TESTS_DIR = Path(__file__).resolve().parent
NOTEBOOKS_ROOT = TESTS_DIR.parent
PROJECT_ROOT = NOTEBOOKS_ROOT.parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_notebook_via_script import run_notebook_cell_by_cell


def _create_venv_and_run(venv_root: Path, path_arg: str | None) -> int:
    """Create venv at venv_root, install deps (including pytest if path_arg is .py), re-exec with GBX_NOTEBOOK_ISOLATED=1."""
    venv_python = venv_root / "bin" / "python"
    run_pytest = path_arg is not None and path_arg.endswith(".py")

    print("Creating isolated venv...", flush=True)
    subprocess.run(
        [sys.executable, "-m", "venv", str(venv_root)],
        check=True,
        capture_output=True,
        timeout=60,
        stdin=subprocess.DEVNULL,
    )
    print("Installing nbformat, nbconvert, requests...", flush=True)
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "-q", "nbformat", "nbconvert", "requests"],
        check=True,
        capture_output=True,
        cwd=str(PROJECT_ROOT),
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    if run_pytest:
        print("Installing pytest...", flush=True)
        subprocess.run(
            [str(venv_python), "-m", "pip", "install", "-q", "pytest", "testbook"],
            check=True,
            capture_output=True,
            cwd=str(PROJECT_ROOT),
            timeout=120,
            stdin=subprocess.DEVNULL,
        )
    print("Installing geobrix (editable)...", flush=True)
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "-q", "-e", str(PROJECT_ROOT / "python" / "geobrix")],
        check=True,
        capture_output=True,
        cwd=str(PROJECT_ROOT),
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    # Install deps used by sample-data notebooks (Sentinel-2, London boroughs, etc.) so %pip cells
    # and notebooks without a %pip cell (e.g. workdir_path_minimal) see them in the same venv.
    print("Installing notebook deps (pystac-client, planetary-computer, geopandas)...", flush=True)
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "-q", "pystac-client", "planetary-computer", "geopandas"],
        check=True,
        capture_output=True,
        cwd=str(PROJECT_ROOT),
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    print("Running in isolated env (GBX_NOTEBOOK_ISOLATED=1)...", flush=True)
    env = os.environ.copy()
    env["GBX_NOTEBOOK_ISOLATED"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [str(venv_python), str(TESTS_DIR / "run_notebooks_cell_by_cell.py")] + (sys.argv[1:] or [])
    r = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        timeout=3600,
        stdin=subprocess.DEVNULL,
    )
    return r.returncode


def discover_notebooks(path_arg: str | None) -> list[Path]:
    """Return list of notebook paths to run. path_arg limits scope when provided."""
    if not path_arg:
        # Default: fixtures + sample-data
        notebooks = []
        for sub in ("tests/fixtures", "sample-data"):
            d = NOTEBOOKS_ROOT / sub
            if d.is_dir():
                notebooks.extend(sorted(d.glob("*.ipynb")))
        return notebooks

    # path_arg can be "sample-data", "fixtures", "tests/fixtures", or path to .ipynb
    p = NOTEBOOKS_ROOT / path_arg
    if p.is_file() and p.suffix == ".ipynb":
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.ipynb"))
    # e.g. path_arg "fixtures" -> notebooks/tests/fixtures
    p2 = NOTEBOOKS_ROOT / "tests" / path_arg
    if p2.is_dir():
        return sorted(p2.glob("*.ipynb"))
    return []


def _run_pytest(path_arg: str) -> int:
    """Run pytest for the given test path (under TESTS_DIR). Return exit code."""
    test_path = TESTS_DIR / path_arg
    if not test_path.exists():
        test_path = TESTS_DIR / "sample-data" / path_arg
    if not test_path.exists():
        test_path = TESTS_DIR / "fixtures" / path_arg
    if not test_path.exists():
        print(f"Test file not found: {path_arg}", file=sys.stderr)
        return 1
    pytest_args = [
        sys.executable, "-m", "pytest", str(test_path), "-v",
        "-s", "--tb=short", "--color=yes",
    ]
    if os.environ.get("GBX_NOTEBOOK_INCLUDE_INTEGRATION") != "1":
        pytest_args.extend(["-m", "not integration"])
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)
    print("Running notebook tests (pytest)...", flush=True)
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)
    r = subprocess.run(pytest_args, cwd=str(TESTS_DIR))
    return r.returncode


def main() -> int:
    use_isolated_env = os.environ.get("GBX_NOTEBOOK_ISOLATED_ENV") == "1"
    already_isolated = os.environ.get("GBX_NOTEBOOK_ISOLATED") == "1"
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None

    if use_isolated_env and not already_isolated:
        with tempfile.TemporaryDirectory(prefix="nb_venv_") as venv_root:
            return _create_venv_and_run(Path(venv_root), path_arg)

    # Inner process (or isolation disabled): path_arg .py -> pytest; else run notebooks
    if path_arg is not None and path_arg.endswith(".py"):
        return _run_pytest(path_arg)

    notebooks = discover_notebooks(path_arg)
    if not notebooks:
        print("No notebooks to run.", file=sys.stderr)
        return 0
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)
    print("Running notebooks (cell-by-cell, isolated env + workdir)...", flush=True)
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", flush=True)
    cwd = PROJECT_ROOT
    use_workdir = already_isolated
    failed = []
    for nb in notebooks:
        workdir = None
        if use_workdir:
            workdir = Path(tempfile.mkdtemp(prefix="nb_work_"))
        try:
            ok = run_notebook_cell_by_cell(nb, cwd=cwd, workdir=workdir)
            if not ok:
                failed.append(nb)
        finally:
            if workdir is not None and workdir.exists():
                shutil.rmtree(workdir, ignore_errors=True)
    if failed:
        print("\nFailed notebooks:", [str(f) for f in failed], file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
