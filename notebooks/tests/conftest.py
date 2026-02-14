"""Pytest configuration and fixtures for notebook tests.

Notebook tests must run inside the geobrix-dev Docker container so that the
Jupyter kernel, GeoBrix package, and paths are consistent. When invoked
locally, use: bash .cursor/commands/gbx-test-notebooks.sh
"""

import os
import sys
from pathlib import Path

import pytest

# Root of the notebooks tree (parent of tests/)
NOTEBOOKS_ROOT = Path(__file__).resolve().parent.parent
# Tests root so subdirs can import test modules
TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))


def pytest_configure(config):
    """Require notebook tests to run inside Docker (gbx:test:notebooks)."""
    if os.environ.get("GBX_NOTEBOOK_TESTS_DOCKER") == "1":
        return
    # Optional: allow CI or explicit opt-out for host runs (e.g. CI without Docker)
    if os.environ.get("GBX_NOTEBOOK_TESTS_ALLOW_HOST") == "1":
        return
    # Running outside container (e.g. pytest notebooks/tests/ on host)
    raise pytest.UsageError(
        "Notebook tests must run inside the geobrix-dev Docker container. "
        "Use: bash .cursor/commands/gbx-test-notebooks.sh"
    )


@pytest.fixture(scope="session")
def notebooks_root() -> Path:
    """Root path of the notebooks directory (notebooks/)."""
    return NOTEBOOKS_ROOT


@pytest.fixture(scope="session")
def setup_sample_data_notebook_path(notebooks_root: Path) -> Path:
    """Path to setup_sample_data.ipynb (mirrors notebooks/sample-data/)."""
    return notebooks_root / "sample-data" / "setup_sample_data.ipynb"
