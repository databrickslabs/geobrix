"""
Tests for notebooks/sample-data/setup_sample_data.ipynb.

When run via gbx-test-notebooks (GBX_NOTEBOOK_TESTS_DOCKER=1), execution tests use
cell-by-cell (no kernel). Otherwise they use testbook (requires Jupyter kernel).
Mirrors the notebook path: notebooks/sample-data/ -> notebooks/tests/sample-data/.
Run: gbx:test:notebooks --path test_setup_sample_data.py
Requires: pytest, nbformat; testbook only when not using cell-by-cell.
"""

import os
import tempfile
from pathlib import Path

import pytest

# Run cell-by-cell only when: (1) under gbx-test-notebooks (Docker), and (2) mode is not quiet
def _use_cell_by_cell() -> bool:
    if os.environ.get("GBX_NOTEBOOK_TESTS_DOCKER") != "1":
        return False
    v = (os.environ.get("GBX_NOTEBOOK_VERBOSITY") or "truncated").strip().lower()
    return v != "quiet"


def test_setup_sample_data_notebook_exists(setup_sample_data_notebook_path: Path) -> None:
    """The setup_sample_data.ipynb file exists at the expected path."""
    assert setup_sample_data_notebook_path.is_file(), (
        f"Notebook not found: {setup_sample_data_notebook_path}"
    )


def test_setup_sample_data_notebook_structure(setup_sample_data_notebook_path: Path) -> None:
    """Notebook has expected structure: cells and at least one code cell with sample module import."""
    import nbformat

    nb = nbformat.read(str(setup_sample_data_notebook_path), as_version=4)
    assert len(nb.cells) > 0, "Notebook has no cells"
    code_cells = [c for c in nb.cells if c.cell_type == "code"]
    assert len(code_cells) > 0, "Notebook has no code cells"
    first_code = "".join(code_cells[0].source)
    assert "databricks.labs.gbx.sample" in first_code, (
        "First code cell should import from databricks.labs.gbx.sample"
    )
    assert "get_volumes_path" in first_code
    assert "run_essential_bundle" in first_code
    assert "run_complete_bundle" in first_code


def test_setup_sample_data_notebook_executes_config_cell(
    setup_sample_data_notebook_path: Path,
) -> None:
    """Execute config cell and pip cell only (no bundle download). Uses cell-by-cell when run via gbx-test-notebooks (non-quiet).
    Asserts that the notebook runs without raising. Full notebook (1GB download) runs only in test_setup_sample_data_notebook_executes_fully."""
    if os.environ.get("GBX_NOTEBOOK_TESTS_DOCKER") == "1" and not _use_cell_by_cell():
        pytest.skip("Execution test skipped in quiet mode (use cell-by-cell when verbosity is not quiet)")
    if _use_cell_by_cell():
        from test_notebook_via_script import run_notebook_cell_by_cell
        project_root = setup_sample_data_notebook_path.resolve().parent.parent.parent
        with tempfile.TemporaryDirectory(prefix="nb_work_") as tmp:
            workdir = Path(tmp)
            # Run only first 2 code cells (config + pip); full bundle run is in test_setup_sample_data_notebook_executes_fully
            ok = run_notebook_cell_by_cell(
                setup_sample_data_notebook_path,
                cwd=project_root,
                workdir=workdir,
                max_code_cells=2,
            )
        assert ok, "Notebook cell-by-cell execution failed (see output above)"
        return
    from testbook import testbook
    with testbook(
        str(setup_sample_data_notebook_path),
        execute=range(2, 3),
    ) as tb:
        path = tb.ref("SAMPLE_DATA_PATH")
        assert path is not None
        path_str = str(path) if not isinstance(path, str) else path
        assert "geobrix" in path_str or "Volumes" in path_str


@pytest.mark.integration
def test_setup_sample_data_notebook_executes_fully(
    setup_sample_data_notebook_path: Path,
) -> None:
    """Execute the full notebook (downloads data). Uses cell-by-cell when run via gbx-test-notebooks (non-quiet). Use --include-integration to run."""
    if os.environ.get("GBX_NOTEBOOK_TESTS_DOCKER") == "1" and not _use_cell_by_cell():
        pytest.skip("Execution test skipped in quiet mode")
    if _use_cell_by_cell():
        from test_notebook_via_script import run_notebook_cell_by_cell
        project_root = setup_sample_data_notebook_path.resolve().parent.parent.parent
        with tempfile.TemporaryDirectory(prefix="nb_work_") as tmp:
            workdir = Path(tmp)
            ok = run_notebook_cell_by_cell(
                setup_sample_data_notebook_path,
                cwd=project_root,
                workdir=workdir,
            )
        assert ok, "Full notebook cell-by-cell execution failed (see output above)"
        return
    from testbook import testbook
    with testbook(str(setup_sample_data_notebook_path), execute=True) as tb:
        path = tb.ref("SAMPLE_DATA_PATH")
        assert path is not None
