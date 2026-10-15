"""
Simplified tests for workdir path rewrite: one data source, assert relative path
is used for both read and write so we don't see "(already exists)" when workdir is empty.

Run: gbx:test:notebooks --path test_workdir_path_rewrite.py
"""

import os
import tempfile
from pathlib import Path

import pytest

# Run cell-by-cell only when under gbx-test-notebooks (Docker)
def _use_cell_by_cell() -> bool:
    if os.environ.get("GBX_NOTEBOOK_TESTS_DOCKER") != "1":
        return False
    v = (os.environ.get("GBX_NOTEBOOK_VERBOSITY") or "truncated").strip().lower()
    return v != "quiet"


@pytest.fixture
def workdir_path_minimal_notebook(notebooks_root: Path) -> Path:
    """Path to workdir_path_minimal.ipynb (config + run_essential_bundle)."""
    return notebooks_root / "tests" / "fixtures" / "workdir_path_minimal.ipynb"


def test_workdir_path_rewrite_config_cell_exec_uses_workdir_path():
    """Unit test: when we replace get_volumes_path( with __gbx_get_volumes_path__( and exec,
    SAMPLE_DATA_PATH must be under workdir. No notebook, no bundle - just exec with same globals.
    """
    with tempfile.TemporaryDirectory(prefix="nb_work_") as tmp:
        workdir = Path(tmp)
        # Same injection as _run_notebook_cells when workdir is set and not allow_absolute_reads
        def _patched_get_volumes_path(catalog: str, schema: str, volume: str) -> str:
            return str(workdir / "Volumes" / catalog / schema / volume / "geobrix-examples")

        globals_dict = {
            "__name__": "__main__",
            "__gbx_workdir__": workdir,
            "__gbx_get_volumes_path__": _patched_get_volumes_path,
        }
        # Config cell source with replace applied (as runner should do)
        source = """from pathlib import Path
CATALOG = "main"
SCHEMA = "default"
VOLUME = "geobrix_samples"
SAMPLE_DATA_PATH = __gbx_get_volumes_path__(CATALOG, SCHEMA, VOLUME)
print(f"SAMPLE_DATA_PATH = {SAMPLE_DATA_PATH}")
"""
        exec(compile(source, "<workdir_rewrite_test>", "exec"), globals_dict)
        path = globals_dict["SAMPLE_DATA_PATH"]
        assert path.startswith(str(workdir)), (
            f"Expected SAMPLE_DATA_PATH under workdir {workdir}, got {path}"
        )
        assert "geobrix-examples" in path


def test_workdir_path_minimal_notebook_uses_workdir_and_no_already_exists(
    capsys: pytest.CaptureFixture[str],
    workdir_path_minimal_notebook: Path,
    notebooks_root: Path,
) -> None:
    """Run minimal notebook (config + run_essential_bundle) with workdir.
    Assert SAMPLE_DATA_PATH in output is under workdir and we do NOT see '(already exists)'
    for every source (so read/write use workdir, not real /Volumes).
    """
    if not _use_cell_by_cell():
        pytest.skip("Run via gbx:test:notebooks (cell-by-cell with workdir)")
    assert workdir_path_minimal_notebook.is_file(), f"Fixture missing: {workdir_path_minimal_notebook}"

    from test_notebook_via_script import run_notebook_cell_by_cell

    project_root = notebooks_root.parent
    with tempfile.TemporaryDirectory(prefix="nb_work_") as tmp:
        workdir = Path(tmp)
        ok = run_notebook_cell_by_cell(
            workdir_path_minimal_notebook,
            cwd=project_root,
            workdir=workdir,
        )
    assert ok, "Minimal notebook cell-by-cell execution failed (see output above)"

    captured = capsys.readouterr()
    out = captured.out + captured.err

    # Path must be under workdir (relative path switch worked for both read and write)
    assert str(workdir) in out, (
        f"Expected SAMPLE_DATA_PATH to contain workdir {workdir}. Output snippet: {out[:500]}"
    )
    # When workdir is used and empty, we must not see "(already exists)" for every source
    # (that would mean we're still reading/writing the real /Volumes path).
    assert "(already exists)" not in out, (
        "Output contained '(already exists)' - path rewrite did not apply; reads/writes used real /Volumes. "
        "SAMPLE_DATA_PATH should be under workdir so bundle downloads to temp."
    )
