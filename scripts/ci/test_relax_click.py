"""scripts/native/relax-click.py: the one edit that admits click 8.2.1 in rasterio, and the revision stamp.

Pure Python, no Docker and no network: the sdist is a three-file scratch tree.
"""

import subprocess
import sys

import pytest
from conftest import REPO_ROOT

RELAX = REPO_ROOT / "scripts" / "native" / "relax-click.py"
PYPROJECT = 'dependencies = [\n    "affine",\n    "click>=4.0,!=8.2.*",\n    "click-plugins",\n    "cligj>=0.5",\n]\n'


def sdist(tmp_path, pyproject, setup_py=""):
    (tmp_path / "rasterio").mkdir()
    (tmp_path / "rasterio" / "__init__.py").write_text(
        'import os\n__version__ = "1.5.2"\n'
    )
    (tmp_path / "pyproject.toml").write_text(pyproject)
    if setup_py:
        (tmp_path / "setup.py").write_text(setup_py)
    return tmp_path


def relax(root):
    return subprocess.run(
        [sys.executable, str(RELAX), str(root), "--revision", "7"],
        capture_output=True,
        text=True,
    )


def test_relax_click_edits_exactly_one_requirement_and_stamps_the_revision(tmp_path):
    (tmp_path / "ok").mkdir()
    ok = relax(sdist(tmp_path / "ok", PYPROJECT))
    assert ok.returncode == 0, ok.stdout + ok.stderr
    text = (tmp_path / "ok" / "pyproject.toml").read_text()
    assert '"click>=4.0",' in text and "8.2" not in text
    # the neighbours that start with "click" or "cligj" are untouched
    assert '"click-plugins",' in text and '"cligj>=0.5",' in text
    assert (
        '__version__ = "1.5.2+gbx7"'
        in (tmp_path / "ok" / "rasterio" / "__init__.py").read_text()
    )
    assert "before:" in ok.stdout and "after:" in ok.stdout

    # no exclusion to remove, and two of them, are both refused; nothing is stamped then
    for name, pyproject, setup_py in (
        ("none", PYPROJECT.replace(",!=8.2.*", ""), ""),
        ("two", PYPROJECT, 'install_requires = ["click>=4.0,!=8.2.*"]\n'),
    ):
        (tmp_path / name).mkdir()
        bad = relax(sdist(tmp_path / name, pyproject, setup_py))
        assert (
            bad.returncode == 1 and "RELAX FAILED: expected exactly one" in bad.stdout
        ), name
        assert (
            "gbx" not in (tmp_path / name / "rasterio" / "__init__.py").read_text()
        ), name
