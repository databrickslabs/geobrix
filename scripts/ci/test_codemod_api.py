"""The API codemod (a migration tool): a second run over the repo's callers changes nothing."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "manifest"))

import codemod_api  # noqa: E402


def test_a_second_codemod_run_over_the_callers_changes_nothing():
    from databricks.labs.gbx import manifest

    targets = codemod_api._targets(sorted(manifest.load().functions))
    for root in ("python/geobrix/test", "docs/tests/python", "examples"):
        for path in sorted((REPO_ROOT / root).rglob("*")):
            if path.suffix == ".py":
                old = path.read_text()
                new, _ = codemod_api.rewrite(old, targets)
                assert new == old, f"{path} still changes"
            elif path.suffix == ".ipynb":
                old, new, _ = codemod_api._notebook(path, targets)
                assert new == old, f"{path} still changes"

    # the new alias bound only inside another function does not reach the rewritten call
    name = next(n for n, m in targets.items() if m == "gridx.bng")
    src = (
        "from databricks.labs.gbx.pygx import functions as gx\n\n\n"
        "def other():\n"
        "    from databricks.labs.gbx.gridx.bng import functions as bx\n\n"
        "    return bx\n\n\n"
        f"def test_it():\n    return gx.{name}(1)\n"
    )
    new, issues = codemod_api.rewrite(src, targets)
    assert "\nfrom databricks.labs.gbx.gridx.bng import functions as bx\n" in new
    assert codemod_api._undefined(new, {"bx"}) == [] and not issues
    assert codemod_api.rewrite(new, targets)[0] == new

    # fault 1: the import goes after a parenthesized import, never inside it
    src = (
        "from databricks.labs.gbx.pygx import functions as gx\n"
        "from databricks.labs.gbx.util import (\n    a,\n    b,\n)\n\n\n"
        f"def test_it():\n    return gx.{name}(1)\n"
    )
    new, issues = codemod_api.rewrite(src, targets)
    compile(new, "fixture", "exec")
    assert "    b,\n)\nfrom databricks.labs.gbx.gridx.bng import functions as bx\n" in new and not issues

    # fault 2: an alias bound only inside a function (parenthesized import too) is rewritten
    src = (
        "def test_it():\n    from databricks.labs.gbx.pygx import (\n        functions as gx,\n    )\n"
        f"    return gx.{name}(1)\n"
    )
    new, issues = codemod_api.rewrite(src, targets)
    assert f"bx.{name}(1)" in new and not issues
    assert codemod_api._undefined(new, {"bx"}) == []

    # fault 3: a %run target's binding counts; an unreadable target is reported
    import json
    import tempfile

    cells = [{"cell_type": "code", "source": ["%run ./cfg_nb"]}, {"cell_type": "code", "source": [f"gx.{name}(1)\n"]}]
    nb = json.dumps({"cells": cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5}, indent=1) + "\n"
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "nb.ipynb").write_text(nb)
        old, new, issues = codemod_api._notebook(Path(d) / "nb.ipynb", targets)
        assert new == old and any("cfg_nb cannot be read" in i for i in issues)
        (Path(d) / "cfg_nb.py").write_text("from databricks.labs.gbx.pygx import functions as gx\n")
        old, new, issues = codemod_api._notebook(Path(d) / "nb.ipynb", targets)
        assert f"bx.{name}(1)" in new and not issues

    # a rewrite that would not compile is reported, not written
    assert codemod_api._compile_issue("x = (\n") and not codemod_api._compile_issue("%run x\ny = 1\n")


def test_the_modules_pass_moves_paths_names_and_helpers_and_is_idempotent():
    targets = dict(codemod_api.HELPERS)
    src = (
        "from databricks.labs.gbx.pyrx import functions as rx\n"
        "from databricks.labs.gbx.pyrx import mvs, functions as prx2\n"
        "from databricks.labs.gbx.pyrx import Manifest, configure_gdal_env\n"
        "from databricks.labs.gbx.pyvx.file_read import vector_file_read\n"
        "from databricks.labs.gbx.pyrx.functions import _uf_clip, rst_clip_grouped\n"
        'PATCH = "databricks.labs.gbx.pyrx.ortho.some_name"\n\n\n'
        "def test_it():\n    return rx.bin_points_tiled(1)\n"
    )
    new, issues = codemod_api._apply(src, targets, "", True)
    assert not issues, issues
    assert "from databricks.labs.gbx.rasterx import mvs, functions as prx2\n" not in new  # functions stays
    assert "from databricks.labs.gbx.pyrx import functions as prx2\n" in new
    assert "from databricks.labs.gbx.rasterx import mvs\n" in new
    assert "from databricks.labs.gbx.pyrx import configure_gdal_env\n" in new
    assert "from databricks.labs.gbx.rasterx.checkpoint import Manifest\n" in new
    assert "from databricks.labs.gbx.vectorx.file_read import vector_file_read\n" in new
    assert "from databricks.labs.gbx.pyrx.functions import _uf_clip\n" in new
    assert "from databricks.labs.gbx.rasterx.helpers import rst_clip_grouped\n" in new
    assert 'PATCH = "databricks.labs.gbx.rasterx.ortho.some_name"' in new
    assert "rx_helpers.bin_points_tiled(1)" in new
    assert "from databricks.labs.gbx.rasterx import helpers as rx_helpers\n" in new
    assert codemod_api._apply(new, targets, "", True)[0] == new

    # a moved name reached through the package alias is left for a hand edit
    _, issues = codemod_api.rewrite_modules("from databricks.labs.gbx import pyrx as p\n\nx = p.Manifest('a')\n")
    assert any("p.Manifest" in i for i in issues)
