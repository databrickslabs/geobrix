"""Unit tests for ``pyrx/__init__.py`` and the lazy ``tile_range`` of ``rasterx.helpers``.

Three test groups:

1. **The new home resolves, the old one raises** — ``tile_range`` and ``rst_viewshed_towers`` import from
   ``rasterx.helpers`` (``tile_range`` loads on first use and returns the right tuple); on ``pyrx`` they
   raise with a ``Fix:`` that names the new module, for an attribute, a ``from`` import and a dotted import.

2. **Unknown attr raises** — accessing an undeclared attribute raises
   ``AttributeError`` whose message names the attribute.

3. **No-rasterio-at-import guard** — a fresh subprocess imports
   ``databricks.labs.gbx.pyrx`` together with the pure ``virtual_tile`` schema
   submodule and asserts that ``rasterio`` is NOT present in ``sys.modules``.
   This locks the heavy-tier contract: collecting pyrx UDFs must not pull
   rasterio (which is absent in the heavy-tier build environment).
"""

import subprocess
import sys

import pytest

import databricks.labs.gbx.pyrx as pyrx

# ---------------------------------------------------------------------------
# 1. Lazy symbols resolve
# ---------------------------------------------------------------------------


def test_tile_range_importable_from_package():
    from databricks.labs.gbx.rasterx.helpers import tile_range

    assert callable(tile_range)


def test_rst_viewshed_towers_importable_from_package():
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    assert callable(rst_viewshed_towers)


def test_tile_range_via_attribute():
    from databricks.labs.gbx.rasterx import helpers

    assert callable(helpers.tile_range)


@pytest.mark.parametrize(
    "name, home",
    [
        ("tile_range", "rasterx.helpers.tile_range"),
        ("rst_viewshed_towers", "rasterx.helpers.rst_viewshed_towers"),
        ("Manifest", "rasterx.checkpoint.Manifest"),
        ("dense_fuse", "rasterx.mvs.dense_fuse"),
        ("mvs", "rasterx.mvs"),
        ("helpers", "rasterx.helpers"),
    ],
)
def test_a_moved_name_on_pyrx_raises_with_the_fix(name, home):
    with pytest.raises(AttributeError, match=rf"{name} moved to .*{home}.*Fix: from "):
        getattr(pyrx, name)
    with pytest.raises(ImportError, match=r"Fix: from databricks\.labs\.gbx\.rasterx"):
        exec(f"from databricks.labs.gbx.pyrx import {name}")


def test_a_moved_module_cannot_be_imported_from_its_old_path():
    with pytest.raises(
        ImportError, match=r"moved to databricks\.labs\.gbx\.rasterx\.mvs.*Fix: from "
    ):
        exec("import databricks.labs.gbx.pyrx.mvs")
    with pytest.raises(
        ImportError, match=r"Fix: from databricks\.labs\.gbx\.vectorx import file_read"
    ):
        exec("from databricks.labs.gbx.pyvx.file_read import vector_file_read")
    with pytest.raises(
        ImportError,
        match=r"Fix: from databricks\.labs\.gbx\.gridx\.helpers import h3_los_visible",
    ):
        exec("from databricks.labs.gbx.pygx import h3_los_visible")


def test_tile_range_returns_correct_tuple():
    # origin 0, tile 1000 m; coord at 5000 (tile-5 start), radius 1500 →
    # window [3500, 6500] → tiles 3..6
    from databricks.labs.gbx.rasterx.helpers import tile_range

    assert tile_range(5000.0, 0.0, 1500.0, 1000.0) == (3, 6)


# ---------------------------------------------------------------------------
# 2. Unknown attr raises AttributeError with the attr name in the message
# ---------------------------------------------------------------------------


def test_unknown_attr_raises_attribute_error():
    with pytest.raises(AttributeError):
        _ = pyrx.does_not_exist_xyz


def test_unknown_attr_message_names_attribute():
    with pytest.raises(AttributeError, match="does_not_exist_xyz"):
        _ = pyrx.does_not_exist_xyz


# ---------------------------------------------------------------------------
# 3. Regression guard: import pyrx + virtual_tile pulls NO rasterio
# ---------------------------------------------------------------------------


def test_importing_pyrx_and_virtual_tile_does_not_import_rasterio():
    """Fresh-interpreter check: bare pyrx import must not transitively load rasterio.

    Runs in a subprocess so other tests that already imported rasterio cannot
    mask a regression.  Importing ``pyrx.core.virtual_tile`` exercises the
    pure schema path — the module that triggers heavy-tier collection.
    """
    code = (
        "import sys; "
        "import databricks.labs.gbx.pyrx; "
        "import databricks.labs.gbx.pyrx.core.virtual_tile; "
        "import databricks.labs.gbx.rasterx.helpers; "
        "assert 'rasterio' not in sys.modules, "
        "sorted(m for m in sys.modules if 'rasterio' in m)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
