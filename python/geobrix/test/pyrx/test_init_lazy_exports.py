"""Unit tests for the PEP 562 lazy-export ``__getattr__`` in ``pyrx/__init__.py``.

Three test groups:

1. **Lazy symbols resolve** — ``tile_range`` and ``rst_viewshed_towers`` are
   importable via both ``from … import`` and attribute access, and ``tile_range``
   returns the correct tuple for a known input.

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
    from databricks.labs.gbx.pyrx import tile_range

    assert callable(tile_range)


def test_rst_viewshed_towers_importable_from_package():
    from databricks.labs.gbx.pyrx import rst_viewshed_towers

    assert callable(rst_viewshed_towers)


def test_tile_range_via_attribute():
    assert callable(pyrx.tile_range)


def test_rst_viewshed_towers_via_attribute():
    assert callable(pyrx.rst_viewshed_towers)


def test_tile_range_returns_correct_tuple():
    # origin 0, tile 1000 m; coord at 5000 (tile-5 start), radius 1500 →
    # window [3500, 6500] → tiles 3..6
    from databricks.labs.gbx.pyrx import tile_range

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
        "assert 'rasterio' not in sys.modules, "
        "sorted(m for m in sys.modules if 'rasterio' in m)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
