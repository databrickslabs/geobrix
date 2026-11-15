"""
Tests for Python API Reference Examples.

These are structure tests: each example the page shows is a defined, callable function.
"""
import sys
from pathlib import Path

import pytest

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import python_api

EXAMPLES = [
    "verify_installation",
    "import_pattern_rasterx",
    "import_pattern_gridx",
    "import_pattern_vectorx",
    "rst_boundingbox_example",
    "rst_width_example",
    "rst_height_example",
    "rst_numbands_example",
    "rst_metadata_example",
    "rst_srid_example",
    "rasterx_complete_example",
    "bng_cellarea_example",
    "bng_pointtocell_example",
    "gridx_complete_example",
    "st_legacyaswkb_example",
    "dataframe_select_operations",
    "dataframe_filter_operations",
    "dataframe_withcolumn_operations",
    "using_with_sql",
    "type_hints_and_ide_support",
    "error_handling_example",
]


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_is_a_callable(name):
    assert callable(getattr(python_api, name, None)), f"python_api.{name}"
