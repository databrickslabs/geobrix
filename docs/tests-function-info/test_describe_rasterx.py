"""
RasterX: print and assert DESCRIBE FUNCTION / DESCRIBE FUNCTION EXTENDED for every gbx_rst_* function.
"""

import pytest

from helpers import describe_function, render_describe_output

PREFIX = "gbx_rst_"


@pytest.fixture(scope="module")
def rasterx_functions(registered_gbx_functions):
    """RasterX function names (gbx_rst_*)."""
    return [f for f in registered_gbx_functions if f.startswith(PREFIX)]


@pytest.mark.parametrize("func_name", [], ids=[])
def _placeholder(func_name):
    """Placeholder for parametrize; actual tests are generated per-function."""


def test_rasterx_describe_output_for_each_function(spark, rasterx_functions, capsys):
    """
    For each registered RasterX function, run DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED,
    print the rendered output, and assert it is non-empty.
    """
    if not rasterx_functions:
        pytest.skip("No RasterX functions registered (JAR or registration failed)")
    missing = []
    for name in sorted(rasterx_functions):
        brief, extended = describe_function(spark, name)
        rendered = render_describe_output(name, brief, extended)
        with capsys.disabled():
            print(rendered)
        if "ERROR:" in brief or "ERROR:" in extended:
            missing.append(name)
        assert brief, f"DESCRIBE FUNCTION {name} produced no output"
        assert extended, f"DESCRIBE FUNCTION EXTENDED {name} produced no output"
    assert not missing, f"DESCRIBE failed for: {missing}"


def test_rasterx_each_function_has_non_empty_description(spark, rasterx_functions):
    """Each RasterX function returns at least one row from DESCRIBE FUNCTION EXTENDED."""
    if not rasterx_functions:
        pytest.skip("No RasterX functions registered")
    for name in sorted(rasterx_functions):
        _, extended = describe_function(spark, name)
        assert "ERROR:" not in extended, f"DESCRIBE EXTENDED failed for {name}"
        assert extended.strip(), f"DESCRIBE EXTENDED {name} was empty"
