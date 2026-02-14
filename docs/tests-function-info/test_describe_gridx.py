"""
GridX (BNG): print and assert DESCRIBE FUNCTION / DESCRIBE FUNCTION EXTENDED for every gbx_bng_* function.
"""

import pytest

from helpers import describe_function, render_describe_output

PREFIX = "gbx_bng_"


@pytest.fixture(scope="module")
def gridx_functions(registered_gbx_functions):
    """GridX/BNG function names (gbx_bng_*)."""
    return [f for f in registered_gbx_functions if f.startswith(PREFIX)]


def test_gridx_describe_output_for_each_function(spark, gridx_functions, capsys):
    """
    For each registered GridX function, run DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED,
    print the rendered output, and assert it is non-empty.
    """
    if not gridx_functions:
        pytest.skip("No GridX functions registered (JAR or registration failed)")
    missing = []
    for name in sorted(gridx_functions):
        brief, extended = describe_function(spark, name)
        rendered = render_describe_output(name, brief, extended)
        with capsys.disabled():
            print(rendered)
        if "ERROR:" in brief or "ERROR:" in extended:
            missing.append(name)
        assert brief, f"DESCRIBE FUNCTION {name} produced no output"
        assert extended, f"DESCRIBE FUNCTION EXTENDED {name} produced no output"
    assert not missing, f"DESCRIBE failed for: {missing}"


def test_gridx_each_function_has_non_empty_description(spark, gridx_functions):
    """Each GridX function returns at least one row from DESCRIBE FUNCTION EXTENDED."""
    if not gridx_functions:
        pytest.skip("No GridX functions registered")
    for name in sorted(gridx_functions):
        _, extended = describe_function(spark, name)
        assert "ERROR:" not in extended, f"DESCRIBE EXTENDED failed for {name}"
        assert extended.strip(), f"DESCRIBE EXTENDED {name} was empty"
