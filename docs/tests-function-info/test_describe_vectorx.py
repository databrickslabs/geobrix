"""
VectorX: print and assert DESCRIBE FUNCTION / DESCRIBE FUNCTION EXTENDED for every gbx_st_* function.
"""

import pytest

from helpers import describe_function, render_describe_output

PREFIX = "gbx_st_"


@pytest.fixture(scope="module")
def vectorx_functions(registered_gbx_functions):
    """VectorX function names (gbx_st_*)."""
    return [f for f in registered_gbx_functions if f.startswith(PREFIX)]


def test_vectorx_describe_output_for_each_function(spark, vectorx_functions, capsys):
    """
    For each registered VectorX function, run DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED,
    print the rendered output, and assert it is non-empty.
    """
    if not vectorx_functions:
        pytest.skip("No VectorX functions registered (JAR or registration failed)")
    missing = []
    for name in sorted(vectorx_functions):
        brief, extended = describe_function(spark, name)
        rendered = render_describe_output(name, brief, extended)
        with capsys.disabled():
            print(rendered)
        if "ERROR:" in brief or "ERROR:" in extended:
            missing.append(name)
        assert brief, f"DESCRIBE FUNCTION {name} produced no output"
        assert extended, f"DESCRIBE FUNCTION EXTENDED {name} produced no output"
    assert not missing, f"DESCRIBE failed for: {missing}"


def test_vectorx_each_function_has_non_empty_description(spark, vectorx_functions):
    """Each VectorX function returns at least one row from DESCRIBE FUNCTION EXTENDED."""
    if not vectorx_functions:
        pytest.skip("No VectorX functions registered")
    for name in sorted(vectorx_functions):
        _, extended = describe_function(spark, name)
        assert "ERROR:" not in extended, f"DESCRIBE EXTENDED failed for {name}"
        assert extended.strip(), f"DESCRIBE EXTENDED {name} was empty"
