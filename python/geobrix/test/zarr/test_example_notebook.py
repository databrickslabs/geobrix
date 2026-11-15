"""The Zarr weather-features example notebook: the pieces that can be checked without a cluster.

The notebook runs on Databricks; these rows pin what its text promises. Its wind-speed block
function keeps a missing cell missing, and its checks fail the run when an invariant breaks
(the notebook runners only fail a notebook on an exception).
"""

import ast
import json
import pathlib

import numpy as np
import pandas as pd
import pytest

NOTEBOOK = (
    pathlib.Path(__file__).resolve().parents[4]
    / "examples/notebooks/zarr-weather-features/01_zarr_weather_features.ipynb"
)

pytestmark = pytest.mark.skipif(
    not NOTEBOOK.exists(), reason="the example notebook is not in this checkout"
)


def _code():
    """The parsed code cells; a cell of notebook magics (``%pip``) is not Python."""
    trees = []
    for c in json.loads(NOTEBOOK.read_text())["cells"]:
        if c["cell_type"] == "code":
            try:
                trees.append(ast.parse("".join(c["source"])))
            except SyntaxError:
                pass
    return trees


def _wind_speed_block():
    """The notebook's block function, without its ``pandas_udf`` decorator."""
    for tree in _code():
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "wind_speed_block":
                node.decorator_list = []
                scope = {"np": np, "pd": pd}
                exec(compile(ast.Module([node], []), str(NOTEBOOK), "exec"), scope)
                return scope["wind_speed_block"]
    raise AssertionError("the notebook has no wind_speed_block")


def _blocks(u, v, dtype="float32"):
    return [np.array(x, dtype).tobytes() for x in (u, v)]


@pytest.mark.parametrize(
    "fill_text, u, v, want",
    [
        ("-9999.0", [3, -9999, 6, 5], [4, 1, -9999, 12], [5, -9999, -9999, 13]),
        ("NaN", [3, np.nan, 6], [4, 1, np.nan], [5, np.nan, np.nan]),
        (None, [3, np.nan], [4, 1], [5, np.nan]),
    ],
    ids=["numeric sentinel", "NaN fill", "no fill"],
)
def test_a_missing_cell_stays_missing_in_the_wind_speed_block(fill_text, u, v, want):
    fn = _wind_speed_block()
    u_bytes, v_bytes = _blocks(u, v)
    (got,) = fn(
        pd.Series([u_bytes]),
        pd.Series([v_bytes]),
        pd.Series(["float32"]),
        pd.Series([fill_text]),
    )
    np.testing.assert_array_equal(np.frombuffer(got, "float32"), np.float32(want))


def test_the_notebook_asserts_the_invariants_its_text_states():
    asserted = {
        ast.unparse(node.test)
        for tree in _code()
        for node in ast.walk(tree)
        if isinstance(node, ast.Assert)
    }
    for invariant in (
        "cube_after == cube_before",
        "largest_difference == 0",
        "differing == 0",
    ):
        assert invariant in asserted, f"the notebook does not assert {invariant!r}"
