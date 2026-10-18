"""Structural checks for the silver LiDAR step.

``wc_lidar_pts`` is a streaming table over the ``laz_inventory`` stream, so each
update reads only the newly inventoried tiles; the surface steps stay
materialized views that read it as a batch. The README tells users which
datasets to reset after a change, so every dataset its refresh commands name
must be one this pipeline defines.

The transformations import ``pyspark.pipelines`` and GeoBrix inside a pipeline,
so these tests read the source (AST) instead of importing it.
"""

import ast
import pathlib
import re

_LAKEFLOW = pathlib.Path(__file__).parents[1]
_TRANSFORMATIONS = _LAKEFLOW / "transformations"


def _functions(path):
    tree = ast.parse(path.read_text())
    return {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}


def _dataset_decorator(fn):
    """(decorator name, dataset name) of a ``@dp.table`` / ``@dp.materialized_view``."""
    for dec in fn.decorator_list:
        if (
            isinstance(dec, ast.Call)
            and isinstance(dec.func, ast.Attribute)
            and dec.func.attr in ("table", "materialized_view")
        ):
            names = [k.value.value for k in dec.keywords if k.arg == "name"]
            return dec.func.attr, (names[0] if names else fn.name)
    return None


def _table_reads(fn):
    """[(reader, table)] for each ``spark.read.table(...)`` / ``spark.readStream.table(...)``."""
    reads = []
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr in ("read", "readStream")
        ):
            reads.append((node.func.value.attr, node.args[0].value))
    return reads


def _pipeline_datasets():
    names = set()
    for path in _TRANSFORMATIONS.glob("*.py"):
        for fn in _functions(path).values():
            found = _dataset_decorator(fn)
            if found:
                names.add(found[1])
    return names


def test_wc_lidar_pts_is_a_streaming_table_over_the_inventory_stream():
    fns = _functions(_TRANSFORMATIONS / "silver_surfaces.py")
    pts = fns["wc_lidar_pts"]
    assert _dataset_decorator(pts) == ("table", "wc_lidar_pts")
    assert _table_reads(pts) == [("readStream", "laz_inventory")]

    (call,) = [
        n
        for n in ast.walk(pts)
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "read_paths"
    ]
    kwargs = {k.arg: k.value for k in call.keywords}
    assert "schema_path" in kwargs  # read_paths needs it for a stream
    assert kwargs["on_error"].value == "skip"
    assert "num_partitions" in kwargs

    # The surface steps recompute from the whole table on each update.
    for name in ("wc_surface_dsm", "wc_surface_dtm"):
        assert _dataset_decorator(fns[name]) == ("materialized_view", name)
        assert _table_reads(fns[name]) == [("read", "wc_lidar_pts")]


def test_readme_refresh_commands_name_pipeline_datasets():
    readme = (_LAKEFLOW / "README.md").read_text()
    commands = re.findall(r"--(?:full-)?refresh ([\w,]+)", readme)
    assert any("wc_lidar_pts" in c for c in commands)
    named = {name for c in commands for name in c.split(",")}
    assert named <= _pipeline_datasets(), named - _pipeline_datasets()
