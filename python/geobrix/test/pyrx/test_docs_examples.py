"""Executable tests for the pyrx documentation examples.

Loads docs/tests/python/api/pyrx_functions.py by file path (importlib) so this
test file can live in the pyrx test tree and run under gbx:test:pyrx without
needing the docs tree on sys.path.

parents index verification (from python/geobrix/test/pyrx/test_docs_examples.py):
  parents[0] = .../python/geobrix/test/pyrx
  parents[1] = .../python/geobrix/test
  parents[2] = .../python/geobrix
  parents[3] = .../python
  parents[4] = .../geobrix  (repo root)
"""

import importlib.util
from pathlib import Path

_EX = Path(__file__).resolve().parents[4] / "docs/tests/python/api/pyrx_functions.py"
_spec = importlib.util.spec_from_file_location("pyrx_doc_examples", _EX)
pyrx_doc_examples = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pyrx_doc_examples)


def test_setup_example(spark):
    """pyrx_setup_example loads the samples and creates the four temp views."""
    df = pyrx_doc_examples.pyrx_setup_example(spark)
    assert "tile" in df.columns
    assert df.count() == 1
    # Every example on the page reads from one of these views.
    for view in ("rasters", "multiband_rasters", "dem_rasters", "netcdf_rasters"):
        assert spark.catalog.tableExists(view), f"setup did not create view {view}"


def test_accessors_example(spark):
    """pyrx_accessors_example row matches the synthetic raster (4x3, 2 bands, EPSG:4326)."""
    row = pyrx_doc_examples.pyrx_accessors_example(spark)
    assert row["width"] == 4
    assert row["height"] == 3
    assert row["srid"] == 4326
    assert row["bands"] == 2


def test_transform_example(spark):
    """pyrx_transform_example reprojects to 3857."""
    srid = pyrx_doc_examples.pyrx_transform_example(spark)
    assert srid == 3857


def test_clip_example(spark):
    """pyrx_clip_example returns a tile smaller than the 4x3 original."""
    row = pyrx_doc_examples.pyrx_clip_example(spark)
    assert 0 < row["w"] < 4
    assert 0 < row["h"] < 3


def test_polygonize_example(spark):
    """pyrx_polygonize_example yields at least one polygon with value 5.0."""
    rows = pyrx_doc_examples.pyrx_polygonize_example(spark)
    assert len(rows) >= 1
    assert any(r["value"] == 5.0 for r in rows)


def test_sql_example(spark):
    """pyrx_sql_example queries gbx_rst_* SQL functions and returns width=4, srid=4326."""
    row = pyrx_doc_examples.pyrx_sql_example(spark)
    assert row["width"] == 4
    assert row["srid"] == 4326


def test_output_constants_present():
    """Each example has a matching _output string constant."""
    for name in [
        "pyrx_setup_example_output",
        "pyrx_accessors_example_output",
        "pyrx_transform_example_output",
        "pyrx_clip_example_output",
        "pyrx_polygonize_example_output",
        "pyrx_sql_example_output",
    ]:
        val = getattr(pyrx_doc_examples, name, None)
        assert val is not None, f"Missing constant: {name}"
        assert isinstance(val, str) and val.strip(), f"Empty constant: {name}"
