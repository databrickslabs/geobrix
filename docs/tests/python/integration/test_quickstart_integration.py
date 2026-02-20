"""
Integration tests for Quick Start examples that require DBR or integration env.

These tests are physically in integration/ and marked @pytest.mark.integration.
They require Databricks Runtime (CREATE VIEW from shapefile_ogr, st_geomfromwkb)
or data at /data/ paths. Run with -m integration or --include-integration.

Source examples: docs/tests/python/quickstart/examples.py
"""

import importlib.util
import pytest
from pathlib import Path


def _load_quickstart_examples():
    """Load quickstart examples module (same pattern as quickstart/test_examples.py)."""
    quickstart_dir = Path(__file__).resolve().parent.parent / "quickstart"
    spec = importlib.util.spec_from_file_location("examples", quickstart_dir / "examples.py")
    examples = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(examples)
    return examples


def _strip_sql_comment_lines(stmt):
    """Drop leading comment and empty lines so CREATE VIEW isn't skipped when snippet starts with --."""
    lines = stmt.strip().splitlines()
    while lines and (lines[0].strip().startswith("--") or not lines[0].strip()):
        lines.pop(0)
    return "\n".join(lines).strip()


@pytest.mark.integration
def test_exec_sql_read_and_use_snippet(spark):
    """Quick-start SQL_READ_AND_USE: create view from shapefile, then SELECT. DBR-only (CREATE VIEW from shapefile_ogr)."""
    examples = _load_quickstart_examples()
    for stmt in examples.SQL_READ_AND_USE.strip().split(";"):
        stmt = _strip_sql_comment_lines(stmt)
        if not stmt:
            continue
        spark.sql(stmt).show()


@pytest.mark.integration
def test_convert_to_databricks_geometry_with_nyc_data(spark, sample_nyc_subway_shp):
    """Convert GeoBrix WKB to Databricks GEOMETRY. Requires DBR spatial functions (st_geomfromwkb, etc.)."""
    examples = _load_quickstart_examples()
    result_df = examples.convert_to_databricks_geometry(spark, sample_nyc_subway_shp)

    assert result_df is not None
    columns = result_df.columns
    assert "geometry" in columns
    assert "area" in columns
    assert "length" in columns

    rows = result_df.collect()
    assert len(rows) > 0
    first_row = rows[0]
    assert first_row["geometry"] is not None
    assert first_row["area"] is not None
    assert first_row["length"] is not None
    assert first_row["area"] > 0
    assert first_row["length"] > 0


@pytest.mark.integration
def test_pattern_read_process_convert_executes(spark, sample_nyc_subway_shp):
    """Read-process-convert pattern. Requires DBR (VectorX + spatial functions)."""
    examples = _load_quickstart_examples()
    result_df = examples.pattern_read_process_convert(spark, sample_nyc_subway_shp)

    assert result_df is not None
    assert "geometry" in result_df.columns
    assert result_df.count() > 0


@pytest.mark.integration
def test_pattern_multi_format_reading_structure(spark):
    """Multi-format reading pattern. Requires data at /data/ paths (integration env)."""
    examples = _load_quickstart_examples()
    result = examples.pattern_multi_format_reading(spark)

    assert isinstance(result, dict)
    assert "geotiffs" in result
    assert "shapefiles" in result
    assert "geojson" in result
    assert "geopackage" in result
