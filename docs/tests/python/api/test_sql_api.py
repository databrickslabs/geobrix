"""
Tests for SQL API Reference Examples (docs/docs/api/sql.mdx).

Validates that all snippet constants exist and that registration + SQL execute
so the docs are compile/execute validated per cursor rules.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import sql_api


# Constants that must exist (one per doc block on sql.mdx)
SQL_API_CONSTANTS = [
    "REGISTER_PYTHON",
    "REGISTER_SCALA",
    "SQL_LIST_FUNCTIONS",
    "SQL_DESCRIBE",
    "SQL_READ_AND_QUERY_RASTERS",
    "SQL_FILTER_RASTERS",
    "SQL_RASTER_TRANSFORMATIONS",
    "SQL_BNG_CELL_OPERATIONS",
    "SQL_BNG_POINT_TO_CELL",
    "SQL_BNG_SPATIAL_AGGREGATION",
    "SQL_BNG_MULTI_RESOLUTION",
    "SQL_VECTORX_MIGRATION_WORKFLOW",
    "SQL_VECTORX_SPATIAL_FUNCTIONS",
]

# Output constants for "Example output" blocks (must exist and be non-empty)
SQL_API_OUTPUT_CONSTANTS = [
    "SQL_LIST_FUNCTIONS_output",
    "SQL_DESCRIBE_output",
    "SQL_READ_AND_QUERY_RASTERS_output",
    "SQL_FILTER_RASTERS_output",
    "SQL_RASTER_TRANSFORMATIONS_output",
    "SQL_BNG_CELL_OPERATIONS_output",
    "SQL_BNG_POINT_TO_CELL_output",
    "SQL_BNG_SPATIAL_AGGREGATION_output",
    "SQL_BNG_MULTI_RESOLUTION_output",
    "SQL_VECTORX_MIGRATION_WORKFLOW_output",
    "SQL_VECTORX_SPATIAL_FUNCTIONS_output",
]


class TestSqlApiConstants:
    """Compile-time style validation: all doc constants exist and are non-empty strings."""

    @pytest.mark.parametrize("name", SQL_API_CONSTANTS)
    def test_constant_exists_and_is_string(self, name):
        assert hasattr(sql_api, name), f"sql_api must define {name}"
        val = getattr(sql_api, name)
        assert isinstance(val, str), f"{name} must be a string"
        assert len(val.strip()) > 0, f"{name} must be non-empty"

    @pytest.mark.parametrize("name", SQL_API_OUTPUT_CONSTANTS)
    def test_output_constant_exists_and_is_string(self, name):
        assert hasattr(sql_api, name), f"sql_api must define {name} for Example output"
        val = getattr(sql_api, name)
        assert isinstance(val, str), f"{name} must be a string"
        assert len(val.strip()) > 0, f"{name} must be non-empty"


def test_register_functions_python_callable(spark):
    """Registration helper is callable (structure check)."""
    assert callable(sql_api.register_functions_python)


def test_register_and_show_functions(spark):
    """Register via Python and run SHOW FUNCTIONS (compile + execute validation)."""
    sql_api.register_functions_python(spark)
    # Spark SHOW FUNCTIONS LIKE uses * as wildcard (not %)
    df = spark.sql("SHOW FUNCTIONS LIKE 'gbx_*'")
    names = [row.function for row in df.collect()]
    assert any("gbx_rst_" in n for n in names), "Expected at least one gbx_rst_ function"
    assert any("gbx_bng_" in n for n in names), "Expected at least one gbx_bng_ function"
    assert any("gbx_st_" in n for n in names), "Expected at least one gbx_st_ function"
