"""Helpers for function-info tests: run DESCRIBE and capture output."""

from typing import List, Tuple


def describe_function(spark, name: str) -> Tuple[str, str]:
    """
    Run DESCRIBE FUNCTION and DESCRIBE FUNCTION EXTENDED for a function name.
    Returns (describe_brief, describe_extended) as strings. Each string is one
    line per row (the function_desc column value), so test output mimics how
    Spark displays the result to users.
    """
    brief = []
    extended = []
    try:
        for row in spark.sql(f"DESCRIBE FUNCTION {name}").collect():
            brief.append(row[0])  # single column: the description line
    except Exception as e:
        brief.append(f"ERROR: {e}")
    try:
        for row in spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").collect():
            extended.append(row[0])  # single column: the description line
    except Exception as e:
        extended.append(f"ERROR: {e}")
    return ("\n".join(brief), "\n".join(extended))


def render_describe_output(name: str, brief: str, extended: str) -> str:
    """Format DESCRIBE output for printing in tests."""
    return (
        f"\n{'='*60}\n"
        f"DESCRIBE FUNCTION {name}\n"
        f"{'='*60}\n{brief}\n\n"
        f"{'='*60}\n"
        f"DESCRIBE FUNCTION EXTENDED {name}\n"
        f"{'='*60}\n{extended}\n"
    )
