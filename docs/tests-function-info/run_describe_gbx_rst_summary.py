#!/usr/bin/env python3
"""
Run DESCRIBE FUNCTION EXTENDED gbx_rst_summary and print the result as a user would see it.

Usage (from repo root, inside Docker):
  python3 docs/tests-function-info/run_describe_gbx_rst_summary.py

This shows that the result is a table (one column, multiple rows), not raw JSON/dicts.
The test suite prints row.asDict() for machine-readable assertions; real users see .show().
"""

import os
import sys
from pathlib import Path

# Project root: this script is in docs/tests-function-info/
SCRIPT_DIR = Path(__file__).resolve().parent
DOCS_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = DOCS_DIR.parent
GEOBRIX_JAR = PROJECT_ROOT / "target" / "geobrix-0.2.0-jar-with-dependencies.jar"


def main():
    if not GEOBRIX_JAR.exists():
        print(f"GeoBrix JAR not found at {GEOBRIX_JAR}", file=sys.stderr)
        print("Run 'mvn package' first (e.g. in Docker).", file=sys.stderr)
        sys.exit(1)

    if "JAVA_TOOL_OPTIONS" in os.environ:
        del os.environ["JAVA_TOOL_OPTIONS"]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("DESCRIBE gbx_rst_summary") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.executor.extraClassPath", str(GEOBRIX_JAR)) \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=/usr/local/lib") \
        .getOrCreate()

    # Register GeoBrix so gbx_rst_summary exists
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)

    name = "gbx_rst_summary"
    print("=" * 60)
    print(f"DESCRIBE FUNCTION EXTENDED {name}")
    print("=" * 60)
    print()
    print("How Spark displays it (table with one column):")
    print()
    df = spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}")
    df.show(truncate=False, vertical=False)
    print()
    print("Same content as plain lines (column 'function_desc'):")
    print("-" * 60)
    col_name = df.columns[0]
    for row in df.collect():
        print(row[col_name])
    print("-" * 60)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
