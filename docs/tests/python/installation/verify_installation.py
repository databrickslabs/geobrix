"""
Installation verification snippets (one-copy for docs/docs/installation.mdx).

Displayed in Installation > Verification. Tested by test_verify_installation.py.
"""

VERIFY_PYTHON_INSTALLATION = """from databricks.labs.gbx.rasterx import functions as rx

# Register functions
rx.register(spark)

# List registered functions
spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_*'").show()"""

VERIFY_SQL_FUNCTIONS = """-- List all GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';

-- Describe a specific function
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;"""

# Example output (displayed under code via CodeFromTest outputConstant)
VERIFY_PYTHON_INSTALLATION_output = """
+--------------------+
|function            |
+--------------------+
|gbx_rst_asformat    |
|gbx_rst_avg         |
|gbx_rst_bandmetadata|
|gbx_rst_boundingbox |
|...                 |
+--------------------+"""

VERIFY_SQL_FUNCTIONS_output = """
+--------------------+
|function            |
+--------------------+
|gbx_rst_asformat    |
|gbx_rst_avg         |
|gbx_rst_boundingbox |
|gbx_bng_cellarea    |
|...                 |
+--------------------+

-DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox
Function: gbx_rst_boundingbox
Type: ...
"""
