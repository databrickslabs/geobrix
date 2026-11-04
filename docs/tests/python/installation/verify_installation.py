"""
Installation verification snippets (one-copy for docs/docs/installation/*.mdx).

Displayed in Installation (register, stage the JAR, install the Unity Catalog functions, verify) and
Databricks Container Service > Verify the install. Tested by test_verify_installation.py.
"""

REGISTER_PYTHON_INSTALLATION = """from databricks.labs import gbx

# The gbx_* SQL functions and the *_gbx readers and writers, once per session
gbx.register(spark)"""

STAGE_JAR_PYTHON = """from databricks.labs import gbx

# Copy the JAR this env wheel carries to a Volume folder (with a .sha256 file beside it)
gbx.stage_jar("/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/jar")
# Then add the printed path as a cluster library on a dedicated cluster and restart the cluster."""

JVM_JOB_PYTHON = """from databricks.labs import gbx
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Register the functions in this session, then run the Scala code on the same session.
gbx.register(spark)
spark._jvm.com.example.geo.JvmJobBody.run(spark._jsparkSession, "main.geobrix.cells")"""

UC_INSTALL_PYTHON = """from databricks.labs import gbx

# Once, from a Serverless environment 6 or DBR 19 notebook.
# The folder holds the env 6 wheel of the same version.
gbx.uc.install("<catalog>", "<schema>", "/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6")"""

VERIFY_PYTHON_INSTALLATION = """from databricks.labs import gbx

# Register functions and the lightweight readers
gbx.register(spark)

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
