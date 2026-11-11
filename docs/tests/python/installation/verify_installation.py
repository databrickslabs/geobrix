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

# Register the functions and the lightweight readers
registered = gbx.register(spark)

# Each name maps to the level it runs at
print(len(registered), "functions registered")
print("gbx_rst_width runs at level", registered["gbx_rst_width"].level)

# Read the signature of one function
spark.sql("DESCRIBE FUNCTION gbx_rst_width").show(truncate=False)"""

VERIFY_SQL_FUNCTIONS = """-- Read the signature of one function
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;"""

# Example output (displayed under code via CodeFromTest outputConstant)
VERIFY_PYTHON_INSTALLATION_output = """
<N> functions registered
gbx_rst_width runs at level L1
+------------------------------------+
|function_desc                       |
+------------------------------------+
|Function: gbx_rst_width             |
|...                                 |
+------------------------------------+"""

VERIFY_SQL_FUNCTIONS_output = """
Function: gbx_rst_boundingbox
Type: ...
"""
