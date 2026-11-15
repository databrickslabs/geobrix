"""pyrx.ds — pure-Python/PySpark DataSource V2 raster readers + writer.

Light-tier swap-out for the GDAL-backed Scala readers. The manager registers them
(``databricks.labs.gbx.register(spark)``); importing this package registers nothing.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401
