"""pyrx.ds — pure-Python/PySpark DataSource V2 raster readers + writer.

Light-tier swap-out for the GDAL-backed Scala readers.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from databricks.labs.gbx.ds import register  # noqa: E402,F401
from databricks.labs.gbx.ds.register import _try_register_on_import

_try_register_on_import()
