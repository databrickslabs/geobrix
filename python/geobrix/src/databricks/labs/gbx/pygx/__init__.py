"""pygx — pure-Python/PySpark light GridX tier (Serverless-safe).

Mirrors the heavyweight ``gridx`` functions (``gbx_quadbin_*``, ``gbx_bng_*``)
with no JVM, no JAR, no native GDAL. See databricks.labs.gbx.pygx.functions.
"""

# Pure-Python H3 utilities (non-columnar; importable by notebooks and scripts).
from ._h3 import h3_los_visible, h3_viewshed_towers  # noqa: F401
