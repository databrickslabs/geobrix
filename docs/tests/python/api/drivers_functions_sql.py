"""
SQL example for the driver listing function.

Single source of truth for the SQL tab of ``### drivers`` in docs/docs/common-functions.mdx. The example is run by
``test_drivers_functions_sql.py`` against the live ``gbx_drivers`` table function.
"""


def drivers_sql_example():
    """List the vector drivers that GeoBrix allows and that can write."""
    return """
-- One row per vector driver of this runtime, filtered to the ones that can write.
SELECT driver, description, extensions
FROM gbx_drivers('vector')
WHERE write AND allowed AND available
ORDER BY driver;
"""


drivers_sql_example_output = """
+--------------+----------------------------+------------------------+
|driver        |description                 |extensions              |
+--------------+----------------------------+------------------------+
|BAG           |Bathymetry Attributed Grid  |[bag]                   |
|CSV           |Comma Separated Value (.csv)|[csv, tsv, psv]         |
|DGN           |Microstation DGN            |[dgn]                   |
|DXF           |AutoCAD DXF                 |[dxf]                   |
|ESRI Shapefile|ESRI Shapefile              |[shp, dbf, shz, shp.zip]|
+--------------+----------------------------+------------------------+
only showing top 5 rows of 38
"""
