"""
Python example for the driver listing function.

Single source of truth for the Python tab of ``### drivers`` in docs/docs/common-functions.mdx.
"""

try:
    from databricks.labs.gbx.drivers import functions as dx
except ImportError:
    dx = None


def drivers_python_example(spark):
    """List the vector drivers that GeoBrix allows and that can write, then check one raster driver.

    ``dx.drivers`` returns a DataFrame with one row per driver of the current environment. Filter it
    like any DataFrame: ``write`` and ``allowed`` are booleans.
    """
    vector_writers = (
        dx.drivers("vector")
        .where("write AND allowed AND available")
        .select("driver", "description", "extensions")
        .orderBy("driver")
    )
    names = [r["driver"] for r in vector_writers.collect()]
    assert "ESRI Shapefile" in names, names
    aaigrid = dx.drivers("raster").where("driver = 'AAIGrid'").first()
    assert aaigrid["read"] and aaigrid["write"] and aaigrid["allowed"], aaigrid
    return vector_writers


drivers_python_example_output = """
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
