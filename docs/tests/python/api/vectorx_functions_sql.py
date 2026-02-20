"""
SQL examples for VectorX functions documentation.

Used by the function-info generator and by docs via CodeFromTest.
"""


def st_legacyaswkb_sql_example():
    """Convert legacy Mosaic geometry to WKB (SQL). Requires table with geom_legacy column."""
    return """
SELECT gbx_st_legacyaswkb(geom_legacy) AS wkb FROM legacy_table;
"""
