"""
SQL examples for the PMTiles UDAF.

The PMTiles DataSource writer (`df.write.format("pmtiles").mode("overwrite").save(path)`)
is documented in `docs/docs/packages/pmtiles.mdx` — it is not a SQL function and
therefore has no `*_sql_example()` entry here.

These examples are exercised by `test_pmtiles_functions_sql.py` so they stay
green against the live `gbx_pmtiles_agg` UDAF.
"""


def pmtiles_agg_sql_example():
    """Aggregate a column of tile bytes into a single PMTile binary blob."""
    return """
-- Build a 9-tile PMTile pyramid from an existing `tiles_z2(z, x, y, bytes)` table.
-- The result column `pmt` is a BINARY blob containing the full PMTile v3 archive.
SELECT gbx_pmtiles_agg(bytes, z, x, y, '{"name":"my_tileset"}') AS pmt
FROM tiles_z2;
"""


def pmtiles_agg_4arg_sql_example():
    """Aggregate without metadata — metadata defaults to '{}'."""
    return """
-- 4-arg form: metadata defaults to '{}'. Result is still a valid PMTile v3 blob.
SELECT gbx_pmtiles_agg(bytes, z, x, y) AS pmt
FROM tiles_z2;
"""


# =============================================================================
# EXAMPLE OUTPUT (show-type result for docs, same style as the other API pages)
# =============================================================================

pmtiles_agg_sql_example_output = """
+-----------------------------------------------------+
|pmt                                                  |
+-----------------------------------------------------+
|[50 4D 54 69 6C 65 73 03 7F 00 00 00 00 00 00 00 ...]|
+-----------------------------------------------------+
(BINARY: PMTiles v3 archive. Starts with the magic bytes "PMTiles" and version byte 3; holds the 9 tiles at zoom 2; cut here)
"""
