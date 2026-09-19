"""
Python code examples for the light tier of the PMTiles aggregate function.
Single source of truth for the Python Light tab in docs/docs/api/pmtiles-functions.mdx.

pmtiles_agg is a grouped aggregate available in both tiers.  In the light tier it
runs as a pandas_udf GROUPED_AGG (Serverless-safe); the Python wrapper
``databricks.labs.gbx.pmtiles.functions.pmtiles_agg`` is tier-agnostic.

The light aggregate returns **BINARY** — a raw PMTiles v3 archive blob.
"""

try:
    from databricks.labs.gbx.pmtiles import functions as px
except ImportError:
    px = None


# ---------------------------------------------------------------------------
# pmtiles_agg -- fold (z, x, y, bytes) tile rows into a PMTiles v3 archive
# Fixture: 9 synthetic tiles at zoom level 2 (x in [0,2], y in [0,2])
# Output: BINARY (PMTiles v3 magic bytes b'PMTiles' + version byte 3)
# ---------------------------------------------------------------------------


def pmtiles_agg_python_light_example(spark):
    """Aggregate (z, x, y, bytes) tile rows into a PMTiles v3 BINARY blob using the light pmtiles tier.

    Multi-row fixture: 9 synthetic tiles at zoom level 2 (x in [0,2], y in [0,2]).
    Each tile payload is a short ASCII byte string.  The light aggregate runs as a
    pandas_udf GROUPED_AGG — ``agg(px.pmtiles_agg(...))`` folds all rows into one
    PMTile v3 archive.  Returns BINARY containing the full PMTile v3 archive.
    """
    from databricks.labs.gbx.pmtiles import register_pmtiles_agg  # noqa: PLC0415

    register_pmtiles_agg(spark)

    test_data = [
        (2, x, y, f"tile_{x}_{y}".encode("utf-8"))
        for x in range(3)
        for y in range(3)
    ]
    df = spark.createDataFrame(test_data, ["z", "x", "y", "tile_bytes"])
    result = (
        df.agg(
            px.pmtiles_agg(
                "tile_bytes",
                "z",
                "x",
                "y",
                '{"name":"my_tileset"}',
            ).alias("pmt")
        )
        .first()
    )
    return result["pmt"]


pmtiles_agg_python_light_example_output = """
+------------------------------------------+
|pmt                                       |
+------------------------------------------+
|[50 4D 54 69 6C 65 73 03 ...]             |
+------------------------------------------+
(BINARY: PMTiles v3 archive — starts with magic bytes b'PMTiles' + version byte 3; contains 9 synthetic tiles at zoom 2)
"""
