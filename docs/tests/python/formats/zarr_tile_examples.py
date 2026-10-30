"""Zarr tile examples -- single source of truth.

Code shown on docs/docs/api/tile-structure.mdx (Zarr tile metadata) is imported from here, and
every function runs for real in test_zarr_tile_examples.py on the sample ERA5-like cube
(``time`` 48 hourly steps x ``latitude`` 181 x ``longitude`` 360, variables ``t2m``, ``u10``,
``v10`` and ``msl``). The page shows each function's body: the ``def`` line, the docstring and the
closing ``return`` are not shown.

Names that stand for what is yours when you run an example:

- ``spark``: the Spark session (a notebook has one);
- ``cube``: the path of a Zarr store, as on the Zarr Stores page.

The ``*_output`` constants are what the examples print. The tests compare them with a real run,
so they cannot drift.
"""

zarr_tile_metadata_output = """
virtual: True | path_mode: None
window: (0, 0, 180, 91)
count = '1'
driver = 'Zarr'
format = 'zarr'
height = '181'
sliceCoord_time = '2023-01-01T00:00:00.000000000'
sliceDims = 'time=0'
width = '360'
zarr_array = 't2m'
zarr_crs = 'EPSG:4326'
zarr_gt = '-0.5,1.0,0.0,90.5,0.0,-1.0'
zarr_table = ''
zarr_version = ''
not shown: sourcePath, zarr_meta, zarr_refs, zarr_root
materialized: True
driver: GTiff | zarr_array: t2m
"""


def zarr_tile_metadata(spark, cube):
    """The metadata of the first tile of one hour of 2 m temperature, then the same tile with its pixels."""
    from databricks.labs.gbx.zarr import gbx_zarr_read

    def first_tile(**options):
        tiles = gbx_zarr_read(
            spark,
            cube,
            variables=["t2m"],
            isel={"time": 0},
            output="tiles",
            **options,
        )
        by_position = tiles.orderBy("tile.window.row_off", "tile.window.col_off")
        return by_position.first()["tile"]

    # A virtual tile holds no pixels, only where to read them.
    tile = first_tile()
    print("virtual:", tile.raster is None, "| path_mode:", tile.path_mode)
    print("window:", tuple(tile.window))
    long_or_local = {"sourcePath", "zarr_meta", "zarr_refs", "zarr_root"}
    for key in sorted(set(tile.metadata) - long_or_local):
        print(key, "=", repr(tile.metadata[key]))
    print("not shown:", ", ".join(sorted(long_or_local)))

    # Read now, the tile holds GeoTIFF bytes and keeps where it came from.
    held = first_tile(virtual_tiles=False)
    meta = held.metadata
    print("materialized:", held.raster is not None)
    print("driver:", meta["driver"], "| zarr_array:", meta["zarr_array"])
    return tile
