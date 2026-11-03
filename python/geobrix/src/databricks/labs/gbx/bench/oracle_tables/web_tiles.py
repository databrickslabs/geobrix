"""Oracle table for the web-tile functions (`gbx_rst_tilexyz`, `gbx_rst_xyzpyramid`, `gbx_pmtiles_agg`): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions and the slippy-map tile scheme, never from the function under test.
`tile_4326` is 64 x 48 pixels of 0.01 degrees from (-1.0, 52.0), so it spans longitude -1.0..-0.36 and latitude
51.52..52.0. A zoom-11 tile (x 1020, y 678, spanning longitude -0.703..-0.527 and latitude 51.727..51.836) lies wholly inside
it, so its image is opaque; at zoom 10 the tile x 510, y 339 (longitude -0.703..-0.352) sticks out east of the raster, so
its image is part transparent; tile 0/0 of zoom 10 is far away. The tiles of a pyramid are counted with the standard
longitude/latitude to tile formula, written out below. A PMTiles archive is read with the pmtiles library's header decoder
and the spec's directory layout."""

import math

from databricks.labs.gbx.bench.oracles import Build, Case, _gtiff, _tile, builder

TXYZ, PYR, PMA = "gbx_rst_tilexyz", "gbx_rst_xyzpyramid", "gbx_pmtiles_agg"
TILE, VIRT, ERR, CORRUPT = (
    Build("tile_4326"),
    Build("tile_virtual"),
    Build("tile_error"),
    Build("tile_corrupt"),
)
ONLY = ("session",)
PNG_256 = {"driver": "PNG", "width": 256, "height": 256, "count": 4}
BOUNDS = (-1.0, 51.52, -0.36, 52.0)  # tile_4326: west, south, east, north


@builder("tile_flat_40")
def _tile_flat_40(tmpdir):
    """`tile_4326`'s grid and extent, every pixel 40 (no NoData): a rescale of it has one known value."""
    import numpy as np

    return _tile(
        _gtiff(
            np.full((1, 48, 64), 40.0),
            "EPSG:4326",
            nodata=None,
            origin=(-1.0, 52.0),
            px=0.01,
        )
    )


def _tile_of(lon, lat, z):
    """The slippy-map (x, y) of a longitude/latitude at zoom z."""
    n = 2**z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def _pyramid(min_z, max_z):
    west, south, east, north = BOUNDS
    out = []
    for z in range(min_z, max_z + 1):
        x0, y0 = _tile_of(west, north, z)
        x1, y1 = _tile_of(east - 1e-9, south + 1e-9, z)
        out += [(z, x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]
    return out


def _payload(magic: bytes, n: int) -> bytes:
    return magic + b"tile-%d" % n


PNG, JPEG = b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff"
TILES = (
    (_payload(PNG, 0), 0, 0, 0),
    (_payload(PNG, 1), 1, 0, 0),
    (_payload(PNG, 2), 1, 1, 1),
)
WEB_TILES = (
    # --- tilexyz: a web tile of the raster, as image bytes
    Case(
        "tilexyz_covered",
        TXYZ,
        (TILE, 11, 1020, 678),
        "image_meta",
        {**PNG_256, "alpha": "opaque"},
        ONLY,
    ),
    Case(
        "tilexyz_virtual",
        TXYZ,
        (VIRT, 11, 1020, 678),
        "image_meta",
        {**PNG_256, "alpha": "opaque"},
        ONLY,
    ),
    Case(
        "tilexyz_partial",
        TXYZ,
        (TILE, 10, 510, 339),
        "image_meta",
        {**PNG_256, "alpha": "mixed"},
        ONLY,
    ),
    Case(
        "tilexyz_out_of_extent",
        TXYZ,
        (TILE, 10, 0, 0),
        "image_meta",
        {**PNG_256, "alpha": "transparent"},
        ONLY,
    ),
    Case(
        "tilexyz_jpeg",
        TXYZ,
        (TILE, 11, 1020, 678, "JPEG"),
        "image_meta",
        {"driver": "JPEG", "width": 256, "height": 256, "count": 3},
        ONLY,
    ),
    Case(
        "tilexyz_size",
        TXYZ,
        (TILE, 11, 1020, 678, "PNG", 64),
        "image_meta",
        {"driver": "PNG", "width": 64, "height": 64, "count": 4, "alpha": "opaque"},
        ONLY,
    ),
    # a NULL tile, an error tile and a tile that cannot be read give a transparent tile, never NULL (a slippy-map
    # server needs a body); a NULL coordinate is NULL
    *(
        Case(
            f"tilexyz_{name}",
            TXYZ,
            (tile, 11, 1020, 678),
            "image_meta",
            {**PNG_256, "alpha": "transparent"},
            ONLY,
        )
        for name, tile in (
            ("null_tile", None),
            ("error_tile", ERR),
            ("corrupt_tile", CORRUPT),
        )
    ),
    Case("tilexyz_null_coordinate", TXYZ, (TILE, None, 1020, 678), "null", None, ONLY),
    # a rescale between two numbers maps them to 0 and 255: 40 of 0..100 is 102 (a pixel of 40 everywhere, so the
    # warp cannot change it); a pair with min above max raises and names it
    Case(
        "tilexyz_rescale_pair",
        TXYZ,
        (Build("tile_flat_40"), 11, 1020, 678, "PNG", 256, "bilinear", "0,100"),
        "image_meta",
        {**PNG_256, "alpha": "opaque", "gray": (101, 103)},
        ONLY,
    ),
    Case(
        "tilexyz_rescale_reversed",
        TXYZ,
        (TILE, 11, 1020, 678, "PNG", 256, "bilinear", "100,0"),
        "raises",
        "100.0",
        ONLY,
    ),
    # a bad argument raises and names the value (H13)
    Case(
        "tilexyz_bad_format",
        TXYZ,
        (TILE, 11, 1020, 678, "TIFF"),
        "raises",
        "TIFF",
        ONLY,
    ),
    Case(
        "tilexyz_bad_size",
        TXYZ,
        (TILE, 11, 1020, 678, "PNG", 5000),
        "raises",
        "5000",
        ONLY,
    ),
    # --- xyzpyramid: every tile the raster touches, once
    Case(
        "xyzpyramid_z8_9",
        PYR,
        (TILE, 8, 9),
        "xyz_tiles",
        {"tiles": _pyramid(8, 9), "size": 256},
        ONLY,
    ),
    Case(
        "xyzpyramid_size",
        PYR,
        (TILE, 8, 8, "PNG", 32),
        "xyz_tiles",
        {"tiles": _pyramid(8, 8), "size": 32},
        ONLY,
    ),
    *(
        Case(f"xyzpyramid_{name}", PYR, (tile, 8, 9), "no_rows", None, ONLY)
        for name, tile in (
            ("null_tile", None),
            ("error_tile", ERR),
            ("corrupt_tile", CORRUPT),
        )
    ),
    Case("xyzpyramid_bad_zoom_range", PYR, (TILE, 9, 8), "raises", "max_z", ONLY),
    Case("xyzpyramid_bad_format", PYR, (TILE, 8, 9, "TIFF"), "raises", "TIFF", ONLY),
    # --- pmtiles_agg: one archive per group
    Case(
        "pmtiles_agg_png",
        PMA,
        (TILES, '{"name":"t"}'),
        "pmtiles_archive",
        {
            "tile_type": "png",
            "metadata": {"name": "t"},
            "tiles": {(z, x, y): b for b, z, x, y in TILES},
        },
        ONLY,
    ),
    Case(
        "pmtiles_agg_no_metadata",
        PMA,
        (TILES,),
        "pmtiles_archive",
        {
            "tile_type": "png",
            "metadata": {},
            "tiles": {(z, x, y): b for b, z, x, y in TILES},
        },
        ONLY,
    ),
    Case(
        "pmtiles_agg_jpeg",
        PMA,
        (((_payload(JPEG, 0), 0, 0, 0),), "{}"),
        "pmtiles_archive",
        {"tile_type": "jpeg", "metadata": {}, "tiles": {(0, 0, 0): _payload(JPEG, 0)}},
        ONLY,
    ),
    # a NULL payload is skipped; a group with no payload is NULL
    Case(
        "pmtiles_agg_null_payload",
        PMA,
        (((TILES[0]), (None, 1, 0, 0)), "{}"),
        "pmtiles_archive",
        {"tile_type": "png", "metadata": {}, "tiles": {(0, 0, 0): TILES[0][0]}},
        ONLY,
    ),
    Case(
        "pmtiles_agg_empty",
        PMA,
        (((None, 0, 0, 0), (None, 1, 0, 0)), "{}"),
        "null",
        None,
        ONLY,
    ),
)
