"""Pure-Python regression: the light PMTiles-agg tile merge must NOT flip latitude.

Tiles are written y-down (MVT spec). mvt.decode defaults to y-up, so decoding the
merge without y_coord_down=True and re-encoding with it mirrors every merged tile's
y — features jump as the tile grid changes per zoom (the "tiles moving / latitude
discrepancy" bug). This asserts a same-tile multi-feature merge round-trips the
tile-local y unchanged.
"""

import pytest

mvt = pytest.importorskip("mapbox_vector_tile")

from databricks.labs.gbx.pmtiles._agg_light import _merge_mvt_blobs  # noqa: E402

_EXTENT = 4096


def _enc(feats):
    return mvt.encode(
        {"name": "l", "features": feats},
        default_options={"extents": _EXTENT, "y_coord_down": True},
    )


def test_merge_preserves_ydown_coords():
    b1 = _enc(
        [
            {
                "geometry": {"type": "Point", "coordinates": [1000, 300]},
                "properties": {"id": 1},
            }
        ]
    )
    b2 = _enc(
        [
            {
                "geometry": {"type": "Point", "coordinates": [2000, 600]},
                "properties": {"id": 2},
            }
        ]
    )
    merged = _merge_mvt_blobs([b1, b2])
    feats = mvt.decode(merged, default_options={"y_coord_down": True})["l"]["features"]
    got = {f["properties"]["id"]: tuple(f["geometry"]["coordinates"]) for f in feats}
    # y (300, 600) must survive; a y-flip would yield 4096-y (3796, 3496).
    assert got == {1: (1000, 300), 2: (2000, 600)}, f"merge flipped/moved coords: {got}"
