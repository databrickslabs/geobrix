"""gbx_pmtiles_agg vector merge (POLYGON multi-feature).

Packs two POLYGON single-feature MVT blobs for the same (z,x,y). Decodes the packed
tile from the archive and asserts both features are present with their geometry
type and property values intact. (The function has no JVM level: it measured slower
than the Python one on dedicated compute.)

POLYGON is mandatory — points-only gives a false pass per the MVT tile-local
contract (see CLAUDE.md).
"""

import mapbox_vector_tile as mvt
from shapely.geometry import Polygon

from databricks.labs.gbx.pmtiles._agg_light import _assemble_archive

# Two distinct tile-local polygons (in [0, 4096] pixel space), different ids.
_EXTENT = 4096
_POLY_A = Polygon([(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)])
_POLY_B = Polygon([(300, 300), (400, 300), (400, 400), (300, 400), (300, 300)])


def _make_mvt_blob(poly: Polygon, prop_id: int, layer: str = "bldg") -> bytes:
    return mvt.encode(
        {
            "name": layer,
            "features": [{"geometry": poly, "properties": {"id": prop_id}}],
        },
        default_options={"extents": _EXTENT, "y_coord_down": True},
    )


_BLOB_A = _make_mvt_blob(_POLY_A, prop_id=1)
_BLOB_B = _make_mvt_blob(_POLY_B, prop_id=2)

_Z, _X, _Y = 3, 2, 4


def _read_tile_from_archive(archive: bytes, z: int, x: int, y: int) -> bytes:
    """Read and return tile (z, x, y) from a PMTiles archive blob (gzip or
    uncompressed directories)."""
    from databricks.labs.gbx.pmtiles._directory import iter_tiles

    tile = dict(iter_tiles(archive)).get((z, x, y))
    assert tile is not None, f"tile ({z},{x},{y}) missing from archive"
    return tile


def _decode_features(tile: bytes) -> dict:
    """Return {id: geometry_type} for all features in the 'bldg' layer."""
    decoded = mvt.decode(tile)
    assert "bldg" in decoded, f"layer 'bldg' missing; layers: {list(decoded.keys())}"
    return {
        f["properties"]["id"]: f["geometry"]["type"]
        for f in decoded["bldg"]["features"]
    }


# ── Light tier (Spark-free) ─────────────────────────────────────────────────


def test_light_vector_merge_parity_polygon(tmp_path):
    """Light tier: two POLYGON blobs for same (z,x,y) → both in merged tile."""
    archive = _assemble_archive([_BLOB_A, _BLOB_B], [_Z, _Z], [_X, _X], [_Y, _Y], {})
    assert archive is not None
    tile = _read_tile_from_archive(archive, _Z, _X, _Y)
    feats = _decode_features(tile)
    assert set(feats.keys()) == {
        1,
        2,
    }, f"light: expected ids {{1,2}}; got {set(feats.keys())}"
    assert feats[1] == "Polygon", f"light: id=1 not Polygon: {feats[1]}"
    assert feats[2] == "Polygon", f"light: id=2 not Polygon: {feats[2]}"
