import pytest

mvt = pytest.importorskip(
    "mapbox_vector_tile",
    reason="mapbox-vector-tile not installed (a light-tier extra (e.g. geobrix[light_env6]) or [test] required)",
)
from shapely import to_wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import _mvt  # noqa: E402


def _decode(blob, layer="layer"):
    tile = mvt.decode(blob)
    return tile[layer]["features"]


def test_encode_layer_preserves_native_attr_types():
    feats = [
        {
            "geometry": to_wkb(Point(10, 20)),
            "properties": {"name": "a", "pop": 42, "h": 3.5, "ok": True},
        },
    ]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert props["name"] == "a"
    assert props["pop"] == 42 and isinstance(props["pop"], int)
    assert props["h"] == 3.5 and isinstance(props["h"], float)
    assert props["ok"] is True


def test_encode_layer_unsupported_type_falls_back_to_string():
    feats = [{"geometry": to_wkb(Point(1, 1)), "properties": {"b": b"\x00\x01"}}]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert isinstance(props["b"], str)  # bytes -> str fallback


def test_encode_layer_accepts_wkt():
    # Geom-input consistency: encode_layer must route through _geom.parse_geom so a WKT
    # (and EWKT) string geom encodes identically to the WKB path.
    feats = [
        {"geometry": "POINT (10 20)", "properties": {"id": 1}},
        {"geometry": "SRID=4326;POINT (30 40)", "properties": {"id": 2}},
    ]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    out = _decode(blob)
    ids = sorted(ff["properties"]["id"] for ff in out)
    assert ids == [1, 2]


def test_pyramid_tiles_accepts_wkt():
    # pyramid_tiles must decode WKT/EWKT via parse_geom, not assume WKB bytes.
    rows = list(
        _mvt.pyramid_tiles("SRID=4326;POINT (0 0)", {"id": 7}, 0, 2, "layer", 4096)
    )
    zs = sorted(r[0] for r in rows)
    assert zs == [0, 1, 2]
    feats = _decode(rows[0][3])
    assert feats[0]["properties"]["id"] == 7


def test_pyramid_tiles_caps_and_schema():
    # A point at lon/lat 0,0 over zooms 0..2 -> one tile per zoom (3 rows).
    rows = list(
        _mvt.pyramid_tiles(to_wkb(Point(0.0, 0.0)), {"id": 7}, 0, 2, "layer", 4096)
    )
    zs = sorted(r[0] for r in rows)
    assert zs == [0, 1, 2]
    for z, x, y, blob in rows:
        assert isinstance(z, int) and isinstance(x, int) and isinstance(y, int)
        assert isinstance(blob, (bytes, bytearray)) and len(blob) > 0
        # Each emitted tile must be a well-formed, decodable MVT proto whose
        # attributes survive end-to-end with native types (id stays an int).
        feats = _decode(blob)
        assert len(feats) == 1
        assert feats[0]["properties"]["id"] == 7 and isinstance(
            feats[0]["properties"]["id"], int
        )


def test_pyramid_rejects_too_many_tiles():
    import pytest

    with pytest.raises(ValueError):
        from shapely.geometry import box

        list(
            _mvt.pyramid_tiles(
                to_wkb(box(-179, -85, 179, 85)), {}, 0, 20, "layer", 4096
            )
        )


def test_pyramid_rejects_negative_min_z():
    # Mirrors the heavy require(minZ >= 0); negative zoom must raise, not emit garbage.
    with pytest.raises(ValueError, match="min_z must be >= 0"):
        list(
            _mvt.pyramid_tiles(to_wkb(Point(0.0, 0.0)), {"id": 1}, -1, 2, "layer", 4096)
        )


def test_pyramid_rejects_inverted_range():
    # Mirrors the heavy require(maxZ >= minZ); inverted range must raise, not yield zero rows.
    with pytest.raises(ValueError, match="max_z .* must be >= min_z"):
        list(
            _mvt.pyramid_tiles(to_wkb(Point(0.0, 0.0)), {"id": 1}, 3, 1, "layer", 4096)
        )


def test_pyramid_buffers_tiles_across_seams():
    """A polygon straddling a tile boundary must be encoded with a buffer: each covered
    tile's geometry overhangs the [0, extent] core (coords < 0 or > extent) so MapLibre
    renders the seam without a hard clip line (the "tiles moving/disappearing on zoom" bug).
    """
    from shapely.geometry import box as _box

    extent = 4096
    # A polygon spanning the z1 antimeridian-free split at lon=0 -> crosses the x=0|x=1
    # tile boundary at z1, so both tiles must carry buffered overhang.
    poly = _box(-10.0, -10.0, 10.0, 10.0)  # straddles lon=0 (z1 x-tile seam)
    tiles = list(_mvt.pyramid_tiles(to_wkb(poly), {"id": 1}, 1, 1, "layer", extent))
    assert tiles, "expected tiles at z1"
    coords = []
    for _z, _x, _y, blob in tiles:
        for f in _decode(blob):

            def _walk(c):
                if c and isinstance(c[0], (int, float)):
                    yield c
                else:
                    for e in c:
                        yield from _walk(e)

            coords.extend(_walk(f["geometry"]["coordinates"]))
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    # buffer overhang: at least one coordinate must fall outside the [0, extent] core.
    assert min(xs) < 0 or max(xs) > extent or min(ys) < 0 or max(ys) > extent, (
        f"no buffer overhang: x[{min(xs)},{max(xs)}] y[{min(ys)},{max(ys)}] "
        f"all within [0,{extent}] -> hard-clipped at seams"
    )


def test_pyramid_uses_web_mercator_latitude():
    """Tile-local Y must use Web Mercator (latitude is nonlinear), not a linear
    lat mapping. A point tiled at a low and a high zoom must decode back to the
    SAME latitude; a linear mapping drifts by degrees at low zoom (data appears
    to move north as you zoom in) and only converges at high zoom.
    """
    import math

    from shapely.geometry import Point

    def _merc_inv(z, x, y, px, py, ext):  # inverse of the XYZ/WebMercator tile pixel
        n = 2**z
        lon = (x + px / ext) / n * 360.0 - 180.0
        g = (y + py / ext) / n
        lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * g))))
        return lon, lat

    lon0, lat0 = -103.121, 31.55
    got = {}
    for z in (6, 13):
        zz, xx, yy, blob = list(
            _mvt.pyramid_tiles(to_wkb(Point(lon0, lat0)), {"id": 1}, z, z, "l", 4096)
        )[0]
        # Decode with the SAME y orientation the encoder wrote (y_coord_down=True, MVT
        # spec: origin top-left, y increases south) -- matching how MapLibre reads tiles.
        feat = mvt.decode(blob, default_options={"y_coord_down": True})["l"]["features"]
        px, py = feat[0]["geometry"]["coordinates"]
        got[z] = _merc_inv(zz, xx, yy, px, py, 4096)
    for z in (6, 13):
        assert abs(got[z][0] - lon0) < 1e-3, f"z{z} lon {got[z][0]} != {lon0}"
        assert abs(got[z][1] - lat0) < 1e-3, f"z{z} lat {got[z][1]} != {lat0}"
