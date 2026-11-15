import json

import numpy as np

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import fingerprint as fp


def test_scalar_fingerprint():
    s = fp.fingerprint_output(64)
    d = json.loads(s)
    assert d["kind"] == "scalar" and d["value"] == 64


def test_scalar_list_fingerprint():
    s = fp.fingerprint_output([1.0, 2.5, 3.0])
    d = json.loads(s)
    assert d["kind"] == "scalar_list" and d["values"] == [1.0, 2.5, 3.0]


def test_raster_fingerprint_per_band_stats():
    raster = dg.make_tile_bytes(
        tile_px=16, bands=2, dtype="float32", srid=4326, nodata_frac=0.1, seed=1
    )
    s = fp.fingerprint_output(raster)
    d = json.loads(s)
    assert d["kind"] == "raster"
    assert len(d["bands"]) == 2
    b0 = d["bands"][0]
    for k in ("shape", "dtype", "nodata_count", "min", "max", "mean", "std"):
        assert k in b0
    assert b0["shape"] == [16, 16]


def test_raster_fingerprint_is_deterministic():
    raster = dg.make_tile_bytes(
        tile_px=16, bands=1, dtype="float32", srid=4326, nodata_frac=0.0, seed=2
    )
    assert fp.fingerprint_output(raster) == fp.fingerprint_output(raster)


def test_numpy_scalar_fingerprint_serializes():
    # numpy scalars (e.g. from a core reduction) must serialize, not crash json.dumps.
    s_int = fp.fingerprint_output(np.int32(5))
    assert json.loads(s_int)["value"] == 5
    s_float = fp.fingerprint_output(np.float32(2.5))
    assert json.loads(s_float)["value"] == 2.5
    s_list = fp.fingerprint_output([np.float64(1.0), np.int64(2)])
    assert json.loads(s_list)["values"] == [1.0, 2]


# --- H3 / quadbin cell-id parity gate ----------------------------------------
# Heavy (Uber H3 `geoToH3(lat,lon,res)` / CARTO `Quadbin.pointToCell`) and light
# (`h3.latlng_to_cell` + `str_to_int` / `quadbin.point_to_cell`) compute the SAME
# standard cell id for a point. These pins fail loudly if either side's cell-id
# convention drifts (which would silently invalidate the `cells_hash` pass signal).
def test_h3_cell_id_pinned_for_known_point():
    import numpy as np

    from databricks.labs.gbx.pyrx.core import gridagg

    # POINT(-73.99 40.75) (lower Manhattan) @ H3 res 7 -> standard addr 872a100d2ffffff.
    # Vectorized cell-id API (single-element arrays); pin guards the convention.
    cell = gridagg._h3_cells(np.array([-73.99]), np.array([40.75]), 7)[0]
    assert int(cell) == 608725924560502783


def test_quadbin_cell_id_pinned_for_known_point():
    import numpy as np

    from databricks.labs.gbx.pygx import _quadbin

    # Same point @ quadbin res 10 -> standard CARTO quadbin cell id (the encoder
    # raster_to_grid uses, shared with heavy and tessellate).
    cell = _quadbin.point_as_cell_vec(np.array([-73.99]), np.array([40.75]), 10)[0]
    assert int(cell) == 5234172679656833023


# --- dggs_grid fingerprint (bucket B, grid fns) ------------------------------
# raster_to_grid returns one list per band of {"cellID": int, "measure": float|int}.
# The fingerprint flattens cells across bands, records the cell COUNT, a hash of
# the sorted (signed-int64) cell ids, and order-independent agg stats over measures.
def test_dggs_grid_fingerprint_shape():
    cells = [
        [{"cellID": 10, "measure": 1.0}, {"cellID": 5, "measure": 3.0}],
        [{"cellID": 7, "measure": 2.0}],
    ]
    d = json.loads(fp.fingerprint_dggs_grid(cells))
    assert d["kind"] == "dggs_grid"
    assert d["count"] == 3
    assert isinstance(d["cells_hash"], str) and len(d["cells_hash"]) == 64
    for k in ("min", "max", "mean", "std"):
        assert k in d["agg"]
    assert d["agg"]["min"] == 1.0 and d["agg"]["max"] == 3.0


def test_dggs_grid_fingerprint_order_independent_hash():
    a = [[{"cellID": 5, "measure": 1.0}, {"cellID": 10, "measure": 2.0}]]
    b = [[{"cellID": 10, "measure": 2.0}, {"cellID": 5, "measure": 1.0}]]
    assert fp.fingerprint_dggs_grid(a) == fp.fingerprint_dggs_grid(b)


def test_dggs_grid_fingerprint_signed_int64_canonical():
    # An H3 id >= 2^63 (raw unsigned from light) and its signed-int64 form (heavy's
    # LongMap representation) must hash identically after canonicalization.
    big = 2**63 + 12345
    signed = big - 2**64
    light = [[{"cellID": big, "measure": 1.0}]]
    heavy = [[{"cellID": signed, "measure": 1.0}]]
    assert fp.fingerprint_dggs_grid(light) == fp.fingerprint_dggs_grid(heavy)


def test_dggs_grid_fingerprint_count_only_when_no_values():
    # tessellate-style: cells with no measure -> empty agg, count + hash still set.
    cells = [[{"cellID": 3}, {"cellID": 4}]]
    d = json.loads(fp.fingerprint_dggs_grid(cells))
    assert d["count"] == 2
    assert d["agg"] == {}


# --- vector fingerprint (bucket B, vector fns) -------------------------------
# contour returns [{"geom_wkb": bytes, "value": float}] (LineStrings -> length);
# polygonize returns [(geom_wkb, value)] (polygons -> area). The fingerprint
# records the feature COUNT, the total measure (length for lines, area for
# polygons), and order-independent agg stats over the attributes.
def _line_feature(coords, value):
    import shapely.wkb
    from shapely.geometry import LineString

    return {"geom_wkb": shapely.wkb.dumps(LineString(coords)), "value": float(value)}


def _poly_feature(value):
    import shapely.wkb
    from shapely.geometry import Polygon

    return (
        shapely.wkb.dumps(Polygon([(0, 0), (0, 2), (2, 2), (2, 0)])),
        float(value),
    )


def test_vector_fingerprint_lines_uses_length():
    feats = [_line_feature([(0, 0), (3, 4)], 1.0), _line_feature([(0, 0), (0, 5)], 2.0)]
    d = json.loads(fp.fingerprint_vector(feats))
    assert d["kind"] == "vector"
    assert d["count"] == 2
    # 5.0 (3-4-5 triangle) + 5.0 = 10.0
    assert abs(d["measure"] - 10.0) < 1e-9
    assert d["attr_agg"]["min"] == 1.0 and d["attr_agg"]["max"] == 2.0


def test_vector_fingerprint_polygons_uses_area():
    feats = [_poly_feature(1.0), _poly_feature(5.0)]
    d = json.loads(fp.fingerprint_vector(feats))
    assert d["count"] == 2
    # two 2x2 squares -> 4.0 + 4.0 = 8.0
    assert abs(d["measure"] - 8.0) < 1e-9


def test_vector_fingerprint_accepts_tuple_features():
    # polygonize emits (geom_wkb, value) tuples; contour emits dicts. Both work.
    feats = [_poly_feature(2.0)]
    d = json.loads(fp.fingerprint_vector(feats))
    assert d["count"] == 1
    assert abs(d["measure"] - 4.0) < 1e-9


def test_vector_fingerprint_order_independent():
    a = [_line_feature([(0, 0), (3, 4)], 1.0), _line_feature([(0, 0), (0, 5)], 2.0)]
    b = [_line_feature([(0, 0), (0, 5)], 2.0), _line_feature([(0, 0), (3, 4)], 1.0)]
    assert fp.fingerprint_vector(a) == fp.fingerprint_vector(b)
