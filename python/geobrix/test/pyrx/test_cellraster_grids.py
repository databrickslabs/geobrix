"""Grid-adapter refactor tests for pyrx.core.cellraster.

Two guarantees:

* **H3 regression** — the grid-adapter refactor must not change the H3 rasterize
  output. We rasterize a fixed H3 cell set, then round-trip it back through
  ``gridagg.raster_to_grid("h3", "avg")`` and recover the exact per-cell values.
* **quadbin parity** — the new ``grid="quadbin"`` path burns a small quadbin cell
  set into a raster whose band NoData is ``-9999`` (§2.6), and a round-trip through
  ``gridagg.raster_to_grid(..., "quadbin", "avg")`` recovers the same cells and
  values (within 1e-9).

Single source of truth: quadbin cell math comes only from ``pygx._quadbin``.
Pure Python (h3 + quadbin + rasterio + numpy); runs on host.
"""

import h3

from databricks.labs.gbx.pygx import _bng as bng
from databricks.labs.gbx.pygx import _quadbin as qb
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import cellraster as cr
from databricks.labs.gbx.pyrx.core import gridagg


def _h3_cells(res=9):
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    return [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]


def _bng_cells(res=3):
    """A small BNG 1km (res=3) cell set as STRING ids (public BNG surface)."""
    from shapely.geometry import box

    # A London-area box in EPSG:27700 eastings/northings (27700-native, no WGS84).
    poly = box(530000, 180000, 534000, 183000)
    return [bng.format(c) for c in bng.polyfill(poly, res)]


def _quadbin_cells(res=12):
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    ewkb = to_wkb(set_srid(box(-74.02, 40.68, -73.95, 40.73), 4326), include_srid=True)
    return qb.polyfill(ewkb, res)


def test_h3_rasterize_roundtrip_unchanged():
    """H3 rasterize -> raster_to_grid recovers the burned per-cell values.

    This is the regression gate for the grid-adapter refactor: the H3 output
    must be identical to the pre-refactor behavior. Because a centroid burn is
    the inverse of the pixel-centroid raster_to_grid reducer, avg over each
    cell's covered pixels must return that cell's burned value exactly.
    """
    res = 9
    cells = _h3_cells(res)
    # Distinct, non-trivial values per cell.
    cell_values = {c: float(100 + i) for i, c in enumerate(cells)}

    g = cr.compute_gridspec(cells, kring_pad=0)  # tight grid: only the cells
    raster = cr.cells_to_raster(cell_values, *g, resolution=res)

    with _serde.open_tile(raster) as ds:
        assert ds.nodata == -9999.0
        recovered = gridagg.raster_to_grid(ds, res, "h3", "avg")[0]

    got = {int(r["cellID"]): r["measure"] for r in recovered}
    for c, v in cell_values.items():
        # h3.str_to_int gives the unsigned id; cells_to_raster stores signed via
        # int(c) & _U64 round-trip, so compare on the unsigned canonical id.
        cid = h3.str_to_int(cr._h3_str(c))
        assert cid in got, f"H3 cell {cid} missing from round-trip"
        assert abs(got[cid] - v) < 1e-9, f"H3 cell {cid}: {got[cid]} != {v}"


def test_quadbin_rasterize_roundtrip_and_nodata():
    """quadbin rasterize_agg burns cells -> raster (nodata -9999) -> recovers values."""
    res = 12
    cells = _quadbin_cells(res)
    assert len(cells) >= 2, "need a multi-cell quadbin set"
    cell_values = {c: float(10 + i) for i, c in enumerate(cells)}

    g = cr.compute_gridspec(cells, kring_pad=0, grid="quadbin")
    raster = cr.cells_to_raster(cell_values, *g, resolution=res, grid="quadbin")

    with _serde.open_tile(raster) as ds:
        assert ds.nodata == -9999.0, "band NoData must be -9999 (§2.6)"
        recovered = gridagg.raster_to_grid(ds, res, "quadbin", "avg")[0]

    got = {int(r["cellID"]): r["measure"] for r in recovered}
    for c, v in cell_values.items():
        cid = int(c) & cr._U64
        assert cid in got, f"quadbin cell {cid} missing from round-trip"
        assert abs(got[cid] - v) < 1e-9, f"quadbin cell {cid}: {got[cid]} != {v}"


def test_quadbin_gridspec_rejects_mixed_resolution():
    import pytest

    a = qb.point_as_cell(-73.9, 40.7, 10)
    b = qb.point_as_cell(-73.9, 40.7, 11)
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([a, b], grid="quadbin")


def test_bng_rasterize_roundtrip_and_nodata():
    """BNG rasterize_agg burns STRING cells -> 27700 raster -> recovers values.

    BNG is EPSG:27700-native (no WGS84 hop): the sample points and the output
    raster are both in 27700, so the round-trip through
    ``gridagg.raster_to_grid(..., "bng", "avg")`` (which itself binds BNG at
    27700) recovers the exact per-cell values. The band NoData must be -9999
    (§2.6) and the raster CRS must be EPSG:27700.
    """
    res = 3  # 1km
    cells = _bng_cells(res)  # STRING ids, e.g. "TQ3080"
    assert len(cells) >= 2, "need a multi-cell BNG set"
    cell_values = {c: float(50 + i) for i, c in enumerate(cells)}

    # cellraster works in native key space; the BNG adapter's to_key parses the
    # STRING id to the internal Long, so we feed STRING ids straight through.
    g = cr.compute_gridspec(cells, srid=27700, kring_pad=0, grid="bng")
    assert g[7] == 27700, "BNG gridspec srid must be 27700"
    raster = cr.cells_to_raster(cell_values, *g, resolution=res, grid="bng")

    with _serde.open_tile(raster) as ds:
        assert ds.nodata == -9999.0, "band NoData must be -9999 (§2.6)"
        assert ds.crs.to_epsg() == 27700, "BNG raster CRS must be EPSG:27700"
        recovered = gridagg.raster_to_grid(ds, res, "bng", "avg")[0]

    # raster_to_grid renders BNG ids to the public STRING form.
    got = {r["cellID"]: r["measure"] for r in recovered}
    for c, v in cell_values.items():
        assert c in got, f"BNG cell {c} missing from round-trip"
        assert abs(got[c] - v) < 1e-9, f"BNG cell {c}: {got[c]} != {v}"


def test_bng_gridspec_rejects_mixed_resolution():
    import pytest

    a = bng.point_as_cell(530000, 180000, 3)  # 1km
    b = bng.point_as_cell(530000, 180000, 4)  # 100m
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([a, b], grid="bng")
