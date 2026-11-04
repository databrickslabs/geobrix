"""The UC grid ports (quadbin, custom grid, BNG and H3 scalars; the ring table functions): what the oracle rows do not
say. The rows (bench/oracle_tables/grid_*.py) hold the values and the refusals; this file holds the array cap, which is
a mechanism of its own, and the bytes a UC result carries.
"""

import pytest
import shapely

from databricks.labs.gbx.bench.oracles import UcGeometry
from databricks.labs.gbx.uc import handlers

QB_C = 5233961839712272383  # tile (163, 395) at zoom 10
QB_Z26 = 5305923650554237558
QB_SQUARE = "POLYGON((-122.6 37.8, -122.2 37.8, -122.2 38.0, -122.6 38.0, -122.6 37.8))"


def _geom(wkt, srid):
    return UcGeometry(shapely.to_wkb(shapely.from_wkt(wkt)), srid)


# --- the array cap: the one mechanism of the block ------------------------------------------------------------------


def test_an_array_over_the_cap_is_refused_with_a_fix_and_never_truncated(monkeypatch):
    """A cell array is judged by one helper. Over the cap it is refused whole; under it, it comes back whole."""
    g = _geom(QB_SQUARE, 4326)
    whole = handlers.quadbin_geomkring(g, 10, 1)
    assert len(whole) > 5
    monkeypatch.setattr(handlers, "_array_cap", lambda: 5 * handlers.ARRAY_ITEM_BYTES)
    with pytest.raises(ValueError, match=r"over the .*Fix: .*gbx_quadbin_geomkring"):
        handlers.quadbin_geomkring(g, 10, 1)
    monkeypatch.undo()
    assert handlers.quadbin_geomkring(g, 10, 1) == whole


def test_a_ring_too_large_to_build_is_refused_before_it_is_built():
    """The size of a ring of k is known up front: 4M cells at zoom 26 are refused at once, not computed."""
    with pytest.raises(ValueError, match=r"over the .*Fix: "):
        handlers.quadbin_kring(QB_Z26, 1000)


# --- what a UC result carries -----------------------------------------------------------------------------------------


def test_quadbin_bytes_are_little_endian_ewkb_with_the_4326_srid():
    for blob in (
        handlers.quadbin_aswkb(QB_C),
        handlers.quadbin_centroid(QB_C),
        handlers.quadbin_cellunion([QB_C]),
    ):
        assert blob[:1] == b"\x01"
        assert shapely.get_srid(shapely.from_wkb(blob)) == 4326


def test_quadbin_chips_are_ewkb_with_the_4326_srid():
    chips = handlers.quadbin_tessellate(_geom(QB_SQUARE, 4326), 10)
    assert chips and all(
        shapely.get_srid(shapely.from_wkb(c["geom"])) == 4326
        and c["geom"][:1] == b"\x01"
        for c in chips
    )


def test_bng_chip_results_are_ewkb_with_srid_27700_and_a_tuple_chip_is_read_by_position():
    box = shapely.box
    west = {
        "cellid": "TQ3080",
        "core": False,
        "chip": shapely.to_wkb(box(530000, 180000, 530500, 181000)),
    }
    south = ("TQ3080", False, shapely.to_wkb(box(530000, 180000, 531000, 180500)))
    for fn, area in (
        (handlers.bng_cellunion, 750000.0),
        (handlers.bng_cellintersection, 250000.0),
    ):
        out = fn(west, south)
        g = shapely.from_wkb(out["chip"])
        assert out["chip"][:1] == b"\x01" and shapely.get_srid(g) == 27700
        assert abs(g.area - area) < 1e-6
