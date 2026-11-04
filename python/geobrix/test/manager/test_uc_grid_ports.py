"""The UC grid ports (quadbin, custom grid, BNG and H3 scalars; the ring table functions): what the oracle rows do not
say. The rows (bench/oracle_tables/grid_*.py) hold the values and the refusals; this file holds the array cap, which is
a mechanism of its own, and the bytes a UC result carries.
"""

import pytest
import shapely

from databricks.labs.gbx.bench.oracle_tables import (  # noqa: F401 - registers the builders
    grid_scalar,
)
from databricks.labs.gbx.bench.oracles import BUILDERS, UcGeometry
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


BNG_SQ = shapely.from_wkt(
    "POLYGON((530000 180000, 532000 180000, 532000 182000, 530000 182000, 530000 180000))"
)
BNG_POINT = shapely.from_wkt("POINT(530500 180500)")
H3_POINT = shapely.from_wkt("POINT(-122.4 37.8)")

ARRAY_HANDLERS = [  # (handler, arguments, the session function a Fix names)
    (handlers.bng_kring, ("TQ3080", 3), "gbx_bng_kring"),
    (handlers.bng_polyfill, (_geom(BNG_SQ.wkt, 27700), 3), "gbx_bng_polyfill"),
    (
        handlers.bng_geomkring,
        (_geom(BNG_POINT.wkt, 27700), 3, 2),
        "gbx_bng_geomkring",
    ),
    (
        handlers.bng_tessellate,
        (_geom(BNG_SQ.wkt, 27700), 3, False),
        "gbx_bng_tessellate",
    ),
    (handlers.h3_geomkring, (_geom(H3_POINT.wkt, 4326), 8, 2), "gbx_h3_geomkring"),
    (
        handlers.custom_polyfill,
        (_geom(BNG_SQ.wkt, 27700), BUILDERS["custom_grid_27700"](None), 0),
        "gbx_custom_polyfill",
    ),
    (handlers.quadbin_polyfill, (_geom(QB_SQUARE, 4326), 10), "gbx_quadbin_polyfill"),
    (
        handlers.quadbin_tessellate,
        (_geom(QB_SQUARE, 4326), 10),
        "gbx_quadbin_tessellate",
    ),
]


@pytest.mark.parametrize(
    "handler,args,fn", ARRAY_HANDLERS, ids=[a[2] for a in ARRAY_HANDLERS]
)
def test_every_grid_array_handler_refuses_a_result_over_the_cap_and_never_truncates(
    monkeypatch, handler, args, fn
):
    """The cap helper judges the ring, polyfill, geometry-ring and tessellate handlers alike: an array past it is a
    refusal that names the session function, not NULL and not a cut array."""
    whole = handler(*args)
    assert whole and len(whole) > 2
    monkeypatch.setattr(handlers, "_array_cap", lambda: 2 * handlers.ARRAY_ITEM_BYTES)
    with pytest.raises(ValueError, match=rf"over the .*Fix: .*{fn}") as err:
        handler(*args)
    # a fill of a geometry names the geometry's size as the thing to lower, not k
    if "polyfill" in fn:
        assert "lower k" not in str(err.value) and "coarser resolution" in str(
            err.value
        )
    monkeypatch.undo()
    assert handler(*args) == whole


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
