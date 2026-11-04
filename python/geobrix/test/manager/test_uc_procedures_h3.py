"""``gbx_points_to_h3_surface``: the cells no point fell in are added as NULL cells before the fill, so ``fill_rings`` fills
them. The text is asserted on the host; the fill of the padded set runs on the real fill core.

Fail-on-revert check: the padding. Without it the absent neighbours are never in the set the fill is given.
"""

import re

import pytest

from databricks.labs.gbx.uc import procedures


def _text(name):
    return procedures.RECIPES[name]("cat", "sch")


# --- the H3 surface: absent neighbours are added as NULL cells ------------------------------------------------------


def test_the_h3_surface_pads_the_absent_neighbours_as_null_cells_before_the_fill():
    text = _text("gbx_points_to_h3_surface")
    pad = text.index("h3_kring(cellid, greatest(fill_rings, 0))")
    # the padded cells are what the fill is given: they feed the collect_list that is its first argument
    assert pad > text.index("collect_list(struct(cellid, value)) AS cells FROM (")
    assert "g.cells" in text[: text.index("collect_list(struct(cellid, value))")]
    # the product function, not a GeoBrix one: no catalog prefix; the padding cells carry no value
    assert not re.search(r"gbx_[a-z0-9_]*h3_kring", text)
    assert "CAST(NULL AS DOUBLE) AS value" in text[pad - 200 : pad + 200]
    # a cell with a value keeps it (max ignores the NULL of its own padding row)
    assert "max(value) AS value" in text


def test_filling_the_padded_cells_gives_the_absent_neighbours_a_value():
    """What the padding changes, on the fill core: before it, the cells that no point fell in were never in the set."""
    import h3

    from databricks.labs.gbx.pygx import _cellfill

    def ring(cell, d):
        return [h3.str_to_int(c) for c in h3.grid_ring(h3.int_to_str(cell), d)]

    centre = h3.latlng_to_cell(37.77, -122.42, 8)
    # the ring around the centre in order, so that every other cell is between two cells that have a value
    around = [sorted(h3.grid_ring(centre, 1))[0]]
    while len(around) < 6:
        around.append(
            next(
                c
                for c in h3.grid_ring(centre, 1)
                if c not in around and h3.are_neighbor_cells(c, around[-1])
            )
        )
    points = {h3.str_to_int(c): float(i + 1) for i, c in enumerate(around[::2])}
    points[h3.str_to_int(centre)] = None
    absent = [h3.str_to_int(c) for c in around[1::2]]

    # as it was: only the cells with a point
    unpadded = dict(_cellfill.fill(points, 1, "mean", 2.0, ring))
    # as it is now: the cells within fill_rings of any cell, with no value where no point fell
    padded_in = dict(points)
    for cell in list(points):
        for c in h3.grid_disk(h3.int_to_str(cell), 1):
            padded_in.setdefault(h3.str_to_int(c), None)
    padded = dict(_cellfill.fill(padded_in, 1, "mean", 2.0, ring))

    assert all(c not in unpadded for c in absent)  # never in the set, so never filled
    for (
        c
    ) in (
        absent
    ):  # now in the set, filled from the two cells beside it that have a value
        beside = [
            points[h3.str_to_int(n)]
            for n in h3.grid_ring(h3.int_to_str(c), 1)
            if points.get(h3.str_to_int(n)) is not None
        ]
        assert len(beside) == 2 and padded[c] == pytest.approx(sum(beside) / 2)
    assert all(padded[c] == unpadded[c] for c in unpadded if unpadded[c] is not None)
    assert len(padded) > len(unpadded)
