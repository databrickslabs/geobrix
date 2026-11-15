"""The six 500 km squares (one letter): S, T, N, O, H, J.

A name parses to the cell of its own square (not a 100 km letter pair's square), the cell formats back to the name,
its polygon is the 500 km square from the south-west corner, and every point of the square encodes to that cell.
"""

import pytest

from databricks.labs.gbx.pygx import _bng

_SQUARES = {
    "S": (0, 0),
    "T": (500000, 0),
    "N": (0, 500000),
    "O": (500000, 500000),
    "H": (0, 1000000),
    "J": (500000, 1000000),
}


@pytest.mark.parametrize("name", list(_SQUARES))
def test_a_one_letter_name_is_its_own_square_and_round_trips(name):
    x, y = _SQUARES[name]
    cell = _bng.parse(name)
    assert _bng.format(cell) == name
    assert _bng.cell_id_to_geometry(cell).bounds == (x, y, x + 500000, y + 500000)
    assert _bng.area(cell) == 250000.0
    assert _bng.is_valid(cell)


@pytest.mark.parametrize("name", list(_SQUARES))
def test_every_point_of_a_500km_square_is_that_cell(name):
    x, y = _SQUARES[name]
    cell = _bng.parse(name)
    for e, n in ((x, y), (x + 250000, y + 250000), (x + 499999, y + 499999)):
        assert _bng.point_to_cell_id(e, n, -1) == cell


def test_the_six_squares_are_six_distinct_cells():
    assert len({_bng.parse(n) for n in _SQUARES}) == 6


def test_a_500km_cell_rings_around_its_own_square():
    """The ring of N holds the squares next to it that are on the grid: S below, H above, O beside, T and J across."""
    n = _bng.parse("N")
    assert _bng.k_ring(n, 0) == [n]
    loop = {_bng.format(c) for c in _bng.k_loop(n, 1)}
    assert {"S", "H", "O", "T", "J"} <= loop
    assert "N" not in loop
    ring = {_bng.format(c) for c in _bng.k_ring(n, 1)}
    assert ring == loop | {"N"}
    assert {_bng.format(c) for c in _bng.k_loop(_bng.parse("O"), 1)} >= {
        "N",
        "T",
        "S",
        "H",
        "J",
    }
