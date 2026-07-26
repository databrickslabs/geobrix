"""Bit-exactness tests for the vectorized BNG encoder.

The shared numpy core is a rewrite of the current scalar codec body, so the
gate is TWO checks: (1) the refactored scalar reproduces the CURRENT behavior
over a frozen baseline (no regression), and (2) the vec form equals the scalar
element-for-element (shared core => cannot drift, but this documents it and
guards the thin wrappers). Includes res +-6 to catch any int64 overflow /
float64 precision loss, and out-of-GB / negative / boundary coords (the encoder
sees them because encode runs BEFORE is_valid).
"""

import numpy as np
import pytest

from databricks.labs.gbx.pygx import _bng

# Dense EPSG:27700 grid across GB + explicit out-of-GB / negative / boundary coords.
_EAST = np.concatenate(
    [
        np.linspace(0.0, 700000.0, 43),
        np.array(
            [-150000.0, -1.0, 0.0, 99999.5, 100000.0, 529999.9, 530000.0, 700001.0]
        ),
    ]
)
_NORTH = np.concatenate(
    [
        np.linspace(0.0, 1300000.0, 41),
        np.array([-250000.0, -1.0, 0.0, 179999.9, 180000.0, 1300001.0]),
    ]
)
_RESOLUTIONS = [-1, 1, -2, 2, -3, 3, -4, 4, -5, 5, -6, 6]


def _grid():
    EE, NN = np.meshgrid(_EAST, _NORTH)
    return EE.ravel(), NN.ravel()


@pytest.mark.parametrize("res", _RESOLUTIONS)
def test_point_to_cell_id_vec_equals_scalar(res):
    e, n = _grid()
    vec = _bng.point_to_cell_id_vec(e, n, res)
    assert vec.dtype == np.int64
    for ei, ni, c in zip(e, n, vec):
        expected = _bng.point_to_cell_id(float(ei), float(ni), res)
        assert int(c) == expected, f"e={ei} n={ni} res={res}: {int(c)} != {expected}"


@pytest.mark.parametrize("res", _RESOLUTIONS)
def test_scalar_matches_frozen_reference(res):
    # Reference = an inlined copy of the ORIGINAL scalar body, proving the
    # refactor introduced no regression. (Kept local so it can't drift with
    # the module under test.)
    import math

    def ref_get_quadrant(resolution, eastings, northings, divisor):
        if resolution < -1:
            e_q = eastings / divisor
            n_q = northings / divisor
            e_dec = e_q - math.floor(e_q)
            n_dec = n_q - math.floor(n_q)
            if e_dec < 0.5 and n_dec < 0.5:
                return 1
            if e_dec < 0.5:
                return 2
            if n_dec < 0.5:
                return 4
            return 3
        return 0

    def ref_encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution):
        id_placeholder = 10 ** (5 + 2 * n_positions - 2)
        e_letter_shift = 10 ** (3 + 2 * n_positions - 2)
        n_letter_shift = 10 ** (1 + 2 * n_positions - 2)
        e_shift = 10**n_positions
        n_shift = 10
        if resolution == -1:
            val = (id_placeholder + e_letter * e_letter_shift) / 100 + quadrant
        else:
            val = (
                id_placeholder
                + e_letter * e_letter_shift
                + n_letter * n_letter_shift
                + e_bin * e_shift
                + n_bin * n_shift
                + quadrant
            )
        return int(val)

    def ref_point_to_cell_id(eastings, northings, resolution):
        e_int = int(eastings)
        n_int = int(northings)
        e_letter = int(e_int / 100000)
        n_letter = int(n_int / 100000)
        if resolution < 0:
            divisor = 10 ** (6 - abs(resolution) + 1)
        else:
            divisor = 10 ** (6 - resolution)
        quadrant = ref_get_quadrant(resolution, e_int, n_int, divisor)
        n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
        e_bin = math.floor((e_int % 100000) / divisor)
        n_bin = math.floor((n_int % 100000) / divisor)
        return ref_encode(
            e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution
        )

    e, n = _grid()
    for ei, ni in zip(e, n):
        assert _bng.point_to_cell_id(float(ei), float(ni), res) == ref_point_to_cell_id(
            float(ei), float(ni), res
        ), f"e={ei} n={ni} res={res}"


def test_vec_dtype_is_int64_at_high_resolution():
    # res 6 packed id ~1.x*10^15 -- must stay exact in int64, not round in float64.
    e = np.array([529999.0, 530000.0, 123456.0])
    n = np.array([179999.0, 180000.0, 654321.0])
    vec = _bng.point_to_cell_id_vec(e, n, 6)
    assert vec.dtype == np.int64
    for ei, ni, c in zip(e, n, vec):
        assert int(c) == _bng.point_to_cell_id(float(ei), float(ni), 6)
