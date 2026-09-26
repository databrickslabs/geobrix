"""Tests for write_xyz_laz (LAS format-0 XYZ-only encoder) in pyrx/imagery.py.

TDD: test written BEFORE implementation (RED -> GREEN).
"""

import numpy as np


def _decode_laz(path):
    """Read a written .las/.laz back with laspy; return (n, xs, ys, zs, fmt_id)."""
    import laspy

    las = laspy.read(path)
    return (
        len(las.points),
        np.asarray(las.x),
        np.asarray(las.y),
        np.asarray(las.z),
        int(las.header.point_format.id),
    )


def test_write_xyz_laz_roundtrips_xyz_format0(tmp_path):
    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz

    x = np.array([0.0, 1.0, 2.5], dtype=np.float64)
    y = np.array([10.0, 11.0, 12.0], dtype=np.float64)
    z = np.array([100.0, 101.0, 102.0], dtype=np.float64)
    out = write_xyz_laz(str(tmp_path / "xyz.laz"), x, y, z, crs=32611)

    n, xs, ys, zs, fmt = _decode_laz(out)
    assert n == 3
    assert fmt == 0  # XYZ-only point format
    np.testing.assert_allclose(sorted(xs), sorted(x), atol=1e-4)
    np.testing.assert_allclose(sorted(zs), sorted(z), atol=1e-4)
