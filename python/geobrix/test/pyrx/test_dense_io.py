"""Tests for point-cloud IO helpers: read_fused_ply, write_xyzrgb_laz.

TDD: tests written BEFORE implementation (RED -> GREEN).
"""

import struct
import tempfile
import os

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_binary_ply(pts, x_type="float"):
    """Build a minimal binary_little_endian PLY bytes object for testing.

    pts: list of (x, y, z, nx, ny, nz, r, g, b)
    x_type: 'float' (float32) or 'double' (float64) for the x/y/z properties.
    """
    xyz_prop = "double" if x_type == "double" else "float"
    header = (
        "ply\n"
        "format binary_little_endian 1.0\n"
        f"element vertex {len(pts)}\n"
        f"property {xyz_prop} x\n"
        f"property {xyz_prop} y\n"
        f"property {xyz_prop} z\n"
        "property float nx\n"
        "property float ny\n"
        "property float nz\n"
        "property uchar red\n"
        "property uchar green\n"
        "property uchar blue\n"
        "end_header\n"
    )
    if x_type == "double":
        rec_fmt = "<dddfffBBB"
    else:
        rec_fmt = "<ffffffBBB"
    body = b"".join(struct.pack(rec_fmt, *pt) for pt in pts)
    return header.encode("ascii") + body


_SAMPLE_PTS = [
    (1.0, 2.0, 3.0, 0.0, 0.0, 1.0, 255, 128, 64),
    (4.0, 5.0, 6.0, 0.0, 1.0, 0.0, 10, 20, 30),
    (7.0, 8.0, 9.0, 1.0, 0.0, 0.0, 200, 150, 100),
]


# ---------------------------------------------------------------------------
# test_read_fused_ply_roundtrip
# ---------------------------------------------------------------------------


def test_read_fused_ply_roundtrip():
    """Build a tiny binary PLY, read it back, assert x/y/z and r/g/b."""
    from databricks.labs.gbx.pyrx.imagery import read_fused_ply

    ply_bytes = _make_binary_ply(_SAMPLE_PTS)

    with tempfile.NamedTemporaryFile(suffix=".ply", delete=False) as fh:
        fh.write(ply_bytes)
        tmp = fh.name

    try:
        result = read_fused_ply(tmp)

        # Keys present
        assert set(result.keys()) == {"x", "y", "z", "r", "g", "b"}

        # Dtypes
        assert result["x"].dtype == np.float64
        assert result["r"].dtype == np.uint8

        # Length
        assert len(result["x"]) == 3

        # Values — float32 stored in PLY, so approx compare
        xs = [p[0] for p in _SAMPLE_PTS]
        ys = [p[1] for p in _SAMPLE_PTS]
        zs = [p[2] for p in _SAMPLE_PTS]
        np.testing.assert_allclose(result["x"], xs, rtol=1e-5)
        np.testing.assert_allclose(result["y"], ys, rtol=1e-5)
        np.testing.assert_allclose(result["z"], zs, rtol=1e-5)

        # RGB exact
        np.testing.assert_array_equal(result["r"], [255, 10, 200])
        np.testing.assert_array_equal(result["g"], [128, 20, 150])
        np.testing.assert_array_equal(result["b"], [64, 30, 100])
    finally:
        os.unlink(tmp)


def test_read_fused_ply_roundtrip_double():
    """Same test with double-precision x/y/z properties."""
    from databricks.labs.gbx.pyrx.imagery import read_fused_ply

    ply_bytes = _make_binary_ply(_SAMPLE_PTS, x_type="double")
    with tempfile.NamedTemporaryFile(suffix=".ply", delete=False) as fh:
        fh.write(ply_bytes)
        tmp = fh.name

    try:
        result = read_fused_ply(tmp)
        np.testing.assert_allclose(result["x"], [1.0, 4.0, 7.0])
        np.testing.assert_allclose(result["y"], [2.0, 5.0, 8.0])
        np.testing.assert_allclose(result["z"], [3.0, 6.0, 9.0])
        np.testing.assert_array_equal(result["r"], [255, 10, 200])
    finally:
        os.unlink(tmp)


# ---------------------------------------------------------------------------
# test_write_xyzrgb_laz_roundtrip
# ---------------------------------------------------------------------------


def test_write_xyzrgb_laz_roundtrip():
    """Write colored cloud to tmp LAZ (or LAS fallback), read back, check RGB."""
    import laspy

    from databricks.labs.gbx.pyrx.imagery import write_xyzrgb_laz

    x = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    y = np.array([10.0, 11.0, 12.0, 13.0, 14.0])
    z = np.array([100.0, 101.0, 102.0, 103.0, 104.0])
    r = np.array([0, 128, 255, 64, 192], dtype=np.uint8)
    g = np.array([10, 20, 30, 40, 50], dtype=np.uint8)
    b = np.array([200, 150, 100, 50, 0], dtype=np.uint8)

    with tempfile.TemporaryDirectory() as tmpdir:
        laz_path = os.path.join(tmpdir, "cloud.laz")
        out_path = write_xyzrgb_laz(laz_path, x, y, z, r, g, b)

        las = laspy.read(out_path)

        assert len(las.points) == 5

        # XYZ round-trip (tolerance for scale quantization)
        np.testing.assert_allclose(np.array(las.x), x, atol=1e-3)
        np.testing.assert_allclose(np.array(las.y), y, atol=1e-3)
        np.testing.assert_allclose(np.array(las.z), z, atol=1e-3)

        # RGB round-trip: stored as 16-bit (val << 8); recover via >> 8
        np.testing.assert_array_equal(
            (las.red.astype(np.uint16) >> 8).astype(np.uint8), r
        )
        np.testing.assert_array_equal(
            (las.green.astype(np.uint16) >> 8).astype(np.uint8), g
        )
        np.testing.assert_array_equal(
            (las.blue.astype(np.uint16) >> 8).astype(np.uint8), b
        )


def test_write_xyzrgb_laz_returns_path():
    """write_xyzrgb_laz returns the path written (string)."""
    from databricks.labs.gbx.pyrx.imagery import write_xyzrgb_laz

    x = np.array([0.0, 1.0])
    y = np.array([0.0, 1.0])
    z = np.array([0.0, 1.0])
    r = np.array([0, 255], dtype=np.uint8)
    g = np.array([0, 0], dtype=np.uint8)
    b = np.array([0, 0], dtype=np.uint8)

    with tempfile.TemporaryDirectory() as tmpdir:
        laz_path = os.path.join(tmpdir, "out.laz")
        out = write_xyzrgb_laz(laz_path, x, y, z, r, g, b)
        assert isinstance(out, str)
        assert os.path.exists(out)


# ---------------------------------------------------------------------------
# test_read_fused_ply_rejects_ascii
# ---------------------------------------------------------------------------


def test_read_fused_ply_rejects_ascii():
    """A PLY with 'format ascii 1.0' must raise a clear ValueError."""
    from databricks.labs.gbx.pyrx.imagery import read_fused_ply

    header = (
        "ply\n"
        "format ascii 1.0\n"
        "element vertex 1\n"
        "property float x\n"
        "property float y\n"
        "property float z\n"
        "property uchar red\n"
        "property uchar green\n"
        "property uchar blue\n"
        "end_header\n"
        "1.0 2.0 3.0 255 0 0\n"
    )

    with tempfile.NamedTemporaryFile(
        suffix=".ply", delete=False, mode="wb"
    ) as fh:
        fh.write(header.encode("ascii"))
        tmp = fh.name

    try:
        with pytest.raises(ValueError, match="binary_little_endian"):
            read_fused_ply(tmp)
    finally:
        os.unlink(tmp)
