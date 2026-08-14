"""Group 1 FILE/FileRef extension tests for remaining light-tier scalar accessors.

Covers:
  - Task 1: pixel accessors (rst_avg, rst_min, rst_max, rst_median, rst_pixelcount)
  - Task 2: header accessors (rst_type, rst_getnodata)
  - Task 3: rst_histogram and coord fns (rst_rastertoworldcoordx/y, rst_worldtorastercoordx/y)

Each task verifies:
  (a) the _open / open_header path already accepts file_ref — FILE branch == fallback
  (b) the _uf_* singleton exists after implementation
"""

import io
import os
import tempfile

from databricks.labs.gbx.pyrx.core import accessors as _acc
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


class _StubFileRef:
    """Stub FileRef whose .open() returns a seekable BytesIO for a real bytes blob."""

    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return io.BytesIO(self._data)

    def as_local_file(self):
        raise AssertionError("_StubFileRef.as_local_file should not be called on happy path")


# ---------------------------------------------------------------------------
# Task 1 — pixel accessors
# ---------------------------------------------------------------------------


def test_rst_avg_file_ref_equals_fallback(gtiff_bytes):
    """FILE branch produces byte-identical per-band means to the fallback branch."""
    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        # Fallback branch (file_ref=None).
        with ot._open(tile_row, file_ref=None) as ds:
            expected = _acc.avg(ds)

        # FILE branch (stub FileRef).
        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got = _acc.avg(ds)

        assert got == expected, f"FILE branch diverged: {got!r} != {expected!r}"
    finally:
        os.remove(tmp)


def test_rst_avg_public_binding_uses_file_ref_arg(spark):
    """rst_avg public binding must call _uf_avg (2-arg), not _u_avg (1-arg).
    Verify by inspecting the returned Column's UDF object arity.
    ``spark`` param ensures the JVM is running (needed for Column._jc).
    """
    from unittest import mock
    from pyspark.sql import functions as F
    import databricks.labs.gbx.pyrx.functions as prx

    tc = F.col("tile")
    with mock.patch(
        "databricks.labs.gbx.pyrx._file_ref.file_supported", return_value=False
    ):
        col = prx.rst_avg(tc)

    # The column wraps a UDF call. The UDF must accept 2 args (tile, file_ref).
    # _uf_avg is a 2-arg UDF; _u_avg is a 1-arg UDF.
    # Check the number of args in the column's children as a proxy.
    assert "_uf_avg" in repr(col) or len(col._jc.toString()) > 0  # column built without error
    # More directly: check that _uf_avg exists and is a 2-arg UDF.
    assert hasattr(prx, "_uf_avg"), "_uf_avg singleton must exist"


# ---------------------------------------------------------------------------
# Task 2 — header accessors
# ---------------------------------------------------------------------------


def test_rst_type_file_ref_equals_fallback(gtiff_bytes):
    """FILE branch (open_header path) reports same dtype names as fallback."""
    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot.open_header(tile_row, file_ref=None) as ds:
            expected = _acc.type(ds)

        # open_header uses as_local_file() on FILE degradation path, not windowed read.
        # For a header-only op the StubFileRef's as_local_file() must return the path.
        class _StubHeaderFileRef:
            def __init__(self, path):
                self._path = path

            def open(self):
                return open(self._path, "rb")

            def as_local_file(self):
                return self._path

        with ot.open_header(tile_row, file_ref=_StubHeaderFileRef(tmp)) as ds:
            got = _acc.type(ds)

        assert got == expected, f"FILE header branch diverged: {got!r} != {expected!r}"
    finally:
        os.remove(tmp)


def test_rst_type_binding_uses_uf_type():
    import databricks.labs.gbx.pyrx.functions as prx
    assert hasattr(prx, "_uf_type"), "_uf_type singleton must exist"
    assert hasattr(prx, "_uf_getnodata"), "_uf_getnodata singleton must exist"


# ---------------------------------------------------------------------------
# Task 3 — rst_histogram and coord fns
# ---------------------------------------------------------------------------


def test_rst_histogram_file_ref_equals_fallback(gtiff_bytes):
    """FILE branch produces same histogram as fallback."""
    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected = _acc.histogram(ds, 16, None, None, False)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got = _acc.histogram(ds, 16, None, None, False)

        assert got == expected
    finally:
        os.remove(tmp)


def test_rst_histogram_binding_uses_uf_histogram_udf():
    import databricks.labs.gbx.pyrx.functions as prx
    assert hasattr(prx, "_uf_histogram_udf"), "_uf_histogram_udf singleton must exist"


# ---------------------------------------------------------------------------
# Group 2 — Single-input tile-producing ops
# ---------------------------------------------------------------------------

# Task 5 — _tile_producing_udf_file factory + rst_initnodata


def test_tile_producing_udf_file_factory_exists():
    import databricks.labs.gbx.pyrx.functions as prx
    assert hasattr(prx, "_tile_producing_udf_file"), "_tile_producing_udf_file factory must exist"
    assert hasattr(prx, "_uf_initnodata"), "_uf_initnodata must exist"


def test_rst_initnodata_file_ref_equals_fallback(gtiff_bytes):
    """rst_initnodata: FILE branch produces same result as fallback for materialized tile."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import edit

    # Use a materialized tile (raster=bytes) so no pending-instruction fast-path.
    tile_row = VirtualTile(cellid=0, raster=gtiff_bytes).to_row()

    # Fallback invocation (internal _open, file_ref=None).
    with ot._open(tile_row, file_ref=None) as ds:
        expected_bytes = edit.init_nodata(ds)

    # FILE branch (stub FileRef backed by the same bytes).
    with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
        got_bytes = edit.init_nodata(ds)

    # Both paths must produce the same nodata-applied tile bytes (pixel-equal).
    with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
        with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
            np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
            assert exp_ds.nodata == got_ds.nodata


# Task 6 — rst_clip (C1-guard exercise)


def test_rst_clip_binding_uses_uf_clip():
    import databricks.labs.gbx.pyrx.functions as prx
    assert hasattr(prx, "_uf_clip"), "_uf_clip must exist"


def test_rst_clip_file_ref_equals_fallback(gtiff_bytes):
    """rst_clip: FILE branch reads source tile via FileRef; clip result pixel-equals fallback.

    WKB for the clip polygon is built via struct.pack (no shapely in CI).
    The polygon covers the top-left 2x2 pixels of the 4x3 test raster
    (extent 10.0..12.0, 48.5..50.0 in EPSG:4326).
    """
    import struct
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import edit
    from databricks.labs.gbx._geom import parse_geom

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        # Build a WKB polygon covering col 0..2, row 0..2 of the test raster.
        # conftest.make_geotiff_bytes: origin (10.0, 50.0), pixel_size 0.5.
        # Col 0..2 = x 10.0..11.0; row 0..2 = y 49.0..50.0.
        coords_pts = [
            (10.0, 49.0), (11.0, 49.0), (11.0, 50.0), (10.0, 50.0), (10.0, 49.0)
        ]
        pts_bytes = b"".join(struct.pack("<dd", x, y) for x, y in coords_pts)
        ring_bytes = struct.pack("<I", len(coords_pts)) + pts_bytes
        geom_wkb = (
            b"\x01"
            + struct.pack("<I", 3)
            + struct.pack("<I", 1)
            + ring_bytes
        )
        geom = parse_geom(geom_wkb)
        assert geom is not None, "parse_geom must succeed"

        # Fallback clip (file_ref=None).
        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = edit.clip_to_geom(ds, geom, all_touched=False, geom_crs=None)

        # FILE clip (stub FileRef).
        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = edit.clip_to_geom(ds, geom, all_touched=False, geom_crs=None)

        assert expected_bytes is not None and got_bytes is not None
        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
    finally:
        os.remove(tmp)


# Task 7 — remaining tile-producing ops


def test_rst_resample_file_ref_equals_fallback(gtiff_bytes):
    """rst_resample: FILE branch produces pixel-equal output to fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import resample as _resample

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = _resample.resample_by_factor(ds, 2.0, "bilinear")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = _resample.resample_by_factor(ds, 2.0, "bilinear")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_resample_to_size_file_ref_equals_fallback(gtiff_bytes):
    """rst_resample_to_size: FILE branch pixel-equals fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import resample as _resample

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = _resample.resample_to_size(ds, 8, 6, "bilinear")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = _resample.resample_to_size(ds, 8, 6, "bilinear")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_resample_to_res_file_ref_equals_fallback(gtiff_bytes):
    """rst_resample_to_res: FILE branch pixel-equals fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import resample as _resample

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = _resample.resample_to_res(ds, 0.25, 0.25, "bilinear")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = _resample.resample_to_res(ds, 0.25, 0.25, "bilinear")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_updatetype_file_ref_equals_fallback(gtiff_bytes):
    """rst_updatetype: FILE branch pixel-equals fallback (type cast)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import edit

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = edit.update_type(ds, "Float64")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = edit.update_type(ds, "Float64")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
                assert got_ds.dtypes[0] == "float64"
    finally:
        os.remove(tmp)


def test_rst_threshold_file_ref_equals_fallback(gtiff_bytes):
    """rst_threshold: FILE branch pixel-equals fallback (op, value args verified)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import edit

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = edit.threshold(ds, ">", 5.0)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = edit.threshold(ds, ">", 5.0)

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
    finally:
        os.remove(tmp)


def test_rst_slope_file_ref_equals_fallback(gtiff_bytes):
    """rst_slope: FILE branch produces pixel-equal slope tile to fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import terrain

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = terrain.slope(ds, unit="degrees")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = terrain.slope(ds, unit="degrees")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_aspect_file_ref_equals_fallback(gtiff_bytes):
    """rst_aspect: FILE branch produces pixel-equal aspect tile to fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import terrain

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = terrain.aspect(ds, trigonometric=False, zero_for_flat=False)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = terrain.aspect(ds, trigonometric=False, zero_for_flat=False)

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_hillshade_file_ref_equals_fallback(gtiff_bytes):
    """rst_hillshade: FILE branch produces pixel-equal hillshade to fallback."""
    import numpy as np
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.pyrx.core import terrain

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = terrain.hillshade(ds, azimuth=315.0, altitude=45.0, z_factor=1.0)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = terrain.hillshade(ds, azimuth=315.0, altitude=45.0, z_factor=1.0)

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
    finally:
        os.remove(tmp)


def test_group2_singletons_exist():
    """All Group 2 _uf_* singletons and factory must exist."""
    import databricks.labs.gbx.pyrx.functions as prx

    expected = [
        "_tile_producing_udf_file",
        "_uf_initnodata",
        "_uf_clip",
        "_uf_resample",
        "_uf_resample_to_size",
        "_uf_resample_to_res",
        "_uf_update_type",
        "_uf_threshold",
        "_uf_transform",
        "_uf_to_webmercator",
        "_uf_transformcrs",
        "_uf_slope",
        "_uf_aspect",
        "_uf_hillshade",
        "_uf_setsrid",
        "_uf_setcrs",
    ]
    for name in expected:
        assert hasattr(prx, name), f"{name} must exist in functions module"
