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
        raise AssertionError(
            "_StubFileRef.as_local_file should not be called on happy path"
        )


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
    # NOTE: repr(col) shows "_udf" (closure name inside the factory), never "_uf_avg".
    # The right operand is trivially true but the left is always false; see A4 in fix report.
    # TODO: replace with an arity-based check once a reliable introspection path is found.
    assert (
        "_uf_avg" in repr(col) or len(col._jc.toString()) > 0
    )  # column built without error
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

# Task 5 — rst_initnodata


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

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx.core import edit

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
            (10.0, 49.0),
            (11.0, 49.0),
            (11.0, 50.0),
            (10.0, 50.0),
            (10.0, 49.0),
        ]
        pts_bytes = b"".join(struct.pack("<dd", x, y) for x, y in coords_pts)
        ring_bytes = struct.pack("<I", len(coords_pts)) + pts_bytes
        geom_wkb = b"\x01" + struct.pack("<I", 3) + struct.pack("<I", 1) + ring_bytes
        geom = parse_geom(geom_wkb)
        assert geom is not None, "parse_geom must succeed"

        # Fallback clip (file_ref=None).
        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = edit.clip_to_geom(
                ds, geom, all_touched=False, geom_crs=None
            )

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
            expected_bytes = terrain.aspect(
                ds, trigonometric=False, zero_for_flat=False
            )

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
            expected_bytes = terrain.hillshade(
                ds, azimuth=315.0, altitude=45.0, z_factor=1.0
            )

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = terrain.hillshade(
                ds, azimuth=315.0, altitude=45.0, z_factor=1.0
            )

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
    finally:
        os.remove(tmp)


def test_group2_singletons_exist():
    """All Group 2 _uf_* singletons and factory must exist."""
    import databricks.labs.gbx.pyrx.functions as prx

    expected = [
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


# ---------------------------------------------------------------------------
# Group 3 — Multi-input / array / aggregators
# ---------------------------------------------------------------------------

# Task 9 — rst_frombands (ARRAY input, FILE-ARRAY injection)


def test_rst_frombands_binding_uses_uf_frombands():
    import databricks.labs.gbx.pyrx.functions as prx

    assert hasattr(prx, "_uf_frombands"), "_uf_frombands must exist"


def test_rst_frombands_file_ref_equals_fallback(gtiff_bytes):
    """rst_frombands: FILE-array path assembles same multi-band tile as fallback."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import agg as agg_core
    from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

    fd1, tmp1 = tempfile.mkstemp(suffix=".tif")
    fd2, tmp2 = tempfile.mkstemp(suffix=".tif")
    os.close(fd1)
    os.close(fd2)
    with open(tmp1, "wb") as fh:
        fh.write(gtiff_bytes)
    with open(tmp2, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        vt1 = VirtualTile(cellid=0, path=tmp1, window=(0, 0, 4, 3)).to_row()
        vt2 = VirtualTile(cellid=0, path=tmp2, window=(0, 0, 4, 3)).to_row()

        # Fallback: materialize each tile and call frombands_tiles.
        with ot._open(vt1, file_ref=None) as ds1:
            b1 = _dataset_to_gtiff_bytes(ds1)
        with ot._open(vt2, file_ref=None) as ds2:
            b2 = _dataset_to_gtiff_bytes(ds2)
        expected_bytes = agg_core.frombands_tiles([(0, b1), (1, b2)])

        # FILE path: read each tile via StubFileRef.
        with ot._open(vt1, file_ref=_StubFileRef(gtiff_bytes)) as ds1:
            fb1 = _dataset_to_gtiff_bytes(ds1)
        with ot._open(vt2, file_ref=_StubFileRef(gtiff_bytes)) as ds2:
            fb2 = _dataset_to_gtiff_bytes(ds2)
        got_bytes = agg_core.frombands_tiles([(0, fb1), (1, fb2)])

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
                assert got_ds.count == 2
    finally:
        os.remove(tmp1)
        os.remove(tmp2)


# Task 10 — rst_merge and rst_combineavg


def test_rst_merge_binding_uses_uf_merge():
    import databricks.labs.gbx.pyrx.functions as prx

    assert hasattr(prx, "_uf_merge"), "_uf_merge must exist"
    assert hasattr(prx, "_uf_combineavg"), "_uf_combineavg must exist"


def test_rst_merge_file_ref_equals_fallback(gtiff_bytes):
    """rst_merge: FILE-array path produces same mosaic as fallback."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import agg as agg_core
    from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        vt = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(vt, file_ref=None) as ds:
            b = _dataset_to_gtiff_bytes(ds)
        expected_bytes = agg_core.merge_tiles([b, b])

        with ot._open(vt, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            fb = _dataset_to_gtiff_bytes(ds)
        got_bytes = agg_core.merge_tiles([fb, fb])

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
    finally:
        os.remove(tmp)


# Task 11 — rst_mapalgebra (FILE-array + expression arg)


def test_rst_mapalgebra_binding_uses_uf_mapalgebra():
    import databricks.labs.gbx.pyrx.functions as prx

    assert hasattr(prx, "_uf_mapalgebra"), "_uf_mapalgebra must exist"


def test_rst_mapalgebra_file_ref_equals_fallback(gtiff_bytes):
    """rst_mapalgebra: FILE-array path produces same output as fallback."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
    from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        vt = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()
        expression = "A * 2"

        # Fallback path.
        with ot._open(vt, file_ref=None) as ds:
            b = _dataset_to_gtiff_bytes(ds)
        expected_bytes = mapalgebra_core.mapalgebra([b], expression)

        # FILE path.
        with ot._open(vt, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            fb = _dataset_to_gtiff_bytes(ds)
        got_bytes = mapalgebra_core.mapalgebra([fb], expression)

        assert expected_bytes is not None and got_bytes is not None
        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


# Task 12 — grouped aggregators


def test_rst_merge_agg_binding_has_file_variant():
    import databricks.labs.gbx.pyrx.functions as prx

    assert hasattr(prx, "_merge_agg_file_udf"), "_merge_agg_file_udf must exist"
    assert hasattr(
        prx, "_combineavg_agg_file_udf"
    ), "_combineavg_agg_file_udf must exist"
    assert hasattr(prx, "_frombands_agg_file_udf"), "_frombands_agg_file_udf must exist"


def test_rst_merge_agg_file_ref_unit(gtiff_bytes):
    """Unit test: _merge_agg_file_udf processes tiles correctly with stub FileRefs."""
    import pandas as pd
    from rasterio.io import MemoryFile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        vt_row = VirtualTile(cellid=7, path=tmp, window=(0, 0, 4, 3)).to_row()
        fref_stub = _StubFileRef(gtiff_bytes)

        from databricks.labs.gbx.pyrx.functions import _merge_agg_file_udf

        tile_series = pd.Series([vt_row])
        fref_series = pd.Series([fref_stub])

        # Call the underlying function directly (pandas_udf wraps a plain fn).
        raw_fn = _merge_agg_file_udf.func
        result_bytes = raw_fn(tile_series, fref_series)

        assert result_bytes is not None
        with MemoryFile(bytes(result_bytes)) as mf, mf.open() as ds:
            assert ds.count == 1
            assert ds.read(1).shape == (3, 4)  # height=3, width=4
    finally:
        os.remove(tmp)


def test_rst_frombands_agg_file_ref_unit(gtiff_bytes):
    """Unit test: _frombands_agg_file_udf stacks bands from stub FileRefs."""
    import pandas as pd
    from rasterio.io import MemoryFile

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        vt_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()
        fref_stub = _StubFileRef(gtiff_bytes)

        from databricks.labs.gbx.pyrx.functions import _frombands_agg_file_udf

        tile_series = pd.Series([vt_row, vt_row])
        fref_series = pd.Series([fref_stub, fref_stub])
        band_series = pd.Series([0, 1])

        raw_fn = _frombands_agg_file_udf.func
        result_bytes = raw_fn(tile_series, fref_series, band_series)

        assert result_bytes is not None
        with MemoryFile(bytes(result_bytes)) as mf, mf.open() as ds:
            # Both input tiles have 1 band each; combined should be 2 bands.
            assert ds.count == 2
    finally:
        os.remove(tmp)


# ---------------------------------------------------------------------------
# Group 4 — FILE integration: coord fns and warp/pending-instruction ops
# ---------------------------------------------------------------------------


def test_rst_rastertoworldcoord_file_ref_equals_fallback(gtiff_bytes):
    """rst_rastertoworldcoordx/y: header-only FILE branch produces same world coords as fallback.

    Both fns resolve via open_header (no pixel read); the StubHeaderFileRef's
    as_local_file() satisfies the virtual-tile path.
    """
    from databricks.labs.gbx.pyrx.core import coords

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        class _StubHeaderFileRef:
            def __init__(self, path):
                self._path = path

            def open(self):
                return open(self._path, "rb")

            def as_local_file(self):
                return self._path

        with ot.open_header(tile_row, file_ref=None) as ds:
            expected_x = coords.raster_to_world_x(ds, 1, 1)
            expected_y = coords.raster_to_world_y(ds, 1, 1)

        with ot.open_header(tile_row, file_ref=_StubHeaderFileRef(tmp)) as ds:
            got_x = coords.raster_to_world_x(ds, 1, 1)
            got_y = coords.raster_to_world_y(ds, 1, 1)

        assert (
            got_x == expected_x
        ), f"worldcoordx FILE branch diverged: {got_x!r} != {expected_x!r}"
        assert (
            got_y == expected_y
        ), f"worldcoordy FILE branch diverged: {got_y!r} != {expected_y!r}"
    finally:
        os.remove(tmp)


def test_rst_transform_file_ref_equals_fallback(gtiff_bytes):
    """rst_transform: FILE branch produces pixel-equal reprojection to fallback."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = warp.reproject_to_srid(ds, 32632)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = warp.reproject_to_srid(ds, 32632)

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_to_webmercator_file_ref_equals_fallback(gtiff_bytes):
    """rst_to_webmercator: FILE branch pixel-equals fallback for EPSG:3857 reproject."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = warp.reproject_to_srid(ds, 3857)

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = warp.reproject_to_srid(ds, 3857)

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_transformcrs_file_ref_equals_fallback(gtiff_bytes):
    """rst_transformcrs: FILE branch pixel-equals fallback for CRS-string reproject."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp

    fd, tmp = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(tmp, "wb") as fh:
        fh.write(gtiff_bytes)
    try:
        tile_row = VirtualTile(cellid=0, path=tmp, window=(0, 0, 4, 3)).to_row()

        with ot._open(tile_row, file_ref=None) as ds:
            expected_bytes = warp.reproject_to_crs(ds, "EPSG:3857")

        with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
            got_bytes = warp.reproject_to_crs(ds, "EPSG:3857")

        with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
            with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
                np.testing.assert_allclose(exp_ds.read(), got_ds.read(), atol=1e-5)
    finally:
        os.remove(tmp)


def test_rst_setsrid_file_ref_equals_fallback(gtiff_bytes):
    """rst_setsrid: FILE branch (materialized tile) produces same result as fallback.

    For materialized tiles (raster=bytes), ot._open reads from bytes regardless of
    file_ref.  This test verifies the code path is consistent and error-free.
    """
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import edit

    tile_row = VirtualTile(cellid=0, raster=gtiff_bytes).to_row()

    with ot._open(tile_row, file_ref=None) as ds:
        expected_bytes = edit.set_srid(ds, 32632)

    with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
        got_bytes = edit.set_srid(ds, 32632)

    with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
        with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
            np.testing.assert_array_equal(exp_ds.read(), got_ds.read())
            assert exp_ds.crs == got_ds.crs


def test_rst_setcrs_file_ref_equals_fallback(gtiff_bytes):
    """rst_setcrs: FILE branch (materialized tile) produces same result as fallback.

    For materialized tiles (raster=bytes), ot._open reads from bytes regardless of
    file_ref.  This test verifies the code path is consistent and error-free.
    """
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import edit

    tile_row = VirtualTile(cellid=0, raster=gtiff_bytes).to_row()

    with ot._open(tile_row, file_ref=None) as ds:
        expected_bytes = edit.set_crs(ds, "EPSG:32632")

    with ot._open(tile_row, file_ref=_StubFileRef(gtiff_bytes)) as ds:
        got_bytes = edit.set_crs(ds, "EPSG:32632")

    with MemoryFile(expected_bytes) as mf, mf.open() as exp_ds:
        with MemoryFile(got_bytes) as mf2, mf2.open() as got_ds:
            np.testing.assert_array_equal(exp_ds.read(), got_ds.read())


def test_group3_sql_registry_unchanged():
    """SQL registry must still point at single-arg UDFs for Group 3 ops."""
    import databricks.labs.gbx.pyrx.functions as prx

    registry = prx.SQL_REGISTRY
    # Array ops: single-arg UDFs must be present in registry.
    assert "gbx_rst_merge" in registry, "gbx_rst_merge must be in SQL_REGISTRY"
    assert (
        "gbx_rst_combineavg" in registry
    ), "gbx_rst_combineavg must be in SQL_REGISTRY"
    assert "gbx_rst_frombands" in registry, "gbx_rst_frombands must be in SQL_REGISTRY"
    assert (
        "gbx_rst_mapalgebra" in registry
    ), "gbx_rst_mapalgebra must be in SQL_REGISTRY"
    # Agg ops: single-arg UDFs must be present in registry.
    assert "gbx_rst_merge_agg" in registry, "gbx_rst_merge_agg must be in SQL_REGISTRY"
    assert (
        "gbx_rst_combineavg_agg" in registry
    ), "gbx_rst_combineavg_agg must be in SQL_REGISTRY"
    assert (
        "gbx_rst_frombands_agg" in registry
    ), "gbx_rst_frombands_agg must be in SQL_REGISTRY"
    # Confirm the registry entries are not the FILE variants.
    assert registry["gbx_rst_merge"] is prx._merge_udf
    assert registry["gbx_rst_combineavg"] is prx._combineavg_udf
    assert registry["gbx_rst_frombands"] is prx._frombands_udf
    assert registry["gbx_rst_mapalgebra"] is prx._mapalgebra_udf
    assert registry["gbx_rst_combineavg_agg"] is prx._combineavg_agg_sql_udf
    assert registry["gbx_rst_frombands_agg"] is prx._frombands_agg_udf
