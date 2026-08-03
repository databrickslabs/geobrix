"""Unit tests for windowed GTiff re-encode + metadata."""

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds import _encode

EXPECTED_METADATA_KEYS = {
    "path",
    "sourcePath",
    "driver",
    "format",
    "last_command",
    "last_error",
    "all_parents",
    "size",
    "compression",
    "isZipped",
    "isSubset",
}


def test_encode_tile_roundtrips_pixels(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        cellid, raster_bytes, meta = _encode.encode_tile(
            ds, window=(0, 0, 4, 3), source_path="/data/sample.tif", all_parents=""
        )
    assert cellid == -1
    with MemoryFile(raster_bytes) as mf2, mf2.open() as ds2:
        assert ds2.count == 1
        assert (ds2.width, ds2.height) == (4, 3)
        out = ds2.read(1)
    expected = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_allclose(out, expected, rtol=1e-6)


def test_encode_metadata_key_set(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        _cellid, _b, meta = _encode.encode_tile(
            ds, window=(0, 0, 4, 3), source_path="/data/sample.tif", all_parents=""
        )
    assert EXPECTED_METADATA_KEYS <= set(meta.keys())
    assert meta["driver"] == "GTiff"
    assert meta["format"] == "GTiff"
    assert meta["compression"] == "auto"
    assert meta["isZipped"] == "false"
    assert meta["isSubset"] == "false"
    assert meta["last_error"] == ""
    assert meta["sourcePath"] == "/data/sample.tif"
    assert meta["last_command"] == "windowed_extract -srcwin 0 0 4 3"


def test_encode_subwindow_reads_only_that_window(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        _c, raster_bytes, _m = _encode.encode_tile(
            ds, window=(2, 0, 2, 3), source_path="/data/sample.tif", all_parents=""
        )
    with MemoryFile(raster_bytes) as mf2, mf2.open() as ds2:
        assert (ds2.width, ds2.height) == (2, 3)
        out = ds2.read(1)
    full = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_allclose(out, full[:, 2:4], rtol=1e-6)
