"""Tests for PMTiles adaptive naming (Task D1).

Covers:
- _complete_ext with ext="" (directory unit: returns name unchanged).
- _resolve_single_file_output with ext="" (directory unit): all 3 cases.
- PMTilesGbxWriter.path resolution in both modes (pure-unit, no Spark).
- End-to-end round-trip (single-archive + sharded) asserting output lands at the
  resolved path with the correct structure.
- Cross-writer wrong-ext rejection: .gpkg fileName on pmtiles single-archive.
"""

import json
import os

import pytest
from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.ds.pmtiles import PMTilesGbxWriter
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import _complete_ext, _resolve_single_file_output

# ---------------------------------------------------------------------------
# Pure-unit: _complete_ext with ext=""
# ---------------------------------------------------------------------------


def test_complete_ext_empty_returns_unchanged():
    """_complete_ext(name, '') returns name unchanged (directory unit)."""
    assert _complete_ext("tiles", "") == "tiles"
    assert _complete_ext("my_tileset", "") == "my_tileset"
    assert _complete_ext("roads.gpkg", "") == "roads.gpkg"  # no wrong-ext rejection


# ---------------------------------------------------------------------------
# Pure-unit: _resolve_single_file_output with ext="" (directory unit)
# ---------------------------------------------------------------------------


def test_dir_unit_case1_filename_given(tmp_path):
    """ext='': fileName given -> <path>/<fileName> (no extension appended)."""
    parent = str(tmp_path / "newdir")
    out = _resolve_single_file_output(parent, "my_tiles", "")
    assert out == str(tmp_path / "newdir" / "my_tiles")
    assert os.path.isdir(tmp_path / "newdir")  # parent created


def test_dir_unit_case2_existing_dir(tmp_path):
    """ext='': existing dir -> <path>/<basename(path)> (named after dir, under it)."""
    d = tmp_path / "tileset_dir"
    d.mkdir()
    out = _resolve_single_file_output(str(d), None, "")
    assert out == str(d / "tileset_dir")


def test_dir_unit_case3_stem_path(tmp_path):
    """ext='': file-like (non-existing) path -> path unchanged, parent created."""
    stem = str(tmp_path / "sub" / "my_tileset")
    out = _resolve_single_file_output(stem, None, "")
    assert out == stem
    assert os.path.isdir(tmp_path / "sub")  # parent created


def test_dir_unit_no_wrong_ext_rejection(tmp_path):
    """ext='': names with recognized geo extensions are NOT rejected (directory unit)."""
    # This would raise ValueError for non-empty ext; for "" it must not.
    out = _resolve_single_file_output(str(tmp_path), "roads.gpkg", "")
    assert out == str(tmp_path / "roads.gpkg")


# ---------------------------------------------------------------------------
# Pure-unit: PMTilesGbxWriter.path resolution (no Spark, no actual write)
# ---------------------------------------------------------------------------


def test_pmtiles_writer_single_archive_stem(tmp_path):
    """shardZoom=0 + stem path -> self.path ends with .pmtiles."""
    w = PMTilesGbxWriter(
        str(tmp_path / "world"),
        {"shardzoom": "0"},
        overwrite=True,
    )
    assert w.path == str(tmp_path / "world.pmtiles")
    assert w.path.endswith(".pmtiles")


def test_pmtiles_writer_single_archive_filename_option(tmp_path):
    """shardZoom=0 + fileName='tiles' -> <dir>/tiles.pmtiles."""
    out_dir = str(tmp_path / "outdir")
    w = PMTilesGbxWriter(
        out_dir,
        {"shardzoom": "0", "filename": "tiles"},
        overwrite=True,
    )
    assert w.path == str(tmp_path / "outdir" / "tiles.pmtiles")
    assert os.path.isdir(tmp_path / "outdir")


def test_pmtiles_writer_single_archive_existing_dir(tmp_path):
    """shardZoom=0 + existing dir -> <dir>/<dirname>.pmtiles."""
    d = tmp_path / "world_dir"
    d.mkdir()
    w = PMTilesGbxWriter(
        str(d),
        {"shardzoom": "0"},
        overwrite=True,
    )
    assert w.path == str(d / "world_dir.pmtiles")


def test_pmtiles_writer_sharded_stem(tmp_path):
    """shardZoom=6 (default) + stem path -> self.path is the stem (no .pmtiles)."""
    stem = str(tmp_path / "my_tileset")
    w = PMTilesGbxWriter(
        stem,
        {},  # default shardZoom=6
        overwrite=True,
    )
    assert w.path == stem
    assert not w.path.endswith(".pmtiles")


def test_pmtiles_writer_sharded_filename_option(tmp_path):
    """shardZoom=6 + fileName='nation' -> <path>/nation (directory, no ext)."""
    out_dir = str(tmp_path / "outdir")
    w = PMTilesGbxWriter(
        out_dir,
        {"filename": "nation"},
        overwrite=True,
    )
    assert w.path == str(tmp_path / "outdir" / "nation")


def test_pmtiles_writer_sharded_existing_dir(tmp_path):
    """shardZoom=6 + existing dir (no fileName) -> the save dir IS the output root
    (tileset/ lives directly under it); NOT nested under <dirname> (that single-file
    naming-after-dir behavior produced /out/out/tileset for sharded output)."""
    d = tmp_path / "tileset_dir"
    d.mkdir()
    w = PMTilesGbxWriter(
        str(d),
        {},
        overwrite=True,
    )
    assert w.path == str(d)


# ---------------------------------------------------------------------------
# Cross-writer wrong-ext rejection
# ---------------------------------------------------------------------------


def test_pmtiles_single_archive_rejects_gpkg_filename(tmp_path):
    """.gpkg fileName on single-archive pmtiles -> ValueError (wrong geo ext)."""
    with pytest.raises(ValueError, match="expected .pmtiles"):
        PMTilesGbxWriter(
            str(tmp_path),
            {"shardzoom": "0", "filename": "roads.gpkg"},
            overwrite=True,
        )


def test_pmtiles_single_archive_rejects_geojson_filename(tmp_path):
    """.geojson fileName on single-archive pmtiles -> ValueError (wrong geo ext)."""
    with pytest.raises(ValueError, match="expected .pmtiles"):
        PMTilesGbxWriter(
            str(tmp_path),
            {"shardzoom": "0", "filename": "roads.geojson"},
            overwrite=True,
        )


# ---------------------------------------------------------------------------
# End-to-end round-trips (require SparkSession + pmtiles package)
# ---------------------------------------------------------------------------

PNG = b"\x89PNG\r\n\x1a\n"


def _png(tag: int) -> bytes:
    return PNG + bytes([tag])


def _rows(spark, tiles):
    data = [(z, x, y, bytearray(_png(i))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(data, schema="z int, x int, y int, bytes binary")


def _read_tile(path, z, x, y):
    with open(path, "rb") as f:
        return Reader(MmapSource(f)).get(z, x, y)


TILES = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]


def test_single_archive_stem_naming(spark, tmp_path):
    """Single-archive: stem path -> <stem>.pmtiles; tile reads back."""
    register(spark)
    stem = str(tmp_path / "world")
    expected = str(tmp_path / "world.pmtiles")
    _rows(spark, TILES).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(stem)
    assert os.path.isfile(expected), f"Expected {expected}"
    assert _read_tile(expected, 6, 32, 21) is not None


def test_single_archive_filename_option(spark, tmp_path):
    """Single-archive: fileName='tiles' -> <dir>/tiles.pmtiles; tile reads back."""
    register(spark)
    out_dir = str(tmp_path / "out")
    expected = str(tmp_path / "out" / "tiles.pmtiles")
    (
        _rows(spark, TILES)
        .write.format("pmtiles_gbx")
        .mode("overwrite")
        .option("shardZoom", "0")
        .option("fileName", "tiles")
        .save(out_dir)
    )
    assert os.path.isfile(expected), f"Expected {expected}"
    assert _read_tile(expected, 6, 32, 21) is not None


def test_single_archive_existing_dir_naming(spark, tmp_path):
    """Single-archive: existing dir -> <dir>/<dirname>.pmtiles; tile reads back."""
    register(spark)
    d = tmp_path / "world_dir"
    d.mkdir()
    expected = str(d / "world_dir.pmtiles")
    _rows(spark, TILES).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(str(d))
    assert os.path.isfile(expected), f"Expected {expected}"
    assert _read_tile(expected, 6, 32, 21) is not None


def test_sharded_stem_naming(spark, tmp_path):
    """Sharded: stem path -> tileset under stem (no .pmtiles suffix on dir)."""
    register(spark)
    stem = str(tmp_path / "my_tileset")
    _rows(spark, TILES).write.format("pmtiles_gbx").mode("overwrite").save(stem)
    tileset = os.path.join(stem, "tileset")
    assert os.path.isdir(tileset), f"Expected tileset dir at {tileset}"
    assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))


def test_sharded_filename_option(spark, tmp_path):
    """Sharded: fileName='nation' -> <path>/nation/tileset/..."""
    register(spark)
    out_dir = str(tmp_path / "out")
    expected_dir = str(tmp_path / "out" / "nation")
    (
        _rows(spark, TILES)
        .write.format("pmtiles_gbx")
        .mode("overwrite")
        .option("fileName", "nation")
        .save(out_dir)
    )
    tileset = os.path.join(expected_dir, "tileset")
    assert os.path.isdir(tileset), f"Expected tileset dir at {tileset}"
    assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))


def test_sharded_existing_dir_naming(spark, tmp_path):
    """Sharded: existing dir -> <dir>/tileset/... (the save dir is the output root;
    no basename nesting)."""
    register(spark)
    d = tmp_path / "tileset_dir"
    d.mkdir()
    expected_dir = str(d)
    _rows(spark, TILES).write.format("pmtiles_gbx").mode("overwrite").save(str(d))
    tileset = os.path.join(expected_dir, "tileset")
    assert os.path.isdir(tileset), f"Expected tileset dir at {tileset}"
    catalog = json.load(open(os.path.join(tileset, "catalog.json")))
    assert catalog["type"] == "FeatureCollection"
