"""Task 8: reader layout conventions — path-ordered partition emission.

Verifies:
- Raster manifest/tilesTable path: partitions sorted by file_path.
- Raster default (os.walk) path: explicit sort produces path-ordered output
  even when the underlying walk delivers files in non-sorted order.
- Vector default path: partitions are path-ordered via sorted _members().
"""

from __future__ import annotations

import json
from unittest.mock import patch

from databricks.labs.gbx.ds.raster import RasterGbxReader, _TilePartition
from databricks.labs.gbx.ds.vector import VectorGbxReader

# ---------------------------------------------------------------------------
# Raster: path-ordered partition emission — manifest / tilesTable path
# ---------------------------------------------------------------------------


def test_raster_manifest_partitions_path_ordered(tmp_path):
    """Manifest rows with paths in reverse-alpha order → partitions sorted by file_path."""
    manifest = [
        {"path": "/z/z.tif", "window": [0, 0, 4, 4]},
        {"path": "/a/a.tif", "window": [0, 0, 4, 4]},
        {"path": "/m/m.tif", "window": [0, 0, 4, 4]},
    ]
    mf = str(tmp_path / "tiles.json")
    with open(mf, "w") as fh:
        json.dump(manifest, fh)

    r = RasterGbxReader({"path": "/x", "manifest": mf})
    parts = r.partitions()
    paths = [p.file_path for p in parts]
    assert paths == sorted(paths), f"Expected sorted by file_path, got {paths}"


def test_raster_manifest_multi_tile_per_file_grouped(tmp_path):
    """Multiple tiles per file in manifest → tiles from same file are adjacent after sort."""
    manifest = [
        {"path": "/z/z.tif", "window": [0, 0, 4, 4]},
        {"path": "/a/a.tif", "window": [0, 0, 4, 4]},
        {
            "path": "/z/z.tif",
            "window": [4, 0, 4, 4],
        },  # second tile of z.tif, listed last
        {"path": "/a/a.tif", "window": [4, 0, 4, 4]},
    ]
    mf = str(tmp_path / "tiles.json")
    with open(mf, "w") as fh:
        json.dump(manifest, fh)

    r = RasterGbxReader({"path": "/x", "manifest": mf})
    parts = r.partitions()
    paths = [p.file_path for p in parts]
    # After sort: a.tif, a.tif, z.tif, z.tif — same-source tiles are adjacent
    assert paths == sorted(paths), f"Expected sorted by file_path, got {paths}"


# ---------------------------------------------------------------------------
# Raster: path-ordered partition emission — default (os.walk) path
# ---------------------------------------------------------------------------


def test_raster_default_path_partitions_path_ordered():
    """Default os.walk path: even when list_files returns paths in non-sorted order the
    explicit sort in partitions() ensures output is ordered by file_path."""
    with (
        patch("databricks.labs.gbx.ds.file_gbx.list_local_files") as mock_lf,
        patch("databricks.labs.gbx.ds.raster._plan_partitions_for_file") as mock_ppf,
    ):
        # Deliberately non-sorted input to prove the explicit sort does the work.
        mock_lf.return_value = ["/z/z.tif", "/a/a.tif", "/m/m.tif"]
        mock_ppf.side_effect = lambda file_path, **kw: [
            _TilePartition(file_path=file_path, window=(0, 0, 4, 4))
        ]
        r = RasterGbxReader({"path": "/some/dir"})
        parts = r.partitions()
    paths = [p.file_path for p in parts]
    assert paths == sorted(paths), f"Expected sorted by file_path, got {paths}"


def test_raster_default_path_multi_tile_per_file_grouped():
    """Multiple tiles per file: explicit sort groups all tiles of a file together."""
    # Simulate two files where tiles are interleaved (as if two files produced tiles
    # in alternating order — unlikely in practice but the sort must handle it).
    tiles = {
        "/z/z.tif": [(0, 0, 4, 4), (4, 0, 4, 4)],
        "/a/a.tif": [(0, 0, 4, 4), (4, 0, 4, 4)],
    }

    with (
        patch("databricks.labs.gbx.ds.file_gbx.list_local_files") as mock_lf,
        patch("databricks.labs.gbx.ds.raster._plan_partitions_for_file") as mock_ppf,
    ):
        mock_lf.return_value = ["/z/z.tif", "/a/a.tif"]  # non-sorted
        mock_ppf.side_effect = lambda file_path, **kw: [
            _TilePartition(file_path=file_path, window=w) for w in tiles[file_path]
        ]
        r = RasterGbxReader({"path": "/some/dir"})
        parts = r.partitions()

    paths = [p.file_path for p in parts]
    # /a/a.tif tiles first, then /z/z.tif tiles
    assert paths == sorted(paths), f"Expected sorted by file_path, got {paths}"


# ---------------------------------------------------------------------------
# Vector: path-ordered partition emission
# ---------------------------------------------------------------------------


def test_vector_default_partitions_path_ordered(tmp_path):
    """Multiple .geojson files → partitions are emitted in sorted path order."""
    geojson_content = json.dumps({"type": "FeatureCollection", "features": []})
    # Write in deliberately non-alpha order; _members() must sort
    for name in ("z_file.geojson", "a_file.geojson", "m_file.geojson"):
        (tmp_path / name).write_text(geojson_content)

    r = VectorGbxReader({"path": str(tmp_path), "driverName": "GeoJSON"})
    parts = r.partitions()
    paths = [p.path for p in parts]
    assert paths == sorted(paths), f"Expected sorted paths, got {paths}"
