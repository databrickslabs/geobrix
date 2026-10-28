"""Offline tests for the driver-side PMTiles inspector (pmtiles_info)."""

import io

import pytest
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16  # sniffs as PNG
_MVT = b"mvt-payload\x00\x01\x02"  # non-magic bytes => MVT


def _build_archive(tiles, tile_type, *, name="demo"):
    """tiles: list of (z, x, y, payload). Returns PMTiles v3 bytes over SF.

    Note: payloads must be distinct per tile or the writer deduplicates them
    (tile_contents_count < len(tiles)), causing max_zoom to collapse to the
    lowest zoom present.
    """
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(
        tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])
    ):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


@pytest.fixture
def raster_pmtiles():
    # Use distinct payloads per tile so the writer does not deduplicate them;
    # identical bytes collapse to one tile-content entry and max_zoom = min(zs).
    _PNG1 = _PNG[:-1] + b"\x01"
    return _build_archive([(0, 0, 0, _PNG), (1, 0, 0, _PNG1)], TileType.PNG)


@pytest.fixture
def vector_pmtiles():
    return _build_archive([(10, 163, 395, _MVT)], TileType.MVT, name="bldgs")


def test_info_from_bytes_raster(raster_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    info = pmtiles_info(raster_pmtiles)
    assert info["tile_type"] == "png"
    assert info["min_zoom"] == 0
    assert info["max_zoom"] == 1
    assert info["tile_count"] == 2
    minlon, minlat, maxlon, maxlat = info["bounds"]
    assert -122.6 < minlon < maxlon < -122.3
    assert 37.6 < minlat < maxlat < 37.9
    assert info["metadata"].get("name") == "demo"
    assert info["tile_compression"] == "none"


def test_info_from_bytes_vector(vector_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    info = pmtiles_info(vector_pmtiles)
    assert info["tile_type"] == "mvt"
    assert info["min_zoom"] == info["max_zoom"] == 10
    assert info["tile_count"] == 1
    assert info["metadata"].get("name") == "bldgs"


def test_info_from_path(raster_pmtiles, tmp_path):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    p = tmp_path / "r.pmtiles"
    p.write_bytes(raster_pmtiles)
    info = pmtiles_info(str(p))
    assert info["tile_type"] == "png"
    assert info["tile_count"] == 2


def test_info_strips_dbfs_scheme(raster_pmtiles, tmp_path):
    # Databricks Volume paths often arrive scheme-qualified; the bare FUSE path
    # is what the reader opens. Strip dbfs:/file: like plot_file does.
    from databricks.labs.gbx.pmtiles import pmtiles_info

    p = tmp_path / "r.pmtiles"
    p.write_bytes(raster_pmtiles)
    info = pmtiles_info("dbfs:" + str(p))
    assert info["tile_count"] == 2


def test_center_tuple(vector_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    lon, lat, zoom = pmtiles_info(vector_pmtiles)["center"]
    assert -122.6 < lon < -122.3 and 37.6 < lat < 37.9 and zoom == 10


def test_info_reads_the_uncompressed_directories_the_heavy_writer_stores(
    raster_pmtiles,
):
    """An archive whose directories and metadata are stored uncompressed
    (header internal_compression NONE) reports the same info as its gzip form."""
    from test.pmtiles_light._archives import uncompressed_internals

    from databricks.labs.gbx.pmtiles import pmtiles_info

    plain = uncompressed_internals(raster_pmtiles)
    assert plain != raster_pmtiles
    assert pmtiles_info(plain) == pmtiles_info(raster_pmtiles)
