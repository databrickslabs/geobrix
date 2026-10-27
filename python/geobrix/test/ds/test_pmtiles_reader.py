"""Integration tests for the pmtiles_gbx raster reader (source="raster").

Task 2: per-tile mosaic pyramid reader — enumerate tiles, fan out into
InputPartitions, render via _xyz_mosaic core, emit (z, x, y, bytes) rows.
"""

import io
import os

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_bounds

from databricks.labs.gbx.ds._pmtiles_read import PMtilesRasterReader
from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource


def _write_cog(path, w, s, e, n, px=256, val=200):
    data = np.full((3, px, px), val, dtype="uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=px,
        height=px,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(w, s, e, n, px, px),
    ) as ds:
        ds.write(data)


def test_raster_reader_schema_and_fanout(spark, tmp_path):
    # two adjacent quads over a small AOI
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "-122.50,37.74,-122.40,37.79")
        .option("minZoom", "14")
        .option("maxZoom", "16")
        .option("tilesPerPartition", "20")
        .load()
    )
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
    rows = df.collect()
    assert len(rows) > 0
    assert df.rdd.getNumPartitions() >= 2  # fans out via InputPartitions
    # no (z,x,y) duplicates (each tile produced once)
    keys = [(r["z"], r["x"], r["y"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_raster_reader_composites_seam_tile(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    rows = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "-122.50,37.74,-122.40,37.79")
        .option("minZoom", "16")
        .option("maxZoom", "16")
        .load()
        .collect()
    )
    # find a fully-covered tile; assert it carries BOTH source values (composited)
    both = 0
    for r in rows:
        arr = np.asarray(Image.open(io.BytesIO(bytes(r["bytes"]))).convert("RGBA"))
        if float(np.mean(arr[:, :, 3] == 255)) > 0.99:
            lo = (arr[:, :, 0] > 90) & (arr[:, :, 0] < 150)
            hi = arr[:, :, 0] > 190
            if lo.any() and hi.any():
                both += 1
    assert both >= 1, "no tile composited both quads -> the western-quad bug"


def test_raster_reader_bbox_defaults_to_source_union(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("minZoom", "15")
        .option("maxZoom", "15")
        .load()
    )  # no bbox
    assert len(df.collect()) > 0


def _png_bytes(i):
    import io

    import numpy as np
    from PIL import Image

    a = np.full((256, 256, 3), 40 + i * 20, dtype="uint8")
    buf = io.BytesIO()
    Image.fromarray(a).save(buf, format="PNG")
    return buf.getvalue()


def test_archive_reader_roundtrip(spark, tmp_path):
    from pyspark.sql import Row

    spark.dataSource.register(PMTilesGbxDataSource)
    # build a small archive via the writer
    tiles = [Row(z=14, x=2615 + i, y=6330, bytes=_png_bytes(i)) for i in range(3)]
    out = str(tmp_path / "rt.pmtiles")
    spark.createDataFrame(tiles).write.format("pmtiles_gbx").option(
        "shardZoom", "0"
    ).mode("overwrite").save(out)
    # read it back
    back = (
        spark.read.format("pmtiles_gbx")
        .option("source", "archive")
        .option("path", out)
        .load()
        .collect()
    )
    got = {(r["z"], r["x"], r["y"]): bytes(r["bytes"]) for r in back}
    # both the (z,x,y) set AND the tile bytes must round-trip identically
    assert got == {(14, 2615 + i, 6330): _png_bytes(i) for i in range(3)}


def test_raster_reader_rejects_bad_pixel_selection(tmp_path):
    with pytest.raises(ValueError):
        PMtilesRasterReader({"path": str(tmp_path), "pixelSelection": "max"})


def test_reader_rejects_unknown_source(spark, tmp_path):
    spark.dataSource.register(PMTilesGbxDataSource)
    with pytest.raises(Exception):
        spark.read.format("pmtiles_gbx").option("source", "bogus").option(
            "path", str(tmp_path)
        ).load().collect()


def test_raster_reader_bbox_outside_sources_is_empty(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "10,50,10.1,50.1")
        .option("minZoom", "15")
        .option("maxZoom", "15")
        .load()
    )
    assert df.collect() == []
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]


# ---------------------------------------------------------------------------
# Archive reads at scale: range reads, bounded bytes per partition
# ---------------------------------------------------------------------------

_BIG = "big.pmtiles"
_TPP = 1500  # tiles per partition for the multi-partition archive tests


def _write_multi_partition_archive(path, n=12000, seed=7):
    """A PMTiles archive with leaf directories, run-length entries and
    deduplicated (back-referenced) tiles. Sparse random tile ids keep the
    directory from compressing into the root, so the writer emits leaves; the
    tile data (~300 bytes each) dominates the file."""
    import random

    from pmtiles.tile import Compression, TileType, zxy_to_tileid
    from pmtiles.writer import Writer

    rng = random.Random(seed)
    cur = zxy_to_tileid(12, 0, 0)
    tiles = []
    while len(tiles) < n:
        cur += rng.randint(2, 1500)
        if len(tiles) in (_TPP - 4, 3 * _TPP - 2, 6000):
            # identical consecutive tiles -> one run-length entry that straddles
            # a partition boundary
            data = rng.randbytes(300)
            for _ in range(9):
                tiles.append((cur, data))
                cur += 1
            continue
        if len(tiles) > 100 and len(tiles) % 97 == 0:
            data = tiles[len(tiles) - 50][1]  # same bytes as an earlier tile
        else:
            data = rng.randbytes(250 + rng.randint(0, 100))
        tiles.append((cur, data))
    with open(path, "wb") as fh:
        w = Writer(fh)
        for tid, data in tiles:
            w.write_tile(tid, data)
        w.finalize(
            {"tile_type": TileType.PNG, "tile_compression": Compression.NONE}, {}
        )


def _archive_header(path):
    from pmtiles.tile import deserialize_header

    with open(path, "rb") as fh:
        return deserialize_header(fh.read(127))


def _expected_archive_rows(path):
    """Every (z, x, y, bytes) of an archive, in tile-id order, read by the
    pmtiles library's own traversal over the whole file in memory."""
    from pmtiles.reader import MemorySource, all_tiles

    with open(path, "rb") as fh:
        raw = fh.read()
    return [(z, x, y, bytes(d)) for (z, x, y), d in all_tiles(MemorySource(raw))]


class _CountingFile:
    """File-object proxy that tallies the bytes returned by read()/readinto()."""

    def __init__(self, fh, key, tally):
        self._fh, self._key, self._tally = fh, key, tally

    def read(self, *args):
        data = self._fh.read(*args)
        self._tally[self._key] += len(data)
        return data

    def readinto(self, buf):
        n = self._fh.readinto(buf)
        self._tally[self._key] += n or 0
        return n

    def __getattr__(self, name):
        return getattr(self._fh, name)

    def __iter__(self):
        return iter(self._fh)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._fh.close()


def _spy_reads(monkeypatch, name_part):
    """Count bytes read through ``open()`` from every file whose path contains
    ``name_part`` (the source and any staged copy of it), keyed by path."""
    import builtins
    import collections

    tally = collections.Counter()
    real_open = builtins.open

    def spy_open(file, mode="r", *args, **kwargs):
        fh = real_open(file, mode, *args, **kwargs)
        if "r" in mode and name_part in os.fspath(file):
            return _CountingFile(fh, os.fspath(file), tally)
        return fh

    monkeypatch.setattr(builtins, "open", spy_open)
    return tally


def test_archive_reader_multi_partition_output_matches_library(tmp_path):
    """Every partition's rows, concatenated, equal the library's full traversal:
    same tiles, same order, same bytes -- across leaf directories, run-length
    entries split by a partition boundary and deduplicated tiles. A pickled
    partition (the read_paths planning path) reads the same."""
    import pickle

    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    p = str(tmp_path / _BIG)
    _write_multi_partition_archive(p)
    hdr = _archive_header(p)
    assert hdr["leaf_directory_length"] > 0  # leaves exercised
    assert hdr["addressed_tiles_count"] > hdr["tile_entries_count"]  # runs
    assert hdr["tile_entries_count"] > hdr["tile_contents_count"]  # dedup
    expected = _expected_archive_rows(p)

    reader = PMtilesArchiveReader({"path": p, "tilesPerPartition": str(_TPP)})
    parts = reader.partitions()
    assert len(parts) == -(-len(expected) // _TPP)
    assert [t for part in parts for t in part.tiles] == [e[:3] for e in expected]
    got = [row for part in parts for row in reader.read(part)]
    assert got == expected
    again = list(reader.read(pickle.loads(pickle.dumps(parts[1]))))
    assert again == expected[_TPP : 2 * _TPP]


def test_archive_reader_reads_bounded_bytes_per_partition(tmp_path, monkeypatch):
    """Planning reads only the header and directories; the archive is copied to
    worker-local disk once; each partition then reads only the header, the
    directories and its own tiles -- never the whole archive per partition."""
    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    p = str(tmp_path / _BIG)
    _write_multi_partition_archive(p)
    size = os.path.getsize(p)
    hdr = _archive_header(p)
    dir_bytes = 127 + hdr["root_length"] + hdr["leaf_directory_length"]

    tally = _spy_reads(monkeypatch, _BIG)
    reader = PMtilesArchiveReader({"path": p, "tilesPerPartition": str(_TPP)})
    parts = reader.partitions()
    planning = tally[p]
    per_part = []  # (bytes read outside the source, bytes of the partition's tiles)
    for part in parts:
        before = dict(tally)
        rows = list(reader.read(part))
        other = sum(v - before.get(k, 0) for k, v in tally.items() if k != p)
        per_part.append((other, sum(len(r[3]) for r in rows)))
    source_total = tally[p]
    summary = (
        f"archive={size} B, dirs={dir_bytes} B, partitions={len(parts)}, "
        f"planning read {planning} B, source read {source_total} B in total, "
        f"per-partition local reads {[o for o, _ in per_part]}"
    )
    assert len(parts) >= 4, summary
    assert planning <= dir_bytes, summary
    assert source_total <= size + dir_bytes, summary
    for other, tiles in per_part:
        assert other <= dir_bytes + tiles, summary


def test_archive_reader_restages_an_archive_replaced_in_place(tmp_path):
    """Wiring: the staged copy is checked against the source's size + mtime, so
    an archive rewritten at the same path is read fresh in the same process."""
    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    p = str(tmp_path / "swap.pmtiles")

    def read_all():
        r = PMtilesArchiveReader({"path": p, "tilesPerPartition": "500"})
        return [row for part in r.partitions() for row in r.read(part)]

    _write_multi_partition_archive(p, n=1200, seed=1)
    assert read_all() == _expected_archive_rows(p)
    st = os.stat(p)
    _write_multi_partition_archive(p, n=1200, seed=2)
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
    assert read_all() == _expected_archive_rows(p)


def test_archive_reader_spark_multi_partition(spark, tmp_path):
    """End to end on Spark workers: a multi-partition archive with leaf
    directories reads back every tile with its exact bytes."""
    spark.dataSource.register(PMTilesGbxDataSource)
    p = str(tmp_path / _BIG)
    _write_multi_partition_archive(p)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "archive")
        .option("path", p)
        .option("tilesPerPartition", str(_TPP))
        .load()
    )
    assert df.rdd.getNumPartitions() >= 4
    got = sorted((r["z"], r["x"], r["y"], bytes(r["bytes"])) for r in df.collect())
    assert got == sorted(_expected_archive_rows(p))


# ---------------------------------------------------------------------------
# Raster source mode: each source staged once per worker
# ---------------------------------------------------------------------------


def test_raster_reader_stages_each_source_once(tmp_path, monkeypatch):
    """Sources are copied to worker-local disk once per worker process and
    shared by every partition (not re-copied per partition); the rendered tiles
    equal rendering each tile straight from the sources."""
    from databricks.labs.gbx.ds import _xyz_mosaic

    src = tmp_path / "src"
    src.mkdir()
    _write_cog(str(src / "qa.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(src / "qb.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    sources = sorted(str(f) for f in src.iterdir())
    reader = PMtilesRasterReader(
        {
            "path": str(src),
            "bbox": "-122.50,37.74,-122.40,37.79",
            "minZoom": "13",
            "maxZoom": "14",
            "tilesPerPartition": "3",
        }
    )
    parts = reader.partitions()
    assert len(parts) >= 3
    assert sum(len(part.sources) for part in parts) > len(sources)

    tally = _spy_reads(monkeypatch, str(src))
    got = [row for part in parts for row in reader.read(part)]
    reads = {k: v for k, v in tally.items() if k in sources}
    assert reads == {s: os.path.getsize(s) for s in sources}, dict(tally)
    monkeypatch.undo()

    expected = []
    for part in parts:
        for z, x, y in part.tiles:
            png = _xyz_mosaic.render_tile(z, x, y, part.sources)
            if png is not None:
                expected.append((z, x, y, png))
    assert got == expected


def test_raster_reader_renders_rgba_source_from_rgb(tmp_path):
    """Wiring: an RGBA source whose alpha reads 0 still renders opaque tiles
    (the staged copy is render-ready RGB)."""
    from rasterio.enums import ColorInterp

    p = str(tmp_path / "rgba.tif")
    data = np.full((4, 256, 256), 200, dtype="uint8")
    data[3] = 0
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        width=256,
        height=256,
        count=4,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(-122.50, 37.74, -122.40, 37.80, 256, 256),
    ) as ds:
        ds.write(data)
        ds.colorinterp = [
            ColorInterp.red,
            ColorInterp.green,
            ColorInterp.blue,
            ColorInterp.alpha,
        ]
    reader = PMtilesRasterReader(
        {
            "path": str(tmp_path),
            "bbox": "-122.48,37.76,-122.42,37.78",
            "minZoom": "14",
            "maxZoom": "14",
        }
    )
    rows = [row for part in reader.partitions() for row in reader.read(part)]
    assert rows
    alpha = [
        np.asarray(Image.open(io.BytesIO(r[3])).convert("RGBA"))[:, :, 3] for r in rows
    ]
    assert max(float(np.mean(a == 255)) for a in alpha) > 0.99


def test_pmtiles_range_reads_stop_at_the_end_of_the_file(tmp_path):
    """A garbled header or directory entry can hold any length: the read is cut
    at the end of the file instead of allocating it."""
    from databricks.labs.gbx.ds._pmtiles_read import _range_source

    path = tmp_path / "f.bin"
    path.write_bytes(b"0123456789")
    with open(path, "rb") as fh:
        get = _range_source(fh)
        assert get(4, 0x5A5A5A5A5A5A) == b"456789"
        assert get(20, 5) == b"" and get(-1, 5) == b""


def test_archive_with_a_garbled_root_length_fails_cleanly(tmp_path):
    """Planning reads the root directory by the length the header declares; a
    garbled length (here ~99 TB) fails the read with an error, not a giant
    allocation that can take the worker down."""
    import struct

    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    path = str(tmp_path / "a.pmtiles")
    _write_multi_partition_archive(path, n=50)
    with open(path, "r+b") as fh:
        fh.seek(16)  # v3 header: magic + version (8 bytes), root offset, root length
        fh.write(struct.pack("<Q", 0x5A5A5A5A5A5A))
    with pytest.raises(Exception) as info:
        PMtilesArchiveReader({"path": path}).partitions()
    assert not isinstance(info.value, MemoryError)


# ---------------------------------------------------------------------------
# Uncompressed internal directories (header internal_compression = NONE)
# ---------------------------------------------------------------------------


def _with_uncompressed_internals(src, dst):
    """Rewrite a library-written PMTiles archive (gzip root, leaves, metadata)
    so its root directory, leaf directories and metadata are stored
    uncompressed and the header declares ``internal_compression = NONE`` -- the
    layout the heavyweight writer produces. The tile data is copied unchanged;
    offsets and lengths are recomputed in the writer's section order (header,
    root, metadata, leaves, tile data)."""
    import gzip

    from pmtiles.tile import (
        Compression,
        Entry,
        deserialize_directory,
        deserialize_header,
        serialize_directory,
        serialize_header,
    )

    with open(src, "rb") as fh:
        raw = fh.read()
    h = deserialize_header(raw[:127])
    assert h["internal_compression"] == Compression.GZIP

    def section(offset, length):
        return raw[offset : offset + length]

    def plain(entries):
        # serialize_directory gzips; the archive wants the bare varint stream
        return gzip.decompress(serialize_directory(entries))

    leaves = b""
    root_entries = []
    for e in deserialize_directory(section(h["root_offset"], h["root_length"])):
        if e.run_length > 0:  # tile entry in the root: kept as is
            root_entries.append(e)
            continue
        leaf = plain(
            deserialize_directory(
                section(h["leaf_directory_offset"] + e.offset, e.length)
            )
        )
        root_entries.append(Entry(e.tile_id, len(leaves), len(leaf), 0))
        leaves += leaf
    root = plain(root_entries)
    metadata = gzip.decompress(section(h["metadata_offset"], h["metadata_length"]))
    tiles = section(h["tile_data_offset"], h["tile_data_length"])

    h = dict(h, internal_compression=Compression.NONE)
    h["root_offset"] = 127
    h["root_length"] = len(root)
    h["metadata_offset"] = h["root_offset"] + len(root)
    h["metadata_length"] = len(metadata)
    h["leaf_directory_offset"] = h["metadata_offset"] + len(metadata)
    h["leaf_directory_length"] = len(leaves)
    h["tile_data_offset"] = h["leaf_directory_offset"] + len(leaves)
    with open(dst, "wb") as fh:
        fh.write(serialize_header(h) + root + metadata + leaves + tiles)


def test_uncompressed_variant_is_what_the_heavy_writer_stores(tmp_path):
    """Guard on the fixture itself: the rewritten archive declares NONE and its
    root directory is not a gzip stream, so a reader that gunzips fails on it."""
    import gzip

    gz, plain = str(tmp_path / "gz.pmtiles"), str(tmp_path / "plain.pmtiles")
    _write_multi_partition_archive(gz, n=50)
    _with_uncompressed_internals(gz, plain)
    h = _archive_header(plain)
    with open(plain, "rb") as fh:
        root = fh.read()[h["root_offset"] : h["root_offset"] + h["root_length"]]
    assert h["internal_compression"].name == "NONE"
    with pytest.raises(gzip.BadGzipFile):
        gzip.decompress(root)


def test_archive_reader_spark_reads_uncompressed_directories(spark, tmp_path):
    """10 tiles in an archive whose directories and metadata are stored
    uncompressed read back through pmtiles_gbx with the same (z, x, y) rows and
    identical tile bytes as the gzip-directory archive."""
    from pmtiles.tile import Compression, TileType, zxy_to_tileid
    from pmtiles.writer import Writer

    gz, plain = str(tmp_path / "gz.pmtiles"), str(tmp_path / "plain.pmtiles")
    tiles = {(14, 2615 + i, 6330): _png_bytes(i) for i in range(10)}
    with open(gz, "wb") as fh:
        w = Writer(fh)
        for (z, x, y), data in sorted(
            tiles.items(), key=lambda kv: zxy_to_tileid(*kv[0])
        ):
            w.write_tile(zxy_to_tileid(z, x, y), data)
        w.finalize(
            {"tile_type": TileType.PNG, "tile_compression": Compression.NONE},
            {"name": "ten"},
        )
    _with_uncompressed_internals(gz, plain)

    spark.dataSource.register(PMTilesGbxDataSource)

    def read(path):
        rows = (
            spark.read.format("pmtiles_gbx")
            .option("source", "archive")
            .option("path", path)
            .load()
            .collect()
        )
        return {(r["z"], r["x"], r["y"]): bytes(r["bytes"]) for r in rows}

    assert read(gz) == tiles
    assert read(plain) == tiles


def test_archive_reader_uncompressed_directories_with_leaves(tmp_path):
    """The uncompressed variant of a multi-partition archive (root pointing at
    leaf directories, run-length entries, deduplicated tiles) yields the same
    rows as the library traversal of the gzip original: planning and the
    per-tile lookup both decode root and leaf directories by the header."""
    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    gz, plain = str(tmp_path / "gz.pmtiles"), str(tmp_path / "plain.pmtiles")
    _write_multi_partition_archive(gz)
    _with_uncompressed_internals(gz, plain)
    assert _archive_header(plain)["leaf_directory_length"] > 0  # leaves exercised
    expected = _expected_archive_rows(gz)

    reader = PMtilesArchiveReader({"path": plain, "tilesPerPartition": str(_TPP)})
    parts = reader.partitions()
    assert [t for part in parts for t in part.tiles] == [e[:3] for e in expected]
    assert [row for part in parts for row in reader.read(part)] == expected


def test_read_directory_decodes_by_the_declared_compression():
    import gzip

    from pmtiles.tile import Compression, Entry, serialize_directory

    from databricks.labs.gbx.ds._pmtiles_read import _read_directory

    entries = [Entry(5, 0, 10, 1), Entry(9, 10, 7, 3)]
    gzipped = serialize_directory(entries)
    bare = gzip.decompress(gzipped)
    calls = []

    def get_bytes(offset, length):
        calls.append((offset, length))
        return {1: bare, 2: gzipped}[offset]

    def view(comp, offset):
        got = _read_directory(get_bytes, {"internal_compression": comp}, offset, 99)
        return [(e.tile_id, e.offset, e.length, e.run_length) for e in got]

    want = [(5, 0, 10, 1), (9, 10, 7, 3)]
    assert view(Compression.NONE, 1) == want
    assert view(Compression.GZIP, 2) == want
    assert calls == [(1, 99), (2, 99)]


@pytest.mark.parametrize("name,value", [("UNKNOWN", 0), ("BROTLI", 3), ("ZSTD", 4)])
def test_read_directory_rejects_unsupported_compression(name, value):
    from pmtiles.tile import Compression

    from databricks.labs.gbx.ds._pmtiles_read import _read_directory

    header = {"internal_compression": Compression[name]}
    with pytest.raises(ValueError) as info:
        _read_directory(lambda o, n: b"", header, 127, 10)
    assert str(info.value) == (
        f"pmtiles_gbx: internal compression {value} is not supported"
    )


def test_archive_with_an_unsupported_internal_compression_fails_at_planning(tmp_path):
    """A zstd-directory archive is refused when the read is planned, with the
    reader's own error, not a gzip decoding error."""
    from pmtiles.tile import Compression, deserialize_header, serialize_header

    from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader

    path = str(tmp_path / "z.pmtiles")
    _write_multi_partition_archive(path, n=50)
    with open(path, "r+b") as fh:
        h = deserialize_header(fh.read(127))
        fh.seek(0)
        fh.write(serialize_header(dict(h, internal_compression=Compression.ZSTD)))
    with pytest.raises(ValueError, match="internal compression 4 is not supported"):
        PMtilesArchiveReader({"path": path}).partitions()
