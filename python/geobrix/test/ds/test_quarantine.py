"""Bad-record handling over paths (``read_paths(on_error="skip")``, the reads
behind ``declare_ingest(on_error="quarantine")``): what is set aside per format,
what is still read, and how a record is described.

Every corrupt sample is built here, the same way the corrupt-input probe built
it: a valid file, then a few bytes garbled, truncated or rewritten.
"""

import json
import os
import shutil
import sqlite3
import struct
import warnings
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from databricks.labs.gbx.ds import _quarantine as Q
from databricks.labs.gbx.ds import paths as P

MTIME_US = 1_760_000_000_000_000


# --- helpers -------------------------------------------------------------------


def _records(folder):
    if not os.path.isdir(folder) or not os.listdir(folder):
        return []
    rows = pq.read_table(folder).to_pylist()
    return sorted(rows, key=lambda r: (r["_source_path"], r["record_index"] or ""))


def _read(tmp_path, path, fmt, options=None, schema_file=None):
    """``(rows, bad records)`` of one file read through iter_path_batches with a
    bad-record collector writing to a scratch folder (the executor side of
    declare_ingest's quarantine)."""
    scratch = str(tmp_path / "bad_records")
    bad = Q.BadRecords(scratch)
    expected = P.path_schema(fmt, options, schema_file or path)
    batches = list(
        P.iter_path_batches(path, MTIME_US, fmt, options, expected, True, bad)
    )
    bad.close()
    rows = [r for b in batches for r in b.to_pylist()]
    return rows, _records(scratch)


def _classes(records):
    return [(r["record_index"], r["error_class"]) for r in records]


def _garble(path, offset, n):
    with open(path, "r+b") as fh:
        fh.seek(offset)
        data = bytearray(fh.read(n))
        for i in range(len(data)):
            data[i] ^= 0x5A
        fh.seek(offset)
        fh.write(bytes(data))


def _feature(i, geometry=None, **props):
    return {
        "type": "Feature",
        "properties": {"id": i, "name": f"f{i}", **props},
        "geometry": (
            geometry
            if geometry is not None
            else {"type": "Point", "coordinates": [i * 0.1, i * 0.2]}
        ),
    }


def _geojson(path, features):
    with open(path, "w") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)
    return str(path)


def _listing(spark, paths):
    rows = [
        (p, datetime.fromtimestamp(os.stat(p).st_mtime, tz=timezone.utc)) for p in paths
    ]
    return spark.createDataFrame(rows, "path string, modificationTime timestamp")


# --- the on_error contract -------------------------------------------------------


def test_read_paths_on_error_values(spark, tmp_path):
    good = _geojson(tmp_path / "a.geojson", [_feature(0)])
    df = _listing(spark, [good])
    with pytest.raises(ValueError, match="on_error must be one of"):
        P.read_paths(df, "geojson_gbx", on_error="quarantine")
    with pytest.raises(ValueError, match="bad_records_dir needs on_error='skip'"):
        P.read_paths(df, "geojson_gbx", bad_records_dir=str(tmp_path / "x"))


def test_fail_mode_error_names_the_file(spark, tmp_path):
    """laspy's error for a truncated LAS file does not name the file;
    on_error="fail" adds it, keeping the exception's type and message."""
    good, offset, reclen = _las(tmp_path / "good.las", 100)
    bad = str(tmp_path / "cut.las")
    shutil.copy(good, bad)
    with open(bad, "r+b") as fh:
        fh.truncate(offset + 50 * reclen + 7)
    expected = P.path_schema("lidar_gbx", None, good)
    with pytest.raises(Exception) as info:
        list(P.iter_path_batches(bad, None, "lidar_gbx", None, expected))
    assert bad not in str(info.value)
    assert f"while reading {bad}" in info.value.__notes__
    with pytest.raises(Exception, match=f"while reading {bad}"):
        P.read_paths(_listing(spark, [good, bad]), "lidar_gbx").collect()


def test_skip_reads_the_good_files_and_sets_the_bad_one_aside(spark, tmp_path):
    """read_paths(on_error="skip") over Spark: the unreadable file is one
    whole-file record, written by the task to the scratch folder; the other
    files are read in full."""
    good = _geojson(tmp_path / "good.geojson", [_feature(i) for i in range(3)])
    bad = tmp_path / "bad.geojson"
    bad.write_bytes(b"\x00not a geospatial file" * 20)
    scratch = str(tmp_path / "scratch")
    rows = P.read_paths(
        _listing(spark, [good, str(bad)]),
        "geojson_gbx",
        schema_path=good,
        on_error="skip",
        bad_records_dir=scratch,
    ).collect()
    assert sorted(r["id"] for r in rows) == [0, 1, 2]
    (rec,) = _records(scratch)
    assert rec["_source_path"] == str(bad) and rec["record_index"] is None
    assert rec["error_class"] == "pyogrio.errors.DataSourceError"
    assert rec["_source_mtime"] is not None
    # Each task names its fragment after its stage, partition and attempt.
    (name,) = os.listdir(scratch)
    assert name.startswith("s") and "-p" in name and "-a0-" in name


def test_skip_without_a_folder_logs_and_skips(spark, tmp_path):
    good = _geojson(tmp_path / "good.geojson", [_feature(0)])
    bad = tmp_path / "bad.geojson"
    bad.write_bytes(b"garbage" * 30)
    rows = P.read_paths(
        _listing(spark, [str(bad), good]),
        "geojson_gbx",
        schema_path=good,
        on_error="skip",
    ).collect()
    assert [r["id"] for r in rows] == [0]


# --- the partition boundary --------------------------------------------------------


class _Panic(BaseException):
    """Stands in for pyo3's PanicException (a BaseException)."""


def _patch_vector_read(monkeypatch, then):
    from databricks.labs.gbx.ds import vector

    real = vector.VectorGbxReader.read

    def read(self, partition):
        it = real(self, partition)
        yield next(it)
        warnings.warn("decoder lost sync", UserWarning)
        then()

    monkeypatch.setattr(vector.VectorGbxReader, "read", read)


def test_an_error_mid_file_keeps_the_rows_read_and_says_why(tmp_path, monkeypatch):
    """Any exception (a Rust panic included) is a record of the file with the
    warnings seen while reading and the rows read before it; those rows stay."""
    path = _geojson(tmp_path / "a.geojson", [_feature(i) for i in range(5)])

    def panic():
        raise _Panic("slice index starts at 1 but ends at 0")

    _patch_vector_read(monkeypatch, panic)
    rows, (rec,) = _read(tmp_path, path, "geojson_gbx", {"chunkSize": "2"})
    assert [r["id"] for r in rows] == [0, 1]
    assert rec["record_index"] is None
    assert rec["error_class"].endswith("_Panic")
    assert "slice index starts at 1" in rec["error_message"]
    assert "warnings: UserWarning: decoder lost sync" in rec["error_message"]
    assert "2 rows of this file were read before the error" in rec["error_message"]


def test_keyboard_interrupt_is_not_a_bad_record(tmp_path, monkeypatch):
    path = _geojson(tmp_path / "a.geojson", [_feature(i) for i in range(5)])

    def interrupt():
        raise KeyboardInterrupt

    _patch_vector_read(monkeypatch, interrupt)
    with pytest.raises(KeyboardInterrupt):
        _read(tmp_path, path, "geojson_gbx", {"chunkSize": "2"})


def test_describe_follows_the_cause_chain():
    try:
        try:
            raise ValueError("ZIPDecode: Decoding error at scanline 128")
        except ValueError as inner:
            raise RuntimeError("Read failed. See previous exception") from inner
    except RuntimeError as e:
        msg = Q.describe(e, None, 3)
    assert msg.startswith("RuntimeError: Read failed.")
    assert "caused by ValueError: ZIPDecode: Decoding error at scanline 128" in msg
    assert msg.endswith("3 rows of this file were read before the error")


# --- vector: rows that are not data --------------------------------------------------


def test_vector_rows_that_are_not_data_are_set_aside(tmp_path):
    """A geometry WKB cannot hold (an unclosed ring) and invalid UTF-8 text are
    records of their own. A NULL geometry (no geometry, or one GDAL cannot
    decode: an unknown type, unreadable coordinates) and an empty one are data:
    they stay in the rows."""
    feats = [_feature(i) for i in range(8)]
    feats[1]["geometry"] = None
    feats[2]["geometry"] = {"type": "Pointy", "coordinates": [1.0, 2.0]}
    feats[3]["geometry"] = {"type": "Point", "coordinates": ["a", "b"]}
    feats[4]["geometry"] = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0]]]}
    feats[5]["geometry"] = {"type": "MultiPoint", "coordinates": []}
    text = json.dumps({"type": "FeatureCollection", "features": feats})
    raw = text.replace('"f6"', '"f6\udcff\udcfe"').encode("utf-8", "surrogateescape")
    path = tmp_path / "a.geojson"
    path.write_bytes(raw)
    good = _geojson(tmp_path / "good.geojson", [_feature(0)])
    rows, recs = _read(tmp_path, str(path), "geojson_gbx", None, good)
    assert sorted(r["id"] for r in rows) == [0, 1, 2, 3, 5, 7]
    by_id = {r["id"]: r["geom_0"] for r in rows}
    assert by_id[1] is None and by_id[2] is None and by_id[3] is None
    assert shapely.from_wkb(by_id[5]).is_empty
    assert _classes(recs) == [
        ("feature=4", Q.UNREADABLE_GEOMETRY),
        ("feature=6", Q.INVALID_TEXT),
    ]
    # The record keeps the feature's other columns (escaped where not text).
    assert json.loads(recs[0]["raw"]) == {"id": 4, "name": "f4"}
    assert json.loads(recs[1]["raw"])["name"] == "f6\\xff\\xfe"


def test_wkt_output_sets_an_unreadable_geometry_aside(tmp_path):
    """With asWKB=false the WKB is decoded to WKT inside the reader; an
    unreadable one is a record, not a failed file, and a NULL one stays."""
    feats = [_feature(i) for i in range(4)]
    feats[1]["geometry"] = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0]]]}
    feats[3]["geometry"] = None
    path = _geojson(tmp_path / "a.geojson", feats)
    rows, recs = _read(tmp_path, path, "geojson_gbx", {"asWKB": "false"})
    assert sorted(r["id"] for r in rows) == [0, 2, 3]
    assert [r["geom_0"] for r in rows if r["id"] == 3] == [None]
    assert _classes(recs) == [("feature=1", Q.UNREADABLE_GEOMETRY)]


def test_schema_drift_is_a_record_of_the_file(tmp_path):
    good = _geojson(tmp_path / "good.geojson", [_feature(0)])
    drifted = _geojson(tmp_path / "drift.geojson", [_feature(0, id="x")])
    rows, (rec,) = _read(tmp_path, drifted, "geojson_gbx", None, good)
    assert rows == [] and rec["record_index"] is None
    assert rec["error_class"] == "ValueError"
    assert "changed types ['id']" in rec["error_message"]


# --- vector: features a file declares but the reader drops ---------------------------


def test_geojsonseq_dropped_lines_are_found_and_recorded(tmp_path):
    lines = [json.dumps(_feature(i)) for i in range(5)]
    lines[2] = '{"type": "Feature", "geometry": {'
    path = tmp_path / "a.geojsonl"
    path.write_text("\n".join(lines) + "\n\n")
    rows, recs = _read(tmp_path, str(path), "geojsonl_gbx", None, None)
    assert sorted(r["id"] for r in rows) == [0, 1, 3, 4]
    (rec,) = recs
    assert (rec["record_index"], rec["error_class"]) == ("line=3", Q.RECORDS_NOT_READ)
    assert json.loads(rec["raw"]) == {"line": lines[2]}


def test_geojsonseq_rs_records_count_as_records(tmp_path):
    """RFC 8142 sequences: one record per RS (0x1E), even when it spans lines."""
    path = tmp_path / "a.geojsonl"
    path.write_bytes(
        b"".join(
            b"\x1e" + json.dumps(_feature(i), indent=1).encode() + b"\n"
            for i in range(3)
        )
    )
    assert Q.sequence_record_count(str(path)) == 3
    assert Q.unparseable_lines(str(path)) == []


def _shapefile(folder, n=20):
    import pyogrio
    import shapely

    folder.mkdir()
    path = str(folder / "pts.shp")
    tbl = pa.table(
        {
            "id": pa.array(range(n), pa.int32()),
            "geometry": shapely.to_wkb(shapely.points(range(n), range(n))),
        }
    )
    pyogrio.write_arrow(
        tbl,
        path,
        driver="ESRI Shapefile",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return path


def _dbf_layout(dbf):
    with open(dbf, "rb") as fh:
        n, header, reclen = struct.unpack("<IHH", fh.read(12)[4:12])
    return n, header, reclen


def test_shapefile_dropped_records_are_counted_deleted_ones_are_not(tmp_path):
    """A record the .dbf marks deleted is not data and not a bad record; records
    lost to a truncated .dbf are one record of the file."""
    path = _shapefile(tmp_path / "shp")
    dbf = path[:-4] + ".dbf"
    n, header, reclen = _dbf_layout(dbf)
    with open(dbf, "r+b") as fh:
        fh.seek(header + 4 * reclen)
        fh.write(b"*")
    rows, recs = _read(tmp_path, path, "shapefile_gbx")
    assert len(rows) == n - 1 and recs == []

    other = _shapefile(tmp_path / "shp2")
    with open(other[:-4] + ".dbf", "r+b") as fh:
        fh.truncate(header + 15 * reclen)
    rows, (rec,) = _read(tmp_path, other, "shapefile_gbx", None, path)
    assert len(rows) == 15
    assert (rec["record_index"], rec["error_class"]) == (None, Q.RECORDS_NOT_READ)
    assert "5 of the 20 features" in rec["error_message"]


def _gpkg_with_a_bad_page(path, n=2000, garble=True):
    """A 2000-polygon GeoPackage with the SQLite page holding feature 1000
    garbled (unless not ``garble``): OGR's scan ends there without an error."""
    import pyogrio
    import shapely

    geoms = [shapely.box(i * 0.01, 0, i * 0.01 + 0.005, 0.005) for i in range(n)]
    tbl = pa.table(
        {
            "id": pa.array(range(n), pa.int32()),
            "name": pa.array([f"feature-{i:05d}-" + "x" * 40 for i in range(n)]),
            "geometry": shapely.to_wkb(np.array(geoms)),
        }
    )
    pyogrio.write_arrow(
        tbl,
        str(path),
        driver="GPKG",
        layer="feats",
        geometry_name="geometry",
        geometry_type="Polygon",
        crs="EPSG:4326",
    )
    con = sqlite3.connect(str(path))
    page = con.execute("PRAGMA page_size").fetchone()[0]
    con.close()
    data = open(path, "rb").read()
    at = data.index(b"feature-01000-")
    start = (at // page) * page
    on_page = {
        int(data[i + 8 : i + 13])
        for i in range(start, start + page)
        if data.startswith(b"feature-", i)
    }
    if garble:
        _garble(str(path), start, page)
    return str(path), on_page


def test_gpkg_read_resumes_past_an_unreadable_page(tmp_path):
    """The scan ends early without an error; the read resumes past the page and
    each feature on it is a record, by fid; every other feature is read."""
    path, on_page = _gpkg_with_a_bad_page(tmp_path / "a.gpkg")
    rows, recs = _read(tmp_path, path, "gpkg_gbx", {"chunkSize": "100"})
    lost = {int(r["record_index"].split("=")[1]) - 1 for r in recs}  # fid = id + 1
    assert {r["error_class"] for r in recs} == {Q.RECORDS_NOT_READ}
    assert on_page <= lost and len(lost) < 2 * len(on_page)
    got = {r["id"] for r in rows}
    assert len(got) == len(rows) == 2000 - len(lost)
    assert got | lost == set(range(2000)) and not (got & lost)


def test_vector_counts_are_not_checked_under_a_filter(tmp_path):
    path, _ = _gpkg_with_a_bad_page(tmp_path / "a.gpkg", garble=False)
    rows, recs = _read(tmp_path, path, "gpkg_gbx", {"where": "id < 500"})
    assert len(rows) == 500 and recs == []


# --- raster: the tile window ----------------------------------------------------------


def _tiled_tif(path, compress="deflate"):
    import rasterio

    data = (np.arange(256 * 256, dtype="uint16") % 4096).reshape(256, 256)
    with rasterio.open(
        str(path),
        "w",
        driver="GTiff",
        width=256,
        height=256,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(0, 1, 1 / 256, 1 / 256),
        tiled=True,
        blockxsize=64,
        blockysize=64,
        compress=compress,
    ) as ds:
        ds.write(data, 1)
    return str(path)


def _garble_block(path, x, y):
    import rasterio

    with rasterio.open(path) as ds:
        off = int(ds.get_tag_item(f"BLOCK_OFFSET_{x}_{y}", "TIFF", bidx=1))
        size = int(ds.get_tag_item(f"BLOCK_SIZE_{x}_{y}", "TIFF", bidx=1))
    _garble(path, off + size // 4, size // 2)


def test_raster_window_that_cannot_be_decoded_is_set_aside(spark, tmp_path):
    """Each tile window is read on its own (virtualTiles=false + tileSize): the
    one over the garbled block is a record with GDAL's reason; the other 15
    windows are read. A file that cannot even be planned is one record."""
    path = _tiled_tif(tmp_path / "a.tif")
    _garble_block(path, 2, 1)
    junk = tmp_path / "junk.tif"
    junk.write_bytes(b"II*\x00" + b"\x5a" * 400)
    scratch = str(tmp_path / "scratch")
    rows = P.read_paths(
        _listing(spark, [path, str(junk)]),
        "gtiff_gbx",
        {"virtualTiles": "false", "tileSize": "64"},
        schema_path=path,
        num_partitions=4,
        on_error="skip",
        bad_records_dir=scratch,
    ).collect()
    assert len(rows) == 15
    window, planned = _records(scratch)
    assert window["record_index"] == "window=128,64,64,64"
    assert window["error_class"] == "rasterio.errors.RasterioIOError"
    assert "IReadBlock failed at X offset 2, Y offset 1" in window["error_message"]
    assert planned["_source_path"] == str(junk) and planned["record_index"] is None


# --- LiDAR ----------------------------------------------------------------------------


def _las(path, n=5000):
    import laspy

    las = laspy.LasData(laspy.LasHeader(point_format=3, version="1.2"))
    rng = np.random.default_rng(3)
    las.x = rng.uniform(0, 100, n)
    las.y = rng.uniform(0, 100, n)
    las.z = rng.uniform(0, 10, n)
    las.write(str(path))
    with laspy.open(str(path)) as r:
        return str(path), r.header.offset_to_point_data, r.header.point_format.size


def test_lidar_truncated_file_reads_its_complete_points(tmp_path):
    path, offset, reclen = _las(tmp_path / "a.las")
    with open(path, "r+b") as fh:
        fh.truncate(offset + 2500 * reclen + 7)
    rows, (rec,) = _read(tmp_path, path, "lidar_gbx", {"chunkSize": "1000"})
    assert len(rows) == 2500
    assert (rec["record_index"], rec["error_class"]) == (None, Q.RECORDS_NOT_READ)
    assert "ends after 2500 of the 5000 points" in rec["error_message"]


def test_lidar_garbled_points_outside_the_header_bounds_are_set_aside(tmp_path):
    path, offset, reclen = _las(tmp_path / "a.las")
    for i in range(2500, 2510):  # X of 10 records -> far outside the bounds
        with open(path, "r+b") as fh:
            fh.seek(offset + i * reclen)
            fh.write(struct.pack("<i", 2_000_000_000))
    rows, recs = _read(tmp_path, path, "lidar_gbx", {"chunkSize": "1000"})
    assert len(rows) == 4990
    assert _classes(recs) == [("points=2500-2509", Q.POINT_OUT_OF_BOUNDS)]


def test_lidar_files_it_used_to_skip_are_records(tmp_path):
    """An empty or unreadable LAS file is a record instead of a warning."""
    good, _, _ = _las(tmp_path / "good.las", 10)
    empty = tmp_path / "empty.las"
    empty.write_bytes(b"")
    bad = tmp_path / "bad.las"
    bad.write_bytes(b"XXXX" + open(good, "rb").read()[4:])
    for path, cls in ((empty, Q.UNREADABLE_FILE), (bad, "laspy.errors.LaspyException")):
        rows, (rec,) = _read(tmp_path, str(path), "lidar_gbx", None, good)
        assert rows == [] and (rec["record_index"], rec["error_class"]) == (None, cls)
        shutil.rmtree(tmp_path / "bad_records")


def test_lidar_chunk_that_fails_to_decode_is_narrowed_down(tmp_path, monkeypatch):
    """A chunk whose decode raises (a LAZ panic) is re-read in pieces from a
    fresh open: the points that decode are kept, the piece that fails is one
    record, and the read goes on after it."""
    import laspy

    path, _, _ = _las(tmp_path / "a.las")
    real = laspy.LasReader.read_points
    state = {"pos": {}}

    def read_points(self, n):
        start = state["pos"].get(id(self), 0)
        if start <= 2200 < start + n:
            raise _Panic("decoder panicked")
        out = real(self, n)
        state["pos"][id(self)] = start + len(out)
        return out

    real_seek = laspy.LasReader.seek

    def seek(self, pos, whence=0):
        state["pos"][id(self)] = pos
        return real_seek(self, pos, whence)

    monkeypatch.setattr(laspy.LasReader, "read_points", read_points)
    monkeypatch.setattr(laspy.LasReader, "seek", seek)
    rows, recs = _read(tmp_path, path, "lidar_gbx", {"chunkSize": "2000"})
    assert len(rows) == 4000
    ((index, cls),) = _classes(recs)
    assert index == "points=2000-2999" and cls.endswith("_Panic")


def test_lidar_record_length_counting_missing_extra_bytes_reads_nothing(tmp_path):
    """A header whose point record length counts 4 extra bytes the point data
    does not hold (5000 records of 34 bytes, the header says 38) reads no
    point: the records would decode shifted."""
    path, _, reclen = _las(tmp_path / "a.las")
    assert reclen == 34
    with open(path, "r+b") as fh:  # LAS 1.2: point data record length at byte 105
        fh.seek(105)
        fh.write(struct.pack("<H", 38))
    rows, (rec,) = _read(tmp_path, path, "lidar_gbx")
    assert rows == []
    assert (rec["record_index"], rec["error_class"]) == (None, Q.UNREADABLE_FILE)
    assert "38 bytes" in rec["error_message"]


def _laz(path, n=5000):
    """A one-chunk LAZ file with UTM-like offsets: a zero-filled point record
    decodes to the offset corner, which lies inside the header bounds."""
    import laspy

    header = laspy.LasHeader(point_format=3, version="1.2")
    header.offsets = np.array([500000.0, 4000000.0, 100.0])
    header.scales = np.array([0.01, 0.01, 0.01])
    las = laspy.LasData(header)
    rng = np.random.default_rng(3)
    las.x = rng.uniform(500000, 500100, n)
    las.y = rng.uniform(4000000, 4000100, n)
    las.z = rng.uniform(100, 110, n)
    las.write(str(path))
    return str(path)


def _declare_points(path, count):
    with open(path, "r+b") as fh:  # LAS 1.2: point count at byte 107
        fh.seek(107)
        fh.write(struct.pack("<I", count))


def _at_offset_corner(rows):
    return [r for r in rows if (r["x"], r["y"], r["z"]) == (500000.0, 4000000.0, 100.0)]


def test_lidar_laz_declaring_more_points_than_it_holds_fabricates_none(tmp_path):
    """A one-chunk LAZ whose header declares 6000 points but holds 5000: the
    5000 are read and the 1000 missing ones are a record, never zero-filled
    rows (laspy's parallel LAZ decoder fills a read past the data after a seek
    with zeros instead of failing)."""
    path = _laz(tmp_path / "a.laz")
    _declare_points(path, 6000)
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert len(rows) == 5000 and _at_offset_corner(rows) == []
    ((index, _),) = _classes(recs)
    assert index == "points=5000-5999"


def test_lidar_laz_declaring_a_huge_point_count_fabricates_none(tmp_path):
    path = _laz(tmp_path / "a.laz")
    _declare_points(path, 10**9)
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert len(rows) == 5000 and _at_offset_corner(rows) == []
    capped = [r for r in recs if r["error_class"] == Q.RECORDS_NOT_READ]
    assert len(capped) == 1 and "more than a" in capped[0]["error_message"]
    assert [r["record_index"] for r in recs if r["record_index"]][0].startswith(
        "points=5000-"
    )


def test_lidar_valid_laz_reads_every_point_and_no_record(tmp_path):
    path = _laz(tmp_path / "a.laz")
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert len(rows) == 5000 and recs == []


# --- EXIF -----------------------------------------------------------------------------


def _jpg(path):
    """A JPEG whose scan data (noise) is most of the file."""
    from PIL import Image

    rng = np.random.default_rng(5)
    Image.fromarray(rng.integers(0, 255, (256, 256, 3), dtype="uint8")).save(
        str(path), quality=90
    )
    return str(path)


def test_exif_file_that_is_not_a_jpeg_is_a_record(tmp_path):
    """Set aside: no JPEG start marker, or a header that cannot be decoded. A
    missing or late end marker is not: a truncated scan or a Motion Photo
    (an MP4 after the image) still has its metadata; it is a warning."""
    good = _jpg(tmp_path / "good.jpg")
    data = open(good, "rb").read()
    junk = tmp_path / "junk.jpg"
    junk.write_bytes(b"\x00" * 300)
    garbled = tmp_path / "garbled.jpg"
    garbled.write_bytes(b"\xff\xd8" + b"\x5a" * 400)
    for path, why in ((junk, "no JPEG start marker"), (garbled, "cannot be decoded")):
        rows, (rec,) = _read(tmp_path, str(path), "exif_gbx", None, good)
        assert rows == [] and rec["error_class"] == Q.NOT_JPEG
        assert why in rec["error_message"]
        shutil.rmtree(tmp_path / "bad_records")
    cut = tmp_path / "cut.jpg"
    cut.write_bytes(data[: len(data) // 2])
    motion = tmp_path / "motion.jpg"
    motion.write_bytes(data + b"\x00\x00\x00\x18ftypmp42" + os.urandom(3 << 20))
    for path in (cut, motion):
        rows, recs = _read(tmp_path, str(path), "exif_gbx", None, good)
        assert len(rows) == 1 and recs == []


def test_exif_qc_decode_error_is_a_record(tmp_path):
    good = _jpg(tmp_path / "good.jpg")
    data = open(good, "rb").read()
    cut = tmp_path / "cut.jpg"
    cut.write_bytes(data[: len(data) // 2])
    rows, (rec,) = _read(tmp_path, str(cut), "exif_gbx", {"mode": "qc"}, good)
    assert rows == [] and rec["record_index"] is None
    assert rec["error_class"] == "OSError"
    assert "truncated" in rec["error_message"].lower()


# --- NetCDF ----------------------------------------------------------------------------

_N = 20


def _slice_values(t):
    return np.arange(_N * _N, dtype="float32").reshape(_N, _N) + 1000.0 * t


def _nc_with_a_bad_slice(path, bad_t=1, ntime=3):
    """(time, lat, lon) 'temp', one zlib chunk per time slice, slice ``bad_t``'s
    chunk garbled (found by inflating every candidate stream)."""
    import zlib

    from netCDF4 import Dataset

    with Dataset(str(path), "w", format="NETCDF4") as ds:
        ds.createDimension("time", ntime)
        ds.createDimension("lat", _N)
        ds.createDimension("lon", _N)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name, lon.standard_name = "latitude", "longitude"
        lat[:] = np.linspace(50.0, 49.0, _N)
        lon[:] = np.linspace(10.0, 11.0, _N)
        ds.createVariable("time", "f8", ("time",))[:] = np.arange(ntime, dtype="f8")
        v = ds.createVariable(
            "temp",
            "f4",
            ("time", "lat", "lon"),
            zlib=True,
            shuffle=False,
            chunksizes=(1, _N, _N),
        )
        for t in range(ntime):
            v[t, :, :] = _slice_values(t)
    want = _slice_values(bad_t).astype("<f4").tobytes()
    data = bytearray(open(path, "rb").read())
    for i in range(len(data) - 1):
        if data[i] != 0x78:
            continue
        d = zlib.decompressobj()
        try:
            out = d.decompress(bytes(data[i : i + 2 * len(want) + 64]))
        except zlib.error:
            continue
        if d.eof and out == want:
            mid = i + 16
            for j in range(mid, mid + 8):
                data[j] ^= 0x5A
            open(path, "wb").write(bytes(data))
            return str(path)
    raise AssertionError("no zlib chunk found")


def test_netcdf_slice_that_cannot_be_read_is_set_aside(tmp_path):
    path = _nc_with_a_bad_slice(tmp_path / "a.nc")
    rows, (rec,) = _read(tmp_path, path, "netcdf_gbx", {"fanout": "time"})
    assert len(rows) == 2
    assert rec["record_index"] == "variable=temp,time=1"
    assert "HDF error" in rec["error_message"]


def test_netcdf_classic_file_cut_short_is_a_record(tmp_path):
    import xarray as xr

    path = str(tmp_path / "a.nc")
    xr.Dataset(
        {"t": (("y", "x"), np.ones((50, 50), "f4"))},
        coords={"y": np.arange(50.0), "x": np.arange(50.0)},
    ).to_netcdf(path, format="NETCDF3_CLASSIC")
    good = str(tmp_path / "good.nc")
    shutil.copy(path, good)
    with open(path, "r+b") as fh:
        fh.truncate(os.path.getsize(path) - 3000)
    rows, (rec,) = _read(tmp_path, path, "netcdf_gbx", None, good)
    assert rows == []
    assert (rec["record_index"], rec["error_class"]) == (None, Q.RECORDS_NOT_READ)


# --- PMTiles ---------------------------------------------------------------------------


def _png(i):
    import io

    from PIL import Image

    buf = io.BytesIO()
    rng = np.random.default_rng(i)
    Image.fromarray(rng.integers(0, 255, (64, 64, 3), dtype="uint8")).save(
        buf, format="PNG"
    )
    return buf.getvalue()


def _pmtiles_archive(path, tiles):
    from pmtiles.tile import Compression, TileType, zxy_to_tileid

    from databricks.labs.gbx.ds.tiles._header import build_header_info
    from databricks.labs.gbx.ds.tiles.backend import PMTilesBackend
    from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

    info = build_header_info(
        [(z, x, y) for z, x, y, _ in tiles],
        SlippyGrid(),
        TileType.PNG,
        Compression.NONE,
        {},
    )
    ordered = sorted((zxy_to_tileid(z, x, y), b) for z, x, y, b in tiles)
    PMTilesBackend().assemble(iter(ordered), info, str(path))
    return str(path)


def test_pmtiles_tiles_of_a_truncated_archive_are_records(spark, tmp_path):
    """Tiles past the end of a truncated archive come back short: each is a
    record; the tiles before the cut are read."""
    path = _pmtiles_archive(
        tmp_path / "a.pmtiles",
        [(3, x, y, _png(8 * x + y)) for x in range(4) for y in range(4)],
    )
    good = str(tmp_path / "good.pmtiles")
    shutil.copy(path, good)
    with open(path, "r+b") as fh:
        fh.truncate(os.path.getsize(path) - 3 * len(_png(0)) // 2)
    scratch = str(tmp_path / "scratch")
    rows = P.read_paths(
        _listing(spark, [path]),
        "pmtiles_gbx",
        {"source": "archive", "tilesPerPartition": "4"},
        schema_path=good,
        on_error="skip",
        bad_records_dir=scratch,
    ).collect()
    recs = _records(scratch)
    assert {r["error_class"] for r in recs} == {Q.SHORT_TILE}
    assert all(r["record_index"].startswith("tile=3/") for r in recs)
    assert len(rows) + len(recs) == 16 and 1 <= len(recs) <= 3


# --- the collector ---------------------------------------------------------------------


def test_fragments_hold_the_fragment_schema_and_flush_in_bounded_files(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(Q, "_FLUSH_ROWS", 2)
    scratch = str(tmp_path / "s")
    bad = Q.BadRecords(scratch)
    for i in range(5):
        bad.add("/v/a.gpkg", MTIME_US, f"feature={i}", Q.UNREADABLE_GEOMETRY, "x", "{}")
    bad.close()
    names = sorted(os.listdir(scratch))
    assert len(names) == 3 and all(n.startswith("local-") for n in names)
    table = pq.read_table(scratch)
    assert table.schema.equals(Q.FRAGMENT_SCHEMA)
    assert table.num_rows == 5


def test_a_folder_that_cannot_be_written_says_what_to_do(tmp_path):
    blocker = tmp_path / "landing"
    blocker.write_text("a file, not a folder")
    bad = Q.BadRecords(str(blocker / ".gbx_scratch" / "x"))
    bad.add("/v/a.gpkg", MTIME_US, None, "ValueError", "x")
    with pytest.raises(OSError, match="scratch_dir"):
        bad.close()


# --- fix round 2: valid data that must not be set aside ---------------------------


def _wkb_points(points):
    return b"".join(struct.pack("<dd", *p) for p in points)


_ARC = struct.pack("<BII", 1, 8, 3) + _wkb_points([(0, 0), (1, 1), (2, 0)])
_COMPOUND = struct.pack("<BII", 1, 9, 1) + _ARC
_RING = (
    struct.pack("<BII", 1, 9, 2)
    + _ARC
    + struct.pack("<BII", 1, 2, 2)
    + _wkb_points([(2, 0), (0, 0)])
)
_CURVE_POLYGON = struct.pack("<BII", 1, 10, 1) + _RING
_MULTI_CURVE = struct.pack("<BII", 1, 11, 1) + _COMPOUND
_TRIANGLE = (
    struct.pack("<BII", 1, 17, 1)
    + struct.pack("<I", 4)
    + _wkb_points([(0, 0), (1, 0), (0, 1), (0, 0)])
)
_TIN = struct.pack("<BII", 1, 16, 1) + _TRIANGLE
_POLYHEDRAL = struct.pack("<BII", 1, 15, 1) + struct.pack("<BI", 1, 3) + _TRIANGLE[5:]
_CURVE_IN_COLLECTION = struct.pack("<BII", 1, 7, 1) + _ARC
_ARC_Z_ISO = struct.pack("<BII", 1, 1008, 3) + b"".join(
    struct.pack("<ddd", x, y, 0) for x, y in [(0, 0), (1, 1), (2, 0)]
)
_ARC_EWKB_SRID = struct.pack("<BIII", 1, 0x20000008, 4326, 3) + _wkb_points(
    [(0, 0), (1, 1), (2, 0)]
)


def test_curves_and_surfaces_are_readable_geometries():
    """shapely raises NotImplementedError on curves (even with
    on_invalid="ignore") and returns None for TIN / Triangle /
    PolyhedralSurface: none of them is a bad record; only garbage is."""
    geoms = [
        _ARC,
        _COMPOUND,
        _CURVE_POLYGON,
        _MULTI_CURVE,
        _TRIANGLE,
        _TIN,
        _POLYHEDRAL,
        _CURVE_IN_COLLECTION,
        _ARC_Z_ISO,
        _ARC_EWKB_SRID,
        b"\x01\x63\x00\x00\x00" + b"\xab" * 20,
        None,
    ]
    n = len(geoms)
    batch = pa.record_batch(
        [
            pa.array(range(n), pa.int32()),
            pa.array(geoms, pa.binary()),
            pa.array(["4326"] * n),
            pa.array(["+proj=longlat"] * n),
        ],
        names=["id", "g", "g_srid", "g_srid_proj"],
    )
    assert Q.bad_rows(batch, "g") == {
        n - 2: (Q.UNREADABLE_GEOMETRY, "g is not readable WKB")
    }


_CURVES_AND_SURFACES = [
    _ARC,
    _COMPOUND,
    _CURVE_POLYGON,
    _MULTI_CURVE,
    _TRIANGLE,
    _TIN,
    _POLYHEDRAL,
    _ARC_Z_ISO,
    _ARC_EWKB_SRID,
]


def test_truncated_curves_and_surfaces_are_not_readable_geometries():
    """A curve or surface is data only when its WKB body is whole: a truncated
    one (only its header is a curve's), or one declaring a million arc points
    it does not hold, is a bad record as binary output and the reader's
    unreadable marker as WKT output; a whole one of each type is data."""
    broken = [g[:-9] for g in _CURVES_AND_SURFACES]
    broken.append(struct.pack("<BII", 1, 8, 10**6))
    geoms = broken + _CURVES_AND_SURFACES
    n = len(geoms)
    batch = pa.record_batch(
        [
            pa.array(range(n), pa.int32()),
            pa.array(geoms, pa.binary()),
            pa.array(["4326"] * n),
            pa.array(["+proj=longlat"] * n),
        ],
        names=["id", "g", "g_srid", "g_srid_proj"],
    )
    assert Q.bad_rows(batch, "g") == {
        i: (Q.UNREADABLE_GEOMETRY, "g is not readable WKB") for i in range(len(broken))
    }
    assert list(Q.wkt_or_marker(geoms)) == [Q.UNREADABLE_WKB] * len(broken) + [
        Q.UNWRITABLE_WKB
    ] * len(_CURVES_AND_SURFACES)
    assert Q.wkt_or_marker([_ARC[:-9]])[0] == Q.UNREADABLE_WKB


def _layer(path, driver, geometry_type, geoms):
    import pyogrio

    pyogrio.write_arrow(
        pa.table(
            {
                "id": pa.array(range(len(geoms)), pa.int32()),
                "geometry": pa.array(geoms, pa.binary()),
            }
        ),
        str(path),
        driver=driver,
        layer="l",
        geometry_name="geometry",
        geometry_type=geometry_type,
        crs="EPSG:4326",
    )
    return str(path)


@pytest.mark.parametrize(
    "fmt, name, driver, geometry_type, geoms",
    [
        ("gpkg_gbx", "curves.gpkg", "GPKG", "Unknown", [_COMPOUND, _CURVE_POLYGON] * 3),
        ("gpkg_gbx", "tin.gpkg", "GPKG", "Unknown", [_TIN, _TIN]),
        (
            "file_gdb_gbx",
            "curves.gdb",
            "OpenFileGDB",
            "MultiLineString",
            [_MULTI_CURVE] * 4,
        ),
    ],
)
def test_curved_and_surface_layers_ingest_whole(
    tmp_path, fmt, name, driver, geometry_type, geoms
):
    """Curved features anywhere in the file (here in every batch) are read."""
    path = _layer(tmp_path / name, driver, geometry_type, geoms)
    rows, recs = _read(tmp_path, path, fmt, {"chunkSize": "2"})
    assert len(rows) == len(geoms) and recs == []


def test_cdf5_netcdf_is_not_mistaken_for_a_truncated_file(tmp_path):
    from netCDF4 import Dataset

    path = str(tmp_path / "cdf5.nc")
    with Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
        ds.createDimension("time", None)
        ds.createDimension("y", 8)
        ds.createDimension("x", 8)
        ds.createVariable("y", "f8", ("y",))[:] = np.arange(8.0)
        ds.createVariable("x", "f8", ("x",))[:] = np.arange(8.0)
        ds.createVariable("r", "f4", ("time", "y", "x"))[0:2] = np.ones((2, 8, 8))
    assert open(path, "rb").read(4) == b"CDF\x05"
    rows, recs = _read(tmp_path, path, "netcdf_gbx")
    assert len(rows) == 1 and recs == []


def test_zipped_shapefile_deleted_records_are_not_counted(tmp_path):
    import zipfile

    shp = _shapefile(tmp_path / "shp")
    dbf = shp[:-4] + ".dbf"
    n, header, reclen = _dbf_layout(dbf)
    with open(dbf, "r+b") as fh:
        fh.seek(header + 3 * reclen)
        fh.write(b"*")
    zipped = str(tmp_path / "pts.shp.zip")
    with zipfile.ZipFile(zipped, "w") as zf:
        for f in sorted(os.listdir(tmp_path / "shp")):
            zf.write(tmp_path / "shp" / f, f)
    rows, recs = _read(tmp_path, zipped, "shapefile_gbx")
    assert len(rows) == n - 1 and recs == []


def test_lidar_points_just_past_slightly_off_header_bounds_are_data(tmp_path):
    """A header whose bounds are a little off (rounded) does not set valid
    points aside: the tolerance is a small part of the extent."""
    import laspy

    path, offset, reclen = _las(tmp_path / "a.las", 1000)
    with laspy.open(path) as r:
        maxs = r.header.maxs
    with open(path, "r+b") as fh:  # LAS 1.2: max x at byte 179
        fh.seek(179)
        fh.write(struct.pack("<d", float(maxs[0]) - 0.02))
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert len(rows) == 1000 and recs == []


def test_lidar_waveform_data_after_the_points_is_not_extra_points(tmp_path):
    """LAS 1.3 with internal waveform packets: the bytes after the points are
    waveform data, not point records the header does not declare."""
    import laspy

    las = laspy.LasData(laspy.LasHeader(point_format=4, version="1.3"))
    las.x, las.y, las.z = [1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]
    path = str(tmp_path / "wave.las")
    las.write(path)
    with laspy.open(path) as r:
        end = r.header.offset_to_point_data + 3 * r.header.point_format.size
    with open(path, "r+b") as fh:
        fh.seek(6)
        fh.write(struct.pack("<H", 2))  # global encoding: waveform data internal
        fh.seek(227)
        fh.write(struct.pack("<Q", end))  # start of waveform data packet record
        fh.seek(end)
        fh.write(b"\x00" * 60 + os.urandom(4000))  # packet header + data
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert len(rows) == 3 and recs == []


# --- fix round 2: environment errors fail the read; they are not bad data --------


def test_memory_and_import_errors_fail_the_read(tmp_path, monkeypatch):
    path = _geojson(tmp_path / "a.geojson", [_feature(i) for i in range(5)])
    for err in (MemoryError(), ModuleNotFoundError("No module named 'lazrs'")):

        def boom(err=err):
            raise err

        _patch_vector_read(monkeypatch, boom)
        with pytest.raises(type(err)):
            _read(tmp_path, path, "geojson_gbx", {"chunkSize": "2"})


def test_transient_io_errors_are_retried_then_fail_the_read(tmp_path, monkeypatch):
    import errno

    from databricks.labs.gbx.ds import _listing, vector

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    path = _geojson(tmp_path / "a.geojson", [_feature(i) for i in range(3)])
    real = vector.VectorGbxReader.partitions
    state = {"calls": 0, "fail": 1}

    def partitions(self):
        state["calls"] += 1
        if state["calls"] <= state["fail"]:
            raise OSError(errno.EIO, "Input/output error", path)
        return real(self)

    monkeypatch.setattr(vector.VectorGbxReader, "partitions", partitions)
    rows, recs = _read(tmp_path, path, "geojson_gbx")  # one miss: retried
    assert len(rows) == 3 and recs == []
    state.update(calls=0, fail=100)
    with pytest.raises(OSError, match="Input/output error"):
        _read(tmp_path, path, "geojson_gbx")
    # A transient miss, then a data error: still a bad record of the file.
    state.update(calls=0)

    def partitions_then_garbled(self):
        state["calls"] += 1
        if state["calls"] == 1:
            raise OSError(errno.EIO, "Input/output error", path)
        raise ValueError("garbled header")

    monkeypatch.setattr(vector.VectorGbxReader, "partitions", partitions_then_garbled)
    rows, (rec,) = _read(tmp_path, path, "geojson_gbx")
    assert rows == [] and rec["error_class"] == "ValueError"


def test_lidar_environment_errors_fail_the_read(tmp_path, monkeypatch):
    import laspy
    from laspy.errors import LaspyException

    from databricks.labs.gbx.ds import _listing

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    good, _, _ = _las(tmp_path / "good.las", 10)
    expected = P.path_schema("lidar_gbx", None, good)
    for err in (
        LaspyException("No LazBackend selected, cannot decompress data"),
        MemoryError(),
    ):

        def opener(*a, err=err, **k):
            raise err

        monkeypatch.setattr(laspy, "open", opener)
        bad = Q.BadRecords(str(tmp_path / "b"))
        want = Q.EnvironmentFailure if isinstance(err, LaspyException) else type(err)
        with pytest.raises(want):
            list(
                P.iter_path_batches(good, None, "lidar_gbx", None, expected, True, bad)
            )


def test_exif_image_too_large_for_qc_fails_the_read(tmp_path, monkeypatch):
    from databricks.labs.gbx.ds import file_gbx

    good = _jpg(tmp_path / "good.jpg")
    monkeypatch.setattr(file_gbx, "materialize_decision", lambda *a, **k: "reject")
    with pytest.raises(Q.FileTooLarge, match="qc"):
        _read(tmp_path, good, "exif_gbx", {"mode": "qc"})


def test_lidar_rereads_of_a_failing_chunk_are_bounded(tmp_path, monkeypatch):
    """A decoder that fails on every read, under a garbled huge point count,
    gives the file up after a bounded number of reads (one record), and a small
    always-failing file ends quickly too."""
    import laspy

    from databricks.labs.gbx.ds import lidar

    path, _, _ = _las(tmp_path / "a.las")
    calls = {"n": 0}

    def read_points(self, n):
        calls["n"] += 1
        raise _Panic("decoder panicked")

    monkeypatch.setattr(laspy.LasReader, "read_points", read_points)
    rows, recs = _read(tmp_path, path, "lidar_gbx", {"chunkSize": "100"})
    assert rows == [] and calls["n"] <= 110
    assert _classes(recs)[-1][0] == "points=0-4999"
    shutil.rmtree(tmp_path / "bad_records")

    calls["n"] = 0
    monkeypatch.setattr(lidar, "_readable_points", lambda *a: 10**9)
    rows, recs = _read(tmp_path, path, "lidar_gbx")
    assert rows == [] and calls["n"] <= 60
    assert [i for i, _ in _classes(recs)] == ["points=0-999999999"]


# --- fix round 2: what a bad record holds -------------------------------------


def test_raw_is_capped_and_binary_values_are_described():
    batch = pa.record_batch(
        [pa.array(["x" * 10_000]), pa.array([b"\x00" * 2_000_000])],
        names=["text", "payload"],
    )
    raw = Q.row_json(batch, 0)
    assert len(raw) <= 4096
    doc = json.loads(raw)
    assert doc["truncated"] is True
    small = pa.record_batch([pa.array([b"\x00" * 300])], names=["payload"])
    assert json.loads(Q.row_json(small, 0)) == {"payload": "<binary, 300 bytes>"}


def test_wkt_geometries_are_not_parsed_only_the_marker_is_checked(monkeypatch):
    import shapely as _shapely

    from databricks.labs.gbx.ds.vector import _UNREADABLE_WKB

    def no_parse(*a, **k):
        raise AssertionError("WKT parsed")

    monkeypatch.setattr(_shapely, "from_wkt", no_parse)
    batch = pa.record_batch(
        [
            pa.array(["POINT (1 1)", _UNREADABLE_WKB, None]),
            pa.array(["4326"] * 3),
            pa.array(["p"] * 3),
        ],
        names=["g", "g_srid", "g_srid_proj"],
    )
    assert list(Q.bad_rows(batch, "g")) == [1]


def test_skip_log_lines_name_the_error_class_once(caplog):
    import logging

    bad = Q.BadRecords(None)
    with caplog.at_level(logging.WARNING, logger=Q.__name__):
        bad.add("/v/a.gpkg", None, None, "ValueError", "ValueError: schema differs")
    (line,) = [r.getMessage() for r in caplog.records]
    assert line.count("ValueError") == 1


# --- fix round 3 -------------------------------------------------------------


def _failing_reads(monkeypatch, bad_lo, bad_hi):
    """laspy reads (and seeks) behave as if points [bad_lo, bad_hi) cannot be
    decoded: a read that overlaps them raises; returns a read counter."""
    import laspy

    real, real_seek = laspy.LasReader.read_points, laspy.LasReader.seek
    pos, calls = {}, {"n": 0}

    def read_points(self, n):
        calls["n"] += 1
        start = pos.get(id(self), 0)
        if start < bad_hi and bad_lo < start + n:
            raise _Panic("decoder panicked")
        out = real(self, n)
        pos[id(self)] = start + len(out)
        return out

    def seek(self, p, whence=0):
        pos[id(self)] = p
        return real_seek(self, p, whence)

    monkeypatch.setattr(laspy.LasReader, "read_points", read_points)
    monkeypatch.setattr(laspy.LasReader, "seek", seek)
    return calls


@pytest.mark.parametrize("chunk", [None, "10000", "5000"])
def test_one_undecodable_chunk_mid_file_keeps_every_other_point(
    tmp_path, monkeypatch, chunk
):
    """The review's probe: 300,000 points, 10,000-49,999 undecodable (one LAZ
    chunk from where it breaks). Every other point is read, whatever the
    chunkSize (also when the bad stretch spans several chunks), in a bounded
    number of reads."""
    path, _, _ = _las(tmp_path / "a.las", 300_000)
    calls = _failing_reads(monkeypatch, 10_000, 50_000)
    options = {"dimensions": "x", **({"chunkSize": chunk} if chunk else {})}
    rows, recs = _read(tmp_path, path, "lidar_gbx", options)
    assert len(rows) == 260_000
    ((index, cls),) = _classes(recs)
    assert index == "points=10000-49999" and cls.endswith("_Panic")
    assert calls["n"] <= 120


def test_lidar_reopen_environment_errors_fail_the_read(tmp_path, monkeypatch):
    import laspy

    path, _, _ = _las(tmp_path / "a.las", 3000)
    _failing_reads(monkeypatch, 1000, 2000)
    real_open = laspy.open
    opened = {"n": 0}

    def opener(*a, **k):
        opened["n"] += 1
        if opened["n"] > 1:  # the reopen after the failed chunk
            raise MemoryError
        return real_open(*a, **k)

    monkeypatch.setattr(laspy, "open", opener)
    with pytest.raises(MemoryError):
        _read(tmp_path, path, "lidar_gbx", {"chunkSize": "1000"})


def test_dbf_count_io_errors_fail_the_read(tmp_path, monkeypatch):
    import errno

    shp = _shapefile(tmp_path / "shp")
    dbf = shp[:-4] + ".dbf"
    n, header, reclen = _dbf_layout(dbf)
    with open(dbf, "r+b") as fh:
        fh.truncate(header + 15 * reclen)  # a shortfall: the .dbf is checked

    def eio(path):
        raise OSError(errno.EIO, "Input/output error", path)

    monkeypatch.setattr(Q, "dbf_deleted_count", eio)
    with pytest.raises(OSError, match="Input/output error"):
        _read(tmp_path, shp, "shapefile_gbx")


def test_jpeg_with_a_huge_declared_size_is_a_jpeg(tmp_path):
    """A JPEG whose frame header declares 192 MP makes PIL refuse to open it
    (DecompressionBombError); it is still a JPEG, and its metadata is read."""
    good = _jpg(tmp_path / "good.jpg")
    data = bytearray(open(good, "rb").read())
    sof = data.index(b"\xff\xc0")
    data[sof + 5 : sof + 9] = struct.pack(">HH", 12_000, 16_000)
    big = tmp_path / "big.jpg"
    big.write_bytes(bytes(data))
    rows, recs = _read(tmp_path, str(big), "exif_gbx", None, good)
    assert len(rows) == 1 and recs == []


def test_every_raw_is_capped(tmp_path):
    """A GeoJSONSeq line, or any raw a reader hands over, is capped too."""
    lines = [json.dumps(_feature(i)) for i in range(4)]
    lines[2] = '{"type": "Feature", "properties": {"note": "' + "x" * 20_000
    path = tmp_path / "a.geojsonl"
    path.write_text("\n".join(lines) + "\n")
    rows, (rec,) = _read(tmp_path, str(path), "geojsonl_gbx", None, None)
    assert len(rows) == 3 and rec["record_index"] == "line=3"
    assert len(rec["raw"]) <= 4096 and json.loads(rec["raw"])["truncated"] is True


def test_collections_of_surfaces_are_readable_geometries():
    gc_tin = struct.pack("<BII", 1, 7, 1) + _TIN
    gc_mixed = (
        struct.pack("<BII", 1, 7, 2)
        + struct.pack("<BIdd", 1, 1, 1.0, 2.0)
        + _POLYHEDRAL
    )
    gc_nested = struct.pack("<BII", 1, 7, 1) + gc_tin
    gc_big_endian = struct.pack(">BII", 0, 7, 1) + _TRIANGLE
    geoms = [gc_tin, gc_mixed, gc_nested, gc_big_endian, b"\x01\x07\x00\x00\x00\xff"]
    n = len(geoms)
    batch = pa.record_batch(
        [
            pa.array(geoms, pa.binary()),
            pa.array(["4326"] * n),
            pa.array(["p"] * n),
        ],
        names=["g", "g_srid", "g_srid_proj"],
    )
    assert list(Q.bad_rows(batch, "g")) == [n - 1]
