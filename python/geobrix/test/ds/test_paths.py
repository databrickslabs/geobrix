"""read_paths: per-path reads over a DataFrame of file paths (batch + streaming)."""

import json
import os
import shutil
from datetime import datetime, timezone

import pyarrow as pa
import pytest
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import paths as P

# Streaming file sources need an explicit schema (binaryFile's fixed layout).
_BINARY_SCHEMA = "path string, modificationTime timestamp, length long, content binary"

# Longest an availableNow test stream may run before it is stopped and failed.
_STREAM_TIMEOUT_S = 300


def _await(query):
    """Wait for an availableNow query; it is always stopped, and one still running
    after _STREAM_TIMEOUT_S fails the test instead of blocking the run."""
    try:
        done = query.awaitTermination(_STREAM_TIMEOUT_S)
        assert done, f"{query.name} still running: {query.status}"
    finally:
        query.stop()


def _geojson(path, n, extra_prop=None):
    feats = []
    for i in range(n):
        props = {"name": f"f{i}", "v": i}
        if extra_prop:
            props[extra_prop] = "x"
        feats.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": {"type": "Point", "coordinates": [i, i]},
            }
        )
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": feats}, f)
    return path


def _paths_df(spark, files):
    rows = [
        (
            f"dbfs:{p}" if p.startswith("/") else p,
            datetime.fromtimestamp(os.stat(p).st_mtime, tz=timezone.utc),
        )
        for p in files
    ]
    return spark.createDataFrame(rows, "path string, modificationTime timestamp")


def _batch_read(spark, fmt, path, **opts):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    r = spark.read.format(fmt)
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load(path)


def test_resolve_format_aliases_and_case():
    assert (
        P.resolve_format("gpkg_gbx")
        is P.resolve_format("GPKG")
        is P.resolve_format("gpkg")
    )
    with pytest.raises(ValueError, match="Supported"):
        P.resolve_format("cog_gbx_nope")


def test_every_supported_format_is_a_registered_reader():
    for name in P.SUPPORTED_FORMATS:
        assert P.resolve_format(name).name() == name


def test_iter_path_batches_handles_timestamp_columns(exif_jpeg):
    # exif_gbx has a TIMESTAMP column; the expected Arrow schema needs a timezone.
    batches = list(P.iter_path_batches(str(exif_jpeg), 1_000_000, "exif_gbx", {}))
    assert sum(b.num_rows for b in batches) == 1
    assert batches[0].schema.field("timestamp").type == pa.timestamp("us", tz="UTC")


def test_resolve_format_rejects_unsupported_reader():
    with pytest.raises(ValueError, match="file_gbx"):
        P.resolve_format("file_gbx")


def test_read_paths_geojson_matches_batch_reader(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 3)
    b = _geojson(str(tmp_path / "b.geojson"), 2)
    got = P.read_paths(_paths_df(spark, [a, b]), "geojson_gbx")
    want = _batch_read(spark, "geojson_gbx", str(tmp_path))
    cols = [c for c in got.columns if not c.startswith("_source_")]
    assert sorted(cols) == sorted(want.columns)
    assert got.select(cols).exceptAll(want.select(cols)).count() == 0
    assert got.count() == want.count() == 5


def test_read_paths_lineage_strips_scheme_and_keeps_mtime(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 2)
    got = P.read_paths(_paths_df(spark, [a]), "geojson").collect()
    assert {r[P.SOURCE_PATH_COL] for r in got} == {a}
    want = int(os.stat(a).st_mtime)
    assert all(int(r[P.SOURCE_MTIME_COL].timestamp()) == want for r in got)


def test_read_paths_no_mtime_col_gives_null(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    df = spark.createDataFrame([(a,)], "path string")
    got = P.read_paths(df, "geojson_gbx", mtime_col=None).collect()
    assert got[0][P.SOURCE_MTIME_COL] is None


def test_read_paths_lineage_off(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    got = P.read_paths(_paths_df(spark, [a]), "geojson_gbx", lineage=False)
    assert not [c for c in got.columns if c.startswith("_source_")]


def test_read_paths_gpkg_layer_option(spark, tmp_path):
    import pyogrio
    import shapely

    p = str(tmp_path / "z.gpkg")
    tbl = pa.table(
        {"geom": shapely.to_wkb(shapely.points([0, 1], [0, 1])), "k": [1, 2]}
    )
    pyogrio.write_arrow(
        tbl,
        p,
        layer="pts",
        driver="GPKG",
        geometry_name="geom",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    got = P.read_paths(_paths_df(spark, [p]), "gpkg_gbx", {"layerName": "pts"})
    assert got.count() == 2


def test_read_paths_lidar(spark, tmp_path):
    import laspy
    import numpy as np

    hdr = laspy.LasHeader(point_format=3, version="1.4")
    hdr.offsets, hdr.scales = [0.0, 0.0, 0.0], [0.01, 0.01, 0.01]
    las = laspy.LasData(hdr)
    las.x = np.arange(10.0)
    las.y = np.arange(10.0)
    las.z = np.arange(10.0)
    p = str(tmp_path / "t.las")
    las.write(p)
    got = P.read_paths(_paths_df(spark, [p]), "lidar_gbx")
    want = _batch_read(spark, "lidar_gbx", p)
    assert got.count() == want.count() == 10


def test_read_paths_exif(spark, exif_jpeg):
    got = P.read_paths(_paths_df(spark, [str(exif_jpeg)]), "exif_gbx").collect()
    assert len(got) == 1 and got[0][P.SOURCE_PATH_COL] == str(exif_jpeg)


def test_read_paths_schema_mismatch_names_file(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    b = _geojson(str(tmp_path / "b.geojson"), 1, extra_prop="surprise")
    with pytest.raises(Exception, match=r"b\.geojson.*surprise"):
        P.read_paths(_paths_df(spark, [a, b]), "geojson_gbx", schema_path=a).collect()


def test_read_paths_streaming_requires_schema_path(spark, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    stream = spark.readStream.format("binaryFile").schema(_BINARY_SCHEMA).load(str(d))
    with pytest.raises(ValueError, match="schema_path"):
        P.read_paths(stream, "geojson_gbx")


def test_read_paths_streaming_available_now(spark, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    a = _geojson(str(d / "a.geojson"), 3)
    stream = (
        spark.readStream.format("binaryFile")
        .schema(_BINARY_SCHEMA)
        .option("pathGlobFilter", "*.geojson")
        .load(str(d))
        .select("path", "modificationTime")
    )
    out = P.read_paths(stream, "geojson_gbx", schema_path=a)
    q = (
        out.writeStream.format("memory")
        .queryName("rp_stream")
        .trigger(availableNow=True)
        .option("checkpointLocation", str(tmp_path / "ck"))
        .start()
    )
    _await(q)
    assert spark.table("rp_stream").count() == 3


def test_read_paths_num_partitions(spark, tmp_path):
    files = [_geojson(str(tmp_path / f"{i}.geojson"), 1) for i in range(4)]
    got = P.read_paths(_paths_df(spark, files), "geojson_gbx", num_partitions=3)
    assert (
        got.rdd.getNumPartitions() == 3
    )  # test-only RDD use; the module never touches .rdd


def test_read_paths_empty_df(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    empty = spark.createDataFrame([], "path string, modificationTime timestamp")
    assert P.read_paths(empty, "geojson_gbx", schema_path=a).count() == 0


def test_read_paths_zero_feature_file(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 2)
    z = _geojson(str(tmp_path / "z.geojson"), 0)
    assert (
        P.read_paths(_paths_df(spark, [a, z]), "geojson_gbx", schema_path=a).count()
        == 2
    )


def test_iter_path_batches_missing_file_skips(tmp_path, caplog):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    expected = P.path_schema("geojson_gbx", None, a)
    gone = str(tmp_path / "gone.geojson")
    assert list(P.iter_path_batches(gone, None, "geojson_gbx", None, expected)) == []
    assert "gone.geojson" in caplog.text


def _geojson_props(path, props):
    feat = {
        "type": "Feature",
        "properties": props,
        "geometry": {"type": "Point", "coordinates": [0, 0]},
    }
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": [feat]}, f)
    return path


def test_read_paths_reorders_columns_by_name(spark, tmp_path):
    a = _geojson_props(str(tmp_path / "a.geojson"), {"a": "A1", "b": "B1"})
    b = _geojson_props(str(tmp_path / "b.geojson"), {"b": "B2", "a": "A2"})
    got = P.read_paths(_paths_df(spark, [a, b]), "geojson_gbx", schema_path=a).collect()
    by_file = {r[P.SOURCE_PATH_COL]: (r["a"], r["b"]) for r in got}
    assert by_file == {a: ("A1", "B1"), b: ("A2", "B2")}


def test_norm_type_accepts_large_variants():
    assert P._norm_type(pa.large_string()) == pa.string()
    assert P._norm_type(pa.large_binary()) == pa.binary()
    assert P._norm_type(pa.large_list(pa.large_string())) == pa.list_(pa.string())


def test_check_schema_accepts_large_and_plain_variants():
    got = pa.schema(
        [
            ("s", pa.large_string()),
            ("b", pa.large_binary()),
            ("l", pa.large_list(pa.int32())),
        ]
    )
    want = pa.schema(
        [("s", pa.string()), ("b", pa.binary()), ("l", pa.list_(pa.int32()))]
    )
    P._check_schema("x.geojson", got, want)


def test_check_schema_changed_type_names_path_and_field():
    got = pa.schema([("k", pa.int64())])
    want = pa.schema([("k", pa.string())])
    with pytest.raises(ValueError, match=r"x\.geojson.*changed types \['k'\]"):
        P._check_schema("x.geojson", got, want)


def test_check_schema_missing_and_extra_fields():
    want = pa.schema([("a", pa.string()), ("b", pa.string())])
    with pytest.raises(ValueError, match=r"x\.geojson.*missing \['b'\]"):
        P._check_schema("x.geojson", pa.schema([("a", pa.string())]), want)
    with pytest.raises(ValueError, match=r"x\.geojson.*extra \['c'\]"):
        P._check_schema("x.geojson", pa.schema(list(want) + [("c", pa.string())]), want)


def _tif(path, w=64, h=64, value=1):
    import numpy as np
    import rasterio

    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=rasterio.Affine(1, 0, 0, 0, -1, h),
    )
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.full((1, h, w), value, dtype="uint8"))
    return path


def _set_op_safe(df, cols):
    """exceptAll rejects MAP columns; compare tile.metadata as sorted entries."""
    from pyspark.sql import functions as F

    meta = F.array_sort(F.map_entries("tile.metadata"))
    return df.select(cols).withColumn("tile", F.col("tile").withField("metadata", meta))


def test_read_paths_gtiff_matches_batch_reader(spark, tmp_path):
    a = _tif(str(tmp_path / "a.tif"))
    got = P.read_paths(_paths_df(spark, [a]), "gtiff_gbx")
    want = _batch_read(spark, "gtiff_gbx", a)
    cols = [c for c in got.columns if not c.startswith("_source_")]
    assert got.count() == want.count()
    assert _set_op_safe(got, cols).exceptAll(_set_op_safe(want, cols)).count() == 0


def test_read_paths_raster_splits_a_large_file_across_partitions(spark, tmp_path):
    from pyspark.sql import functions as F

    a = _tif(str(tmp_path / "big.tif"), w=2048, h=2048)
    opts = {"tileSize": "512"}  # 16 deterministic windows from one file
    want_df = _batch_read(spark, "raster_gbx", a, **opts)
    want = want_df.count()
    got = P.read_paths(_paths_df(spark, [a]), "raster_gbx", opts, num_partitions=4)
    assert want == 16 and got.count() == want
    cols = [c for c in got.columns if not c.startswith("_source_")]
    assert _set_op_safe(got, cols).exceptAll(_set_op_safe(want_df, cols)).count() == 0
    assert got.rdd.getNumPartitions() == 4  # windows spread after the plan stage
    # One file's windows land in more than one task (not just 4 empty slots).
    assert got.select(F.spark_partition_id()).distinct().count() > 1


def test_iter_path_batches_converts_raster_rows(tmp_path):
    # The connector path: one file, every window, tuple rows -> Arrow batches.
    a = _tif(str(tmp_path / "a.tif"))
    expected = P.path_schema("gtiff_gbx", {"tileSize": "32"}, a)
    batches = list(
        P.iter_path_batches(a, 1_000_000, "gtiff_gbx", {"tileSize": "32"}, expected)
    )
    assert sum(b.num_rows for b in batches) == 4
    assert batches[0].schema.names == [f.name for f in expected.fields]
    tiles = [t for b in batches for t in b.column("tile").to_pylist()]
    assert {(t["window"]["col_off"], t["window"]["row_off"]) for t in tiles} == {
        (0, 0),
        (32, 0),
        (0, 32),
        (32, 32),
    }


def test_plan_rows_round_trip_raster_partitions(tmp_path, caplog):
    import pickle

    a = _tif(str(tmp_path / "a.tif"))
    gone = str(tmp_path / "gone.tif")
    src = pa.RecordBatch.from_arrays(
        [pa.array([a, gone]), pa.array([1_000_000, None], P._TS)],
        names=["path", "mtime"],
    )
    schema = P.path_schema("raster_gbx", None, a, lineage=False)
    plan = P._plan_batches("raster_gbx", {"tileSize": "32"}, schema)
    rows = [r for b in plan(iter([src])) for r in b.to_pylist()]
    assert {r["path"] for r in rows} == {a} and len(rows) == 4
    assert "gone.tif" in caplog.text
    parts = [pickle.loads(r["part"]) for r in rows]
    assert sorted(p.window for p in parts) == [
        (0, 0, 32, 32),
        (0, 32, 32, 32),
        (32, 0, 32, 32),
        (32, 32, 32, 32),
    ]
    assert all(p.file_path == a for p in parts)


def test_read_paths_raster_streaming_two_stage(spark, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    a = _tif(str(d / "a.tif"))
    stream = (
        spark.readStream.format("binaryFile")
        .schema(_BINARY_SCHEMA)
        .option("pathGlobFilter", "*.tif")
        .load(str(d))
        .select("path", "modificationTime")
    )
    out = P.read_paths(
        stream, "gtiff_gbx", {"tileSize": "32"}, schema_path=a, num_partitions=2
    )
    q = (
        out.writeStream.format("memory")
        .queryName("rp_raster_stream")
        .trigger(availableNow=True)
        .option("checkpointLocation", str(tmp_path / "ck"))
        .start()
    )
    _await(q)
    got = spark.table("rp_raster_stream").collect()
    assert len(got) == 4 and {r[P.SOURCE_PATH_COL] for r in got} == {a}


def _plan_src(*paths_mtimes):
    paths, mtimes = zip(*paths_mtimes)
    return pa.RecordBatch.from_arrays(
        [pa.array(list(paths)), pa.array(list(mtimes), P._TS)],
        names=["path", "mtime"],
    )


def test_plan_stage_flushes_at_the_row_cap(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(P, "_PLAN_ROWS_PER_BATCH", 3)
    a = _tif(str(tmp_path / "big.tif"), w=2048, h=2048)
    opts = {"tileSize": "512"}
    schema = P.path_schema("raster_gbx", opts, a, lineage=False)
    plan = list(P._plan_batches("raster_gbx", opts, schema)(iter([_plan_src((a, 1))])))
    assert [b.num_rows for b in plan] == [3, 3, 3, 3, 3, 1]
    # The cap travels with the plan function, so the Spark read flushes too.
    got = P.read_paths(_paths_df(spark, [a]), "raster_gbx", opts, num_partitions=4)
    want = _batch_read(spark, "raster_gbx", a, **opts)
    cols = [c for c in got.columns if not c.startswith("_source_")]
    assert got.count() == want.count() == 16
    assert _set_op_safe(got, cols).exceptAll(_set_op_safe(want, cols)).count() == 0


def test_read_paths_raster_lineage_survives_the_shuffle(spark, tmp_path):
    a = _tif(str(tmp_path / "a.tif"), value=1)
    b = _tif(str(tmp_path / "b.tif"), value=2)
    os.utime(a, (1_600_000_000, 1_600_000_000))
    os.utime(b, (1_700_000_000, 1_700_000_000))
    got = P.read_paths(
        _paths_df(spark, [a, b]), "gtiff_gbx", {"tileSize": "32"}, num_partitions=2
    ).collect()
    assert len(got) == 8
    mtime = {a: 1_600_000_000, b: 1_700_000_000}
    for r in got:
        assert r[P.SOURCE_PATH_COL] == r["tile"]["path"]
        assert int(r[P.SOURCE_MTIME_COL].timestamp()) == mtime[r["tile"]["path"]]
    assert {r[P.SOURCE_PATH_COL] for r in got} == {a, b}


def test_read_stage_skips_a_file_deleted_after_planning(tmp_path, caplog, monkeypatch):
    # One stat attempt: the FUSE retry backoff is not under test here.
    monkeypatch.setattr(P._listing, "_retry_transient", lambda fn: fn())
    a = _tif(str(tmp_path / "a.tif"))
    b = _tif(str(tmp_path / "b.tif"))
    opts = {"tileSize": "32"}
    schema = P.path_schema("gtiff_gbx", opts, a)
    plan = list(
        P._plan_batches("gtiff_gbx", opts, schema)(
            iter([_plan_src((a, 1_000_000), (b, 2_000_000))])
        )
    )
    assert sum(x.num_rows for x in plan) == 8
    os.remove(b)
    out = list(P._read_planned("gtiff_gbx", opts, schema, True)(iter(plan)))
    paths = [p for x in out for p in x.column(P.SOURCE_PATH_COL).to_pylist()]
    assert paths == [a] * 4
    assert "b.tif" in caplog.text and "no longer exists" in caplog.text


@pytest.mark.parametrize("opt", ["manifest", "tilesTable", "TILESTABLE"])
def test_read_paths_rejects_manifest_options_for_raster(spark, tmp_path, opt):
    a = _tif(str(tmp_path / "a.tif"))
    with pytest.raises(ValueError, match=f"(?i){opt}"):
        P.read_paths(_paths_df(spark, [a]), "raster_gbx", {opt: "x"})


# --- schema file pick (folder schema_path) ----------------------------------


def _schema_names(schema):
    return [f.name for f in schema.fields]


def test_path_schema_folder_opens_only_the_first_matching_file(tmp_path, monkeypatch):
    d = tmp_path / "landing"
    d.mkdir()
    (d / "0_notes.txt").write_text("not a vector file")  # sorts first, no match
    (d / "_hidden.geojson").write_text("{broken")  # hidden: skipped
    a = _geojson(str(d / "a.geojson"), 1)
    _geojson(str(d / "b.geojson"), 1, extra_prop="surprise")  # drifted
    opened = []
    real = P._reader_schema

    def spy(fmt, options, path):
        opened.append(path)
        return real(fmt, options, path)

    monkeypatch.setattr(P, "_reader_schema", spy)
    schema = P.path_schema("geojson_gbx", None, str(d))
    assert opened == [a]
    assert "surprise" not in _schema_names(schema)
    assert _schema_names(schema)[-2:] == [P.SOURCE_PATH_COL, P.SOURCE_MTIME_COL]


def _shapefile(path, cols):
    import pyogrio
    import shapely

    tbl = pa.table({"geometry": shapely.to_wkb(shapely.points([0, 1], [0, 1])), **cols})
    pyogrio.write_arrow(
        tbl,
        path,
        driver="ESRI Shapefile",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return path


def test_path_schema_folder_of_shapefiles_with_different_schemas(tmp_path):
    # The shapefile reader rejects a folder whose members differ; the schema pick
    # opens only the first .shp, so a later drifted file cannot block it.
    d = tmp_path / "shp"
    d.mkdir()
    _shapefile(str(d / "a.shp"), {"k": [1, 2]})
    _shapefile(str(d / "b.shp"), {"other": ["x", "y"]})
    names = _schema_names(P.path_schema("shapefile_gbx", None, str(d)))
    assert "k" in names and "other" not in names


def test_path_schema_folder_honors_glob(tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    _geojson(str(d / "a_old.geojson"), 1, extra_prop="legacy")  # sorts first
    _geojson(str(d / "b.geojson"), 1)
    assert "legacy" in _schema_names(P.path_schema("geojson_gbx", None, str(d)))
    names = _schema_names(P.path_schema("geojson_gbx", None, str(d), glob="b*"))
    assert "legacy" not in names and "name" in names


def test_path_schema_folder_without_a_match_names_the_pattern(tmp_path):
    (tmp_path / "x.txt").write_text("x")
    with pytest.raises(ValueError, match=r"\*\.\{geojson,json,GEOJSON,JSON\}"):
        P.path_schema("geojson_gbx", None, str(tmp_path))


def _gdb(path, cols):
    """An unzipped File Geodatabase folder with one point layer."""
    import pyogrio
    import shapely

    tbl = pa.table({"SHAPE": shapely.to_wkb(shapely.points([0], [0])), **cols})
    pyogrio.write_arrow(
        tbl,
        path,
        layer="pts",
        driver="OpenFileGDB",
        geometry_name="SHAPE",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return path


def test_path_schema_gdb_folder_is_one_dataset(tmp_path):
    gdb = _gdb(str(tmp_path / "survey.gdb"), {"k": [1]})
    assert "k" in _schema_names(P.path_schema("file_gdb_gbx", None, gdb))


def test_path_schema_folder_of_gdb_folders_takes_the_first_gdb(tmp_path):
    """A folder of unzipped .gdb folders: the first .gdb folder (sorted) is the
    schema dataset for the readers that read one; its files are never matched
    one by one."""
    d = tmp_path / "gdbs"
    d.mkdir()
    _gdb(str(d / "b.gdb"), {"other": [1]})
    _gdb(str(d / "a.gdb"), {"k": [1]})
    _gdb(str(d / "_hidden.gdb"), {"hidden": [1]})
    for fmt in ("file_gdb_gbx", "vector_gbx"):
        assert P._schema_file(fmt, str(d)) == str(d / "a.gdb"), fmt
        names = _schema_names(P.path_schema(fmt, None, str(d)))
        assert "k" in names and "other" not in names, fmt
    # A zipped .gdb.zip that sorts first is still taken first.
    shutil.make_archive(str(d / "0"), "zip", str(d), "b.gdb")
    os.rename(str(d / "0.zip"), str(d / "0.gdb.zip"))
    assert P._schema_file("file_gdb_gbx", str(d)) == str(d / "0.gdb.zip")
    # Other readers do not take a .gdb folder.
    with pytest.raises(ValueError, match="No file under"):
        P.path_schema("gpkg_gbx", None, str(d))


def test_read_paths_schema_kwarg_skips_schema_resolution(spark, tmp_path, monkeypatch):
    a = _geojson(str(tmp_path / "a.geojson"), 2)
    schema = P.path_schema("geojson_gbx", None, a, lineage=False)

    def boom(*a, **k):
        raise AssertionError("schema resolved again")

    monkeypatch.setattr(P, "path_schema", boom)
    monkeypatch.setattr(P, "_reader_schema", boom)
    got = P.read_paths(_paths_df(spark, [a]), "geojson_gbx", schema=schema)
    assert got.columns[-2:] == [P.SOURCE_PATH_COL, P.SOURCE_MTIME_COL]
    assert got.count() == 2
    off = P.read_paths(
        _paths_df(spark, [a]), "geojson_gbx", schema=schema, lineage=False
    )
    assert off.columns == _schema_names(schema)
    # A stream needs no schema_path when the schema is given.
    d = tmp_path / "in"
    d.mkdir()
    stream = spark.readStream.format("binaryFile").schema(_BINARY_SCHEMA).load(str(d))
    full = StructType(list(schema.fields) + P._lineage_fields())
    assert P.read_paths(stream, "geojson_gbx", schema=full).isStreaming


# --- default file patterns (Hadoop glob syntax) ------------------------------

_GLOB_NAMES = [
    "a.gpkg",
    "a.gpkg-journal",
    "a.geojson",
    "a.json",
    "a.geojsonl",
    "a.geojsons",
    "a.shp",
    "a.shx",
    "a.dbf",
    "a.prj",
    "a.cpg",
    "a.shz",
    "a.shp.zip",
    "a.zip",
    "a.gdb.zip",
    "a00000001.gdbtable",
    "a.las",
    "a.laz",
    "a.jpg",
    "a.jpeg",
    "a.JPG",
    "a.tif",
    "a.tiff",
    "a.TIF",
    "a.tif.aux.xml",
    "a.ovr",
    "notes.txt",
    # Upper-case extensions are common (camera / drone imagery); mixed case is not.
    "b.JPEG",
    "b.Jpg",
    "b.TIFF",
    "b.LAS",
    "b.LAZ",
    "b.SHP",
    "b.SHX",
    "b.GPKG",
    "b.GEOJSON",
    "b.GDB.ZIP",
]


def test_default_globs_per_format():
    assert P.DEFAULT_GLOBS == {
        "vector_gbx": (
            "*.{gpkg,geojson,json,geojsonl,geojsons,shp,shz,shp.zip,gdb.zip,"
            "GPKG,GEOJSON,JSON,GEOJSONL,GEOJSONS,SHP,SHZ,SHP.ZIP,GDB.ZIP}"
        ),
        "shapefile_gbx": "*.{shp,shz,shp.zip,SHP,SHZ,SHP.ZIP}",
        "geojson_gbx": "*.{geojson,json,GEOJSON,JSON}",
        "geojsonl_gbx": "*.{geojsonl,geojsons,GEOJSONL,GEOJSONS}",
        "gpkg_gbx": "*.{gpkg,GPKG}",
        "file_gdb_gbx": "*.{gdb.zip,GDB.ZIP}",
        "lidar_gbx": "*.{las,laz,LAS,LAZ}",
        "exif_gbx": "*.{jpg,jpeg,JPG,JPEG}",
        "raster_gbx": "*.{tif,tiff,TIF,TIFF}",
        "gtiff_gbx": "*.{tif,tiff,TIF,TIFF}",
    }
    assert set(P.DEFAULT_GLOBS) == set(P.SUPPORTED_FORMATS)
    assert P.default_glob("GPKG") == "*.{gpkg,GPKG}"


def test_vector_glob_is_the_union_of_the_vector_globs():
    vector = [
        "shapefile_gbx",
        "geojson_gbx",
        "geojsonl_gbx",
        "gpkg_gbx",
        "file_gdb_gbx",
    ]
    for name in _GLOB_NAMES:
        assert P.glob_match(name, P.DEFAULT_GLOBS["vector_gbx"]) == any(
            P.glob_match(name, P.DEFAULT_GLOBS[f]) for f in vector
        ), name


def test_glob_match_agrees_with_spark_path_glob_filter(spark, tmp_path):
    """The default patterns are Spark pathGlobFilter (Hadoop glob) patterns, and
    glob_match (the schema-file pick) matches exactly what Spark lists."""
    for name in _GLOB_NAMES:
        (tmp_path / name).write_bytes(b"x")
    expected = {
        "shapefile_gbx": {"a.shp", "a.shz", "a.shp.zip", "b.SHP"},
        "geojson_gbx": {"a.geojson", "a.json", "b.GEOJSON"},
        # Lower and upper case match; mixed case (b.Jpg) does not.
        "exif_gbx": {"a.jpg", "a.jpeg", "a.JPG", "b.JPEG"},
        "gtiff_gbx": {"a.tif", "a.tiff", "a.TIF", "b.TIFF"},
        "lidar_gbx": {"a.las", "a.laz", "b.LAS", "b.LAZ"},
        "gpkg_gbx": {"a.gpkg", "b.GPKG"},
        "file_gdb_gbx": {"a.gdb.zip", "b.GDB.ZIP"},
    }
    for fmt, glob in P.DEFAULT_GLOBS.items():
        listed = {
            os.path.basename(r[0])
            for r in spark.read.format("binaryFile")
            .option("pathGlobFilter", glob)
            .load(str(tmp_path))
            .select("path")
            .collect()
        }
        assert listed == {n for n in _GLOB_NAMES if P.glob_match(n, glob)}, fmt
        assert listed, fmt
        if fmt in expected:
            assert listed == expected[fmt], fmt

    # Backslash escapes: Hadoop copies "\\X" into its regex, so an escaped glob
    # character is literal and an escaped letter is a regex class.
    esc = tmp_path / "esc"
    esc.mkdir()
    for name in _ESCAPE_NAMES:
        (esc / name).write_bytes(b"x")
    for glob, want in _ESCAPE_GLOBS.items():
        listed = {
            os.path.basename(r[0])
            for r in spark.read.format("binaryFile")
            .option("pathGlobFilter", glob)
            .load(str(esc))
            .select("path")
            .collect()
        }
        assert listed == want, glob
        assert {n for n in _ESCAPE_NAMES if P.glob_match(n, glob)} == want, glob


_ESCAPE_NAMES = [
    "a*.x",
    "ab.x",
    "a1.x",
    "ad.x",
    "a?.x",
    "a[1].x",
    "a{b}.x",
    "a,b.x",
    "a b.x",
    "a.x",
]

_ESCAPE_GLOBS = {
    r"a\*.x": {"a*.x"},
    "a*.x": set(_ESCAPE_NAMES),
    r"a\?.x": {"a?.x"},
    "a?.x": {"a*.x", "ab.x", "a1.x", "ad.x", "a?.x"},
    r"a\[1\].x": {"a[1].x"},
    "a[1].x": {"a1.x"},
    r"a\{b\}.x": {"a{b}.x"},
    r"{a\,b,ab}.x": {"a,b.x", "ab.x"},
    r"a\.x": {"a.x"},
    r"a\d.x": {"a1.x"},
    r"a[\d].x": {"a1.x"},
    r"a\w.x": {"ab.x", "a1.x", "ad.x"},
    r"a\sb.x": {"a b.x"},
}


@pytest.mark.parametrize(
    "glob", [r"a\1", r"a\p{L}", r"\Qa\E", r"a\b", "a\\\u00e9", "a[\\S]"]
)
def test_glob_escapes_without_an_exact_python_match_are_rejected(glob):
    with pytest.raises(ValueError, match="unsupported escape"):
        P.glob_match("a", glob)


@pytest.mark.parametrize("glob", ["a\\", "a[b", "{a,b", "[]"])
def test_malformed_globs_are_rejected(glob):
    with pytest.raises(ValueError, match="glob"):
        P.glob_match("a", glob)


# --- object-store URIs -------------------------------------------------------

_URIS = [
    "s3://bucket/a.geojson",
    "abfss://c@acct.dfs.core.windows.net/a.geojson",
    "gs://b/a.geojson",
    # GDAL virtual file systems: os.stat cannot see them either.
    "/vsis3/bucket/a.geojson",
    "/vsizip//Volumes/c/s/v/a.zip/a.geojson",
    "/vsicurl/https://host/a.geojson",
    # In-memory GDAL files are no file-ingestion input either.
    "/vsimem/a.geojson",
]


@pytest.mark.parametrize("uri", _URIS)
def test_object_store_uris_are_rejected_without_retrying(tmp_path, monkeypatch, uri):
    tries = []
    monkeypatch.setattr(P._listing, "_retry_transient", lambda fn: tries.append(1))
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    expected = P.path_schema("geojson_gbx", None, a)
    msg = "read_paths reads Unity Catalog Volume / FUSE paths; got " + uri
    with pytest.raises(ValueError, match=msg.replace(".", r"\.")):
        P._exists(uri)
    with pytest.raises(ValueError, match="Unity Catalog Volume"):
        list(P.iter_path_batches(uri, None, "geojson_gbx", None, expected))
    with pytest.raises(ValueError, match="Unity Catalog Volume"):
        P.path_schema("geojson_gbx", None, uri)
    tif = _tif(str(tmp_path / "a.tif"))
    rschema = P.path_schema("gtiff_gbx", None, tif)
    with pytest.raises(ValueError, match="Unity Catalog Volume"):
        list(P._plan_batches("gtiff_gbx", {}, rschema)(iter([_plan_src((uri, 1))])))
    planned = pa.RecordBatch.from_arrays(
        [pa.array([uri]), pa.array([1], P._TS), pa.array([b"x"], pa.binary())],
        names=["path", "mtime", "part"],
    )
    with pytest.raises(ValueError, match="Unity Catalog Volume"):
        list(P._read_planned("gtiff_gbx", {}, rschema, True)(iter([planned])))
    assert tries == []


def test_read_paths_object_store_uri_fails_the_query(spark, tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    df = spark.createDataFrame(
        [("s3://bucket/b.geojson", None)], "path string, modificationTime timestamp"
    )
    with pytest.raises(Exception, match="Unity Catalog Volume / FUSE paths"):
        P.read_paths(df, "geojson_gbx", schema_path=a).collect()


def test_scheme_prefixed_local_paths_are_accepted(tmp_path):
    a = _geojson(str(tmp_path / "a.geojson"), 1)
    for p in ("dbfs:" + a, "file:" + a, "file://" + a, a):
        assert P._exists(P._fuse_path(p))


# --- streaming increments and exact batch-reader parity ----------------------


def test_read_paths_stream_second_run_reads_only_the_new_file(spark, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    a = _geojson(str(d / "a.geojson"), 3)
    out, ck = str(tmp_path / "out"), str(tmp_path / "ck")

    def run():
        stream = (
            spark.readStream.format("binaryFile")
            .schema(_BINARY_SCHEMA)
            .option("pathGlobFilter", "*.geojson")
            .load(str(d))
            .select("path", "modificationTime")
        )
        _await(
            P.read_paths(stream, "geojson_gbx", schema_path=a)
            .writeStream.format("parquet")
            .option("path", out)
            .option("checkpointLocation", ck)
            .trigger(availableNow=True)
            .start()
        )
        rows = spark.read.parquet(out).select(P.SOURCE_PATH_COL).collect()
        return sorted(r[0] for r in rows)

    assert run() == [a] * 3
    b = _geojson(str(d / "b.geojson"), 2)
    assert run() == [a] * 3 + [b] * 2  # run 2 added only b's rows


def _comparable(df):
    """exceptAll rejects MAP columns: compare maps (top level or one struct level
    down) as sorted entry arrays."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import MapType

    cols = []
    for f in df.schema.fields:
        if isinstance(f.dataType, MapType):
            cols.append(F.array_sort(F.map_entries(f.name)).alias(f.name))
        elif isinstance(f.dataType, StructType) and any(
            isinstance(s.dataType, MapType) for s in f.dataType.fields
        ):
            c = F.col(f.name)
            for s in f.dataType.fields:
                if isinstance(s.dataType, MapType):
                    c = c.withField(
                        s.name, F.array_sort(F.map_entries(f"{f.name}.{s.name}"))
                    )
            cols.append(c.alias(f.name))
        else:
            cols.append(F.col(f.name))
    return df.select(cols)


def _gpkg_file(path):
    import pyogrio
    import shapely

    tbl = pa.table(
        {
            "geom": shapely.to_wkb(shapely.points([0, 1, 2], [0, 1, 2])),
            "k": [1, 2, 3],
            "s": ["a", "b", "c"],
        }
    )
    pyogrio.write_arrow(
        tbl,
        path,
        layer="pts",
        driver="GPKG",
        geometry_name="geom",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return path


def _las_file(path):
    import laspy
    import numpy as np

    hdr = laspy.LasHeader(point_format=3, version="1.4")
    hdr.offsets, hdr.scales = [0.0, 0.0, 0.0], [0.01, 0.01, 0.01]
    las = laspy.LasData(hdr)
    las.x = np.arange(10.0)
    las.y = np.arange(10.0) * 2
    las.z = np.arange(10.0) * 3
    las.write(path)
    return path


@pytest.mark.parametrize(
    "fmt,make",
    [
        ("gpkg_gbx", lambda d: [_gpkg_file(f"{d}/a.gpkg"), _gpkg_file(f"{d}/b.gpkg")]),
        (
            "shapefile_gbx",
            lambda d: [
                _shapefile(f"{d}/a.shp", {"k": [1, 2]}),
                _shapefile(f"{d}/b.shp", {"k": [3, 4]}),
            ],
        ),
        ("lidar_gbx", lambda d: [_las_file(f"{d}/a.las"), _las_file(f"{d}/b.las")]),
    ],
)
def test_read_paths_equals_the_batch_reader(spark, tmp_path, fmt, make):
    files = make(str(tmp_path))
    got = P.read_paths(_paths_df(spark, files), fmt)
    want = _batch_read(spark, fmt, str(tmp_path))
    cols = [c for c in got.columns if not c.startswith("_source_")]
    assert cols == want.columns
    g, w = _comparable(got.select(cols)), _comparable(want.select(cols))
    assert got.count() == want.count() > 0
    assert g.exceptAll(w).count() == 0 and w.exceptAll(g).count() == 0
