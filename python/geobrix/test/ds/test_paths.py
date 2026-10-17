"""read_paths: per-path reads over a DataFrame of file paths (batch + streaming)."""

import json
import os
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from databricks.labs.gbx.ds import paths as P

# Streaming file sources need an explicit schema (binaryFile's fixed layout).
_BINARY_SCHEMA = "path string, modificationTime timestamp, length long, content binary"


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
    q.awaitTermination()
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
    q.awaitTermination()
    got = spark.table("rp_raster_stream").collect()
    assert len(got) == 4 and {r[P.SOURCE_PATH_COL] for r in got} == {a}
