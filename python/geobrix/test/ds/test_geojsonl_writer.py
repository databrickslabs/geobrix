"""Tests for the multi-file GeoJSONL writer (geojsonl_gbx).

Unlike the single-file vector writers, geojsonl_gbx writes a DIRECTORY of
newline-delimited GeoJSONL shards -- one per non-empty partition, NO driver
merge. An optional maxRecordsPerFile splits a large partition into multiple
shards. Round-trips with the geojson_gbx (multi=true) directory reader.
"""

import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType
from shapely import Point
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds.register import register


def _wkb_df(spark, n=6):
    rows = [
        (str(i), i, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(n)
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    )


def _shards(out):
    return [n for n in os.listdir(out) if n.endswith((".geojsonl", ".geojsons"))]


def test_writer_arrow_table_all_null_column_keeps_declared_type():
    # An attribute column that is entirely null in a partition must keep its
    # declared type (string), not collapse to Arrow 'null' type -- pyogrio/GDAL
    # rejects a null-typed field ("Type 'n' for field ... is not supported").
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import _writer_arrow_table

    schema = StructType(
        [
            StructField("tlid", LongType(), True),
            StructField("plus4", StringType(), True),  # all null below
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    cols = {
        "tlid": [1, 2],
        "plus4": [None, None],
        "geom_0": [bytes(to_wkb(Point(1.0, 2.0))), bytes(to_wkb(Point(3.0, 4.0)))],
        "geom_0_srid": ["4326", "4326"],
        "geom_0_srid_proj": ["", ""],
    }
    tbl = _writer_arrow_table(cols, schema, "geom_0")
    assert tbl.schema.field("plus4").type == pa.string()  # NOT pa.null()
    assert tbl.schema.field("geom_0").type == pa.binary()
    assert tbl.schema.field("tlid").type == pa.int64()


def test_the_type_and_crs_come_from_the_first_non_null_geometry():
    # A leading NULL geometry carries its own (here 4326) srid; the type and the CRS are those
    # of the first geometry that exists, read from the same row.
    from types import SimpleNamespace

    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import GeoJSONLGbxWriter

    writer = SimpleNamespace(
        geometry_type_override=None,
        geom_col="geom_0",
        srid_col="geom_0_srid",
        proj_col="geom_0_srid_proj",
    )

    def infer(wkbs, srids):
        tbl = pa.table(
            {
                "geom_0": pa.array(wkbs, pa.binary()),
                "geom_0_srid": pa.array(srids, pa.string()),
                "geom_0_srid_proj": pa.array([""] * len(wkbs), pa.string()),
            }
        )
        return GeoJSONLGbxWriter._infer_geom_crs(writer, tbl)

    point = bytes(to_wkb(Point(530000.0, 180000.0)))
    assert infer([None, point], ["4326", "27700"]) == ("Point", "EPSG:27700")
    assert infer([point, point], ["27700", "4326"]) == ("Point", "EPSG:27700")
    assert infer([None, None], ["4326", "27700"]) == ("Unknown", None)


def test_all_null_attr_column_writes_and_roundtrips(spark, tmp_path):
    # End-to-end regression for a TIGER-style column (e.g. PLUS4L) that is null
    # for every row: the geojsonl write must not raise the pyogrio FieldError.
    register(spark)
    out = str(tmp_path / "nullattr")
    rows = [
        (str(i), None, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(4)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, plus4 string, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(2, F.col("geom_0"))
    df.write.format("geojsonl_gbx").mode("overwrite").save(out)  # must not raise

    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 4


def test_copy_file_to_fuse_never_chmods(tmp_path, monkeypatch):
    # UC Volumes (FUSE) reject chmod; the byte-only copy must never call it
    # (shutil.copy/copy2 would, via copymode -> PermissionError on a Volume).
    import os as _os

    from databricks.labs.gbx.ds.vector import _copy_file_to_fuse

    src = tmp_path / "s.bin"
    src.write_bytes(b"geojsonl-bytes")
    dst = tmp_path / "d.bin"
    monkeypatch.setattr(
        _os, "chmod", lambda *a, **k: pytest.fail("Volume copy must not chmod")
    )
    _copy_file_to_fuse(str(src), str(dst))  # must not raise / chmod
    assert dst.read_bytes() == b"geojsonl-bytes"


def test_copy_tree_to_fuse_never_chmods(tmp_path, monkeypatch):
    import os as _os

    from databricks.labs.gbx.ds.vector import _copy_tree_to_fuse

    src = tmp_path / "src.gdb"
    (src / "sub").mkdir(parents=True)
    (src / "a.bin").write_bytes(b"a")
    (src / "sub" / "b.bin").write_bytes(b"b")
    dst = tmp_path / "out.gdb"
    monkeypatch.setattr(
        _os, "chmod", lambda *a, **k: pytest.fail("Volume copytree must not chmod")
    )
    _copy_tree_to_fuse(str(src), str(dst))
    assert (dst / "a.bin").read_bytes() == b"a"
    assert (dst / "sub" / "b.bin").read_bytes() == b"b"


def test_writers_copy_to_volume_byte_only():
    # Guard the call sites: writers must copy to the (FUSE) Volume via the
    # byte-only helpers, never shutil.copy/copy2/copytree, which chmod the
    # destination and fail with 'Operation not permitted' on a UC Volume.
    import inspect

    from databricks.labs.gbx.ds import vector

    for cls in (vector.VectorGbxWriter, vector.GeoJSONLGbxWriter):
        src = inspect.getsource(cls)
        for bad in ("shutil.copy(", "shutil.copy2(", "shutil.copytree("):
            assert bad not in src, f"{bad} in {cls.__name__} is not Volume-safe"


def test_multi_file_one_shard_per_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "shards")
    df = _wkb_df(spark, 6).repartition(3, F.col("geom_0"))
    nparts = df.rdd.getNumPartitions()
    df.write.format("geojsonl_gbx").mode("overwrite").save(out)

    # Output is a DIRECTORY with EXACTLY one .geojsonl shard per (non-empty) partition.
    assert os.path.isdir(out)
    shards = _shards(out)
    assert (
        len(shards) == nparts
    ), f"expected exactly {nparts} shards (one per partition), got {len(shards)}: {shards}"

    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 6
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(6)}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_max_records_per_file_splits_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "split")
    n, k = 10, 3  # ceil(10/3) == 4 shards
    _wkb_df(spark, n).repartition(1, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).option("maxRecordsPerFile", str(k)).save(out)

    shards = _shards(out)
    expected = -(-n // k)  # ceil
    assert len(shards) == expected, f"expected {expected} shards, got {shards}"

    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == n


def test_output_is_directory_not_single_file(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "dir")
    _wkb_df(spark, 4).repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    assert os.path.isdir(out)
    assert not os.path.isfile(out)
    # every published member is a shard (plus an optional _SUCCESS marker)
    members = set(os.listdir(out)) - {"_SUCCESS"}
    assert members and all(m.endswith((".geojsonl", ".geojsons")) for m in members)


def test_wkt_input_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "wkt")
    rows = [
        ("a", "POINT (-73.9 40.7)", "4326", ""),
        ("b", "POINT (-0.1 51.5)", "4326", ""),
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 string, geom_0_srid string, geom_0_srid_proj string",
    )
    df.repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2
    assert {r["name"] for r in back.collect()} == {"a", "b"}


def test_overwrite_clears_previous_shards(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "ow")
    _wkb_df(spark, 8).repartition(4, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    first = set(_shards(out))
    # rewrite with fewer rows/partitions -> stale shards must be gone
    _wkb_df(spark, 2).repartition(1, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    second = set(_shards(out))
    assert first.isdisjoint(second), "overwrite left stale shards behind"
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists")
    _wkb_df(spark, 4).repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark, 4).write.format("geojsonl_gbx").mode("append").save(out)
    assert "append" in str(ei.value).lower()


def test_serverless_safe_no_jvm_or_conf():
    """The writer code must not touch the JVM, sparkContext, spark.conf, or RDDs."""
    import inspect

    from databricks.labs.gbx.ds import vector

    src = inspect.getsource(vector.GeoJSONLGbxWriter)
    for forbidden in ("_jvm", "sparkContext", "spark.conf.set", ".rdd"):
        assert forbidden not in src, f"{forbidden} found in GeoJSONLGbxWriter"


def test_geomcol_sridcol_options_avoid_renaming(spark, tmp_path):
    # A frame with non-convention column names writes via the geomCol/sridCol
    # options without the user renaming anything.
    register(spark)
    out = str(tmp_path / "renamed")
    rows = [
        ("a", bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    df = spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    ).repartition(2, F.col("the_geom"))
    (
        df.write.format("geojsonl_gbx")
        .mode("overwrite")
        .option("geomCol", "the_geom")
        .option("sridCol", "epsg")
        .option("projCol", "p4")
        .save(out)
    )
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2
    assert {r["name"] for r in back.collect()} == {"a", "b"}
