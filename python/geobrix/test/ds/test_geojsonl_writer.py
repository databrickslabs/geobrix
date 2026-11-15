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


def _wkt_df(spark):
    rows = [
        ("a", "POINT (-73.9 40.7)", "4326", ""),
        ("b", "POINT (-0.1 51.5)", "4326", ""),
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, geom_0 string, geom_0_srid string, geom_0_srid_proj string",
    )


def _renamed_df(spark):
    rows = [
        ("a", bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    return spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    )


def _null_attr_df(spark):
    # A TIGER-style column that is null for every row: pyogrio rejects an Arrow 'null' typed field.
    rows = [
        (str(i), None, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(4)
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, plus4 string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    )


# name -> (frame, repartition column, partitions, writer options, shards expected or None when hashing may
# leave a partition empty, names back)
ROUND_TRIP = {
    "one_shard_per_partition": (lambda s: _wkb_df(s, 6), "geom_0", 3, {}, 3, {str(i) for i in range(6)}),
    "max_records_per_file_splits_a_partition": (
        lambda s: _wkb_df(s, 10), "geom_0", 1, {"maxRecordsPerFile": "3"}, 4, {str(i) for i in range(10)},
    ),
    "wkt_input": (_wkt_df, "geom_0", 2, {}, None, {"a", "b"}),
    "geomcol_sridcol_projcol_avoid_renaming": (
        _renamed_df, "the_geom", 2, {"geomCol": "the_geom", "sridCol": "epsg", "projCol": "p4"}, None, {"a", "b"},
    ),
    "all_null_attribute_column": (_null_attr_df, "geom_0", 2, {}, None, {"0", "1", "2", "3"}),
}  # fmt: skip


@pytest.mark.parametrize(
    "make,col,parts,opts,shards,names", ROUND_TRIP.values(), ids=ROUND_TRIP.keys()
)
def test_geojsonl_writes_a_directory_of_shards_that_read_back(
    spark, tmp_path, make, col, parts, opts, shards, names
):
    """One .geojsonl shard per non-empty partition (no driver merge), nothing else in the folder, and the
    geojson_gbx directory reader returns every row with its geometry."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    out = str(tmp_path / "out")
    w = (
        make(spark)
        .repartition(parts, F.col(col))
        .write.format("geojsonl_gbx")
        .mode("overwrite")
    )
    for k, v in opts.items():
        w = w.option(k, v)
    w.save(out)

    assert os.path.isdir(out)
    assert len(_shards(out)) == shards if shards else _shards(out)
    assert set(os.listdir(out)) - {"_SUCCESS"} == set(_shards(out))
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert {r["name"] for r in back.collect()} == names
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert {
        _from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()
    } == {"Point"}


def test_overwrite_clears_the_previous_shards_and_append_is_refused(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    out = str(tmp_path / "ow")
    _wkb_df(spark, 8).repartition(4, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    first = set(_shards(out))
    _wkb_df(spark, 2).repartition(1, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    assert first.isdisjoint(set(_shards(out))), "overwrite left stale shards behind"
    assert (
        spark.read.format("geojson_gbx").option("multi", "true").load(out).count() == 2
    )
    with pytest.raises(Exception, match="(?i)append"):
        _wkb_df(spark, 4).write.format("geojsonl_gbx").mode("append").save(out)


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


def test_serverless_safe_no_jvm_or_conf():
    """The writer code must not touch the JVM, sparkContext, spark.conf, or RDDs."""
    import inspect

    from databricks.labs.gbx.ds import vector

    src = inspect.getsource(vector.GeoJSONLGbxWriter)
    for forbidden in ("_jvm", "sparkContext", "spark.conf.set", ".rdd"):
        assert forbidden not in src, f"{forbidden} found in GeoJSONLGbxWriter"
