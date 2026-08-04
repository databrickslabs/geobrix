import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from shapely import LineString, Point, Polygon
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds import _scratch
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import (
    VectorGbxWriter,
    _geometry_type_of,
    _srid_to_crs,
    _writer_col_roles,
)


def test_default_gpkg_layer_avoids_reserved_prefix():
    from databricks.labs.gbx.ds.vector import _default_gpkg_layer

    assert _default_gpkg_layer("/a/b/roads") == "roads"
    assert _default_gpkg_layer("/a/b/roads.gpkg") == "roads"
    assert _default_gpkg_layer("/a/b/gpkg") == "layer"  # reserved prefix
    assert _default_gpkg_layer("/a/b/gpkgdata") == "layer"
    assert _default_gpkg_layer("/") == "layer"  # empty stem


def test_gpkg_write_to_path_named_gpkg(spark, tmp_path):
    # Saving to a path whose stem is 'gpkg' must not fail with the GeoPackage
    # reserved-prefix error (the layer name falls back to 'layer').
    # The writer appends .gpkg so the resolved file is out + ".gpkg".
    register(spark)
    out = str(tmp_path / "gpkg")  # stem == "gpkg"
    rows = [("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", "")]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    )
    df.write.format("gpkg_gbx").mode("overwrite").save(out)  # must not raise
    # Writer resolves 'gpkg' -> 'gpkg.gpkg'; read from the resolved path.
    back = spark.read.format("gpkg_gbx").load(out + ".gpkg")
    assert back.count() == 1


def test_non_geojson_commit_streams_without_concat(spark, tmp_path, monkeypatch):
    # GPKG/Shapefile/FileGDB must append fragments per-partition (bounded driver
    # memory), NOT concat every partition into one in-memory table (which OOMs a
    # single-node driver on large inputs). Make concat_tables raise; a 2-partition
    # GPKG write must still succeed.
    import pyarrow as pa

    monkeypatch.setattr(
        pa,
        "concat_tables",
        lambda *a, **k: pytest.fail("commit must not concat for GPKG"),
    )
    register(spark)
    out = str(tmp_path / "streamed.gpkg")
    rows = [
        ("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", ""),
        ("b", bytearray(to_wkb(Point(3.0, 4.0))), "4326", ""),
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    ).repartition(2, F.col("geom_0"))
    df.write.format("gpkg_gbx").mode("overwrite").save(out)  # must not raise / concat
    assert spark.read.format("gpkg_gbx").load(out).count() == 2


def test_should_stream_policy():
    # All pyogrio single-file writers (GeoJSON, Shapefile, GPKG) stream fragments
    # into one write (bounded driver memory, single pass). OpenFileGDB uses the
    # native osgeo path and is excluded.
    from databricks.labs.gbx.ds.vector import _should_stream

    assert _should_stream("GeoJSON") is True
    assert _should_stream("ESRI Shapefile") is True
    assert _should_stream("GPKG") is True
    assert _should_stream("OpenFileGDB") is False


def test_stream_record_batches_chains_and_drops_meta(tmp_path):
    import pyarrow as pa
    import pyarrow.feather as feather

    from databricks.labs.gbx.ds.vector import _stream_record_batches

    schema = pa.schema(
        [
            ("name", pa.string()),
            ("geom_0", pa.binary()),
            ("geom_0_srid", pa.string()),
            ("geom_0_srid_proj", pa.string()),
        ]
    )
    f1 = str(tmp_path / "frag1.arrow")
    f2 = str(tmp_path / "frag2.arrow")
    feather.write_feather(
        pa.table(
            {
                "name": ["a"],
                "geom_0": [b"x"],
                "geom_0_srid": ["4326"],
                "geom_0_srid_proj": [""],
            },
            schema=schema,
        ),
        f1,
    )
    feather.write_feather(
        pa.table(
            {
                "name": ["b", "c"],
                "geom_0": [b"y", b"z"],
                "geom_0_srid": ["4326", "4326"],
                "geom_0_srid_proj": ["", ""],
            },
            schema=schema,
        ),
        f2,
    )
    drop = {"geom_0_srid", "geom_0_srid_proj"}
    batches = list(_stream_record_batches([f1, f2], drop))
    assert sum(b.num_rows for b in batches) == 3  # chained across fragments
    for b in batches:
        assert b.schema.names == ["name", "geom_0"]  # meta cols dropped


def _roundtrip_streamed(spark, tmp_path, driver, name):
    register(spark)
    out = str(tmp_path / name)
    rows = [
        (str(i), None, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(20)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, note string, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(4, F.col("geom_0"))
    df.write.format("vector_gbx").mode("overwrite").option("driverName", driver).save(
        out
    )
    back = spark.read.format("vector_gbx").load(out)
    assert back.count() == 20
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(20)}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_geojson_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed GeoJSON write round-trips all rows/geoms (incl. an all-null col).
    _roundtrip_streamed(spark, tmp_path, "GeoJSON", "streamed.geojson")


def test_shapefile_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed Shapefile write round-trips all rows/geoms (incl. an all-null col).
    _roundtrip_streamed(spark, tmp_path, "ESRI Shapefile", "streamed_shp")


def test_gpkg_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed GPKG write (geom column renamed to 'geom' per batch) round-trips.
    _roundtrip_streamed(spark, tmp_path, "GPKG", "streamed.gpkg")


def test_scratch_dir_unique_per_write(tmp_path):
    # Two concurrent writes to the same parent directory must NOT share a scratch
    # dir, or one write's commit cleanup (rmtree) would delete the other's
    # in-flight Arrow fragments.
    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    parent = str(tmp_path)
    w1 = VectorGbxWriter(f"{parent}/a.geojson", schema, "GeoJSON", {}, overwrite=True)
    w2 = VectorGbxWriter(f"{parent}/b.geojson", schema, "GeoJSON", {}, overwrite=True)
    assert w1.scratch_dir != w2.scratch_dir
    # Each write's scratch is a unique subdir under the shared hidden container
    # <parent>/.gbx_scratch/ (so a concurrent write's rmtree can't touch it, and
    # the recursive reader skips the dot-prefixed container).
    container = os.path.join(parent, _scratch.SCRATCH_CONTAINER)
    assert os.path.dirname(w1.scratch_dir) == container
    assert os.path.dirname(w2.scratch_dir) == container


def test_geometry_type_of_point_and_line():
    assert _geometry_type_of(to_wkb(Point(1, 2))) == "Point"
    assert _geometry_type_of(to_wkb(LineString([(0, 0), (1, 1)]))) == "LineString"


def test_srid_to_crs():
    assert _srid_to_crs("4326", "") == "EPSG:4326"
    assert _srid_to_crs("0", "+proj=longlat +datum=WGS84 +no_defs") == (
        "+proj=longlat +datum=WGS84 +no_defs"
    )
    assert _srid_to_crs("0", "") is None
    assert _srid_to_crs("", "") is None


def test_srid_to_crs_esri_code_labels_esri():
    # An ESRI-code SRID (54008 = World Sinusoidal, no EPSG code) must classify as
    # ESRI, not be blindly prefixed "EPSG:" (the old authority-name gap that Spec R2
    # subsumes). Routes through resolve_crs' authoritative epsg->esri classification.
    assert _srid_to_crs("54008", "") == "ESRI:54008"
    assert _srid_to_crs("102008", "") == "ESRI:102008"
    # An EPSG code still classifies EPSG.
    assert _srid_to_crs("27700", "") == "EPSG:27700"
    # An unresolvable/garbage SRID falls back to the PROJ4 string (never raises).
    assert _srid_to_crs("99999999", "+proj=longlat +datum=WGS84 +no_defs") == (
        "+proj=longlat +datum=WGS84 +no_defs"
    )
    # Unresolvable with no PROJ4 fallback -> None (CRS-less, lenient, no raise).
    assert _srid_to_crs("99999999", "") is None


def test_writer_col_roles_named_geom():
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("pop", IntegerType()),
            StructField("SHAPE", BinaryType()),
            StructField("SHAPE_srid", StringType()),
            StructField("SHAPE_srid_proj", StringType()),
        ]
    )
    geom, srid, proj, attrs = _writer_col_roles(schema)
    assert (geom, srid, proj) == ("SHAPE", "SHAPE_srid", "SHAPE_srid_proj")
    assert attrs == ["name", "pop"]


def _wkb_df(spark):
    rows = [
        ("a", 10, bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", 20, bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    )


def test_geojson_roundtrip_single_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "out.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)

    back = spark.read.format("vector_gbx").load(out)
    assert back.count() == 2
    got = {r["name"]: r["pop"] for r in back.collect()}
    assert got == {"a": 10, "b": 20}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_multi_partition_merge(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "multi.geojson")
    rows = [
        (str(i), i, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(50)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(4, F.col("geom_0"))
    assert df.rdd.getNumPartitions() == 4
    df.write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("vector_gbx").load(out)
    assert back.count() == 50
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(50)}


def test_crs_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "crs.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("vector_gbx").load(out)
    scol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0]
    assert {r[scol] for r in back.select(scol).collect()} == {"4326"}


def test_geometry_type_override(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "poly.geojson")
    poly = to_wkb(Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]))
    df = spark.createDataFrame(
        [("p", bytearray(poly), "4326", "")],
        schema="name string, geom_0 binary, geom_0_srid string, "
        "geom_0_srid_proj string",
    )
    df.coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).option("geometryType", "Polygon").save(out)
    back = spark.read.format("vector_gbx").load(out)
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert _from_wkb(bytes(back.collect()[0][gcol])).geom_type == "Polygon"


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).write.format("vector_gbx").mode("append").option(
            "driverName", "GeoJSON"
        ).save(out)
    assert "append" in str(ei.value).lower()


@pytest.mark.parametrize(
    "fmt,target",
    [
        ("geojson_gbx", "named.geojson"),
        ("gpkg_gbx", "named.gpkg"),
        ("shapefile_gbx", "named.shp"),
    ],
)
def test_named_writer_roundtrip(spark, tmp_path, fmt, target):
    register(spark)
    out = str(tmp_path / target)
    _wkb_df(spark).coalesce(1).write.format(fmt).mode("overwrite").save(out)
    back = spark.read.format(fmt).load(out)
    assert back.count() == 2
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert {
        _from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()
    } == {"Point"}


def _ogr_can_create(driver: str) -> bool:
    try:
        import tempfile

        import pyarrow as pa
        import pyogrio
        from shapely import Point as _P
        from shapely import to_wkb as _twkb

        d = tempfile.mkdtemp()
        path = d + ("/t.gdb" if driver == "OpenFileGDB" else "/t.out")
        tbl = pa.table({"g": [_twkb(_P(0, 0))]})
        pyogrio.write_arrow(
            tbl,
            path,
            driver=driver,
            geometry_name="g",
            geometry_type="Point",
            crs="EPSG:4326",
        )
        return True
    except Exception:
        return False


def test_file_gdb_writer_roundtrip(spark, tmp_path):
    import importlib.util

    register(spark)
    if importlib.util.find_spec("osgeo") is None:
        pytest.skip(
            "native osgeo (heavy GDAL natives) not present; file_gdb_gbx write requires osgeo"
        )
    out = str(tmp_path / "out.gdb")
    _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
    back = spark.read.format("file_gdb_gbx").load(out)
    assert back.count() == 2


def test_gpkg_reads_from_readonly_dir(spark, tmp_path):
    # GPKG written by the writer must read back even when the file/dir are
    # read-only (simulating a read-only Volume) -- i.e. no WAL that forces a
    # reader-side checkpoint write.
    import os
    import stat

    register(spark)
    out = str(tmp_path / "ro.gpkg")
    _wkb_df(spark).coalesce(1).write.format("gpkg_gbx").mode("overwrite").save(out)
    # make the output file + its dir read-only
    os.chmod(out, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    os.chmod(str(tmp_path), stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
    try:
        back = spark.read.format("gpkg_gbx").load(out)
        assert back.count() == 2
    finally:
        os.chmod(str(tmp_path), 0o755)
        os.chmod(out, 0o644)


def test_file_gdb_osgeo_hybrid_roundtrip(spark, tmp_path):
    import pytest

    pytest.importorskip("osgeo", reason="native osgeo (heavy GDAL natives) not present")
    register(spark)
    out = str(tmp_path / "hybrid.gdb")
    _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
    back = spark.read.format("file_gdb_gbx").load(out)
    assert back.count() == 2


def test_file_gdb_clear_error_without_osgeo(spark, tmp_path):
    import importlib.util

    import pytest

    if importlib.util.find_spec("osgeo") is not None:
        pytest.skip("osgeo present -- the no-natives error path is not exercised")
    register(spark)
    out = str(tmp_path / "noosgeo.gdb")
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(
            out
        )
    assert "osgeo" in str(ei.value).lower() or "native" in str(ei.value).lower()


def test_file_gdb_multi_fragment_streaming(spark, tmp_path):
    """Multi-partition FileGDB write: fragment streaming + OGR transaction batching
    must produce the same row count and preserve attribute + geometry values.

    This validates both the fragment-streaming path (no full-dataset in-memory
    concat before the write) and the transaction batching (no per-feature
    auto-commit corruption). Uses >1 partition so at least two fragments land.
    """
    import importlib.util

    import pytest
    from shapely import from_wkb as _from_wkb

    if importlib.util.find_spec("osgeo") is None:
        pytest.skip(
            "native osgeo (heavy GDAL natives) not present; file_gdb_gbx write requires osgeo"
        )

    register(spark)
    # Build a small dataset that is guaranteed to span multiple partitions. Use
    # Polygon geometry so we exercise the geometry survival path (not just Point).
    from shapely import Polygon as _Polygon

    def _box(x, y):
        return bytearray(
            to_wkb(_Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y)]))
        )

    rows = [(f"feat_{i}", i * 10, _box(float(i), 0.0), "4326", "") for i in range(10)]
    df = spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    ).repartition(
        3, F.col("name")
    )  # force >=2 fragments

    out = str(tmp_path / "multi.gdb")
    df.write.format("file_gdb_gbx").mode("overwrite").save(out)

    back = spark.read.format("file_gdb_gbx").load(out)
    rows_back = back.collect()

    # Row count must survive streaming + transaction batching.
    assert len(rows_back) == 10, f"expected 10 rows, got {len(rows_back)}"

    # Attribute values must survive intact.
    names_back = {r["name"] for r in rows_back}
    assert names_back == {
        f"feat_{i}" for i in range(10)
    }, f"name mismatch: {names_back}"
    pops_back = {r["name"]: r["pop"] for r in rows_back}
    for i in range(10):
        assert (
            pops_back[f"feat_{i}"] == i * 10
        ), f"pop mismatch for feat_{i}: {pops_back[f'feat_{i}']}"

    # Geometry must survive (non-null, correct type). OpenFileGDB promotes Polygon
    # to MultiPolygon in the layer geometry type, so accept both.
    gcol = next(
        f.name
        for f in back.schema.fields
        if f.name not in ("name", "pop")
        and not f.name.endswith(("_srid", "_srid_proj"))
    )
    geom_types = {
        _from_wkb(bytes(r[gcol])).geom_type for r in rows_back if r[gcol] is not None
    }
    assert geom_types <= {
        "Polygon",
        "MultiPolygon",
    }, f"geometry type mismatch: {geom_types}"
    assert len(geom_types) > 0, "no non-null geometries read back"


def test_file_gdb_tx_batch_boundary(spark, tmp_path, monkeypatch):
    """The OGR mid-loop CommitTransaction/StartTransaction path must not drop or
    double-write features.

    With the default _GDB_TX_BATCH=100_000 a 10-feature test never triggers the
    boundary commit.  Monkeypatching the module-level constant to 3 forces the
    commit/restart cycle at rows 3, 6 (and the tail commit at row 7), so this
    test exercises the code path the default value leaves dead.

    Asserts:
    (a) exact row count == 7  (no drop, no double-write at batch boundaries)
    (b) attribute values survive intact across commit boundaries
    (c) geometry values survive (non-null, correct shapely type)
    """
    import importlib.util

    from shapely import Polygon as _Polygon
    from shapely import from_wkb as _from_wkb

    import databricks.labs.gbx.ds.vector as _vec_mod

    if importlib.util.find_spec("osgeo") is None:
        pytest.skip(
            "native osgeo (heavy GDAL natives) not present; "
            "file_gdb_gbx write requires osgeo"
        )

    # Patch the module-level batch size to 3 so 7 features cross two boundaries.
    monkeypatch.setattr(_vec_mod, "_GDB_TX_BATCH", 3)

    register(spark)

    def _box(x, y):
        return bytearray(
            to_wkb(_Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y)]))
        )

    # 7 features, 2 partitions -> >=2 fragments; batch=3 means commits at
    # features 3 and 6, then a tail commit for feature 7.
    rows = [(f"r{i}", i * 5, _box(float(i), 0.0), "4326", "") for i in range(7)]
    df = spark.createDataFrame(
        rows,
        schema="name string, val int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(2, F.col("name"))

    out = str(tmp_path / "boundary.gdb")
    df.write.format("file_gdb_gbx").mode("overwrite").save(out)

    back = spark.read.format("file_gdb_gbx").load(out)
    rows_back = back.collect()

    # (a) exact row count -- no drop or double-write
    assert len(rows_back) == 7, f"expected 7 rows, got {len(rows_back)}"

    # (b) attribute values survive
    name_to_val = {r["name"]: r["val"] for r in rows_back}
    assert set(name_to_val.keys()) == {
        f"r{i}" for i in range(7)
    }, f"name set mismatch: {set(name_to_val.keys())}"
    for i in range(7):
        assert (
            name_to_val[f"r{i}"] == i * 5
        ), f"val mismatch for r{i}: got {name_to_val[f'r{i}']}"

    # (c) geometry survives (non-null; OpenFileGDB may promote Polygon->MultiPolygon)
    gcol = next(
        f.name
        for f in back.schema.fields
        if f.name not in ("name", "val")
        and not f.name.endswith(("_srid", "_srid_proj"))
    )
    geom_types = {
        _from_wkb(bytes(r[gcol])).geom_type for r in rows_back if r[gcol] is not None
    }
    assert geom_types <= {
        "Polygon",
        "MultiPolygon",
    }, f"unexpected geometry types: {geom_types}"
    assert len(geom_types) > 0, "all geometries were null after round-trip"


def test_gpkg_output_uses_format_default_geom_name(spark, tmp_path):
    # GPKG output should use the format-default geometry column name `geom`,
    # not the input column name, so an arbitrary input name doesn't leak out.
    register(spark)
    out = str(tmp_path / "out.gpkg")
    rows = [("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", "")]
    df = spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    )
    (
        df.write.format("gpkg_gbx")
        .mode("overwrite")
        .option("geomCol", "the_geom")
        .option("sridCol", "epsg")
        .save(out)
    )
    import pyogrio

    info = pyogrio.read_info(out)
    assert info["geometry_name"] == "geom"


def test_classic_write_path_roundtrip(spark, tmp_path):
    # Exercise the classic pyogrio.raw.write fallback path directly (write_arrow
    # works locally so the auto-fallback won't trigger here) to prove the
    # arrow-table -> (geometry, field_data, fields) conversion round-trips.
    register(spark)
    import os
    import tempfile

    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    out = str(tmp_path / "classic.gpkg")
    w = VectorGbxWriter(
        out, _wkb_df(spark).schema, "GPKG", {"driverName": "GPKG"}, True
    )
    tbl = pa.table(
        {
            "name": ["a", "b"],
            "pop": [10, 20],
            "geom_0": [
                bytearray(to_wkb(Point(-73.9, 40.7))),
                bytearray(to_wkb(Point(-0.1, 51.5))),
            ],
            "geom_0_srid": ["4326", "4326"],
            "geom_0_srid_proj": ["", ""],
        }
    )
    d = tempfile.mkdtemp()
    local_out = os.path.join(d, "classic.gpkg")
    w._write_local_classic([tbl], local_out, "Point", "EPSG:4326")
    back = spark.read.format("gpkg_gbx").load(local_out)
    assert back.count() == 2


# ---------------------------------------------------------------------------
# Shapefile zip=true option
# ---------------------------------------------------------------------------


def test_shapefile_zip_output_naming():
    """zip=true path-normalisation: bare stem, .shp, and .shp.zip all land at .shp.zip."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    opts_base = {"driverName": "ESRI Shapefile", "zip": "true"}

    w1 = VectorGbxWriter("/tmp/roads", schema, "ESRI Shapefile", opts_base, True)
    assert w1.path == "/tmp/roads.shp.zip", w1.path

    w2 = VectorGbxWriter("/tmp/roads.shp", schema, "ESRI Shapefile", opts_base, True)
    assert w2.path == "/tmp/roads.shp.zip", w2.path

    w3 = VectorGbxWriter(
        "/tmp/roads.shp.zip", schema, "ESRI Shapefile", opts_base, True
    )
    assert w3.path == "/tmp/roads.shp.zip", w3.path


def test_shapefile_zip_ignored_for_other_drivers():
    """zip=true is silently ignored for non-Shapefile drivers."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    w = VectorGbxWriter(
        "/tmp/out.geojson",
        schema,
        "GeoJSON",
        {"driverName": "GeoJSON", "zip": "true"},
        True,
    )
    assert w.path == "/tmp/out.geojson"
    assert w.zip is False


def test_filegdb_zip_output_naming():
    """zip=true for OpenFileGDB: bare stem, .gdb, and .gdb.zip all land at .gdb.zip."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    opts = {"driverName": "OpenFileGDB", "zip": "true"}
    assert (
        VectorGbxWriter("/tmp/roads", schema, "OpenFileGDB", opts, True).path
        == "/tmp/roads.gdb.zip"
    )
    assert (
        VectorGbxWriter("/tmp/roads.gdb", schema, "OpenFileGDB", opts, True).path
        == "/tmp/roads.gdb.zip"
    )
    assert (
        VectorGbxWriter("/tmp/roads.gdb.zip", schema, "OpenFileGDB", opts, True).path
        == "/tmp/roads.gdb.zip"
    )


def test_zip_honored_per_driver():
    """zip=true is honored only for the directory/sidecar writers (Shapefile, FileGDB)."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )

    def zflag(driver):
        return VectorGbxWriter(
            "/tmp/x", schema, driver, {"driverName": driver, "zip": "true"}, True
        ).zip

    assert zflag("ESRI Shapefile") is True
    assert zflag("OpenFileGDB") is True
    assert zflag("GPKG") is False
    assert zflag("GeoJSON") is False


def test_zip_gdb_bundle_preserves_top_level_dir(tmp_path):
    """A .gdb directory packs into .gdb.zip with the .gdb folder at the archive root."""
    import zipfile

    from databricks.labs.gbx.ds.vector import _zip_gdb_bundle

    gdb = tmp_path / "roads.gdb"
    (gdb / "sub").mkdir(parents=True)
    (gdb / "a00000001.gdbtable").write_bytes(b"x")
    (gdb / "gdb").write_bytes(b"y")  # FileGDB has a file literally named 'gdb'
    (gdb / "sub" / "nested.bin").write_bytes(b"z")
    zp = str(tmp_path / "roads.gdb.zip")
    _zip_gdb_bundle(str(gdb), zp)
    names = sorted(zipfile.ZipFile(zp).namelist())
    assert names, "archive is empty"
    assert all(n.startswith("roads.gdb/") for n in names), names
    assert "roads.gdb/a00000001.gdbtable" in names
    assert "roads.gdb/sub/nested.bin" in names


def test_shapefile_zip_produces_single_file(spark, tmp_path):
    """zip=true writes a single .shp.zip file, not a directory or sidecar set."""
    register(spark)
    out = str(tmp_path / "roads")
    _wkb_df(spark).coalesce(1).write.format("shapefile_gbx").mode("overwrite").option(
        "zip", "true"
    ).save(out)
    expected = str(tmp_path / "roads.shp.zip")
    assert os.path.isfile(expected), f"Expected {expected} to exist"
    # No bare sidecar files in the parent dir.
    names = os.listdir(str(tmp_path))
    sidecars = [n for n in names if n.startswith("roads.") and not n.endswith(".zip")]
    assert sidecars == [], f"Unexpected sidecar files: {sidecars}"


def test_shapefile_zip_archive_contents(spark, tmp_path):
    """The .shp.zip archive contains .shp/.shx/.dbf at the archive root (no subdir)."""
    import zipfile

    register(spark)
    out = str(tmp_path / "bundle")
    _wkb_df(spark).coalesce(1).write.format("shapefile_gbx").mode("overwrite").option(
        "zip", "true"
    ).save(out)
    zip_path = str(tmp_path / "bundle.shp.zip")
    assert os.path.isfile(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    # Must have .shp, .shx, .dbf at root (no subdirectory prefix)
    assert any(n.endswith(".shp") and "/" not in n for n in names), names
    assert any(n.endswith(".shx") and "/" not in n for n in names), names
    assert any(n.endswith(".dbf") and "/" not in n for n in names), names


def test_shapefile_zip_roundtrip(spark, tmp_path):
    """zip=true multi-partition write round-trips via shapefile_gbx reader."""
    register(spark)
    out = str(tmp_path / "roundtrip")
    rows = [
        (str(i), bytearray(to_wkb(Point(float(i), 40.0))), "4326", "")
        for i in range(10)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    ).repartition(3, F.col("geom_0"))
    df.write.format("shapefile_gbx").mode("overwrite").option("zip", "true").save(out)

    zip_path = str(tmp_path / "roundtrip.shp.zip")
    assert os.path.isfile(zip_path)
    # Read back via shapefile_gbx (uses /vsizip/ internally)
    back = spark.read.format("shapefile_gbx").load(zip_path)
    assert back.count() == 10
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geom_types = {
        _from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()
    }
    assert geom_types == {"Point"}


def test_shapefile_no_zip_no_regression(spark, tmp_path):
    """zip=false (default) shapefile write still produces sidecar files, not a zip."""
    register(spark)
    out = str(tmp_path / "nozip_shp")
    _wkb_df(spark).coalesce(1).write.format("shapefile_gbx").mode("overwrite").save(out)
    # The shapefile sidecar files land either in the parent dir (e.g. nozip_shp.shp)
    # or inside the output directory (nozip_shp/nozip_shp.shp), depending on the path.
    # Either way, there must be no .zip file anywhere, and the reader must round-trip.
    import glob

    zip_files = glob.glob(str(tmp_path / "*.zip"))
    assert zip_files == [], f"Unexpected .zip in {zip_files}"
    back = spark.read.format("shapefile_gbx").load(out)
    assert back.count() == 2
