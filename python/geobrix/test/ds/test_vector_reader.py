import json
import os
from unittest.mock import patch

import pytest
from shapely import from_wkb

from databricks.labs.gbx.ds.register import register

_GJ = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "a", "pop": 10},
            "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
        },
        {
            "type": "Feature",
            "properties": {"name": "b", "pop": 20},
            "geometry": {"type": "Point", "coordinates": [-0.1, 51.5]},
        },
    ],
}


def _gj_path(tmp):
    p = os.path.join(tmp, "pts.geojson")
    with open(p, "w") as f:
        json.dump(_GJ, f)
    return p


def test_vector_gbx_reads_wkb_schema(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").load(p)
    assert df.columns == ["name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    rows = df.orderBy("name").collect()
    assert rows[0]["name"] == "a" and rows[0]["pop"] == 10
    assert rows[0]["geom_0_srid"] == "4326"
    assert from_wkb(bytes(rows[0]["geom_0"])).geom_type == "Point"
    assert df.count() == 2


def test_vector_gbx_wkt_option(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").option("asWKB", "false").load(p)
    g = df.orderBy("name").collect()[0]["geom_0"]
    assert isinstance(g, str) and g.upper().startswith("POINT")


def test_vector_gbx_chunksize_reads_all(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").option("chunkSize", "1").load(p)
    # One partition per FILE now (not per offset-chunk): splitting a single GeoJSON by
    # feature offset would re-parse the whole file on every chunk. chunkSize only bounds the
    # Arrow batch size within the single read, so a one-file source = 1 partition and still
    # returns all features.
    assert df.rdd.getNumPartitions() == 1
    assert df.count() == 2


def test_ogr_gbx_reads_directory(spark, tmp_path):
    register(spark)
    d = os.path.join(str(tmp_path), "many")
    os.makedirs(d)
    for k in range(3):
        with open(os.path.join(d, f"p{k}.geojson"), "w") as f:
            json.dump(_GJ, f)
    df = spark.read.format("geojson_gbx").load(d)
    assert df.count() == 6  # 3 files x 2 features


def test_vector_gbx_read_yields_recordbatch_wkb(spark, tmp_path):
    """read() must be Arrow-native: yield pyarrow.RecordBatch (not Python tuples),
    with WKB geometry that round-trips."""
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_path(str(tmp_path))
    rdr = VectorGbxReader({"path": p})
    parts = list(rdr.partitions())
    batches = []
    for part in parts:
        for b in rdr.read(part):
            assert isinstance(b, pa.RecordBatch)
            batches.append(b)
    tbl = pa.Table.from_batches(batches)
    assert tbl.column_names == [
        "name",
        "pop",
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
    ]
    names = tbl.column("name").to_pylist()
    srids = set(tbl.column("geom_0_srid").to_pylist())
    geoms = tbl.column("geom_0").to_pylist()
    assert set(names) == {"a", "b"}
    assert srids == {"4326"}
    assert all(from_wkb(bytes(g)).geom_type == "Point" for g in geoms)


def test_vector_gbx_read_yields_recordbatch_wkt(spark, tmp_path):
    """asWKB=false: vectorized WKB->WKT in the Arrow output."""
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import _GeoJSONReader

    p = _gj_path(str(tmp_path))
    rdr = _GeoJSONReader({"path": p, "asWKB": "false"})
    batches = [b for part in rdr.partitions() for b in rdr.read(part)]
    tbl = pa.Table.from_batches(batches)
    assert pa.types.is_string(tbl.schema.field("geom_0").type)
    for g in tbl.column("geom_0").to_pylist():
        assert isinstance(g, str) and g.upper().startswith("POINT")


def test_shapefile_gbx_reads_directory_of_shp_zip(spark, tmp_path):
    """A directory of copy_*.shp.zip files is enumerated and read by shapefile_gbx.
    Each .shp.zip contains a small shapefile written by the shapefile_gbx writer so
    the round-trip exercises the actual path the scaled bench uses."""
    from databricks.labs.gbx.bench.corpus_vector import (
        generate_polygon_seed,
        transcode_vector_seed,
    )

    register(spark)
    n_features = 10
    seed_df = generate_polygon_seed(spark, n_features)
    seeds = transcode_vector_seed(
        spark, seed_df, ["shapefile_gbx"], str(tmp_path / "seeds")
    )
    shp_zip = seeds["shapefile_gbx"]
    assert shp_zip.endswith(".shp.zip")

    # Build a copies directory with 2 copies of the seed .shp.zip
    copies_dir = str(tmp_path / "copies")
    os.makedirs(copies_dir)
    import shutil

    for i in range(2):
        shutil.copy(shp_zip, os.path.join(copies_dir, f"copy_{i}.shp.zip"))

    df = spark.read.format("shapefile_gbx").load(copies_dir)
    assert df.count() == n_features * 2  # 2 copies × n_features features each


# ---------------------------------------------------------------------------
# FILE access mode tests (Task 2: make access option honest — managed/external
# reject with a pointer to vector_file_read; auto/fuse read via FUSE unchanged)
# ---------------------------------------------------------------------------

# Helpers shared by FILE tests
_GJ_FILE = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
    "features": [
        {
            "type": "Feature",
            "properties": {"id": 1},
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        },
        {
            "type": "Feature",
            "properties": {"id": 2},
            "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
        },
    ],
}


def _gj_file_path(tmp):
    p = os.path.join(tmp, "file_test.geojson")
    with open(p, "w") as f:
        json.dump(_GJ_FILE, f)
    return p


def _read_all(rdr):
    """Collect all pyarrow rows from a VectorGbxReader into a plain list of dicts."""
    import pyarrow as pa

    batches = [b for part in rdr.partitions() for b in rdr.read(part)]
    tbl = pa.Table.from_batches(batches)
    return tbl.to_pydict()


def test_vector_gbx_access_managed_rejected(tmp_path):
    """DataSource reader is FUSE-only; access='managed' points the caller at the
    function-layer vector_file_read (mirrors the FUSE-only writer's rejection)."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))
    with pytest.raises(ValueError, match="vector_file_read"):
        VectorGbxReader({"path": p, "access": "managed"})


def test_vector_gbx_access_external_rejected(tmp_path):
    """DataSource reader is FUSE-only; access='external' points the caller at the
    function-layer vector_file_read."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))
    with pytest.raises(ValueError, match="vector_file_read"):
        VectorGbxReader({"path": p, "access": "external"})


def test_vector_gbx_access_auto_reads_fuse(tmp_path):
    """access='auto' reads correctly (unchanged FUSE behavior)."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    rows = _read_all(VectorGbxReader({"path": p, "access": "auto"}))

    assert sorted(rows["id"]) == [1, 2]


def test_vector_gbx_access_fuse_reads_fuse(tmp_path):
    """'fuse' is an explicit synonym for the default FUSE read."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    rows = _read_all(VectorGbxReader({"path": p, "access": "fuse"}))

    assert sorted(rows["id"]) == [1, 2]


def test_members_generic_read_excludes_shapefile_sidecars(tmp_path):
    """A generic (no-driver) directory read must open only the primary vector
    file(s) and skip shapefile sidecars.

    Regression: a streamed Shapefile write produces roads.shp plus .shx/.dbf/
    .prj/.cpg sidecars in the same directory. Reading the directory back with
    no driverName previously enumerated every file (extensions=None), handing
    the .cpg to pyogrio, which rejects it ('not recognized as being in a
    supported file format'). _members() must filter to recognized primary
    extensions so only roads.shp is returned.
    """
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    d = tmp_path / "streamed_shp"
    d.mkdir()
    for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        (d / f"roads{ext}").write_bytes(b"")
    # An unrelated non-geo file must also be excluded.
    (d / "readme.txt").write_text("notes")

    rdr = VectorGbxReader({"path": str(d)})  # no driverName -> generic
    members = rdr._members()

    assert members == [str(d / "roads.shp")], members


def test_vector_staging_amortized_once_per_source(tmp_path):
    """Multiple partitions targeting the same source file stage it only once.

    Verifies the module-level _VEC_STAGED_FILES cache: calling _staged() repeatedly
    with the same path triggers shutil.copy only on the first call.
    """
    import shutil

    from databricks.labs.gbx.ds import vector as vec_mod
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    # Use a plain GeoJSON reader but force _needs_stage=True to test the caching path
    # without requiring a real GPKG binary.
    copy_count = {"n": 0}
    original_copy = shutil.copy

    def counting_copy(src, dst):
        copy_count["n"] += 1
        return original_copy(src, dst)

    # Clear the module-level cache before the test.
    saved = dict(vec_mod._VEC_STAGED_FILES)
    vec_mod._VEC_STAGED_FILES.clear()
    try:
        rdr = VectorGbxReader({"path": p})

        with (
            patch.object(rdr, "_needs_stage", return_value=True),
            patch(
                "databricks.labs.gbx.ds.vector.shutil.copy", side_effect=counting_copy
            ),
        ):
            # First staged call: should copy.
            with rdr._staged(p) as local_a:
                pass
            # Second staged call with same path: should HIT the cache, no copy.
            with rdr._staged(p) as local_b:
                pass

        assert (
            copy_count["n"] == 1
        ), f"Expected shutil.copy called once (amortization), got {copy_count['n']}"
        # Both calls yielded the same local path from the cache.
        assert local_a == local_b
    finally:
        # Restore the module-level cache to its prior state.
        vec_mod._VEC_STAGED_FILES.clear()
        vec_mod._VEC_STAGED_FILES.update(saved)


def test_vector_access_invalid_option_raises():
    """An unrecognized 'access' option raises ValueError at __init__ time."""
    import tempfile

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as f:
        f.write(b'{"type":"FeatureCollection","features":[]}')
        tmppath = f.name

    try:
        with pytest.raises(ValueError, match="access.*option must be"):
            VectorGbxReader({"path": tmppath, "access": "bogus"})
    finally:
        os.unlink(tmppath)


# ---------------------------------------------------------------------------
# IMPORTANT-1: driver-side capability probe — read() must NOT re-probe tier
# ---------------------------------------------------------------------------


def test_vector_read_does_not_reprobe_file_access_tier(tmp_path):
    """read() works correctly after Task 2: access='managed' rejects at __init__ time.

    After Task 2, access='managed'/'external' raise at __init__ with a pointer
    to vector_file_read.  The FUSE path (access='auto') must still read data
    normally.  This is the regression guard that the full read path works.
    """
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    # access='managed' must raise — reject at construction time.
    with pytest.raises(ValueError, match="vector_file_read"):
        VectorGbxReader({"path": p, "access": "managed"})

    # FUSE path (auto) must still work end-to-end.
    rdr = VectorGbxReader({"path": p, "access": "auto"})
    parts = rdr.partitions()
    batches = [b for part in parts for b in rdr.read(part)]
    tbl = pa.Table.from_batches(batches)
    assert tbl.num_rows == 2


def test_vector_auto_access_fuse_runtime_reads_correctly(tmp_path):
    """access='auto' reads data correctly (no error).

    After Task 4 no tier probe is made in __init__, so no mock is needed.
    """
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    # Must not raise — auto mode, no probe in __init__.
    rdr = VectorGbxReader({"path": p, "access": "auto"})
    rows = _read_all(rdr)

    assert sorted(rows["id"]) == [1, 2]


# ---------------------------------------------------------------------------
# Task 4 (no-reprobe) + Task 2 (managed/external rejection) regression guards
# ---------------------------------------------------------------------------


def test_vector_reader_managed_external_rejected_unconditionally(tmp_path):
    """access='managed'/'external' must ALWAYS raise with a pointer to vector_file_read.

    After Task 2, the DataSource reader is declared FUSE-only.  The rejection is
    unconditional — it does not matter what tier the runtime is, nor whether a
    SparkSession is present.  The old test (no_false_break_on_connect) asserted
    that managed/external did NOT raise; that behavior is now intentionally changed.
    """
    from databricks.labs.gbx.ds import vector as vec

    p = str(tmp_path / "pts.geojson")
    (tmp_path / "pts.geojson").write_bytes(
        b'{"type":"FeatureCollection","features":[]}'
    )

    with pytest.raises(ValueError, match="vector_file_read"):
        vec._GeoJSONReader({"path": p, "access": "managed"})


# ---------------------------------------------------------------------------
# Coded-value domain field handling (#128)
# ---------------------------------------------------------------------------


# Fixtures (conftest.py) write real OGR coded-value domains via osgeo. pyogrio
# 0.13 / GDAL 3.12 returns a sorted non-negative Integer domain (asc) as
# dictionary<string, int32> whose INDICES ARE THE STORED CODES, and the unsorted
# (uns) / negative (neg) / String (scode) domains as plain codes -- so a reader
# cannot rely on the Arrow dictionary for descriptions. Row 4 is all-NULL.
_CODES = {
    "asc": [1, 2, 3, None],
    "uns": [10, 33, 7, None],
    "neg": [-1, 0, 1, None],
    "scode": ["A", "B", None, "A"],
}
_DESCRIPTIONS = {
    "asc": ["one", "two", "three", None],
    "uns": ["ten", "thirtythree", "seven", None],
    "neg": ["minus", "zero", "plus", None],
    "scode": ["Alpha", "Bravo", None, "Alpha"],
}
_PLAIN = [100, 200, 300, None]
_RANGE = [5, 6, 7, None]
_CODED_READERS = {"gdb": "_FileGdbReader", "gpkg": "_GpkgReader"}
_CODED_FORMATS = {"gdb": "file_gdb_gbx", "gpkg": "gpkg_gbx"}


@pytest.fixture(params=["gdb", "gpkg"])
def coded_src(request):
    """(kind, path) for each real coded-domain fixture."""
    fixture = {"gdb": "gdb_with_coded_domains", "gpkg": "gpkg_with_coded_domains"}
    return request.param, request.getfixturevalue(fixture[request.param])


def _coded_reader(kind, path, mode=None):
    from databricks.labs.gbx.ds import vector as vec

    opts = {"path": path}
    if mode:
        opts["codedDomains"] = mode
    return getattr(vec, _CODED_READERS[kind])(opts)


def _read_table(rdr):
    import pyarrow as pa

    return pa.Table.from_batches([b for p in rdr.partitions() for b in rdr.read(p)])


def _assert_schema_matches_data(rdr, tbl):
    """schema() (what Spark is told) must declare exactly the Arrow types read()
    emits, or Spark rejects / mis-reads the batches."""
    from pyspark.sql.pandas.types import to_arrow_schema

    declared = to_arrow_schema(rdr.schema())
    assert declared.names == tbl.schema.names
    for name in declared.names:
        assert declared.field(name).type == tbl.schema.field(name).type, name


def test_vector_coded_domain_default_returns_codes_with_declared_types(coded_src):
    """Default codedDomains='code': every coded field yields its exact stored
    codes in the declared OGR type (Integer stays IntegerType, not string), and
    schema() agrees with the data read() produces."""
    from pyspark.sql.types import IntegerType, StringType

    kind, path = coded_src
    rdr = _coded_reader(kind, path)
    tbl = _read_table(rdr)
    for col, codes in _CODES.items():
        assert tbl.column(col).to_pylist() == codes, col
    types = {f.name: f.dataType for f in rdr.schema().fields}
    for col in ("asc", "uns", "neg"):
        assert types[col] == IntegerType(), col
    assert types["scode"] == StringType()
    _assert_schema_matches_data(rdr, tbl)


@pytest.mark.parametrize("mode", ["code", "description"])
def test_vector_coded_domain_nulls_preserved(coded_src, mode):
    """A NULL in a coded field stays NULL in both modes (never 0, -1, or the
    description of some code)."""
    kind, path = coded_src
    tbl = _read_table(_coded_reader(kind, path, mode))
    for col in ("asc", "uns", "neg"):
        assert tbl.column(col).to_pylist()[3] is None, col
    assert tbl.column("scode").to_pylist()[2] is None


@pytest.mark.parametrize("mode", ["code", "description"])
def test_vector_coded_domain_non_domain_fields_unchanged(coded_src, mode):
    """A plain field (no domain) is untouched in both modes: values and type."""
    from pyspark.sql.types import IntegerType

    kind, path = coded_src
    rdr = _coded_reader(kind, path, mode)
    tbl = _read_table(rdr)
    assert tbl.column("plain").to_pylist() == _PLAIN
    assert {f.name: f.dataType for f in rdr.schema().fields}["plain"] == IntegerType()


def test_vector_coded_domain_description_mode_returns_strings(coded_src):
    """codedDomains='description' maps EVERY coded field (dictionary-encoded or
    plain, integer or string domain) to its domain description, and schema()
    declares StringType for exactly those fields."""
    from pyspark.sql.types import IntegerType, StringType

    kind, path = coded_src
    rdr = _coded_reader(kind, path, "description")
    tbl = _read_table(rdr)
    for col, descs in _DESCRIPTIONS.items():
        assert tbl.column(col).to_pylist() == descs, col
    types = {f.name: f.dataType for f in rdr.schema().fields}
    for col in _DESCRIPTIONS:
        assert types[col] == StringType(), col
    assert types["plain"] == IntegerType()
    _assert_schema_matches_data(rdr, tbl)


@pytest.mark.parametrize("mode", ["code", "description"])
def test_vector_coded_domain_spark_read(spark, coded_src, mode):
    """End-to-end through Spark: the declared schema and the batches agree, so
    collect() returns the expected values (a mismatch fails the Arrow read)."""
    kind, path = coded_src
    register(spark)
    df = spark.read.format(_CODED_FORMATS[kind]).option("codedDomains", mode).load(path)
    rows = df.select(*_CODES, "plain").collect()
    expected = _CODES if mode == "code" else _DESCRIPTIONS
    for col, vals in expected.items():
        assert [r[col] for r in rows] == vals, col
    assert [r["plain"] for r in rows] == _PLAIN


@pytest.mark.parametrize("mode", ["code", "description"])
def test_vector_coded_domain_range_domain_passes_through(coded_src, mode):
    """A field bound to a RANGE domain carries domain metadata too, but has no
    codes: it stays an integer field with its stored values in both modes."""
    from pyspark.sql.types import IntegerType

    kind, path = coded_src
    rdr = _coded_reader(kind, path, mode)
    tbl = _read_table(rdr)
    assert tbl.column("rng").to_pylist() == _RANGE
    assert {f.name: f.dataType for f in rdr.schema().fields}["rng"] == IntegerType()
    _assert_schema_matches_data(rdr, tbl)


def test_vector_coded_domain_unresolvable_domain_raises(coded_src, monkeypatch):
    """Description mode never silently falls back to codes: a field whose coded
    domain is missing from the source's definitions fails with a clear error."""
    from databricks.labs.gbx.ds import vector as vec

    kind, path = coded_src
    monkeypatch.setattr(vec, "_read_coded_domains", lambda *a, **k: {})
    rdr = _coded_reader(kind, path, "description")
    with pytest.raises(ValueError, match="could not be resolved"):
        _read_table(rdr)


def test_vector_coded_domain_description_from_gdb_zip(gdb_with_coded_domains, tmp_path):
    """Description mode resolves domains through the zipped-FileGDB form too."""
    from databricks.labs.gbx.ds import vector as vec

    zpath = str(tmp_path / "coded.gdb.zip")
    vec._zip_gdb_bundle(gdb_with_coded_domains, zpath)
    tbl = _read_table(_coded_reader("gdb", zpath, "description"))
    for col, descs in _DESCRIPTIONS.items():
        assert tbl.column(col).to_pylist() == descs, col


def test_vector_gpkg_domains_without_constraints_table(tmp_path):
    """A GeoPackage with no gpkg_data_column_constraints table has no domain
    definitions (not an error)."""
    import geopandas as gpd
    import shapely

    from databricks.labs.gbx.ds import vector as vec

    path = str(tmp_path / "nodom.gpkg")
    gpd.GeoDataFrame({"a": [1]}, geometry=[shapely.Point(0, 0)], crs=4326).to_file(
        path, driver="GPKG"
    )
    assert vec._read_gpkg_domains(path) == {}


def test_vector_coded_domain_option_rejects_invalid_value():
    """Invalid codedDomains option value raises ValueError with clear message."""
    import tempfile

    from databricks.labs.gbx.ds.vector import _GeoJSONReader

    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as f:
        f.write(b'{"type":"FeatureCollection","features":[]}')
        tmppath = f.name

    try:
        with pytest.raises(ValueError, match="codedDomains.*code.*description"):
            _GeoJSONReader({"path": tmppath, "codedDomains": "invalid"})
    finally:
        os.unlink(tmppath)


def test_vector_members_via_shared_core(tmp_path):
    from databricks.labs.gbx.ds import vector as vec

    (tmp_path / "a.geojson").write_bytes(b"{}")
    (tmp_path / "b.json").write_bytes(b"{}")
    (tmp_path / "c.shp").write_bytes(b"x")
    (tmp_path / "_tmp.geojson").write_bytes(b"{}")
    r = vec._GeoJSONReader({"path": str(tmp_path)})
    members = r._members()
    assert members == [str(tmp_path / "a.geojson"), str(tmp_path / "b.json")]
