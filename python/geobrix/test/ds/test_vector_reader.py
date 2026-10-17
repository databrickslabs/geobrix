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


def test_vector_staging_amortized_once_per_source(tmp_path, monkeypatch):
    """Wiring: the reader stages through the shared bounded StageCache, so
    repeated partitions of one source copy it once (the cache itself is tested
    centrally in test_local_temp.py)."""
    from databricks.labs.gbx.ds import vector as vec_mod
    from databricks.labs.gbx.ds.vector import VectorGbxReader
    from databricks.labs.gbx.pyrx.core.local_temp import StageCache

    p = _gj_file_path(str(tmp_path))
    copies = []

    def counting_copy(src, dst):
        copies.append(src)
        vec_mod._copy_vec_source(src, dst)

    monkeypatch.setattr(
        vec_mod, "_VEC_STAGE", StageCache("gbx_vecstage_t_", counting_copy)
    )
    rdr = VectorGbxReader({"path": p})
    with patch.object(rdr, "_needs_stage", return_value=True):
        with rdr._staged(p) as local_a:
            pass
        with rdr._staged(p) as local_b:
            pass
    assert copies == [p] and local_a == local_b


def test_vector_reader_restages_a_source_replaced_in_place(tmp_path):
    """Wiring: the reader's StageCache validates the source signature, so a GPKG
    replaced at the same path is read fresh in the same worker process."""
    import pyarrow as pa
    import pyogrio
    import shapely

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = str(tmp_path / "pts.gpkg")

    def write(n, bump_s):
        if os.path.exists(p):
            os.remove(p)
        tbl = pa.table({"geom": shapely.to_wkb(shapely.points(range(n), range(n)))})
        pyogrio.write_arrow(
            tbl,
            p,
            layer="pts",
            driver="GPKG",
            geometry_name="geom",
            geometry_type="Point",
            crs="EPSG:4326",
        )
        st = os.stat(p)
        os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + bump_s * 1_000_000_000))

    def count():
        rdr = VectorGbxReader({"path": p, "driverName": "GPKG"})
        return sum(b.num_rows for part in rdr.partitions() for b in rdr.read(part))

    write(5, 0)
    assert count() == 5
    write(3, 5)
    assert count() == 3


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


# ---------------------------------------------------------------------------
# Streaming reads + byte-bounded batches (#127)
# ---------------------------------------------------------------------------


def _points_gpkg(path, n):
    import geopandas as gpd
    import shapely

    gpd.GeoDataFrame(
        {"id": list(range(n)), "name": [f"n{i}" for i in range(n)]},
        geometry=[shapely.Point(i, i) for i in range(n)],
        crs=4326,
    ).to_file(path, driver="GPKG")
    return path


def _read_batches(rdr):
    return [b for p in rdr.partitions() for b in rdr.read(p)]


def test_vector_stream_reads_layer_in_chunksize_batches(tmp_path, monkeypatch):
    """read() streams the layer via pyogrio.open_arrow in chunkSize batches (never a
    whole-layer read_arrow) and the rows equal a single-batch read."""
    import pyarrow as pa
    import pyogrio

    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 25)
    whole = pa.Table.from_batches(_read_batches(vec._GpkgReader({"path": path})))

    def _no_read_arrow(*a, **k):
        raise AssertionError("read() must stream with open_arrow, not read_arrow")

    monkeypatch.setattr(pyogrio, "read_arrow", _no_read_arrow)
    batches = _read_batches(vec._GpkgReader({"path": path, "chunkSize": "4"}))
    assert len(batches) == 7 and all(b.num_rows <= 4 for b in batches)
    assert pa.Table.from_batches(batches).equals(whole)


def test_vector_stream_byte_budget_slices_batches(tmp_path, monkeypatch):
    """A batch whose variable-width bytes exceed the budget is yielded as several
    slices: each holds at most the budget (or is a single larger row), rows in order."""
    import pyarrow as pa
    import pyarrow.compute as pc

    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 20)
    whole = pa.Table.from_batches(_read_batches(vec._GpkgReader({"path": path})))
    monkeypatch.setattr(vec, "_ARROW_BATCH_BYTES_LIMIT", 200)
    batches = _read_batches(vec._GpkgReader({"path": path}))
    assert len(batches) > 1
    for b in batches:
        var_bytes = sum(
            pc.sum(pc.binary_length(b.column(c))).as_py() for c in ("name", "geom")
        )
        assert b.num_rows == 1 or var_bytes <= 200
    assert pa.Table.from_batches(batches).equals(whole)


def test_vector_stream_byte_budget_applies_in_wkt_mode(tmp_path, monkeypatch):
    """WKT output is larger than its WKB source, so WKT mode slices on a smaller
    budget; the decoded geometries match the single-batch read."""
    import pyarrow as pa

    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 20)
    opts = {"path": path, "asWKB": "false"}
    whole = pa.Table.from_batches(_read_batches(vec._GpkgReader(opts)))
    monkeypatch.setattr(vec, "_ARROW_BATCH_BYTES_LIMIT", 800)
    batches = _read_batches(vec._GpkgReader(opts))
    assert len(batches) > 1
    assert pa.Table.from_batches(batches).equals(whole)


def test_vector_stream_value_over_offset_limit_raises(tmp_path, monkeypatch):
    """A single value too large for a Spark binary/string row fails with a clear
    error naming the layer, the column and the feature -- before Spark sees it."""
    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 5)
    monkeypatch.setattr(vec, "_ARROW_OFFSET_LIMIT", 10)
    with pytest.raises(
        ValueError, match=r"Feature 0 in layer .* over the 10-byte limit"
    ):
        _read_batches(vec._GpkgReader({"path": path}))


def test_vector_stream_empty_layer_yields_declared_schema(tmp_path):
    """An empty layer yields one empty batch carrying the declared schema."""
    from pyspark.sql.pandas.types import to_arrow_schema

    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 3)
    rdr = vec._GpkgReader({"path": path, "where": "id < 0"})
    batches = _read_batches(rdr)
    assert len(batches) == 1 and batches[0].num_rows == 0
    assert batches[0].schema.names == to_arrow_schema(rdr.schema()).names


def test_vector_stream_partition_feature_range(tmp_path):
    """A partition with a feature range (skip/count) streams exactly those features
    -- open_arrow takes no max_features, so the reader stops the stream itself."""
    import pyarrow as pa

    from databricks.labs.gbx.ds import vector as vec

    path = _points_gpkg(str(tmp_path / "pts.gpkg"), 25)
    rdr = vec._GpkgReader({"path": path, "chunkSize": "4"})
    (p,) = rdr.partitions()
    part = vec._ChunkPartition(p.path, p.driver, p.layer, p.as_wkb, 5, 7)
    tbl = pa.Table.from_batches(list(rdr.read(part)))
    assert tbl.column("id").to_pylist() == list(range(5, 12))


def test_vector_coded_domain_unknown_code_falls_back_to_its_text():
    """Description mode never drops a value: a code missing from its domain comes
    back as the code's text, and NULL stays NULL."""
    import pyarrow as pa

    from databricks.labs.gbx.ds import vector as vec

    out = vec._map_codes_to_descriptions(
        pa.array([1, 99, None], pa.int32()), [("1", "one")]
    )
    assert out.to_pylist() == ["one", "99", None]


def test_vector_ogr_time_fields_read_as_text(spark, tmp_path):
    """OGR Time fields (e.g. GeoJSON "12:34:56") read as strings in GDAL's format
    (HH:MM:SS, plus .sss with milliseconds) -- Spark has no TIME type -- and the
    declared schema agrees with the data (it used to fail with getLong)."""
    from pyspark.sql.types import StringType

    from databricks.labs.gbx.ds import vector as vec

    p = tmp_path / "t.geojson"
    p.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"t": t},
                        "geometry": {"type": "Point", "coordinates": [i, i]},
                    }
                    for i, t in enumerate(["12:34:56", "07:08:09.250", None])
                ],
            }
        )
    )
    rdr = vec._GeoJSONReader({"path": str(p)})
    assert rdr.schema()["t"].dataType == StringType()
    register(spark)
    rows = spark.read.format("geojson_gbx").load(str(p)).select("t").collect()
    assert [r["t"] for r in rows] == ["12:34:56", "07:08:09.250", None]


# ---------------------------------------------------------------------------
# The reader's OGR driver: forced, never passed as an open option
# ---------------------------------------------------------------------------


def _write_layer(path, driver, n=3):
    """A point layer with ``id`` / ``name`` fields, written by pyogrio."""
    import pyarrow as pa
    import pyogrio
    import shapely

    xs = list(range(n))
    tbl = pa.table(
        {
            "geometry": shapely.to_wkb(shapely.points(xs, xs)),
            "id": xs,
            "name": [f"n{i}" for i in xs],
        }
    )
    pyogrio.write_arrow(
        tbl,
        path,
        layer="pts",
        driver=driver,
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return path


def _gdal_warnings(caught):
    """GDAL warnings that every clean open used to emit (not a data problem)."""
    noise = ("open option DRIVER", "non conformant file extension")
    return [str(w.message) for w in caught if any(n in str(w.message) for n in noise)]


_NAMED = [
    ("shapefile_gbx", "ESRI Shapefile", "pts.shp"),
    ("geojson_gbx", "GeoJSON", "pts.geojson"),
    ("geojsonl_gbx", "GeoJSONSeq", "pts.geojsonl"),
    ("gpkg_gbx", "GPKG", "pts.gpkg"),
    ("file_gdb_gbx", "OpenFileGDB", "pts.gdb"),
]


def _source(name):
    from databricks.labs.gbx.ds.register import _SOURCES

    return {s.name(): s for s in _SOURCES}[name]


@pytest.mark.parametrize("fmt,driver,fname", _NAMED)
@pytest.mark.parametrize("domains", ["code", "description"])
def test_vector_reads_pass_no_driver_open_option(tmp_path, fmt, driver, fname, domains):
    """pyogrio's read functions take no ``driver`` argument: a ``driver=`` keyword
    is passed on as the GDAL open option DRIVER, which the drivers reject with a
    warning on every open and never use to pick the driver. schema() (both coded
    domain modes) and read() must open without it."""
    import warnings

    path = _write_layer(str(tmp_path / fname), driver)
    ds = _source(fmt)({"path": path, "codedDomains": domains})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert {"id", "name"} <= set(ds.schema().fieldNames())
        assert sum(b.num_rows for b in _read_batches(ds.reader(None))) == 3
    assert _gdal_warnings(caught) == []


@pytest.mark.parametrize(
    "fmt,driver,fname",
    [
        ("geojson_gbx", "GeoJSON", "src.geojson"),
        ("geojsonl_gbx", "GeoJSONSeq", "src.geojsonl"),
        ("gpkg_gbx", "GPKG", "src.gpkg"),
    ],
)
def test_vector_forced_driver_reads_a_misnamed_file(tmp_path, fmt, driver, fname):
    """The reader's driver is forced, whatever the file is named: GDAL alone picks
    the CSV driver for a GeoJSON (or GeoJSONSeq) file named data.csv and reads it as
    a garbled 0-row CSV layer."""
    import shutil
    import warnings

    misnamed = str(tmp_path / "data.csv")
    shutil.copyfile(_write_layer(str(tmp_path / fname), driver), misnamed)
    ds = _source(fmt)({"path": misnamed})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert {"id", "name"} <= set(ds.schema().fieldNames())
        batches = _read_batches(ds.reader(None))
    assert sum(b.num_rows for b in batches) == 3
    assert _gdal_warnings(caught) == []


@pytest.mark.parametrize(
    "fmt,want", [("shapefile_gbx", "ESRI Shapefile"), ("file_gdb_gbx", "OpenFileGDB")]
)
def test_vector_named_reader_rejects_a_file_of_another_format(tmp_path, fmt, want):
    """ESRI Shapefile and OpenFileGDB have no GDAL prefix that forces them (GDAL
    picks them by extension), so a file GDAL opens with another driver is rejected
    instead of being read as that format."""
    path = _write_layer(str(tmp_path / "pts.geojson"), "GeoJSON")
    with pytest.raises(
        ValueError, match=rf"pts\.geojson.*{want}.*GeoJSON driver.*geojson_gbx"
    ):
        _source(fmt)({"path": path}).schema()


def test_vector_gpkg_path_with_colons_opens(tmp_path):
    """GDAL's ``GPKG:<file>:<table>`` syntax splits on ':' -- the forced path keeps a
    file name with colons (a timestamped delivery) whole."""
    d = tmp_path / "2026-10-17T10:00:00"
    d.mkdir()
    path = _write_layer(str(d / "run:1.gpkg"), "GPKG")
    ds = _source("gpkg_gbx")({"path": path})
    assert {"id", "name"} <= set(ds.schema().fieldNames())
    assert sum(b.num_rows for b in _read_batches(ds.reader(None))) == 3


# --- the GeoJSON family, equivalent drivers, and driver names in any case -----


def _features(n=3):
    return [
        {
            "type": "Feature",
            "properties": {"id": i, "name": f"n{i}"},
            "geometry": {"type": "Point", "coordinates": [i, i]},
        }
        for i in range(n)
    ]


def _esrijson(path, n=3):
    with open(path, "w") as f:
        json.dump(
            {
                "geometryType": "esriGeometryPoint",
                "spatialReference": {"wkid": 4326},
                "fields": [
                    {"name": "id", "type": "esriFieldTypeInteger"},
                    {"name": "name", "type": "esriFieldTypeString"},
                ],
                "features": [
                    {
                        "attributes": {"id": i, "name": f"n{i}"},
                        "geometry": {"x": i, "y": i},
                    }
                    for i in range(n)
                ],
            },
            f,
        )
    return path


def _topojson(path, n=3):
    geoms = [
        {
            "type": "Point",
            "coordinates": [i, i],
            "properties": {"id": i, "name": f"n{i}"},
        }
        for i in range(n)
    ]
    with open(path, "w") as f:
        json.dump(
            {
                "type": "Topology",
                "objects": {"pts": {"type": "GeometryCollection", "geometries": geoms}},
                "arcs": [],
            },
            f,
        )
    return path


def _seq(path, n=3):
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(ft) for ft in _features(n)) + "\n")
    return path


def _rows_and_fields(ds):
    names = set(ds.schema().fieldNames())
    return sum(b.num_rows for b in _read_batches(ds.reader(None))), names


@pytest.mark.parametrize(
    "fname,make",
    [
        ("pts.geojsonl", _seq),  # GeoJSONSeq file, no multi
        ("seq.geojson", _seq),  # GeoJSONSeq content named .geojson
        ("esri.json", _esrijson),
        ("topo.json", _topojson),
        ("esri.csv", _esrijson),  # misnamed: forced with the family's prefixes
        ("topo.csv", _topojson),
        ("seq.csv", _seq),
    ],
)
def test_geojson_reader_reads_the_geojson_family(tmp_path, fname, make):
    """geojson_gbx reads GeoJSON, GeoJSONSeq, ESRIJSON and TopoJSON: whichever of
    them GDAL detects, or (for a file named for another format) the first of them
    whose GDAL prefix opens it."""
    from databricks.labs.gbx.ds import paths as P

    path = make(str(tmp_path / fname))
    rows, names = _rows_and_fields(_source("geojson_gbx")({"path": path}))
    assert rows == 3 and {"id", "name"} <= names
    expected = P.path_schema("geojson_gbx", None, path)
    assert (
        sum(
            b.num_rows
            for b in P.iter_path_batches(path, None, "geojson_gbx", None, expected)
        )
        == 3
    )


@pytest.mark.parametrize(
    "driver_name,write_driver,fname",
    [
        ("gpkg", "GPKG", "pts.gpkg"),
        ("geojson", "GeoJSON", "pts.geojson"),
        ("esri shapefile", "ESRI Shapefile", "pts.shp"),
        ("SQLite", "GPKG", "pts.gpkg"),  # GDAL picks GPKG: equivalent
        ("FileGDB", "OpenFileGDB", "pts.gdb"),  # read with OpenFileGDB
    ],
)
def test_vector_driver_name_any_case_and_equivalent_drivers(
    tmp_path, driver_name, write_driver, fname
):
    path = _write_layer(str(tmp_path / fname), write_driver)
    ds = _source("vector_gbx")({"path": path, "driverName": driver_name})
    rows, names = _rows_and_fields(ds)
    assert rows == 3 and {"id", "name"} <= names


def test_vector_driver_name_kml_reads_what_libkml_opens(tmp_path):
    path = tmp_path / "a.kml"
    path.write_text(
        '<?xml version="1.0"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
        "<Placemark><name>a</name><Point><coordinates>1,2</coordinates></Point>"
        "</Placemark></Document></kml>"
    )
    ds = _source("vector_gbx")({"path": str(path), "driverName": "KML"})
    rows, names = _rows_and_fields(ds)
    assert rows == 1 and "Name" in names


def test_vector_unknown_driver_name_lists_close_matches(tmp_path):
    ds = _source("vector_gbx")(
        {"path": str(tmp_path / "a.geojson"), "driverName": "GeoJSN"}
    )
    with pytest.raises(
        ValueError, match=r"'GeoJSN' is not an OGR vector driver.*'GeoJSON'"
    ):
        ds.schema()


@pytest.mark.parametrize(
    "fname,content,detected",
    [
        ("data.csv", b"id,WKT\n1,POINT (1 1)\n", r"with the CSV driver.*'CSV'"),
        ("data.bin", bytes(range(256)) * 4, r"not recognize it as GeoJSON/GeoJSONSeq"),
    ],
)
def test_geojson_reader_rejects_a_non_geojson_file_saying_why(
    tmp_path, fname, content, detected
):
    """A file no GeoJSON-family driver opens fails with the file, the requested
    driver and what GDAL made of it -- not a bare pyogrio error."""
    path = tmp_path / fname
    path.write_bytes(content)
    with pytest.raises(ValueError, match=rf"{fname}.*'GeoJSON'.*{detected}"):
        _source("geojson_gbx")({"path": str(path)}).schema()


@pytest.mark.parametrize("fmt", ["geojson_gbx", "vector_gbx"])
def test_vector_read_paths_opens_a_geojson_once(tmp_path, monkeypatch, fmt):
    """The per-file schema check and the read open a GeoJSON once: every open of
    a GeoJSON parses the whole file."""
    import pyogrio

    from databricks.labs.gbx.ds import paths as P

    a = _write_layer(str(tmp_path / "a.geojson"), "GeoJSON")
    b = _write_layer(str(tmp_path / "b.geojson"), "GeoJSON")
    expected = P.path_schema(fmt, None, a)
    opens = []
    for fn in ("read_info", "read_arrow", "open_arrow"):

        def spy(path, *args, _real=getattr(pyogrio, fn), _fn=fn, **kw):
            opens.append((_fn, str(path)))
            return _real(path, *args, **kw)

        monkeypatch.setattr(pyogrio, fn, spy)
    rows = sum(x.num_rows for x in P.iter_path_batches(b, None, fmt, None, expected))
    assert rows == 3
    assert [o for o in opens if o[1].endswith("b.geojson")] == [("open_arrow", b)]


def test_vector_reader_reads_a_members_info_once(tmp_path, monkeypatch):
    import pyogrio

    path = _write_layer(str(tmp_path / "pts.gpkg"), "GPKG")
    rdr = _source("gpkg_gbx")({"path": path}).reader(None)
    calls = []
    real = pyogrio.read_info
    monkeypatch.setattr(
        pyogrio, "read_info", lambda *a, **k: calls.append(a) or real(*a, **k)
    )
    rdr.schema()
    rdr.declared_schema()
    assert len(calls) == 1


def test_vector_misnamed_gpkg_resolves_coded_domains(gpkg_with_coded_domains, tmp_path):
    """Description mode reads a misnamed GeoPackage's domain definitions through
    the same forced driver as its features."""
    import shutil

    misnamed = str(tmp_path / "data.csv")
    shutil.copyfile(gpkg_with_coded_domains, misnamed)
    want = _read_table(_coded_reader("gpkg", gpkg_with_coded_domains, "description"))
    rdr = _coded_reader("gpkg", misnamed, "description")
    got = _read_table(rdr)
    for col in _DESCRIPTIONS:
        assert got.column(col).to_pylist() == want.column(col).to_pylist(), col
    _assert_schema_matches_data(rdr, got)


@pytest.mark.parametrize("fmt", ["geojson_gbx", "shapefile_gbx", "vector_gbx"])
def test_vector_missing_file_is_reported_missing(tmp_path, fmt):
    """A path that does not exist fails as missing, not as a driver mismatch."""
    from pyogrio.errors import DataSourceError

    opts = {"path": str(tmp_path / "gone"), "driverName": "GeoJSON"}
    with pytest.raises(DataSourceError, match="No such file"):
        _source(fmt)(opts if fmt == "vector_gbx" else {"path": opts["path"]}).schema()
