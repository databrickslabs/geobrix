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

    with pytest.raises(ValueError, match="vector_file_read"):
        vec._GeoJSONReader({"path": p, "access": "external"})


def test_vector_members_via_shared_core(tmp_path):
    from databricks.labs.gbx.ds import vector as vec

    (tmp_path / "a.geojson").write_bytes(b"{}")
    (tmp_path / "b.json").write_bytes(b"{}")
    (tmp_path / "c.shp").write_bytes(b"x")
    (tmp_path / "_tmp.geojson").write_bytes(b"{}")
    r = vec._GeoJSONReader({"path": str(tmp_path)})
    members = r._members()
    assert members == [str(tmp_path / "a.geojson"), str(tmp_path / "b.json")]
