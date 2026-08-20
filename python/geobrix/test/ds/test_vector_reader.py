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
# FILE access mode tests (Task 6: vector reader gains file_gbx FILE read)
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


def test_vector_file_read_managed_tier_returns_same_rows(tmp_path):
    """With access='managed' and FILE-capable tier, rows match the FUSE baseline."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    # Baseline: FUSE read (no access option)
    rdr_fuse = VectorGbxReader({"path": p})
    fuse_rows = _read_all(rdr_fuse)

    # FILE read: mock the tier as FILE-capable so managed access is allowed
    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier_vec:
        mock_tier_vec.return_value = "read_files"
        rdr_file = VectorGbxReader({"path": p, "access": "managed"})
        file_rows = _read_all(rdr_file)

    assert file_rows["id"] == fuse_rows["id"]
    assert len(file_rows["id"]) == 2


def test_vector_file_read_external_tier_returns_same_rows(tmp_path):
    """With access='external' and FILE-capable tier, rows match the FUSE baseline."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    rdr_fuse = VectorGbxReader({"path": p})
    fuse_rows = _read_all(rdr_fuse)

    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier_vec:
        mock_tier_vec.return_value = "list_files"
        rdr_file = VectorGbxReader({"path": p, "access": "external"})
        file_rows = _read_all(rdr_file)

    assert sorted(file_rows["id"]) == sorted(fuse_rows["id"])
    assert len(file_rows["id"]) == 2


def test_vector_fuse_fallback_access_auto_unchanged(tmp_path):
    """access='auto' on a FUSE-only runtime reads correctly (unchanged FUSE behavior)."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier_vec:
        mock_tier_vec.return_value = "fuse"
        rdr = VectorGbxReader({"path": p, "access": "auto"})
        rows = _read_all(rdr)

    assert sorted(rows["id"]) == [1, 2]


def test_vector_explicit_file_on_fuse_raises(tmp_path):
    """access='managed' on a FUSE-only runtime raises ValueError at construction (driver-side).

    The NO-GATING error must be raised in __init__ (driver-side), not per-partition
    in read() (executor-side). On Spark Connect workers getActiveSession() returns None
    so probing the tier in read() would silently resolve to "fuse" on FILE-capable
    runtimes, giving a false positive for valid managed/external requests.
    """
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier_vec:
        mock_tier_vec.return_value = "fuse"
        with pytest.raises(ValueError, match="Requested managed FILE access mode"):
            VectorGbxReader({"path": p, "access": "managed"})


def test_vector_explicit_external_on_fuse_raises(tmp_path):
    """access='external' on a FUSE-only runtime raises ValueError at construction (driver-side)."""
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier_vec:
        mock_tier_vec.return_value = "fuse"
        with pytest.raises(ValueError, match="Requested external FILE access mode"):
            VectorGbxReader({"path": p, "access": "external"})


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
    """read() must NOT call file_access_tier — validation was moved to __init__.

    On Spark Connect workers SparkSession.getActiveSession() returns None so
    file_access_tier() called from read() always resolves to 'fuse', giving a
    false ValueError for valid FILE-capable runtimes.  After the fix, read()
    does not probe at all — the resolved tier is established once in __init__.
    """
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    # Construct with FILE-capable tier (so __init__ doesn't raise)
    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_init_tier:
        mock_init_tier.return_value = "read_files"
        rdr = VectorGbxReader({"path": p, "access": "managed"})

    # read() must not call file_access_tier at all
    mock_init_tier.reset_mock()
    parts = rdr.partitions()
    batches = [b for part in parts for b in rdr.read(part)]
    assert (
        not mock_init_tier.called
    ), "read() called file_access_tier — it should not; the tier was resolved in __init__"
    # Verify rows were actually read (regression guard)
    tbl = pa.Table.from_batches(batches)
    assert tbl.num_rows == 2


def test_vector_auto_access_fuse_runtime_reads_correctly(tmp_path):
    """access='auto' on a fuse-only runtime reads data correctly (no error).

    With the driver-side probe, auto mode on a fuse-only runtime must not raise
    even when the init-time probe runs without a full Spark session.
    """
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_file_path(str(tmp_path))

    with patch("databricks.labs.gbx.ds.vector.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        # Must not raise — auto downgrades silently
        rdr = VectorGbxReader({"path": p, "access": "auto"})
        rows = _read_all(rdr)

    assert sorted(rows["id"]) == [1, 2]
