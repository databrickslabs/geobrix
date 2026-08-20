"""Tests for VectorGbxWriter FILE-related behavior (Task 7).

After Task 7 the DataSource writer (df.write.format) is FUSE-only.
FILE-tier writes moved to pyvx.file_write.vector_file_write.

Test inventory:
- FUSE-mode default behavior (unchanged)
- Rejection of file_mode=managed/external at construction (new behavior)
- Full FUSE-mode write round-trip (unchanged)

Relocated coverage (moved to test/pyvx/test_file_write.py):
- Managed bytes assembly → test_vector_file_write_managed_builds_bytes_row_and_delegates
- External Volume copy → test_vector_file_write_external_copies_and_references
- layout forwarding → test_vector_file_write_layout_forwarded
- target forwarding → test_vector_file_write_target_forwarded
- external-without-filespace error → test_vector_file_write_external_no_filespace_raises

Dropped coverage:
- commit() FILE mode integration (managed/external never reach commit() now)
- zip+FILE single-archive integration (zip assembly in FUSE mode is covered by
  test_vector_writer.py; zip+FILE is now a two-step caller operation)
- _commit_as_file method (deleted; all coverage in test_file_write.py)
- FUSE-tier actionable error (open_for_write behavior, tested below gbx_file_write mock)
"""

from __future__ import annotations

import os

import pytest
from pyspark.sql.types import BinaryType, StringType, StructField, StructType
from shapely import Point, to_wkb

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import VectorGbxWriter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_POINT_SCHEMA = StructType(
    [
        StructField("geom_0", BinaryType(), True),
        StructField("geom_0_srid", StringType(), True),
        StructField("geom_0_srid_proj", StringType(), True),
    ]
)


def _make_writer(path, driver, extra_opts=None, overwrite=True):
    """Convenience factory for VectorGbxWriter (FUSE mode only after Task 7)."""
    opts = {"driverName": driver}
    if extra_opts:
        opts.update(extra_opts)
    return VectorGbxWriter(path, _POINT_SCHEMA, driver, opts, overwrite)


def _wkb_df(spark):
    rows = [
        ("a", bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    )


# ---------------------------------------------------------------------------
# Constructor tests — FUSE mode (unchanged)
# ---------------------------------------------------------------------------


def test_vector_writer_fuse_mode_is_default():
    """No file_mode option → defaults to fuse; self.path is a filesystem path."""
    w = _make_writer("/tmp/out.geojson", "GeoJSON")
    assert w._file_mode == "fuse"
    assert w.path == "/tmp/out.geojson"


# ---------------------------------------------------------------------------
# Constructor tests — FILE modes are now REJECTED (Task 7)
# Coverage for the rejected behavior lives in test/pyvx/test_file_write.py.
# ---------------------------------------------------------------------------


def test_vector_writer_file_mode_managed_raises_with_function_layer_hint():
    """file_mode=managed raises ValueError pointing at vector_file_write."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "GPKG",
            extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
        )


def test_vector_writer_file_mode_external_raises_with_function_layer_hint():
    """file_mode=external raises ValueError pointing at vector_file_write."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "GeoJSON",
            extra_opts={"fileMode": "external", "filespace": "/Volumes/c/s/v"},
        )


def test_vector_writer_file_mode_managed_no_filespace_also_raises():
    """file_mode=managed without filespace raises ValueError (rejection before filespace check)."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "GPKG",
            extra_opts={"fileMode": "managed"},
        )


def test_vector_writer_file_mode_opengdb_managed_raises():
    """file_mode=managed with OpenFileGDB raises ValueError (rejection before driver check)."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "OpenFileGDB",
            extra_opts={
                "fileMode": "managed",
                "filespace": "/Volumes/c/s/v",
            },
        )


def test_vector_writer_file_mode_opengdb_zip_managed_raises():
    """OpenFileGDB + zip=true + file_mode=managed is still rejected."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "OpenFileGDB",
            extra_opts={
                "fileMode": "managed",
                "filespace": "/Volumes/c/s/v",
                "zip": "true",
            },
        )


def test_vector_writer_file_mode_nonzip_shapefile_managed_raises():
    """file_mode=managed with ESRI Shapefile raises ValueError."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "ESRI Shapefile",
            extra_opts={
                "fileMode": "managed",
                "filespace": "/Volumes/c/s/v",
            },
        )


def test_vector_writer_file_mode_zip_shapefile_managed_raises():
    """file_mode=managed + ESRI Shapefile + zip=true is also rejected."""
    with pytest.raises(ValueError, match="vector_file_write"):
        _make_writer(
            "cat.sch.roads",
            "ESRI Shapefile",
            extra_opts={
                "fileMode": "managed",
                "filespace": "/Volumes/c/s/v",
                "zip": "true",
            },
        )


def test_vector_writer_file_mode_managed_error_is_fuse_only():
    """The rejection error message says the writer is FUSE-only."""
    with pytest.raises(ValueError, match="FUSE-only"):
        _make_writer(
            "cat.sch.roads",
            "GPKG",
            extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
        )


# ---------------------------------------------------------------------------
# Full Spark write: FUSE mode (safe to test via .save(); no Databricks APIs needed)
# ---------------------------------------------------------------------------


def test_full_write_fuse_mode_unchanged(spark, tmp_path):
    """FUSE mode (no fileMode option) writes file to disk unchanged (byte-identical
    to existing behavior; open_for_write is never called)."""
    register(spark)
    out = str(tmp_path / "roads.gpkg")
    _wkb_df(spark).coalesce(1).write.format("gpkg_gbx").mode("overwrite").save(out)
    assert os.path.isfile(out), "FUSE write must produce a file at self.path"
    # Verify round-trip: file is a valid GPKG with 2 rows.
    back = spark.read.format("gpkg_gbx").load(out)
    assert back.count() == 2


# ---------------------------------------------------------------------------
# Task 7: DataSource writer is now FUSE-only — FILE modes raise at construction
# (behavior moved to pyvx.file_write.vector_file_write)
# ---------------------------------------------------------------------------


def test_vector_datasource_writer_rejects_file_modes_pointing_to_function_layer():
    """file_mode=managed/external must raise ValueError pointing at vector_file_write.

    The DataSource writer (df.write.format) is FUSE-only after Task 7.  FILE-tier
    writes moved to the function layer (pyvx.file_write.vector_file_write).
    """
    from pyspark.sql.types import BinaryType, StructField, StructType

    schema = StructType([StructField("geom", BinaryType())])
    for mode in ("managed", "external"):
        try:
            VectorGbxWriter(
                "cat.sch.tbl",
                schema,
                "GeoJSON",
                {
                    "filemode": mode,
                    "filespace": "/Volumes/c/s/v",
                    "driverName": "GeoJSON",
                },
                overwrite=True,
            )
            raised = False
        except ValueError as e:
            raised = True
            assert "vector_file_write" in str(
                e
            ), f"expected 'vector_file_write' in error for fileMode={mode}, got: {e}"
        assert raised, f"expected ValueError for fileMode={mode}"
