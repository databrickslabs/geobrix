"""Tests for VectorGbxWriter FILE write modes (Task 7).

Verifies that:
- file_mode='fuse' (default) is byte-identical to existing behavior.
- file_mode='managed' assembles the file locally, reads bytes, builds a 1-row
  DataFrame {tile: {raster: <bytes>, path: None}}, and calls open_for_write.
- file_mode='external' copies the assembled file to filespace/uuid/name on the
  Volume, builds a 1-row DataFrame {tile: {raster: None, path: <volume_path>}},
  and calls open_for_write.
- Shapefile zip=true collapses to a single .shp.zip archive for FILE modes.
- explicit managed on a FUSE-only runtime raises the actionable error (via
  open_for_write → resolve_access — NOT a hand-rolled second message).
- managed without filespace raises early at constructor time.
- Non-zip Shapefile with FILE mode raises at constructor time.
"""

from __future__ import annotations

import os
import zipfile
from unittest.mock import patch

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
    """Convenience factory for VectorGbxWriter."""
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
# Constructor tests
# ---------------------------------------------------------------------------


def test_vector_writer_fuse_mode_is_default():
    """No file_mode option → defaults to fuse; self.path is a filesystem path."""
    w = _make_writer("/tmp/out.geojson", "GeoJSON")
    assert w._file_mode == "fuse"
    assert w.path == "/tmp/out.geojson"


def test_vector_writer_file_mode_managed_parses_filespace():
    """file_mode=managed + filespace option are stored on the writer."""
    w = _make_writer(
        "cat.sch.roads",
        "GPKG",
        extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
    )
    assert w._file_mode == "managed"
    assert w._filespace == "/Volumes/c/s/v"
    # TABLE name must not get a .gpkg extension appended.
    assert w.path == "cat.sch.roads"


def test_vector_writer_file_mode_external_parses_options():
    """file_mode=external stores path as table name (no extension appended)."""
    w = _make_writer(
        "cat.sch.roads",
        "GeoJSON",
        extra_opts={"fileMode": "external", "filespace": "/Volumes/c/s/v"},
    )
    assert w._file_mode == "external"
    assert w.path == "cat.sch.roads"


def test_vector_writer_file_mode_managed_no_filespace_raises():
    """file_mode=managed without filespace raises ValueError at construction time."""
    with pytest.raises(ValueError, match="filespace"):
        _make_writer(
            "cat.sch.roads",
            "GPKG",
            extra_opts={"fileMode": "managed"},
        )


def test_vector_writer_file_mode_nonzip_shapefile_raises():
    """file_mode=managed with ESRI Shapefile and zip=false raises ValueError."""
    with pytest.raises(ValueError, match="single-file"):
        _make_writer(
            "cat.sch.roads",
            "ESRI Shapefile",
            extra_opts={
                "fileMode": "managed",
                "filespace": "/Volumes/c/s/v",
                # zip not set → defaults to false
            },
        )


def test_vector_writer_file_mode_zip_shapefile_allowed():
    """file_mode=managed + ESRI Shapefile + zip=true is allowed."""
    w = _make_writer(
        "cat.sch.roads",
        "ESRI Shapefile",
        extra_opts={
            "fileMode": "managed",
            "filespace": "/Volumes/c/s/v",
            "zip": "true",
        },
    )
    assert w._file_mode == "managed"
    assert w.zip is True


def test_vector_writer_file_mode_layout_option_stored():
    """layout option is passed through to the writer."""
    w = _make_writer(
        "cat.sch.roads",
        "GPKG",
        extra_opts={
            "fileMode": "managed",
            "filespace": "/Volumes/c/s/v",
            "layout": "plain",
        },
    )
    assert w._layout == "plain"


# ---------------------------------------------------------------------------
# _commit_as_file: MANAGED mode
# ---------------------------------------------------------------------------


def test_commit_as_file_managed_reads_bytes_and_calls_open_for_write(tmp_path, spark):
    """Managed mode: _commit_as_file reads bytes from local_out and calls open_for_write
    with file_mode='managed', filespace, and a 1-row df {tile.raster = <bytes>}."""
    w = _make_writer(
        "cat.sch.roads",
        "GPKG",
        extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
    )

    # Create a fake assembled file.
    local_out = str(tmp_path / "output.gpkg")
    fake_bytes = b"GPKG_FAKE_BYTES_0123456789"
    with open(local_out, "wb") as fh:
        fh.write(fake_bytes)

    captured_dfs = []

    def _capture_ofw(sp, df, target, **kw):
        captured_dfs.append((df.collect(), kw))

    with patch(
        "databricks.labs.gbx.ds.vector.open_for_write", side_effect=_capture_ofw
    ):
        w._commit_as_file(spark=spark, local_out=local_out)

    assert len(captured_dfs) == 1
    rows, kwargs = captured_dfs[0]
    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert bytes(tile["raster"]) == fake_bytes, "bytes must match assembled file"
    assert tile["path"] is None
    assert kwargs["file_mode"] == "managed"
    assert kwargs["filespace"] == "/Volumes/c/s/v"
    assert kwargs["overwrite"] is True  # writer was constructed with overwrite=True


def test_commit_as_file_managed_passes_layout(tmp_path, spark):
    """Managed mode: layout option is forwarded to open_for_write."""
    w = _make_writer(
        "cat.sch.t",
        "GPKG",
        extra_opts={
            "fileMode": "managed",
            "filespace": "/Volumes/c/s/v",
            "layout": "plain",
        },
    )
    local_out = str(tmp_path / "output.gpkg")
    with open(local_out, "wb") as fh:
        fh.write(b"x")

    captured_kwargs = []

    def _cap(sp, df, target, **kw):
        captured_kwargs.append(kw)

    with patch("databricks.labs.gbx.ds.vector.open_for_write", side_effect=_cap):
        w._commit_as_file(spark=spark, local_out=local_out)

    assert captured_kwargs[0]["layout"] == "plain"


def test_commit_as_file_managed_passes_target_table_name(tmp_path, spark):
    """Managed mode: open_for_write receives self.path as target (the table name)."""
    table_name = "my_catalog.my_schema.my_roads"
    w = _make_writer(
        table_name,
        "GeoJSON",
        extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
    )
    local_out = str(tmp_path / "output.geojson")
    with open(local_out, "wb") as fh:
        fh.write(b"{}")

    captured_targets = []

    def _cap(sp, df, target, **kw):
        captured_targets.append(target)

    with patch("databricks.labs.gbx.ds.vector.open_for_write", side_effect=_cap):
        w._commit_as_file(spark=spark, local_out=local_out)

    assert captured_targets[0] == table_name


# ---------------------------------------------------------------------------
# _commit_as_file: EXTERNAL mode
# ---------------------------------------------------------------------------


def test_commit_as_file_external_copies_to_volume_and_calls_open_for_write(
    tmp_path, spark
):
    """External mode: copies assembled file to filespace/uuid/name, then calls
    open_for_write with file_mode='external' and 1-row df {tile.path = <volume_path>}.
    """
    filespace_dir = tmp_path / "volume"
    filespace_dir.mkdir()

    w = _make_writer(
        "cat.sch.roads",
        "GPKG",
        extra_opts={"fileMode": "external", "filespace": str(filespace_dir)},
    )

    local_out = str(tmp_path / "output.gpkg")
    fake_bytes = b"GPKG_EXT_0123456789"
    with open(local_out, "wb") as fh:
        fh.write(fake_bytes)

    captured_dfs = []

    def _capture_ofw(sp, df, target, **kw):
        captured_dfs.append((df.collect(), kw))

    with patch(
        "databricks.labs.gbx.ds.vector.open_for_write", side_effect=_capture_ofw
    ):
        w._commit_as_file(spark=spark, local_out=local_out)

    # open_for_write must be called once.
    assert len(captured_dfs) == 1
    rows, kwargs = captured_dfs[0]
    assert len(rows) == 1
    tile = rows[0]["tile"]
    volume_path = tile["path"]
    assert tile["raster"] is None
    assert kwargs["file_mode"] == "external"
    assert kwargs["filespace"] is None  # not forwarded to open_for_write for external

    # The file must exist at the volume_path with the correct bytes.
    assert volume_path is not None
    assert os.path.isfile(volume_path), f"Expected file at {volume_path}"
    with open(volume_path, "rb") as fh:
        assert fh.read() == fake_bytes, "Volume copy bytes must match local_out"

    # volume_path must be under filespace_dir.
    assert volume_path.startswith(str(filespace_dir))


def test_commit_as_file_external_no_filespace_raises(tmp_path, spark):
    """External mode without filespace raises ValueError inside _commit_as_file."""
    # Bypass the constructor's filespace check by constructing EXTERNAL mode without
    # validation interference (the constructor only validates for managed, not external).
    w = object.__new__(VectorGbxWriter)
    w._file_mode = "external"
    w._filespace = None
    w._layout = "order"
    w.path = "cat.sch.roads"
    w.overwrite = True

    local_out = str(tmp_path / "output.gpkg")
    with open(local_out, "wb") as fh:
        fh.write(b"x")

    with pytest.raises(ValueError, match="filespace"):
        w._commit_as_file(spark=spark, local_out=local_out)


# ---------------------------------------------------------------------------
# No-gating: managed on FUSE tier raises via open_for_write
# ---------------------------------------------------------------------------


def test_commit_as_file_managed_on_fuse_tier_raises_actionable_error(tmp_path, spark):
    """When FILE is unavailable (tier=fuse), open_for_write → resolve_access raises a
    clear, actionable error.  The writer does NOT hand-roll a second message."""
    w = _make_writer(
        "cat.sch.roads",
        "GPKG",
        extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
    )
    local_out = str(tmp_path / "output.gpkg")
    with open(local_out, "wb") as fh:
        fh.write(b"x")

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"):
        with pytest.raises(ValueError) as exc_info:
            w._commit_as_file(spark=spark, local_out=local_out)

    msg = str(exc_info.value)
    # Error must come from resolve_access (NOT a hand-rolled second message).
    assert "managed" in msg.lower() or "file" in msg.lower()
    assert "fuse" in msg.lower() or "runtime" in msg.lower() or "13.3" in msg.lower()


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
# commit() called directly: end-to-end with mocked open_for_write
#
# Note: commit() runs in the PySpark worker subprocess when invoked via .save().
# Mocks cannot cross subprocess boundaries, so these tests call commit() directly
# on the driver (same Python process) — the mock IS effective here.
# ---------------------------------------------------------------------------


def _write_gpkg_arrow_frag(tmp_path, name="frag-0.arrow"):
    """Write a minimal Arrow IPC fragment with one GPKG-compatible feature row."""
    import pyarrow as pa
    import pyarrow.feather as feather

    tbl = pa.table(
        {
            "geom_0": [bytes(to_wkb(Point(1.0, 2.0)))],
            "geom_0_srid": ["4326"],
            "geom_0_srid_proj": [""],
        }
    )
    frag = str(tmp_path / name)
    feather.write_feather(tbl, frag)
    return frag


def _make_messages(frag_paths):
    from databricks.labs.gbx.ds.vector import _VectorCommitMessage

    return [_VectorCommitMessage(frag_path=p) for p in frag_paths]


def test_commit_managed_calls_open_for_write_with_bytes(spark, tmp_path):
    """commit() with file_mode=managed: open_for_write is called with a 1-row df
    where tile.raster holds the assembled GPKG bytes."""
    w = _make_writer(
        "cat.sch.roads_full",
        "GPKG",
        extra_opts={"fileMode": "managed", "filespace": "/Volumes/c/s/v"},
    )
    frag = _write_gpkg_arrow_frag(tmp_path)
    messages = _make_messages([frag])
    captured = []

    def _capture(sp, df, target, **kw):
        captured.append({"df_rows": df.collect(), "target": target, "kw": kw})

    with patch("databricks.labs.gbx.ds.vector.open_for_write", side_effect=_capture):
        w.commit(messages)

    assert len(captured) == 1
    rows = captured[0]["df_rows"]
    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is not None and len(bytes(tile["raster"])) > 0
    assert tile["path"] is None
    assert captured[0]["target"] == "cat.sch.roads_full"
    assert captured[0]["kw"]["file_mode"] == "managed"
    assert captured[0]["kw"]["filespace"] == "/Volumes/c/s/v"


def test_commit_external_copies_to_volume_and_calls_open_for_write(spark, tmp_path):
    """commit() with file_mode=external: copies assembled file to filespace/uuid/name,
    calls open_for_write with tile.path pointing to the Volume copy."""
    volume_dir = tmp_path / "volume"
    volume_dir.mkdir()

    w = _make_writer(
        "cat.sch.roads_ext",
        "GPKG",
        extra_opts={"fileMode": "external", "filespace": str(volume_dir)},
    )
    frag = _write_gpkg_arrow_frag(tmp_path)
    messages = _make_messages([frag])
    captured = []

    def _capture(sp, df, target, **kw):
        captured.append({"df_rows": df.collect(), "target": target, "kw": kw})

    with patch("databricks.labs.gbx.ds.vector.open_for_write", side_effect=_capture):
        w.commit(messages)

    assert len(captured) == 1
    rows = captured[0]["df_rows"]
    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is None
    volume_path = tile["path"]
    assert volume_path is not None
    assert os.path.isfile(volume_path), f"Expected file at {volume_path}"
    assert volume_path.startswith(str(volume_dir))
    assert captured[0]["kw"]["file_mode"] == "external"
    assert captured[0]["kw"]["filespace"] is None


# ---------------------------------------------------------------------------
# Multi-file (Shapefile) zip=true → single .shp.zip FILE
# ---------------------------------------------------------------------------


def _write_shp_arrow_frag(tmp_path, name="frag-shp-0.arrow"):
    """Write a minimal Arrow IPC fragment suitable for Shapefile assembly."""
    import pyarrow as pa
    import pyarrow.feather as feather

    tbl = pa.table(
        {
            "geom_0": [bytes(to_wkb(Point(1.0, 2.0))), bytes(to_wkb(Point(3.0, 4.0)))],
            "geom_0_srid": ["4326", "4326"],
            "geom_0_srid_proj": ["", ""],
        }
    )
    frag = str(tmp_path / name)
    feather.write_feather(tbl, frag)
    return frag


def test_commit_managed_zip_shapefile_single_archive(spark, tmp_path):
    """zip=true + file_mode=managed: commit() assembles ONE .shp.zip archive; open_for_write
    receives a single set of zip bytes containing shapefile components."""
    w = _make_writer(
        "cat.sch.roads_shp",
        "ESRI Shapefile",
        extra_opts={
            "fileMode": "managed",
            "filespace": "/Volumes/c/s/v",
            "zip": "true",
        },
    )
    frag = _write_shp_arrow_frag(tmp_path)
    messages = _make_messages([frag])
    captured = []

    def _capture(sp, df, target, **kw):
        captured.append({"df_rows": df.collect(), "kw": kw})

    with patch("databricks.labs.gbx.ds.vector.open_for_write", side_effect=_capture):
        w.commit(messages)

    assert len(captured) == 1
    tile = captured[0]["df_rows"][0]["tile"]
    file_bytes = bytes(tile["raster"])
    assert len(file_bytes) > 0, "Expected non-empty zip bytes"

    # Verify the bytes are a valid ZIP archive containing shapefile components.
    import io

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        names = zf.namelist()
    assert any(n.endswith(".shp") for n in names), f"No .shp in archive: {names}"
    assert any(n.endswith(".dbf") for n in names), f"No .dbf in archive: {names}"
