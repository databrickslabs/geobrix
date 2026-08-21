"""Task 2: gbx_file_read — new [path, size, file] reference contract.

Old tests that asserted a `content` column have been replaced: the previous
implementation returned bytes (via binaryFile / read_files SELECT content), which
was both wrong (read_files(format=>'file') has no `content` column) and against
the design (gbx_file_read returns a reference, never bytes).

New contract: returns DataFrame[path STRING, size BIGINT, file <FILE-ref or null>].
"""

from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.gbx.ds import file_gbx

# ---------------------------------------------------------------------------
# _classify_source (unchanged helper — kept as a smoke test)
# ---------------------------------------------------------------------------


def test_source_type_auto_classifies_path_vs_table():
    assert file_gbx._classify_source("/Volumes/c/s/v/x") == "location"
    assert file_gbx._classify_source("dbfs:/Volumes/c/s/v") == "location"
    assert file_gbx._classify_source("s3://bucket/key") == "location"
    assert file_gbx._classify_source("catalog.schema.table") == "table"


# ---------------------------------------------------------------------------
# location + read_files tier → enumerate_files → DataFrame [path,size,file]
# ---------------------------------------------------------------------------


def test_location_read_files_tier_uses_enumerate_files_no_content():
    """location + read_files tier: enumerate_files → DataFrame [path,size,file], no content."""
    mock_spark = MagicMock()
    fake_df = MagicMock()

    with (
        patch(
            "databricks.labs.gbx.ds.file_gbx.file_access_tier",
            return_value="read_files",
        ),
        patch(
            "databricks.labs.gbx.ds.file_gbx.enumerate_files", return_value=fake_df
        ) as mock_enum,
    ):
        out = file_gbx.gbx_file_read(
            mock_spark, "/Volumes/c/s/v", source_type="location"
        )

    mock_enum.assert_called_once()
    # Must not fall back to binaryFile or run raw SQL
    mock_spark.read.format.assert_not_called()
    # The returned DataFrame comes directly from enumerate_files (no re-selection)
    assert out is fake_df


# ---------------------------------------------------------------------------
# location + FUSE tier → enumerate_files → list → createDataFrame
# ---------------------------------------------------------------------------


def test_location_fuse_tier_normalizes_list_to_dataframe():
    """location + FUSE tier: enumerate_files → list of dicts → createDataFrame [path,size,file]."""
    mock_spark = MagicMock()
    fuse_list = [
        {"path": "/Volumes/c/s/v/a.tif", "size": 100, "file": None},
        {"path": "/Volumes/c/s/v/b.tif", "size": 200, "file": None},
    ]
    fake_df = MagicMock()
    mock_spark.createDataFrame.return_value = fake_df

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"),
        patch(
            "databricks.labs.gbx.ds.file_gbx.enumerate_files", return_value=fuse_list
        ),
    ):
        out = file_gbx.gbx_file_read(
            mock_spark, "/Volumes/c/s/v", source_type="location"
        )

    mock_spark.createDataFrame.assert_called_once()
    call_args = mock_spark.createDataFrame.call_args
    # First positional arg is the data list
    passed_data = call_args[0][0]
    assert passed_data == fuse_list
    # Schema keyword arg must be a StructType with path, size, file
    schema_arg = call_args[1].get("schema") or (
        call_args[0][1] if len(call_args[0]) > 1 else None
    )
    assert schema_arg is not None, "schema argument must be passed to createDataFrame"
    field_names = [f.name for f in schema_arg.fields]
    assert field_names == ["path", "size", "file"]
    # file column must be nullable (holds None on FUSE)
    file_field = next(f for f in schema_arg.fields if f.name == "file")
    assert file_field.nullable
    # Must NOT call binaryFile
    mock_spark.read.format.assert_not_called()
    assert out is fake_df


# ---------------------------------------------------------------------------
# location + list_files tier → enumerate_files → DataFrame (no normalization)
# ---------------------------------------------------------------------------


def test_location_list_files_tier_uses_enumerate_files():
    """location + list_files tier: enumerate_files returns DataFrame directly."""
    mock_spark = MagicMock()
    fake_df = MagicMock()

    with (
        patch(
            "databricks.labs.gbx.ds.file_gbx.file_access_tier",
            return_value="list_files",
        ),
        patch(
            "databricks.labs.gbx.ds.file_gbx.enumerate_files", return_value=fake_df
        ) as mock_enum,
    ):
        out = file_gbx.gbx_file_read(
            mock_spark, "/Volumes/c/s/v", source_type="location"
        )

    mock_enum.assert_called_once()
    # DataFrame path: no createDataFrame, no binaryFile, no raw SQL
    mock_spark.createDataFrame.assert_not_called()
    mock_spark.read.format.assert_not_called()
    mock_spark.sql.assert_not_called()
    assert out is fake_df


# ---------------------------------------------------------------------------
# access gating: external on FUSE tier raises
# ---------------------------------------------------------------------------


def test_access_external_on_fuse_tier_raises():
    """access='external' on fuse tier raises a clear ValueError."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"):
        with pytest.raises(ValueError) as exc_info:
            file_gbx.gbx_file_read(
                mock_spark,
                "/Volumes/c/s/v",
                source_type="location",
                access="external",
            )

    err = str(exc_info.value)
    assert "external" in err.lower()
    # Must name the requirement and suggest auto
    assert "DBR" in err or "FILE" in err
    assert "auto" in err.lower()


# ---------------------------------------------------------------------------
# access gating: managed on location source raises
# ---------------------------------------------------------------------------


def test_access_managed_on_location_source_raises():
    """access='managed' on a location/path source raises a clear ValueError."""
    mock_spark = MagicMock()

    # Tier detection should not even be needed for this path, but patching for safety
    with patch(
        "databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="read_files"
    ):
        with pytest.raises(ValueError) as exc_info:
            file_gbx.gbx_file_read(
                mock_spark,
                "/Volumes/c/s/v",
                source_type="location",
                access="managed",
            )

    err = str(exc_info.value)
    assert "managed" in err.lower()
    # Must suggest alternatives
    assert "external" in err.lower() or "table" in err.lower() or "auto" in err.lower()
    # Must not suggest a silent downgrade
    assert "silently" not in err.lower()


# ---------------------------------------------------------------------------
# access gating: managed on table source → delegates to read_file_table
# ---------------------------------------------------------------------------


def test_access_managed_on_table_source_delegates_resolve_file_table():
    """access='managed' on table source delegates to resolve_file_table → [path,size,file]."""
    mock_spark = MagicMock()
    resolved_df = MagicMock()
    selected_df = MagicMock()
    resolved_df.select.return_value = selected_df

    with (
        patch(
            "databricks.labs.gbx.ds.file_gbx.file_access_tier",
            return_value="read_files",
        ),
        patch(
            "databricks.labs.gbx.ds.file_gbx.resolve_file_table",
            return_value=resolved_df,
        ) as rft,
    ):
        out = file_gbx.gbx_file_read(
            mock_spark, "cat.sch.tbl", source_type="table", access="managed"
        )

    rft.assert_called_once_with(mock_spark, "cat.sch.tbl", skip_ordering=False)
    resolved_df.select.assert_called_once()
    assert out is selected_df


# ---------------------------------------------------------------------------
# access gating: auto on FUSE tier never raises
# ---------------------------------------------------------------------------


def test_access_auto_on_fuse_tier_does_not_raise():
    """access='auto' on fuse tier never raises; normalizes FUSE list to DataFrame."""
    mock_spark = MagicMock()
    fuse_list = [{"path": "/Volumes/c/s/v/a.tif", "size": 100, "file": None}]
    fake_df = MagicMock()
    mock_spark.createDataFrame.return_value = fake_df

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"),
        patch(
            "databricks.labs.gbx.ds.file_gbx.enumerate_files", return_value=fuse_list
        ),
    ):
        # Must not raise
        out = file_gbx.gbx_file_read(
            mock_spark, "/Volumes/c/s/v", source_type="location", access="auto"
        )

    mock_spark.createDataFrame.assert_called_once()
    assert out is fake_df


# ---------------------------------------------------------------------------
# table mode (default access="auto"): delegates to read_file_table
# ---------------------------------------------------------------------------


def test_table_mode_delegates_to_resolve_file_table():
    """table mode (default access='auto') delegates to resolve_file_table → [path,size,file]."""
    mock_spark = MagicMock()
    resolved_df = MagicMock()
    selected_df = MagicMock()
    resolved_df.select.return_value = selected_df

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"),
        patch(
            "databricks.labs.gbx.ds.file_gbx.resolve_file_table",
            return_value=resolved_df,
        ) as rft,
    ):
        out = file_gbx.gbx_file_read(mock_spark, "cat.sch.tbl", source_type="table")

    rft.assert_called_once_with(mock_spark, "cat.sch.tbl", skip_ordering=False)
    resolved_df.select.assert_called_once()
    # No content column — only the [path, size, file] contract.
    assert out is selected_df
