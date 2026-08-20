"""Task 2: generic gbx_file_read (location + table modes)."""

from unittest.mock import MagicMock, patch

from databricks.labs.gbx.ds import file_gbx


def test_source_type_auto_classifies_path_vs_table():
    assert file_gbx._classify_source("/Volumes/c/s/v/x") == "location"
    assert file_gbx._classify_source("dbfs:/Volumes/c/s/v") == "location"
    assert file_gbx._classify_source("s3://bucket/key") == "location"
    assert file_gbx._classify_source("catalog.schema.table") == "table"


def test_location_fuse_tier_reads_via_binaryfile(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"hello")
    (tmp_path / "b.bin").write_bytes(b"world")
    mock_spark = MagicMock()
    reader = mock_spark.read.format.return_value.load.return_value
    reader.selectExpr.return_value = "BINARYFILE_DF"
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"):
        out = file_gbx.gbx_file_read(mock_spark, str(tmp_path), source_type="location")
    mock_spark.read.format.assert_called_with("binaryFile")
    # Verify option kwargs are forwarded to load() — exercises the recursive/hidden logic.
    mock_spark.read.format.return_value.load.assert_called_once_with(
        str(tmp_path),
        recursiveFileLookup="true",
        pathGlobFilter="[!._]*",
    )
    assert out == "BINARYFILE_DF"


def test_location_list_files_tier_falls_back_to_binaryfile(tmp_path):
    """list_files tier (DBR-18 metadata-only) must not use read_files SQL; use binaryFile."""
    mock_spark = MagicMock()
    reader = mock_spark.read.format.return_value.load.return_value
    reader.selectExpr.return_value = "LISTFILES_BF_DF"
    with patch(
        "databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="list_files"
    ):
        out = file_gbx.gbx_file_read(mock_spark, str(tmp_path), source_type="location")
    mock_spark.read.format.assert_called_with("binaryFile")
    mock_spark.sql.assert_not_called()
    assert out == "LISTFILES_BF_DF"


def test_location_file_tier_uses_read_files_content():
    mock_spark = MagicMock()
    mock_spark.sql.return_value = "READFILES_DF"
    with patch(
        "databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="read_files"
    ):
        out = file_gbx.gbx_file_read(
            mock_spark, "/Volumes/c/s/v", source_type="location"
        )
    sql = mock_spark.sql.call_args[0][0]
    assert "read_files" in sql and "format => 'file'" in sql
    assert "content" in sql
    assert out == "READFILES_DF"


def test_table_mode_delegates_to_read_file_table():
    mock_spark = MagicMock()
    tile_df = MagicMock()
    tile_df.selectExpr.return_value = "PATH_DF"
    with patch(
        "databricks.labs.gbx.pyrx.file_table.read_file_table", return_value=tile_df
    ) as rft:
        out = file_gbx.gbx_file_read(mock_spark, "cat.sch.tbl", source_type="table")
    rft.assert_called_once_with(mock_spark, "cat.sch.tbl")
    assert out == "PATH_DF"
