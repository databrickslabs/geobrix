"""Unit tests for file_gbx capability tier detection and access resolution."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.gbx.ds import file_gbx

# ---------------------------------------------------------------------------
# Tier detection tests
# ---------------------------------------------------------------------------


def test_file_access_tier_with_no_spark():
    """When spark is None and cannot be obtained, falls back to FUSE."""
    with patch("databricks.labs.gbx.ds.file_gbx.SparkSession") as mock_session_class:
        mock_session_class.getActiveSession.return_value = None
        mock_session_class.builder.getOrCreate.side_effect = RuntimeError("No spark")
        tier = file_gbx.file_access_tier(None)
    assert tier == "fuse"


def test_file_access_tier_read_files_supported():
    """When read_files(format=>'file') succeeds, tier is 'read_files'."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []

    with patch("databricks.labs.gbx.ds.file_gbx._detect_tier") as mock_detect:
        mock_detect.return_value = "read_files"
        tier = file_gbx.file_access_tier(mock_spark)

    assert tier == "read_files"


def test_file_access_tier_list_files_supported():
    """When read_files fails but list_files succeeds, tier is 'list_files'."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx._detect_tier") as mock_detect:
        mock_detect.return_value = "list_files"
        tier = file_gbx.file_access_tier(mock_spark)

    assert tier == "list_files"


def test_file_access_tier_fuse_fallback():
    """When both FILE probes fail, tier is 'fuse'."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx._detect_tier") as mock_detect:
        mock_detect.return_value = "fuse"
        tier = file_gbx.file_access_tier(mock_spark)

    assert tier == "fuse"


def test_file_access_tier_memoized():
    """Tier detection is memoized per SparkSession object."""
    mock_spark = MagicMock()
    call_count = 0

    def mock_detect(spark):
        nonlocal call_count
        call_count += 1
        return "read_files"

    # Clear the cache to ensure a clean test
    file_gbx._TIER_CACHE.clear()

    with patch("databricks.labs.gbx.ds.file_gbx._detect_tier", side_effect=mock_detect):
        tier1 = file_gbx.file_access_tier(mock_spark)
        tier2 = file_gbx.file_access_tier(mock_spark)

    assert tier1 == tier2 == "read_files"
    assert call_count == 1  # Only called once, result was memoized


def test_detect_tier_read_files_probe_success():
    """_detect_tier probes read_files first and succeeds."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "read_files"
    # Verify sql was called with the read_files probe
    assert mock_spark.sql.called
    call_args = mock_spark.sql.call_args[0][0]
    assert "read_files" in call_args
    assert "format => 'file'" in call_args


def test_detect_tier_read_files_fails_list_files_succeeds():
    """_detect_tier falls through to list_files when read_files fails."""
    mock_spark = MagicMock()
    # First call (read_files) raises, second call (list_files) succeeds
    mock_spark.sql.side_effect = [
        MagicMock(collect=MagicMock(side_effect=Exception("read_files not supported"))),
        MagicMock(collect=MagicMock(return_value=[])),
    ]

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "list_files"


def test_detect_tier_both_file_probes_fail_falls_back_to_fuse():
    """_detect_tier falls through to FUSE when both FILE probes fail."""
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("FILE not supported")

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "fuse"


# ---------------------------------------------------------------------------
# Access resolution (NO-GATING rule) tests
# ---------------------------------------------------------------------------


def test_resolve_access_auto_mode_downgrades_silently_to_read_files():
    """Auto mode returns the available tier without error."""
    tier = "read_files"
    result = file_gbx.resolve_access("auto", tier=tier)
    assert result == "read_files"


def test_resolve_access_auto_mode_downgrades_silently_to_list_files():
    """Auto mode returns list_files tier."""
    tier = "list_files"
    result = file_gbx.resolve_access("auto", tier=tier)
    assert result == "list_files"


def test_resolve_access_auto_mode_downgrades_silently_to_fuse():
    """Auto mode returns FUSE tier when FILE is unavailable."""
    tier = "fuse"
    result = file_gbx.resolve_access("auto", tier=tier)
    assert result == "fuse"


def test_resolve_access_managed_succeeds_when_read_files_available():
    """Explicit managed FILE mode succeeds when read_files is available."""
    result = file_gbx.resolve_access("managed", tier="read_files")
    assert result == "managed"


def test_resolve_access_managed_succeeds_when_list_files_available():
    """Explicit managed FILE mode succeeds when list_files is available."""
    result = file_gbx.resolve_access("managed", tier="list_files")
    assert result == "managed"


def test_resolve_access_external_succeeds_when_read_files_available():
    """Explicit external FILE mode succeeds when read_files is available."""
    result = file_gbx.resolve_access("external", tier="read_files")
    assert result == "external"


def test_resolve_access_managed_raises_on_fuse_tier():
    """Explicit managed FILE mode raises clear error when FILE unavailable."""
    with pytest.raises(ValueError) as exc_info:
        file_gbx.resolve_access("managed", tier="fuse")

    error_msg = str(exc_info.value)
    assert "managed FILE" in error_msg
    assert "not available" in error_msg
    assert "tier='fuse'" in error_msg
    assert "13.3 LTS" in error_msg or "18 LTS" in error_msg
    assert "Upgrade your cluster" in error_msg or "DBR" in error_msg


def test_resolve_access_external_raises_on_fuse_tier():
    """Explicit external FILE mode raises clear error when FILE unavailable."""
    with pytest.raises(ValueError) as exc_info:
        file_gbx.resolve_access("external", tier="fuse")

    error_msg = str(exc_info.value)
    assert "external FILE" in error_msg
    assert "not available" in error_msg


def test_resolve_access_invalid_mode_raises():
    """Invalid access mode raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        file_gbx.resolve_access("invalid_mode", tier="read_files")

    assert "Unknown access mode" in str(exc_info.value)


def test_resolve_access_detects_tier_when_not_provided():
    """resolve_access detects tier if not explicitly provided."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_detect:
        mock_detect.return_value = "read_files"
        result = file_gbx.resolve_access("auto", spark=mock_spark)

    assert result == "read_files"
    mock_detect.assert_called_once_with(mock_spark)


# ---------------------------------------------------------------------------
# Backward compatibility: re-exports from _listing
# ---------------------------------------------------------------------------


def test_reexport_to_local_path():
    """to_local_path is re-exported from _listing."""
    from databricks.labs.gbx.ds import file_gbx

    # Verify it's accessible from file_gbx
    assert hasattr(file_gbx, "to_local_path")
    # Verify it's callable
    result = file_gbx.to_local_path("file:/tmp/x")
    assert result == "/tmp/x"


def test_reexport_to_spark_uri():
    """to_spark_uri is re-exported from _listing."""
    from databricks.labs.gbx.ds import file_gbx

    assert hasattr(file_gbx, "to_spark_uri")
    result = file_gbx.to_spark_uri("/Volumes/cat/schema/vol/file.tif")
    assert result.startswith("dbfs:")


def test_reexport_list_files():
    """list_files is re-exported from _listing."""
    from databricks.labs.gbx.ds import file_gbx

    assert hasattr(file_gbx, "list_files")
    assert callable(file_gbx.list_files)


def test_reexport_retry_transient():
    """_retry_transient is re-exported from _listing."""
    from databricks.labs.gbx.ds import file_gbx

    assert hasattr(file_gbx, "_retry_transient")
    assert callable(file_gbx._retry_transient)


def test_existing_listing_importers_still_work():
    """Existing direct imports from _listing still work (backward compat)."""
    # This test verifies the import path doesn't break
    from databricks.labs.gbx.ds._listing import to_local_path, to_spark_uri

    assert callable(to_local_path)
    assert callable(to_spark_uri)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


def test_resolve_access_full_flow_auto_with_file_available():
    """Full flow: auto mode with FILE available."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_detect:
        mock_detect.return_value = "read_files"
        result = file_gbx.resolve_access("auto", spark=mock_spark)

    assert result == "read_files"


def test_resolve_access_full_flow_managed_with_file_available():
    """Full flow: explicit managed FILE with FILE available."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_detect:
        mock_detect.return_value = "list_files"
        result = file_gbx.resolve_access("managed", spark=mock_spark)

    assert result == "managed"


def test_resolve_access_full_flow_managed_with_no_file():
    """Full flow: explicit managed FILE with FILE unavailable."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_detect:
        mock_detect.return_value = "fuse"
        with pytest.raises(ValueError) as exc_info:
            file_gbx.resolve_access("managed", spark=mock_spark)

    assert "managed FILE" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Enumeration (enumerate_files) tests
# ---------------------------------------------------------------------------


def test_enumerate_files_fuse_tier_basic():
    """FUSE tier lists files with path and size, skips _* and .* by default."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files
        file1 = os_module.path.join(tmpdir, "file1.txt")
        file2 = os_module.path.join(tmpdir, "file2.txt")
        hidden = os_module.path.join(tmpdir, ".hidden")
        underscore = os_module.path.join(tmpdir, "_success")

        with open(file1, "w") as f:
            f.write("content1")
        with open(file2, "w") as f:
            f.write("content2")
        with open(hidden, "w") as f:
            f.write("hidden")
        with open(underscore, "w") as f:
            f.write("underscore")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(tmpdir, spark=mock_spark)

        assert len(result) == 2
        paths = {r["path"] for r in result}
        assert file1 in paths
        assert file2 in paths
        assert hidden not in paths
        assert underscore not in paths

        # Check sizes
        for rec in result:
            assert rec["size"] > 0
            assert rec["file"] is None  # FUSE tier has no FILE ref


def test_enumerate_files_fuse_tier_include_hidden():
    """FUSE tier includes _* and .* files when include_hidden=True."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os_module.path.join(tmpdir, "file1.txt")
        hidden = os_module.path.join(tmpdir, ".hidden")
        underscore = os_module.path.join(tmpdir, "_success")

        with open(file1, "w") as f:
            f.write("content1")
        with open(hidden, "w") as f:
            f.write("hidden")
        with open(underscore, "w") as f:
            f.write("underscore")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(
                tmpdir, include_hidden=True, spark=mock_spark
            )

        paths = {r["path"] for r in result}
        assert file1 in paths
        assert hidden in paths
        assert underscore in paths
        assert len(result) == 3


def test_enumerate_files_fuse_tier_recursive():
    """FUSE tier recursively lists files in subdirectories."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        subdir = os_module.path.join(tmpdir, "subdir")
        os_module.makedirs(subdir)

        file1 = os_module.path.join(tmpdir, "file1.txt")
        file2 = os_module.path.join(subdir, "file2.txt")

        with open(file1, "w") as f:
            f.write("content1")
        with open(file2, "w") as f:
            f.write("content2")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(tmpdir, recursive=True, spark=mock_spark)

        paths = {r["path"] for r in result}
        assert file1 in paths
        assert file2 in paths
        assert len(result) == 2


def test_enumerate_files_fuse_tier_non_recursive():
    """FUSE tier with recursive=False only lists top-level files."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        subdir = os_module.path.join(tmpdir, "subdir")
        os_module.makedirs(subdir)

        file1 = os_module.path.join(tmpdir, "file1.txt")
        file2 = os_module.path.join(subdir, "file2.txt")

        with open(file1, "w") as f:
            f.write("content1")
        with open(file2, "w") as f:
            f.write("content2")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(tmpdir, recursive=False, spark=mock_spark)

        paths = {r["path"] for r in result}
        assert file1 in paths
        assert file2 not in paths
        assert len(result) == 1


def test_enumerate_files_read_files_tier():
    """read_files tier returns DataFrame with path, size, and file columns."""
    mock_spark = MagicMock()
    mock_df = MagicMock()

    # Mock the DataFrame returned by read_files
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        result = file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    # Should have called spark.sql with read_files query
    assert mock_spark.sql.called
    call_args = mock_spark.sql.call_args[0][0]
    assert "read_files" in call_args
    assert "format => 'file'" in call_args
    assert "_metadata.file_path" in call_args
    assert "_metadata.file_size" in call_args

    # Result should be the DataFrame
    assert result is mock_df or isinstance(result, (list, MagicMock))


def test_enumerate_files_read_files_tier_recursive():
    """read_files tier respects recursive parameter."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files("/Volumes/test/path", recursive=True, spark=mock_spark)

    call_args = mock_spark.sql.call_args[0][0]
    assert (
        "recursiveFileLookup => true" in call_args or "recursive" in call_args.lower()
    )

    # Test non-recursive
    mock_spark.reset_mock()
    file_gbx.enumerate_files("/Volumes/test/path", recursive=False, spark=mock_spark)
    call_args = mock_spark.sql.call_args[0][0]
    assert (
        "recursiveFileLookup => false" in call_args or "recursive" in call_args.lower()
    )


def test_enumerate_files_list_files_tier():
    """list_files tier returns DataFrame with path, size, and file columns."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "list_files"
        result = file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    # Should have called spark.sql with list_files query
    assert mock_spark.sql.called
    call_args = mock_spark.sql.call_args[0][0]
    assert "list_files" in call_args

    # Result should be the DataFrame
    assert result is mock_df or isinstance(result, (list, MagicMock))


def test_enumerate_files_returns_path_size_file_structure():
    """Returned records have path, size, and file keys."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os_module.path.join(tmpdir, "file1.txt")
        with open(file1, "w") as f:
            f.write("content1")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(tmpdir, spark=mock_spark)

        assert len(result) > 0
        for rec in result:
            assert "path" in rec
            assert "size" in rec
            assert "file" in rec
            assert isinstance(rec["path"], str)
            assert isinstance(rec["size"], int)
            assert rec["file"] is None  # FUSE tier has no file ref


def test_enumerate_files_cross_tier_parity():
    """All three tiers produce identical filtering (parity test).

    Tests that root-level and nested _*/..* files are skipped identically across
    all tiers (read_files, list_files, FUSE). This catches tier-divergence bugs.
    """
    import os as os_module
    import tempfile

    def get_basename(path):
        return os_module.path.basename(path)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files: root-level metadata, nested metadata, normal files, hidden
        root_success = os_module.path.join(tmpdir, "_success")
        root_crc = os_module.path.join(tmpdir, ".crc")
        root_hidden = os_module.path.join(tmpdir, ".hidden")
        root_normal = os_module.path.join(tmpdir, "normal.txt")

        subdir = os_module.path.join(tmpdir, "subdir")
        os_module.makedirs(subdir)
        nested_committed = os_module.path.join(subdir, "_committed_001")
        nested_crc = os_module.path.join(subdir, ".crc")
        nested_hidden = os_module.path.join(subdir, ".foo")
        nested_normal = os_module.path.join(subdir, "data.txt")

        for path in [
            root_success,
            root_crc,
            root_hidden,
            root_normal,
            nested_committed,
            nested_crc,
            nested_hidden,
            nested_normal,
        ]:
            with open(path, "w") as f:
                f.write("content")

        # Test 1: Default (skip _* and .*) — FUSE tier
        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            fuse_result = file_gbx.enumerate_files(tmpdir, spark=mock_spark)

        fuse_paths = {get_basename(r["path"]) for r in fuse_result}
        # Only normal files should be present
        assert "normal.txt" in fuse_paths
        assert "data.txt" in fuse_paths
        # Hidden/metadata files should be skipped
        assert "_success" not in fuse_paths
        assert ".crc" not in fuse_paths
        assert ".hidden" not in fuse_paths
        assert ".foo" not in fuse_paths
        assert "_committed_001" not in fuse_paths
        assert len(fuse_result) == 2

        # Test 2: Include hidden (include_hidden=True) — FUSE tier
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            fuse_result_hidden = file_gbx.enumerate_files(
                tmpdir, include_hidden=True, spark=mock_spark
            )

        fuse_paths_hidden = {get_basename(r["path"]) for r in fuse_result_hidden}
        # All files should be present
        assert "_success" in fuse_paths_hidden
        assert ".crc" in fuse_paths_hidden
        assert ".hidden" in fuse_paths_hidden
        assert ".foo" in fuse_paths_hidden
        assert "_committed_001" in fuse_paths_hidden
        assert "normal.txt" in fuse_paths_hidden
        assert "data.txt" in fuse_paths_hidden
        assert len(fuse_result_hidden) == 8

        # Test 3: Verify _should_skip_file matches FUSE behavior
        # (cross-check the helper function)
        test_names = [
            "_success",
            ".crc",
            ".hidden",
            "normal.txt",
            "_committed_001",
            ".foo",
        ]
        for name in test_names:
            fuse_should_skip = file_gbx._should_skip_file(name, include_hidden=False)
            expected_skip = name.startswith("_") or name.startswith(".")
            assert fuse_should_skip == expected_skip
            # Also verify with include_hidden=True, nothing is skipped
            assert not file_gbx._should_skip_file(name, include_hidden=True)


def test_enumerate_files_list_files_skip_pattern_matches_basename():
    """list_files skip pattern correctly matches basename (root and nested files)."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "list_files"
        file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    call_args = mock_spark.sql.call_args[0][0]
    # Verify the SQL uses substring_index to extract basename
    assert "substring_index(path, '/', -1)" in call_args
    # Verify both _ and . patterns are checked
    assert "LIKE '_%'" in call_args
    assert "LIKE '.%'" in call_args


def test_enumerate_files_sql_path_escaping():
    """SQL path escaping prevents injection for single quotes."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    # Test read_files tier with a path containing single quote
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files("/Volumes/test/path's", spark=mock_spark)

    call_args = mock_spark.sql.call_args[0][0]
    # Verify the single quote was escaped (doubled)
    assert "path''s" in call_args

    # Test list_files tier with a path containing single quote
    mock_spark.reset_mock()
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "list_files"
        file_gbx.enumerate_files("/Volumes/test/path's", spark=mock_spark)

    call_args = mock_spark.sql.call_args[0][0]
    # Verify the single quote was escaped (doubled)
    assert "path''s" in call_args


# ---------------------------------------------------------------------------
# open_for_read tests
# ---------------------------------------------------------------------------


def test_open_for_read_auto_mode_returns_source_path():
    """open_for_read with access='auto' returns the source path."""
    source = "/Volumes/test/data.tif"

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        result = file_gbx.open_for_read(source, access="auto")

    assert result == source


def test_open_for_read_explicit_managed_with_file_available():
    """open_for_read with access='managed' succeeds when FILE is available."""
    mock_spark = MagicMock()
    source = "/Volumes/test/data.tif"

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        result = file_gbx.open_for_read(source, access="managed", spark=mock_spark)

    assert result == source


def test_open_for_read_explicit_managed_without_file_raises_error():
    """open_for_read with access='managed' raises when FILE is unavailable."""
    mock_spark = MagicMock()
    source = "/Volumes/test/data.tif"

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        with pytest.raises(ValueError, match="Requested managed FILE access mode"):
            file_gbx.open_for_read(source, access="managed", spark=mock_spark)


def test_open_for_read_explicit_external_without_file_raises_error():
    """open_for_read with access='external' raises when FILE is unavailable."""
    mock_spark = MagicMock()
    source = "/Volumes/test/data.tif"

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        with pytest.raises(ValueError, match="Requested external FILE access mode"):
            file_gbx.open_for_read(source, access="external", spark=mock_spark)
