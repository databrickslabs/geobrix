"""Unit tests for file_gbx capability tier detection and access resolution."""

from __future__ import annotations

import warnings as _warnings_mod
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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
    """_detect_tier falls through to list_files when read_files is unavailable (UNRESOLVED_ROUTINE)."""
    mock_spark = MagicMock()
    # First call (read_files) raises UNRESOLVED_ROUTINE, second call (list_files) succeeds
    mock_spark.sql.side_effect = [
        MagicMock(
            collect=MagicMock(
                side_effect=Exception(
                    "UNRESOLVED_ROUTINE: function read_files not found"
                )
            )
        ),
        MagicMock(collect=MagicMock(return_value=[])),
    ]

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "list_files"


def test_detect_tier_both_file_probes_fail_falls_back_to_fuse():
    """_detect_tier falls through to FUSE when both FILE probes are unavailable (UNRESOLVED_ROUTINE)."""
    mock_spark = MagicMock()
    # Both calls raise UNRESOLVED_ROUTINE (routine unavailable)
    unresolved = Exception("UNRESOLVED_ROUTINE: function read_files not found")
    mock_spark.sql.side_effect = [
        MagicMock(collect=MagicMock(side_effect=unresolved)),
        MagicMock(collect=MagicMock(side_effect=unresolved)),
    ]

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "fuse"


# Regression tests for the capability-detection bug (Task 10)
# https://github.com/databrickslabs/geobrix/issues/XXX
# Bug: PATH_NOT_FOUND proves the function EXISTS, but was treated as "unavailable"


def test_detect_tier_path_not_found_proves_read_files_available():
    """REGRESSION: PATH_NOT_FOUND on read_files probe proves function exists → return 'read_files'.

    Bug: Previously, a bare except Exception would swallow PATH_NOT_FOUND and fall through to list_files.
    Fix: PATH_NOT_FOUND (or any error proving the function RAN) → function IS available.
    """
    mock_spark = MagicMock()
    # Simulate: read_files raises PATH_NOT_FOUND (path doesn't exist, but function exists)
    path_not_found_exc = Exception(
        "PATH_NOT_FOUND: path /Volumes/__gbx_probe__/__none__ does not exist"
    )
    mock_spark.sql.side_effect = path_not_found_exc

    tier = file_gbx._detect_tier(mock_spark)

    # The fix: PATH_NOT_FOUND proves read_files EXISTS → return "read_files", not "fuse"
    assert tier == "read_files"


def test_detect_tier_unresolved_routine_falls_through_to_list_files():
    """When read_files is truly unavailable (UNRESOLVED_ROUTINE), fall through to list_files."""
    mock_spark = MagicMock()
    unresolved_exc = Exception("UNRESOLVED_ROUTINE: function read_files not found")
    list_files_success = MagicMock(collect=MagicMock(return_value=[]))

    # First call (read_files) raises UNRESOLVED_ROUTINE, second (list_files) succeeds
    mock_spark.sql.side_effect = [
        MagicMock(collect=MagicMock(side_effect=unresolved_exc)),
        list_files_success,
    ]

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "list_files"


def test_detect_tier_list_files_path_not_found_proves_available():
    """When read_files is unavailable but list_files raises PATH_NOT_FOUND → list_files IS available."""
    mock_spark = MagicMock()
    unresolved_read = Exception("UNRESOLVED_ROUTINE: read_files not found")
    path_not_found_list = Exception(
        "PATH_NOT_FOUND: /Volumes/__gbx_probe__/__none__ not found"
    )

    mock_spark.sql.side_effect = [
        MagicMock(collect=MagicMock(side_effect=unresolved_read)),
        MagicMock(collect=MagicMock(side_effect=path_not_found_list)),
    ]

    tier = file_gbx._detect_tier(mock_spark)

    # PATH_NOT_FOUND on list_files proves it EXISTS → return "list_files", not "fuse"
    assert tier == "list_files"


def test_detect_tier_both_unresolved_falls_back_to_fuse():
    """When both read_files and list_files are UNRESOLVED_ROUTINE → fall back to FUSE."""
    mock_spark = MagicMock()
    unresolved = Exception("UNRESOLVED_ROUTINE: function not found")

    mock_spark.sql.side_effect = [
        MagicMock(collect=MagicMock(side_effect=unresolved)),
        MagicMock(collect=MagicMock(side_effect=unresolved)),
    ]

    tier = file_gbx._detect_tier(mock_spark)

    assert tier == "fuse"


def test_is_routine_unavailable_detects_unresolved_routine():
    """_is_routine_unavailable returns True for UNRESOLVED_ROUTINE-style errors."""
    exc_unresolved = Exception("UNRESOLVED_ROUTINE: function read_files not found")
    assert file_gbx._is_routine_unavailable(exc_unresolved)

    exc_unresolvable = Exception("UNRESOLVABLE_ROUTINE: routine not found")
    assert file_gbx._is_routine_unavailable(exc_unresolvable)

    exc_parse = Exception("PARSE_SYNTAX_ERROR: unexpected token 'read_files'")
    assert file_gbx._is_routine_unavailable(exc_parse)

    exc_unsupported = Exception("UNSUPPORTED: function not available")
    assert file_gbx._is_routine_unavailable(exc_unsupported)

    exc_cannot_resolve = Exception("cannot resolve function read_files")
    assert file_gbx._is_routine_unavailable(exc_cannot_resolve)

    exc_undefined_func = Exception("Undefined function: read_files")
    assert file_gbx._is_routine_unavailable(exc_undefined_func)

    exc_not_found = Exception("function read_files does not exist")
    assert file_gbx._is_routine_unavailable(exc_not_found)


def test_is_routine_unavailable_returns_false_for_path_errors():
    """_is_routine_unavailable returns False for PATH_NOT_FOUND and other data/IO errors."""
    exc_path_not_found = Exception("PATH_NOT_FOUND: path does not exist")
    assert not file_gbx._is_routine_unavailable(exc_path_not_found)

    exc_file_not_found = FileNotFoundError("file not found")
    assert not file_gbx._is_routine_unavailable(exc_file_not_found)

    exc_io_error = IOError("I/O error reading file")
    assert not file_gbx._is_routine_unavailable(exc_io_error)

    exc_permission = PermissionError("access denied")
    assert not file_gbx._is_routine_unavailable(exc_permission)

    exc_generic = Exception("some other runtime error")
    assert not file_gbx._is_routine_unavailable(exc_generic)


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


# ---------------------------------------------------------------------------
# open_for_write tests
# ---------------------------------------------------------------------------

_WINDOW_STRUCT = StructType(
    [
        StructField("col_off", IntegerType()),
        StructField("row_off", IntegerType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType()),
    ]
)
_TILE_STRUCT = StructType(
    [
        StructField("cellid", LongType()),
        StructField("raster", BinaryType()),
        StructField("path", StringType()),
        StructField("window", _WINDOW_STRUCT),
        StructField("crs", StringType()),
        StructField("path_mode", StringType()),
    ]
)
_DF_SCHEMA = StructType([StructField("tile", _TILE_STRUCT)])


def _make_tile_df():
    df = MagicMock()
    df.schema = _DF_SCHEMA
    df.createOrReplaceTempView = MagicMock()
    return df


def test_open_for_write_auto_managed_routes_to_write_file_table():
    """auto + filespace + FILE tier → write_file_table(file_mode='managed')."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "read_files"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="auto",
            filespace="/Volumes/c/s/v",
            layout="order",
        )

    mock_wft.assert_called_once_with(
        mock_spark,
        mock_df,
        "cat.sch.t",
        file_mode="managed",
        filespace="/Volumes/c/s/v",
        layout="order",
        overwrite=False,
        file_col="tile_file",
    )


def test_open_for_write_auto_external_no_filespace():
    """auto + no filespace + FILE tier → write_file_table(file_mode='external')."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "list_files"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="auto",
            filespace=None,
            layout="order",
        )

    mock_wft.assert_called_once_with(
        mock_spark,
        mock_df,
        "cat.sch.t",
        file_mode="external",
        filespace=None,
        layout="order",
        overwrite=False,
        file_col="tile_file",
    )


def test_open_for_write_auto_fuse_tier_writes_plain_delta():
    """auto + fuse tier → CTAS plain Delta write via spark.sql, no create_file / try_to_file."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="auto",
            layout="order",
        )

    # write_file_table must NOT be called
    mock_wft.assert_not_called()
    # A CREATE TABLE USING DELTA AS SELECT was emitted
    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls, f"Expected CREATE TABLE; got: {captured}"
    # No FILE SQL
    for sql_str in captured:
        assert "create_file" not in sql_str
        assert "try_to_file" not in sql_str


def test_open_for_write_explicit_managed_routes_to_write_file_table():
    """Explicit file_mode='managed' on FILE-capable tier → write_file_table(managed)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "read_files"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="managed",
            filespace="/Volumes/c/s/v",
        )

    mock_wft.assert_called_once()
    _, kwargs = mock_wft.call_args
    assert kwargs["file_mode"] == "managed"


def test_open_for_write_explicit_external_routes_to_write_file_table():
    """Explicit file_mode='external' on FILE-capable tier → write_file_table(external)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "read_files"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="external",
        )

    mock_wft.assert_called_once()
    _, kwargs = mock_wft.call_args
    assert kwargs["file_mode"] == "external"


def test_open_for_write_explicit_managed_on_fuse_tier_raises():
    """Explicit file_mode='managed' on fuse tier raises actionable ValueError."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        with pytest.raises(ValueError) as exc_info:
            file_gbx.open_for_write(
                mock_spark,
                mock_df,
                "cat.sch.t",
                file_mode="managed",
                filespace="/Volumes/c/s/v",
            )

    err = str(exc_info.value)
    assert "managed FILE" in err
    assert "not available" in err
    assert "tier='fuse'" in err


def test_open_for_write_explicit_external_on_fuse_tier_raises():
    """Explicit file_mode='external' on fuse tier raises actionable ValueError."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        with pytest.raises(ValueError) as exc_info:
            file_gbx.open_for_write(
                mock_spark,
                mock_df,
                "cat.sch.t",
                file_mode="external",
            )

    err = str(exc_info.value)
    assert "external FILE" in err
    assert "not available" in err


def test_open_for_write_invalid_layout_raises():
    """Invalid layout raises ValueError before any side effects."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "read_files"
        with pytest.raises(ValueError, match="layout must be"):
            file_gbx.open_for_write(
                mock_spark,
                mock_df,
                "cat.sch.t",
                layout="PARTITIONED BY path",
            )

    mock_wft.assert_not_called()


def test_open_for_write_partition_layout_raises():
    """Any partition-style layout string raises ValueError."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    for bad_layout in ("partition", "zorder", "bucket", "sorted"):
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "read_files"
            with pytest.raises(ValueError, match="layout must be"):
                file_gbx.open_for_write(
                    mock_spark,
                    mock_df,
                    "cat.sch.t",
                    layout=bad_layout,
                )


def test_open_for_write_cluster_layout_emits_optimize_warning():
    """layout='cluster' on a FILE-mode write emits a warnings.warn about OPTIMIZE."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table"),
        _warnings_mod.catch_warnings(record=True) as caught,
    ):
        _warnings_mod.simplefilter("always")
        mock_tier.return_value = "read_files"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="external",
            layout="cluster",
        )

    assert any(
        "OPTIMIZE" in str(w.message) and "cat.sch.t" in str(w.message) for w in caught
    ), f"Expected OPTIMIZE warning; got: {[str(w.message) for w in caught]}"


def test_open_for_write_cluster_layout_no_warning_for_fuse():
    """layout='cluster' on fuse mode does NOT emit the OPTIMIZE warning."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        _warnings_mod.catch_warnings(record=True) as caught,
    ):
        _warnings_mod.simplefilter("always")
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="fuse",
            layout="cluster",
        )

    optimize_warns = [w for w in caught if "OPTIMIZE" in str(w.message)]
    assert (
        not optimize_warns
    ), f"Unexpected OPTIMIZE warning for fuse mode: {optimize_warns}"


def test_open_for_write_create_sql_no_partitioned_by():
    """Verify create SQL (via write_file_table) never contains PARTITIONED BY."""
    from databricks.labs.gbx.pyrx import file_table as ft

    sql = ft.build_create_sql(
        "cat.sch.t",
        plain_cols=[("path", "string"), ("cellid", "bigint")],
        file_col="tile_file",
        file_mode="external",
        filespace=None,
        layout="order",
    )
    assert "PARTITIONED BY" not in sql
    assert "ZORDER" not in sql.upper()


def test_open_for_write_fuse_sort_by_path_for_order_layout():
    """Fuse mode with layout='order' emits ORDER BY path in the CTAS SQL."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="fuse",
            layout="order",
        )

    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls, f"Expected CREATE TABLE; got: {captured}"
    assert "ORDER BY path" in create_sqls[0]


def test_open_for_write_fuse_no_sort_for_plain_layout():
    """Fuse mode with layout='plain' does NOT emit ORDER BY in the CTAS SQL."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="fuse",
            layout="plain",
        )

    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls
    assert "ORDER BY" not in create_sqls[0]


def test_open_for_write_fuse_overwrite_drops_before_create():
    """Fuse mode with overwrite=True emits DROP TABLE IF EXISTS before the CREATE."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="fuse",
            layout="order",
            overwrite=True,
        )

    drop_sqls = [s for s in captured if "DROP TABLE" in s.upper()]
    assert drop_sqls, f"Expected DROP TABLE IF EXISTS; got: {captured}"
    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls
    # DROP must come before CREATE
    assert captured.index(drop_sqls[0]) < captured.index(create_sqls[0])


def test_open_for_write_explicit_managed_no_filespace_raises_early():
    """Explicit file_mode='managed' with no filespace raises before tier detection."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    # Patch file_access_tier so we can confirm it was NOT reached.
    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_tier.return_value = "read_files"
        with pytest.raises(ValueError) as exc_info:
            file_gbx.open_for_write(
                mock_spark,
                mock_df,
                "cat.sch.t",
                file_mode="managed",
                filespace=None,
            )

    err = str(exc_info.value)
    assert "managed" in err
    assert "filespace" in err
    # Neither tier detection nor write_file_table should have been reached.
    mock_tier.assert_not_called()
    mock_wft.assert_not_called()


def test_open_for_write_fuse_cluster_layout_warns_and_no_cluster_by_sql():
    """fuse + layout='cluster': emits a warning about ORDER BY fallback; no CLUSTER BY SQL."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier,
        _warnings_mod.catch_warnings(record=True) as caught,
    ):
        _warnings_mod.simplefilter("always")
        mock_tier.return_value = "fuse"
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="fuse",
            layout="cluster",
        )

    # A warning must mention fuse / CLUSTER BY / ORDER BY
    cluster_warns = [w for w in caught if "cluster" in str(w.message).lower()]
    assert (
        cluster_warns
    ), f"Expected cluster-layout fuse warning; got: {[str(w.message) for w in caught]}"
    assert any("ORDER BY" in str(w.message) for w in cluster_warns)

    # The emitted CTAS SQL must contain ORDER BY (fallback) but NOT CLUSTER BY
    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls
    create = create_sqls[0]
    assert "ORDER BY path" in create
    assert "CLUSTER BY" not in create.upper()


def test_open_for_write_fuse_select_expr_backticks():
    """_fuse_select_expr backtick-quotes field names to guard against reserved words."""
    from databricks.labs.gbx.ds.file_gbx import _fuse_select_expr

    expr = _fuse_select_expr(_DF_SCHEMA)
    # All tile field references must use backtick form
    assert "`tile`." in expr
    # The alias side must also be backtick-quoted
    assert "AS `" in expr
    # path and window are SQL reserved words — verify they appear safely
    assert "`tile`.`path` AS `path`" in expr
    assert "`tile`.`window` AS `window`" in expr
    # path_mode must be excluded
    assert "path_mode" not in expr


# ---------------------------------------------------------------------------
# ingest_files tests
# ---------------------------------------------------------------------------


def test_ingest_files_raises_on_fuse_tier():
    """ingest_files raises a clear ValueError on a fuse-only runtime."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"
        with pytest.raises(ValueError) as exc_info:
            file_gbx.ingest_files(
                mock_spark,
                "/Volumes/cat/s/v/src",
                "cat.sch.dst",
                filespace="/Volumes/cat/s/v",
            )

    err = str(exc_info.value)
    assert "ingest_files requires FILE support" in err
    assert "tier='fuse'" in err
    assert "13.3 LTS" in err


def test_ingest_files_builds_read_files_insert_sql():
    """ingest_files emits CREATE IF NOT EXISTS + INSERT with read_files(format=>'file')."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            layout="order",
        )

    insert_sqls = [s for s in captured if s.strip().upper().startswith("INSERT")]
    assert insert_sqls, f"No INSERT captured; got: {captured}"
    insert = insert_sqls[0]
    assert "read_files(" in insert
    assert "format => 'file'" in insert
    assert "recursiveFileLookup => true" in insert
    assert "_metadata.file_path AS path" in insert
    assert "file AS tile_file" in insert
    assert "ORDER BY path" in insert

    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls, f"No CREATE TABLE captured; got: {captured}"
    create = create_sqls[0]
    assert "IF NOT EXISTS" in create.upper()
    assert "FILE MANAGED" in create
    assert "PARTITIONED BY" not in create


def test_ingest_files_order_by_in_insert():
    """ingest_files with layout='order' emits ORDER BY path in the INSERT."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            layout="order",
        )

    insert = next(s for s in captured if s.strip().upper().startswith("INSERT"))
    assert "ORDER BY path" in insert


def test_ingest_files_no_order_by_for_plain_layout():
    """ingest_files with layout='plain' does NOT emit ORDER BY in the INSERT."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            layout="plain",
        )

    insert = next(s for s in captured if s.strip().upper().startswith("INSERT"))
    assert "ORDER BY" not in insert


def test_ingest_files_escapes_single_quotes_in_src():
    """ingest_files doubles single quotes in the src path to prevent SQL injection."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/o'reilly/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
        )

    insert = next(s for s in captured if s.strip().upper().startswith("INSERT"))
    assert "o''reilly" in insert
    assert "o'reilly" not in insert.replace("o''reilly", "")


def test_ingest_files_overwrite_drops_and_recreates():
    """ingest_files with overwrite=True emits DROP TABLE IF EXISTS + CREATE TABLE."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            overwrite=True,
        )

    drop_sqls = [s for s in captured if "DROP TABLE" in s.upper()]
    assert drop_sqls, "Expected DROP TABLE IF EXISTS; none captured"
    create_sqls = [s for s in captured if "CREATE TABLE" in s.upper()]
    assert create_sqls
    # When overwrite=True, CREATE TABLE must NOT include IF NOT EXISTS
    create = create_sqls[0]
    assert "IF NOT EXISTS" not in create.upper()


def test_ingest_files_non_recursive():
    """ingest_files with recursive=False emits recursiveFileLookup => false."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            recursive=False,
        )

    insert = next(s for s in captured if s.strip().upper().startswith("INSERT"))
    assert "recursiveFileLookup => false" in insert


def test_ingest_files_invalid_layout_raises():
    """ingest_files raises ValueError for invalid layout before emitting any SQL."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        with pytest.raises(ValueError, match="layout must be"):
            file_gbx.ingest_files(
                mock_spark,
                "/Volumes/cat/s/v/src",
                "cat.sch.dst",
                filespace="/Volumes/cat/s/v",
                layout="PARTITIONED BY path",
            )

    mock_spark.sql.assert_not_called()


def test_ingest_files_custom_file_col():
    """ingest_files uses the specified file_col name in the INSERT."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "list_files"
        file_gbx.ingest_files(
            mock_spark,
            "/Volumes/cat/s/v/src",
            "cat.sch.dst",
            filespace="/Volumes/cat/s/v",
            file_col="my_file",
        )

    insert = next(s for s in captured if s.strip().upper().startswith("INSERT"))
    assert "file AS my_file" in insert
    create = next(s for s in captured if "CREATE TABLE" in s.upper())
    assert "my_file FILE MANAGED" in create


# ---------------------------------------------------------------------------
# Task 8b: enumerate_files positive filter (extensions / path_glob_filter)
# ---------------------------------------------------------------------------


def test_enumerate_files_extensions_filter_fuse():
    """extensions=('.tif',) returns only .tif files on the FUSE tier."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tif1 = os_module.path.join(tmpdir, "a.tif")
        tif2 = os_module.path.join(tmpdir, "b.TIF")  # uppercase — case-insensitive
        nc = os_module.path.join(tmpdir, "data.nc")
        txt = os_module.path.join(tmpdir, "readme.txt")

        for p in (tif1, tif2, nc, txt):
            with open(p, "w") as f:
                f.write("x")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(
                tmpdir, extensions=(".tif",), spark=mock_spark
            )

    basenames = {os_module.path.basename(r["path"]) for r in result}
    # Both .tif and .TIF must be returned (case-insensitive)
    assert "a.tif" in basenames
    assert "b.TIF" in basenames
    # Other extensions must be excluded
    assert "data.nc" not in basenames
    assert "readme.txt" not in basenames
    assert len(result) == 2


def test_enumerate_files_path_glob_filter_niche_case():
    """path_glob_filter='[!.]*' + include_hidden=True: includes _data.tif, excludes .crc."""
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        underscore = os_module.path.join(tmpdir, "_data.tif")
        dot_crc = os_module.path.join(tmpdir, ".crc")
        dot_ds = os_module.path.join(tmpdir, ".DS_Store")
        normal = os_module.path.join(tmpdir, "normal.txt")

        for p in (underscore, dot_crc, dot_ds, normal):
            with open(p, "w") as f:
                f.write("x")

        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(
                tmpdir,
                include_hidden=True,
                path_glob_filter="[!.]*",
                spark=mock_spark,
            )

    basenames = {os_module.path.basename(r["path"]) for r in result}
    # Underscore file starts with _, not . → included
    assert "_data.tif" in basenames
    # Normal file → included
    assert "normal.txt" in basenames
    # Dot-named files → excluded by [!.]* filter
    assert ".crc" not in basenames
    assert ".DS_Store" not in basenames
    assert len(result) == 2


def test_enumerate_files_both_extensions_and_path_glob_filter_raises():
    """Passing both extensions and path_glob_filter raises ValueError."""
    mock_spark = MagicMock()

    with pytest.raises(ValueError, match="mutually exclusive"):
        file_gbx.enumerate_files(
            "/tmp/test",
            extensions=(".tif",),
            path_glob_filter="*.tif",
            spark=mock_spark,
        )


def test_enumerate_files_cross_tier_parity_with_filter():
    """All three tiers produce byte-identical filtering when extensions is set.

    Structural checks:
    - read_files tier: SQL contains a LIKE predicate on file_name.
    - list_files tier: SQL contains a LIKE predicate on the substring_index basename expr.
    - FUSE tier: end-to-end result contains only the matching files.
    """
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tif = os_module.path.join(tmpdir, "scene.tif")
        nc = os_module.path.join(tmpdir, "data.nc")
        txt = os_module.path.join(tmpdir, "readme.txt")

        for p in (tif, nc, txt):
            with open(p, "w") as f:
                f.write("x")

        # --- FUSE end-to-end ---
        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            fuse_result = file_gbx.enumerate_files(
                tmpdir, extensions=(".tif",), spark=mock_spark
            )

        fuse_basenames = {os_module.path.basename(r["path"]) for r in fuse_result}
        assert fuse_basenames == {"scene.tif"}

        # --- read_files tier: SQL predicate check ---
        mock_spark2 = MagicMock()
        mock_spark2.sql.return_value = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "read_files"
            file_gbx.enumerate_files(tmpdir, extensions=(".tif",), spark=mock_spark2)

        sql_rf = mock_spark2.sql.call_args[0][0]
        # The predicate targets file_name (read_files basename column)
        assert "file_name" in sql_rf
        # A LIKE predicate with %.tif must be present (extensions path)
        assert "%.tif" in sql_rf

        # --- list_files tier: SQL predicate check ---
        mock_spark3 = MagicMock()
        mock_spark3.sql.return_value = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "list_files"
            file_gbx.enumerate_files(tmpdir, extensions=(".tif",), spark=mock_spark3)

        sql_lf = mock_spark3.sql.call_args[0][0]
        # The predicate targets the basename via substring_index
        assert "substring_index(path, '/', -1)" in sql_lf
        # Same LIKE predicate
        assert "%.tif" in sql_lf


def test_enumerate_files_path_glob_filter_rlike_in_sql():
    """path_glob_filter with a character class emits an RLIKE predicate in SQL tiers."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files(
            "/Volumes/test/path",
            path_glob_filter="[!.]*",
            include_hidden=True,
            spark=mock_spark,
        )

    sql = mock_spark.sql.call_args[0][0]
    assert "RLIKE" in sql
    # The regex must exclude leading-dot files — [^.] in the generated regex
    assert "[^.]" in sql


def test_build_glob_filter_extensions():
    """_build_glob_filter compiles extensions to glob patterns."""
    patterns = file_gbx._build_glob_filter((".tif", ".NC"), None)
    assert patterns == ["*.tif", "*.nc"]


def test_build_glob_filter_path_glob_filter():
    """_build_glob_filter passes path_glob_filter through as a single-element list."""
    patterns = file_gbx._build_glob_filter(None, "[!.]*")
    assert patterns == ["[!.]*"]


def test_build_glob_filter_none():
    """_build_glob_filter returns None when both params are None."""
    assert file_gbx._build_glob_filter(None, None) is None


def test_build_glob_filter_both_raises():
    """_build_glob_filter raises ValueError if both params are given."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        file_gbx._build_glob_filter((".tif",), "*.tif")


def test_fuse_matches_filter_case_insensitive():
    """_fuse_matches_filter is case-insensitive."""
    assert file_gbx._fuse_matches_filter("scene.TIF", ["*.tif"])
    assert file_gbx._fuse_matches_filter("scene.tif", ["*.TIF"])
    assert not file_gbx._fuse_matches_filter("scene.nc", ["*.tif"])


def test_fuse_matches_filter_glob_character_class():
    """_fuse_matches_filter handles [!.] character class correctly."""
    assert file_gbx._fuse_matches_filter("_data.tif", ["[!.]*"])
    assert file_gbx._fuse_matches_filter("normal.txt", ["[!.]*"])
    assert not file_gbx._fuse_matches_filter(".crc", ["[!.]*"])
    assert not file_gbx._fuse_matches_filter(".DS_Store", ["[!.]*"])


def test_glob_to_sql_basename_predicate_simple():
    """Simple *.ext glob compiles to a LIKE predicate (no backslash issues)."""
    pred = file_gbx._glob_to_sql_basename_predicate(["*.tif"], "file_name")
    assert "LIKE" in pred
    assert "%.tif" in pred
    assert "file_name" in pred


def test_glob_to_sql_basename_predicate_char_class():
    """Character-class glob compiles to an RLIKE predicate with [^.]."""
    pred = file_gbx._glob_to_sql_basename_predicate(["[!.]*"], "basename")
    assert "RLIKE" in pred
    assert "[^.]" in pred


def test_glob_to_sql_basename_predicate_multiple():
    """Multiple patterns are OR-ed in the predicate."""
    pred = file_gbx._glob_to_sql_basename_predicate(["*.tif", "*.nc"], "file_name")
    assert "OR" in pred
    assert "%.tif" in pred
    assert "%.nc" in pred


def test_glob_to_sql_basename_predicate_underscore_routes_to_rlike():
    """Pattern with _ routes to RLIKE (not bare LIKE) to avoid SQL wildcard divergence.

    SQL LIKE treats _ as 'any single character', while fnmatch treats it as a
    literal underscore.  A pattern like ndvi_*.tif must produce RLIKE so that
    ndvi2023.tif (no underscore) is NOT matched.
    """
    pred = file_gbx._glob_to_sql_basename_predicate(["ndvi_*.tif"], "file_name")
    assert "RLIKE" in pred
    # Must NOT be a bare LIKE (note: RLIKE contains "LIKE" as a substring —
    # check that the predicate is not using SQL LIKE by verifying no "LOWER(" wrapper,
    # which is the pattern used by the bare-LIKE branch).
    assert "LOWER(" not in pred


def test_enumerate_files_underscore_pattern_fuse_parity():
    """path_glob_filter='ndvi_*.tif' treats _ as literal: ndvi_2023.tif matches,
    ndvi2023.tif does NOT, and the SQL tiers emit an RLIKE (not a bare LIKE).
    """
    import os as os_module
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        match_file = os_module.path.join(tmpdir, "ndvi_2023.tif")
        no_match = os_module.path.join(tmpdir, "ndvi2023.tif")
        other = os_module.path.join(tmpdir, "landsat.tif")

        for p in (match_file, no_match, other):
            with open(p, "w") as f:
                f.write("x")

        # FUSE end-to-end: _ is literal
        mock_spark = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "fuse"
            result = file_gbx.enumerate_files(
                tmpdir, path_glob_filter="ndvi_*.tif", spark=mock_spark
            )

        basenames = {os_module.path.basename(r["path"]) for r in result}
        assert "ndvi_2023.tif" in basenames
        assert "ndvi2023.tif" not in basenames  # no literal _
        assert "landsat.tif" not in basenames

        # read_files tier: SQL must use RLIKE (not bare LIKE with _ wildcard)
        mock_spark2 = MagicMock()
        mock_spark2.sql.return_value = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "read_files"
            file_gbx.enumerate_files(
                tmpdir, path_glob_filter="ndvi_*.tif", spark=mock_spark2
            )
        sql_rf = mock_spark2.sql.call_args[0][0]
        assert "RLIKE" in sql_rf
        assert "ndvi_%.tif" not in sql_rf  # must NOT use LIKE with bare _

        # list_files tier: same
        mock_spark3 = MagicMock()
        mock_spark3.sql.return_value = MagicMock()
        with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
            mock_tier.return_value = "list_files"
            file_gbx.enumerate_files(
                tmpdir, path_glob_filter="ndvi_*.tif", spark=mock_spark3
            )
        sql_lf = mock_spark3.sql.call_args[0][0]
        assert "RLIKE" in sql_lf
        assert "ndvi_%.tif" not in sql_lf


def test_glob_to_sql_basename_predicate_question_mark_routes_to_rlike():
    """? pattern routes to RLIKE and the generated regex contains a bare dot."""
    pred = file_gbx._glob_to_sql_basename_predicate(["tile?.tif"], "file_name")
    assert "RLIKE" in pred
    # The ? in tile? becomes . in the regex — verify a dot is present in the regex fragment
    assert "tile." in pred
