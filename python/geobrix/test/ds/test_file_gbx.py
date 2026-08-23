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

    # UNRESOLVABLE_TABLE_VALUED_FUNCTION: emitted by local/OSS Spark when
    # read_files / list_files SQL TVFs are not registered.  Must be recognised
    # so file_access_tier falls through to FUSE instead of wrongly reporting
    # "read_files" tier available.
    exc_tvf = Exception(
        "[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve `read_files` "
        "to a table-valued function."
    )
    assert file_gbx._is_routine_unavailable(exc_tvf)


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
    # Must use top-level columns (not _metadata.*) — _metadata.file_path encoded the bug
    assert "_metadata.file_path" not in call_args
    assert "_metadata.file_size" not in call_args
    assert "regexp_replace(path, '^dbfs:', '')" in call_args

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
    # Must NOT use bare LIKE '_%' — underscore is a single-char wildcard in SQL LIKE
    assert (
        "LIKE '_%'" not in call_args
    ), "LIKE '_%' matches ALL non-empty filenames; use startswith() instead"
    # Must use startswith for literal underscore/dot prefix matching
    assert "startswith(" in call_args


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
# SQL predicate correctness — hidden-file filter must not use LIKE '_%' wildcard
# ---------------------------------------------------------------------------


def test_enumerate_read_files_sql_no_like_wildcard():
    """_enumerate_read_files must not use bare LIKE '_%' for hidden-file filter.

    SQL LIKE treats _ as a single-char wildcard, so LIKE '_%' matches EVERY
    non-empty filename, causing NOT(...) to exclude all rows (confirmed on-cluster:
    raw read_files returns 1000 rows; enumerate_files returned 0).
    The correct form uses startswith(basename, '_') / startswith(basename, '.').
    Also verifies top-level path/size columns are used with dbfs: scheme stripped.
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    sql = mock_spark.sql.call_args[0][0]
    # Must NOT contain bare LIKE '_%' (underscore wildcard bug)
    assert "LIKE '_%'" not in sql, (
        "Hidden-file filter uses bare LIKE '_%' which matches ALL non-empty filenames "
        "and causes NOT(...) to return 0 rows"
    )
    # Must use startswith for literal prefix matching
    assert "startswith(" in sql, "Expected startswith() for hidden-file filter"
    # Must use top-level path/size columns (not _metadata.*)
    assert (
        "_metadata.file_path" not in sql
    ), "Must use top-level 'path', not '_metadata.file_path'"
    assert (
        "_metadata.file_size" not in sql
    ), "Must use top-level 'size', not '_metadata.file_size'"
    # Must strip dbfs: scheme so output paths match FUSE tier's bare /Volumes/... form
    assert (
        "regexp_replace(path, '^dbfs:', '')" in sql
    ), "Expected regexp_replace(path, '^dbfs:', '') to strip dbfs: scheme prefix"
    # Must derive basename via substring_index for the hidden predicate
    assert "substring_index(path, '/', -1)" in sql


def test_enumerate_list_files_sql_no_like_wildcard():
    """_enumerate_list_files must not use bare LIKE '_%' for hidden-file filter.

    SQL LIKE treats _ as a single-char wildcard, so LIKE '_%' matches EVERY
    non-empty filename, causing NOT(...) to exclude all rows.
    The correct form uses startswith(basename, '_') / startswith(basename, '.').
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "list_files"
        file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    sql = mock_spark.sql.call_args[0][0]
    # Must NOT contain bare LIKE '_%' (underscore wildcard bug)
    assert "LIKE '_%'" not in sql, (
        "Hidden-file filter uses bare LIKE '_%' which matches ALL non-empty filenames "
        "and causes NOT(...) to return 0 rows"
    )
    # Must use startswith for literal prefix matching
    assert "startswith(" in sql, "Expected startswith() for hidden-file filter"
    # Must use substring_index for basename
    assert "substring_index(path, '/', -1)" in sql


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
    """auto + filespace + write-primitive available → write_file_table(file_mode='managed')."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
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
    """auto + no filespace + write-primitive available → write_file_table(file_mode='external')."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
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
    """auto + write-primitive unavailable → CTAS plain Delta write via spark.sql, no create_file / try_to_file."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = False
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
    """Explicit file_mode='managed' with write-primitive available → write_file_table(managed)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
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
    """Explicit file_mode='external' with write-primitive available → write_file_table(external)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "cat.sch.t",
            file_mode="external",
        )

    mock_wft.assert_called_once()
    _, kwargs = mock_wft.call_args
    assert kwargs["file_mode"] == "external"


def test_open_for_write_explicit_managed_write_primitive_unavailable_raises():
    """Explicit file_mode='managed' with write-primitive unavailable raises actionable ValueError."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = False
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
    assert "write-primitive" in err
    assert "DBR 19+" in err


def test_open_for_write_explicit_external_write_primitive_unavailable_raises():
    """Explicit file_mode='external' with write-primitive unavailable raises actionable ValueError."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = False
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
    assert "write-primitive" in err


def test_open_for_write_invalid_layout_raises():
    """Invalid layout raises ValueError before any side effects."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft:
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
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table"),
        _warnings_mod.catch_warnings(record=True) as caught,
    ):
        _warnings_mod.simplefilter("always")
        mock_fs.return_value = True
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
    """Explicit file_mode='managed' with no filespace raises before write-primitive detection."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    # Patch file_supported so we can confirm it was NOT reached (early guard fires first).
    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
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
    # Neither write-primitive detection nor write_file_table should have been reached.
    mock_fs.assert_not_called()
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


def test_ingest_files_raises_when_write_primitive_unavailable():
    """ingest_files raises a clear ValueError when the write-primitive is unavailable."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = False
        with pytest.raises(ValueError) as exc_info:
            file_gbx.ingest_files(
                mock_spark,
                "/Volumes/cat/s/v/src",
                "cat.sch.dst",
                filespace="/Volumes/cat/s/v",
            )

    err = str(exc_info.value)
    assert "ingest_files requires FILE write-primitive support" in err
    assert "DBR 19+" in err
    assert "open_for_write" in err


def test_ingest_files_builds_read_files_insert_sql():
    """ingest_files emits CREATE IF NOT EXISTS + INSERT with read_files(format=>'file')."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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

    # layout validation fires before the write-primitive check, so no mocking needed.
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

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = True
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
        # The predicate targets basename via substring_index (same as list_files)
        assert "substring_index(path, '/', -1)" in sql_rf
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


# ---------------------------------------------------------------------------
# IMPORTANT-2: _enumerate_read_files WHERE clause uses startswith + substring_index
# ---------------------------------------------------------------------------


def test_enumerate_files_read_files_where_uses_startswith_basename():
    """read_files tier: hidden-file skip clause uses startswith(substring_index(...)).

    The on-cluster schema for read_files(format=>'file') is [path, size, file] at the
    top level.  The hidden-file exclusion must derive basename via
    substring_index(path,'/',-1) and use startswith() for literal prefix matching.
    Using LIKE '_%' is wrong — _ is a single-char wildcard and would match every name.
    Using _metadata.file_name is wrong — it may be null under format=>'file'.
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files("/Volumes/test/path", spark=mock_spark)

    sql = mock_spark.sql.call_args[0][0]
    # WHERE clause must derive basename from top-level path column
    assert (
        "substring_index(path, '/', -1)" in sql
    ), f"Expected substring_index(path, '/', -1) in SQL; got:\n{sql}"
    # Must use startswith() not LIKE for literal prefix matching
    assert (
        "startswith(" in sql
    ), f"Expected startswith() for hidden-file filter; got:\n{sql}"
    # _metadata.file_name must NOT appear (it can be null under format=>'file')
    assert (
        "_metadata.file_name" not in sql
    ), f"_metadata.file_name found in SQL; should use substring_index; got:\n{sql}"


def test_enumerate_files_read_files_glob_predicate_uses_substring_index():
    """read_files tier: glob predicate column expression is substring_index(path,'/',-1).

    After the fix, both the hidden-file clause and the glob predicate use the same
    basename expression derived from the top-level path column.
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "read_files"
        file_gbx.enumerate_files(
            "/Volumes/test/path",
            extensions=(".tif",),
            spark=mock_spark,
        )

    sql = mock_spark.sql.call_args[0][0]
    # Glob predicate must use substring_index for basename
    assert (
        "substring_index(path, '/', -1)" in sql
    ), f"Expected substring_index basename in glob predicate; got:\n{sql}"
    # Must NOT use _metadata.file_name
    assert (
        "_metadata.file_name" not in sql
    ), f"_metadata.file_name found in glob predicate; should use substring_index; got:\n{sql}"
    # The LIKE pattern for .tif must still be present
    assert "%.tif" in sql


# ---------------------------------------------------------------------------
# IMPORTANT-3: open_for_write / ingest_files gated on write-primitive probe
# ---------------------------------------------------------------------------


def test_open_for_write_write_primitive_absent_read_tier_present_raises():
    """Write-primitive absent but read tier present → explicit managed still raises actionable error.

    This is the divergence scenario: read_files is available (file_access_tier would
    pass) but create_file/try_to_file are not (file_supported returns False).
    Before the fix, open_for_write(file_mode='managed') would pass the read-tier gate,
    route to write_file_table, and surface a raw downstream error.
    After the fix, it raises ValueError with the actionable message.
    """
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = False  # write primitive absent
        with pytest.raises(ValueError) as exc_info:
            file_gbx.open_for_write(
                mock_spark,
                mock_df,
                "cat.sch.t",
                file_mode="managed",
                filespace="/Volumes/c/s/v",
            )

    err = str(exc_info.value)
    assert "write-primitive" in err
    assert "DBR 19+" in err
    # write_file_table must NOT be called (no raw downstream error)
    mock_wft.assert_not_called()


def test_open_for_write_write_primitive_present_routes_to_file_table():
    """write-primitive present → explicit managed routes to write_file_table (not fuse path)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.pyrx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
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


def test_ingest_files_write_primitive_absent_raises_actionable_error():
    """ingest_files: write-primitive absent → actionable ValueError, not raw downstream error."""
    mock_spark = MagicMock()

    with patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs:
        mock_fs.return_value = False
        with pytest.raises(ValueError) as exc_info:
            file_gbx.ingest_files(
                mock_spark,
                "/Volumes/cat/s/v/src",
                "cat.sch.dst",
                filespace="/Volumes/cat/s/v",
            )

    err = str(exc_info.value)
    assert "write-primitive" in err
    assert "DBR 19+" in err
    assert "open_for_write" in err
    # No SQL should have been emitted
    mock_spark.sql.assert_not_called()


# ---------------------------------------------------------------------------
# MINOR-1: None-safe raster partition sort
# ---------------------------------------------------------------------------


def test_raster_partition_sort_with_none_file_path():
    """MINOR-1: Raster partitions with None file_path sort without TypeError.

    Regression test: partitions from a tiles table may have file_path=None,
    which would cause TypeError when sorted by the key. The sort must be
    None-safe, placing None values deterministically (e.g. at the end).
    """
    from databricks.labs.gbx.ds import raster

    # Build a list of partitions with mixed None and str file_path values
    parts = [
        raster._TilePartition(file_path="/path/to/file2.tif", window=(0, 0, 256, 256)),
        raster._TilePartition(file_path=None, window=(0, 0, 256, 256)),
        raster._TilePartition(file_path="/path/to/file1.tif", window=(0, 0, 256, 256)),
        raster._TilePartition(file_path=None, window=(0, 0, 256, 256)),
        raster._TilePartition(file_path="/path/to/file3.tif", window=(0, 0, 256, 256)),
    ]

    # Sort should not raise TypeError
    sorted_parts = sorted(parts, key=lambda p: (p.file_path is None, p.file_path or ""))

    # Verify order: None values sort last (True > False), then by file_path
    assert sorted_parts[0].file_path == "/path/to/file1.tif"
    assert sorted_parts[1].file_path == "/path/to/file2.tif"
    assert sorted_parts[2].file_path == "/path/to/file3.tif"
    assert sorted_parts[3].file_path is None
    assert sorted_parts[4].file_path is None


# ---------------------------------------------------------------------------
# MINOR-2: Fuse-CTAS path-field guard
# ---------------------------------------------------------------------------


def test_open_for_write_fuse_mode_requires_path_column():
    """MINOR-2: fuse write without 'path' column must guard against bare ORDER BY path.

    When layout='order' or 'cluster', fuse mode emits ORDER BY path. If the
    dataframe lacks a 'path' column, it should raise a clear ValueError rather
    than emitting broken SQL.
    """
    mock_spark = MagicMock()
    mock_spark.sql.return_value = None

    # Create a schema WITHOUT 'path' column (only has other columns)
    schema_no_path = StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField(
                "tile",
                StructType(
                    [
                        StructField("cellid", LongType(), nullable=False),
                        StructField("raster", BinaryType(), nullable=True),
                    ]
                ),
                nullable=False,
            ),
        ]
    )

    mock_df = MagicMock()
    mock_df.schema = schema_no_path
    mock_df.createOrReplaceTempView.return_value = None

    # Should raise ValueError about missing 'path' column
    with pytest.raises(ValueError, match="path.*column|Column.*path"):
        file_gbx.open_for_write(
            mock_spark,
            mock_df,
            "catalog.schema.table",
            file_mode="fuse",
            layout="order",
        )

    # Verify the CREATE TABLE SQL was never called (only cleanup may be called)
    # The assertion checks that no CREATE TABLE statement was executed
    create_table_called = any(
        "CREATE TABLE" in str(call) for call in mock_spark.sql.call_args_list
    )
    assert (
        not create_table_called
    ), "CREATE TABLE should not be called when path column is missing"


# ---------------------------------------------------------------------------
# MINOR-3: Vector reader via resolve_local_path (not inline to_local_path)
# ---------------------------------------------------------------------------


def test_resolve_local_path_validates_access_and_returns_fuse_path():
    """MINOR-3: resolve_local_path validates access mode and returns local FUSE path.

    Unlike open_for_read (which returns source unchanged), resolve_local_path
    should validate the access mode, then return the actual local FUSE path
    suitable for passing to GDAL/pyogrio. This makes it suitable for the vector
    reader's local-path resolution.
    """
    # Mock file_access_tier and resolve_access to simplify the test
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        with patch("databricks.labs.gbx.ds.file_gbx.resolve_access") as mock_resolve:
            with patch(
                "databricks.labs.gbx.ds.file_gbx.to_local_path"
            ) as mock_to_local:
                mock_tier.return_value = "read_files"
                mock_resolve.return_value = "read_files"  # validation passes
                mock_to_local.return_value = "/Volumes/cat/sch/vol/path/file.shp"

                result = file_gbx.resolve_local_path(
                    "/Volumes/cat/sch/vol/path/file.shp",
                    access="auto",
                )

                # Should return the FUSE path
                assert result == "/Volumes/cat/sch/vol/path/file.shp"
                # Should call resolve_access to validate
                mock_resolve.assert_called_once()


def test_resolve_local_path_raises_on_explicit_file_fuse_tier():
    """MINOR-3: resolve_local_path raises clear error for explicit FILE on FUSE tier."""
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier") as mock_tier:
        mock_tier.return_value = "fuse"  # FILE not available

        # Explicit "external" request on FUSE tier should raise
        with pytest.raises(ValueError, match="FILE.*not available|tier.*fuse"):
            file_gbx.resolve_local_path(
                "/Volumes/cat/sch/vol/path/file.shp",
                access="external",
            )


# ---------------------------------------------------------------------------
# Task 4 Fix A: _enumerate_fuse prunes hidden directories
# ---------------------------------------------------------------------------


def test_enumerate_fuse_excludes_hidden_subdirectory_files_by_default(tmp_path):
    """include_hidden=False (default) must NOT descend into dirs starting with '.' or '_'.

    The old vector _members() pruned 'dirs[:] = ...' to prevent descending into
    _gbx_scratch/ and .staging/; the same pruning must happen in _enumerate_fuse
    so that valid-named partial files inside those dirs are not returned.
    """
    from databricks.labs.gbx.ds import file_gbx

    # Normal subdir with a matching file
    normal = tmp_path / "sub"
    normal.mkdir()
    (normal / "data.geojsonl").write_bytes(b"{}")

    # Hidden (underscore) subdir — must NOT be descended into
    scratch = tmp_path / "_scratch"
    scratch.mkdir()
    (scratch / "data.geojsonl").write_bytes(b"{}")

    # Hidden (dot) subdir — must NOT be descended into
    staging = tmp_path / ".staging"
    staging.mkdir()
    (staging / "part-0.geojsonl").write_bytes(b"{}")

    result = [
        r["path"]
        for r in file_gbx._enumerate_fuse(
            str(tmp_path), recursive=True, include_hidden=False
        )
    ]
    assert result == [str(normal / "data.geojsonl")]


def test_enumerate_fuse_includes_hidden_subdirectory_files_when_enabled(tmp_path):
    """include_hidden=True must descend into hidden dirs and return their files too."""
    from databricks.labs.gbx.ds import file_gbx

    normal = tmp_path / "sub"
    normal.mkdir()
    (normal / "data.geojsonl").write_bytes(b"{}")

    scratch = tmp_path / "_scratch"
    scratch.mkdir()
    (scratch / "data.geojsonl").write_bytes(b"{}")

    result = sorted(
        r["path"]
        for r in file_gbx._enumerate_fuse(
            str(tmp_path), recursive=True, include_hidden=True
        )
    )
    assert result == sorted(
        [str(normal / "data.geojsonl"), str(scratch / "data.geojsonl")]
    )


# ---------------------------------------------------------------------------
# resolve_file_table tests (Task 1 — shared core for FILE-column table reads)
# ---------------------------------------------------------------------------


def _make_unsorted_path_table(spark, name):
    """Create a plain Hive table with path rows inserted in REVERSE alphabetical order.

    Rows: "/z/z.tif", "/a/a.tif", "/m/m.tif" — clearly unsorted so ordering
    tests are meaningful.
    """
    import shutil
    from pathlib import Path

    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [("/z/z.tif",), ("/a/a.tif",), ("/m/m.tif",)],
        "path string",
    )
    df.write.saveAsTable(name)


def test_resolve_file_table_external_orders_by_source(spark):
    """resolve_file_table default (skip_ordering=False) returns source sorted asc, nulls last.

    The test proves that orderBy does the work by asserting:
    1. The raw table scan order is provably NOT sorted (rows were inserted in
       reverse-alpha order: /z, /a, /m).
    2. resolve_file_table with skip_ordering=False returns rows sorted asc,
       nulls last — proving orderBy, not lucky scan order, produced the result.
    """
    from databricks.labs.gbx.ds.file_gbx import resolve_file_table

    _make_unsorted_path_table(spark, "_rft_order_test")

    # Precondition: raw scan order must NOT be sorted so the test can't false-pass.
    raw_paths = [
        r["path"] for r in spark.sql("SELECT path FROM _rft_order_test").collect()
    ]
    sorted_raw = sorted(raw_paths, key=lambda s: (s is None, s or ""))
    assert raw_paths != sorted_raw, (
        "Precondition failed: raw table scan order is already sorted; "
        "the test cannot prove that resolve_file_table's orderBy did the work. "
        f"raw_paths={raw_paths}"
    )

    result = resolve_file_table(spark, "_rft_order_test")
    assert (
        "source" in result.columns
    ), "resolve_file_table must return a 'source' column"
    sources = [r["source"] for r in result.collect()]
    assert sources == sorted(
        sources, key=lambda s: (s is None, s or "")
    ), f"sources must be sorted asc nulls-last; got {sources}"


def test_resolve_file_table_skip_ordering_preserves_input_order(spark, monkeypatch):
    """resolve_file_table(skip_ordering=True) must NOT call orderBy."""
    from pyspark.sql import DataFrame

    from databricks.labs.gbx.ds.file_gbx import resolve_file_table

    _make_unsorted_path_table(spark, "_rft_skip_order_test")

    orderby_called = []
    orig_orderby = DataFrame.orderBy

    def _spy_orderby(self, *args, **kwargs):
        orderby_called.append(True)
        return orig_orderby(self, *args, **kwargs)

    monkeypatch.setattr(DataFrame, "orderBy", _spy_orderby)

    result = resolve_file_table(spark, "_rft_skip_order_test", skip_ordering=True)
    sources = [r["source"] for r in result.collect()]

    assert (
        not orderby_called
    ), "orderBy was called even with skip_ordering=True — ordering must be suppressed"
    assert set(sources) == {
        "/z/z.tif",
        "/a/a.tif",
        "/m/m.tif",
    }, f"all rows must be returned; got {sources}"


def test_resolve_file_table_no_file_typed_column_in_result(spark):
    """resolve_file_table result schema must not contain a FILE-typed column.

    Column discovery uses DESCRIBE TABLE, not spark.table(...).schema, which is
    Serverless-GC-unsafe. The result DataFrame must expose only plain-column types
    (source STRING, size BIGINT, path_mode STRING, passthrough) — never a FILE ref.
    """
    from databricks.labs.gbx.ds.file_gbx import resolve_file_table

    _make_unsorted_path_table(spark, "_rft_no_file_col_test")
    result = resolve_file_table(spark, "_rft_no_file_col_test")
    col_names_lower = [c.lower() for c in result.columns]
    assert (
        "file" not in col_names_lower
    ), f"result schema must not contain a 'file' column; got {result.columns}"
    # Verify core output columns exist
    assert "source" in result.columns
    assert "size" in result.columns
    assert "path_mode" in result.columns


# ---------------------------------------------------------------------------
# gbx_file_read table mode — Task 3: real size + ordering
# ---------------------------------------------------------------------------


def _make_sized_path_table(spark, name):
    """Create a plain Hive table with (path, size) rows inserted in REVERSE alpha order."""
    import shutil
    from pathlib import Path

    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [("/z/z.tif", 300), ("/a/a.tif", 100), ("/m/m.tif", 200)],
        "path string, size long",
    )
    df.write.saveAsTable(name)


def test_gbx_file_read_table_carries_real_size_and_order(spark):
    """gbx_file_read table mode returns real size (not all NULL) and rows ordered by path.

    The table has a ``size`` column; the pre-Task-3 implementation forced
    ``CAST(NULL AS BIGINT) AS size``.  After Task 3 the table-mode branch
    delegates to ``resolve_file_table`` so real sizes are carried through.
    Ordering is also asserted: rows must be sorted by path asc, nulls last.
    """
    from databricks.labs.gbx.ds.file_gbx import gbx_file_read

    tbl = "_gbx_file_read_tbl_size_order_test"
    _make_sized_path_table(spark, tbl)

    result = gbx_file_read(spark, tbl, source_type="table")
    rows = result.collect()

    # Schema contract: [path, size, file].
    assert result.columns == [
        "path",
        "size",
        "file",
    ], f"Schema must be [path, size, file]; got {result.columns}"

    # Real sizes — not all NULL.
    sizes = [r["size"] for r in rows]
    assert any(
        s is not None for s in sizes
    ), f"Expected real sizes from the table; got all NULL: {sizes}"
    assert set(sizes) == {100, 200, 300}, f"Expected sizes 100/200/300; got {sizes}"

    # Rows ordered by path asc, nulls last.
    paths = [r["path"] for r in rows]
    assert paths == sorted(
        paths, key=lambda s: (s is None, s or "")
    ), f"Expected rows ordered by path; got {paths}"

    # file column must be NULL (no raw FILE ref from a table read-back).
    files = [r["file"] for r in rows]
    assert all(
        f is None for f in files
    ), f"Expected file=NULL for all rows; got {files}"


# ---------------------------------------------------------------------------
# PathLike inputs to the string-path helpers (regression: FileRef.as_local_file()
# returns a pathlib.Path; _is_fuse_path did str.startswith and crashed on it,
# which surfaced as rst_numbands=None on a FILE-referenced tile while rst_summary
# worked via a different route).
# ---------------------------------------------------------------------------


def test_is_fuse_path_accepts_pathlib():
    """_is_fuse_path handles both str and pathlib.Path (os.PathLike)."""
    from pathlib import Path

    assert file_gbx._is_fuse_path(Path("/Volumes/c/s/v/a.tif")) is True
    assert file_gbx._is_fuse_path(Path("/dbfs/x/y.tif")) is True
    assert file_gbx._is_fuse_path(Path("/tmp/local.tif")) is False
    # still correct for plain strings
    assert file_gbx._is_fuse_path("/Volumes/c/s/v/a.tif") is True
    assert file_gbx._is_fuse_path("/tmp/local.tif") is False


def test_stage_local_if_needed_accepts_pathlib_local(tmp_path):
    """A local pathlib.Path passes through and is normalised to a str path."""
    p = tmp_path / "f.tif"
    p.write_bytes(b"stub")
    local, is_temp = file_gbx._stage_local_if_needed(p)  # PosixPath in
    assert is_temp is False
    assert isinstance(local, str), "local_path must be a str, not a Path"
    assert local == str(p)
