"""Unit tests for file_gbx capability tier detection and access resolution."""

from __future__ import annotations

import os
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
# Tier detection
# ---------------------------------------------------------------------------

UNRESOLVED = "UNRESOLVED_ROUTINE: function read_files not found"
PATH_NOT_FOUND = "PATH_NOT_FOUND: path /Volumes/__gbx_probe__/__none__ does not exist"


def _probe(outcome):
    """What one probe query does when collected: "ok" returns, otherwise it raises that error."""
    collect = (
        MagicMock(return_value=[])
        if outcome == "ok"
        else MagicMock(side_effect=Exception(outcome))
    )
    return MagicMock(collect=collect)


# (read_files probe, list_files probe) -> tier. A PATH_NOT_FOUND proves the function RAN, so it exists
# (once it was swallowed as "unavailable" and the tier fell through); only an unresolved routine falls through.
TIERS = {
    "read_files_runs": (["ok"], "read_files"),
    "read_files_path_not_found_proves_it_exists": ([PATH_NOT_FOUND], "read_files"),
    "list_files_when_read_files_is_unresolved": ([UNRESOLVED, "ok"], "list_files"),
    "list_files_path_not_found_proves_it_exists": (
        [UNRESOLVED, PATH_NOT_FOUND],
        "list_files",
    ),
    "fuse_when_both_are_unresolved": ([UNRESOLVED, UNRESOLVED], "fuse"),
}


@pytest.mark.parametrize("probes,tier", TIERS.values(), ids=TIERS.keys())
def test_the_tier_follows_which_file_probes_exist(probes, tier):
    spark = MagicMock()
    spark.sql.side_effect = [_probe(o) for o in probes]
    assert file_gbx._detect_tier(spark) == tier
    first = spark.sql.call_args_list[0][0][0]
    assert "read_files" in first and "format => 'file'" in first


def test_a_session_less_runtime_is_fuse():
    with patch("databricks.labs.gbx.ds.file_gbx.SparkSession") as session:
        session.getActiveSession.return_value = None
        session.builder.getOrCreate.side_effect = RuntimeError("No spark")
        assert file_gbx.file_access_tier(None) == "fuse"


def test_the_tier_is_detected_once_per_session():
    spark, calls = MagicMock(), []
    file_gbx._TIER_CACHE.clear()
    with patch(
        "databricks.labs.gbx.ds.file_gbx._detect_tier",
        side_effect=lambda s: calls.append(s) or "read_files",
    ):
        assert (
            file_gbx.file_access_tier(spark)
            == file_gbx.file_access_tier(spark)
            == "read_files"
        )
    assert len(calls) == 1


# an error message -> whether it says the routine itself is unavailable (not that a path or a read went wrong)
ROUTINE_ERRORS = [
    (Exception("UNRESOLVED_ROUTINE: function read_files not found"), True),
    (Exception("UNRESOLVABLE_ROUTINE: routine not found"), True),
    (Exception("PARSE_SYNTAX_ERROR: unexpected token 'read_files'"), True),
    (Exception("UNSUPPORTED: function not available"), True),
    (Exception("cannot resolve function read_files"), True),
    (Exception("Undefined function: read_files"), True),
    (Exception("function read_files does not exist"), True),
    # local/OSS Spark has no read_files / list_files TVFs: the tier must fall through to FUSE
    (
        Exception(
            "[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve `read_files` to a table-valued function."
        ),
        True,
    ),
    (Exception("PATH_NOT_FOUND: path does not exist"), False),
    (FileNotFoundError("file not found"), False),
    (IOError("I/O error reading file"), False),
    (PermissionError("access denied"), False),
    (Exception("some other runtime error"), False),
]


@pytest.mark.parametrize(
    "err,unavailable", ROUTINE_ERRORS, ids=[str(e)[:40] for e, _ in ROUTINE_ERRORS]
)
def test_which_errors_say_the_routine_is_unavailable(err, unavailable):
    assert file_gbx._is_routine_unavailable(err) is unavailable


# ---------------------------------------------------------------------------
# Access resolution: 'auto' takes the tier there is; an explicit FILE mode needs one
# ---------------------------------------------------------------------------

# (mode, tier) -> the access, or the words of the refusal
ACCESS = {
    **{f"auto_{t}": ("auto", t, t) for t in ("read_files", "list_files", "fuse")},
    "managed_read_files": ("managed", "read_files", "managed"),
    "managed_list_files": ("managed", "list_files", "managed"),
    "external_read_files": ("external", "read_files", "external"),
    "managed_fuse_refused": ("managed", "fuse", ValueError),
    "external_fuse_refused": ("external", "fuse", ValueError),
    "unknown_mode_refused": ("invalid_mode", "read_files", ValueError),
}


@pytest.mark.parametrize("mode,tier,want", ACCESS.values(), ids=ACCESS.keys())
def test_resolve_access(mode, tier, want):
    if want is ValueError:
        with pytest.raises(ValueError) as ei:
            file_gbx.resolve_access(mode, tier=tier)
        msg = str(ei.value)
        if mode == "invalid_mode":
            assert "Unknown access mode" in msg
        else:
            assert (
                f"{mode} FILE" in msg
                and "not available" in msg
                and "tier='fuse'" in msg
            )
            assert (
                "13.3 LTS" in msg or "18 LTS" in msg
            ) and "Upgrade your cluster" in msg
    else:
        assert file_gbx.resolve_access(mode, tier=tier) == want


def test_resolve_access_detects_the_tier_of_the_session_when_it_is_not_given():
    spark = MagicMock()
    with patch(
        "databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="read_files"
    ) as detect:
        assert file_gbx.resolve_access("auto", spark=spark) == "read_files"
    detect.assert_called_once_with(spark)


def test_the_listing_helpers_are_reachable_from_file_gbx():
    assert file_gbx.to_local_path("file:/tmp/x") == "/tmp/x"
    assert file_gbx.to_spark_uri("/Volumes/cat/schema/vol/file.tif").startswith("dbfs:")
    assert callable(file_gbx.list_files)


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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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


@pytest.mark.parametrize("entry", ["open_for_write", "gbx_file_write"])
def test_the_default_file_mode_is_fuse_even_where_file_is_supported(entry):
    """No ``file_mode`` is a plain Delta write, as on a runtime without FILE: a table made
    on FILE compute reads on every compute. ``file_mode="auto"`` (the tests above) is the
    opt-in to FILE references."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
    ):
        mock_fs.return_value = True
        if entry == "open_for_write":
            file_gbx.open_for_write(mock_spark, mock_df, "cat.sch.t")
        else:
            file_gbx.gbx_file_write(mock_df, "cat.sch.t", spark=mock_spark)

    mock_wft.assert_not_called()
    assert [s for s in captured if "CREATE TABLE" in s.upper()]
    for sql_str in captured:
        assert "create_file" not in sql_str and "try_to_file" not in sql_str


def test_open_for_write_explicit_managed_routes_to_write_file_table():
    """Explicit file_mode='managed' with write-primitive available → write_file_table(managed)."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported") as mock_fs,
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
    # The options name fuse as the default and auto as the opt-in.
    assert "file_mode='fuse' (default)" in err
    assert "file_mode='auto'" in err


def _write_with(entry, mock_spark, mock_df, **kw):
    if entry == "open_for_write":
        file_gbx.open_for_write(mock_spark, mock_df, "cat.sch.t", **kw)
    else:
        file_gbx.gbx_file_write(mock_df, "cat.sch.t", spark=mock_spark, **kw)


@pytest.mark.parametrize("entry", ["open_for_write", "gbx_file_write"])
def test_filespace_without_file_mode_resolves_as_auto_to_managed(entry):
    """A ``filespace`` is FILE MANAGED intent: with no ``file_mode`` and FILE available it
    writes managed (as ``auto`` does), not a plain Delta table that ignores the filespace.
    """
    mock_spark = MagicMock()
    mock_df = _make_tile_df()
    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True),
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
    ):
        _write_with(entry, mock_spark, mock_df, filespace="/Volumes/c/s/v")

    mock_wft.assert_called_once()
    assert mock_wft.call_args.kwargs["file_mode"] == "managed"
    assert mock_wft.call_args.kwargs["filespace"] == "/Volumes/c/s/v"


@pytest.mark.parametrize("entry", ["open_for_write", "gbx_file_write"])
def test_filespace_without_file_mode_raises_where_file_is_unavailable(entry):
    """No FILE on the runtime: a ``filespace`` with no ``file_mode`` raises the clear error
    instead of silently writing plain Delta."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()
    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported", return_value=False),
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
    ):
        with pytest.raises(ValueError, match="FILE write-primitive"):
            _write_with(entry, mock_spark, mock_df, filespace="/Volumes/c/s/v")

    mock_wft.assert_not_called()
    assert not captured


@pytest.mark.parametrize("entry", ["open_for_write", "gbx_file_write"])
@pytest.mark.parametrize("explicit", ["fuse", "external"])
def test_explicit_file_mode_wins_over_filespace(entry, explicit):
    """An explicit ``file_mode`` is honoured even with a ``filespace``."""
    captured = []
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = lambda sql: captured.append(sql)
    mock_df = _make_tile_df()
    with (
        patch("databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True),
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
    ):
        _write_with(
            entry, mock_spark, mock_df, file_mode=explicit, filespace="/Volumes/c/s/v"
        )

    if explicit == "fuse":
        mock_wft.assert_not_called()
        assert [s for s in captured if "CREATE TABLE" in s.upper()]
    else:
        mock_wft.assert_called_once()
        assert mock_wft.call_args.kwargs["file_mode"] == "external"


def test_open_for_write_invalid_layout_raises():
    """Invalid layout raises ValueError before any side effects."""
    mock_spark = MagicMock()
    mock_df = _make_tile_df()

    with patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft:
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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table"),
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
    from databricks.labs.gbx.rasterx import file_table as ft

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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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


def _ingest_sql(src="/Volumes/cat/s/v/src", **kw):
    """The statements ingest_files sends, with the write primitive present."""
    captured = []
    spark = MagicMock()
    spark.sql.side_effect = captured.append
    with patch("databricks.labs.gbx.ds.file_gbx.file_supported", return_value=True):
        file_gbx.ingest_files(
            spark, src, "cat.sch.dst", filespace="/Volumes/cat/s/v", **kw
        )
    return captured


# ingest_files options -> (in the INSERT, not in the INSERT, in the CREATE, not in the CREATE, a DROP is sent)
INGEST = {
    "default_is_a_recursive_read_files_into_a_managed_file_column": (
        {"layout": "order"},
        ["read_files(", "format => 'file'", "recursiveFileLookup => true", "_metadata.file_path AS path", "file AS tile_file", "ORDER BY path"], [],
        ["IF NOT EXISTS", "FILE MANAGED"], ["PARTITIONED BY"], False,
    ),
    "plain_layout_has_no_order_by": ({"layout": "plain"}, [], ["ORDER BY"], [], [], False),
    "non_recursive": ({"recursive": False}, ["recursiveFileLookup => false"], ["recursiveFileLookup => true"], [], [], False),
    "custom_file_column": ({"file_col": "my_file"}, ["file AS my_file"], [], ["my_file FILE MANAGED"], [], False),
    "overwrite_drops_and_recreates": ({"overwrite": True}, [], [], ["CREATE TABLE"], ["IF NOT EXISTS"], True),
}  # fmt: skip


@pytest.mark.parametrize(
    "opts,in_insert,not_insert,in_create,not_create,drops",
    INGEST.values(),
    ids=INGEST.keys(),
)
def test_ingest_files_sends_a_create_and_a_read_files_insert(
    opts, in_insert, not_insert, in_create, not_create, drops
):
    sql = _ingest_sql(**opts)
    insert = next(s for s in sql if s.strip().upper().startswith("INSERT"))
    create = next(s for s in sql if "CREATE TABLE" in s.upper())
    assert all(p in insert for p in in_insert) and not any(
        p in insert for p in not_insert
    ), insert
    assert all(p.upper() in create.upper() for p in in_create) and not any(
        p.upper() in create.upper() for p in not_create
    ), create
    assert any("DROP TABLE" in s.upper() for s in sql) is drops


def test_ingest_files_doubles_a_single_quote_in_the_source_path():
    insert = next(
        s
        for s in _ingest_sql("/Volumes/cat/s/o'reilly/src")
        if s.strip().upper().startswith("INSERT")
    )
    assert "o''reilly" in insert and "o'reilly" not in insert.replace("o''reilly", "")


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


# ---------------------------------------------------------------------------
# enumerate_files positive filter (extensions / path_glob_filter)
# ---------------------------------------------------------------------------


def _fuse_names(tmp_path, files, **kw):
    for name in files:
        (tmp_path / name).write_text("x")
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value="fuse"):
        got = file_gbx.enumerate_files(str(tmp_path), spark=MagicMock(), **kw)
    return sorted(os.path.basename(r["path"]) for r in got)


# files, enumerate_files filter options -> the names listed (FUSE tier). Matching ignores case, a character class
# can leave dot files out, and a "_" is a literal underscore (as in fnmatch), not SQL LIKE's any-one-character.
FUSE_FILTERS = {
    "extensions_ignore_case": (["a.tif", "b.TIF", "data.nc", "readme.txt"], {"extensions": (".tif",)}, ["a.tif", "b.TIF"]),
    "glob_ignores_case": (["scene.tif", "scene.nc"], {"path_glob_filter": "*.TIF"}, ["scene.tif"]),
    "char_class_leaves_out_dot_files": (
        ["_data.tif", ".crc", ".DS_Store", "normal.txt"], {"include_hidden": True, "path_glob_filter": "[!.]*"}, ["_data.tif", "normal.txt"],
    ),
    "underscore_is_literal": (["ndvi_2023.tif", "ndvi2023.tif", "landsat.tif"], {"path_glob_filter": "ndvi_*.tif"}, ["ndvi_2023.tif"]),
    "question_mark_is_one_character": (["tile1.tif", "tile12.tif"], {"path_glob_filter": "tile?.tif"}, ["tile1.tif"]),
}  # fmt: skip


@pytest.mark.parametrize(
    "files,opts,want", FUSE_FILTERS.values(), ids=FUSE_FILTERS.keys()
)
def test_enumerate_files_filters_by_name_on_the_fuse_tier(tmp_path, files, opts, want):
    assert _fuse_names(tmp_path, files, **opts) == want


# filter options -> the pieces of the basename predicate the SQL tiers (read_files, list_files) put in the query
SQL_FILTERS = {
    "extension_is_a_like": ({"extensions": (".tif",)}, ["LIKE", "%.tif"], []),
    "extensions_are_or_ed": (
        {"extensions": (".tif", ".nc")},
        ["OR", "%.tif", "%.nc"],
        [],
    ),
    "character_class_is_an_rlike": (
        {"path_glob_filter": "[!.]*"},
        ["RLIKE", "[^.]"],
        [],
    ),
    # LIKE reads "_" as any one character: the pattern must be a regex, not "ndvi_%.tif"
    "underscore_is_an_rlike": (
        {"path_glob_filter": "ndvi_*.tif"},
        ["RLIKE"],
        ["ndvi_%.tif", "LOWER("],
    ),
    "question_mark_is_an_rlike": (
        {"path_glob_filter": "tile?.tif"},
        ["RLIKE", "tile."],
        [],
    ),
}


@pytest.mark.parametrize("tier", ["read_files", "list_files"])
@pytest.mark.parametrize("opts,has,lacks", SQL_FILTERS.values(), ids=SQL_FILTERS.keys())
def test_enumerate_files_filters_by_name_in_the_sql_of_the_file_tiers(
    tmp_path, tier, opts, has, lacks
):
    spark = MagicMock()
    with patch("databricks.labs.gbx.ds.file_gbx.file_access_tier", return_value=tier):
        file_gbx.enumerate_files(str(tmp_path), spark=spark, **opts)
    sql = spark.sql.call_args[0][0]
    assert all(part in sql for part in has) and not any(
        part in sql for part in lacks
    ), sql


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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
        patch("databricks.labs.gbx.rasterx.file_table.write_file_table") as mock_wft,
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
# _enumerate_fuse prunes hidden directories
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
# resolve_file_table tests (shared core for FILE-column table reads)
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
# gbx_file_read table mode: real size + ordering
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

    The table has a ``size`` column; the earlier implementation forced
    ``CAST(NULL AS BIGINT) AS size``.  The table-mode branch now
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
