"""Task 1: session-free enumeration core — _accept_basename + list_local_files.

Also covers the listing-efficiency fix (need_size=False):
  - list_local_files must never stat individual files (FUSE overhead prevention).
  - _enumerate_fuse(need_size=False) returns None sizes.
  - _enumerate_fuse(need_size=True) still returns correct sizes.
  - Single-file, hidden-dir-pruning, and recursive variants verified under scandir.
"""

import os
from unittest.mock import patch

import pytest

from databricks.labs.gbx.ds import file_gbx


def test_accept_basename_skips_hidden_by_default():
    assert (
        file_gbx._accept_basename("_SUCCESS", include_hidden=False, glob_patterns=None)
        is False
    )
    assert (
        file_gbx._accept_basename(".crc", include_hidden=False, glob_patterns=None)
        is False
    )
    assert (
        file_gbx._accept_basename("a.tif", include_hidden=False, glob_patterns=None)
        is True
    )


def test_accept_basename_include_hidden_admits_underscore():
    assert (
        file_gbx._accept_basename("_data.tif", include_hidden=True, glob_patterns=None)
        is True
    )


def test_accept_basename_glob_is_anded_case_insensitive():
    # include_hidden True but glob still excludes non-matching names
    assert (
        file_gbx._accept_basename("a.TIF", include_hidden=True, glob_patterns=["*.tif"])
        is True
    )
    assert (
        file_gbx._accept_basename("a.nc", include_hidden=True, glob_patterns=["*.tif"])
        is False
    )


def test_list_local_files_sorted_paths_and_extension_filter(tmp_path):
    (tmp_path / "b.tif").write_bytes(b"x")
    (tmp_path / "a.tif").write_bytes(b"x")
    (tmp_path / "c.nc").write_bytes(b"x")
    (tmp_path / "_SUCCESS").write_bytes(b"")
    out = file_gbx.list_local_files(str(tmp_path), extensions=(".tif",))
    assert out == sorted(out)
    assert [os.path.basename(p) for p in out] == ["a.tif", "b.tif"]


def test_list_local_files_raises_when_no_match(tmp_path):
    (tmp_path / "a.nc").write_bytes(b"x")
    with pytest.raises(FileNotFoundError):
        file_gbx.list_local_files(str(tmp_path), extensions=(".tif",))


# ---------------------------------------------------------------------------
# Listing-efficiency fix: need_size parameter
# ---------------------------------------------------------------------------


def test_list_local_files_no_stat_per_file(tmp_path):
    """list_local_files must NOT call os.stat on individual files.

    Before the fix, _enumerate_fuse called os.stat(fp) per file regardless of
    whether the caller needed the size.  list_local_files discards size, so all
    those stat syscalls were pure overhead (measured: ~165 s for 10k-file FUSE
    Volume).  After the fix, list_local_files passes need_size=False and no
    per-file stat is issued.
    """
    for name in ("a.tif", "b.tif", "c.tif"):
        (tmp_path / name).write_bytes(b"data")
    (tmp_path / "_SUCCESS").write_bytes(b"")

    stat_paths: list[str] = []
    real_stat = os.stat

    def tracking_stat(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        stat_paths.append(str(path))
        return real_stat(path, *args, **kwargs)

    with patch("os.stat", tracking_stat):
        result = file_gbx.list_local_files(str(tmp_path), extensions=(".tif",))

    assert [os.path.basename(p) for p in result] == ["a.tif", "b.tif", "c.tif"]
    # No individual .tif file should have been stat'd.
    tif_stats = [p for p in stat_paths if p.endswith(".tif")]
    assert tif_stats == [], f"os.stat was called on files: {tif_stats}"


def test_enumerate_fuse_need_size_false_returns_none_sizes(tmp_path):
    """_enumerate_fuse(need_size=False) returns size=None for every entry."""
    for name in ("a.tif", "b.tif"):
        (tmp_path / name).write_bytes(b"x" * 100)

    result = file_gbx._enumerate_fuse(
        str(tmp_path),
        recursive=True,
        include_hidden=False,
        need_size=False,
    )

    assert len(result) == 2
    assert all(
        r["size"] is None for r in result
    ), "size must be None when need_size=False"
    assert [os.path.basename(r["path"]) for r in result] == ["a.tif", "b.tif"]


def test_enumerate_fuse_need_size_true_returns_correct_sizes(tmp_path):
    """_enumerate_fuse(need_size=True) still returns correct byte sizes."""
    (tmp_path / "small.tif").write_bytes(b"hi")  # 2 bytes
    (tmp_path / "large.tif").write_bytes(b"x" * 99)  # 99 bytes

    result = file_gbx._enumerate_fuse(
        str(tmp_path),
        recursive=True,
        include_hidden=False,
        need_size=True,
    )

    sizes = {os.path.basename(r["path"]): r["size"] for r in result}
    assert sizes["small.tif"] == 2
    assert sizes["large.tif"] == 99
    assert all(r["file"] is None for r in result)


def test_enumerate_fuse_single_file_returns_size(tmp_path):
    """_enumerate_fuse handles a single-file path and returns correct size."""
    f = tmp_path / "tile.tif"
    f.write_bytes(b"abc")

    result = file_gbx._enumerate_fuse(
        str(f),
        recursive=True,
        include_hidden=False,
        need_size=True,
    )

    assert len(result) == 1
    assert result[0]["size"] == 3
    assert result[0]["file"] is None


def test_enumerate_fuse_single_file_need_size_false(tmp_path):
    """_enumerate_fuse single-file case: need_size=False → size None, path returned."""
    f = tmp_path / "tile.tif"
    f.write_bytes(b"xyz")

    result = file_gbx._enumerate_fuse(
        str(f),
        recursive=True,
        include_hidden=False,
        need_size=False,
    )

    assert len(result) == 1
    assert result[0]["size"] is None
    assert result[0]["path"] == str(f)


def test_enumerate_fuse_hidden_dir_pruning(tmp_path):
    """Hidden subdirectories (_staging, .tmp) are not descended when include_hidden=False."""
    hidden = tmp_path / "_staging"
    hidden.mkdir()
    (hidden / "inside.tif").write_bytes(b"x")
    (tmp_path / "visible.tif").write_bytes(b"y")

    result = file_gbx._enumerate_fuse(
        str(tmp_path),
        recursive=True,
        include_hidden=False,
    )

    paths = [os.path.basename(r["path"]) for r in result]
    assert paths == ["visible.tif"]


def test_enumerate_fuse_non_recursive_skips_subdir(tmp_path):
    """_enumerate_fuse with recursive=False returns only top-level files."""
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "deep.tif").write_bytes(b"x")
    (tmp_path / "top.tif").write_bytes(b"y")

    result = file_gbx._enumerate_fuse(
        str(tmp_path),
        recursive=False,
        include_hidden=False,
    )

    paths = [os.path.basename(r["path"]) for r in result]
    assert paths == ["top.tif"]


def test_enumerate_fuse_sorted_output(tmp_path):
    """_enumerate_fuse returns paths in sorted order regardless of fs ordering."""
    for name in ("z.tif", "a.tif", "m.tif"):
        (tmp_path / name).write_bytes(b"x")

    result = file_gbx._enumerate_fuse(
        str(tmp_path),
        recursive=True,
        include_hidden=False,
    )

    paths = [r["path"] for r in result]
    assert paths == sorted(paths)
