"""Unit tests for recursive path listing with regex filter."""

import os

import pytest

from databricks.labs.gbx.ds import _listing


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "one.tif").write_bytes(b"x")
    (tmp_path / "a" / "two.tif").write_bytes(b"x")
    (tmp_path / "a" / "skip.txt").write_bytes(b"x")
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "three.tif").write_bytes(b"x")
    return tmp_path


def test_lists_all_files_recursively_default_regex(tree):
    files = _listing.list_files(str(tree), filter_regex=".*")
    assert len(files) == 4
    assert all(os.path.isabs(f) for f in files)


def test_regex_filters_by_full_path(tree):
    files = _listing.list_files(str(tree), filter_regex=r".*\.tif$")
    assert len(files) == 3
    assert all(f.endswith(".tif") for f in files)


def test_single_file_path_returns_that_file(tree):
    target = str(tree / "a" / "one.tif")
    files = _listing.list_files(target, filter_regex=".*")
    assert files == [target]


def test_no_match_raises(tree):
    with pytest.raises(FileNotFoundError):
        _listing.list_files(str(tree), filter_regex=r".*\.nope$")


# ---------------------------------------------------------------------------
# _retry_transient unit tests
# ---------------------------------------------------------------------------


def test_retry_succeeds_after_transient_failures(monkeypatch):
    """Function that fails N-1 times then succeeds returns the success value."""
    calls = []

    def _flaky():
        calls.append(1)
        if len(calls) < 3:
            raise FileNotFoundError("transient")
        return "ok"

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    result = _listing._retry_transient(_flaky, attempts=5, backoff=0.1)
    assert result == "ok"
    assert len(calls) == 3


def test_retry_reraises_after_all_attempts_exhausted(monkeypatch):
    """Function that always raises re-raises after `attempts` tries."""
    calls = []

    def _always_fail():
        calls.append(1)
        raise FileNotFoundError("persistent")

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    with pytest.raises(FileNotFoundError, match="persistent"):
        _listing._retry_transient(_always_fail, attempts=4, backoff=0.1)
    assert len(calls) == 4


def test_retry_does_not_suppress_non_oserror(monkeypatch):
    """Non-OSError exceptions are NOT retried — propagated immediately."""
    calls = []

    def _bad():
        calls.append(1)
        raise ValueError("programming error")

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    with pytest.raises(ValueError, match="programming error"):
        _listing._retry_transient(_bad, attempts=5, backoff=0.1)
    assert len(calls) == 1  # no retry


def test_retry_sleeps_with_linear_backoff(monkeypatch):
    """Each retry sleeps backoff * attempt seconds (linear backoff)."""
    sleeps = []
    calls = []

    def _flaky():
        calls.append(1)
        if len(calls) < 4:
            raise FileNotFoundError("transient")
        return "done"

    monkeypatch.setattr(_listing.time, "sleep", lambda s: sleeps.append(s))
    _listing._retry_transient(_flaky, attempts=10, backoff=0.5)
    # 3 failures → 3 sleeps with linear backoff: 0.5, 1.0, 1.5
    assert sleeps == [0.5, 1.0, 1.5]


def test_list_files_strips_file_scheme(tree):
    """A scheme-qualified input (columns store dbfs:/file: paths) lists the same
    files as the bare path -- list_files strips the scheme before os.* resolves it."""
    bare = _listing.list_files(str(tree), filter_regex=r".*\.tif$")
    qualified = _listing.list_files("file:" + str(tree), filter_regex=r".*\.tif$")
    assert qualified == bare
