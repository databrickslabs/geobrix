"""
Tests for databricks.labs.gbx.sample (sample-data bundle helpers).

Covers get_volumes_path, get_temp_dir, _ensure_dir, _is_volume_path,
_path_exists_for_skip, _copy_final_to_volumes, _bundle_debug, download_to_path
(skip_if_exists), and run_essential_bundle / run_complete_bundle (with mocked
requests where needed) to raise coverage without requiring network or UC.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Public API from package
from databricks.labs.gbx.sample import (
    get_temp_dir,
    get_volumes_path,
    run_complete_bundle,
    run_essential_bundle,
)
# Internal helpers for coverage
from databricks.labs.gbx.sample import _bundle as _bundle_mod


# ========== __init__ (package surface) ==========


def test_sample_package_all():
    """Package __all__ exposes the expected names."""
    import databricks.labs.gbx.sample as sample

    assert hasattr(sample, "get_volumes_path")
    assert hasattr(sample, "get_temp_dir")
    assert hasattr(sample, "run_essential_bundle")
    assert hasattr(sample, "run_complete_bundle")
    assert set(sample.__all__) == {
        "get_temp_dir",
        "get_volumes_path",
        "run_complete_bundle",
        "run_essential_bundle",
    }


# ========== get_volumes_path ==========


def test_get_volumes_path():
    assert get_volumes_path("main", "default", "geobrix_samples") == (
        "/Volumes/main/default/geobrix_samples/geobrix-examples"
    )
    assert get_volumes_path("cat", "sch", "vol") == (
        "/Volumes/cat/sch/vol/geobrix-examples"
    )


# ========== get_temp_dir ==========


def test_get_temp_dir_system_temp():
    p = get_temp_dir(None)
    assert p.is_dir()
    assert "geobrix_bundle_build" in str(p)
    assert p == Path(tempfile.gettempdir()) / "geobrix_bundle_build"


def test_get_temp_dir_provided_path():
    with tempfile.TemporaryDirectory() as d:
        p = get_temp_dir(d)
        assert p == Path(d)
        assert p.is_dir()


def test_get_temp_dir_provided_path_creates_parents():
    with tempfile.TemporaryDirectory() as d:
        sub = os.path.join(d, "a", "b")
        p = get_temp_dir(sub)
        assert p == Path(sub)
        assert p.is_dir()


# ========== _unity_catalog_volume_root ==========


def test_unity_catalog_volume_root_under_volumes():
    root = _bundle_mod._unity_catalog_volume_root(Path("/Volumes/cat/sch/vol/foo/bar"))
    assert root is not None
    assert root == Path("/Volumes/cat/sch/vol")


def test_unity_catalog_volume_root_not_volumes():
    assert _bundle_mod._unity_catalog_volume_root(Path("/tmp/foo")) is None
    assert _bundle_mod._unity_catalog_volume_root(Path("relative")) is None


def test_unity_catalog_volume_root_exactly_volume():
    root = _bundle_mod._unity_catalog_volume_root(Path("/Volumes/cat/sch/vol"))
    assert root == Path("/Volumes/cat/sch/vol")


# ========== _ensure_dir ==========


def test_ensure_dir_creates_path():
    with tempfile.TemporaryDirectory() as d:
        sub = Path(d) / "a" / "b"
        _bundle_mod._ensure_dir(sub)
        assert sub.is_dir()


def test_ensure_dir_volume_root_skip():
    with tempfile.TemporaryDirectory() as d:
        vol_root = Path(d)
        _bundle_mod._ensure_dir(vol_root, volume_root=vol_root)
        assert vol_root.is_dir()


# ========== _is_volume_path ==========


def test_is_volume_path_true():
    assert _bundle_mod._is_volume_path(Path("/Volumes/cat/sch/vol/foo")) is True
    assert _bundle_mod._is_volume_path(Path("/volumes/cat/sch/vol")) is True


def test_is_volume_path_false():
    assert _bundle_mod._is_volume_path(Path("/tmp/foo")) is False
    assert _bundle_mod._is_volume_path(Path("relative")) is False
    assert _bundle_mod._is_volume_path(Path("/Volumes")) is False
    assert _bundle_mod._is_volume_path(Path("/Volumes/cat")) is False


# ========== _path_exists_for_skip ==========


def test_path_exists_for_skip_missing():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "nonexistent"
        exists, size_mb = _bundle_mod._path_exists_for_skip(p)
    assert exists is False
    assert size_mb is None


def test_path_exists_for_skip_exists():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"hello")
        f.flush()
        path = Path(f.name)
    try:
        exists, size_mb = _bundle_mod._path_exists_for_skip(path)
        assert exists is True
        assert size_mb is not None
        assert size_mb >= 0
    finally:
        path.unlink(missing_ok=True)


# ========== _copy_final_to_volumes ==========


def test_copy_final_to_volumes():
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src.txt"
        src.write_text("content")
        dest = Path(tmp) / "out" / "dest.txt"
        result = _bundle_mod._copy_final_to_volumes(src, dest, "Test copy")
        assert result == dest
        assert dest.read_text() == "content"


def test_copy_final_to_volumes_skip_if_exists(capsys):
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "existing.txt"
        dest.write_text("already")
        src = Path(tmp) / "new.txt"
        src.write_text("new")
        result = _bundle_mod._copy_final_to_volumes(src, dest, "Test skip")
        assert result == dest
        assert dest.read_text() == "already"
    out = capsys.readouterr().out
    assert "already exists" in out or "MB" in out


# ========== _bundle_debug ==========


def test_bundle_debug_no_env(capsys):
    with patch.dict(os.environ, {}, clear=False):
        if "GBX_BUNDLE_DEBUG" in os.environ:
            del os.environ["GBX_BUNDLE_DEBUG"]
        _bundle_mod._bundle_debug("msg")
    assert "[bundle]" not in capsys.readouterr().out


def test_bundle_debug_with_env(capsys):
    with patch.dict(os.environ, {"GBX_BUNDLE_DEBUG": "1"}):
        _bundle_mod._bundle_debug("test message")
    assert "[bundle] test message" in capsys.readouterr().out


# ========== download_to_path (skip_if_exists, no network) ==========


def test_download_to_path_skip_if_exists():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
        f.write(b"existing")
        f.flush()
        path = Path(f.name)
    try:
        result = _bundle_mod.download_to_path(
            "https://example.com/file.bin",
            path,
            "Existing file",
            skip_if_exists=True,
            quiet=True,
        )
        assert result == path
        assert path.read_bytes() == b"existing"
    finally:
        path.unlink(missing_ok=True)


def test_download_to_path_requires_requests_when_download_needed():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "new.bin"
        with patch.object(_bundle_mod, "requests", None):
            with pytest.raises(RuntimeError, match="requests is required"):
                _bundle_mod.download_to_path(
                    "https://example.com/file.bin",
                    path,
                    "Need download",
                    skip_if_exists=False,
                    quiet=True,
                )


# ========== run_essential_bundle ==========


@pytest.mark.integration
def test_run_essential_bundle_returns_dict_shape():
    """Run with temp path; may hit network errors but must return dict with expected keys."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d) / "geobrix-examples"
        result = run_essential_bundle(str(base), temp_dir=d)
    assert "errors" in result
    assert "file_count" in result
    assert "total_size_mb" in result
    assert isinstance(result["errors"], list)
    assert isinstance(result["file_count"], int)
    assert isinstance(result["total_size_mb"], (int, float))


# ========== run_complete_bundle ==========


@pytest.mark.integration
def test_run_complete_bundle_returns_dict_shape():
    """Run with temp path; may hit network errors but must return dict with expected keys."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d) / "geobrix-examples"
        result = run_complete_bundle(str(base), temp_dir=d)
    assert "errors" in result
    assert "file_count" in result
    assert "total_size_mb" in result
    assert isinstance(result["errors"], list)
    assert isinstance(result["file_count"], int)
    assert isinstance(result["total_size_mb"], (int, float))
