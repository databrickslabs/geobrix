import os
import tempfile
from pathlib import Path

import pytest

from databricks.labs.gbx.pyrx.core.local_temp import (
    gc_stale_local_temp,
    local_temp_root,
    new_local_temp_dir,
    new_local_temp_file,
)


def test_root_defaults_to_gettempdir(monkeypatch):
    monkeypatch.delenv("GBX_LOCAL_TEMP_DIR", raising=False)
    assert local_temp_root() == tempfile.gettempdir()


def test_root_honors_env(monkeypatch, tmp_path):
    target = tmp_path / "gbxtmp"
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(target))
    root = local_temp_root()
    assert root == str(target)
    assert os.path.isdir(root)  # created


def test_root_sanitizes_whitespace_and_bom(monkeypatch, tmp_path):
    target = tmp_path / "gbxtmp"
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", f"﻿  {target}  ​")
    assert local_temp_root() == str(target)


def test_root_empty_after_strip_falls_back(monkeypatch):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", "  ﻿ ​ ")
    assert local_temp_root() == tempfile.gettempdir()


def test_root_unusable_env_warns_and_falls_back(monkeypatch, tmp_path):
    # A path whose parent is a file cannot be made into a dir.
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(blocker / "sub"))
    with pytest.warns(RuntimeWarning):
        assert local_temp_root() == tempfile.gettempdir()


def test_new_local_temp_dir_creates_unique_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    a = new_local_temp_dir("gbx_x_")
    b = new_local_temp_dir("gbx_x_")
    assert a != b
    assert os.path.isdir(a) and os.path.isdir(b)
    assert Path(a).parent == tmp_path and os.path.basename(a).startswith("gbx_x_")


def test_new_local_temp_file_creates_empty_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    p = new_local_temp_file(suffix=".nc", prefix="gbx_y_")
    assert os.path.isfile(p) and os.path.getsize(p) == 0
    assert p.endswith(".nc") and os.path.basename(p).startswith("gbx_y_")
    assert Path(p).parent == tmp_path
    # fd not leaked: we can reopen and write
    with open(p, "wb") as f:
        f.write(b"data")
    assert os.path.getsize(p) == 4


def test_gc_stale_local_temp_uses_root_and_ages(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    stale = new_local_temp_dir("gbx_gc_")
    old = __import__("time").time() - 10_000
    os.utime(stale, (old, old))
    fresh = new_local_temp_dir("gbx_gc_")
    gc_stale_local_temp("gbx_gc_", ttl_seconds=3600)
    assert not os.path.exists(stale)
    assert os.path.exists(fresh)


def test_gc_stale_local_temp_missing_root_no_raise(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "does_not_exist"))
    # resolver will create it; removing then GC'ing must not raise
    root = local_temp_root()
    os.rmdir(root)
    gc_stale_local_temp("gbx_gc_")  # no raise
