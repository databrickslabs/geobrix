"""Isolation + self-GC behavior of the two-phase light-writer scratch dirs.

Covers ds/_scratch.py (unique namespacing, age-based GC, never-raise contract)
and the reader-side guarantee that a writer's hidden scratch container is never
enumerated as input (so concurrent jobs / multiple users can't corrupt or
re-ingest one another's in-flight fragments).
"""

from __future__ import annotations

import os
import time

from databricks.labs.gbx.ds import _scratch
from databricks.labs.gbx.ds.vector import VectorGbxReader


# --------------------------------------------------------------------------- #
# new_scratch_dir
# --------------------------------------------------------------------------- #
def test_new_scratch_dir_is_under_hidden_container(tmp_path):
    p = _scratch.new_scratch_dir(str(tmp_path))
    parent, leaf = os.path.split(p)
    assert os.path.basename(parent) == _scratch.SCRATCH_CONTAINER
    assert os.path.dirname(parent) == str(tmp_path)
    assert len(leaf) == 32  # uuid4().hex


def test_new_scratch_dir_is_unique_per_call(tmp_path):
    a = _scratch.new_scratch_dir(str(tmp_path))
    b = _scratch.new_scratch_dir(str(tmp_path))
    assert a != b


def test_new_scratch_dir_does_not_create(tmp_path):
    p = _scratch.new_scratch_dir(str(tmp_path))
    assert not os.path.exists(p)  # caller creates it lazily


# --------------------------------------------------------------------------- #
# remove_scratch_dir (subdir removal + empty-container pruning)
# --------------------------------------------------------------------------- #
def test_remove_scratch_dir_prunes_empty_container(tmp_path):
    """A committed singleFile write must leave NO .gbx_scratch behind."""
    sub = _scratch.new_scratch_dir(str(tmp_path))
    os.makedirs(sub, exist_ok=True)
    with open(os.path.join(sub, "frag.arrow"), "w") as f:
        f.write("x")
    container = os.path.join(str(tmp_path), _scratch.SCRATCH_CONTAINER)
    assert os.path.isdir(container)

    _scratch.remove_scratch_dir(sub)

    assert not os.path.exists(sub)  # per-write subdir gone
    assert not os.path.exists(container)  # empty container pruned, no stray dir


def test_remove_scratch_dir_keeps_container_with_live_sibling(tmp_path):
    """A concurrent in-flight write's subdir must keep the container alive."""
    a = _scratch.new_scratch_dir(str(tmp_path))
    b = _scratch.new_scratch_dir(str(tmp_path))
    for d in (a, b):
        os.makedirs(d, exist_ok=True)
    container = os.path.join(str(tmp_path), _scratch.SCRATCH_CONTAINER)

    _scratch.remove_scratch_dir(a)  # commit one write

    assert not os.path.exists(a)  # this write's subdir gone
    assert os.path.exists(b)  # concurrent write untouched
    assert os.path.isdir(container)  # container kept (not empty)


def test_remove_scratch_dir_empty_path_is_noop(tmp_path):
    _scratch.remove_scratch_dir("")  # parts mode passes "" -- must not raise


# --------------------------------------------------------------------------- #
# gc_stale_scratch
# --------------------------------------------------------------------------- #
def _mk(container: str, name: str, age_seconds: float) -> str:
    sub = os.path.join(container, name)
    os.makedirs(sub, exist_ok=True)
    with open(os.path.join(sub, "frag.arrow"), "w") as f:
        f.write("x")
    when = time.time() - age_seconds
    os.utime(sub, (when, when))
    return sub


def test_gc_removes_stale_keeps_fresh(tmp_path):
    container = os.path.join(str(tmp_path), _scratch.SCRATCH_CONTAINER)
    stale = _mk(container, "stale", _scratch.DEFAULT_STALE_TTL_SECONDS + 3600)
    fresh = _mk(container, "fresh", 5)  # an in-flight concurrent write

    _scratch.gc_stale_scratch(str(tmp_path))

    assert not os.path.exists(stale)  # orphan reclaimed
    assert os.path.exists(fresh)  # live concurrent write untouched


def test_gc_respects_custom_ttl(tmp_path):
    container = os.path.join(str(tmp_path), _scratch.SCRATCH_CONTAINER)
    sub = _mk(container, "sub", 120)
    _scratch.gc_stale_scratch(str(tmp_path), ttl_seconds=60)
    assert not os.path.exists(sub)


def test_gc_missing_container_is_noop(tmp_path):
    # no .gbx_scratch under tmp_path -- must not raise
    _scratch.gc_stale_scratch(str(tmp_path))


def test_gc_local_temp_removes_stale_only(tmp_path, monkeypatch):
    import tempfile

    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    old = os.path.join(str(tmp_path), "gbx_vecout_old")
    new = os.path.join(str(tmp_path), "gbx_vecout_new")
    other = os.path.join(str(tmp_path), "unrelated_dir")
    for d, age in (
        (old, _scratch.DEFAULT_STALE_TTL_SECONDS + 100),
        (new, 5),
        (other, _scratch.DEFAULT_STALE_TTL_SECONDS + 100),
    ):
        os.makedirs(d)
        when = time.time() - age
        os.utime(d, (when, when))

    _scratch.gc_stale_local_temp("gbx_vecout_")

    assert not os.path.exists(old)  # stale, matching prefix -> gone
    assert os.path.exists(new)  # fresh -> kept
    assert os.path.exists(other)  # non-matching prefix -> never touched


# --------------------------------------------------------------------------- #
# reader never enumerates a writer's scratch container
# --------------------------------------------------------------------------- #
def test_reader_skips_hidden_and_marker_scratch_dirs(tmp_path):
    # one real input file
    real = tmp_path / "roads.geojson"
    real.write_text('{"type":"FeatureCollection","features":[]}')

    # an in-flight writer scratch (new hidden container) with a frag that
    # happens to share the reader's extension
    hidden = tmp_path / _scratch.SCRATCH_CONTAINER / "abc123"
    hidden.mkdir(parents=True)
    (hidden / "frag.geojson").write_text("{}")

    # a legacy underscore-prefixed scratch dir
    legacy = tmp_path / "_vec_scratch_deadbeef"
    legacy.mkdir()
    (legacy / "frag.geojson").write_text("{}")

    reader = VectorGbxReader({"path": str(tmp_path), "driverName": "GeoJSON"})
    members = reader._members()

    assert members == [str(real)]  # scratch fragments excluded from input
