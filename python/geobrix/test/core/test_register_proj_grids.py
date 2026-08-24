import types

import pytest

from databricks.labs.gbx import crs_grids
from databricks.labs.gbx.core import proj_grids


@pytest.fixture(autouse=True)
def _clear():
    proj_grids.set_registered_dirs([], replace=True)
    yield
    proj_grids.set_registered_dirs([], replace=True)


def _fake_jvm():
    """A minimal stand-in for spark._jvm whose ProjGridRegistry.set() is a no-op,
    so _apply_heavy takes the heavy-tier-active branch without a real JVM."""
    reg = types.SimpleNamespace(set=lambda dirs, replace: None)
    ops = types.SimpleNamespace(ProjGridRegistry=reg)
    return types.SimpleNamespace(
        com=types.SimpleNamespace(
            databricks=types.SimpleNamespace(
                labs=types.SimpleNamespace(gbx=types.SimpleNamespace(operations=ops))
            )
        )
    )


class _FakeSpark:
    def __init__(self, jvm):
        self._jvm = jvm


def test_records_dirs_and_returns_list(tmp_path):
    d = tmp_path / "grids"
    d.mkdir()
    (d / "OSTN15.gsb").write_bytes(b"\x00")
    out = crs_grids.register_proj_grids(spark=None, dirs=str(d))
    assert out == [str(d)]
    assert proj_grids.get_registered_dirs() == [str(d)]


def test_missing_dir_warns_but_records(recwarn):
    out = crs_grids.register_proj_grids(spark=None, dirs="/nope/not/here")
    assert out == ["/nope/not/here"]
    assert any("not" in str(w.message).lower() for w in recwarn.list)


def test_top_level_reexport():
    import databricks.labs.gbx as gbx

    assert gbx.register_proj_grids is crs_grids.register_proj_grids


def test_replace_true_resets(tmp_path):
    crs_grids.register_proj_grids(spark=None, dirs="/Volumes/a")
    out = crs_grids.register_proj_grids(spark=None, dirs="/Volumes/b", replace=True)
    assert out == ["/Volumes/b"]


def test_warn_heavy_volume_grids_fires_on_volume(recwarn):
    # Heavyweight tier cannot read a Volume-hosted grid → warn names the Volume dir(s).
    crs_grids._warn_heavy_volume_grids(["/Volumes/c/s/proj-grids", "/local/grids"])
    msgs = [str(w.message) for w in recwarn.list]
    assert any("heavyweight tier" in m and "/Volumes/c/s/proj-grids" in m for m in msgs)
    # ...and does NOT name the local dir as a problem.
    assert not any("/local/grids" in m for m in msgs)


def test_warn_heavy_volume_grids_silent_on_local(recwarn):
    crs_grids._warn_heavy_volume_grids(["/local/grids", "/tmp/g", "dbfs:/mnt/g"])
    assert recwarn.list == []


def test_apply_heavy_warns_on_volume_when_jvm_present(recwarn):
    # JVM present (heavy active) + a Volume dir → the heavy-volume warning fires.
    crs_grids._apply_heavy(_FakeSpark(_fake_jvm()), ["/Volumes/c/s/proj-grids"])
    assert any("heavyweight tier" in str(w.message) for w in recwarn.list)


def test_apply_heavy_silent_when_no_jvm(recwarn):
    # Light-only session (spark._jvm is None) → no heavy warning even for a Volume dir.
    crs_grids._apply_heavy(None, ["/Volumes/c/s/proj-grids"])
    assert not any("heavyweight tier" in str(w.message) for w in recwarn.list)


def test_register_proj_grids_light_only_no_heavy_warn(recwarn):
    # register_proj_grids on a light-only session must not emit the heavy-volume warning.
    crs_grids.register_proj_grids(spark=None, dirs="/Volumes/c/s/proj-grids")
    assert not any("heavyweight tier" in str(w.message) for w in recwarn.list)
