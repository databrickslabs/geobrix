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
    so _apply_heavy takes the heavy-tier-active branch without a real JVM. Its
    PythonUtils.toSeq stands in for Spark's list-to-Seq conversion."""
    reg = types.SimpleNamespace(set=lambda dirs, replace: None)
    ops = types.SimpleNamespace(ProjGridRegistry=reg)
    utils = types.SimpleNamespace(toSeq=lambda xs: list(xs))
    ns = types.SimpleNamespace
    return ns(
        com=ns(databricks=ns(labs=ns(gbx=ns(operations=ops)))),
        org=ns(apache=ns(spark=ns(api=ns(python=ns(PythonUtils=utils))))),
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


def test_projsync_path_is_the_payloads(monkeypatch):
    # the public accessor hands back the env wheel's projsync (None without the payload)
    from databricks.labs.gbx import _native

    monkeypatch.setattr(
        _native, "projsync_path", lambda: "/payload/_native/x86_64/bin/projsync"
    )
    assert crs_grids.projsync_path() == "/payload/_native/x86_64/bin/projsync"


def _jvm_with(reg):
    """A stand-in JVM whose ProjGridRegistry is `reg`."""
    jvm = _fake_jvm()
    jvm.com.databricks.labs.gbx.operations.ProjGridRegistry = reg
    return jvm


def test_apply_heavy_quiet_only_where_heavy_cannot_run(tmp_path, monkeypatch, caplog):
    """The heavy push is quiet when the JVM has no GeoBrix JAR (a JavaPackage) or the access mode refuses py4j (one
    INFO line), and raises on anything else; the light registration stands in every quiet case.
    """
    import logging

    from py4j.protocol import Py4JError

    class JavaPackage:  # py4j's stand-in for a class name the JVM does not have
        def set(self, *_):
            raise AssertionError("a JavaPackage is never called")

    d = str(tmp_path)
    assert crs_grids.register_proj_grids(_FakeSpark(_jvm_with(JavaPackage())), d) == [d]

    def refuse(*_):
        raise Py4JError(
            "An error occurred while calling z:com.databricks.labs.gbx.operations.ProjGridRegistry.set. Trace:\n"
            "py4j.security.Py4JSecurityException: Method public static scala.collection.immutable.Seq "
            "com.databricks.labs.gbx.operations.ProjGridRegistry.set(...) is not whitelisted on class class ..."
        )

    monkeypatch.setattr(crs_grids, "_heavy_refusal_logged", False)
    refused = _FakeSpark(_jvm_with(types.SimpleNamespace(set=refuse)))
    with caplog.at_level(logging.INFO, logger="databricks.labs.gbx.crs_grids"):
        assert crs_grids.register_proj_grids(refused, d, replace=True) == [d]
        crs_grids.register_proj_grids(refused, d, replace=True)
    lines = [r.getMessage() for r in caplog.records if "access mode" in r.getMessage()]
    assert len(lines) == 1 and "unavailable" in lines[0], lines
    assert proj_grids.get_registered_dirs() == [d]

    def broken(*_):
        raise Py4JError(
            "An error occurred while calling ProjGridRegistry.set: something else"
        )

    with pytest.raises(Py4JError, match="something else"):
        crs_grids.register_proj_grids(
            _FakeSpark(_jvm_with(types.SimpleNamespace(set=broken))), d
        )
