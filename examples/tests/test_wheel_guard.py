"""The wheel guard fails a run whose imported geobrix is not the staged wheel (no workspace, no Spark).

Run: gbx:test:notebooks --path test_wheel_guard.py  (or pytest examples/tests/test_wheel_guard.py)
"""

import json
import sys
import types
import zipfile

import pytest

import _wheel_guard as g

NEW_SHA, OLD_SHA = "a" * 40, "b" * 40


def _files(tag):
    return {
        "__init__.py": "VERSION = '0.6.0'\n",
        "pyrx/core/focal.py": "def focal():\n    return %r\n" % tag,
        "_vendor/x86_64/rasterio.py": "vendored %s\n" % tag,  # left out of the identity
    }


def _info(sha, dirty=False):
    return 'ENV = 6\nVERSION = "0.6.0+env6"\nGIT_SHA = "%s"\nGIT_DIRTY = %s\n' % (sha, dirty)


def _wheel(tmp_path, name, tag, sha, dirty=False):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as z:
        for rel, text in {**_files(tag), "_build_info.py": _info(sha, dirty)}.items():
            z.writestr(g.PKG_PREFIX + rel, text)
        z.writestr("geobrix-0.6.0+env6.dist-info/METADATA", "Name: geobrix\n")
    return str(path)


def _install(tmp_path, monkeypatch, tag, sha, dirty=False, where="site"):
    """Fake the import of databricks.labs.gbx from a tree holding wheel `tag`'s files."""
    root = tmp_path / where / "databricks" / "labs" / "gbx"
    for rel, text in {**_files(tag), "_build_info.py": _info(sha, dirty)}.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    mod = types.ModuleType("databricks.labs.gbx")
    mod.__file__ = str(root / "__init__.py")
    for name in ("databricks", "databricks.labs"):
        monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "databricks.labs.gbx", mod)
    return root


def _says():
    lines = []
    return lines, lines.append


def test_the_wheel_and_the_installed_tree_have_one_identity_and_vendored_files_do_not_count(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "new", NEW_SHA)
    w, i = g.wheel_identity(whl), g.installed_identity()
    assert w["digest"] == i["digest"] and w["git_sha"] == i["git_sha"] == NEW_SHA and w["dirty"] is False
    assert sorted(i["files"]) == ["__init__.py", "_build_info.py", "pyrx/core/focal.py"]


def test_the_guard_passes_when_the_driver_and_the_workers_import_the_staged_wheel(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "new", NEW_SHA)
    lines, say = _says()
    v = g.run_guard({"wheel": whl, "git_sha": NEW_SHA}, say=say, workers=lambda: [json.loads(g.worker_identity())])
    assert v["ok"] and any("WHEEL GUARD ok: driver and 1 worker" in x for x in lines)
    assert any("module=" in x and NEW_SHA[:12] in x for x in lines)  # the commit and the module path are printed


def test_an_old_same_version_geobrix_on_the_driver_fails_the_run_with_a_fix(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "old", OLD_SHA)  # the cluster library: same version, old bytes
    lines, say = _says()
    with pytest.raises(RuntimeError) as e:
        g.run_guard({"wheel": whl, "git_sha": NEW_SHA}, say=say, workers=lambda: [json.loads(g.worker_identity())])
    msg = str(e.value)
    assert "WHEEL GUARD FAILED" in msg and "driver imports build commit bbbbbbbbbbbb" in msg and "is commit aaaaaaaaaaaa" in msg
    assert "source digest" in msg and "pyrx/core/focal.py" in msg  # the file that differs is named
    assert "Fix: another geobrix of the same version" in msg and "Uninstall that library" in msg
    assert any("WHEEL GUARD driver: module=" in x and "bbbbbbbbbbbb" in x for x in lines)


def test_a_stale_worker_fails_the_run_though_the_driver_is_right(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "new", NEW_SHA)
    stale = {"module": "/databricks/python3/lib/databricks/labs/gbx/__init__.py", "git_sha": OLD_SHA, "digest": "0" * 64,
             "dirty": False, "host": "10.0.0.7"}
    _, say = _says()
    with pytest.raises(RuntimeError) as e:
        g.run_guard({"wheel": whl}, say=say, workers=lambda: [json.loads(g.worker_identity()), stale])
    msg = str(e.value)
    assert "worker 10.0.0.7 imports build commit bbbbbbbbbbbb" in msg and "driver imports" not in msg


def test_a_dirty_flag_a_missing_import_and_an_unreadable_wheel_all_fail(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA, dirty=False)
    _install(tmp_path, monkeypatch, "new", NEW_SHA, dirty=True)
    _, say = _says()
    with pytest.raises(RuntimeError, match="dirty=True, the staged wheel has dirty=False"):
        g.run_guard({"wheel": whl}, say=say)
    monkeypatch.delitem(sys.modules, "databricks.labs.gbx")
    monkeypatch.setattr(sys, "meta_path", [])  # nothing can be imported
    with pytest.raises(RuntimeError, match="driver cannot import geobrix"):
        g.run_guard({"wheel": whl}, say=say)
    with pytest.raises(RuntimeError, match="cannot read the staged wheel"):
        g.run_guard({"wheel": str(tmp_path / "gone.whl")}, say=say)


def test_the_launchers_commit_must_agree_with_the_wheel_it_staged(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "new", NEW_SHA)
    _, say = _says()
    with pytest.raises(RuntimeError, match="the launcher staged git_sha bbbbbbbbbbbb but the wheel"):
        g.run_guard({"wheel": whl, "git_sha": OLD_SHA}, say=say)


def test_with_nothing_staged_the_identity_is_printed_and_the_run_goes_on(tmp_path, monkeypatch):
    _install(tmp_path, monkeypatch, "new", NEW_SHA)
    lines, say = _says()
    assert g.run_guard(None, say=say)["ok"] and any("nothing to compare with" in x for x in lines)


def test_the_cell_source_is_self_contained_and_runs_as_the_notebook_runs_it(tmp_path, monkeypatch):
    whl = _wheel(tmp_path, "new.whl", "new", NEW_SHA)
    _install(tmp_path, monkeypatch, "old", OLD_SHA)
    src = g.guard_source(wheel=whl, git_sha=NEW_SHA, workers=False)
    assert "import _wheel_guard" not in src and "WHEEL_GUARD = run_guard(" in src
    with pytest.raises(RuntimeError, match="WHEEL GUARD FAILED"):
        exec(compile(src, "guard-cell", "exec"), {"__name__": "__main__"})
    _install(tmp_path, monkeypatch, "new", NEW_SHA, where="site2")
    env = {"__name__": "__main__"}
    exec(compile(src, "guard-cell", "exec"), env)
    assert env["WHEEL_GUARD"]["ok"]


def test_every_geobrix_pip_line_gets_a_forced_install_first_and_a_forced_one_is_left_alone():
    w = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"
    forced = '%pip install --force-reinstall --no-deps "geobrix @ file://' + w + '"\n'
    lines = ['%pip install "geobrix[stac] @ file://' + w + '"\n', "dbutils.library.restartPython()"]
    out, n = g.force_pip_lines(lines)
    assert n == 1 and out == [forced, lines[0], lines[1]]
    assert g.force_pip_lines(out) == (out, 0)  # a second pass adds nothing
    other = ['%pip install "overturemaps>=1.0.2,<2"\n', forced]
    assert g.force_pip_lines(other) == (other, 0)
    assert g.pip_wheel_of(lines) == w and g.pip_wheel_of(other[:1]) is None


# ---- the notebook runner: what the uploaded copy of a notebook gets

import base64
import pathlib

import run_notebooks_serverless as runner

WHL = "/Volumes/c/s/v/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl"


def _nb(tmp_path, *cells):
    path = tmp_path / "probe.ipynb"
    path.write_text(json.dumps({"cells": [{"cell_type": "code", "metadata": {}, "outputs": [], "source": c} for c in cells],
                                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}))
    return path


@pytest.fixture(autouse=True)
def _no_sdk(monkeypatch):
    """The upload imports ImportFormat from the SDK; the tests need only its name."""
    fake = types.ModuleType("databricks.sdk.service.workspace")
    fake.ImportFormat = types.SimpleNamespace(JUPYTER="JUPYTER")
    for name in ("databricks.sdk", "databricks.sdk.service"):
        monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.workspace", fake)


def _uploaded(path, strip_pip, wheel_guard=True, guard=True, report_only=False):
    sent = {}
    w = types.SimpleNamespace(workspace=types.SimpleNamespace(
        mkdirs=lambda p: None, import_=lambda path, format, content, overwrite: sent.update(nb=json.loads(base64.b64decode(content)))))
    runner.WHEEL_GUARD.update(on=wheel_guard, wheel="/Volumes/c/s/v/default.whl", report_only_without_pip=report_only)
    try:
        runner._import_notebook(w, pathlib.Path(path), "/Users/x/y", strip_pip, guard=guard)
    finally:
        runner.WHEEL_GUARD.update(on=False, wheel=None, report_only_without_pip=False)
    return ["".join(c["source"]) if not isinstance(c["source"], str) else c["source"] for c in sent["nb"]["cells"]]


def test_a_classic_cluster_run_forces_the_notebooks_wheel_in_and_guards_it_after_the_restart(tmp_path):
    nb = _nb(tmp_path, '%pip install "geobrix[stac] @ file://' + WHL + '"\ndbutils.library.restartPython()', "import databricks.labs.gbx\n")
    cells = _uploaded(nb, strip_pip=False)
    assert cells[0].startswith('%pip install --force-reinstall --no-deps "geobrix @ file://' + WHL + '"\n%pip install "geobrix[stac]')
    assert "run_guard(" in cells[1] and ("'wheel': '" + WHL + "'") in cells[1]  # the wheel the notebook installs, not the default
    assert cells[2] == "import databricks.labs.gbx\n"


def test_a_serverless_run_has_no_pip_cell_and_the_guard_comes_first_with_the_environment_wheel(tmp_path):
    nb = _nb(tmp_path, '%pip install "geobrix @ file://' + WHL + '"', "import databricks.labs.gbx\n")
    cells = _uploaded(nb, strip_pip=True)
    assert len(cells) == 2 and "run_guard(" in cells[0] and "'wheel': '/Volumes/c/s/v/default.whl'" in cells[0]
    assert cells[1] == "import databricks.labs.gbx\n"


def test_the_guard_can_be_switched_off_and_a_run_dependency_notebook_never_gets_it(tmp_path):
    nb = _nb(tmp_path, '%pip install "geobrix @ file://' + WHL + '"', "x = 1\n")
    assert len(_uploaded(nb, strip_pip=False, wheel_guard=False)) == 2
    assert len(_uploaded(nb, strip_pip=False, guard=False)) == 2


def test_an_existing_cluster_with_no_wheel_given_and_no_pip_cell_only_reports_what_is_imported(tmp_path):
    nb = _nb(tmp_path, "x = 1\n")  # a notebook with no %pip of geobrix: the cluster's own library is what it uses
    cells = _uploaded(nb, strip_pip=False, report_only=True)
    assert "run_guard({}," in cells[0] or "run_guard({}, spark=spark)" in cells[0]  # nothing expected: report only
    cells = _uploaded(nb, strip_pip=False, report_only=False)  # --wheel given (or a new cluster): that wheel is expected
    assert "'wheel': '/Volumes/c/s/v/default.whl'" in cells[0]


# ---- the shard runner: the checkout's package must be what the driver and the workers import after the overlay


def _checkout(tmp_path, tag, extra=None):
    root = tmp_path / "checkout" / "src" / "databricks" / "labs" / "gbx"
    for rel, text in {**_files(tag), **(extra or {})}.items():
        if rel.startswith("_vendor"):
            continue
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    return root


def test_the_checkout_must_be_what_is_imported_after_the_overlay_not_the_wheels_older_copy(tmp_path, monkeypatch):
    import shutil

    checkout = _checkout(tmp_path, "checkout")
    installed = _install(tmp_path, monkeypatch, "wheel", NEW_SHA)  # the wheel's copy: another build, plus its _build_info.py
    _, say = _says()
    expected = {"tree": str(checkout)}
    workers = lambda: [json.loads(g.worker_identity(("_build_info.py",)))]  # noqa: E731
    # before the overlay the installed package is the wheel's: the shard would test the wheel's code
    with pytest.raises(RuntimeError) as e:
        g.run_guard(expected, say=say, workers=workers, exclude=("_build_info.py",))
    assert "pyrx/core/focal.py" in str(e.value) and "not the checkout's" in str(e.value) and "worker" in str(e.value)
    # the overlay (the runner's copytree) makes the checkout win, whatever the wheel's _build_info.py says
    shutil.copytree(checkout, installed, dirs_exist_ok=True)
    verdict = g.run_guard(expected, say=say, workers=workers, exclude=("_build_info.py",))
    assert verdict["ok"]
    # a module the checkout dropped but the wheel kept (the runner deletes it) would also fail the check
    (installed / "pyrx" / "core" / "gone.py").write_text("x = 1\n")
    with pytest.raises(RuntimeError, match="gone.py"):
        g.run_guard(expected, say=say, workers=workers, exclude=("_build_info.py",))


def test_a_worker_that_still_holds_the_wheels_copy_fails_the_checkout_check(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path, "checkout")
    _install(tmp_path, monkeypatch, "checkout", NEW_SHA)  # the driver is right
    _, say = _says()
    stale = {"module": "/cluster_libraries/databricks/labs/gbx/__init__.py", "git_sha": OLD_SHA, "digest": "0" * 64, "host": "w1"}
    with pytest.raises(RuntimeError) as e:
        g.run_guard({"tree": str(checkout)}, say=say, workers=lambda: [stale], exclude=("_build_info.py",))
    assert "worker w1 imports source digest" in str(e.value) and "driver imports" not in str(e.value)
