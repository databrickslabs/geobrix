"""The UC build probe (bench.uc_build_probe): a UC function reports the geobrix build it imports, and the install fails
when that is not the build of the wheel file at the path the function names. Nothing here touches Databricks.
"""

import json
import re
import types
import zipfile

import pytest

from databricks.labs.gbx import uc
from databricks.labs.gbx.bench import uc_build_probe as probe

FILES = {"__init__.py": "x = 1\n", "a.py": "A = 1\n", "sub/b.py": "B = 2\n"}


def _info(sha, dirty=False):
    return 'VERSION = "0.6.0+env6"\nGIT_SHA = "%s"\nGIT_DIRTY = %s\n' % (sha, dirty)


def _wheel(path, sha, files=FILES, dirty=False):
    with zipfile.ZipFile(path, "w") as z:
        for rel, text in {**files, "_build_info.py": _info(sha, dirty)}.items():
            z.writestr(probe.PKG_PREFIX + rel, text)
        z.writestr(
            probe.PKG_PREFIX + "_vendor/skipped.py", "V = 1\n"
        )  # left out of the identity
        z.writestr("geobrix-0.6.0.dist-info/METADATA", "Name: geobrix\n")
    return str(path)


def _installed(tmp_path, sha, files=FILES):
    """An unpacked copy of a wheel: the tree a UC sandbox would import."""
    root = tmp_path / "site" / "databricks" / "labs" / "gbx"
    for rel, text in {**files, "_build_info.py": _info(sha)}.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    (root / "__pycache__").mkdir(exist_ok=True)
    (root / "__pycache__" / "junk.py").write_text("J = 1\n")
    return root


def _run_sandbox(monkeypatch, root):
    """Run the shipped source with `import databricks.labs.gbx` pointed at `root`; return its handler's JSON."""
    import importlib

    mod = types.SimpleNamespace(__file__=str(root / "__init__.py"))
    real = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name, *a: mod if name == "databricks.labs.gbx" else real(name, *a),
    )
    ns = {}
    exec(compile(probe.sandbox_source(), "sandbox", "exec"), ns)
    return json.loads(ns["handler"]())


def test_the_function_and_the_wheel_give_one_identity_for_one_build(
    tmp_path, monkeypatch
):
    wheel = _wheel(tmp_path / "w.whl", "a" * 40)
    got = _run_sandbox(monkeypatch, _installed(tmp_path, "a" * 40))
    want = probe.wheel_identity(wheel)
    assert (got["git_sha"], got["dirty"], got["digest"]) == (
        want["git_sha"],
        want["dirty"],
        want["digest"],
    )
    assert probe.problems(got, want) == []


def test_an_older_build_under_the_same_name_is_reported(tmp_path, monkeypatch):
    """Fail on revert: the staged wheel changed a file and its commit; the function still imports the old tree."""
    new = probe.wheel_identity(
        _wheel(tmp_path / "w.whl", "b" * 40, {**FILES, "a.py": "A = 2\n"})
    )
    old = _run_sandbox(monkeypatch, _installed(tmp_path, "a" * 40))
    bad = probe.problems(old, new)
    assert any(m.startswith("git_sha") for m in bad) and any(
        m.startswith("digest") for m in bad
    )


def test_a_function_that_cannot_import_geobrix_is_a_problem():
    assert "could not import" in probe.problems({"error": "ImportError: no"}, {})[0]


def test_the_probe_declares_the_environment_the_installed_functions_declare():
    folder = "/Volumes/c/s/v/f"
    wheel = f"{folder}/{uc._env_wheel_name()}"
    stmts = uc.install("c", "s", folder, dry_run=True)

    def spec(text):
        return re.search(
            r"ENVIRONMENT \(dependencies = (.*), environment_version = '6'\)", text
        ).group(1)

    product = next(
        s for s in stmts if "LANGUAGE PYTHON" in s and "ENVIRONMENT (dependencies" in s
    )
    from databricks.labs.gbx import __version__, manifest

    ddl_text = probe.probe_ddl(
        "c", "s", wheel, manifest.load().uc_dependencies, __version__.split("+", 1)[0]
    )
    assert spec(ddl_text) == spec(product)  # one spec, so one environment
    assert "`c`.`s`.gbx_probe_build()" in ddl_text and "HANDLER 'handler'" in ddl_text
    assert "$$" not in probe.sandbox_source()


class _Spark:
    def __init__(self, reply):
        self.reply, self.calls = reply, []

    def sql(self, text):
        self.calls.append(text)
        return self

    def collect(self):
        return [(json.dumps(self.reply),)]


def test_the_install_check_passes_for_the_staged_build_and_fails_for_another(
    tmp_path, monkeypatch
):
    wheel = _wheel(tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl", "c" * 40)
    monkeypatch.setattr(uc, "_single_wheel", lambda folder: wheel)
    want = probe.wheel_identity(wheel)
    ok = _Spark({k: want[k] for k in ("git_sha", "dirty", "digest")})
    said = []
    assert (
        probe.assert_installed_build(ok, "c", "s", "/Volumes/c/s/v/f", say=said.append)[
            "git_sha"
        ]
        == "c" * 40
    )
    assert (
        ok.calls[0].startswith("CREATE OR REPLACE FUNCTION `c`.`s`.gbx_probe_build()")
        and "SELECT" in ok.calls[1]
    )
    assert said and "commit cccccccccccc" in said[0]
    stale = _Spark(
        {
            **{k: want[k] for k in ("git_sha", "dirty", "digest")},
            "git_sha": "d" * 40,
            "digest": "0" * 64,
        }
    )
    with pytest.raises(
        RuntimeError,
        match="another geobrix build.*Fix: stage the wheel into a NEW folder",
    ):
        probe.assert_installed_build(
            stale, "c", "s", "/Volumes/c/s/v/f", say=lambda m: None
        )


class _FailingSelect(_Spark):
    def collect(self):
        raise RuntimeError("the function could not start")


def test_the_install_check_drops_its_probe_function_whatever_the_outcome(
    tmp_path, monkeypatch
):
    """Fail on revert: the probe left gbx_probe_build in the schema next to the product functions."""
    wheel = _wheel(tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl", "c" * 40)
    monkeypatch.setattr(uc, "_single_wheel", lambda folder: wheel)
    want = probe.wheel_identity(wheel)
    drop = "DROP FUNCTION IF EXISTS `c`.`s`.gbx_probe_build"
    ok = _Spark({k: want[k] for k in ("git_sha", "dirty", "digest")})
    probe.assert_installed_build(ok, "c", "s", "/Volumes/c/s/v/f", say=lambda m: None)
    assert ok.calls[-1] == drop
    stale = _Spark(
        {**{k: want[k] for k in ("git_sha", "dirty", "digest")}, "git_sha": "d" * 40}
    )
    with pytest.raises(RuntimeError, match="another geobrix build"):
        probe.assert_installed_build(
            stale, "c", "s", "/Volumes/c/s/v/f", say=lambda m: None
        )
    assert stale.calls[-1] == drop
    broken = _FailingSelect({})
    with pytest.raises(RuntimeError, match="could not start"):
        probe.assert_installed_build(
            broken, "c", "s", "/Volumes/c/s/v/f", say=lambda m: None
        )
    assert broken.calls[-1] == drop
