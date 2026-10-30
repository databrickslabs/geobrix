"""Tests for the JAR-packaging guard in examples/tests/stage_jar_to_volume.py.

The guard refuses a JAR before any upload. Its message must say why: a JAR that bundles classes
the runtime provides needs a pom.xml fix, one that is missing or not a zip file needs a rebuild.
No Maven, no Docker and no workspace: the container build step is stubbed and the real
check_jar_packaging.py runs on JARs made here.
"""

import importlib
import os
import shutil
import subprocess
import sys
import types
import zipfile
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
JAR_NAME = "geobrix-0.0.2-jar-with-dependencies.jar"


@pytest.fixture
def stage(tmp_path, monkeypatch):
    """main() of stage_jar_to_volume, run against a scratch project tree with no upload."""
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    monkeypatch.setenv("GBX_BUNDLE_SKIP_JAR_UPLOAD", "1")
    monkeypatch.delenv("GBX_BUNDLE_SKIP_JAR", raising=False)
    monkeypatch.setenv("GBX_DEV_CONTAINER", "gbx-test-container")
    # The module copies databricks_cluster_config.env into os.environ at import; undo that.
    with mock.patch.dict(os.environ):
        module = importlib.import_module("stage_jar_to_volume")
    (tmp_path / "scripts" / "ci").mkdir(parents=True)
    shutil.copy(
        REPO_ROOT / "scripts" / "ci" / "check_jar_packaging.py",
        tmp_path / "scripts" / "ci" / "check_jar_packaging.py",
    )
    (tmp_path / "target").mkdir()
    (tmp_path / "examples" / "tests").mkdir(parents=True)
    monkeypatch.setattr(module, "TESTS_DIR", tmp_path / "examples" / "tests")
    real_run = subprocess.run
    docker_calls = []

    def run(cmd, **kwargs):
        if cmd[0] == "docker":  # the Maven build in the dev container
            docker_calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(module.subprocess, "run", run)
    module.docker_calls = docker_calls
    return module, tmp_path


def _jar(root: Path, names) -> Path:
    jar = root / "target" / JAR_NAME
    with zipfile.ZipFile(jar, "w") as zf:
        for name in names:
            zf.writestr(name, b"x")
    return jar


def test_a_jar_that_bundles_scala_is_refused_with_the_bundling_reason(stage, capsys):
    module, root = stage
    _jar(root, ["com/databricks/labs/gbx/A.class", "scala/Predef.class"])
    assert module.main() == 1
    err = capsys.readouterr().err
    assert "bundles classes the runtime provides" in err


def test_a_jar_that_is_not_a_zip_file_is_refused_without_blaming_bundling(
    stage, capsys
):
    module, root = stage
    (root / "target" / JAR_NAME).write_text("this is not a zip file")
    assert module.main() == 1
    err = capsys.readouterr().err
    assert "could not be checked" in err and JAR_NAME in err
    assert "bundles classes" not in err


def test_a_guard_that_fails_unexpectedly_is_refused_without_blaming_the_jar(
    stage, capsys, monkeypatch
):
    module, root = stage
    _jar(root, ["com/databricks/labs/gbx/A.class"])
    inner = subprocess.run

    def run(cmd, **kwargs):
        if "check_jar_packaging.py" in str(cmd[1]):  # exit 2 is its usage error
            return subprocess.CompletedProcess(cmd, 2)
        return inner(cmd, **kwargs)

    monkeypatch.setattr(subprocess, "run", run)
    assert module.main() == 1
    err = capsys.readouterr().err
    assert "exit 2" in err and "could not be checked" not in err


def test_a_clean_jar_passes_the_guard(stage, capsys):
    module, root = stage
    _jar(root, ["com/databricks/labs/gbx/A.class"])
    assert module.main() == 0
    assert "skipping Databricks upload" in capsys.readouterr().out


@pytest.mark.parametrize("value", [None, "", "  "], ids=["unset", "empty", "blank"])
def test_without_a_container_name_it_stops_instead_of_using_the_main_container(
    stage, monkeypatch, capsys, value
):
    """The name comes from the checkout's gbx-container.sh via the gbx command; a missing one must
    never fall back to geobrix-dev, whose target/ it would then wipe."""
    module, root = stage
    _jar(root, ["com/databricks/labs/gbx/A.class"])
    if value is None:
        monkeypatch.delenv("GBX_DEV_CONTAINER")
    else:
        monkeypatch.setenv("GBX_DEV_CONTAINER", value)

    assert module.main() == 2

    assert "GBX_DEV_CONTAINER" in capsys.readouterr().err
    assert module.docker_calls == []


# --- --from-wheel: the JAR bundled in an env wheel, checked and staged with a sha256 sidecar -----------------------

import base64
import hashlib


def _wheel(tmp_path, *, version="0.6.0", env=6, jar_names=None, jar_bytes=None, recorded=None, record=True):
    """An env wheel with a RECORD, as pip lists it. ``recorded``: the sha256 hex the RECORD says (default: the JAR's)."""
    jar_names = [f"geobrix-{version}-jar-with-dependencies.jar"] if jar_names is None else jar_names
    if jar_bytes is None:
        buf = tmp_path / "bundled.jar"
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("com/databricks/labs/gbx/A.class", b"x")
        jar_bytes = buf.read_bytes()
    whl = tmp_path / f"geobrix-{version}+env{env}-py3-none-any.whl"
    rows = []
    with zipfile.ZipFile(whl, "w") as zf:
        for n in jar_names:
            zf.writestr(f"databricks/labs/gbx/_jar/{n}", jar_bytes)
            hexd = recorded or hashlib.sha256(jar_bytes).hexdigest()
            b64 = base64.urlsafe_b64encode(bytes.fromhex(hexd)).rstrip(b"=").decode()
            rows.append(f"databricks/labs/gbx/_jar/{n},sha256={b64},{len(jar_bytes)}")
        if record:
            zf.writestr(f"geobrix-{version}+env{env}.dist-info/RECORD", "\n".join(rows) + "\n")
    return whl, hashlib.sha256(jar_bytes).hexdigest(), len(jar_bytes)


class _Files:
    def __init__(self, short=()):
        self.calls, self.sizes, self.short = [], {}, set(short)

    def create_directory(self, path):
        self.calls.append(("mkdir", path))

    def upload(self, path, fh, overwrite):
        data = fh.read()
        self.sizes[path] = len(data) - (1 if path.rsplit("/", 1)[1] in self.short else 0)
        self.calls.append(("upload", path, data))

    def get_metadata(self, path):
        return types.SimpleNamespace(content_length=self.sizes[path])


@pytest.fixture
def from_wheel(stage, monkeypatch, tmp_path):
    """``run(wheel, files=None)`` -> exit code of ``main()`` for ``--from-wheel <wheel>``; files: a fake Files API."""
    module, root = stage

    def run(wheel, files=None):
        monkeypatch.setattr(sys, "argv", ["stage_jar_to_volume.py", "--from-wheel", str(wheel)])
        if files is None:
            monkeypatch.setenv("GBX_BUNDLE_SKIP_JAR_UPLOAD", "1")
        else:
            monkeypatch.delenv("GBX_BUNDLE_SKIP_JAR_UPLOAD")
            sdk = types.ModuleType("databricks.sdk")
            sdk.WorkspaceClient = lambda **kw: types.SimpleNamespace(files=files)
            monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
            for k, v in {"DATABRICKS_CONFIG_PROFILE": "p", "GBX_BUNDLE_VOLUME_CATALOG": "c",
                         "GBX_BUNDLE_VOLUME_SCHEMA": "s", "GBX_BUNDLE_VOLUME_NAME": "v"}.items():  # fmt: skip
                monkeypatch.setenv(k, v)
        return module.main()

    run.module = module
    return run


def test_from_wheel_checks_the_bundled_jar_and_builds_nothing(from_wheel, tmp_path, capsys):
    whl, sha, _ = _wheel(tmp_path)
    assert from_wheel(whl) == 0
    out = capsys.readouterr().out
    assert sha in out and "skipping Databricks upload" in out
    assert from_wheel.module.docker_calls == []  # no Maven, no container
    assert not list(tmp_path.glob("gbx-wheel-jar-*"))  # the extracted copy is removed


def test_from_wheel_stages_the_jar_then_its_sidecar_into_the_versioned_jar_folder(from_wheel, tmp_path):
    whl, sha, size = _wheel(tmp_path)
    files = _Files()
    assert from_wheel(whl, files) == 0
    name = "geobrix-0.6.0-jar-with-dependencies.jar"
    folder = "/Volumes/c/s/v/geobrix/0.6.0/jar"
    assert [c[:2] for c in files.calls] == [("mkdir", folder), ("upload", f"{folder}/{name}"),
                                           ("upload", f"{folder}/{name}.sha256")]  # fmt: skip
    assert len(files.calls[1][2]) == size  # the JAR's own bytes
    assert files.calls[2][2] == f"{sha}  {name}\n".encode()  # sha256sum's format


def test_from_wheel_stops_on_a_short_upload_before_the_sidecar(from_wheel, tmp_path, capsys):
    whl, _, _ = _wheel(tmp_path)
    files = _Files(short={"geobrix-0.6.0-jar-with-dependencies.jar"})
    assert from_wheel(whl, files) == 1
    assert "upload verification failed" in capsys.readouterr().err
    assert not [c for c in files.calls if c[0] == "upload" and c[1].endswith(".sha256")]


@pytest.mark.parametrize(
    "kw, why",
    [
        ({"jar_names": []}, "bundles 0 JARs"),
        ({"jar_names": ["geobrix-0.6.0-jar-with-dependencies.jar", "geobrix-0.5.2-jar-with-dependencies.jar"]},
         "bundles 2 JARs"),
        ({"jar_names": ["geobrix-0.5.2-jar-with-dependencies.jar"]}, "wheel is version 0.6.0"),
        ({"recorded": "ab" * 32}, "RECORD lists abababababab"),
        ({"record": False}, "holds 0 RECORD files"),
    ],
)  # fmt: skip
def test_from_wheel_refuses_a_wheel_whose_jar_is_not_the_one_it_recorded(from_wheel, tmp_path, capsys, kw, why):
    whl, _, _ = _wheel(tmp_path, **kw)
    files = _Files()
    assert from_wheel(whl, files) == 1
    assert why in capsys.readouterr().err and files.calls == []  # nothing reached the workspace


def test_from_wheel_refuses_a_jar_that_the_record_does_not_list(from_wheel, tmp_path, capsys):
    whl, _, _ = _wheel(tmp_path)
    src_dir = tmp_path / "other"
    src_dir.mkdir()
    bare = src_dir / whl.name
    with zipfile.ZipFile(whl) as src, zipfile.ZipFile(bare, "w") as dst:
        for item in src.infolist():  # the same wheel with a RECORD that has no row for the JAR
            dst.writestr(item.filename, "x,,\n" if item.filename.endswith("RECORD") else src.read(item.filename))
    assert from_wheel(bare, _Files()) == 1
    assert "RECORD does not list" in capsys.readouterr().err


def test_from_wheel_refuses_a_jar_that_bundles_scala(from_wheel, tmp_path, capsys):
    buf = tmp_path / "scala.jar"
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("scala/Predef.class", b"x")
    whl, _, _ = _wheel(tmp_path, jar_bytes=buf.read_bytes())
    assert from_wheel(whl, _Files()) == 1
    assert "bundles classes the runtime provides" in capsys.readouterr().err


@pytest.mark.parametrize("name", ["missing.whl", "geobrix-0.6.0+env6-py3-none-any.whl"])
def test_from_wheel_names_a_file_that_is_not_there_or_not_a_wheel(from_wheel, tmp_path, capsys, name):
    path = tmp_path / name
    if name != "missing.whl":
        path.write_text("not a zip")
    assert from_wheel(path, _Files()) == 1
    err = capsys.readouterr().err
    assert ("PRECONDITION MISSING" if name == "missing.whl" else "not a readable geobrix wheel") in err


def test_from_wheel_needs_a_path(stage, monkeypatch, capsys):
    module, _ = stage
    monkeypatch.setattr(sys, "argv", ["stage_jar_to_volume.py", "--from-wheel"])
    assert module.main() == 2 and "--from-wheel needs the path" in capsys.readouterr().err
