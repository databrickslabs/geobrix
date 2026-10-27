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

    def run(cmd, **kwargs):
        if cmd[0] == "docker":  # the Maven build in geobrix-dev
            return subprocess.CompletedProcess(cmd, 0)
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(module.subprocess, "run", run)
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


def test_a_clean_jar_passes_the_guard(stage, capsys):
    module, root = stage
    _jar(root, ["com/databricks/labs/gbx/A.class"])
    assert module.main() == 0
    assert "skipping Databricks upload" in capsys.readouterr().out
