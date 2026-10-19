"""Tests for scripts/ci/check_jar_packaging.py (the fat JAR must not bundle scala-library)."""

import subprocess
import sys
import zipfile
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_jar_packaging.py")


def _jar(tmp_path: Path, names: list[str]) -> Path:
    jar = tmp_path / "geobrix-0.0.0-jar-with-dependencies.jar"
    with zipfile.ZipFile(jar, "w") as zf:
        for name in names:
            zf.writestr(name, b"x")
    return jar


def _run(jar: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(jar)], capture_output=True, text=True
    )


def test_rejects_bundled_scala_classes(tmp_path):
    res = _run(
        _jar(tmp_path, ["com/databricks/labs/gbx/A.class", "scala/Predef.class"])
    )
    assert res.returncode == 1
    assert "scala/Predef.class" in res.stderr
    assert "provided" in res.stderr


def test_rejects_scala_library_properties(tmp_path):
    res = _run(
        _jar(tmp_path, ["com/databricks/labs/gbx/A.class", "library.properties"])
    )
    assert res.returncode == 1


def test_accepts_jar_without_scala_library(tmp_path):
    res = _run(
        _jar(
            tmp_path,
            [
                "com/databricks/labs/gbx/A.class",
                "org/gdal/gdal/gdal.class",
                "META-INF/MANIFEST.MF",
            ],
        )
    )
    assert res.returncode == 0, res.stderr
    assert "OK" in res.stdout


def test_usage_error_without_argument():
    res = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True)
    assert res.returncode == 2
