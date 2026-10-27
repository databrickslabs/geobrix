"""Tests for scripts/ci/check_jar_packaging.py (the fat JAR must not bundle Scala, Spark or log4j)."""

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


def test_rejects_bundled_log4j(tmp_path):
    res = _run(
        _jar(
            tmp_path,
            [
                "com/databricks/labs/gbx/A.class",
                "org/apache/logging/log4j/core/Logger.class",
            ],
        )
    )
    assert res.returncode == 1
    assert "org/apache/logging/log4j/core/Logger.class" in res.stderr
    assert "log4j" in res.stderr


def test_rejects_bundled_spark_classes(tmp_path):
    res = _run(
        _jar(
            tmp_path,
            ["com/databricks/labs/gbx/A.class", "org/apache/spark/SparkContext.class"],
        )
    )
    assert res.returncode == 1
    assert "org/apache/spark/SparkContext.class" in res.stderr
    assert "Spark" in res.stderr


def test_accepts_geobrix_spark_adapter_classes(tmp_path):
    res = _run(
        _jar(
            tmp_path,
            [
                "org/apache/spark/sql/adapters/Column.class",
                "com/databricks/labs/gbx/A.class",
            ],
        )
    )
    assert res.returncode == 0, res.stderr
    assert "OK" in res.stdout


def test_ignores_directory_entries_above_geobrix_spark_adapters(tmp_path):
    # The real fat JAR carries zip directory entries for the ancestors of GeoBrix's own
    # org/apache/spark/sql/adapters/ package; those hold no Spark classes.
    res = _run(
        _jar(
            tmp_path,
            [
                "org/",
                "org/apache/",
                "org/apache/spark/",
                "org/apache/spark/sql/",
                "org/apache/spark/sql/adapters/",
                "org/apache/spark/sql/adapters/Column.class",
                "org/apache/logging/",
            ],
        )
    )
    assert res.returncode == 0, res.stderr


def test_reports_every_offending_group(tmp_path):
    res = _run(
        _jar(
            tmp_path,
            [
                "scala/Predef.class",
                "org/apache/logging/log4j/core/Logger.class",
                "org/apache/spark/SparkContext.class",
            ],
        )
    )
    assert res.returncode == 1
    assert "scala/Predef.class" in res.stderr
    assert "org/apache/logging/log4j/core/Logger.class" in res.stderr
    assert "org/apache/spark/SparkContext.class" in res.stderr


def test_usage_error_without_argument():
    res = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True)
    assert res.returncode == 2
    assert "usage" in res.stderr


def test_a_missing_jar_is_exit_3_and_not_reported_as_bundling(tmp_path):
    res = _run(tmp_path / "geobrix-0.0.0-jar-with-dependencies.jar")
    assert res.returncode == 3
    assert "cannot read" in res.stderr and "bundles" not in res.stderr
    assert "Traceback" not in res.stderr


def test_a_corrupt_jar_is_exit_3_and_not_reported_as_bundling(tmp_path):
    jar = tmp_path / "geobrix-0.0.0-jar-with-dependencies.jar"
    jar.write_text("this is not a zip file")
    res = _run(jar)
    assert res.returncode == 3
    assert "cannot read" in res.stderr and "bundles" not in res.stderr
    assert "Traceback" not in res.stderr
