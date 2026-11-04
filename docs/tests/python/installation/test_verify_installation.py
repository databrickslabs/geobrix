"""
Tests for installation verification snippets (one-copy doc).
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import verify_installation


def test_verify_python_installation_constant():
    """Installation doc: Python verification snippet is defined."""
    assert hasattr(verify_installation, "VERIFY_PYTHON_INSTALLATION")
    s = verify_installation.VERIFY_PYTHON_INSTALLATION
    assert "gbx.register(spark)" in s
    assert "SHOW FUNCTIONS" in s and "gbx_rst_*" in s


def test_verify_sql_functions_constant():
    """Installation doc: SQL verification snippet is defined."""
    assert hasattr(verify_installation, "VERIFY_SQL_FUNCTIONS")
    s = verify_installation.VERIFY_SQL_FUNCTIONS
    assert "SHOW FUNCTIONS LIKE 'gbx_*'" in s
    assert "DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox" in s


def test_verify_output_constants():
    """Installation doc: example output constants for Verification section."""
    assert hasattr(verify_installation, "VERIFY_PYTHON_INSTALLATION_output")
    assert hasattr(verify_installation, "VERIFY_SQL_FUNCTIONS_output")
    assert "function" in verify_installation.VERIFY_PYTHON_INSTALLATION_output
    assert "gbx_rst" in verify_installation.VERIFY_SQL_FUNCTIONS_output


def test_register_snippet_runs(spark):
    """Installation doc: the register snippet runs against a real session."""
    s = verify_installation.REGISTER_PYTHON_INSTALLATION
    assert "gbx.register(spark)" in s
    exec(compile(s, "REGISTER_PYTHON_INSTALLATION", "exec"), {"spark": spark})
    assert spark.sql("SHOW FUNCTIONS LIKE 'gbx_rst_width'").count() >= 1


def test_stage_jar_snippet_copies_the_jar_and_refuses_a_second(tmp_path, monkeypatch):
    """Installation doc: stage_jar copies the wheel's JAR with a sidecar and refuses another JAR in the folder."""
    from databricks.labs.gbx import _native, stage

    assert "gbx.stage_jar(" in verify_installation.STAGE_JAR_PYTHON
    jar = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    jar.write_bytes(b"jar bytes")
    monkeypatch.setattr(_native, "jar_path", lambda: str(jar))
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)  # the folder is a local one in this test
    folder = tmp_path / "volume" / "jar"
    from databricks.labs import gbx

    staged = gbx.stage_jar(str(folder))
    assert staged == str(folder / jar.name)
    sidecar = (folder / (jar.name + ".sha256")).read_text()
    assert sidecar.split()[1] == jar.name and len(sidecar.split()[0]) == 64
    assert gbx.stage_jar(str(folder)) == staged  # a second call leaves the staged copy as it is
    (folder / "geobrix-0.5.2-jar-with-dependencies.jar").write_bytes(b"old")
    with pytest.raises(RuntimeError, match="one geobrix JAR per folder"):
        gbx.stage_jar(str(folder))


def test_uc_install_snippet_previews_its_statements():
    """Installation doc: the install call is valid; a dry run returns the statements and sends none."""
    from databricks.labs import gbx

    assert "gbx.uc.install(" in verify_installation.UC_INSTALL_PYTHON
    stmts = gbx.uc.install("main", "geobrix", "/Volumes/main/geobrix/artifacts/env6", dry_run=True)
    assert stmts and all("main" in s for s in stmts[:1])
