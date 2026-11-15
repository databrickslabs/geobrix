"""
Tests for the Install without PyPI access snippets (offline_install.py).
"""

import hashlib
import json
import re
import shlex
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import offline_install  # noqa: E402

VOL = "/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6"
WHEEL = "geobrix-0.6.0+env6-py3-none-any.whl"


def test_the_download_command_parses_with_the_cli(monkeypatch):
    """The documented command is one the CLI takes: both arches by default, the UC pins, the env wheel."""
    from databricks.labs.gbx import stage

    cmd = offline_install.DOWNLOAD_SHELL.split("\n\n")[-1].replace("\\\n", " ")
    argv = shlex.split(cmd.split("\n", 1)[1])
    assert argv[:3] == ["python", "-m", "databricks.labs.gbx.stage"]
    got = {}
    monkeypatch.setattr(stage, "download_deps", lambda out, **kw: got.update(out=out, **kw))
    assert stage.main(argv[3:]) == 0
    assert got == {
        "out": "gbx-wheels",
        "arches": ("x86_64", "aarch64"),
        "extras": (),
        "uc": True,
        "wheel": WHEEL,
        "python": None,
    }


def test_the_staging_snippet_copies_and_checks_each_file(tmp_path, monkeypatch, capsys):
    """stage_deps copies the folder the download filled, sha256-checked, and prints the install lines."""
    from databricks.labs.gbx import stage

    src = tmp_path / "gbx-wheels"
    files = {WHEEL: b"env wheel", "x86_64/h3-4.5.0-py3-none-any.whl": b"h3", "aarch64/h3-4.5.0-py3-none-any.whl": b"h3"}
    for rel, data in files.items():
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_bytes(data)
    (src / "wheels.txt").write_text("".join(f"{hashlib.sha256(d).hexdigest()}  {r}\n" for r, d in files.items()))
    vol = tmp_path / "vol"
    code = offline_install.STAGE_DEPS_PYTHON.replace(VOL, str(vol)).replace('"gbx-wheels"', repr(str(src)))
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)  # the folder is a local one in this test
    monkeypatch.setattr(stage, "_on_databricks", lambda: True)  # copy as through the Volume's mount
    exec(compile(code, "STAGE_DEPS_PYTHON", "exec"), {})
    assert all((vol / r).read_bytes() == d for r, d in files.items())
    out = capsys.readouterr().out
    assert f"--find-links {vol}/x86_64" in out and "deps='volume'" in out
    assert (vol / "requirements-x86_64.txt").read_text().startswith("--no-index\n")


def test_the_notebook_and_serverless_lines_match_what_stage_deps_prints():
    from databricks.labs.gbx import stage

    printed = stage.install_lines(VOL, WHEEL, ["x86_64"])
    nb = " ".join(offline_install.NOTEBOOK_PIP.replace("\\\n", " ").split())
    assert nb in printed
    serverless = offline_install.SERVERLESS_DEPENDENCIES.splitlines()
    assert " | ".join(serverless) in printed


def test_the_uc_install_from_a_volume_names_no_index():
    """The documented install, as a dry run: every Python function installs from the Volume folders, without PyPI."""
    from databricks.labs import gbx

    code = offline_install.UC_INSTALL_FROM_VOLUME.replace("<catalog>", "main").replace("<schema>", "geobrix")
    code = code.replace("<volume>", "artifacts").replace("gbx.uc.install(", "stmts = gbx.uc.install(")
    code = code.replace('deps="volume",', 'deps="volume", dry_run=True,')
    ns = {}
    exec(compile(code, "UC_INSTALL_FROM_VOLUME", "exec"), ns)
    found = [re.search(r"dependencies = '(\[.*?\])'", s) for s in ns["stmts"]]
    envs = [json.loads(m.group(1).replace("\\'", "'")) for m in found if m]
    folder = "/Volumes/main/geobrix/artifacts/geobrix/0.6.0/env6"
    assert envs and all(e[:3] == ["--no-index", f"--find-links={folder}/x86_64", f"--find-links={folder}/aarch64"] for e in envs)
    with pytest.raises(ValueError, match="deps='volume' only"):
        gbx.uc.install("main", "geobrix", folder, dry_run=True, arch="aarch64")
