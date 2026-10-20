"""Fixtures for the scripts/ci tests of the gbx:dcs:* commands.

A command runs from a copy in a scratch project tree (so target/ and python/geobrix/dist/
are the test's own) with a fake `docker` (_fake_docker.py) first on PATH: no Docker daemon,
no registry. GBX_DCS_REPO is always gbx-test/none, and the fake refuses any argument naming
the real repository.
"""

import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_REPO = "gbx-test/none"


class DcsCommand:
    def __init__(self, tmp_path: Path, command: str):
        self.root = tmp_path / "repo"
        commands = self.root / "scripts" / "commands"
        commands.mkdir(parents=True)
        for name in (command, "common.sh"):
            shutil.copy(REPO_ROOT / "scripts" / "commands" / name, commands / name)
        (self.root / "scripts" / "ci").mkdir()
        shutil.copy(
            REPO_ROOT / "scripts" / "ci" / "check_jar_packaging.py",
            self.root / "scripts" / "ci" / "check_jar_packaging.py",
        )
        (self.root / "scripts" / "docker" / "dcs").mkdir(parents=True)
        (self.root / "scripts" / "docker" / "dcs" / "Dockerfile").write_text(
            "FROM scratch\n"
        )
        self.script = commands / command
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        fake = Path(__file__).with_name("_fake_docker.py")
        docker = self.bin / "docker"
        docker.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{fake}" "$@"\n')
        docker.chmod(0o755)
        self.log = tmp_path / "docker.log"
        self.state_file = tmp_path / "docker.json"
        self.state = {"images": {}}

    def jar(self, version: str, names=("com/databricks/labs/gbx/A.class",)) -> Path:
        target = self.root / "target"
        target.mkdir(exist_ok=True)
        jar = target / f"geobrix-{version}-jar-with-dependencies.jar"
        with zipfile.ZipFile(jar, "w") as zf:
            for name in names:
                zf.writestr(name, b"x")
        return jar

    def wheel(self, version: str) -> Path:
        dist = self.root / "python" / "geobrix" / "dist"
        dist.mkdir(parents=True, exist_ok=True)
        whl = dist / f"geobrix-{version}-py3-none-any.whl"
        whl.write_bytes(b"not a real wheel")
        return whl

    def run(self, *args: str, env=None) -> subprocess.CompletedProcess:
        self.state_file.write_text(json.dumps(self.state))
        full = {k: v for k, v in os.environ.items() if k != "PIP_INDEX_URL"}
        full.update(
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            GBX_DCS_REPO=TEST_REPO,
            FAKE_DOCKER_LOG=str(self.log),
            FAKE_DOCKER_STATE=str(self.state_file),
        )
        full.update(env or {})
        res = subprocess.run(
            ["bash", str(self.script), *args],
            capture_output=True,
            text=True,
            env=full,
            timeout=120,
        )
        res.output = res.stdout + res.stderr
        assert not any("mjohns797" in a for c in self.calls for a in c), self.calls
        return res

    @property
    def calls(self) -> list:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def commands(self) -> list:
        return [c[0] for c in self.calls]


@pytest.fixture
def dcs_publish(tmp_path):
    return DcsCommand(tmp_path, "gbx-dcs-publish.sh")


@pytest.fixture
def dcs_build(tmp_path):
    return DcsCommand(tmp_path, "gbx-dcs-build.sh")
