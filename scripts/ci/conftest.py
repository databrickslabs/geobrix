"""Fixtures for the scripts/ci tests of the gbx:dcs:* commands.

A command runs from a copy in a scratch project tree (so target/ and python/geobrix/dist/
are the test's own) with a fake `docker` (_fake_docker.py) first on PATH: no Docker daemon,
no registry. GBX_DCS_REPO is always gbx-test/none, and the fake refuses any argument naming
the real repository.
"""

import fcntl
import hashlib
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

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
        shutil.copy(  # common.sh sources it
            REPO_ROOT / "scripts" / "docker" / "gbx-container.sh",
            self.root / "scripts" / "docker" / "gbx-container.sh",
        )
        (self.root / "scripts" / "docker" / "dcs" / "Dockerfile").write_text(
            "FROM scratch\n"
        )
        for name in ("dcs_lock.py", "verify_image.py"):
            shutil.copy(
                REPO_ROOT / "scripts" / "docker" / "dcs" / name,
                self.root / "scripts" / "docker" / "dcs" / name,
            )
        (self.root / "scripts" / "native").mkdir(parents=True)
        shutil.copy(  # dcs_lock.py prints no index host: its tool output goes through this
            REPO_ROOT / "scripts" / "native" / "redact.py",
            self.root / "scripts" / "native" / "redact.py",
        )
        (self.root / "scripts" / "native" / "pins.conf").write_text(
            "GBX_NATIVE_REVISION=1\nRASTERIO_VERSION=1.5.2\n"
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

    def env_wheel(self, version: str, jar_classes=("com/databricks/labs/gbx/A.class",)):
        """dist/env6/geobrix-<version>+env6-py3-none-any.whl holding its JAR under _jar/ (the image's JAR)."""
        dist = self.root / "dist" / "env6"
        dist.mkdir(parents=True, exist_ok=True)
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as zf:
            for name in jar_classes:
                zf.writestr(name, b"x")
        whl = dist / f"geobrix-{version}+env6-py3-none-any.whl"
        with zipfile.ZipFile(whl, "w") as zf:
            zf.writestr("databricks/labs/gbx/__init__.py", "")
            zf.writestr(
                f"databricks/labs/gbx/_jar/geobrix-{version}-jar-with-dependencies.jar",
                jar.getvalue(),
            )
        return whl

    def lock(self, arch: str = "aarch64") -> dict:
        """The lock, the base list, the built rasterio wheel and a wheelhouse cache for one architecture.

        The cache holds every wheel the lock needs, so dcs_lock.py fetches nothing. Returns the wheel
        names the context's wheelhouse/ must end up with.
        """
        env = self.root / "python" / "geobrix" / "envs" / "env6"
        env.mkdir(parents=True, exist_ok=True)
        dist = self.root / "build" / "native" / "out" / arch / "bindings" / "dist"
        dist.mkdir(parents=True, exist_ok=True)
        rio = dist / f"rasterio-1.5.2+gbx1-cp312-cp312-linux_{arch}.whl"
        rio.write_bytes(b"our rasterio")
        sha = hashlib.sha256(rio.read_bytes()).hexdigest()
        h = "a" * 64
        (env / f"dcs-{arch}.lock").write_text(
            f"click==8.2.1 \\\n    --hash=sha256:{h}\n"
            f"idna==3.20 \\\n    --hash=sha256:{h}\n"
            f"rasterio==1.5.2+gbx1 \\\n    --hash=sha256:{sha}\n"
        )
        (env / f"dcs-base-{arch}.txt").write_text(
            "# base-image: databricksruntime/environment@sha256:" + "1" * 64 + "\nclick==8.2.1\nidna==3.11\n"
        )
        cache = self.root / "build" / "dcs" / "wheelhouse" / arch
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "idna-3.20-py3-none-any.whl").write_bytes(b"idna")
        return {
            "idna-3.20-py3-none-any.whl",
            rio.name,
        }

    def _env(self, env=None) -> dict:
        self.state_file.write_text(json.dumps(self.state))
        full = {k: v for k, v in os.environ.items() if k != "PIP_INDEX_URL"}
        full.update(
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            GBX_DCS_REPO=TEST_REPO,
            FAKE_DOCKER_LOG=str(self.log),
            FAKE_DOCKER_STATE=str(self.state_file),
        )
        full.update(env or {})
        return full

    def run(self, *args: str, env=None) -> subprocess.CompletedProcess:
        res = subprocess.run(
            ["bash", str(self.script), *args],
            capture_output=True,
            text=True,
            env=self._env(env),
            timeout=120,
        )
        res.output = res.stdout + res.stderr
        assert not any("mjohns797" in a for c in self.calls for a in c), self.calls
        return res

    def start(self, *args: str, env=None) -> subprocess.Popen:
        """Start the command in its own process group, so a test can signal it and its
        children together, as a terminal does for Ctrl-C. SIGINT is reset to its default in
        the child: a runner started with SIGINT ignored (`cmd &` in a non-interactive shell)
        would otherwise hand that to bash, which cannot trap a signal ignored at entry.
        """
        return subprocess.Popen(
            ["bash", str(self.script), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=self._env(env),
            start_new_session=True,
            preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
        )

    def path_without(self, *names: str) -> str:
        """A PATH of the fake docker plus the system tools the commands use, minus `names`
        (for a test of a missing tool)."""
        tools = self.bin.parent / "tools-without"
        tools.mkdir(exist_ok=True)
        for tool in (
            "awk",
            "basename",
            "bash",
            "cat",
            "chmod",
            "cp",
            "cut",
            "dirname",
            "grep",
            "mkdir",
            "mktemp",
            "printf",
            "rm",
            "sed",
            "sort",
            "tr",
            "wc",
        ):
            found = shutil.which(tool)
            if tool not in names and found and not (tools / tool).exists():
                (tools / tool).symlink_to(found)
        return f"{self.bin}{os.pathsep}{tools}"

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


class ContainerSandbox:
    """A scratch copy of scripts/commands/ run against a fake `docker` (_fake_docker_local.py).

    For the tests of the gbx:test:* commands: nothing reaches a Docker daemon, work sent into
    the container is recorded (see exec_log), and the container-side test lock is real: the
    holder script runs on this machine on a lock file under tmp_path, with a flock(1) shim
    (_flock_shim.py) when the machine has none (macOS).
    """

    def __init__(self, tmp_path: Path):
        self.root = tmp_path / "repo"
        shutil.copytree(
            REPO_ROOT / "scripts" / "commands", self.root / "scripts" / "commands"
        )
        self.commands = self.root / "scripts" / "commands"
        docker_dir = self.root / "scripts" / "docker"
        docker_dir.mkdir()
        for name in ("gbx-lock.sh", "gbx-container.sh"):
            shutil.copy(REPO_ROOT / "scripts" / "docker" / name, docker_dir)
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        self._shim("docker", "_fake_docker_local.py")
        if shutil.which("flock") is None:
            self._shim("flock", "_flock_shim.py")
        self.lock_file = tmp_path / "gbx-test.lock"
        self.work_lock_file = tmp_path / "gbx-work.lock"
        self.exec_log = tmp_path / "exec.log"
        self.events = tmp_path / "events.log"

    def _shim(self, name: str, module: str) -> None:
        target = self.bin / name
        target.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{Path(__file__).with_name(module)}" "$@"\n'
        )
        target.chmod(0o755)

    def write_command(self, name: str, body: str) -> Path:
        path = self.commands / name
        path.write_text(body)
        path.chmod(0o755)
        return path

    def env(self, **extra) -> dict:
        env = {k: v for k, v in os.environ.items() if not k.startswith("GBX_")}
        env.update(
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            GBX_TEST_LOCK_FILE=str(self.lock_file),
            GBX_WORK_LOCK_FILE=str(self.work_lock_file),
            FAKE_DOCKER_ROOT=str(self.root),
            GBX_DEMO_EVENTS=str(self.events),
            FAKE_DOCKER_EXEC_LOG=str(self.exec_log),
        )
        env.update(extra)
        return env

    def popen(self, command: str, *args: str, **env) -> subprocess.Popen:
        return subprocess.Popen(
            ["bash", str(self.commands / command), *args],
            cwd=self.root,
            env=self.env(**env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            # A runner started with SIGINT ignored (`cmd &` in a non-interactive shell) would hand
            # that to bash, which cannot trap a signal ignored at entry (see `start` above).
            preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
        )

    def run(self, command: str, *args: str, timeout: int = 60, **env):
        proc = self.popen(command, *args, **env)
        out, _ = proc.communicate(timeout=timeout)
        return SimpleNamespace(returncode=proc.returncode, output=out)

    def execs(self) -> list:
        if not self.exec_log.exists():
            return []
        return [json.loads(x) for x in self.exec_log.read_text().splitlines()]

    def exec_envs(self) -> list:
        """The `docker exec -e KEY=VALUE` pairs of each exec call, parallel to execs()."""
        log = Path(str(self.exec_log) + ".env")
        if not log.exists():
            return []
        return [json.loads(x) for x in log.read_text().splitlines()]

    @contextmanager
    def lock_held_by(self, info: str, lock: Path = None, legacy: bool = False):
        """Hold a container lock from this process, with `info` as the holder.

        The RUN lock by default; pass lock=self.work_lock_file for the WORK lock. `legacy`
        writes the info into the lock file itself, as the first version of the helper did.
        """
        lock = lock or self.lock_file
        fd = os.open(str(lock), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            if legacy:
                os.truncate(str(lock), 0)
                os.write(fd, (info + "\n").encode())
            else:
                Path(str(lock) + ".info").write_text(info + "\n")
            yield
        finally:
            os.close(fd)


@pytest.fixture
def sandbox(tmp_path):
    return ContainerSandbox(tmp_path)
