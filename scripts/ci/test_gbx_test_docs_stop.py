"""Guards for gbx:docs:stop --port: one port, and never another checkout's server.

A tiny `python3 -m http.server` stands in for Docusaurus (the stop command goes by listening
port and working directory, not by what the server is), on random free ports; the ports the
real servers use (3000, 3001) are never touched.
"""

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import REPO_ROOT


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _listening(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _wait(predicate, seconds=8.0) -> bool:
    end = time.time() + seconds
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.1)
    return False


class DocsSandbox:
    def __init__(self, tmp_path: Path):
        self.root = (tmp_path / "repo").resolve()
        commands = self.root / "scripts" / "commands"
        commands.mkdir(parents=True)
        for name in (
            "gbx-docs-stop.sh",
            "gbx-docs-dev.sh",
            "gbx-docs-start.sh",
            "gbx-docs-serve-local.sh",
            "gbx-docs-restart.sh",
            "gbx-docs-build.sh",
            "common.sh",
        ):
            shutil.copy(REPO_ROOT / "scripts" / "commands" / name, commands)
        (self.root / "scripts" / "docker").mkdir()
        shutil.copy(
            REPO_ROOT / "scripts" / "docker" / "gbx-container.sh",
            self.root / "scripts" / "docker",
        )
        self.tmp = tmp_path
        self.servers = []
        self.ports = []

    def serve(self, cwd: Path) -> int:
        cwd.mkdir(parents=True, exist_ok=True)
        port = _free_port()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(port),
                "--bind",
                "127.0.0.1",
            ],
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.servers.append(proc)
        self.ports.append(port)
        assert _wait(lambda: _listening(port)), "listener did not start"
        return port

    def stop(self, *args):
        return subprocess.run(
            ["bash", str(self.root / "scripts" / "commands" / "gbx-docs-stop.sh"), *args],
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def cleanup(self):
        for proc in self.servers:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
        for port in self.ports:
            for suffix in ("pid", "log"):
                Path(f"/tmp/docusaurus-{port}.{suffix}").unlink(missing_ok=True)


@pytest.fixture
def docs(tmp_path):
    box = DocsSandbox(tmp_path)
    yield box
    box.cleanup()


def test_stop_port_stops_only_that_port(docs):
    mine = docs.root / "docs"
    a = docs.serve(mine)
    b = docs.serve(mine)
    res = docs.stop("--port", str(a))
    assert res.returncode == 0, res.stdout + res.stderr
    assert _wait(lambda: not _listening(a)), "the named port is still serving"
    assert _listening(b), "stop --port also stopped another port"


def test_stop_port_refuses_another_checkout_without_force(docs, tmp_path):
    other = (tmp_path / "other-checkout" / "docs").resolve()
    port = docs.serve(other)
    res = docs.stop("--port", str(port))
    out = res.stdout + res.stderr
    assert res.returncode != 0
    assert _listening(port), "a server of another checkout was stopped without --force"
    assert str(other.parent) in out, "the message must name the owning checkout"
    assert "--force" in out
    assert "gbx-docs-stop.sh" in out and str(other.parent) in out


def test_stop_port_force_stops_another_checkout(docs, tmp_path):
    other = (tmp_path / "other-checkout" / "docs").resolve()
    port = docs.serve(other)
    res = docs.stop("--port", str(port), "--force")
    assert res.returncode == 0, res.stdout + res.stderr
    assert _wait(lambda: not _listening(port))


def test_stop_port_with_nothing_listening_is_ok(docs):
    res = docs.stop("--port", str(_free_port()))
    assert res.returncode == 0, res.stdout + res.stderr


def test_stop_without_port_names_what_it_stops(docs):
    # No --port: stop-all keeps today's behaviour. Its sweep covers only the fixed docs ports,
    # which a test must not use, so assert the surface (the help text) rather than run it.
    res = docs.stop("--help")
    assert res.returncode == 0
    assert "--port" in res.stdout and "--force" in res.stdout


def test_dev_restart_leaves_other_ports_alone_and_refuses_other_checkout(docs, tmp_path):
    other = (tmp_path / "other-checkout" / "docs").resolve()
    held = docs.serve(other)
    res = subprocess.run(
        [
            "bash",
            str(docs.root / "scripts" / "commands" / "gbx-docs-dev.sh"),
            "--port",
            str(held),
        ],
        cwd=docs.root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    out = res.stdout + res.stderr
    assert res.returncode != 0
    assert _listening(held), "dev stopped a server held by another checkout"
    assert "--force" in out and str(other.parent) in out


def test_restart_refuses_another_checkout_without_force(docs, tmp_path):
    other = (tmp_path / "other-checkout" / "docs").resolve()
    held = docs.serve(other)
    res = subprocess.run(
        [
            "bash",
            str(docs.root / "scripts" / "commands" / "gbx-docs-restart.sh"),
            "--port",
            str(held),
        ],
        cwd=docs.root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode != 0
    assert _listening(held)
    assert "--force" in res.stdout + res.stderr


def test_build_leaves_another_checkouts_server_running(docs, tmp_path):
    # The sandbox has no docs/ directory, so the build exits right after the stop step.
    other = (tmp_path / "other-checkout" / "docs").resolve()
    held = docs.serve(other)
    Path(f"/tmp/docusaurus-{held}.pid").write_text(str(docs.servers[-1].pid))
    res = subprocess.run(
        ["bash", str(docs.root / "scripts" / "commands" / "gbx-docs-build.sh"), "--no-restart"],
        cwd=docs.root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert _listening(held), "build stopped a server of another checkout"
    assert f"port {held} alone" in res.stdout + res.stderr


def test_build_stops_its_own_servers_only(docs):
    mine = docs.root / "docs"
    own = docs.serve(mine)
    Path(f"/tmp/docusaurus-{own}.pid").write_text(str(docs.servers[-1].pid))
    res = subprocess.run(
        ["bash", str(docs.root / "scripts" / "commands" / "gbx-docs-build.sh"), "--no-restart"],
        cwd=docs.root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode != 0  # no docs/ build tree in the sandbox
    assert _wait(lambda: not _listening(own)), "build left its own checkout's server up"


@pytest.mark.parametrize(
    "script",
    [
        "gbx-docs-stop.sh",
        "gbx-docs-restart.sh",
        "gbx-docs-dev.sh",
        "gbx-docs-start.sh",
        "gbx-docs-serve-local.sh",
    ],
)
@pytest.mark.parametrize("args", [["--port"], ["--port", "abc"], ["--port", "--force"]])
def test_port_without_a_number_exits_2_with_usage(docs, script, args):
    # `--port` as the last argument used to loop forever (`shift 2` fails with one argument left and shifts nothing).
    res = subprocess.run(
        ["bash", str(docs.root / "scripts" / "commands" / script), *args],
        cwd=docs.root,
        capture_output=True,
        text=True,
        timeout=20,
    )
    out = res.stdout + res.stderr
    assert res.returncode == 2, out
    assert "--port" in out and "USAGE" in out
