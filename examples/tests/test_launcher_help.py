"""Every launcher in examples/tests answers ``--help`` / ``-h`` before any contact.

A launcher builds a JAR, stages artifacts to a Volume, or submits a job, so a stray ``--help``
must not start any of that. Each launcher runs in a clean subprocess: no profile or token, an
empty HOME, and stand-ins for the Databricks SDK, ``docker`` and ``databricks`` that record
any use. Launchers are discovered (every non-test script with a ``__main__`` guard), so a new
one is covered without touching this file.

Pure (no workspace, no databricks-sdk, no Spark). Run:
  gbx:test:notebooks --path test_launcher_help.py
"""

import os
import pathlib
import subprocess
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parent

LAUNCHERS = sorted(
    p.name
    for p in TESTS_DIR.glob("*.py")
    if not p.name.startswith(("test_", "conftest")) and '__name__ == "__main__"' in p.read_text()
)

_FAKE_SDK = '''\
import os


class WorkspaceClient:
    def __init__(self, *args, **kwargs):
        with open(os.environ["CONTACT_MARKER"], "a") as f:
            f.write("WorkspaceClient\\n")
        raise RuntimeError("workspace contact")
'''

_FAKE_SDK_SERVICE = """\
import types


def __getattr__(name):
    return types.SimpleNamespace()
"""

_FAKE_TOOL = '#!/bin/sh\necho "$(basename "$0")" >> "$CONTACT_MARKER"\nexit 1\n'


@pytest.fixture(scope="module")
def clean_run(tmp_path_factory):
    """(env, marker): an environment with no credentials whose SDK, docker and CLI record use."""
    root = tmp_path_factory.mktemp("launcher_help")
    sdk = root / "stub" / "databricks" / "sdk"
    (sdk / "service").mkdir(parents=True)
    (sdk / "__init__.py").write_text(_FAKE_SDK)
    (sdk / "service" / "__init__.py").write_text(_FAKE_SDK_SERVICE)
    bin_dir = root / "bin"
    bin_dir.mkdir()
    for tool in ("docker", "databricks"):
        (bin_dir / tool).write_text(_FAKE_TOOL)
        (bin_dir / tool).chmod(0o755)
    home = root / "home"
    home.mkdir()
    marker = root / "contact.txt"
    env = {
        "PATH": f"{bin_dir}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "HOME": str(home),
        "PYTHONPATH": str(root / "stub"),
        "CONTACT_MARKER": str(marker),
    }
    return env, marker


@pytest.mark.parametrize("script", LAUNCHERS)
def test_help_exits_zero_before_any_contact(script, clean_run):
    env, marker = clean_run
    for flag in ("--help", "-h"):
        done = subprocess.run(
            [sys.executable, str(TESTS_DIR / script), flag],
            env=env,
            cwd=env["HOME"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        contact = marker.read_text() if marker.exists() else ""
        assert done.returncode == 0, f"{script} {flag}: exit {done.returncode}\n{done.stderr[-600:]}"
        assert done.stdout.strip(), f"{script} {flag} printed no usage"
        assert not contact, f"{script} {flag} contacted: {contact}"
