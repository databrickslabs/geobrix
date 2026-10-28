"""Every launcher in examples/tests answers ``--help`` / ``-h`` before any contact.

A launcher builds a JAR, stages artifacts to a Volume, submits a job or runs a bundle, so a stray
``--help`` must not start any of that. Each launcher runs in a clean subprocess: no profile or token, an
empty HOME, stand-ins for ``docker`` and ``databricks`` that record any use, and the REAL Python
environment: no fake SDK, only a ``sitecustomize`` hook that records any attempt to import
``databricks.sdk``. A launcher that imports the SDK at module level therefore fails, whether or not the
SDK is installed. Launchers are discovered (every script with a ``__main__`` entry point, ``test_*``
named ones included), so a new one is covered without touching this file.

Pure (no workspace, no Spark). Run:
  gbx:test:notebooks --path test_launcher_help.py
"""

import os
import pathlib
import re
import subprocess
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parent

_MAIN_GUARD = re.compile(r"^if __name__ == [\"']__main__[\"']:", re.M)

LAUNCHERS = sorted(
    p.name
    for p in TESTS_DIR.glob("*.py")
    if p.name not in ("conftest.py", pathlib.Path(__file__).name)
    and _MAIN_GUARD.search(p.read_text())
)

_RECORD_SDK_IMPORT = """\
import os
import sys


class _Recorder:
    def find_spec(self, name, path=None, target=None):
        if name == "databricks.sdk" or name.startswith("databricks.sdk."):
            with open(os.environ["CONTACT_MARKER"], "a") as f:
                f.write("import " + name + "\\n")
        return None


sys.meta_path.insert(0, _Recorder())
"""

_FAKE_TOOL = '#!/bin/sh\necho "$(basename "$0")" >> "$CONTACT_MARKER"\nexit 1\n'


@pytest.fixture(scope="module")
def clean_run(tmp_path_factory):
    """(env, marker): an environment with no credentials, whose docker and CLI record use and in which an
    import of the Databricks SDK is recorded."""
    root = tmp_path_factory.mktemp("launcher_help")
    hook = root / "hook"
    hook.mkdir()
    (hook / "sitecustomize.py").write_text(_RECORD_SDK_IMPORT)
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
        "PYTHONPATH": str(hook),
        "CONTACT_MARKER": str(marker),
    }
    return env, marker


@pytest.mark.parametrize("script", LAUNCHERS)
def test_help_exits_zero_before_any_contact(script, clean_run):
    env, marker = clean_run
    for flag in ("--help", "-h"):
        marker.unlink(missing_ok=True)  # contact is judged per launcher and flag
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
