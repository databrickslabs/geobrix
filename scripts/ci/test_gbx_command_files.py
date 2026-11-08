"""Every gbx command script is registered and executable.

A command is a ``scripts/commands/gbx-<name>.sh`` implementation plus its ``.md`` registration. The script
must be committed with the executable bit, so ``scripts/commands/gbx-<name>.sh`` runs directly; a script
committed as mode 100644 runs only through ``bash <script>`` and fails each command's own CI check.
This test covers every command at once, so a new command cannot land without the bit.
"""

import subprocess

from conftest import REPO_ROOT

COMMANDS = REPO_ROOT / "scripts" / "commands"


def _scripts():
    return sorted(COMMANDS.glob("gbx-*.sh"))


def test_there_are_command_scripts():
    assert _scripts(), f"no gbx-*.sh found under {COMMANDS}"


def test_every_command_script_has_its_md_registration():
    missing = [s.name for s in _scripts() if not s.with_suffix(".md").is_file()]
    assert not missing, f"command scripts without a .md registration: {missing}"


def test_every_command_script_is_committed_executable():
    out = subprocess.run(
        ["git", "ls-files", "-s", "--", "scripts/commands/gbx-*.sh"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    not_exec = [line.split("\t", 1)[1] for line in out.splitlines() if not line.startswith("100755 ")]
    assert not not_exec, (
        "command scripts committed without the executable bit: "
        f"{not_exec}. Fix: chmod +x <script> && git update-index --chmod=+x <script>"
    )
