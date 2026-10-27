"""Tests for the nested-`claude -p` lockdown that scripts/commands/gbx-qc-fix.sh applies.

The QC judge shells out to `claude -p` for its LLM checks. It ran with the default tool set
from the repo, where the project's .claude/settings.local.json allowlists test-runner
commands, so a push could have run one. The fix gives that subprocess no tools, no MCP
servers and no session, and disables both judges inside it.

The command runs against a scratch QC_HOME (never ~/.claude): a synthetic qc_io.py that has
the pre-lockdown shape, and, when the real judge is installed, a copy of its real qc_io.py.
"""

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import REPO_ROOT

QC_FIX = REPO_ROOT / "scripts" / "commands" / "gbx-qc-fix.sh"
REAL_QC = Path.home() / ".claude" / "qc-judge"
MARKER = "GBX-QC-FIX"

QC_CORE = f"""
def compute_verdict(sev, spec):
    # {MARKER}: WARN-check ERROR (tooling failure) must not hard-block;
    # CRITICAL still blocks on error (can't-verify = block).
    on_error = spec.get("on_error", "block" if sev == "critical" else "skip")
    return on_error
"""
QC = f"""
def _inline_env_flag(cmd, name):
    '''{MARKER}'''
    return False
"""
# The shape qc_io.py has after adjustments 1-3 and before the lockdown.
QC_IO = f"""
import os
import subprocess


def _effective_llm_model(model):
    '''{MARKER}: gateway model alias.'''
    return model


def run_llm_check(spec, ctx, *, model, llm_timeout):
    prompt = "p"
    model = _effective_llm_model(model)  # {MARKER}
    try:
        r = subprocess.run(
            ["claude", "-p", "--model", model, "--output-format", "text"],
            input=prompt, capture_output=True, text=True, timeout=llm_timeout,
        )
    except subprocess.TimeoutExpired:
        return None
    return r
"""
LOCKDOWN_ARGV = ["--tools", "", "--strict-mcp-config", "--no-session-persistence"]


def _qc_home(tmp_path: Path, qc_io: str) -> Path:
    home = tmp_path / "qc-judge"
    home.mkdir()
    (home / "qc_core.py").write_text(QC_CORE)
    (home / "qc.py").write_text(QC)
    (home / "qc_io.py").write_text(qc_io)
    return home


def _fix(home: Path, *args: str):
    res = subprocess.run(
        ["bash", str(QC_FIX), *args],
        env={**os.environ, "QC_HOME": str(home)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    res.output = res.stdout + res.stderr
    return res


def _load(home: Path):
    spec = importlib.util.spec_from_file_location("qc_io_under_test", home / "qc_io.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_llm_check(module):
    seen = {}

    def fake_run(argv, **kwargs):
        seen["argv"], seen["env"] = argv, kwargs.get("env")
        return subprocess.CompletedProcess(
            argv, 0, stdout="PASS\thigh\tok\n", stderr=""
        )

    with patch.object(module.subprocess, "run", side_effect=fake_run):
        module.run_llm_check({"id": "t"}, {"range": "a..b"}, model="m", llm_timeout=5)
    return seen


def _assert_locked_down(seen):
    argv = seen["argv"]
    assert argv[:2] == ["claude", "-p"]
    i = argv.index("--tools")
    assert (
        argv[i : i + 4] == LOCKDOWN_ARGV
    )  # "--tools" takes the empty string: no tools
    assert seen["env"]["QC_DISABLE"] == "1" and seen["env"]["JUDGE_DISABLE"] == "1"
    assert seen["env"]["PATH"] == os.environ["PATH"]  # the rest of the env is kept


def test_check_reports_the_lockdown_missing_and_changes_nothing(tmp_path):
    home = _qc_home(tmp_path, QC_IO)

    res = _fix(home, "--check")

    assert res.returncode == 1
    assert "nested-claude-lockdown: would-apply" in res.output
    assert (home / "qc_io.py").read_text() == QC_IO
    assert list(home.glob("*.bak")) == []


def test_fix_locks_down_the_nested_claude_and_backs_up_the_file(tmp_path):
    home = _qc_home(tmp_path, QC_IO)

    res = _fix(home)

    assert res.returncode == 0, res.output
    assert "nested-claude-lockdown: applied" in res.output
    assert len(list(home.glob("qc_io.py.gbx-qc-fix.*.bak"))) == 1
    _assert_locked_down(_run_llm_check(_load(home)))


def test_fix_is_idempotent(tmp_path):
    home = _qc_home(tmp_path, QC_IO)
    _fix(home)
    patched = (home / "qc_io.py").read_text()

    res = _fix(home)

    assert res.returncode == 0, res.output
    assert "nested-claude-lockdown: already" in res.output
    assert (home / "qc_io.py").read_text() == patched
    assert len(list(home.glob("qc_io.py.gbx-qc-fix.*.bak"))) == 1  # no second backup
    assert _fix(home, "--check").returncode == 0


PARTIAL_LOCKDOWNS = {
    # the session flag and a JUDGE_DISABLE mention, but no `--tools ""`
    "no-tools-flag": '["claude", "-p", "--no-session-persistence"], env={"JUDGE_DISABLE": "1"}',
    # the argv, but not the env keys
    "no-env-keys": '["claude", "-p", "--tools", "", "--strict-mcp-config", "--no-session-persistence"]',
    # everything but --strict-mcp-config
    "no-strict-mcp": '["claude", "-p", "--tools", "", "--no-session-persistence"], '
    'env={"QC_DISABLE": "1", "JUDGE_DISABLE": "1"}',
}


@pytest.mark.parametrize("partial", sorted(PARTIAL_LOCKDOWNS))
def test_a_partial_lockdown_is_not_read_as_applied(tmp_path, partial):
    """'already' needs the whole lockdown: the argv pair, both flags and both env keys."""
    old_call = 'subprocess.run(\n            ["claude", "-p", "--model", model, "--output-format", "text"],'
    io = QC_IO.replace(
        old_call, f"subprocess.run(\n            {PARTIAL_LOCKDOWNS[partial]},"
    )
    assert io != QC_IO
    home = _qc_home(tmp_path, io)

    res = _fix(home)

    assert "nested-claude-lockdown: already" not in res.output
    assert (
        res.returncode == 1 and "nested-claude-lockdown: missing-anchor" in res.output
    )


def test_a_restructured_judge_is_reported_not_guessed(tmp_path):
    home = _qc_home(
        tmp_path, QC_IO.replace('"-p", "--model"', '"-p", "--effort", "--model"')
    )

    res = _fix(home)

    assert res.returncode == 1
    assert "nested-claude-lockdown: missing-anchor" in res.output
    assert "--tools" not in (home / "qc_io.py").read_text()


@pytest.mark.skipif(
    not (REAL_QC / "qc_io.py").exists(), reason="the QC judge is not installed"
)
def test_the_installed_judges_qc_io_is_locked_down_by_the_command(tmp_path):
    """Fidelity: after the command, (a copy of) the real qc_io.py runs claude locked down."""
    real = (REAL_QC / "qc_io.py").read_text()
    home = _qc_home(tmp_path, real)
    sys.path.insert(0, str(home))
    try:
        res = _fix(home)
        assert res.returncode == 0, res.output
        module = _load(home)
    finally:
        sys.path.remove(str(home))
        shutil.rmtree(home, ignore_errors=True)

    _assert_locked_down(_run_llm_check(module))  # applied now, or already carried
