"""Tests for the GitHub Actions pinning scripts in scripts/security/.

resolve-action-ref picks, for each action, the newest release that is at least GBX_ACTION_MIN_AGE_DAYS days old
(default 7), and resolves its tag to a commit SHA. pin-gh-actions rewrites the matching `uses:` lines to that SHA with
one tag comment. Both run here against a fake `gh` on PATH (no network) and a scratch git repository.
"""

import json
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone

import pytest
from conftest import REPO_ROOT

SECURITY = REPO_ROOT / "scripts" / "security"
OLD_SHA = "a" * 40
SHAS = {"v1.0.0": "1" * 40, "v1.1.0": "2" * 40, "v2.0.0": "3" * 40}

pytestmark = pytest.mark.skipif(shutil.which("jq") is None, reason="resolve-action-ref needs jq")


def _iso(days_ago):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fake_gh(tmp_path, releases):
    """A `gh` that answers the two calls resolve-action-ref makes: the release list (one JSON object per line, as
    `--jq` would print) and the tag ref (`commit <sha>`). Anything else fails, so a new call shows up here."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    lines = "\n".join(json.dumps({"published_at": p, "tag_name": t}) for t, p in releases)
    refs = "\n".join(f'  *git/ref/tags/{tag}) echo "commit {sha}" ;;' for tag, sha in SHAS.items())
    gh = bindir / "gh"
    gh.write_text(
        "#!/bin/sh\n"
        'case "$2" in\n'
        f"  */releases) cat <<'JSON'\n{lines}\nJSON\n ;;\n"
        f"{refs}\n"
        '  *) echo "fake gh: unexpected call: $*" >&2; exit 9 ;;\n'
        "esac\n"
    )
    gh.chmod(0o755)
    return {**os.environ, "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}"}


def _resolve(env, *args):
    return subprocess.run(
        ["sh", str(SECURITY / "resolve-action-ref"), *args], env=env, capture_output=True, text=True, check=False
    )


def test_resolve_skips_a_release_younger_than_the_minimum_age(tmp_path):
    """v2.0.0 is 3 days old, so the 7-day default picks v1.1.0 (30 days), not the newest tag."""
    env = _fake_gh(tmp_path, [("v1.0.0", _iso(90)), ("v1.1.0", _iso(30)), ("v2.0.0", _iso(3))])
    out = _resolve(env, f"owner/act@{OLD_SHA}")
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == f"⚠ owner/act@{OLD_SHA}: owner/act@{SHAS['v1.1.0']}  # v1.1.0"


def test_resolve_takes_the_minimum_age_from_the_environment(tmp_path):
    """With GBX_ACTION_MIN_AGE_DAYS=1 the 3-day-old v2.0.0 qualifies; a pin already at it prints a check mark."""
    env = _fake_gh(tmp_path, [("v1.1.0", _iso(30)), ("v2.0.0", _iso(3))])
    env["GBX_ACTION_MIN_AGE_DAYS"] = "1"
    out = _resolve(env, f"owner/act@{SHAS['v2.0.0']}")
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == f"✓ owner/act@{SHAS['v2.0.0']}"


def test_resolve_names_the_minimum_age_when_no_release_qualifies(tmp_path):
    env = _fake_gh(tmp_path, [("v2.0.0", _iso(3))])
    out = _resolve(env, f"owner/act@{OLD_SHA}")
    assert out.stdout == ""
    assert "no release at least 7 days old" in out.stderr


def test_pin_replaces_the_ref_and_its_tag_comment(tmp_path):
    """The old `# v1.0.0` comment is replaced, not left beside the new one; an unrelated action is untouched."""
    repo = tmp_path / "repo"
    wf = repo / ".github" / "workflows"
    wf.mkdir(parents=True)
    other = "        uses: other/act@" + "b" * 40 + "  # v9.0.0\n"
    (wf / "ci.yml").write_text(f"steps:\n      - uses: owner/act@{OLD_SHA}  # v1.0.0\n      - name: x\n{other}")
    git = {"cwd": repo, "check": True, "capture_output": True}
    subprocess.run(["git", "init", "-q"], **git)
    subprocess.run(["git", "add", "."], **git)
    ident = ["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"]
    subprocess.run(["git", *ident, "commit", "-q", "--no-verify", "-m", "base"], **git)
    line = f"⚠ owner/act@{OLD_SHA}: owner/act@{SHAS['v1.1.0']}  # v1.1.0\n"
    out = subprocess.run(
        ["sh", str(SECURITY / "pin-gh-actions")], cwd=repo, input=line, capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stderr
    text = (wf / "ci.yml").read_text()
    assert f"      - uses: owner/act@{SHAS['v1.1.0']}  # v1.1.0\n" in text
    assert "v1.0.0" not in text and OLD_SHA not in text
    assert other in text
