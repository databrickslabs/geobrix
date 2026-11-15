"""Tests for docs/scripts/check-sentence-length.py (warn on added prose sentences over 25 words)."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_SRC = REPO_ROOT / "docs" / "scripts" / "check-sentence-length.py"

LONG = " ".join(f"word{i}" for i in range(30)) + "."
SHORT = "This sentence is short."


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo, check=True, capture_output=True,
    )


def _repo(tmp_path: Path, before: dict, after: dict) -> Path:
    """A temp repo whose last commit changes files from `before` to `after`."""
    repo = tmp_path / "repo"
    (repo / "docs" / "scripts").mkdir(parents=True)
    shutil.copy(SCRIPT_SRC, repo / "docs" / "scripts" / "check-sentence-length.py")
    _git(repo, "init", "-q")
    for step in (before, after):
        for rel, text in step.items():
            path = repo / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "step")
    return repo


def _run(repo: Path, qc_range) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "QC_RANGE"}
    if qc_range is not None:
        env["QC_RANGE"] = qc_range
    return subprocess.run(
        [sys.executable, str(repo / "docs" / "scripts" / "check-sentence-length.py")],
        cwd=repo, env=env, capture_output=True, text=True,
    )


def test_added_long_sentence_is_flagged_with_its_line(tmp_path):
    page = "docs/docs/a.mdx"
    repo = _repo(tmp_path, {page: f"# T\n\n{SHORT}\n"}, {page: f"# T\n\n{SHORT}\n\n{LONG}\n"})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 1
    assert f"{page}:5: 30 words: word0 word1" in res.stdout
    assert "1 sentence" in res.stdout


def test_fence_table_heading_frontmatter_and_import_are_not_checked(tmp_path):
    page = "docs/docs/a.mdx"
    body = (
        "---\ndescription: " + LONG + "\n---\n\n"
        "import X from '@site/x';\n\n"
        "# " + LONG + "\n\n"
        "```python\n" + LONG + "\n```\n\n"
        "| a | b |\n|---|---|\n| " + LONG + " | x |\n\n"
        ":::note\n" + SHORT + "\n:::\n"
    )
    repo = _repo(tmp_path, {page: "x\n"}, {page: body})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 0, res.stdout
    assert "0 sentence" in res.stdout


def test_fence_state_comes_from_the_whole_file_not_the_hunk(tmp_path):
    page = "docs/docs/a.mdx"
    before = "```text\nfirst\nsecond\n```\n"
    after = f"```text\nfirst\n{LONG}\nsecond\n```\n"
    repo = _repo(tmp_path, {page: before}, {page: after})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 0, res.stdout


def test_unchanged_long_sentence_is_not_flagged(tmp_path):
    page = "docs/docs/a.mdx"
    repo = _repo(tmp_path, {page: f"# T\n\n{LONG}\n"}, {page: f"# T\n\n{LONG}\n\n{SHORT}\n"})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 0, res.stdout


def test_code_spans_links_and_wrapped_lines(tmp_path):
    page = "scripts/commands/x.md"
    code = "`" + " ".join(f"c{i}" for i in range(40)) + "`"
    link = "[short text](https://example.com/" + "/".join(f"p{i}" for i in range(40)) + ")"
    wrapped_a = " ".join(f"w{i}" for i in range(15))
    wrapped_b = " ".join(f"v{i}" for i in range(15)) + "."
    body = f"Use {code} and {link} now.\n\n{wrapped_a}\n{wrapped_b}\n"
    repo = _repo(tmp_path, {page: "x\n"}, {page: body})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 1
    assert f"{page}:3: 30 words" in res.stdout
    assert "1 sentence" in res.stdout


def test_out_of_scope_files_are_ignored(tmp_path):
    files = {
        "docs/docs/release-notes-0-5-and-earlier.mdx": LONG + "\n",
        "docs/tests/notes.md": LONG + "\n",
        "docs/build-static-zip/a.md": LONG + "\n",
        "README.md": LONG + "\n",
    }
    repo = _repo(tmp_path, {"docs/docs/z.mdx": "x\n"}, files)
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 0, res.stdout


def test_unset_or_empty_range_exits_zero(tmp_path):
    repo = _repo(tmp_path, {"docs/docs/a.mdx": "x\n"}, {"docs/docs/a.mdx": LONG + "\n"})
    for value in (None, ""):
        res = _run(repo, value)
        assert res.returncode == 0
        assert "QC_RANGE" in res.stdout


def test_bad_range_exits_zero(tmp_path):
    repo = _repo(tmp_path, {"docs/docs/a.mdx": "x\n"}, {"docs/docs/a.mdx": LONG + "\n"})
    res = _run(repo, "nosuchref..HEAD")
    assert res.returncode == 0


def test_bold_lead_in_ends_its_own_sentence(tmp_path):
    page = "docs/docs/a.mdx"
    body = f"**Short lead in.** {' '.join(f'w{i}' for i in range(20))}.\n"
    repo = _repo(tmp_path, {page: "x\n"}, {page: body})
    res = _run(repo, "HEAD~1..HEAD")
    assert res.returncode == 0, res.stdout
