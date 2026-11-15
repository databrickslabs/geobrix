"""gbx:docs:diagrams: option handling, and --html-only rewrites the committed diagram HTML unchanged."""
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SH = str(REPO_ROOT / "scripts" / "commands" / "gbx-docs-diagrams.sh")
HTML = REPO_ROOT / "resources" / "images" / "diagrams" / "rasterx" / "rasterx-function-categories.html"


def _run(*args):
    return subprocess.run(["bash", SH, *args], capture_output=True, text=True, cwd=REPO_ROOT)


def test_help_exits_0_and_names_the_options():
    r = _run("--help")
    assert r.returncode == 0
    for opt in ("--html-only", "--theme", "--log"):
        assert opt in r.stdout


def test_an_unknown_option_or_a_bad_theme_exits_2():
    assert _run("--nope").returncode == 2
    assert _run("--theme", "purple").returncode == 2


def test_html_only_reproduces_the_committed_html():
    before = HTML.read_text()
    r = _run("--html-only")
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    assert HTML.read_text() == before
