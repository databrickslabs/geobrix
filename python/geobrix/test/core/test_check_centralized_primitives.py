"""Tests for docs/scripts/check-centralized-primitives.py.

Verifies the pure _evaluate() core and that main() is non-blocking on warn-only hits.
"""

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_cp",
    Path(__file__).resolve().parents[4]
    / "docs/scripts/check-centralized-primitives.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_evaluate_flags_fail_and_warn_and_respects_allow():
    m = _load()
    rows = [
        {
            "primitive": "scratch",
            "signature": r"\btempfile\.mkdtemp\(",
            "allow": "src/gbx/ds/_scratch.py",
            "level": "fail",
            "rule": "writer-scaffolding.md",
        },
        {
            "primitive": "crs",
            "signature": r"ProjCRS\.from_",
            "allow": "src/gbx/pyrx/core/crs.py",
            "level": "warn",
            "rule": "crs-resolution.md",
        },
    ]
    files = {
        "src/gbx/ds/_write_bad.py": "import tempfile\nd = tempfile.mkdtemp()\n",  # scratch FAIL
        "src/gbx/ds/_scratch.py": "d = tempfile.mkdtemp()\n",  # allow-listed → ignored
        "src/gbx/ds/x.py": "c = ProjCRS.from_epsg(4326)\n",  # crs WARN
        "src/gbx/pyrx/core/crs.py": "c = ProjCRS.from_epsg(4326)\n",  # allow-listed → ignored
    }
    advisories, violations = m._evaluate(rows, files)
    assert any("scratch" in v and "_write_bad.py" in v for v in violations)
    assert all("_scratch.py" not in v for v in violations)  # allow respected
    assert any("crs" in a and "x.py" in a for a in advisories)
    assert all("core/crs.py" not in a for a in advisories)  # allow respected
    assert len(violations) == 1  # only the fail-level scratch hit


def test_main_exit_zero_when_only_warn(monkeypatch, tmp_path):
    m = _load()
    monkeypatch.setattr(
        m,
        "_load_registry",
        lambda: [
            {
                "primitive": "crs",
                "signature": r"ProjCRS\.from_",
                "allow": "",
                "level": "warn",
                "rule": "crs-resolution.md",
            }
        ],
    )
    monkeypatch.setattr(
        m, "_gather_files", lambda: {"a.py": "ProjCRS.from_epsg(4326)\n"}
    )
    assert m.main() == 0  # warn-only → non-blocking


def test_advisory_truncation_notice(monkeypatch, capsys):
    """When advisories exceed the print cap (40), a '… and N more' line must appear.

    TDD: this test was written failing-first against the original advisories[:40] truncation
    that produced no notice for the hidden hits.  The fix adds a trailing notice line.
    """
    m = _load()
    n_hits = 45  # more than the 40-item cap
    monkeypatch.setattr(
        m,
        "_load_registry",
        lambda: [
            {
                "primitive": f"dummy{i}",
                "signature": rf"\bdummy_{i}_call\(",
                "allow": "",
                "level": "warn",
                "rule": "some-rule.md",
            }
            for i in range(n_hits)
        ],
    )
    monkeypatch.setattr(
        m,
        "_gather_files",
        lambda: {f"file{i}.py": f"dummy_{i}_call()\n" for i in range(n_hits)},
    )
    assert m.main() == 0  # warn-only → non-blocking
    captured = capsys.readouterr()
    # The header must report the true total
    assert f"{n_hits} warn-level hit(s)" in captured.out
    # The truncation notice must appear for the 5 hidden hits
    assert f"… and {n_hits - 40} more advisory hit(s)" in captured.out
