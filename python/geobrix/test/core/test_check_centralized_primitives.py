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
