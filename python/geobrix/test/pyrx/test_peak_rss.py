"""peak_rss_mib: diagnostics-only peak RSS that must not need the POSIX ``resource`` module."""

import ast
import subprocess
import sys
import types
from pathlib import Path

import pytest

from databricks.labs.gbx.pyrx.core import budget

_SRC = Path(__file__).resolve().parents[2] / "src"


def _hide(monkeypatch, *names):
    for name in names:
        monkeypatch.setitem(sys.modules, name, None)  # None => ImportError on import


def test_peak_rss_positive_and_matches_resource():
    import resource

    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    expected = raw / (1024 * 1024) if sys.platform == "darwin" else raw / 1024
    got = budget.peak_rss_mib()
    assert isinstance(got, float) and got > 0
    # high-water mark only grows; allow small drift between the two reads
    assert expected <= got <= expected + 64


def test_peak_rss_falls_back_to_psutil_rss(monkeypatch):
    _hide(monkeypatch, "resource")
    info = types.SimpleNamespace(rss=200 * 1024 * 1024)
    fake = types.SimpleNamespace(
        Process=lambda: types.SimpleNamespace(memory_info=lambda: info)
    )
    monkeypatch.setitem(sys.modules, "psutil", fake)
    assert budget.peak_rss_mib() == pytest.approx(200.0)


def test_peak_rss_psutil_prefers_peak_wset(monkeypatch):
    _hide(monkeypatch, "resource")
    info = types.SimpleNamespace(rss=100 * 1024 * 1024, peak_wset=300 * 1024 * 1024)
    fake = types.SimpleNamespace(
        Process=lambda: types.SimpleNamespace(memory_info=lambda: info)
    )
    monkeypatch.setitem(sys.modules, "psutil", fake)
    assert budget.peak_rss_mib() == pytest.approx(300.0)


def test_peak_rss_none_without_resource_or_psutil(monkeypatch):
    _hide(monkeypatch, "resource", "psutil")
    assert budget.peak_rss_mib() is None


def test_preparer_and_open_tile_import_without_resource():
    code = (
        "import sys\n"
        "sys.modules['resource'] = None\n"
        "import databricks.labs.gbx.pyrx.core.preparer as p\n"
        "import databricks.labs.gbx.pyrx.core.open_tile\n"
        "import databricks.labs.gbx.bench.runner as r\n"
        "import databricks.labs.gbx.bench.compression_sweep\n"
        "v = p._peak_rss_mib()\n"
        "assert v is None or v > 0\n"
        "assert r.peak_rss_mb() >= 0.0\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-2000:]


def _module_level_imports(tree):
    """Imports executed at import time: module body, descending into if/try/with but not defs."""
    stack = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Import):
            yield from (a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module.split(".")[0]
        elif isinstance(node, (ast.If, ast.Try, ast.With)):
            for field in ("body", "orelse", "finalbody", "handlers"):
                for child in getattr(node, field, []) or []:
                    stack.extend(
                        child.body if isinstance(child, ast.ExceptHandler) else [child]
                    )


def test_no_module_level_import_of_posix_only_resource():
    offenders = [
        str(p.relative_to(_SRC))
        for p in _SRC.rglob("*.py")
        if "resource"
        in set(_module_level_imports(ast.parse(p.read_text(encoding="utf-8"))))
    ]
    assert (
        offenders == []
    ), f"module-level `import resource` breaks import on non-POSIX: {offenders}"
