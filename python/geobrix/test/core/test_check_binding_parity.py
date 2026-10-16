"""Tests for docs/scripts/check-binding-parity.py: the per-package heavy-module check.

check_heavy_module_coverage must resolve each canonical name to exactly one heavy
Python module (longest matching prefix) and report it missing when only another module
-- e.g. a light registration table -- mentions it (the gap behind GitHub issue #102).
"""

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_bp",
    Path(__file__).resolve().parents[4] / "docs/scripts/check-binding-parity.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def _modules(tmp_path, **sources):
    out = {}
    for name, text in sources.items():
        p = tmp_path / f"{name}.py"
        p.write_text(text)
        out[name] = p
    return out


def test_name_only_in_another_module_is_missing(tmp_path, monkeypatch):
    m = _load()
    mods = _modules(
        tmp_path,
        rasterx='def rst_a(t):\n    return call("gbx_rst_a", t)\n',
        pyrx='_UDFS = {"gbx_rst_a": 1, "gbx_rst_b": 2}\n',
    )
    monkeypatch.setattr(m, "PREFIX_TO_HEAVY_MODULE", {"gbx_rst_": mods["rasterx"]})
    monkeypatch.setattr(m, "PYTHON_REGISTERED", set())
    missing = m.check_heavy_module_coverage({"gbx_rst_a", "gbx_rst_b"})
    assert missing == {mods["rasterx"]: ["gbx_rst_b"]}


def test_longest_prefix_picks_the_module(tmp_path, monkeypatch):
    m = _load()
    mods = _modules(
        tmp_path,
        vectorx='call("gbx_st_x")\n',
        legacy='call("gbx_st_legacyaswkb")\n',
    )
    monkeypatch.setattr(
        m,
        "PREFIX_TO_HEAVY_MODULE",
        {"gbx_st_legacy": mods["legacy"], "gbx_st_": mods["vectorx"]},
    )
    monkeypatch.setattr(m, "PYTHON_REGISTERED", set())
    assert m.check_heavy_module_coverage({"gbx_st_x", "gbx_st_legacyaswkb"}) == {}
    # The legacy name in vectorx alone does not satisfy its own module.
    mods["legacy"].write_text("")
    mods["vectorx"].write_text('call("gbx_st_x")\ncall("gbx_st_legacyaswkb")\n')
    assert m.check_heavy_module_coverage({"gbx_st_legacyaswkb"}) == {
        mods["legacy"]: ["gbx_st_legacyaswkb"]
    }


def test_light_only_names_are_exempt(tmp_path, monkeypatch):
    m = _load()
    mods = _modules(tmp_path, rasterx="")
    monkeypatch.setattr(m, "PREFIX_TO_HEAVY_MODULE", {"gbx_rst_": mods["rasterx"]})
    monkeypatch.setattr(m, "PYTHON_REGISTERED", {"gbx_rst_fromfile"})
    assert m.check_heavy_module_coverage({"gbx_rst_fromfile"}) == {}
