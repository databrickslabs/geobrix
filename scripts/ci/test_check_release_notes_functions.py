"""check-release-notes-functions: the base revision's function set.

A revision before the 0.6 manifest has no functions.json, so every function looked "added" (258 against main, 16
of them flagged though they shipped in 0.5.x). The base then reads that revision's function-info.json instead.
"""

import importlib.util
import json

from conftest import REPO_ROOT

_SPEC = importlib.util.spec_from_file_location(
    "check_release_notes_functions", REPO_ROOT / "docs/scripts/check-release-notes-functions.py"
)
mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mod)


def _fake_show(files):
    def show(rev, path):
        return files.get((rev, path.name), "")

    return show


def test_a_base_without_the_manifest_reads_its_function_info(monkeypatch):
    info = json.dumps({"functions": {"_package_rasterx": "--- rasterx ---", "gbx_rst_type": {}, "gbx_bng_aswkt": {}}})
    monkeypatch.setattr(mod, "_show", _fake_show({("old", "function-info.json"): info}))
    assert mod.names_at("old") == {"gbx_rst_type", "gbx_bng_aswkt"}


def test_a_base_with_the_manifest_reads_the_manifest(monkeypatch):
    manifest = json.dumps({"functions": [{"sql_name": "gbx_rst_type"}]})
    info = json.dumps({"functions": {"gbx_other": {}}})
    monkeypatch.setattr(
        mod, "_show", _fake_show({("new", "functions.json"): manifest, ("new", "function-info.json"): info})
    )
    assert mod.names_at("new") == {"gbx_rst_type"}


def test_a_function_that_shipped_before_the_manifest_is_not_added(monkeypatch):
    info = json.dumps({"functions": {"gbx_rst_type": {}}})
    manifest = json.dumps({"functions": [{"sql_name": "gbx_rst_type"}, {"sql_name": "gbx_rst_gdal"}]})
    monkeypatch.setattr(
        mod, "_show", _fake_show({("old", "function-info.json"): info, ("HEAD", "functions.json"): manifest})
    )
    assert mod.added_functions("old..HEAD") == ["gbx_rst_gdal"]
