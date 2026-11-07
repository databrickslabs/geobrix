"""Tests for docs/scripts/check-binding-parity.py.

The check reads the function manifest as its source of truth. A gate that reads an empty or
moved source must FAIL, not pass: 0 canonical functions once printed "OK" while verifying nothing.
Also pins the per-package heavy-module check (GitHub issue #102) and the manifest-derived Scala
requirement.
"""

import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "docs/scripts"


def _load():
    sys.path.insert(0, str(SCRIPTS))
    try:
        spec = importlib.util.spec_from_file_location(
            "check_bp", SCRIPTS / "check-binding-parity.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(SCRIPTS))
    return mod


def _row(l1=True, **extra):
    levels = {"L3": {}, "L4": {}}
    if l1:
        levels["L1"] = {"companion": "x.Y"}
    return {"levels": levels, **extra}


def _fake(monkeypatch, m, rows, *, legacy=(), scala=(), python=(), info=()):
    monkeypatch.setattr(m, "manifest_rows", lambda: rows)
    monkeypatch.setattr(m, "canonical_sql", lambda: set(rows))
    monkeypatch.setattr(m, "legacy_sql", lambda: set(legacy))
    monkeypatch.setattr(m, "scala_names", lambda: set(scala))
    monkeypatch.setattr(m, "python_names", lambda: set(python))
    monkeypatch.setattr(m, "function_info_keys", lambda: set(info))
    monkeypatch.setattr(m, "check_heavy_module_coverage", lambda rows: {})


# --- the committed tree ---------------------------------------------------------------------------------


def test_the_committed_tree_checks_a_real_number_of_functions(capsys):
    m = _load()
    assert m.main() == 0, capsys.readouterr().out
    assert len(m.canonical_sql()) > 200


def test_package_map_matches_the_manifest_loader():
    from databricks.labs.gbx import manifest

    assert _load().PACKAGE_OF_PREFIX == manifest.PACKAGE_OF_PREFIX


# --- a silent zero never passes -------------------------------------------------------------------------


def test_zero_canonical_functions_fails(monkeypatch, capsys):
    m = _load()
    _fake(monkeypatch, m, {}, scala={"gbx_a"}, python={"gbx_a"}, info={"gbx_a"})
    assert m.main() == 1
    assert "0 canonical functions" in capsys.readouterr().out


def test_an_empty_binding_scan_fails(monkeypatch, capsys):
    m = _load()
    rows = {"gbx_rst_a": _row(l1=False)}
    _fake(monkeypatch, m, rows, python={"gbx_rst_a"}, info={"gbx_rst_a"})
    # Scala scan found nothing, but the row has no L1: only the empty scan itself fails.
    assert m.main() == 1
    assert "Scala (override def name) found 0 names" in capsys.readouterr().out


# --- what is compared -----------------------------------------------------------------------------------


def test_a_manifest_name_missing_from_a_binding_fails(monkeypatch, capsys):
    m = _load()
    rows = {"gbx_rst_a": _row(), "gbx_rst_b": _row()}
    _fake(
        monkeypatch,
        m,
        rows,
        scala={"gbx_rst_a", "gbx_rst_b"},
        python={"gbx_rst_a", "gbx_rst_b"},
        info={"gbx_rst_a"},
    )
    assert m.main() == 1
    out = capsys.readouterr().out
    assert "missing from function-info.json" in out and "gbx_rst_b" in out


def test_scala_is_required_only_for_a_row_with_an_l1_level(monkeypatch, capsys):
    m = _load()
    rows = {"gbx_rst_jvm": _row(), "gbx_rst_light": _row(l1=False)}
    names = {"gbx_rst_jvm", "gbx_rst_light"}
    _fake(monkeypatch, m, rows, scala={"gbx_rst_jvm"}, python=names, info=names)
    assert m.main() == 0, capsys.readouterr().out
    _fake(monkeypatch, m, rows, scala={"gbx_rst_light"}, python=names, info=names)
    assert m.main() == 1
    out = capsys.readouterr().out
    assert "missing from Scala" in out and "gbx_rst_jvm" in out


def test_a_legacy_list_name_without_a_manifest_row_fails(monkeypatch, capsys):
    m = _load()
    rows = {"gbx_rst_a": _row(l1=False)}
    _fake(
        monkeypatch,
        m,
        rows,
        legacy={"gbx_rst_a", "gbx_rst_gone"},
        scala={"gbx_x"},
        python={"gbx_rst_a"},
        info={"gbx_rst_a"},
    )
    assert m.main() == 1
    assert "gbx_rst_gone" in capsys.readouterr().out


def test_canonical_comes_from_the_manifest_json(tmp_path, monkeypatch):
    m = _load()
    js = tmp_path / "functions.json"
    js.write_text(
        json.dumps(
            {
                "functions": [
                    {"sql_name": "gbx_rst_s", "levels": {}},
                    {"sql_name": "gbx_mvp_rst_p", "status": "mvp", "levels": {}},
                ]
            }
        )
    )
    monkeypatch.setitem(m.stable_rows.__globals__, "_JSON", js)
    assert m.canonical_sql() == {"gbx_rst_s"}


# --- the per-package heavy-module check -----------------------------------------------------------------


def _module(root, module, text):
    path = root / "databricks/labs/gbx" / module.replace(".", "/") / "functions.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_name_only_in_another_module_is_missing(tmp_path):
    m = _load()
    raster = _module(tmp_path, "rasterx", 'call("gbx_rst_a")\n')
    _module(tmp_path, "pyrx", '_UDFS = {"gbx_rst_a": 1, "gbx_rst_b": 2}\n')
    rows = {"gbx_rst_a": _row(), "gbx_rst_b": _row()}
    assert m.check_heavy_module_coverage(rows, tmp_path) == {raster: ["gbx_rst_b"]}


def test_each_row_resolves_to_its_own_prefix_package(tmp_path):
    m = _load()
    h3 = _module(tmp_path, "gridx.h3", "")
    _module(tmp_path, "rasterx", 'call("gbx_h3_cell_bbox")\n')
    rows = {"gbx_h3_cell_bbox": _row()}
    # A name in another package's module does not count: it must be in gridx.h3.
    assert m.check_heavy_module_coverage(rows, tmp_path) == {h3: ["gbx_h3_cell_bbox"]}
    h3.write_text('call("gbx_h3_cell_bbox")\n')
    assert m.check_heavy_module_coverage(rows, tmp_path) == {}


def test_a_row_python_module_overrides_the_prefix(tmp_path):
    m = _load()
    _module(tmp_path, "rasterx", "")
    other = _module(tmp_path, "pmtiles", 'call("gbx_rst_a")\n')
    rows = {"gbx_rst_a": _row(python={"module": "pmtiles"})}
    assert m.check_heavy_module_coverage(rows, tmp_path) == {}
    other.write_text("")
    assert m.check_heavy_module_coverage(rows, tmp_path) == {other: ["gbx_rst_a"]}


def test_light_only_and_probe_rows_are_exempt(tmp_path):
    m = _load()
    _module(tmp_path, "rasterx", "")
    rows = {"gbx_rst_light": _row(l1=False), "gbx_rst_probe": _row(python=None)}
    assert m.check_heavy_module_coverage(rows, tmp_path) == {}
