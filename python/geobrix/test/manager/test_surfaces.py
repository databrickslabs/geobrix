"""The generated surfaces: one golden rendering per output kind, the drift gate on the committed tree,
and the runtime of the generated Python API (first call registers; SQL defaults fill skipped optionals).
"""

import importlib.util
import sys
from pathlib import Path

from databricks.labs.gbx import manifest

REPO = Path(__file__).resolve().parents[4]
FIX = Path(__file__).with_name("fixtures") / "surfaces"


def _tool(name):
    path = REPO / "scripts" / "manifest" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"gbx_manifest_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_python_entry_golden():
    surfaces = _tool("surfaces")
    fn = manifest.load().functions["gbx_st_makevalid"]
    assert surfaces.render_python_entry(fn) == (FIX / "st_makevalid.py.txt").read_text()


def test_doc_block_golden():
    surfaces = _tool("surfaces")
    fn = manifest.load().functions["gbx_st_makevalid"]
    assert surfaces.render_doc_block(fn) == (FIX / "st_makevalid.mdx.txt").read_text()


def test_readme_keeps_the_vizx_badge_outside_the_generated_region():
    readme = (REPO / "README.md").read_text()
    begin, end = readme.index("<!-- BEGIN GENERATED counts -->"), readme.index(
        "<!-- END GENERATED counts -->"
    )
    assert "badge/VizX-" in readme and "badge/VizX-" not in readme[begin:end]


def test_canonical_manifest_keeps_the_driver_classes_and_connect_class():
    import json

    gen = _tool("gen_surfaces")
    raw = json.loads(gen.MANIFEST_JSON.read_text())
    again = json.loads(gen._canonical_manifest(raw))
    assert again == {**raw, "functions": again["functions"]}
    assert sorted(again["functions"], key=lambda r: r["sql_name"]) == sorted(
        raw["functions"], key=lambda r: r["sql_name"]
    )
    assert set(again["gdal"]["raster_drivers"]) == {
        "allowed",
        "bytes",
        "bytes_refused_light",
        "refused",
    }
    assert any("connect_class" in r["levels"].get("L2", {}) for r in again["functions"])


def test_the_committed_tree_has_no_drift():
    findings = _tool("gen_surfaces").check()
    assert findings == [], "\n".join(findings)


def test_a_hand_edit_inside_a_region_is_drift(monkeypatch):
    gen = _tool("gen_surfaces")
    target = REPO / "python/geobrix/src/databricks/labs/gbx/gridx/bng/functions.py"
    edited = target.read_text().replace(
        "def bng_eastnorthasbng(", "def bng_eastnorthasbng_x(", 1
    )
    real = gen.read
    monkeypatch.setattr(gen, "read", lambda p: edited if p == target else real(p))
    assert any("gridx/bng/functions.py" in f for f in gen.check("python"))


def test_middle_optional_none_takes_the_sql_default(spark):
    """A synthetic row with two optionals (no manifest row is touched, nothing registers): the first left as
    None before the second is set gets its SQL default; a trailing None is not sent."""
    import dataclasses

    from databricks.labs.gbx import _api
    from databricks.labs.gbx.manifest import Param

    base = manifest.load().functions["gbx_st_makevalid"]
    fn = dataclasses.replace(
        base,
        params=(
            Param("geom", ("BINARY", "STRING"), "GEOMETRY"),
            Param("mode", ("STRING",), "STRING", "'linework'", py="lit"),
            Param("tolerance", ("DOUBLE",), "DOUBLE", "0.0"),
        ),
        min_args=1,
    )
    middle = [str(c) for c in _api._columns(fn, ("g", None, 2.0), fill_all=False)]
    assert len(middle) == 3 and "linework" in middle[1] and "2.0" in middle[2], middle
    trailing = _api._columns(fn, ("g", "ogc", None), fill_all=False)
    assert len(trailing) == 2


def test_first_python_call_registers_through_the_manager_on_connect(
    monkeypatch, capsys
):
    from databricks.labs.gbx import _api, manager
    from databricks.labs.gbx.manager import messages

    from .connect_stub import ConnectStub

    spark = ConnectStub()
    seen = []

    def fake_register(s, target="session", only=None, **kw):
        seen.append(tuple(only))
        messages.reminder(s)
        return {n: object() for n in only}

    monkeypatch.setattr(manager, "register", fake_register)
    _api.ensure(spark, "gbx_bng_eastnorthasbng")
    _api.ensure(spark, "gbx_bng_eastnorthasbng")
    assert seen == [("gbx_bng_eastnorthasbng",)]
    assert capsys.readouterr().out.count("geobrix 0.6.0") == 1
