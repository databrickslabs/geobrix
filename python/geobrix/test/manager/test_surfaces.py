"""The generated surfaces: one golden rendering per output kind, the drift gate on the committed tree,
and the runtime of the generated Python API (first call registers; SQL defaults fill skipped optionals).
"""

import importlib.util
import sys
from pathlib import Path

import pytest

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


# --- the generator's less-travelled paths ---------------------------------------------------------------


def test_docs_section_first_insertion_keeps_prose_and_drops_the_parameter_list():
    gen = _tool("gen_surfaces")
    section = (
        "<Tier both/> <Impl udtf/>\n\nIntro.\n\n"
        "**Signature:** `f(a)` — does X.\n\n"
        "**Parameters:**\n- `a` - thing\n\nMore.\n"
    )
    out = gen._docs_section(section, "<!--REGION-->\n")
    assert out.startswith("<!--REGION-->\n")
    assert out.index("<Impl udtf/>") > out.index("<!--REGION-->")
    assert "Does X." in out and "More." in out and "Intro." in out
    assert "**Parameters:**" not in out and "Signature" not in out


def test_docs_section_keeps_a_parameters_paragraph_that_is_not_the_signatures():
    """A **Parameters:** paragraph elsewhere (in a note, say) is hand-written prose: it stays, and check()
    names it, rather than the generator deleting it."""
    gen = _tool("gen_surfaces")
    section = (
        "<Tier both/>\n\n**Signature:** `f(a)` — does X.\n\nMid.\n\n"
        ":::note\n**Parameters:**\n- `a` - kept\n:::\n"
    )
    assert "- `a` - kept" in gen._docs_section(section, "<!--R-->\n")


def test_check_names_a_hand_written_parameters_paragraph_beside_a_region():
    gen = _tool("gen_surfaces")
    page = gen.REPO / "docs/docs/api/availability.mdx"
    text = "{/* END GENERATED gbx_x */}\n\n**Parameters:**\n- `a` - thing\n\n### next\n"
    (found,) = gen._hand_parameters(page, text)
    assert "hand-written **Parameters:**" in found and "gbx_x" in found
    assert gen._hand_parameters(page, "{/* END GENERATED gbx_x */}\n\nProse.\n") == []


def _check_with(monkeypatch, edit):
    gen = _tool("gen_surfaces")
    target = REPO / "python/geobrix/src/databricks/labs/gbx/gridx/bng/functions.py"
    real = gen.read
    monkeypatch.setattr(
        gen, "read", lambda p: edit(real(p)) if p == target else real(p)
    )
    return gen.check("python")


def test_check_names_a_hand_written_def_that_repeats_a_generated_one(monkeypatch):
    found = _check_with(
        monkeypatch,
        lambda t: t + "\n\ndef bng_eastnorthasbng(easting):\n    return easting\n",
    )
    assert any("bng_eastnorthasbng is defined twice" in f for f in found)


def test_check_names_a_module_that_stopped_importing_api(monkeypatch):
    found = _check_with(
        monkeypatch,
        lambda t: t.replace("from databricks.labs.gbx import _api  # noqa: F401\n", ""),
    )
    assert any("does not import _api" in f for f in found)


def test_check_names_a_missing_generated_region(monkeypatch):
    gen = _tool("gen_surfaces")
    found = _check_with(monkeypatch, lambda t: t.replace(gen.S.PY_BEGIN, "# gone"))
    assert any("a generated region or docs section is missing" in f for f in found)


def test_availability_renders_reader_and_writer_rows():
    import dataclasses

    surfaces = _tool("surfaces")
    man = manifest.load()
    both = manifest.DataSource(
        "gtiff_gdal", "reader_writer", "scala", "x:y", "api/readers", "0.2.0"
    )
    reader = dataclasses.replace(both, name="ogr", kind="reader")
    out = surfaces.render_availability(
        dataclasses.replace(man, datasources=(both, reader))
    )[1]
    assert "| `gtiff_gdal` | ✓ | ✓ | scala | [api/readers](../api/readers) |" in out
    assert "| `ogr` | ✓ |  | scala |" in out


def test_a_datasource_row_parses(monkeypatch):
    import json
    from types import SimpleNamespace

    raw = json.loads(
        (
            REPO / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
        ).read_text()
    )
    raw["datasources"] = [
        {
            "name": "gtiff_gdal",
            "kind": "reader",
            "impl": "scala",
            "ref": "x:y",
            "page": "api/readers",
            "since": "0.2.0",
        }
    ]
    monkeypatch.setattr(manifest, "json", SimpleNamespace(loads=lambda _t: raw))
    (d,) = manifest.load.__wrapped__().datasources
    assert (d.name, d.kind, d.status) == ("gtiff_gdal", "reader", "stable")


def test_a_migrated_row_stays_in_the_function_info_section_that_holds_it():
    surfaces = _tool("surfaces")
    existing = {
        "functions": {
            "_package_rasterx": "-",
            "gbx_rst_a": {"x": 1},
            "_package_rasterx_h3": "-",
            "gbx_rst_h3_b": {"x": 2},
            "_package_gridx_custom": "-",
            "gbx_custom_c": {"x": 3},
        }
    }
    merged = surfaces.merge_function_info(
        existing,
        {
            "gbx_rst_h3_b": {"x": 20},
            "gbx_custom_c": {"x": 30},
            "gbx_rst_new": {"x": 4},
        },
    )["functions"]
    keys = list(merged)
    assert keys.index("gbx_rst_h3_b") > keys.index("_package_rasterx_h3")
    assert keys.index("gbx_rst_h3_b") < keys.index("_package_gridx_custom")
    assert keys.index("gbx_custom_c") > keys.index("_package_gridx_custom")
    assert merged["gbx_rst_new"] == {"x": 4}
    assert keys.index("gbx_rst_new") < keys.index(
        "_package_rasterx_h3"
    )  # a new rst name


def test_first_statement_respects_quotes_and_comments():
    surfaces = _tool("surfaces")
    sql = "SELECT gbx_f('a;b', 'c--d') -- note; here\nFROM t; SELECT gbx_g(1)"
    assert surfaces.first_statement(sql) == "SELECT gbx_f('a;b', 'c--d') FROM t"


def test_a_hand_example_with_a_checkout_path_ships_a_volume_path(monkeypatch):
    surfaces = _tool("surfaces")
    man = manifest.load()
    name = "gbx_st_makevalid"
    monkeypatch.setattr(
        surfaces,
        "hand_written_sql_examples",
        lambda: {name: f"SELECT {name}(x) FROM '{REPO}/src/test/resources/a/b.tif'"},
    )
    ex = surfaces.function_info_entries(man)[name]["examples"]
    assert "/Volumes/<catalog>/<schema>/<volume>/b.tif" in ex and str(REPO) not in ex


def test_an_aggregate_example_needs_literal_per_group_arguments():
    import dataclasses

    from databricks.labs.gbx.bench import oracles

    surfaces = _tool("surfaces")
    fn = manifest.load().functions["gbx_rst_bng_rasterize_agg"]
    row = next(c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name)
    bad = dataclasses.replace(
        row, args=(row.args[0], oracles.Build("x"), *row.args[2:])
    )
    with pytest.raises(ValueError, match="not a literal"):
        surfaces.example_sql(fn, bad)
