"""Completeness guard for the RasterX docs example TABS.

Every ``<FunctionExamples name=X ...>`` on the raster-functions page renders one tab
per tier listed in ``function-info.json`` ``functions[gbx_X].bindings``. For a tab to
render code AND an output panel, the referenced source module must define both:
    X<suffix>            (the example function / Scala val)
    X<suffix>_output     (the output constant)

Missing either yields an empty/half-rendered tab (as happened with
rst_rastertoworldcoordx, whose SQL example had no ``_output`` constant). This test
catches every such gap at once, plus single-tab ``<CodeFromTest>`` used on a function
whose bindings declare more than one tier (should be the 4-tab component).

Structural only (ast + regex over the mdx + source files) — no Spark, runs on host.
"""

import ast
import json
import os
import re

import pytest

_HERE = os.path.dirname(__file__)
# docs/tests/python/api -> repo root is four levels up.
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
_MDX = os.path.join(_REPO, "docs", "docs", "api", "raster-functions.mdx")
_FI = os.path.join(
    _REPO,
    "src",
    "main",
    "resources",
    "com",
    "databricks",
    "labs",
    "gbx",
    "function-info.json",
)

_SUFFIX = {
    "sql": "_sql_example",
    "python-light": "_python_light_example",
    "python-heavy": "_python_heavy_example",
    "scala": "_scala_example",
}
_PROP_TO_TIER = {
    "sqlSource": "sql",
    "pythonLightSource": "python-light",
    "pythonHeavySource": "python-heavy",
    "scalaSource": "scala",
}


def _load_bindings():
    with open(_FI) as fh:
        return json.load(fh)["functions"]


def _bindings_for(fns, name):
    entry = fns.get(name) or fns.get("gbx_" + name) or {}
    return set(entry.get("bindings", []))


_name_cache = {}


def _module_names(rel_path):
    """Set of top-level names defined in a source module (py def/assign; scala val)."""
    if rel_path in _name_cache:
        return _name_cache[rel_path]
    path = os.path.join(_REPO, rel_path)
    names = set()
    if not os.path.exists(path):
        _name_cache[rel_path] = names
        return names
    if path.endswith(".py"):
        tree = ast.parse(open(path).read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        names.add(t.id)
    else:
        txt = open(path).read()
        names.update(re.findall(r"\bval\s+([A-Za-z0-9_]+)\s*:", txt))
    _name_cache[rel_path] = names
    return names


def _function_examples_blocks():
    mdx = open(_MDX).read()
    out = []
    for block in re.findall(r"<FunctionExamples\b(.*?)/>", mdx, re.S):
        nm = re.search(r'name="([^"]+)"', block)
        if not nm:
            continue
        sources = dict(re.findall(r'(\w+Source)="([^"]+)"', block))
        out.append((nm.group(1), sources))
    return out


_FNS = _load_bindings()
_BLOCKS = _function_examples_blocks()


@pytest.mark.parametrize("name,sources", _BLOCKS, ids=[b[0] for b in _BLOCKS])
def test_function_examples_tabs_complete(name, sources):
    """Each binding-declared tab has both an example and an output constant."""
    tiers = _bindings_for(_FNS, name)
    assert tiers, f"{name}: no bindings in function-info.json — no tabs would render"
    problems = []
    for prop, tier in _PROP_TO_TIER.items():
        if tier not in tiers:
            continue
        src = sources.get(prop)
        if not src:
            problems.append(f"binding '{tier}' present but no {prop} attribute")
            continue
        example = name + _SUFFIX[tier]
        output = example + "_output"
        defined = _module_names(src)
        if example not in defined:
            problems.append(f"tab '{tier}': missing example `{example}` in {src}")
        if output not in defined:
            problems.append(f"tab '{tier}': missing output `{output}` in {src}")
    assert not problems, f"{name} has incomplete example tabs:\n  " + "\n  ".join(
        problems
    )


def test_no_single_tab_on_multitier_function():
    """A <CodeFromTest> single-tab must not be used where bindings declare >1 tier."""
    mdx = open(_MDX).read()
    offenders = []
    for block in re.findall(r"<CodeFromTest\b(.*?)/>", mdx, re.S):
        fn = re.search(r'functionName="([^"]+)"', block)
        if not fn:
            continue
        base = re.sub(
            r"_(sql|python_light|python_heavy|scala)_example$", "", fn.group(1)
        )
        tiers = _bindings_for(_FNS, base)
        if len(tiers) > 1:
            offenders.append(
                f"{base}: single-tab CodeFromTest but bindings={sorted(tiers)}"
            )
    assert (
        not offenders
    ), "single-tab used on multi-tier function(s):\n  " + "\n  ".join(offenders)


def test_blocks_discovered():
    assert (
        len(_BLOCKS) > 50
    ), f"expected 50+ FunctionExamples blocks, found {len(_BLOCKS)}"


def _load_check_script():
    """Import the standalone docs/scripts/check-docs-examples.py (hyphenated name)."""
    import importlib.util

    script = os.path.join(_REPO, "docs", "scripts", "check-docs-examples.py")
    spec = importlib.util.spec_from_file_location("_check_docs_examples", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_annotation_consistency_across_tabs():
    """Tabs of one function whose output table is identical must share the trailing
    annotation. Delegates to the standalone guard (single source of truth) so the
    doc-test suite and `gbx:test:docs-examples` enforce the exact same rule."""
    failures = _load_check_script().check_annotation_consistency()
    assert (
        not failures
    ), "trailing-annotation drift across identical-output tabs:\n  " + (
        "\n  ".join(failures)
    )
