"""The Unity Catalog pointer on the function pages is generated from the manifest, not hand-listed.

Each function section opens with ``<Surfaces ... uc ... />``, written by ``gbx:manifest:gen`` for a function
that has a Unity Catalog level (an ``L4`` block). The ``Surfaces`` component turns the ``uc`` flag into a note
that links to the Unity Catalog page. These tests pin the set of marked functions to the manifest and the
note to its link.

A session-only row with a stub text has no UC function and no pointer. Structural only (the loaded manifest + regex
over the mdx and the component): no Spark, runs on host.
"""

import json
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[4]
_MANIFEST = (
    _REPO / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
)
_PAGES = sorted((_REPO / "docs/docs/api").glob("*-functions.mdx"))
_COMPONENT = _REPO / "docs/src/components/Surfaces.js"

_SECTION = re.compile(
    r"BEGIN GENERATED (gbx_\w+):[^\n]*\*/\}\n<Surfaces ([^>]*)/>"
)


def _manifest_uc_set():
    """Functions with a UC form. The loaded manifest drops a row whose L4 is only a stub (no UC function)."""
    from databricks.labs.gbx import manifest

    return {n for n, f in manifest.load().functions.items() if "L4" in f.levels}


def _marked_on_pages():
    marked, seen = set(), set()
    for page in _PAGES:
        for name, flags in _SECTION.findall(page.read_text()):
            seen.add(name)
            if "uc" in flags.split():
                marked.add(name)
    return marked, seen


def test_marked_functions_equal_manifest_uc_set():
    marked, _ = _marked_on_pages()
    expected = _manifest_uc_set()
    assert expected, "the manifest lists no function with a Unity Catalog level"
    assert marked == expected, (
        f"on pages but not in the manifest: {sorted(marked - expected)}; "
        f"in the manifest but not marked: {sorted(expected - marked)}"
    )


def test_every_unity_catalog_function_has_a_page_section():
    _, seen = _marked_on_pages()
    assert _manifest_uc_set() <= seen


def test_a_stub_row_is_not_a_unity_catalog_function():
    from databricks.labs.gbx import manifest

    stubs = {n for n, f in manifest.load().functions.items() if f.stub}
    assert {"gbx_st_crs", "gbx_rst_fromfile"} <= stubs
    assert not stubs & _manifest_uc_set()
    marked, _ = _marked_on_pages()
    assert not stubs & marked


def test_private_unity_catalog_forms_are_not_marked():
    marked, _ = _marked_on_pages()
    assert not {n for n in marked if n.startswith("gbx__")}
    raw = json.loads(_MANIFEST.read_text())
    private = {p["name"] for p in raw["uc_private"]}
    assert private and not (private & marked)


def test_component_links_the_note_to_the_unity_catalog_page():
    src = _COMPONENT.read_text()
    assert "props.uc &&" in src
    assert "/docs/api/uc-functions" in src
    assert "Unity Catalog function for SQL warehouses" in src
