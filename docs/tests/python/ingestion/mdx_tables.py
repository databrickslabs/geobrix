"""Read the tables of the Ingestion Pipelines pages, so tests can hold them to the code."""

import ast
import inspect
from pathlib import Path

PAGES = Path(__file__).resolve().parents[3] / "docs" / "ingestion"


def page(name):
    """Text of ``docs/docs/ingestion/<name>.mdx``."""
    return (PAGES / f"{name}.mdx").read_text()


def section_rows(name, heading, level="###"):
    """Table rows under ``<level> <heading>`` on page ``name`` (up to the next
    heading), as lists of stripped cells. The first table's header and separator
    rows are dropped; a later table in the section keeps its own."""
    text = page(name)
    marker = f"\n{level} {heading}\n"
    assert marker in text, f"{name}.mdx has no heading {level} {heading!r}"
    section = text.split(marker, 1)[1].split("\n#", 1)[0]
    rows = [
        [c.strip() for c in line.strip().strip("|").split("|")]
        for line in section.splitlines()
        if line.startswith("|")
    ]
    return rows[2:]


def calls(snippet, name):
    """Every call of ``name(...)`` in a Python snippet."""
    return [
        n
        for n in ast.walk(ast.parse(snippet))
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == name
    ]


def bind(fn, call):
    """Bind a displayed call to ``fn``'s real signature (literal arguments are
    evaluated, others stand in as objects); raises TypeError on a mismatch.
    Returns the bound arguments."""
    args = [
        ast.literal_eval(a) if isinstance(a, ast.Constant) else object()
        for a in call.args
    ]
    kwargs = {
        k.arg: ast.literal_eval(k.value)
        if isinstance(k.value, (ast.Constant, ast.Dict))
        else object()
        for k in call.keywords
    }
    return inspect.signature(fn).bind(*args, **kwargs)
